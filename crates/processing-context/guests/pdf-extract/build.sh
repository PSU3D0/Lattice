#!/usr/bin/env bash
set -euo pipefail

SOURCE_URL="https://github.com/jrmuizel/pdf-extract"
SOURCE_TAG="v0.12.0"
SOURCE_COMMIT="b95bf9f6268772d5088f09b0034e488e64294835"
RUST_TOOLCHAIN="1.93.0"
WASM_TOOLS_VERSION="1.245.1"
MAX_MEMORY_BYTES="67108864"
NORMALIZED_CUSTOM_SECTIONS='^(name|producers)$'

HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
MODE=${1:---verify}
case "$MODE" in
    --verify|--write) ;;
    *) echo "usage: $0 [--verify|--write]" >&2; exit 2 ;;
esac

command -v git >/dev/null
command -v patch >/dev/null
command -v python3 >/dev/null
command -v wasm-tools >/dev/null

test "$(rustc +"$RUST_TOOLCHAIN" --version)" = "rustc 1.93.0 (254b59607 2026-01-19)"
test "$(cargo +"$RUST_TOOLCHAIN" --version)" = "cargo 1.93.0 (083ac5135 2025-12-15)"
test "$(wasm-tools --version)" = "wasm-tools $WASM_TOOLS_VERSION"
rustup target list --toolchain "$RUST_TOOLCHAIN" --installed | grep -qx wasm32-unknown-unknown

umask 077
LOCK_ROOT="/tmp/lattice-pdf-transform-v1.lock"
if ! mkdir -m 700 -- "$LOCK_ROOT"; then
    echo "private PDF transform build lock already exists: $LOCK_ROOT" >&2
    exit 1
fi
cleanup() {
    rm -rf -- "$LOCK_ROOT"
}
trap cleanup EXIT HUP INT TERM
if [ ! -d "$LOCK_ROOT" ] \
    || [ -L "$LOCK_ROOT" ] \
    || [ "$(stat -c %u -- "$LOCK_ROOT")" != "$(id -u)" ] \
    || [ "$(stat -c %a -- "$LOCK_ROOT")" != "700" ]; then
    echo "private PDF transform build lock failed owner/type/mode verification" >&2
    exit 1
fi
STAGING_ROOT="$LOCK_ROOT/staging"
RESULT_ONE="$LOCK_ROOT/result-a"
RESULT_TWO="$LOCK_ROOT/result-b"
mkdir -m 700 -- "$RESULT_ONE" "$RESULT_TWO"

RUST_FLAGS=(
    '--cfg getrandom_backend="unsupported"'
    '-C target-feature=-bulk-memory,-bulk-memory-opt,-multivalue,-reference-types,-simd128,-relaxed-simd,-atomics'
    '-C link-arg=--no-entry'
    '-C link-arg=--export-memory'
    '-C link-arg=--compress-relocations'
    "-C link-arg=--max-memory=$MAX_MEMORY_BYTES"
    '-C strip=symbols'
)

build_one() {
    local root=$1
    mkdir -m 700 -- "$root"
    local repository="$root/upstream.git"
    local source="$root/pdf-extract"
    local project="$root/guest"

    git init -q "$repository"
    git -C "$repository" remote add upstream "$SOURCE_URL"
    git -C "$repository" fetch -q --depth 1 upstream "refs/tags/$SOURCE_TAG"
    local actual_commit
    actual_commit=$(git -C "$repository" rev-parse 'FETCH_HEAD^{commit}')
    test "$actual_commit" = "$SOURCE_COMMIT"

    mkdir -p "$source" "$project"
    git -C "$repository" archive "$SOURCE_COMMIT" | tar -x -C "$source"
    test "$(sha256sum "$source/Cargo.toml" | cut -d' ' -f1)" = \
        "20d0372f5d621a3eafc3fb2a839741b7d1fc39f8ebf06ad17321b7e061e3f6b9"
    patch --quiet --fuzz=0 -d "$source" -p1 < "$HERE/pdf-extract-no-wasm-js.patch"
    grep -Fqx 'lopdf = {version = "0.42", default-features = false}' "$source/Cargo.toml"
    ! grep -q 'wasm_js' "$source/Cargo.toml"

    cp "$HERE/Cargo.toml" "$project/Cargo.toml"
    cp "$HERE/Cargo.wasm.lock" "$project/Cargo.lock"
    cp -R "$HERE/src" "$project/src"
    printf '\n[patch.crates-io]\npdf-extract = { path = "%s" }\n' "$source" >> "$project/Cargo.toml"

    local remap="--remap-path-prefix=$root=/build"
    local rustflags="${RUST_FLAGS[*]} $remap"
    env \
        CARGO_HOME="$root/cargo-home" \
        CARGO_TARGET_DIR="$root/target" \
        CARGO_INCREMENTAL=0 \
        RUSTFLAGS="$rustflags" \
        SOURCE_DATE_EPOCH=0 \
        TZ=UTC \
        LC_ALL=C \
        cargo +"$RUST_TOOLCHAIN" build \
            --manifest-path "$project/Cargo.toml" \
            --release \
            --target wasm32-unknown-unknown \
            --locked \
            --quiet

    local raw="$root/target/wasm32-unknown-unknown/release/lattice_pdf_extract_guest.wasm"
    python3 "$HERE/inspect_wasm.py" "$raw"
    wasm-tools strip --delete "$NORMALIZED_CUSTOM_SECTIONS" "$raw" -o "$root/normalized.wasm"
    wasm-tools validate --features=-reference-types,-bulk-memory,-multi-value,-simd,-relaxed-simd,-threads,-memory64,-multi-memory,-function-references,-tail-call "$root/normalized.wasm"
    python3 "$HERE/inspect_wasm.py" "$root/normalized.wasm"
}

build_one "$STAGING_ROOT"
cp "$STAGING_ROOT/normalized.wasm" "$RESULT_ONE/normalized.wasm"
rm -rf -- "$STAGING_ROOT"
build_one "$STAGING_ROOT"
cp "$STAGING_ROOT/normalized.wasm" "$RESULT_TWO/normalized.wasm"
cmp "$RESULT_ONE/normalized.wasm" "$RESULT_TWO/normalized.wasm"

MODULE="$RESULT_ONE/normalized.wasm"
MODULE_SHA256=$(sha256sum "$MODULE" | cut -d' ' -f1)
MODULE_SIZE=$(stat -c %s "$MODULE")
PATCH_SHA256=$(sha256sum "$HERE/pdf-extract-no-wasm-js.patch" | cut -d' ' -f1)
LOCK_SHA256=$(sha256sum "$HERE/Cargo.wasm.lock" | cut -d' ' -f1)
SOURCE_SHA256=$(sha256sum "$HERE/src/lib.rs" | cut -d' ' -f1)
NOTICE_SHA256=$(sha256sum "$HERE/PROVENANCE.md" | cut -d' ' -f1)
BUILD_SCRIPT_SHA256=$(sha256sum "$HERE/build.sh" | cut -d' ' -f1)
INSPECTOR_SHA256=$(sha256sum "$HERE/inspect_wasm.py" | cut -d' ' -f1)
MANIFEST_INPUT_SHA256=$(sha256sum "$HERE/Cargo.toml" | cut -d' ' -f1)
TOOLCHAIN_SHA256=$(sha256sum "$HERE/rust-toolchain.toml" | cut -d' ' -f1)

python3 - "$RESULT_ONE/manifest.json" <<PY
import json
import sys
manifest = {
    "abi_version": "lattice.transform.v1",
    "build": {
        "cargo": "cargo 1.93.0 (083ac5135 2025-12-15)",
        "flags": [
            "--cfg getrandom_backend=\"unsupported\"",
            "-C target-feature=-bulk-memory,-bulk-memory-opt,-multivalue,-reference-types,-simd128,-relaxed-simd,-atomics",
            "-C link-arg=--no-entry",
            "-C link-arg=--export-memory",
            "-C link-arg=--compress-relocations",
            "-C link-arg=--max-memory=$MAX_MEMORY_BYTES",
            "-C strip=symbols",
            "--remap-path-prefix=/tmp/lattice-pdf-transform-v1.lock/staging=/build",
        ],
        "getrandom_backend": "unsupported",
        "canonical_staging_path": "/tmp/lattice-pdf-transform-v1.lock/staging",
        "max_memory_bytes": int("$MAX_MEMORY_BYTES"),
        "panic": "abort",
        "rustc": "rustc 1.93.0 (254b59607 2026-01-19)",
        "target": "wasm32-unknown-unknown",
        "wasm_tools": "wasm-tools $WASM_TOOLS_VERSION",
    },
    "inputs": {
        "build_script_sha256": "$BUILD_SCRIPT_SHA256",
        "cargo_lock_sha256": "$LOCK_SHA256",
        "guest_manifest_sha256": "$MANIFEST_INPUT_SHA256",
        "guest_source_sha256": "$SOURCE_SHA256",
        "inspector_sha256": "$INSPECTOR_SHA256",
        "patch_sha256": "$PATCH_SHA256",
        "provenance_notice_sha256": "$NOTICE_SHA256",
        "rust_toolchain_sha256": "$TOOLCHAIN_SHA256",
    },
    "module": {
        "normalized_custom_sections_removed": ["name", "producers"],
        "sha256": "$MODULE_SHA256",
        "size_bytes": int("$MODULE_SIZE"),
    },
    "source": {
        "commit": "$SOURCE_COMMIT",
        "repository": "$SOURCE_URL",
        "tag": "$SOURCE_TAG",
    },
    "transform_id": "lattice.pdf.extract_text.v1",
}
with open(sys.argv[1], "w", encoding="utf-8") as output:
    json.dump(manifest, output, indent=2, sort_keys=True)
    output.write("\n")
PY

case "$MODE" in
    --write)
        cp "$MODULE" "$HERE/pdf_extract.wasm"
        cp "$RESULT_ONE/manifest.json" "$HERE/pdf_extract.manifest.json"
        ;;
    --verify)
        cmp "$MODULE" "$HERE/pdf_extract.wasm"
        cmp "$RESULT_ONE/manifest.json" "$HERE/pdf_extract.manifest.json"
        ;;
esac

printf 'verified %s bytes sha256=%s\n' "$MODULE_SIZE" "$MODULE_SHA256"
