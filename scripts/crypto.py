#!/usr/bin/env python3
"""Lattice doc encryption utility.

Uses age encryption for sharing private docs in-repo without committing plaintext.

Model:
- Decrypted (local-only, gitignored): `private/impl-docs/...`
- Encrypted (committed): `impl-docs/_encrypted/.../*.age`

Keys:
- Recipients: `.age-recipients`
- Secret key material is sourced from fnox (`fnox export` -> AGE_SECRET_KEY).

Usage:
  python scripts/crypto.py encrypt [--dir <name>|all] [--force]
  python scripts/crypto.py decrypt [--dir <name>|all]
  python scripts/crypto.py edit <encrypted-file.age>
  python scripts/crypto.py status
"""

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parent.parent
RECIPIENTS_FILE = ROOT / ".age-recipients"

# Directory mappings are intentionally explicit.
# Each mapping preserves relative paths from source -> encrypted.
DIRECTORIES = {
    "impl_docs_private": {
        "source": ROOT / "private" / "impl-docs",
        "encrypted": ROOT / "impl-docs" / "_encrypted",
        "pattern": "**/*.md",
    },
}


def tool_cmd(tool: str) -> list[str]:
    """Return command prefix for tool, preferring PATH then mise."""

    if shutil.which(tool):
        return [tool]

    if shutil.which("mise"):
        # Works even when mise shims are not activated in the shell.
        return ["mise", "exec", "--", tool]

    raise RuntimeError(f"{tool} not found on PATH (install it or use mise)")


def get_age_key() -> str:
    env_key = os.environ.get("AGE_SECRET_KEY")
    if env_key:
        return env_key.strip()

    try:
        result = subprocess.run(
            tool_cmd("fnox") + ["export"],
            capture_output=True,
            text=True,
            check=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError) as e:
        raise RuntimeError(
            "Unable to fetch AGE_SECRET_KEY (set env AGE_SECRET_KEY or install/configure fnox)"
        ) from e
    for line in result.stdout.strip().split("\n"):
        if line.startswith("export AGE_SECRET_KEY="):
            return line.split("=", 1)[1].strip("'\"")

    raise RuntimeError("AGE_SECRET_KEY not found (set env or use fnox export)")


def iter_source_files(source_dir: Path, pattern: str) -> Iterable[Path]:
    # Path.glob supports recursive patterns via "**".
    for path in source_dir.glob(pattern):
        if not path.is_file():
            continue
        if path.name.startswith("."):
            continue
        yield path


def iter_encrypted_files(encrypted_dir: Path) -> Iterable[Path]:
    for path in encrypted_dir.glob("**/*.age"):
        if not path.is_file():
            continue
        if path.name.startswith("."):
            continue
        yield path


def load_recipients(path: Path) -> set[str]:
    recipients: set[str] = set()
    if not path.exists():
        return recipients

    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        recipients.add(line)

    return recipients


def decrypt_bytes(enc_path: Path, keyfile: str) -> bytes | None:
    try:
        result = subprocess.run(
            tool_cmd("age") + ["-d", "-i", keyfile, str(enc_path)],
            check=True,
            capture_output=True,
        )
        return result.stdout
    except subprocess.CalledProcessError:
        return None


def encrypt_file(source: Path, dest: Path) -> bool:
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        subprocess.run(
            tool_cmd("age")
            + ["-e", "-R", str(RECIPIENTS_FILE), "-o", str(dest), str(source)],
            check=True,
            capture_output=True,
        )
        return True
    except subprocess.CalledProcessError as e:
        stderr = e.stderr.decode(errors="replace") if e.stderr else str(e)
        print(f"  ERROR: {stderr}", file=sys.stderr)
        return False


def decrypt_file(source: Path, dest: Path, key: str) -> bool:
    dest.parent.mkdir(parents=True, exist_ok=True)
    keyfile = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".key", delete=False) as kf:
            kf.write(key)
            keyfile = kf.name
        subprocess.run(
            tool_cmd("age") + ["-d", "-i", keyfile, "-o", str(dest), str(source)],
            check=True,
            capture_output=True,
        )
        return True
    except subprocess.CalledProcessError as e:
        stderr = e.stderr.decode(errors="replace") if e.stderr else str(e)
        print(f"  ERROR: {stderr}", file=sys.stderr)
        return False
    finally:
        if keyfile is not None:
            os.unlink(keyfile)


def encrypt_dir(
    _name: str,
    config: dict,
    keyfile: str | None,
    force: bool,
) -> tuple[int, int, int]:
    source_dir: Path = config["source"]
    encrypted_dir: Path = config["encrypted"]
    pattern: str = config["pattern"]

    if not source_dir.exists():
        print(f"  Source directory does not exist: {source_dir}")
        return 0, 0, 0

    if not RECIPIENTS_FILE.exists():
        print(f"  Missing recipients file: {RECIPIENTS_FILE}", file=sys.stderr)
        return 0, 0, 1

    encrypted, skipped, failed = 0, 0, 0

    for source_file in iter_source_files(source_dir, pattern):
        rel = source_file.relative_to(source_dir)
        dest = (encrypted_dir / rel).with_name((encrypted_dir / rel).name + ".age")
        dest_rel = dest.relative_to(encrypted_dir)

        if dest.exists() and not force:
            if keyfile is not None:
                existing = decrypt_bytes(dest, keyfile)
                if existing is not None:
                    if existing == source_file.read_bytes():
                        print(f"  {rel} -> {dest_rel} (unchanged)")
                        skipped += 1
                        continue
                else:
                    print(f"  {rel} -> {dest_rel} (decrypt failed; rewriting)")
            else:
                # Without a key we can't compare plaintext.
                print(f"  {rel} -> {dest_rel} (no key; rewriting)")
        else:
            if dest.exists():
                print(f"  {rel} -> {dest_rel} (force)")
            else:
                print(f"  {rel} -> {dest_rel}")

        if encrypt_file(source_file, dest):
            encrypted += 1
        else:
            failed += 1

    return encrypted, skipped, failed


def decrypt_dir(_name: str, config: dict, key: str) -> tuple[int, int]:
    source_dir: Path = config["source"]
    encrypted_dir: Path = config["encrypted"]

    if not encrypted_dir.exists():
        print(f"  Encrypted directory does not exist: {encrypted_dir}")
        return 0, 0

    success, failed = 0, 0
    for enc_file in iter_encrypted_files(encrypted_dir):
        rel = enc_file.relative_to(encrypted_dir)
        dest_rel = rel.with_suffix("")
        dest = source_dir / dest_rel
        print(f"  {rel} -> {dest_rel}")
        if decrypt_file(enc_file, dest, key):
            success += 1
        else:
            failed += 1

    return success, failed


def cmd_encrypt(args):
    dirs_to_process = list(DIRECTORIES.keys()) if args.dir == "all" else [args.dir]

    expected_recipients = load_recipients(RECIPIENTS_FILE)
    if not expected_recipients:
        print(f"Missing or empty recipients file: {RECIPIENTS_FILE}", file=sys.stderr)
        return 1

    # Best-effort key lookup: encryption itself does not require the private key, but
    # avoiding ciphertext churn does (we decrypt existing .age files to compare bytes).
    keyfile = None
    try:
        try:
            key = get_age_key()
            with tempfile.NamedTemporaryFile(mode="w", suffix=".key", delete=False) as kf:
                kf.write(key)
                keyfile = kf.name
        except RuntimeError as e:
            print(f"WARN: {e}. Will rewrite ciphertext even if unchanged.")
            keyfile = None

        total_encrypted, total_skipped, total_failed = 0, 0, 0

        for name in dirs_to_process:
            if name not in DIRECTORIES:
                print(f"Unknown directory: {name}", file=sys.stderr)
                total_failed += 1
                continue

            print(f"\nEncrypting [{name}]:")
            e, s, f = encrypt_dir(name, DIRECTORIES[name], keyfile, args.force)
            total_encrypted += e
            total_skipped += s
            total_failed += f

        print(
            f"\nDone: {total_encrypted} encrypted, {total_skipped} unchanged, {total_failed} failed"
        )
        return 0 if total_failed == 0 else 1

    finally:
        if keyfile is not None:
            os.unlink(keyfile)


def cmd_decrypt(args):
    key = get_age_key()
    dirs_to_process = list(DIRECTORIES.keys()) if args.dir == "all" else [args.dir]
    total_success, total_failed = 0, 0

    for name in dirs_to_process:
        if name not in DIRECTORIES:
            print(f"Unknown directory: {name}", file=sys.stderr)
            total_failed += 1
            continue
        print(f"\nDecrypting [{name}]:")
        s, f = decrypt_dir(name, DIRECTORIES[name], key)
        total_success += s
        total_failed += f

    print(f"\nDone: {total_success} decrypted, {total_failed} failed")
    return 0 if total_failed == 0 else 1


def cmd_edit(args):
    key = get_age_key()
    enc_path = Path(args.file)

    if not enc_path.exists():
        print(f"File not found: {enc_path}", file=sys.stderr)
        return 1

    with tempfile.NamedTemporaryFile(mode="w", suffix=".md", delete=False) as tf:
        tmp_path = Path(tf.name)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".key", delete=False) as kf:
        kf.write(key)
        keyfile = kf.name

    try:
        subprocess.run(
            tool_cmd("age") + ["-d", "-i", keyfile, "-o", str(tmp_path), str(enc_path)],
            check=True,
        )

        original = tmp_path.read_bytes()

        editor = os.environ.get("EDITOR", "vim")
        subprocess.run([editor, str(tmp_path)], check=True)

        updated = tmp_path.read_bytes()
        if updated == original:
            print(f"No changes: {enc_path}")
            return 0

        subprocess.run(
            tool_cmd("age")
            + ["-e", "-R", str(RECIPIENTS_FILE), "-o", str(enc_path), str(tmp_path)],
            check=True,
        )
        print(f"Re-encrypted: {enc_path}")
        return 0

    except subprocess.CalledProcessError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    finally:
        tmp_path.unlink(missing_ok=True)
        os.unlink(keyfile)


def cmd_status(_args):
    print("Encryption Status\n" + "=" * 40)

    for name, config in DIRECTORIES.items():
        source_dir: Path = config["source"]
        encrypted_dir: Path = config["encrypted"]
        pattern: str = config["pattern"]

        source_files = (
            {str(p.relative_to(source_dir)) for p in iter_source_files(source_dir, pattern)}
            if source_dir.exists()
            else set()
        )
        encrypted_files = (
            {str(p.relative_to(encrypted_dir).with_suffix("")) for p in iter_encrypted_files(encrypted_dir)}
            if encrypted_dir.exists()
            else set()
        )

        only_source = source_files - encrypted_files
        only_encrypted = encrypted_files - source_files
        both = source_files & encrypted_files

        print(f"\n[{name}]")
        print(f"  Source: {source_dir}")
        print(f"  Encrypted: {encrypted_dir}")
        print(f"  Files in both: {len(both)}")

        if only_source:
            print(f"  WARN: unencrypted ({len(only_source)}):")
            for f in sorted(only_source)[:10]:
                print(f"    - {f}")
            if len(only_source) > 10:
                print(f"    ... and {len(only_source) - 10} more")

        if only_encrypted:
            print(f"  WARN: not decrypted ({len(only_encrypted)}):")
            for f in sorted(only_encrypted)[:10]:
                print(f"    - {f}")
            if len(only_encrypted) > 10:
                print(f"    ... and {len(only_encrypted) - 10} more")

    return 0


def cmd_add_dir(args):
    name = args.name
    source = args.source

    print(f"To add directory '{name}':")
    print(f"  1. Create source: {source}")
    print("  2. Add to DIRECTORIES in scripts/crypto.py:")
    print(f'     "{name}": {{')
    print(f'         "source": ROOT / "{source}",')
    print('         "encrypted": ROOT / "impl-docs" / "_encrypted" / "<path>",')
    print('         "pattern": "**/*.md",')
    print("     },")
    print(f"  3. Add {source}/ to .gitignore")
    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Lattice document encryption utility",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_encrypt = subparsers.add_parser("encrypt", help="Encrypt source files")
    p_encrypt.add_argument(
        "--dir",
        "-d",
        choices=list(DIRECTORIES.keys()) + ["all"],
        default="all",
        help="Directory mapping to encrypt (default: all)",
    )
    p_encrypt.add_argument(
        "--force",
        action="store_true",
        help="Force re-encrypt even if plaintext unchanged",
    )
    p_encrypt.set_defaults(func=cmd_encrypt)

    p_decrypt = subparsers.add_parser("decrypt", help="Decrypt to source directories")
    p_decrypt.add_argument(
        "--dir",
        "-d",
        choices=list(DIRECTORIES.keys()) + ["all"],
        default="all",
        help="Directory mapping to decrypt (default: all)",
    )
    p_decrypt.set_defaults(func=cmd_decrypt)

    p_edit = subparsers.add_parser("edit", help="Decrypt, edit, re-encrypt a file")
    p_edit.add_argument("file", help="Encrypted .age file to edit")
    p_edit.set_defaults(func=cmd_edit)

    p_status = subparsers.add_parser("status", help="Show encryption status")
    p_status.set_defaults(func=cmd_status)

    p_add = subparsers.add_parser("add-dir", help="Show instructions for adding a new directory")
    p_add.add_argument("name", help="Short name for the directory (e.g., 'team')")
    p_add.add_argument("source", help="Source path relative to repo root (e.g., 'private/team')")
    p_add.set_defaults(func=cmd_add_dir)

    args = parser.parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
