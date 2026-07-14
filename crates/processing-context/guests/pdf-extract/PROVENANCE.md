# PDF transform provenance and license notice

The checked module is built only by `build.sh` from:

- upstream: https://github.com/jrmuizel/pdf-extract
- tag: `v0.12.0`
- peeled commit: `b95bf9f6268772d5088f09b0034e488e64294835`
- local change: `pdf-extract-no-wasm-js.patch`, which removes only lopdf's `wasm_js` feature
- local guest ABI source: `src/lib.rs`

No upstream source tree is copied into this repository. The build atomically creates the fixed private lock directory `/tmp/lattice-pdf-transform-v1.lock` with mode 0700, fails if any entry already exists there, verifies directory type, owner, and mode, and stages below it at the hard canonical path `/tmp/lattice-pdf-transform-v1.lock/staging`. It verifies the exact commit and original `Cargo.toml` hash, applies the checked patch, and builds with the checked `Cargo.wasm.lock` in an isolated `CARGO_HOME` and target directory. The separate `Cargo.lock` resolves the unpatched crates.io graph only for direct native unit tests; it is not an input to the wasm module. Cargo path identities affect Rust/LLVM function ordering, so reproducibility uses two sequential clean builds at that identical canonical staging path, retains `--remap-path-prefix`, keeps results in separate directories below the private lock, and compares them. The normal signal/exit trap removes the owned lock directory; no symlink-following lock file or `flock` is used.

The upstream `pdf-extract` `Cargo.toml` declares `license = "MIT"` and identifies Jeff Muizelaar as its author. That revision does not contain a `LICENSE` file and does not attach separate notices to its generated glyph-name/core-font tables. This notice records those facts rather than claiming more specific provenance for those tables.

The standard MIT license text associated with the upstream package's declared license is:

> Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
>
> The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
>
> THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

## Locked dependency license inventory

This inventory is derived from the package `license` fields for the complete checked `Cargo.lock`. Slash spellings are preserved where registries publish non-SPDX expressions.

```text
adler2 2.0.1: 0BSD OR MIT OR Apache-2.0
adobe-cmap-parser 0.4.1: MIT
aes 0.8.4: MIT OR Apache-2.0
autocfg 1.5.1: Apache-2.0 OR MIT
bitflags 2.13.0: MIT OR Apache-2.0
block-buffer 0.10.4: MIT OR Apache-2.0
block-padding 0.3.3: MIT OR Apache-2.0
cbc 0.1.2: MIT OR Apache-2.0
cff-parser 0.2.0: MIT OR Apache-2.0
cfg-if 1.0.4: MIT OR Apache-2.0
chacha20 0.10.1: MIT OR Apache-2.0
cipher 0.4.4: MIT OR Apache-2.0
cpufeatures 0.2.17: MIT OR Apache-2.0
cpufeatures 0.3.0: MIT OR Apache-2.0
crc32fast 1.5.0: MIT OR Apache-2.0
crypto-common 0.1.7: MIT OR Apache-2.0
digest 0.10.7: MIT OR Apache-2.0
ecb 0.1.2: MIT
encoding_rs 0.8.35: (Apache-2.0 OR MIT) AND BSD-3-Clause
equivalent 1.0.2: Apache-2.0 OR MIT
euclid 0.20.14: MIT / Apache-2.0
flate2 1.1.9: MIT OR Apache-2.0
generic-array 0.14.7: MIT
getrandom 0.4.3: MIT OR Apache-2.0
hashbrown 0.17.1: MIT OR Apache-2.0
indexmap 2.14.0: Apache-2.0 OR MIT
inout 0.1.4: MIT OR Apache-2.0
itoa 1.0.18: MIT OR Apache-2.0
libc 0.2.186: MIT OR Apache-2.0
log 0.4.33: MIT OR Apache-2.0
lopdf 0.42.0: MIT
md-5 0.10.6: MIT OR Apache-2.0
memchr 2.8.3: Unlicense OR MIT
miniz_oxide 0.8.9: MIT OR Zlib OR Apache-2.0
nom 8.0.0: MIT
num-traits 0.2.19: MIT OR Apache-2.0
pdf-extract 0.12.0: MIT
pom 1.1.0: MIT
postscript 0.14.1: Apache-2.0/MIT
proc-macro2 1.0.106: MIT OR Apache-2.0
quote 1.0.46: MIT OR Apache-2.0
r-efi 6.0.0: MIT OR Apache-2.0 OR LGPL-2.1-or-later
rand 0.10.2: MIT OR Apache-2.0
rand_core 0.10.1: MIT OR Apache-2.0
rangemap 1.7.1: MIT/Apache-2.0
sha2 0.10.9: MIT OR Apache-2.0
simd-adler32 0.3.10: MIT
stringprep 0.1.5: MIT/Apache-2.0
syn 2.0.118: MIT OR Apache-2.0
thiserror 2.0.18: MIT OR Apache-2.0
thiserror-impl 2.0.18: MIT OR Apache-2.0
tinyvec 1.12.0: Zlib OR Apache-2.0 OR MIT
tinyvec_macros 0.1.1: MIT OR Apache-2.0 OR Zlib
ttf-parser 0.25.1: MIT OR Apache-2.0
type1-encoding-parser 0.1.1: MIT
typenum 1.20.1: MIT OR Apache-2.0
unicode-bidi 0.3.18: MIT OR Apache-2.0
unicode-ident 1.0.24: (MIT OR Apache-2.0) AND Unicode-3.0
unicode-normalization 0.1.25: MIT OR Apache-2.0
unicode-properties 0.1.4: MIT/Apache-2.0
version_check 0.9.5: MIT/Apache-2.0
weezl 0.1.12: MIT OR Apache-2.0
```

The guest package itself is licensed under the repository's `MIT OR Apache-2.0` terms. Package license files remain authoritative; this inventory is a provenance aid, not legal advice.
