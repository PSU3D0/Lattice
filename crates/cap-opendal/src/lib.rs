//! OpenDAL-backed Lattice capabilities (blob + key-value).
//!
//! This crate merges the former `cap-opendal-core`, `cap-blob-opendal`, and
//! `cap-kv-opendal` micro-crates into a single surface. The shared OpenDAL
//! plumbing (operator factories, layer helpers, error translation) lives in the
//! [`core`] module; the capability implementations live behind the `blob` and
//! `kv` feature flags (both on by default).
//!
//! Merge rationale (packet E2, verifiability-substrate-hardening-plan):
//! the three crates had no external consumers — only the workspace member list
//! referenced the blob/kv crates, and only those two consumed the core. A single
//! crate with feature-gated modules is the lowest-churn shape: it removes two
//! path-dependency hops and a duplicated 24-entry feature matrix while keeping
//! the public types (`OperatorFactory`, `OpendalBlobStore`, `OpendalKvStore`)
//! importable.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod core;

pub use core::error::{OpendalError, Result as OpendalResult};
pub use core::{OperatorFactory, OperatorLayerExt, SchemeOperatorFactory};

#[cfg(feature = "blob")]
pub mod blob;
#[cfg(feature = "blob")]
pub use blob::{BlobStoreBuilder, OpendalBlobStore};

#[cfg(feature = "kv")]
pub mod kv;
#[cfg(feature = "kv")]
pub use kv::{KvStoreBuilder, OpendalKvStore};
