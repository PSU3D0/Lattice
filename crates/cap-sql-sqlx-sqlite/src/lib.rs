#[cfg(target_arch = "wasm32")]
compile_error!("cap-sql-sqlx-sqlite is native-only; expose SQL to wasm through host capability transport instead");

#[cfg(not(target_arch = "wasm32"))]
mod native;

#[cfg(not(target_arch = "wasm32"))]
pub use native::*;
