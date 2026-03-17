pub mod actions;
pub mod ext;
pub mod generated;
pub mod ops;
pub mod runtime;

pub use actions::*;
pub use generated::manifest::*;
pub use generated::profiles::*;
#[cfg(feature = "host-bundle")]
pub use generated::register::register_all;
pub use generated::types::*;

pub const CONNECTOR_FAMILY: &str = "connector.google.sheets";
pub const GOOGLE_SHEETS_APPEND_ROW_IDENTIFIER: &str = "connector.google.sheets.append_row";
pub const GOOGLE_SHEETS_FIND_ROWS_IDENTIFIER: &str = "connector.google.sheets.find_rows";
pub const GOOGLE_SHEETS_UPSERT_ROW_IDENTIFIER: &str = "connector.google.sheets.upsert_row";
