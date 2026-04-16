pub mod actions;
pub mod errors;
pub mod ops;
pub mod runtime;
pub mod types;

pub use actions::*;
pub use errors::SheetPortConnectorError;
pub use types::*;

pub const CONNECTOR_FAMILY: &str = "connector.formualizer.sheetport";
pub const SHEETPORT_EVALUATE_IDENTIFIER: &str = "connector.formualizer.sheetport.evaluate";
