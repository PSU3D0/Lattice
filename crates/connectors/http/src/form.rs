//! The `form!` builder macro for `multipart/form-data` parts (spec §16.5).
//!
//! Ergonomic construction of the `Vec<MultipartPart>` a multipart op takes,
//! from `(field_name => value)` pairs. Each value is converted through
//! `ByteSource::from`, so a `&str`/`Vec<u8>` becomes an inline part and an
//! `Artifact` becomes an artifact-backed part (dereffed under the
//! `workspace::read` grant at send time), matching the §16.5 example:
//!
//! ```ignore
//! use connector_http::form;
//! let parts = form! { "file" => artifact, "kind" => "daily" };
//! ```

/// Build a `Vec<MultipartPart>` from `field => value` pairs. `value` is
/// anything that implements `Into<ByteSource>` (`&str`, `Vec<u8>`,
/// `Artifact`).
#[macro_export]
macro_rules! form {
    ( $( $field:expr => $source:expr ),* $(,)? ) => {
        ::std::vec![
            $(
                $crate::MultipartPart::new(
                    $field,
                    <$crate::ByteSource as ::std::convert::From<_>>::from($source),
                )
            ),*
        ]
    };
}
