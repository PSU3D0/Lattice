use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationCode {
    DuplicateGeneratedModuleName,
    DuplicateSurfaceIdentifier,
    InvalidInputFieldReference,
    InvalidJsonEscapeHatch,
    InvalidPathTemplate,
    InvalidResourceContract,
    InvalidTypeReference,
    UnknownEndpointProfile,
    UnknownOutboundAuthProfile,
    UnsupportedOutboundAuthKind,
    UnsupportedPaginatedOutputShape,
    UnsupportedSurfaceKind,
}

impl ValidationCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            ValidationCode::DuplicateGeneratedModuleName => {
                "connector::duplicate_generated_module_name"
            }
            ValidationCode::DuplicateSurfaceIdentifier => "connector::duplicate_surface_identifier",
            ValidationCode::InvalidInputFieldReference => {
                "connector::invalid_input_field_reference"
            }
            ValidationCode::InvalidJsonEscapeHatch => "connector::invalid_json_escape_hatch",
            ValidationCode::InvalidPathTemplate => "connector::invalid_path_template",
            ValidationCode::InvalidResourceContract => "connector::invalid_resource_contract",
            ValidationCode::InvalidTypeReference => "connector::invalid_type_reference",
            ValidationCode::UnknownEndpointProfile => "connector::unknown_endpoint_profile",
            ValidationCode::UnknownOutboundAuthProfile => {
                "connector::unknown_outbound_auth_profile"
            }
            ValidationCode::UnsupportedOutboundAuthKind => {
                "connector::unsupported_outbound_auth_kind"
            }
            ValidationCode::UnsupportedPaginatedOutputShape => {
                "connector::unsupported_paginated_output_shape"
            }
            ValidationCode::UnsupportedSurfaceKind => "connector::unsupported_surface_kind",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationError {
    pub code: ValidationCode,
    pub path: Option<String>,
    pub message: String,
}

impl ValidationError {
    pub fn new(
        code: ValidationCode,
        path: impl Into<Option<String>>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code,
            path: path.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.path {
            Some(path) => write!(f, "{} at {}: {}", self.code.as_str(), path, self.message),
            None => write!(f, "{}: {}", self.code.as_str(), self.message),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ValidationErrors {
    errors: Vec<ValidationError>,
}

impl ValidationErrors {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, error: ValidationError) {
        self.errors.push(error);
    }

    pub fn extend<I>(&mut self, errors: I)
    where
        I: IntoIterator<Item = ValidationError>,
    {
        self.errors.extend(errors);
    }

    pub fn is_empty(&self) -> bool {
        self.errors.is_empty()
    }

    pub fn len(&self) -> usize {
        self.errors.len()
    }

    pub fn into_vec(self) -> Vec<ValidationError> {
        self.errors
    }

    pub fn as_slice(&self) -> &[ValidationError] {
        &self.errors
    }
}

impl IntoIterator for ValidationErrors {
    type Item = ValidationError;
    type IntoIter = std::vec::IntoIter<ValidationError>;

    fn into_iter(self) -> Self::IntoIter {
        self.errors.into_iter()
    }
}

impl fmt::Display for ValidationErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, error) in self.errors.iter().enumerate() {
            if index > 0 {
                writeln!(f)?;
            }
            write!(f, "{error}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ValidationErrors {}
