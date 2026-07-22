use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Severity {
    Error,
    Fatal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(non_camel_case_types)]
pub enum BrokerErrorClass {
    invalid_encoding,
    unsupported_schema,
    unknown_critical_field,
    unknown_vocabulary,
    host_unauthenticated,
    proof_of_possession_failed,
    grant_not_found,
    grant_not_yet_valid,
    grant_expired,
    grant_revoked,
    scope_mismatch,
    contract_untrusted,
    binding_invalid,
    logical_budget_exhausted,
    dispatch_budget_exhausted,
    replay_conflict,
    reservation_conflict,
    reservation_expired,
    planning_failed,
    origin_denied,
    dispatch_failed,
    outcome_ambiguous,
    response_invalid,
    unsafe_retry_blocked,
    internal_unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BrokerError {
    #[error("BRK001 invalid encoding")]
    Brk001,
    #[error("BRK002 unsupported schema")]
    Brk002,
    #[error("BRK003 unknown critical field")]
    Brk003,
    #[error("BRK004 unknown vocabulary")]
    Brk004,
    #[error("BRK101 host unauthenticated")]
    Brk101,
    #[error("BRK102 proof of possession failed")]
    Brk102,
    #[error("BRK103 grant not found")]
    Brk103,
    #[error("BRK104 grant not yet valid")]
    Brk104,
    #[error("BRK105 grant expired")]
    Brk105,
    #[error("BRK106 grant revoked")]
    Brk106,
    #[error("BRK107 scope mismatch")]
    Brk107,
    #[error("BRK108 contract untrusted")]
    Brk108,
    #[error("BRK109 binding invalid")]
    Brk109,
    #[error("BRK201 logical budget exhausted")]
    Brk201,
    #[error("BRK202 dispatch budget exhausted")]
    Brk202,
    #[error("BRK203 replay conflict")]
    Brk203,
    #[error("BRK204 reservation conflict")]
    Brk204,
    #[error("BRK205 reservation expired")]
    Brk205,
    #[error("BRK301 planning failed")]
    Brk301,
    #[error("BRK302 origin denied")]
    Brk302,
    #[error("BRK303 dispatch failed")]
    Brk303,
    #[error("BRK304 outcome ambiguous")]
    Brk304,
    #[error("BRK305 response invalid")]
    Brk305,
    #[error("BRK306 unsafe retry blocked")]
    Brk306,
    #[error("BRK401 internal unavailable")]
    Brk401,
}

impl BrokerError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Brk001 => "BRK001",
            Self::Brk002 => "BRK002",
            Self::Brk003 => "BRK003",
            Self::Brk004 => "BRK004",
            Self::Brk101 => "BRK101",
            Self::Brk102 => "BRK102",
            Self::Brk103 => "BRK103",
            Self::Brk104 => "BRK104",
            Self::Brk105 => "BRK105",
            Self::Brk106 => "BRK106",
            Self::Brk107 => "BRK107",
            Self::Brk108 => "BRK108",
            Self::Brk109 => "BRK109",
            Self::Brk201 => "BRK201",
            Self::Brk202 => "BRK202",
            Self::Brk203 => "BRK203",
            Self::Brk204 => "BRK204",
            Self::Brk205 => "BRK205",
            Self::Brk301 => "BRK301",
            Self::Brk302 => "BRK302",
            Self::Brk303 => "BRK303",
            Self::Brk304 => "BRK304",
            Self::Brk305 => "BRK305",
            Self::Brk306 => "BRK306",
            Self::Brk401 => "BRK401",
        }
    }

    pub const fn severity(self) -> Severity {
        match self {
            Self::Brk203 | Self::Brk302 => Severity::Fatal,
            _ => Severity::Error,
        }
    }

    pub const fn class(self) -> BrokerErrorClass {
        use BrokerErrorClass::*;
        match self {
            Self::Brk001 => invalid_encoding,
            Self::Brk002 => unsupported_schema,
            Self::Brk003 => unknown_critical_field,
            Self::Brk004 => unknown_vocabulary,
            Self::Brk101 => host_unauthenticated,
            Self::Brk102 => proof_of_possession_failed,
            Self::Brk103 => grant_not_found,
            Self::Brk104 => grant_not_yet_valid,
            Self::Brk105 => grant_expired,
            Self::Brk106 => grant_revoked,
            Self::Brk107 => scope_mismatch,
            Self::Brk108 => contract_untrusted,
            Self::Brk109 => binding_invalid,
            Self::Brk201 => logical_budget_exhausted,
            Self::Brk202 => dispatch_budget_exhausted,
            Self::Brk203 => replay_conflict,
            Self::Brk204 => reservation_conflict,
            Self::Brk205 => reservation_expired,
            Self::Brk301 => planning_failed,
            Self::Brk302 => origin_denied,
            Self::Brk303 => dispatch_failed,
            Self::Brk304 => outcome_ambiguous,
            Self::Brk305 => response_invalid,
            Self::Brk306 => unsafe_retry_blocked,
            Self::Brk401 => internal_unavailable,
        }
    }

    pub fn sanitize<T, E>(_error: E) -> Result<T, Self> {
        Err(Self::Brk401)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialDiagnostic {
    Cred001,
    Cred002,
    Cred003,
    Cred004,
    Cred005,
    Cred006,
    Cred007,
    Cred008,
    Cred009,
    Cred010,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialBoundaryFailure {
    UnknownVocabulary,
    InvalidAuthority,
    RevokedOrEpochMismatch,
    CustodyUnavailable,
    EndpointViolation,
}

impl CredentialBoundaryFailure {
    pub const fn broker_error(self) -> BrokerError {
        match self {
            Self::UnknownVocabulary => BrokerError::Brk004,
            Self::InvalidAuthority => BrokerError::Brk109,
            Self::RevokedOrEpochMismatch => BrokerError::Brk106,
            Self::CustodyUnavailable => BrokerError::Brk401,
            Self::EndpointViolation => BrokerError::Brk302,
        }
    }
}

pub struct PublicError(pub BrokerError);
impl fmt::Display for PublicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
