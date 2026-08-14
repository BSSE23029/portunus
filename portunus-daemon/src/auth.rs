//! Validated control-plane authentication policy and request hook.
//!
//! Authentication is explicitly disabled or uses one prevalidated bearer value no
//! longer than 4,096 bytes. Debug output never contains credentials, and rejection
//! responses do not distinguish missing, malformed, or incorrect values.
//!
//! This module validates request metadata only. It does not provision secrets,
//! authorize individual methods, log credentials, terminate TLS, or manage identity.

use std::fmt;
use thiserror::Error;
use tonic::{service::Interceptor, Code, Request, Status};

const MAX_BEARER_BYTES: usize = 4_096;

#[derive(Clone, Eq, PartialEq)]
pub struct AuthConfig {
    expected: Option<String>,
}

impl AuthConfig {
    /// Inputs: no credential material.
    /// Outputs: explicit pass-through policy for trusted/local deployments.
    /// Logic: represent disabled authentication without an empty-token sentinel.
    #[must_use]
    pub const fn disabled() -> Self {
        Self { expected: None }
    }

    /// Inputs: optional borrowed token from an already-selected configuration source.
    /// Outputs: disabled policy for absence or validated bearer policy for presence.
    /// Logic: keep environment access outside this pure configuration seam.
    /// # Errors
    /// Returns the same validation errors as [`Self::bearer`].
    pub fn from_source(token: Option<&str>) -> Result<Self, AuthError> {
        token.map_or_else(|| Ok(Self::disabled()), Self::bearer)
    }

    /// Inputs: bearer token containing between 1 and 4,089 visible bytes.
    /// Outputs: validated policy retaining the complete expected metadata value.
    /// Logic: bound memory, reject control/whitespace ambiguity, and prefix once.
    /// # Errors
    /// Returns stable empty, oversized, or invalid-character configuration errors.
    pub fn bearer(token: &str) -> Result<Self, AuthError> {
        if token.is_empty() {
            return Err(AuthError::EmptyBearerToken);
        }
        if token.len() + "Bearer ".len() > MAX_BEARER_BYTES {
            return Err(AuthError::BearerTokenTooLong {
                limit: MAX_BEARER_BYTES - "Bearer ".len(),
            });
        }
        if !token.bytes().all(|byte| byte.is_ascii_graphic()) {
            return Err(AuthError::InvalidBearerToken);
        }
        Ok(Self {
            expected: Some(format!("Bearer {token}")),
        })
    }
}

impl fmt::Debug for AuthConfig {
    // Inputs: formatter and authentication policy.
    // Outputs: mode-only diagnostics with all credential bytes redacted.
    // Logic: make routine debug derivations safe at the process boundary.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthConfig")
            .field("enabled", &self.expected.is_some())
            .finish_non_exhaustive()
    }
}

/// Inputs: validated authentication policy and owned gRPC request of any body type.
///
/// Outputs: unchanged request or credential-independent unauthenticated status.
/// Logic: bypass only explicit disabled policy; otherwise compare the bounded header
/// in constant work for its configured length without exposing either credential.
/// # Errors
/// Returns `Unauthenticated` for absent, malformed, or incorrect metadata.
pub fn authorize<T>(config: &AuthConfig, request: Request<T>) -> Result<Request<T>, AuthFailure> {
    let Some(expected) = &config.expected else {
        return Ok(request);
    };
    let supplied = request
        .metadata()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if constant_time_eq(expected.as_bytes(), supplied.as_bytes()) {
        Ok(request)
    } else {
        Err(AuthFailure)
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("authentication required")]
pub struct AuthFailure;

impl AuthFailure {
    /// Inputs: rejected authentication result.
    /// Outputs: stable gRPC unauthenticated status code.
    /// Logic: expose transport classification without allocating a full status.
    #[must_use]
    pub const fn code(self) -> Code {
        Code::Unauthenticated
    }
}

impl From<AuthFailure> for Status {
    // Inputs: credential-independent authentication failure.
    // Outputs: gRPC unauthenticated status without sensitive details.
    // Logic: allocate transport status only at the server interceptor boundary.
    fn from(_: AuthFailure) -> Self {
        Self::unauthenticated("authentication required")
    }
}

#[derive(Clone, Debug)]
pub struct AuthInterceptor {
    config: AuthConfig,
}

impl AuthInterceptor {
    /// Inputs: validated daemon authentication policy.
    /// Outputs: cloneable gRPC interceptor owning no external secret source.
    /// Logic: retain redacted configuration for per-request metadata validation.
    #[must_use]
    pub const fn new(config: AuthConfig) -> Self {
        Self { config }
    }
}

impl Interceptor for AuthInterceptor {
    // Inputs: metadata-only gRPC request before message decoding/dispatch.
    // Outputs: authorized request or tonic-mandated unauthenticated status.
    // Logic: adapt the small internal failure into the trait's required status type.
    #[allow(clippy::result_large_err)]
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        authorize(&self.config, request).map_err(Into::into)
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum AuthError {
    #[error("bearer token cannot be empty")]
    EmptyBearerToken,
    #[error("bearer token exceeds {limit} bytes")]
    BearerTokenTooLong { limit: usize },
    #[error("bearer token must contain visible ASCII bytes only")]
    InvalidBearerToken,
}

// Inputs: expected and supplied credential byte strings.
// Outputs: equality without early return on byte differences.
// Logic: fold length and every paired/padded byte difference into one accumulator.
fn constant_time_eq(expected: &[u8], supplied: &[u8]) -> bool {
    let mut difference = expected.len() ^ supplied.len();
    for (index, expected_byte) in expected.iter().enumerate() {
        difference |= usize::from(*expected_byte ^ supplied.get(index).copied().unwrap_or(0));
    }
    difference == 0
}
