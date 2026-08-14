//! Process-wide structured logging policy for the reference daemon.
//!
//! Portunus libraries emit structured events through `tracing`; they never
//! install a subscriber because the embedding application owns output format,
//! filtering, and destination. This composition-root module validates operator
//! configuration and installs exactly one global subscriber before tasks start.
//!
//! Filter precedence is deterministic:
//!
//! ```text
//! PORTUNUS_LOG ──present/non-empty──> selected filter
//!      │ absent
//!      ▼
//!   RUST_LOG ────present/non-empty──> selected filter
//!      │ absent
//!      ▼
//!     info
//! ```
//!
//! Filters use `tracing_subscriber::EnvFilter` syntax, so both `debug` and
//! targeted directives such as `portunus_engine=trace,tonic=warn` work. This
//! module does not choose log sinks, rotate files, or initialize telemetry in
//! reusable crates; those remain deployment and composition-root concerns.

use thiserror::Error;
use tracing_subscriber::EnvFilter;

/// Validated process-wide logging configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoggingConfig {
    filter: String,
}

impl LoggingConfig {
    /// Validates one complete filter directive.
    ///
    /// **Inputs:** A UTF-8 filter using `EnvFilter` directive syntax.
    ///
    /// **Outputs:** A configuration preserving the directive, or a typed error
    /// containing the rejected input and parser reason.
    ///
    /// **Logic:** Validate at configuration time so startup never silently
    /// disables diagnostics because of an operator typo.
    ///
    /// # Errors
    ///
    /// Returns [`LoggingError::InvalidFilter`] when `filter` is not valid
    /// `EnvFilter` syntax.
    pub fn new(filter: impl Into<String>) -> Result<Self, LoggingError> {
        let filter = filter.into();
        EnvFilter::try_new(&filter).map_err(|error| LoggingError::InvalidFilter {
            filter: filter.clone(),
            reason: error.to_string(),
        })?;
        Ok(Self { filter })
    }

    /// Resolves filter sources without reading ambient process state.
    ///
    /// **Inputs:** Optional primary `PORTUNUS_LOG` and fallback `RUST_LOG`
    /// values; empty or whitespace-only values count as absent.
    ///
    /// **Outputs:** A validated configuration using primary, fallback, then
    /// `info` precedence, or an invalid-filter error.
    ///
    /// **Logic:** Keep precedence pure and directly testable, then let
    /// [`Self::from_env`] provide the environment adapter.
    ///
    /// # Errors
    ///
    /// Returns [`LoggingError::InvalidFilter`] when the highest-precedence
    /// non-empty source is not a valid filter directive.
    pub fn from_sources(
        portunus_log: Option<&str>,
        rust_log: Option<&str>,
    ) -> Result<Self, LoggingError> {
        let filter = [portunus_log, rust_log]
            .into_iter()
            .flatten()
            .map(str::trim)
            .find(|value| !value.is_empty())
            .unwrap_or("info");
        Self::new(filter)
    }

    /// Loads and validates the process logging policy.
    ///
    /// **Inputs:** Optional `PORTUNUS_LOG` and `RUST_LOG` environment variables.
    ///
    /// **Outputs:** The effective validated configuration or a typed filter error.
    ///
    /// **Logic:** Read environment values once during startup and delegate all
    /// precedence and validation decisions to the pure source resolver.
    ///
    /// # Errors
    ///
    /// Returns [`LoggingError::InvalidFilter`] when the selected environment
    /// value is not a valid filter directive.
    pub fn from_env() -> Result<Self, LoggingError> {
        let portunus_log = std::env::var("PORTUNUS_LOG").ok();
        let rust_log = std::env::var("RUST_LOG").ok();
        Self::from_sources(portunus_log.as_deref(), rust_log.as_deref())
    }

    /// Returns the exact effective filter directive.
    ///
    /// **Inputs:** A shared borrow of validated configuration.
    ///
    /// **Outputs:** A string slice tied to the configuration's lifetime.
    ///
    /// **Logic:** Expose effective policy for startup diagnostics and tests
    /// without exposing mutable configuration internals.
    #[must_use]
    pub fn filter(&self) -> &str {
        &self.filter
    }

    // Inputs: a shared borrow of already validated configuration.
    // Outputs: a fresh executable filter or an error if library behavior changed.
    // Logic: rebuild at subscriber installation because EnvFilter is not cloneable.
    fn env_filter(&self) -> Result<EnvFilter, LoggingError> {
        EnvFilter::try_new(&self.filter).map_err(|error| LoggingError::InvalidFilter {
            filter: self.filter.clone(),
            reason: error.to_string(),
        })
    }
}

/// Startup failures produced by logging configuration or global installation.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum LoggingError {
    #[error("invalid logging filter {filter:?}: {reason}")]
    InvalidFilter { filter: String, reason: String },
    #[error("global logging subscriber could not be installed: {reason}")]
    AlreadyInitialized { reason: String },
}

/// Installs the daemon's single process-wide structured logging subscriber.
///
/// **Inputs:** A shared validated configuration; global subscriber state must be
/// uninitialized by the caller.
///
/// **Outputs:** Unit after installation, or a typed error when another component
/// already installed a global subscriber.
///
/// **Logic:** Build the formatting subscriber with the validated filter and use
/// fallible global initialization so duplicate ownership is reported, not panicked.
///
/// # Errors
///
/// Returns [`LoggingError`] for filter reconstruction or duplicate initialization.
pub fn init_global_logging(config: &LoggingConfig) -> Result<(), LoggingError> {
    tracing_subscriber::fmt()
        .with_env_filter(config.env_filter()?)
        .try_init()
        .map_err(|error| LoggingError::AlreadyInitialized {
            reason: error.to_string(),
        })
}
