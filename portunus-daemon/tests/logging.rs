use portunus_daemon::logging::{init_global_logging, LoggingConfig, LoggingError};

// Inputs: primary, ecosystem-standard, empty, and absent logging filter sources.
// Outputs: validated configuration using documented deterministic precedence.
// Logic: prove deployment-specific configuration wins without ambient environment state.
#[test]
fn resolves_filter_precedence_and_default() {
    assert_eq!(
        LoggingConfig::from_sources(Some("debug"), Some("warn"))
            .unwrap()
            .filter(),
        "debug"
    );
    assert_eq!(
        LoggingConfig::from_sources(Some(""), Some("warn"))
            .unwrap()
            .filter(),
        "warn"
    );
    assert_eq!(
        LoggingConfig::from_sources(None, None).unwrap().filter(),
        "info"
    );
}

// Inputs: simple levels and a module-specific structured filter directive.
// Outputs: configurations preserving the exact validated directive.
// Logic: support both operator-friendly levels and targeted backend diagnostics.
#[test]
fn accepts_levels_and_targeted_filters() {
    for filter in [
        "trace",
        "debug",
        "info",
        "warn",
        "error",
        "portunus_engine=debug",
    ] {
        assert_eq!(LoggingConfig::new(filter).unwrap().filter(), filter);
    }
}

// Inputs: a malformed filter directive.
// Outputs: a stable typed error retaining the rejected operator input.
// Logic: reject configuration during startup instead of silently disabling telemetry.
#[test]
fn rejects_invalid_filter_directives() {
    assert!(matches!(
        LoggingConfig::new("portunus_engine[broken=debug"),
        Err(LoggingError::InvalidFilter { filter, .. }) if filter == "portunus_engine[broken=debug"
    ));
}

// Inputs: two installation attempts using the same valid process logging policy.
// Outputs: one successful global installation followed by a typed ownership error.
// Logic: prove the composition root cannot silently replace another global subscriber.
#[test]
fn installs_global_subscriber_exactly_once() {
    let config = LoggingConfig::new("error").unwrap();
    init_global_logging(&config).unwrap();
    assert!(matches!(
        init_global_logging(&config),
        Err(LoggingError::AlreadyInitialized { .. })
    ));
}
