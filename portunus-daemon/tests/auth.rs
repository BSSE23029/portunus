//! Integration coverage for control-plane authentication hooks.

use portunus_daemon::auth::{authorize, AuthConfig, AuthError};
use tonic::{Code, Request};

// Inputs: disabled authentication and a request without metadata.
// Outputs: unchanged authorized request.
// Logic: preserve a deliberate local-development mode without implicit credentials.
#[test]
fn allows_requests_when_authentication_is_disabled() {
    let request = Request::new(());
    assert!(authorize(&AuthConfig::disabled(), request).is_ok());
}

// Inputs: exact, missing, malformed, and incorrect bearer authorization metadata.
// Outputs: exact token accepted and every rejected form mapped to unauthenticated.
// Logic: compare bounded metadata without logging or returning credential contents.
#[test]
fn validates_configured_bearer_credentials() {
    let config = AuthConfig::bearer("secret-token").unwrap();
    let mut valid = Request::new(());
    valid
        .metadata_mut()
        .insert("authorization", "Bearer secret-token".parse().unwrap());
    assert!(authorize(&config, valid).is_ok());

    for value in [None, Some("Basic abc"), Some("Bearer wrong")] {
        let mut request = Request::new(());
        if let Some(value) = value {
            request
                .metadata_mut()
                .insert("authorization", value.parse().unwrap());
        }
        assert_eq!(
            authorize(&config, request).unwrap_err().code(),
            Code::Unauthenticated
        );
    }
}

// Inputs: empty bearer secret.
// Outputs: stable configuration error before server startup.
// Logic: prevent a malformed operational policy from becoming an always-deny server.
#[test]
fn rejects_empty_bearer_configuration() {
    assert_eq!(AuthConfig::bearer(""), Err(AuthError::EmptyBearerToken));
}

// Inputs: absent and present optional environment-derived token sources.
// Outputs: disabled or enabled validated policy with no empty-token fallback.
// Logic: keep ambient configuration resolution pure and independently testable.
#[test]
fn resolves_optional_authentication_source() {
    assert_eq!(
        AuthConfig::from_source(None).unwrap(),
        AuthConfig::disabled()
    );
    assert_eq!(
        AuthConfig::from_source(Some("token")).unwrap(),
        AuthConfig::bearer("token").unwrap()
    );
}
