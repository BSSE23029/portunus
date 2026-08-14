//! Integration coverage for bounded control-plane request admission.

use portunus_daemon::admission::{AdmissionConfig, AdmissionError, AdmissionInterceptor};
use tonic::{service::Interceptor, Code, Request};

// Inputs: zero and exact-minimum concurrent request limits.
// Outputs: stable zero error and usable one-request configuration.
// Logic: reject disabled/unbounded topology before constructing the interceptor.
#[test]
fn validates_request_admission_boundaries() {
    assert_eq!(AdmissionConfig::new(0), Err(AdmissionError::ZeroLimit));
    assert!(AdmissionConfig::new(1).is_ok());
}

// Inputs: absent, numeric, malformed, and zero configuration sources.
// Outputs: deterministic default/override resolution and stable validation errors.
// Logic: keep environment parsing pure so startup never silently ignores bad limits.
#[test]
fn resolves_validated_admission_configuration() {
    assert_eq!(
        AdmissionConfig::from_source(None).unwrap(),
        AdmissionConfig::new(128).unwrap()
    );
    assert_eq!(
        AdmissionConfig::from_source(Some("7")).unwrap(),
        AdmissionConfig::new(7).unwrap()
    );
    assert_eq!(
        AdmissionConfig::from_source(Some("bad")),
        Err(AdmissionError::InvalidLimit)
    );
    assert_eq!(
        AdmissionConfig::from_source(Some("0")),
        Err(AdmissionError::ZeroLimit)
    );
}

// Inputs: one-permit interceptor, exact request, one-over request, then permit drop.
// Outputs: immediate resource exhaustion under load and recovery after completion.
// Logic: prove request extensions retain RAII ownership for the whole accepted call.
#[test]
fn sheds_one_over_the_concurrency_limit() {
    let mut admission = AdmissionInterceptor::new(AdmissionConfig::new(1).unwrap());
    let admitted = admission.call(Request::new(())).unwrap();
    assert_eq!(
        admission.call(Request::new(())).unwrap_err().code(),
        Code::ResourceExhausted
    );
    drop(admitted);
    assert!(admission.call(Request::new(())).is_ok());
}
