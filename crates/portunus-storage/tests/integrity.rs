use portunus_storage::integrity::{ContentId, IntegrityError, IntegrityValidator, Sha1Validator};

struct ExactValidator;

impl IntegrityValidator for ExactValidator {
    // Inputs: candidate bytes and expected application-defined digest bytes.
    // Outputs: success for equality or a stable mismatch error.
    // Logic: model a caller-supplied validator without a cryptographic dependency.
    fn validate(&self, data: &[u8], expected: &[u8]) -> Result<(), IntegrityError> {
        if data == expected {
            Ok(())
        } else {
            Err(IntegrityError::Mismatch)
        }
    }
}

// Inputs: exact SHA-1 digest and matching candidate bytes.
// Outputs: tagged identity and successful compatibility validation.
// Logic: prove SHA-1 remains an adapter over the generic integrity contract.
#[test]
fn validates_sha1_content_identity() {
    let identity =
        ContentId::new("sha1", portunus_storage::integrity::sha1_digest(b"chunk")).unwrap();
    assert_eq!(identity.algorithm(), "sha1");
    assert_eq!(identity.digest().len(), 20);
    Sha1Validator.validate(b"chunk", identity.digest()).unwrap();
}

// Inputs: empty algorithm, empty digest, malformed SHA-1 digest, and wrong bytes.
// Outputs: typed stable errors distinguishing identity shape from mismatch.
// Logic: cover zero/rejected identity boundaries and hostile digest declarations.
#[test]
fn rejects_invalid_identities_and_digests() {
    assert_eq!(
        ContentId::new("", [1]).unwrap_err(),
        IntegrityError::EmptyAlgorithm
    );
    assert_eq!(
        ContentId::new("sha1", []).unwrap_err(),
        IntegrityError::EmptyDigest
    );
    assert_eq!(
        Sha1Validator.validate(b"chunk", &[0; 19]).unwrap_err(),
        IntegrityError::InvalidDigestLength {
            expected: 20,
            actual: 19,
        }
    );
    assert_eq!(
        Sha1Validator.validate(b"wrong", &[0; 20]).unwrap_err(),
        IntegrityError::Mismatch
    );
}

// Inputs: a custom validator and application-defined identity bytes.
// Outputs: success and mismatch through the same public validator trait.
// Logic: ensure storage policy is not coupled to SHA-1 or a fixed digest width.
#[test]
fn supports_application_defined_validators() {
    let identity = ContentId::new("exact-fixture", b"payload").unwrap();
    ExactValidator
        .validate(b"payload", identity.digest())
        .unwrap();
    assert_eq!(
        ExactValidator
            .validate(b"other", identity.digest())
            .unwrap_err(),
        IntegrityError::Mismatch
    );
}
