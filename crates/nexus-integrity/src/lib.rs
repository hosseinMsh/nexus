//! Cryptographic checksum verification and integrity auditing for downloaded files.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum IntegrityError {
    #[error("Checksum mismatch for {algorithm}: expected {expected}, calculated {calculated}")]
    ChecksumMismatch {
        algorithm: String,
        expected: String,
        calculated: String,
    },

    #[error("I/O error during hash calculation: {0}")]
    Io(String),
}

/// Cryptographic hash algorithms supported by NEXUS.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HashAlgorithm {
    Sha256,
    Blake3,
}

/// Computes the cryptographic checksum of a byte slice.
#[must_use]
pub fn compute_hash(data: &[u8], algorithm: HashAlgorithm) -> String {
    match algorithm {
        HashAlgorithm::Sha256 => {
            let mut hasher = Sha256::new();
            hasher.update(data);
            hex::encode(hasher.finalize())
        }
        HashAlgorithm::Blake3 => {
            let hash = blake3::hash(data);
            hash.to_hex().to_string()
        }
    }
}

/// Verifies that the provided data matches the expected checksum string (case-insensitive).
pub fn verify_checksum(
    data: &[u8],
    algorithm: HashAlgorithm,
    expected: &str,
) -> Result<(), IntegrityError> {
    let calculated = compute_hash(data, algorithm);
    let norm_expected = expected.trim().to_lowercase();
    let norm_calculated = calculated.to_lowercase();

    if norm_expected != norm_calculated {
        return Err(IntegrityError::ChecksumMismatch {
            algorithm: format!("{algorithm:?}"),
            expected: norm_expected,
            calculated: norm_calculated,
        });
    }

    Ok(())
}

// Hex encoding helper to avoid external hex crate dependency
mod hex {
    pub fn encode(bytes: impl AsRef<[u8]>) -> String {
        bytes.as_ref().iter().map(|b| format!("{b:02x}")).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sha256_verification() {
        let data = b"hello nexus";
        let hash = compute_hash(data, HashAlgorithm::Sha256);
        assert_eq!(
            hash,
            "60ec57a2655601a0a94c7688edbeeb6aaecf33cd2c1c7fc447a056a360832be1"
        );
        assert!(verify_checksum(data, HashAlgorithm::Sha256, &hash).is_ok());
        assert!(verify_checksum(data, HashAlgorithm::Sha256, "wronghash").is_err());
    }

    #[test]
    fn test_blake3_verification() {
        let data = b"hello nexus";
        let hash = compute_hash(data, HashAlgorithm::Blake3);
        assert!(verify_checksum(data, HashAlgorithm::Blake3, &hash).is_ok());
    }
}
