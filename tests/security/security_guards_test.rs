//! Integration test for security boundaries, SSRF validation, and integrity checking.

use nexus_integrity::{compute_hash, verify_checksum, HashAlgorithm};
use nexus_security::{sanitize_filename, validate_url_ssrf};
use url::Url;

#[test]
fn test_ssrf_and_filename_traversal_guards() {
    // 1. SSRF guards
    let loopback = Url::parse("http://127.0.0.1:9090/admin").unwrap();
    assert!(validate_url_ssrf(&loopback, false).is_err());

    let localhost = Url::parse("http://localhost/secret").unwrap();
    assert!(validate_url_ssrf(&localhost, false).is_err());

    let cgnat = Url::parse("http://100.64.0.1/status").unwrap();
    assert!(validate_url_ssrf(&cgnat, false).is_err());

    let valid_public = Url::parse("https://ftp.gnu.org/gnu/gcc/gcc-14.2.0.tar.gz").unwrap();
    assert!(validate_url_ssrf(&valid_public, false).is_ok());

    // 2. Path traversal guards
    assert!(sanitize_filename("../../etc/passwd").is_err());
    assert!(sanitize_filename("..\\Windows\\System32").is_err());
    assert!(sanitize_filename("image\0.png").is_err());

    let clean = sanitize_filename("archive_2026.tar.gz").unwrap();
    assert_eq!(clean, "archive_2026.tar.gz");

    // 3. Cryptographic integrity
    let payload = b"NEXUS Reliable Multi-Path Download Manager";
    let sha256_hash = compute_hash(payload, HashAlgorithm::Sha256);
    assert!(verify_checksum(payload, HashAlgorithm::Sha256, &sha256_hash).is_ok());

    let blake3_hash = compute_hash(payload, HashAlgorithm::Blake3);
    assert!(verify_checksum(payload, HashAlgorithm::Blake3, &blake3_hash).is_ok());
}
