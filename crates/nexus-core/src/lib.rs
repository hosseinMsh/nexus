//! Core types, identifiers, error primitives, and shared utilities for NEXUS.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::Hash;
use std::str::FromStr;
use thiserror::Error;
use uuid::Uuid;

/// Typed identifier wrapper guaranteeing type-safe IDs across all domain entities.
pub struct Id<T> {
    raw: Uuid,
    _marker: std::marker::PhantomData<fn() -> T>,
}

impl<T> fmt::Debug for Id<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Id").field(&self.raw).finish()
    }
}

impl<T> Id<T> {
    /// Generates a new random v4 unique identifier.
    #[must_use]
    pub fn new() -> Self {
        Self {
            raw: Uuid::new_v4(),
            _marker: std::marker::PhantomData,
        }
    }

    /// Creates a typed identifier from an existing raw UUID.
    #[must_use]
    pub const fn from_uuid(raw: Uuid) -> Self {
        Self {
            raw,
            _marker: std::marker::PhantomData,
        }
    }

    /// Returns the underlying raw UUID.
    #[must_use]
    pub const fn into_inner(self) -> Uuid {
        self.raw
    }

    /// Returns a reference to the raw UUID.
    #[must_use]
    pub const fn as_uuid(&self) -> &Uuid {
        &self.raw
    }
}

impl<T> Default for Id<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Clone for Id<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Id<T> {}

impl<T> PartialEq for Id<T> {
    fn eq(&self, other: &Self) -> bool {
        self.raw == other.raw
    }
}

impl<T> Eq for Id<T> {}

impl<T> PartialOrd for Id<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Id<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.raw.cmp(&other.raw)
    }
}

impl<T> Hash for Id<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.raw.hash(state);
    }
}

impl<T> Serialize for Id<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.raw.serialize(serializer)
    }
}

impl<'de, T> Deserialize<'de> for Id<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = Uuid::deserialize(deserializer)?;
        Ok(Self::from_uuid(raw))
    }
}

impl<T> fmt::Display for Id<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.raw)
    }
}

impl<T> FromStr for Id<T> {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let raw = Uuid::parse_str(s)?;
        Ok(Self::from_uuid(raw))
    }
}

/// Representation of byte sizes with human-readable formatting and parsing.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct ByteSize(pub u64);

impl ByteSize {
    pub const ZERO: Self = Self(0);
    pub const KIB: u64 = 1024;
    pub const MIB: u64 = 1024 * 1024;
    pub const GIB: u64 = 1024 * 1024 * 1024;
    pub const TIB: u64 = 1024 * 1024 * 1024 * 1024;

    #[must_use]
    pub const fn bytes(self) -> u64 {
        self.0
    }

    #[must_use]
    pub fn from_kib(kib: u64) -> Self {
        Self(kib * Self::KIB)
    }

    #[must_use]
    pub fn from_mib(mib: u64) -> Self {
        Self(mib * Self::MIB)
    }

    #[must_use]
    pub fn from_gib(gib: u64) -> Self {
        Self(gib * Self::GIB)
    }

    #[must_use]
    pub fn format_human(&self) -> String {
        let b = self.0 as f64;
        if self.0 >= Self::TIB {
            format!("{:.2} TB", b / (Self::TIB as f64))
        } else if self.0 >= Self::GIB {
            format!("{:.2} GB", b / (Self::GIB as f64))
        } else if self.0 >= Self::MIB {
            format!("{:.2} MB", b / (Self::MIB as f64))
        } else if self.0 >= Self::KIB {
            format!("{:.2} KB", b / (Self::KIB as f64))
        } else {
            format!("{} B", self.0)
        }
    }
}

impl fmt::Display for ByteSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.format_human())
    }
}

/// Core error representation.
#[derive(Error, Debug)]
pub enum CoreError {
    #[error("Invalid identifier: {0}")]
    InvalidIdentifier(String),

    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Validation error: {0}")]
    ValidationError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestTag;

    #[test]
    fn test_typed_id_generation_and_string_roundtrip() {
        let id: Id<TestTag> = Id::new();
        let s = id.to_string();
        let parsed: Id<TestTag> = s.parse().expect("should parse successfully");
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_bytesize_human_formatting() {
        assert_eq!(ByteSize(500).to_string(), "500 B");
        assert_eq!(ByteSize::from_kib(10).to_string(), "10.00 KB");
        assert_eq!(ByteSize::from_mib(50).to_string(), "50.00 MB");
        assert_eq!(ByteSize::from_gib(4).to_string(), "4.00 GB");
    }
}
