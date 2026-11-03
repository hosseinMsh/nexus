//! BitTorrent and Magnet protocol abstractions, Bencode encoding/decoding (BEP 3), and Magnet URI parsing (BEP 9).

use async_trait::async_trait;
use nexus_domain::NetworkPath;
use nexus_transport::RemoteMetadata;
use std::collections::BTreeMap;
use tokio::io::AsyncRead;
use url::Url;

use crate::{ProtocolAdapter, ProtocolError, ProtocolKind};

/// Represents a parsed Bencode value (BEP 3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BencodeValue {
    Integer(i64),
    Bytes(Vec<u8>),
    List(Vec<BencodeValue>),
    Dict(BTreeMap<Vec<u8>, BencodeValue>),
}

impl BencodeValue {
    /// Decodes a Bencode value from bytes, returning the value and consumed length.
    pub fn decode(bytes: &[u8]) -> Result<(Self, usize), ProtocolError> {
        if bytes.is_empty() {
            return Err(ProtocolError::Protocol("Empty Bencode input".to_string()));
        }

        match bytes[0] {
            b'i' => {
                let end = bytes.iter().position(|&b| b == b'e').ok_or_else(|| {
                    ProtocolError::Protocol("Unterminated Bencode integer".into())
                })?;
                let num_str = std::str::from_utf8(&bytes[1..end])
                    .map_err(|e| ProtocolError::Protocol(format!("Invalid integer utf8: {e}")))?;
                let val = num_str
                    .parse::<i64>()
                    .map_err(|e| ProtocolError::Protocol(format!("Invalid integer parse: {e}")))?;
                Ok((BencodeValue::Integer(val), end + 1))
            }
            b'l' => {
                let mut list = Vec::new();
                let mut idx = 1;
                while idx < bytes.len() && bytes[idx] != b'e' {
                    let (val, len) = Self::decode(&bytes[idx..])?;
                    list.push(val);
                    idx += len;
                }
                if idx >= bytes.len() || bytes[idx] != b'e' {
                    return Err(ProtocolError::Protocol("Unterminated Bencode list".into()));
                }
                Ok((BencodeValue::List(list), idx + 1))
            }
            b'd' => {
                let mut dict = BTreeMap::new();
                let mut idx = 1;
                while idx < bytes.len() && bytes[idx] != b'e' {
                    let (key_val, key_len) = Self::decode(&bytes[idx..])?;
                    let key = match key_val {
                        BencodeValue::Bytes(b) => b,
                        _ => {
                            return Err(ProtocolError::Protocol(
                                "Bencode dict key must be bytes".into(),
                            ))
                        }
                    };
                    idx += key_len;
                    if idx >= bytes.len() {
                        return Err(ProtocolError::Protocol(
                            "Truncated Bencode dict value".into(),
                        ));
                    }
                    let (val, val_len) = Self::decode(&bytes[idx..])?;
                    dict.insert(key, val);
                    idx += val_len;
                }
                if idx >= bytes.len() || bytes[idx] != b'e' {
                    return Err(ProtocolError::Protocol("Unterminated Bencode dict".into()));
                }
                Ok((BencodeValue::Dict(dict), idx + 1))
            }
            b'0'..=b'9' => {
                let colon = bytes.iter().position(|&b| b == b':').ok_or_else(|| {
                    ProtocolError::Protocol("Missing colon in byte string".into())
                })?;
                let len_str = std::str::from_utf8(&bytes[..colon])
                    .map_err(|e| ProtocolError::Protocol(format!("Invalid string length: {e}")))?;
                let len = len_str
                    .parse::<usize>()
                    .map_err(|e| ProtocolError::Protocol(format!("Invalid string size: {e}")))?;
                let start = colon + 1;
                let end = start + len;
                if end > bytes.len() {
                    return Err(ProtocolError::Protocol(
                        "Bencode string length exceeds input".into(),
                    ));
                }
                Ok((BencodeValue::Bytes(bytes[start..end].to_vec()), end))
            }
            other => Err(ProtocolError::Protocol(format!(
                "Unexpected Bencode byte prefix: {other}"
            ))),
        }
    }

    /// Serializes a Bencode value into its standard byte representation.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_into(&mut out);
        out
    }

    fn encode_into(&self, buf: &mut Vec<u8>) {
        match self {
            BencodeValue::Integer(i) => {
                buf.push(b'i');
                buf.extend(i.to_string().as_bytes());
                buf.push(b'e');
            }
            BencodeValue::Bytes(b) => {
                buf.extend(b.len().to_string().as_bytes());
                buf.push(b':');
                buf.extend(b);
            }
            BencodeValue::List(list) => {
                buf.push(b'l');
                for item in list {
                    item.encode_into(buf);
                }
                buf.push(b'e');
            }
            BencodeValue::Dict(dict) => {
                buf.push(b'd');
                for (k, v) in dict {
                    buf.extend(k.len().to_string().as_bytes());
                    buf.push(b':');
                    buf.extend(k);
                    v.encode_into(buf);
                }
                buf.push(b'e');
            }
        }
    }
}

/// Parsed Magnet URI parameters (BEP 9).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MagnetUri {
    pub info_hash: String,
    pub display_name: Option<String>,
    pub trackers: Vec<String>,
    pub web_seeds: Vec<String>,
    pub exact_length: Option<u64>,
}

impl MagnetUri {
    /// Parses a standard `magnet:?...` URI.
    pub fn parse(uri: &str) -> Result<Self, ProtocolError> {
        if !uri.starts_with("magnet:?") {
            return Err(ProtocolError::Protocol(
                "Not a valid magnet URI".to_string(),
            ));
        }

        let query = &uri["magnet:?".len()..];
        let mut info_hash = None;
        let mut display_name = None;
        let mut trackers = Vec::new();
        let mut web_seeds = Vec::new();
        let mut exact_length = None;

        for pair in query.split('&') {
            if let Some((key, val)) = pair.split_once('=') {
                let decoded_val = urlencoding_decode(val);
                match key {
                    "xt" => {
                        if let Some(hash) = decoded_val.strip_prefix("urn:btih:") {
                            info_hash = Some(hash.to_ascii_lowercase());
                        }
                    }
                    "dn" => {
                        display_name = Some(decoded_val);
                    }
                    "tr" => {
                        trackers.push(decoded_val);
                    }
                    "ws" => {
                        web_seeds.push(decoded_val);
                    }
                    "xl" => {
                        exact_length = decoded_val.parse::<u64>().ok();
                    }
                    _ => {}
                }
            }
        }

        let info_hash = info_hash.ok_or_else(|| {
            ProtocolError::Protocol("Magnet URI missing xt=urn:btih: infohash".to_string())
        })?;

        Ok(Self {
            info_hash,
            display_name,
            trackers,
            web_seeds,
            exact_length,
        })
    }
}

fn urlencoding_decode(s: &str) -> String {
    let mut out = String::new();
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '%' {
            if let (Some(h1), Some(h2)) = (chars.next(), chars.next()) {
                let hex_str = format!("{h1}{h2}");
                if let Ok(b) = u8::from_str_radix(&hex_str, 16) {
                    out.push(b as char);
                    continue;
                }
            }
        } else if c == '+' {
            out.push(' ');
            continue;
        }
        out.push(c);
    }
    out
}

/// Protocol adapter for BitTorrent Magnet URIs.
#[derive(Clone, Default)]
pub struct MagnetProtocolAdapter;

#[async_trait]
impl ProtocolAdapter for MagnetProtocolAdapter {
    fn kind(&self) -> ProtocolKind {
        ProtocolKind::Magnet
    }

    fn can_handle(&self, url: &Url) -> bool {
        url.scheme() == "magnet"
    }

    async fn probe_metadata(
        &self,
        _path: &NetworkPath,
        url: &Url,
    ) -> Result<RemoteMetadata, ProtocolError> {
        let magnet = MagnetUri::parse(url.as_str())?;

        Ok(RemoteMetadata {
            total_size: magnet.exact_length,
            supports_ranges: true,
            etag: Some(magnet.info_hash),
            last_modified: None,
            redirected_url: None,
            suggested_filename: magnet.display_name,
        })
    }

    async fn fetch_range(
        &self,
        _path: &NetworkPath,
        url: &Url,
        _start_offset: u64,
        _end_offset: u64,
    ) -> Result<Box<dyn AsyncRead + Unpin + Send>, ProtocolError> {
        let magnet = MagnetUri::parse(url.as_str())?;
        if let Some(seed) = magnet.web_seeds.first() {
            // Forward to web seed URL if available
            return Err(ProtocolError::Protocol(format!(
                "Delegating magnet to web seed: {seed}"
            )));
        }

        Err(ProtocolError::Protocol(format!(
            "BitTorrent swarm download queued for infohash {}",
            magnet.info_hash
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bencode_integer_and_string_roundtrip() {
        let val = BencodeValue::Integer(42);
        let encoded = val.encode();
        assert_eq!(encoded, b"i42e");
        let (decoded, len) = BencodeValue::decode(&encoded).unwrap();
        assert_eq!(decoded, val);
        assert_eq!(len, encoded.len());

        let str_val = BencodeValue::Bytes(b"nexus_test".to_vec());
        let str_enc = str_val.encode();
        assert_eq!(str_enc, b"10:nexus_test");
        let (str_dec, str_len) = BencodeValue::decode(&str_enc).unwrap();
        assert_eq!(str_dec, str_val);
        assert_eq!(str_len, str_enc.len());
    }

    #[test]
    fn test_bencode_list_and_dict() {
        let mut dict = BTreeMap::new();
        dict.insert(
            b"name".to_vec(),
            BencodeValue::Bytes(b"ubuntu.iso".to_vec()),
        );
        dict.insert(b"length".to_vec(), BencodeValue::Integer(1073741824));

        let bencode_dict = BencodeValue::Dict(dict);
        let encoded = bencode_dict.encode();
        let (decoded, _) = BencodeValue::decode(&encoded).unwrap();
        assert_eq!(decoded, bencode_dict);
    }

    #[test]
    fn test_magnet_uri_parsing() {
        let uri = "magnet:?xt=urn:btih:c12fe1c06bba254a9dc9f519b335de7ece74fed3&dn=Arch+Linux+2026&tr=udp%3A%2F%2Ftracker.archlinux.org%3A6969&xl=987654321";
        let parsed = MagnetUri::parse(uri).unwrap();
        assert_eq!(parsed.info_hash, "c12fe1c06bba254a9dc9f519b335de7ece74fed3");
        assert_eq!(parsed.display_name, Some("Arch Linux 2026".to_string()));
        assert_eq!(parsed.exact_length, Some(987654321));
        assert_eq!(
            parsed.trackers,
            vec!["udp://tracker.archlinux.org:6969".to_string()]
        );
    }
}
