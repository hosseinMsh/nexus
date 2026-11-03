//! Metalink 4.0 (RFC 5854) and Metalink 3.0 document parsing and multi-mirror resolution.

use crate::ProtocolError;
use url::Url;

/// Mirror URL entry extracted from a Metalink file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetalinkUrl {
    pub url: Url,
    pub priority: u32,
    pub location: Option<String>,
}

/// Cryptographic hash entry extracted from a Metalink file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetalinkHash {
    pub algorithm: String,
    pub hex_digest: String,
}

/// Represents a single file resource described inside a Metalink document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetalinkFile {
    pub name: String,
    pub size: Option<u64>,
    pub hashes: Vec<MetalinkHash>,
    pub urls: Vec<MetalinkUrl>,
}

impl MetalinkFile {
    /// Returns the mirror URLs sorted by priority (lowest priority number = highest preference).
    #[must_use]
    pub fn sorted_mirrors(&self) -> Vec<Url> {
        let mut sorted = self.urls.clone();
        sorted.sort_by_key(|u| u.priority);
        sorted.into_iter().map(|u| u.url).collect()
    }
}

/// A parsed Metalink document containing one or more files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetalinkDocument {
    pub files: Vec<MetalinkFile>,
}

/// Lightweight parser for Metalink XML files without requiring heavy external XML libraries.
pub struct MetalinkParser;

impl MetalinkParser {
    /// Parses Metalink 4.0 or Metalink 3.0 XML content.
    pub fn parse(xml: &str) -> Result<MetalinkDocument, ProtocolError> {
        let mut files = Vec::new();

        // Split into file blocks: <file ...> ... </file>
        let mut remaining = xml;
        while let Some(start_file) = remaining.find("<file") {
            let after_start = &remaining[start_file..];
            let end_tag = match after_start.find("</file>") {
                Some(idx) => idx + "</file>".len(),
                None => break,
            };
            let file_block = &after_start[..end_tag];
            remaining = &after_start[end_tag..];

            // Extract file name
            let name = Self::extract_attribute(file_block, "name")
                .unwrap_or_else(|| "download.bin".to_string());

            // Extract size: <size>12345</size>
            let size =
                Self::extract_tag_content(file_block, "size").and_then(|s| s.parse::<u64>().ok());

            // Extract hashes: <hash type="sha-256">abcd</hash>
            let mut hashes = Vec::new();
            let mut hash_cursor = file_block;
            while let Some(h_start) = hash_cursor.find("<hash") {
                let h_block = &hash_cursor[h_start..];
                let h_end = match h_block.find("</hash>") {
                    Some(idx) => idx + "</hash>".len(),
                    None => break,
                };
                let full_hash_tag = &h_block[..h_end];
                hash_cursor = &h_block[h_end..];

                let algo = Self::extract_attribute(full_hash_tag, "type")
                    .unwrap_or_else(|| "sha-256".to_string());
                if let Some(digest) = Self::extract_tag_content(full_hash_tag, "hash") {
                    hashes.push(MetalinkHash {
                        algorithm: algo.to_ascii_lowercase(),
                        hex_digest: digest.trim().to_ascii_lowercase(),
                    });
                }
            }

            // Extract URLs: <url priority="1" location="us">http://...</url>
            let mut urls = Vec::new();
            let mut url_cursor = file_block;
            while let Some(u_start) = url_cursor.find("<url") {
                let u_block = &url_cursor[u_start..];
                let u_end = match u_block.find("</url>") {
                    Some(idx) => idx + "</url>".len(),
                    None => break,
                };
                let full_url_tag = &u_block[..u_end];
                url_cursor = &u_block[u_end..];

                let priority = Self::extract_attribute(full_url_tag, "priority")
                    .and_then(|p| p.parse::<u32>().ok())
                    .unwrap_or(100);

                let location = Self::extract_attribute(full_url_tag, "location");

                if let Some(raw_url) = Self::extract_tag_content(full_url_tag, "url") {
                    if let Ok(parsed_url) = Url::parse(raw_url.trim()) {
                        urls.push(MetalinkUrl {
                            url: parsed_url,
                            priority,
                            location,
                        });
                    }
                }
            }

            files.push(MetalinkFile {
                name,
                size,
                hashes,
                urls,
            });
        }

        if files.is_empty() {
            return Err(ProtocolError::Protocol(
                "No valid <file> elements found in Metalink XML".to_string(),
            ));
        }

        Ok(MetalinkDocument { files })
    }

    fn extract_attribute(tag: &str, attr: &str) -> Option<String> {
        let pattern = format!("{attr}=\"");
        let start = tag.find(&pattern)? + pattern.len();
        let end = tag[start..].find('"')? + start;
        Some(tag[start..end].to_string())
    }

    fn extract_tag_content<'a>(block: &'a str, tag_name: &str) -> Option<&'a str> {
        let open_tag = format!("<{tag_name}");
        let close_tag = format!("</{tag_name}>");

        let open_pos = block.find(&open_tag)?;
        let content_start = block[open_pos..].find('>')? + open_pos + 1;
        let content_end = block[content_start..].find(&close_tag)? + content_start;

        Some(block[content_start..content_end].trim())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_metalink_xml_mirrors_and_hashes() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<metalink xmlns="urn:ietf:params:xml:ns:metalink">
  <file name="linux-distro.iso">
    <size>734003200</size>
    <hash type="sha-256">e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855</hash>
    <url priority="1" location="us">https://us.mirror.org/linux-distro.iso</url>
    <url priority="2" location="de">https://de.mirror.org/linux-distro.iso</url>
  </file>
</metalink>"#;

        let doc = MetalinkParser::parse(xml).unwrap();
        assert_eq!(doc.files.len(), 1);

        let file = &doc.files[0];
        assert_eq!(file.name, "linux-distro.iso");
        assert_eq!(file.size, Some(734003200));

        assert_eq!(file.hashes.len(), 1);
        assert_eq!(file.hashes[0].algorithm, "sha-256");
        assert_eq!(
            file.hashes[0].hex_digest,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );

        assert_eq!(file.urls.len(), 2);
        let mirrors = file.sorted_mirrors();
        assert_eq!(
            mirrors[0].as_str(),
            "https://us.mirror.org/linux-distro.iso"
        );
        assert_eq!(
            mirrors[1].as_str(),
            "https://de.mirror.org/linux-distro.iso"
        );
    }
}
