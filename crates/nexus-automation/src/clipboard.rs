//! Clipboard URL inspection, link detection, and automatic capture.

use url::Url;

/// Extracts valid download URLs from raw text copied to the system clipboard.
pub struct ClipboardUrlExtractor;

impl ClipboardUrlExtractor {
    /// Extracts unique valid transfer URLs from text.
    #[must_use]
    pub fn extract_urls(text: &str) -> Vec<Url> {
        let mut results = Vec::new();

        for word in text.split_whitespace() {
            // Check for magnet link
            if word.starts_with("magnet:?") {
                if let Ok(u) = Url::parse(word) {
                    if !results.contains(&u) {
                        results.push(u);
                    }
                }
                continue;
            }

            // Check standard schemes
            if word.starts_with("http://")
                || word.starts_with("https://")
                || word.starts_with("ftp://")
                || word.starts_with("ftps://")
                || word.starts_with("webdav://")
                || word.starts_with("webdavs://")
            {
                // Strip common trailing punctuation like '.', ',', ';', ')'
                let trimmed = word.trim_end_matches(['.', ',', ';', ')', ']', '>']);
                if let Ok(u) = Url::parse(trimmed) {
                    if !results.contains(&u) {
                        results.push(u);
                    }
                }
            }
        }

        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_urls_from_clipboard_text() {
        let text = r#"
            Here are the download links for the project:
            https://download.nexus.dev/release/v1.0.tar.gz.
            Also check ftp://ftp.gnu.org/gnu/gcc/gcc-15.1.0.tar.xz,
            and the magnet link: magnet:?xt=urn:btih:c12fe1c06bba254a9dc9f519b335de7ece74fed3&dn=NEXUS
            Repeated: https://download.nexus.dev/release/v1.0.tar.gz
        "#;

        let urls = ClipboardUrlExtractor::extract_urls(text);
        assert_eq!(urls.len(), 3);
        assert_eq!(
            urls[0].as_str(),
            "https://download.nexus.dev/release/v1.0.tar.gz"
        );
        assert_eq!(
            urls[1].as_str(),
            "ftp://ftp.gnu.org/gnu/gcc/gcc-15.1.0.tar.xz"
        );
        assert!(urls[2].as_str().starts_with("magnet:?xt=urn:btih:"));
    }
}
