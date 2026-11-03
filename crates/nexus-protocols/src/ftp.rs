//! File Transfer Protocol (FTP / FTPS) adapter implementing RFC 959 with Range (REST) support.

use async_trait::async_trait;
use nexus_domain::NetworkPath;
use nexus_transport::RemoteMetadata;
use std::net::SocketAddr;
use std::path::Path;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tracing::debug;
use url::Url;

use crate::{ProtocolAdapter, ProtocolError, ProtocolKind};

/// Parses an FTP response line into code and message: e.g. "220 Welcome" -> (220, "Welcome")
#[must_use]
pub fn parse_ftp_code(line: &str) -> Option<(u32, &str)> {
    if line.len() < 3 {
        return None;
    }
    let code_str = &line[0..3];
    let code = code_str.parse::<u32>().ok()?;
    let msg = if line.len() > 4 { &line[4..] } else { "" };
    Some((code, msg.trim()))
}

/// Parses a 227 PASV response: `227 Entering Passive Mode (h1,h2,h3,h4,p1,p2)`
#[must_use]
pub fn parse_pasv_response(line: &str) -> Option<SocketAddr> {
    let start = line.find('(')?;
    let end = line.rfind(')')?;
    let inner = &line[start + 1..end];
    let parts: Vec<u8> = inner
        .split(',')
        .filter_map(|p| p.trim().parse::<u8>().ok())
        .collect();

    if parts.len() != 6 {
        return None;
    }

    let ip = std::net::Ipv4Addr::new(parts[0], parts[1], parts[2], parts[3]);
    let port = (u16::from(parts[4]) << 8) | u16::from(parts[5]);
    Some(SocketAddr::new(std::net::IpAddr::V4(ip), port))
}

/// Parses a 229 EPSV response: `229 Entering Extended Passive Mode (|||port|)`
#[must_use]
pub fn parse_epsv_response(line: &str, default_host: std::net::IpAddr) -> Option<SocketAddr> {
    let start = line.find('(')?;
    let end = line.rfind(')')?;
    let inner = &line[start + 1..end];
    let delimiter = inner.chars().next()?;
    let segments: Vec<&str> = inner.split(delimiter).collect();

    if segments.len() >= 4 {
        let port: u16 = segments[3].parse().ok()?;
        Some(SocketAddr::new(default_host, port))
    } else {
        None
    }
}

/// Protocol adapter for FTP and FTPS transfers.
#[derive(Clone, Default)]
pub struct FtpProtocolAdapter;

impl FtpProtocolAdapter {
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    async fn send_command(
        reader: &mut BufReader<tokio::io::ReadHalf<TcpStream>>,
        writer: &mut tokio::io::WriteHalf<TcpStream>,
        cmd: &str,
    ) -> Result<(u32, String), ProtocolError> {
        writer
            .write_all(format!("{cmd}\r\n").as_bytes())
            .await
            .map_err(|e| {
                ProtocolError::Protocol(format!("Failed to write FTP command '{cmd}': {e}"))
            })?;

        let mut line = String::new();
        reader
            .read_line(&mut line)
            .await
            .map_err(|e| ProtocolError::Protocol(format!("Failed to read FTP response: {e}")))?;

        let (code, msg) = parse_ftp_code(&line)
            .ok_or_else(|| ProtocolError::Protocol(format!("Malformed FTP response: {line}")))?;

        Ok((code, msg.to_string()))
    }

    async fn connect_control_session(
        url: &Url,
    ) -> Result<
        (
            BufReader<tokio::io::ReadHalf<TcpStream>>,
            tokio::io::WriteHalf<TcpStream>,
            std::net::IpAddr,
        ),
        ProtocolError,
    > {
        let host = url
            .host_str()
            .ok_or_else(|| ProtocolError::Protocol("FTP URL missing host".to_string()))?;
        let port = url.port().unwrap_or(21);
        let addr_str = format!("{host}:{port}");

        let stream = TcpStream::connect(&addr_str).await.map_err(|e| {
            ProtocolError::Protocol(format!("Failed to connect to FTP server '{addr_str}': {e}"))
        })?;

        let peer_ip = stream
            .peer_addr()
            .map_err(|e| ProtocolError::Protocol(format!("Failed to get peer address: {e}")))?
            .ip();

        let (rx, mut tx) = tokio::io::split(stream);
        let mut reader = BufReader::new(rx);

        // Read initial 220 banner
        let mut banner = String::new();
        reader
            .read_line(&mut banner)
            .await
            .map_err(|e| ProtocolError::Protocol(format!("Failed to read FTP banner: {e}")))?;

        let (code, _) = parse_ftp_code(&banner)
            .ok_or_else(|| ProtocolError::Protocol(format!("Invalid FTP banner: {banner}")))?;
        if code != 220 {
            return Err(ProtocolError::Protocol(format!(
                "Unexpected banner status: {code}"
            )));
        }

        // Authenticate (USER / PASS)
        let username = if !url.username().is_empty() {
            url.username()
        } else {
            "anonymous"
        };
        let password = url.password().unwrap_or("anonymous@nexus");

        let (user_code, _) =
            Self::send_command(&mut reader, &mut tx, &format!("USER {username}")).await?;
        if user_code == 331 {
            let (pass_code, _) =
                Self::send_command(&mut reader, &mut tx, &format!("PASS {password}")).await?;
            if pass_code != 230 {
                return Err(ProtocolError::Protocol(format!(
                    "FTP authentication failed with code {pass_code}"
                )));
            }
        } else if user_code != 230 {
            return Err(ProtocolError::Protocol(format!(
                "FTP USER command rejected with code {user_code}"
            )));
        }

        // Binary mode
        let (type_code, _) = Self::send_command(&mut reader, &mut tx, "TYPE I").await?;
        if type_code != 200 {
            return Err(ProtocolError::Protocol(format!(
                "FTP TYPE I rejected with code {type_code}"
            )));
        }

        Ok((reader, tx, peer_ip))
    }
}

#[async_trait]
impl ProtocolAdapter for FtpProtocolAdapter {
    fn kind(&self) -> ProtocolKind {
        ProtocolKind::Ftp
    }

    fn can_handle(&self, url: &Url) -> bool {
        url.scheme() == "ftp" || url.scheme() == "ftps"
    }

    async fn probe_metadata(
        &self,
        _path: &NetworkPath,
        url: &Url,
    ) -> Result<RemoteMetadata, ProtocolError> {
        let (mut reader, mut writer, _) = Self::connect_control_session(url).await?;

        let file_path = url.path();
        debug!(url = %url, path = %file_path, "Probing FTP metadata via SIZE");

        // SIZE command (RFC 3659)
        let (size_code, size_msg) =
            Self::send_command(&mut reader, &mut writer, &format!("SIZE {file_path}")).await?;

        let total_size = if size_code == 213 {
            size_msg.parse::<u64>().ok()
        } else {
            None
        };

        let suggested_filename = Path::new(file_path)
            .file_name()
            .and_then(|n| n.to_str())
            .filter(|n| !n.is_empty())
            .map(ToString::to_string);

        // Disconnect
        let _ = Self::send_command(&mut reader, &mut writer, "QUIT").await;

        Ok(RemoteMetadata {
            total_size,
            supports_ranges: true, // FTP REST command allows seeking
            etag: None,
            last_modified: None,
            redirected_url: None,
            suggested_filename,
        })
    }

    async fn fetch_range(
        &self,
        _path: &NetworkPath,
        url: &Url,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<Box<dyn AsyncRead + Unpin + Send>, ProtocolError> {
        let (mut reader, mut writer, peer_ip) = Self::connect_control_session(url).await?;
        let file_path = url.path();

        // EPSV / PASV
        let (epsv_code, epsv_msg) = Self::send_command(&mut reader, &mut writer, "EPSV").await?;
        let data_addr = if epsv_code == 229 {
            parse_epsv_response(&epsv_msg, peer_ip).ok_or_else(|| {
                ProtocolError::Protocol("Failed to parse EPSV response".to_string())
            })?
        } else {
            let (pasv_code, pasv_msg) =
                Self::send_command(&mut reader, &mut writer, "PASV").await?;
            if pasv_code != 227 {
                return Err(ProtocolError::Protocol(format!(
                    "Both EPSV and PASV failed with code {pasv_code}"
                )));
            }
            parse_pasv_response(&pasv_msg).ok_or_else(|| {
                ProtocolError::Protocol("Failed to parse PASV response".to_string())
            })?
        };

        // Connect data socket
        let data_stream = TcpStream::connect(data_addr).await.map_err(|e| {
            ProtocolError::Protocol(format!(
                "Failed to connect to FTP data port {data_addr}: {e}"
            ))
        })?;

        // REST offset if resuming
        if start_offset > 0 {
            let (rest_code, _) =
                Self::send_command(&mut reader, &mut writer, &format!("REST {start_offset}"))
                    .await?;
            if rest_code != 350 {
                return Err(ProtocolError::Protocol(format!(
                    "FTP REST {start_offset} rejected with code {rest_code}"
                )));
            }
        }

        // RETR
        let (retr_code, _) =
            Self::send_command(&mut reader, &mut writer, &format!("RETR {file_path}")).await?;
        if retr_code != 150 && retr_code != 125 {
            return Err(ProtocolError::Protocol(format!(
                "FTP RETR rejected with code {retr_code}"
            )));
        }

        let range_len = end_offset.saturating_sub(start_offset).saturating_add(1);
        let limited_reader = data_stream.take(range_len);

        Ok(Box::new(limited_reader))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ftp_code() {
        assert_eq!(
            parse_ftp_code("220 Welcome to Nexus FTP"),
            Some((220, "Welcome to Nexus FTP"))
        );
        assert_eq!(parse_ftp_code("213 1048576"), Some((213, "1048576")));
        assert_eq!(
            parse_ftp_code("550 File not found"),
            Some((550, "File not found"))
        );
        assert!(parse_ftp_code("").is_none());
    }

    #[test]
    fn test_parse_pasv_response() {
        let resp = "227 Entering Passive Mode (192,168,1,100,7,232)";
        let addr = parse_pasv_response(resp).unwrap();
        assert_eq!(addr.to_string(), "192.168.1.100:2024");
    }

    #[test]
    fn test_parse_epsv_response() {
        let resp = "229 Entering Extended Passive Mode (|||5566|)";
        let ip = "127.0.0.1".parse().unwrap();
        let addr = parse_epsv_response(resp, ip).unwrap();
        assert_eq!(addr.to_string(), "127.0.0.1:5566");
    }
}
