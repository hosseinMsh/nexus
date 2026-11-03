//! Local JSON-RPC 2.0 communication protocol and authentication schema.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum IpcError {
    #[error("Authentication failed: invalid or missing bearer token")]
    Unauthorized,

    #[error("Invalid JSON-RPC version: expected '2.0', got '{0}'")]
    InvalidVersion(String),

    #[error("Method not found: '{0}'")]
    MethodNotFound(String),

    #[error("Invalid parameters: {0}")]
    InvalidParams(String),
}

/// Standard JSON-RPC 2.0 request payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub id: Option<Value>,
    pub method: String,
    #[serde(default)]
    pub params: Option<Value>,
}

/// Standard JSON-RPC 2.0 error payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl JsonRpcError {
    pub const PARSE_ERROR: i32 = -32700;
    pub const INVALID_REQUEST: i32 = -32600;
    pub const METHOD_NOT_FOUND: i32 = -32601;
    pub const INVALID_PARAMS: i32 = -32602;
    pub const INTERNAL_ERROR: i32 = -32603;
    pub const UNAUTHORIZED: i32 = -32001;

    #[must_use]
    pub fn unauthorized() -> Self {
        Self {
            code: Self::UNAUTHORIZED,
            message: "Unauthorized: Invalid or missing authentication token".to_string(),
            data: None,
        }
    }

    #[must_use]
    pub fn method_not_found(method: &str) -> Self {
        Self {
            code: Self::METHOD_NOT_FOUND,
            message: format!("Method '{method}' not found"),
            data: None,
        }
    }
}

/// Standard JSON-RPC 2.0 response payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub id: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

impl JsonRpcResponse {
    #[must_use]
    pub fn success(id: Option<Value>, result: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id,
            result: Some(result),
            error: None,
        }
    }

    #[must_use]
    pub fn error(id: Option<Value>, error: JsonRpcError) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id,
            result: None,
            error: Some(error),
        }
    }
}

/// Validates session bearer tokens against the server session key.
pub fn validate_auth_token(auth_header: Option<&str>, session_token: &str) -> Result<(), IpcError> {
    match auth_header {
        Some(h) if h.starts_with("Bearer ") => {
            let token = &h["Bearer ".len()..];
            if token == session_token {
                Ok(())
            } else {
                Err(IpcError::Unauthorized)
            }
        }
        _ => Err(IpcError::Unauthorized),
    }
}

/// Asynchronous RPC handler callback interface.
#[async_trait::async_trait]
pub trait RpcHandler: Send + Sync {
    async fn handle(&self, method: &str, params: Option<Value>) -> Result<Value, JsonRpcError>;
}

/// Dispatches validated JSON-RPC requests to the application RPC handler.
pub struct RpcDispatcher {
    session_token: String,
    handler: std::sync::Arc<dyn RpcHandler>,
}

pub type JsonRpcDispatcher = RpcDispatcher;

impl RpcDispatcher {
    #[must_use]
    pub fn new(session_token: String, handler: std::sync::Arc<dyn RpcHandler>) -> Self {
        Self {
            session_token,
            handler,
        }
    }

    #[must_use]
    pub fn session_token(&self) -> &str {
        &self.session_token
    }

    /// Authenticates and executes an incoming JSON-RPC 2.0 request.
    pub async fn dispatch(
        &self,
        auth_header: Option<&str>,
        request: JsonRpcRequest,
    ) -> JsonRpcResponse {
        if request.jsonrpc != "2.0" {
            return JsonRpcResponse::error(
                request.id,
                JsonRpcError {
                    code: JsonRpcError::INVALID_REQUEST,
                    message: format!("Expected jsonrpc '2.0', got '{}'", request.jsonrpc),
                    data: None,
                },
            );
        }

        if validate_auth_token(auth_header, &self.session_token).is_err() {
            return JsonRpcResponse::error(request.id, JsonRpcError::unauthorized());
        }

        match self.handler.handle(&request.method, request.params).await {
            Ok(result) => JsonRpcResponse::success(request.id, result),
            Err(err) => JsonRpcResponse::error(request.id, err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_rpc_serialization() {
        let req = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(Value::from(1)),
            method: "download.list".to_string(),
            params: None,
        };

        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"method\":\"download.list\""));

        let resp =
            JsonRpcResponse::success(Some(Value::from(1)), serde_json::json!({"status": "ok"}));
        let resp_json = serde_json::to_string(&resp).unwrap();
        assert!(resp_json.contains("\"result\":{\"status\":\"ok\"}"));
    }

    #[test]
    fn test_token_authentication() {
        let secret = "nexus_secret_token_12345";
        assert!(validate_auth_token(Some("Bearer nexus_secret_token_12345"), secret).is_ok());
        assert!(validate_auth_token(Some("Bearer wrong_token"), secret).is_err());
        assert!(validate_auth_token(None, secret).is_err());
    }
}
