# Local JSON-RPC 2.0 API Specification

NEXUS exposes a high-performance local control API adhering strictly to the [JSON-RPC 2.0 Specification](https://www.jsonrpc.org/specification).

---

## 1. Transport & Authentication

- **Transport:** HTTP on loopback (`http://127.0.0.1:<port>/rpc`) or OS IPC socket.
- **Header:** `Authorization: Bearer <unpredictable-session-token>`

---

## 2. API Methods

### System
- `system.get_info`: Returns version, runtime status, and active platform.

### Download Management
- `download.create`: Initiates a new download task with destination path, URL, custom headers, and path policy.
- `download.list`: Lists download tasks filtered by status (`active`, `paused`, `completed`, `failed`).
- `download.get`: Retrieves detailed task status including segment progress, assigned paths, and speed.
- `download.pause`: Pauses an ongoing download.
- `download.resume`: Resumes a paused or interrupted download.
- `download.cancel`: Cancels an active download and cleans up temporary resources.
- `download.retry`: Retries a failed download.
- `download.delete`: Removes download record and optionally removes associated downloaded files.

### Network & Paths
- `network.list_interfaces`: Enumerates detected physical and logical network interfaces.
- `network.list_paths`: Returns configured network paths.
- `network.create_path`: Registers a new network path with specific interface, proxy, and priorities.
- `network.update_path`: Updates path bandwidth limits, priority, or enabled status.
- `network.test_path`: Executes an end-to-end connectivity and latency check for a path.

### Proxy Management
- `proxy.list`: Lists configured proxy profiles.
- `proxy.create`: Registers a new proxy configuration.

### Queue & Automation
- `queue.list`: Lists queued download tasks.
- `queue.start`: Starts automated sequential or parallel queue processing.
- `queue.pause`: Pauses queue execution.
- `statistics.get`: Returns aggregated transfer throughput, active connection counts, and historical usage.
