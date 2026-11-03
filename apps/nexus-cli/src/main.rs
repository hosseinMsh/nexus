//! NEXUS Command-Line Interface (CLI)

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use nexus_application::NexusEngine;
use nexus_automation::{QueueManager, RuleEngine};
use nexus_domain::{DownloadStatus, InterfaceId, PathLimits, ProxyProfile, ProxyType, TaskId};
use nexus_download::DownloadEngine;
use nexus_events::EventBus;
use nexus_ipc::{JsonRpcDispatcher, JsonRpcRequest};
use nexus_network::NetworkManager;
use nexus_network_platform::SystemNetworkPlatform;
use nexus_path_scheduler::{AdaptivePathScheduler, PathMetricsTracker};
use nexus_protocols::ProtocolManager;
use nexus_proxy::ProxyManager;
use nexus_storage::InMemoryStorage;
use nexus_telemetry::init_telemetry;
use nexus_transport::{ConnectionOptions, HttpTransportClient};
use serde_json::{json, Value};
use std::str::FromStr;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tracing::info;
use url::Url;

#[derive(Parser, Debug)]
#[command(
    name = "nexus",
    author,
    version,
    about = "NEXUS: Native Multi-Path Transfer & Download Manager",
    long_about = None
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, default_value = "info", global = true)]
    log_level: String,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Manage download tasks
    Download(DownloadArgs),

    /// Inspect network interfaces and configured multi-path routes
    Network(NetworkArgs),

    /// Manage proxy profiles
    Proxy(ProxyArgs),

    /// Inspect and configure download priority queue
    Queue(QueueArgs),

    /// List configured automation rules
    Rules,

    /// Display transfer performance metrics and bandwidth statistics
    Stats,

    /// Start a headless JSON-RPC daemon service
    Daemon(DaemonArgs),

    /// Execute a raw JSON-RPC call against a running NEXUS daemon
    Rpc(RpcArgs),

    /// Print application runtime and system information
    Info,
}

#[derive(Args, Debug)]
struct DownloadArgs {
    #[command(subcommand)]
    command: DownloadCommands,
}

#[derive(Subcommand, Debug)]
enum DownloadCommands {
    /// Add a new download task
    Add {
        /// Target URL to download
        url: String,

        /// Destination filename (auto-detected if omitted)
        #[arg(short, long)]
        filename: Option<String>,

        /// Destination directory
        #[arg(short, long, default_value = ".")]
        destination: String,

        /// Priority level (0=Urgent, 1=High, 2=Normal, 3=Low)
        #[arg(short, long, default_value = "2")]
        priority: u32,

        /// Automatically begin transfer immediately
        #[arg(short, long)]
        start: bool,
    },

    /// Start or resume transfer execution for a task
    Start {
        /// Task ID (UUID)
        id: String,
    },

    /// List download tasks
    List {
        /// Filter downloads by status
        #[arg(short, long)]
        status: Option<StatusFilter>,
    },

    /// Inspect detailed information for a specific download task
    Inspect {
        /// Task ID (UUID)
        id: String,
    },

    /// Pause an active download
    Pause {
        /// Task ID (UUID)
        id: String,
    },

    /// Resume a paused download
    Resume {
        /// Task ID (UUID)
        id: String,
    },

    /// Cancel a download
    Cancel {
        /// Task ID (UUID)
        id: String,
    },
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum StatusFilter {
    Created,
    Queued,
    Preparing,
    Downloading,
    Paused,
    Completed,
    Failed,
    Cancelled,
}

impl From<StatusFilter> for DownloadStatus {
    fn from(f: StatusFilter) -> Self {
        match f {
            StatusFilter::Created => Self::Created,
            StatusFilter::Queued => Self::Queued,
            StatusFilter::Preparing => Self::Preparing,
            StatusFilter::Downloading => Self::Downloading,
            StatusFilter::Paused => Self::Paused,
            StatusFilter::Completed => Self::Completed,
            StatusFilter::Failed => Self::Failed,
            StatusFilter::Cancelled => Self::Cancelled,
        }
    }
}

#[derive(Args, Debug)]
struct NetworkArgs {
    #[command(subcommand)]
    command: NetworkCommands,
}

#[derive(Subcommand, Debug)]
enum NetworkCommands {
    /// List detected physical and logical network interfaces
    Interfaces,

    /// List configured network paths
    Paths,

    /// Add a new network path bound to an interface
    AddPath {
        /// Friendly name for the route
        #[arg(short, long)]
        name: String,

        /// Network Interface ID (UUID)
        #[arg(short, long)]
        interface_id: String,

        /// Path priority ranking (higher executes first)
        #[arg(short, long, default_value = "1")]
        priority: u8,
    },
}

#[derive(Args, Debug)]
struct ProxyArgs {
    #[command(subcommand)]
    command: ProxyCommands,
}

#[derive(Subcommand, Debug)]
enum ProxyCommands {
    /// List configured proxy profiles
    List,

    /// Add a new proxy profile
    Add {
        /// Profile name
        #[arg(short, long)]
        name: String,

        /// Proxy protocol (socks5, http)
        #[arg(short = 't', long, default_value = "socks5")]
        proxy_type: String,

        /// Hostname or IP address
        #[arg(long)]
        host: String,

        /// Port number
        #[arg(short, long)]
        port: u16,
    },
}

#[derive(Args, Debug)]
struct QueueArgs {
    #[command(subcommand)]
    command: QueueCommands,
}

#[derive(Subcommand, Debug)]
enum QueueCommands {
    /// Display queue concurrency status and pending slots
    Status,

    /// Set maximum concurrent active transfers
    SetLimit {
        /// Maximum concurrent transfers
        limit: usize,
    },
}

#[derive(Args, Debug)]
struct DaemonArgs {
    /// Port number to listen on
    #[arg(short, long, default_value = "9876")]
    port: u16,

    /// Bearer session authentication token
    #[arg(short, long, default_value = "nexus_secret_token")]
    token: String,
}

#[derive(Args, Debug)]
struct RpcArgs {
    /// JSON-RPC 2.0 method name
    #[arg(short, long)]
    method: String,

    /// Parameters as raw JSON string
    #[arg(short, long)]
    params: Option<String>,

    /// Daemon port
    #[arg(short, long, default_value = "9876")]
    port: u16,

    /// Bearer authentication token
    #[arg(short, long, default_value = "nexus_secret_token")]
    token: String,
}

fn build_local_engine() -> Result<Arc<NexusEngine>> {
    let storage = Arc::new(InMemoryStorage::new());
    let platform = Arc::new(SystemNetworkPlatform);
    let network_manager = Arc::new(NetworkManager::new(platform));
    let proxy_manager = Arc::new(ProxyManager::new());
    let event_bus = Arc::new(EventBus::default());
    let protocol_manager = Arc::new(ProtocolManager::new());
    let queue_manager = Arc::new(QueueManager::new(4));
    let rule_engine = Arc::new(RuleEngine::new());
    let path_scheduler = Arc::new(AdaptivePathScheduler::default());
    let metrics_tracker = Arc::new(PathMetricsTracker::new(3));

    let transport = Arc::new(
        HttpTransportClient::new(&ConnectionOptions::default())
            .context("Failed to initialize HTTP transport")?,
    );
    let download_engine = Arc::new(DownloadEngine::new(
        storage.clone(),
        storage.clone(),
        transport,
        event_bus.clone(),
    ));

    Ok(Arc::new(NexusEngine::new(
        storage.clone(),
        storage.clone(),
        network_manager,
        proxy_manager,
        protocol_manager,
        queue_manager,
        rule_engine,
        path_scheduler,
        metrics_tracker,
        download_engine,
        event_bus,
    )))
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let _ = init_telemetry(&cli.log_level);

    let engine = build_local_engine()?;

    match cli.command {
        Commands::Download(dl) => match dl.command {
            DownloadCommands::Add {
                url,
                filename,
                destination,
                priority,
                start,
            } => {
                let parsed_url = Url::parse(&url).context("Invalid download URL")?;
                let resolved_filename = filename.unwrap_or_else(|| {
                    parsed_url
                        .path_segments()
                        .and_then(|mut s| s.next_back())
                        .filter(|s| !s.is_empty())
                        .unwrap_or("download.file")
                        .to_string()
                });

                let task = engine
                    .create_download_with_options(
                        parsed_url,
                        resolved_filename,
                        destination,
                        Some(priority),
                        None,
                    )
                    .await
                    .context("Failed to create download task")?;

                println!("Successfully created download task:");
                println!("  ID:          {}", task.id);
                println!("  URL:         {}", task.url);
                println!("  Filename:    {}", task.filename);
                println!("  Destination: {}", task.destination_dir);
                println!("  Category:    {:?}", task.category);
                println!("  Priority:    P{}", priority);
                println!("  Status:      {}", task.status);

                if start {
                    engine
                        .start_download(&task.id)
                        .await
                        .context("Failed to start download task")?;
                    println!("Transfer started asynchronously.");
                }
            }
            DownloadCommands::Start { id } => {
                let task_id = TaskId::from_str(&id).context("Invalid Task UUID")?;
                engine
                    .start_download(&task_id)
                    .await
                    .context("Failed to start download")?;
                println!("Task {task_id} started.");
            }
            DownloadCommands::List { status } => {
                let domain_status = status.map(Into::into);
                let tasks = engine
                    .list_downloads(domain_status)
                    .await
                    .context("Failed to list downloads")?;

                if tasks.is_empty() {
                    println!("No downloads found matching the criteria.");
                } else {
                    println!(
                        "{:<38} {:<12} {:<24} {:<12} URL",
                        "ID", "STATUS", "FILENAME", "PROGRESS"
                    );
                    println!(
                        "{:-<38} {:-<12} {:-<24} {:-<12} {:-<25}",
                        "", "", "", "", ""
                    );
                    for t in tasks {
                        let prog = t
                            .progress_fraction()
                            .map(|f| format!("{:.1}%", f * 100.0))
                            .unwrap_or_else(|| "-".to_string());
                        println!(
                            "{:<38} {:<12} {:<24} {:<12} {}",
                            t.id.to_string(),
                            t.status.to_string(),
                            t.filename,
                            prog,
                            t.url
                        );
                    }
                }
            }
            DownloadCommands::Inspect { id } => {
                let task_id = TaskId::from_str(&id).context("Invalid Task UUID")?;
                let task = engine
                    .get_download(&task_id)
                    .await
                    .context("Failed to query download")?
                    .ok_or_else(|| anyhow::anyhow!("Task not found"))?;

                println!("Download Task Details:");
                println!("  ID:               {}", task.id);
                println!("  URL:              {}", task.url);
                println!("  Filename:         {}", task.filename);
                println!("  Destination:      {}", task.destination_dir);
                println!("  Status:           {}", task.status);
                println!("  Category:         {:?}", task.category);
                println!("  Downloaded Bytes: {}", task.downloaded_size.bytes());
                println!(
                    "  Total Bytes:      {}",
                    task.total_size
                        .map(|s| s.bytes().to_string())
                        .unwrap_or_else(|| "Unknown".to_string())
                );
                println!("  Max Connections:  {}", task.policy.max_connections);
                println!("  Created:          {}", task.created_at);
                println!("  Updated:          {}", task.updated_at);
            }
            DownloadCommands::Pause { id } => {
                let task_id = TaskId::from_str(&id).context("Invalid Task UUID")?;
                engine
                    .pause_download(&task_id)
                    .await
                    .context("Failed to pause download")?;
                println!("Task {task_id} successfully paused.");
            }
            DownloadCommands::Resume { id } => {
                let task_id = TaskId::from_str(&id).context("Invalid Task UUID")?;
                engine
                    .resume_download(&task_id)
                    .await
                    .context("Failed to resume download")?;
                println!("Task {task_id} successfully resumed.");
            }
            DownloadCommands::Cancel { id } => {
                let task_id = TaskId::from_str(&id).context("Invalid Task UUID")?;
                engine
                    .cancel_download(&task_id)
                    .await
                    .context("Failed to cancel download")?;
                println!("Task {task_id} successfully cancelled.");
            }
        },
        Commands::Network(net) => match net.command {
            NetworkCommands::Interfaces => {
                let ifaces = engine
                    .network_manager()
                    .list_interfaces()
                    .context("Failed to list network interfaces")?;

                println!(
                    "{:<38} {:<10} {:<16} {:<20} IP ADDRESSES",
                    "ID", "NAME", "TYPE", "STATUS"
                );
                println!(
                    "{:-<38} {:-<10} {:-<16} {:-<20} {:-<25}",
                    "", "", "", "", ""
                );
                for iface in ifaces {
                    let ips: Vec<String> =
                        iface.ip_addresses.iter().map(ToString::to_string).collect();
                    let status = if iface.is_up {
                        "UP (Connected)"
                    } else {
                        "DOWN"
                    };
                    println!(
                        "{:<38} {:<10} {:<16?} {:<20} {}",
                        iface.id,
                        iface.system_name,
                        iface.interface_type,
                        status,
                        ips.join(", ")
                    );
                }
            }
            NetworkCommands::Paths => {
                let paths = engine.network_manager().list_paths().await;
                if paths.is_empty() {
                    println!("No network paths currently configured.");
                } else {
                    println!(
                        "{:<38} {:<18} {:<8} {:<8} PROXY",
                        "PATH ID", "NAME", "PRIORITY", "ENABLED"
                    );
                    println!("{:-<38} {:-<18} {:-<8} {:-<8} {:-<10}", "", "", "", "", "");
                    for p in paths {
                        let proxy_str = p
                            .proxy_id
                            .map(|pid| pid.to_string())
                            .unwrap_or_else(|| "Direct (None)".to_string());
                        println!(
                            "{:<38} {:<18} {:<8} {:<8} {}",
                            p.id, p.name, p.priority, p.enabled, proxy_str
                        );
                    }
                }
            }
            NetworkCommands::AddPath {
                name,
                interface_id,
                priority,
            } => {
                let iface_id = InterfaceId::from_str(&interface_id)
                    .context("Invalid Interface UUID format")?;
                let path = engine
                    .network_manager()
                    .create_path(name, iface_id, PathLimits::default(), priority)
                    .await
                    .context("Failed to create network path")?;
                println!("Created network path: {} (ID: {})", path.name, path.id);
            }
        },
        Commands::Proxy(proxy) => match proxy.command {
            ProxyCommands::List => {
                let profiles = engine.proxy_manager().list_profiles().await;
                if profiles.is_empty() {
                    println!("No proxy profiles configured.");
                } else {
                    println!("{:<38} {:<20} {:<10} HOST:PORT", "ID", "NAME", "TYPE");
                    println!("{:-<38} {:-<20} {:-<10} {:-<25}", "", "", "", "");
                    for p in profiles {
                        println!(
                            "{:<38} {:<20} {:<10?} {}:{}",
                            p.id, p.name, p.proxy_type, p.host, p.port
                        );
                    }
                }
            }
            ProxyCommands::Add {
                name,
                proxy_type,
                host,
                port,
            } => {
                let pt = match proxy_type.to_lowercase().as_str() {
                    "socks5" => ProxyType::Socks5,
                    "http" => ProxyType::Http,
                    "https" => ProxyType::Https,
                    "socks4" => ProxyType::Socks4,
                    _ => anyhow::bail!("Unsupported proxy type '{proxy_type}'"),
                };
                let profile = ProxyProfile {
                    id: nexus_domain::ProxyId::new(),
                    name,
                    proxy_type: pt,
                    host,
                    port,
                    credential_reference: None,
                    enabled: true,
                    dns_policy: nexus_domain::DnsPolicy::Remote,
                };
                let _ = engine.proxy_manager().register_profile(profile).await;
                println!("Proxy profile registered successfully.");
            }
        },
        Commands::Queue(q) => match q.command {
            QueueCommands::Status => {
                let active = engine.queue_manager().active_count().await;
                let pending = engine.queue_manager().pending_count().await;
                println!("NEXUS Download Queue Status:");
                println!("  Active Transfers:   {}", active);
                println!("  Pending In Queue:   {}", pending);
            }
            QueueCommands::SetLimit { limit } => {
                engine.queue_manager().set_max_active_tasks(limit).await;
                println!("Updated active transfer concurrency limit to {limit}.");
            }
        },
        Commands::Rules => {
            let rules = engine.rule_engine().list_rules();
            if rules.is_empty() {
                println!("No automation rules registered.");
            } else {
                println!("{:<38} {:<24} {:<10} ENABLED", "ID", "NAME", "PRIORITY");
                println!("{:-<38} {:-<24} {:-<10} {:-<8}", "", "", "", "");
                for r in rules {
                    println!(
                        "{:<38} {:<24} {:<10} {}",
                        r.id, r.name, r.priority, r.enabled
                    );
                }
            }
        }
        Commands::Stats => {
            let s = engine.stats().await;
            println!("NEXUS Multi-Path Transfer Statistics:");
            println!("  Active Downloads:        {}", s.active_downloads);
            println!("  Queued Downloads:        {}", s.queued_downloads);
            println!("  Paused Downloads:        {}", s.paused_downloads);
            println!("  Completed Downloads:     {}", s.completed_downloads);
            println!("  Failed Downloads:        {}", s.failed_downloads);
            println!(
                "  Total Transferred:       {} bytes",
                s.total_downloaded_bytes
            );
            println!(
                "  Aggregated Throughput:   {:.2} KB/s",
                s.aggregate_throughput_bps as f64 / 1024.0
            );
            println!("  Active Network Paths:    {}", s.active_network_paths);
            println!("  Active Connections:      {}", s.active_connections);
        }
        Commands::Daemon(daemon) => {
            let addr = format!("127.0.0.1:{}", daemon.port);
            let listener = TcpListener::bind(&addr)
                .await
                .context("Failed to bind daemon TCP listener")?;
            println!("NEXUS Headless Daemon running at tcp://{addr}");
            println!("Authentication: Bearer {}", daemon.token);

            let dispatcher = Arc::new(JsonRpcDispatcher::new(daemon.token.clone(), engine.clone()));

            loop {
                let (socket, remote_addr) = listener.accept().await?;
                info!(remote = %remote_addr, "Accepted client connection");
                let disp = dispatcher.clone();

                tokio::spawn(async move {
                    let (reader, mut writer) = socket.into_split();
                    let mut lines = BufReader::new(reader).lines();

                    while let Ok(Some(line)) = lines.next_line().await {
                        if line.trim().is_empty() {
                            continue;
                        }

                        // Try parsing JSON-RPC request line
                        let auth_header = Some(format!("Bearer {}", disp.session_token()));
                        if let Ok(req) = serde_json::from_str::<JsonRpcRequest>(&line) {
                            let resp = disp.dispatch(auth_header.as_deref(), req).await;
                            if let Ok(serialized) = serde_json::to_string(&resp) {
                                let _ = writer.write_all(serialized.as_bytes()).await;
                                let _ = writer.write_all(b"\n").await;
                            }
                        } else {
                            let err_resp = json!({
                                "jsonrpc": "2.0",
                                "id": null,
                                "error": { "code": -32700, "message": "Parse error" }
                            });
                            let _ = writer.write_all(err_resp.to_string().as_bytes()).await;
                            let _ = writer.write_all(b"\n").await;
                        }
                    }
                });
            }
        }
        Commands::Rpc(rpc) => {
            let addr = format!("127.0.0.1:{}", rpc.port);
            let stream = TcpStream::connect(&addr)
                .await
                .context("Failed to connect to NEXUS daemon")?;
            let (reader, mut writer) = stream.into_split();

            let params_val: Option<Value> = match rpc.params {
                Some(p) => Some(serde_json::from_str(&p).context("Invalid JSON in --params")?),
                None => None,
            };

            let req = json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": rpc.method,
                "params": params_val
            });

            writer.write_all(req.to_string().as_bytes()).await?;
            writer.write_all(b"\n").await?;

            let mut lines = BufReader::new(reader).lines();
            if let Some(resp_line) = lines.next_line().await? {
                let parsed: Value =
                    serde_json::from_str(&resp_line).context("Invalid response JSON")?;
                println!("{}", serde_json::to_string_pretty(&parsed)?);
            }
        }
        Commands::Info => {
            println!("NEXUS — Native Multi-Path Transfer & Download Manager");
            println!("Version:         {}", env!("CARGO_PKG_VERSION"));
            println!("License:         MIT");
            println!("Target OS:       {}", std::env::consts::OS);
            println!("Architecture:    {}", std::env::consts::ARCH);
            println!("Rust Edition:    2021");
            println!("Status:          Operational");
        }
    }

    Ok(())
}
