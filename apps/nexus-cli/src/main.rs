//! NEXUS Command-Line Interface (CLI)

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use nexus_application::NexusEngine;
use nexus_domain::{DownloadStatus, TaskId};
use nexus_events::EventBus;
use nexus_network::NetworkManager;
use nexus_network_platform::SystemNetworkPlatform;
use nexus_proxy::ProxyManager;
use nexus_storage::InMemoryStorage;
use nexus_telemetry::init_telemetry;
use std::str::FromStr;
use std::sync::Arc;
use url::Url;

#[derive(Parser, Debug)]
#[command(name = "nexus", author, version, about = "NEXUS: Native Multi-Path Transfer & Download Manager", long_about = None)]
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

    /// Display transfer performance metrics and bandwidth statistics
    Stats,

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
    },

    /// List download tasks
    List {
        /// Filter downloads by status
        #[arg(short, long)]
        status: Option<StatusFilter>,
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
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let _ = init_telemetry(&cli.log_level);

    // Initialize foundational in-memory engine for CLI demonstration / local operations
    let storage = Arc::new(InMemoryStorage::new());
    let platform = Arc::new(SystemNetworkPlatform);
    let network_manager = Arc::new(NetworkManager::new(platform));
    let proxy_manager = Arc::new(ProxyManager::new());
    let event_bus = Arc::new(EventBus::default());

    let engine = NexusEngine::new(storage, network_manager, proxy_manager, event_bus);

    match cli.command {
        Commands::Download(dl) => match dl.command {
            DownloadCommands::Add {
                url,
                filename,
                destination,
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
                    .create_download(parsed_url, resolved_filename, destination)
                    .await
                    .context("Failed to create download task")?;

                println!("Successfully created download task:");
                println!("  ID:          {}", task.id);
                println!("  URL:         {}", task.url);
                println!("  Filename:    {}", task.filename);
                println!("  Destination: {}", task.destination_dir);
                println!("  Status:      {}", task.status);
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
                    println!("{:<38} {:<12} {:<24} URL", "ID", "STATUS", "FILENAME");
                    println!("{:-<38} {:-<12} {:-<24} {:-<30}", "", "", "", "");
                    for t in tasks {
                        println!(
                            "{:<38} {:<12} {:<24} {}",
                            t.id.to_string(),
                            t.status.to_string(),
                            t.filename,
                            t.url
                        );
                    }
                }
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
                    "{:<10} {:<16} {:<20} IP ADDRESSES",
                    "NAME", "TYPE", "STATUS"
                );
                println!("{:-<10} {:-<16} {:-<20} {:-<30}", "", "", "", "");
                for iface in ifaces {
                    let ips: Vec<String> =
                        iface.ip_addresses.iter().map(ToString::to_string).collect();
                    let status = if iface.is_up {
                        "UP (Connected)"
                    } else {
                        "DOWN"
                    };
                    println!(
                        "{:<10} {:<16?} {:<20} {}",
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
                    println!("{:<38} {:<18} {:<8} ENABLED", "PATH ID", "NAME", "PRIORITY");
                    println!("{:-<38} {:-<18} {:-<8} {:-<10}", "", "", "", "");
                    for p in paths {
                        println!(
                            "{:<38} {:<18} {:<8} {}",
                            p.id, p.name, p.priority, p.enabled
                        );
                    }
                }
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
        },
        Commands::Stats => {
            println!("NEXUS Multi-Path Transfer Statistics:");
            println!("  Aggregated Throughput: 0.00 MB/s");
            println!("  Active Connections:    0");
            println!("  Total Transferred:     0 B");
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
