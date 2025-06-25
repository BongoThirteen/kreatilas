use std::path::{PathBuf, absolute};

use anyhow::Context;
use blake3::Hash;
use clap::Parser;
use ed25519_dalek::{
    SigningKey,
    pkcs8::{DecodePrivateKey, EncodePrivateKey, spki::der::pem::LineEnding},
};
use futures::StreamExt;
use iroh::{Endpoint, NodeId, SecretKey, protocol::Router};
use iroh_blobs::{
    export::ExportProgress,
    net_protocol::Blobs,
    provider::AddProgress,
    rpc::client::blobs::WrapOption,
    store::{ExportFormat, ExportMode},
    util::SetTagOption,
};
use librorum::net::Handler;
use rand_core::OsRng;
use reedline::{
    DefaultCompleter, DefaultPrompt, DefaultPromptSegment, ExampleHighlighter, Reedline, Signal,
};
use shlex::Shlex;
use tokio::{fs::try_exists, main};
use tracing_subscriber::{EnvFilter, fmt};

/// Node for a peer-to-peer data distribution network
#[derive(Debug, Clone, Parser)]
#[clap(version, author)]
struct Cli {
    #[clap(short, long)]
    key_file: Option<PathBuf>,
}

#[main]
async fn main() -> anyhow::Result<()> {
    fmt().with_env_filter(EnvFilter::from_default_env()).init();

    let args = Cli::parse();

    let mut builder = Endpoint::builder();

    if let Some(key_file) = args.key_file {
        if !try_exists(&key_file)
            .await
            .context("failed to check for existing key file")?
        {
            let key = SecretKey::generate(OsRng);
            key.secret()
                .write_pkcs8_pem_file(&key_file, LineEnding::LF)
                .with_context(|| {
                    format!(
                        "failed to write private key to provided file `{}`",
                        key_file.display()
                    )
                })?;
            builder = builder.secret_key(key);
        } else {
            let key =
                SecretKey::from(SigningKey::read_pkcs8_pem_file(&key_file).with_context(|| {
                    format!(
                        "failed to read private key from provided file `{}`",
                        key_file.display()
                    )
                })?);
            builder = builder.secret_key(key);
        }
    }

    let endpoint = builder.discovery_n0().bind().await?;
    let blobs = Blobs::memory().build(&endpoint);
    let handler = Handler::spawn(endpoint.clone(), &blobs).await?;

    let blobs_client = blobs.client();

    let node_id = endpoint.node_id();
    let router = Router::builder(endpoint)
        .accept(b"librorum/1", handler.clone())
        .accept(iroh_blobs::ALPN, blobs.clone())
        .spawn();

    println!("Node ID: {}", node_id);

    let commands = vec!["get".into(), "put".into(), "list".into(), "peer".into()];
    let completer = Box::new(DefaultCompleter::new_with_wordlen(commands.clone(), 2));
    let prompt = DefaultPrompt::new(DefaultPromptSegment::Empty, DefaultPromptSegment::Empty);
    let file_prompt = DefaultPrompt::new(
        DefaultPromptSegment::Basic("File path".into()),
        DefaultPromptSegment::Empty,
    );
    let mut file_editor = Reedline::create();
    let mut line_editor = Reedline::create()
        .with_highlighter(Box::new(ExampleHighlighter::new(commands)))
        .with_completer(completer);

    loop {
        let sig = line_editor.read_line(&prompt);
        match sig {
            Ok(Signal::Success(buffer)) => {
                let mut words = Shlex::new(&buffer);
                let Some(cmd) = words.next() else {
                    println!("Error: please specify a command");
                    continue;
                };

                match cmd.as_str() {
                    "get" => {
                        let Some(key) = words.next() else {
                            println!("Error: please specify a key to retrieve");
                            continue;
                        };

                        let key: Hash = match key.parse() {
                            Ok(key) => key,
                            Err(err) => {
                                println!("Error: failed to parse key ({err:#})");
                                continue;
                            }
                        };

                        match handler.get(key).await {
                            Ok(true) => {
                                let file_path = match file_editor.read_line(&file_prompt) {
                                    Ok(Signal::Success(buffer)) => PathBuf::from(buffer),
                                    Ok(Signal::CtrlC) => {
                                        println!("\nExiting.");
                                        continue;
                                    }
                                    Ok(Signal::CtrlD) => {
                                        break;
                                    }
                                    Err(err) => {
                                        println!("Error reading input: {err:#}");
                                        break;
                                    }
                                };

                                let file_path = match absolute(&file_path) {
                                    Ok(file_path) => file_path,
                                    Err(err) => {
                                        println!(
                                            "Failed to get absolute path from `{}` ({err:#})",
                                            file_path.display(),
                                        );
                                        continue;
                                    }
                                };

                                let mut progress = match blobs_client
                                    .export(
                                        key.into(),
                                        file_path,
                                        ExportFormat::Blob,
                                        ExportMode::Copy,
                                    )
                                    .await
                                {
                                    Ok(progress) => progress,
                                    Err(err) => {
                                        println!("Error: {err:#}");
                                        continue;
                                    }
                                };

                                while let Some(prog) = progress.next().await {
                                    let prog = match prog {
                                        Ok(prog) => prog,
                                        Err(err) => {
                                            println!("Error: {err:#}");
                                            continue;
                                        }
                                    };

                                    match prog {
                                        ExportProgress::Found { size, .. } => {
                                            println!("Found file ({} bytes)", size.value());
                                        }
                                        ExportProgress::Progress { offset, .. } => {
                                            println!("Exported {} bytes", offset);
                                        }
                                        ExportProgress::Done { .. } => {
                                            println!("Done");
                                        }
                                        ExportProgress::AllDone => {
                                            println!("All done");
                                        }
                                        ExportProgress::Abort(err) => {
                                            println!("Error: {err:#}");
                                        }
                                    }
                                }
                            }
                            Ok(false) => {
                                println!("Entry not found :(");
                            }
                            Err(err) => {
                                println!("Handler error: {err:#}");
                            }
                        }
                    }
                    "put" => {
                        let Some(file_path) = words.next() else {
                            println!("Error: please specify a key to retrieve");
                            continue;
                        };

                        let file_path = match absolute(&file_path) {
                            Ok(file_path) => file_path,
                            Err(err) => {
                                println!(
                                    "Failed to get absolute path from `{}` ({err:#})",
                                    file_path,
                                );
                                continue;
                            }
                        };

                        let mut progress = match blobs_client
                            .add_from_path(file_path, false, SetTagOption::Auto, WrapOption::NoWrap)
                            .await
                        {
                            Ok(progress) => progress,
                            Err(err) => {
                                println!("Error: {err:#}");
                                continue;
                            }
                        };

                        let mut file_hash = None;

                        while let Some(prog) = progress.next().await {
                            let prog = match prog {
                                Ok(prog) => prog,
                                Err(err) => {
                                    println!("Error: {err:#}");
                                    continue;
                                }
                            };

                            match prog {
                                AddProgress::Found { size, .. } => {
                                    println!("Found file ({} bytes)", size);
                                }
                                AddProgress::Progress { offset, .. } => {
                                    println!("Added {} bytes", offset);
                                }
                                AddProgress::Done { .. } => {
                                    println!("Done");
                                }
                                AddProgress::AllDone { hash, .. } => {
                                    println!("All done");
                                    file_hash = Some(hash);
                                    break;
                                }
                                AddProgress::Abort(err) => {
                                    println!("Error: {err:#}");
                                }
                            }
                        }

                        let Some(file_hash) = file_hash else {
                            continue;
                        };

                        if let Some("local") = words.next().as_deref() {
                            println!("\nKey: {}", file_hash.to_hex());
                            continue;
                        }

                        match handler.put(file_hash.into()).await {
                            Ok(true) => {
                                println!("\nKey: {}", file_hash.to_hex());
                            }
                            Ok(false) => {
                                println!("Not enough peers");
                            }
                            Err(err) => {
                                println!("Hanlder error: {err:#}");
                            }
                        }
                    }
                    "peer" => {
                        let Some(peer_id) = words.next() else {
                            println!("Error: please specify a peer ID");
                            continue;
                        };

                        let peer_id: NodeId = match peer_id.parse() {
                            Ok(key) => key,
                            Err(err) => {
                                println!("Error: failed to parse peer ID ({err:#})");
                                continue;
                            }
                        };

                        handler.add_peer(peer_id).await;
                    }
                    "list" => {
                        let mut listing = match blobs_client.list().await {
                            Ok(listing) => listing,
                            Err(err) => {
                                println!("Error: {err:#}");
                                continue;
                            }
                        };

                        while let Some(entry) = listing.next().await {
                            let entry = match entry {
                                Ok(entry) => entry,
                                Err(err) => {
                                    println!("Error: {err:#}");
                                    continue;
                                }
                            };

                            println!("Blob {} ({} bytes)", entry.hash.to_hex(), entry.size);
                        }
                    }
                    invalid => {
                        println!("`{invalid}` is not a valid command (yet)");
                    }
                }
            }
            Ok(Signal::CtrlD) | Ok(Signal::CtrlC) => {
                println!("\nExiting.");
                break;
            }
            Err(err) => {
                println!("Error reading input: {err:#}");
                break;
            }
        }
    }

    router.shutdown().await?;

    Ok(())
}
