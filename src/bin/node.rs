use std::{
    fs::exists,
    path::{PathBuf, absolute},
};

use anyhow::Context;
use blake3::Hash;
use colored::Colorize;
use ed25519_dalek::{
    SigningKey,
    pkcs8::{DecodePrivateKey, EncodePrivateKey, spki::der::pem::LineEnding},
};
use futures::StreamExt;
use gumdrop::Options;
use iroh::{Endpoint, NodeAddr, SecretKey, Watcher, protocol::Router};
use iroh_base::ticket::NodeTicket;
use iroh_blobs::{api::blobs::BlobStatus, net_protocol::Blobs, store::mem::MemStore};
use kreatilas::net::Kreatilas;
use nu_ansi_term::{Color, Style};
use rand_core::OsRng;
use reedline::{
    DefaultCompleter, DefaultPrompt, DefaultPromptSegment, Highlighter, Reedline, Signal,
    StyledText,
};
use tokio::{fs::try_exists, main};
use tracing_subscriber::{EnvFilter, fmt};

/// Node for a peer-to-peer data distribution network
#[derive(Debug, Clone, Options)]
struct Cli {
    #[options(help = "file to load your node's private key from, or save it to")]
    key_file: Option<PathBuf>,
}

#[main]
async fn main() -> anyhow::Result<()> {
    fmt().with_env_filter(EnvFilter::from_default_env()).init();

    let args = Cli::parse_args_default_or_exit();

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

    let endpoint = builder
        .discovery_n0()
        .bind()
        .await
        .context("failed to bind endpoint")?;
    let store = MemStore::new();
    let blobs = Blobs::new(&store, endpoint.clone(), None);
    let handler = Kreatilas::builder()
        .spawn(endpoint.clone(), &store)
        .await
        .context("failed to spawn handler")?;

    let blobs_client = store.blobs();
    let node_addr = endpoint.node_addr().initialized().await?;
    let node_id = node_addr.node_id;
    let node_ticket = NodeTicket::new(iroh_base::NodeAddr {
        node_id: iroh_base::PublicKey::try_from(node_id.as_bytes())
            .expect("I know this is a valid key"),
        relay_url: node_addr.relay_url.as_deref().cloned().map(Into::into),
        direct_addresses: node_addr.direct_addresses,
    });
    let router = Router::builder(endpoint)
        .accept(kreatilas::ALPN, handler.clone())
        .accept(iroh_blobs::ALPN, blobs.clone())
        .spawn();

    println!(
        "{}{}\n\nUse `{}` to connect\n",
        "You are node ".white(),
        node_id.fmt_short().green(),
        format!("peer {node_ticket}").green()
    );

    let commands = vec!["get".into(), "put".into(), "list".into(), "peer".into()];
    let completer = Box::new(DefaultCompleter::new_with_wordlen(commands.clone(), 2));
    let prompt = DefaultPrompt::new(DefaultPromptSegment::Empty, DefaultPromptSegment::Empty);
    let file_prompt = DefaultPrompt::new(
        DefaultPromptSegment::Basic(" path ".into()),
        DefaultPromptSegment::Empty,
    );
    let mut file_editor = Reedline::create();
    let mut line_editor = Reedline::create()
        .with_highlighter(Box::new(NodeHighlighter))
        .with_completer(completer);

    loop {
        let sig = line_editor.read_line(&prompt);
        match sig {
            Ok(Signal::Success(buffer)) => {
                let mut words = buffer.split(' ');
                let Some(cmd) = words.next() else {
                    error("please specify a command");
                    continue;
                };

                match cmd {
                    "get" => {
                        let Some(key) = words.next() else {
                            error("please specify a key to retrieve");
                            continue;
                        };

                        let key: Hash = match key.parse::<iroh_blobs::Hash>().map(Into::into) {
                            Ok(key) => key,
                            Err(err) => {
                                error(&format!("failed to parse key {}", format!("({err})").red()));
                                continue;
                            }
                        };

                        match handler.get(key).await {
                            Ok(Some(local_size)) => {
                                println!("Where should this {}-byte file go?", local_size);
                                let file_path = match file_editor.read_line(&file_prompt) {
                                    Ok(Signal::Success(buffer)) => PathBuf::from(buffer),
                                    Ok(Signal::CtrlC) => {
                                        continue;
                                    }
                                    Ok(Signal::CtrlD) => {
                                        println!("\n{}", "Exiting".white());
                                        break;
                                    }
                                    Err(err) => {
                                        error(&err.to_string());
                                        break;
                                    }
                                };

                                let file_path = match absolute(&file_path) {
                                    Ok(file_path) => file_path,
                                    Err(err) => {
                                        error(&format!(
                                            "Failed to get absolute path from `{}` {}",
                                            file_path.display(),
                                            format!("({err})").red(),
                                        ));
                                        continue;
                                    }
                                };

                                match blobs_client.export(key, file_path).await {
                                    Ok(size) => {
                                        println!("Wrote {size} bytes");
                                    }
                                    Err(err) => {
                                        error(&err.to_string());
                                        continue;
                                    }
                                }
                            }
                            Ok(None) => {
                                println!("Entry not found {}", ":(".cyan());
                            }
                            Err(err) => {
                                error(&err.to_string());
                            }
                        }
                    }
                    "put" => {
                        let Some(file_path) = words.next() else {
                            error("please specify a file to insert");
                            continue;
                        };

                        let file_path = match absolute(&file_path) {
                            Ok(file_path) => file_path,
                            Err(err) => {
                                error(&format!(
                                    "failed to get absolute path from `{}` {}",
                                    file_path,
                                    format!("({err})").red(),
                                ));
                                continue;
                            }
                        };

                        let file_hash = match blobs_client.add_path(file_path).await {
                            Ok(info) => {
                                println!("Added file to local store");
                                info.hash
                            }
                            Err(err) => {
                                error(&err.to_string());
                                continue;
                            }
                        };

                        if let Some("local") = words.next().as_deref() {
                            println!("\n{}{}", "Put key ".white(), file_hash.to_string().green());
                            continue;
                        }

                        match handler.put(file_hash.into()).await {
                            Ok(true) => {
                                println!(
                                    "\n{}{}",
                                    "Put key ".white(),
                                    file_hash.to_string().green()
                                );
                            }
                            Ok(false) => {
                                println!("Not enough peers {}", ":(".cyan());
                            }
                            Err(err) => {
                                error(&err.to_string());
                            }
                        }
                    }
                    "peer" => {
                        let Some(peer_id) = words.next() else {
                            error("please specify a peer ID");
                            continue;
                        };

                        let peer_id: NodeTicket = match peer_id.parse() {
                            Ok(key) => key,
                            Err(err) => {
                                error(&format!(
                                    "failed to parse peer ID {}",
                                    format!("({err})").red()
                                ));
                                continue;
                            }
                        };

                        let peer_id = iroh_base::NodeAddr::from(peer_id);

                        if let Err(err) = handler
                            .add_peer(NodeAddr::from_parts(
                                peer_id
                                    .node_id
                                    .as_bytes()
                                    .try_into()
                                    .expect("I know these keys are compatible"),
                                peer_id.relay_url.as_deref().cloned().map(Into::into),
                                peer_id.direct_addresses,
                            ))
                            .await
                        {
                            error(&format!("failed to add peer {}", format!("({err})").red()));
                            continue;
                        }

                        println!(
                            "\n{}{}",
                            "Added peer ".white(),
                            peer_id.node_id.fmt_short().green()
                        );
                    }
                    "list" => {
                        let mut listing = match blobs_client.list().stream().await {
                            Ok(listing) => listing,
                            Err(err) => {
                                error(&err.to_string());
                                continue;
                            }
                        };

                        while let Some(entry) = listing.next().await {
                            let entry = match entry {
                                Ok(entry) => entry,
                                Err(err) => {
                                    error(&err.to_string());
                                    continue;
                                }
                            };

                            println!(
                                "Blob {} {}",
                                entry.to_string().green(),
                                if let Ok(BlobStatus::Complete { size })
                                | Ok(BlobStatus::Partial { size: Some(size) }) =
                                    blobs_client.status(entry).await
                                {
                                    format!("({} bytes)", size).white()
                                } else {
                                    format!("(unknown size)").white()
                                },
                            );
                        }
                    }
                    "tags" => {
                        let mut listing = match store.tags().list().await {
                            Ok(listing) => listing,
                            Err(err) => {
                                error(&err.to_string());
                                continue;
                            }
                        };

                        while let Some(entry) = listing.next().await {
                            let entry = match entry {
                                Ok(entry) => entry,
                                Err(err) => {
                                    error(&err.to_string());
                                    continue;
                                }
                            };

                            println!(
                                "Tag {} {}",
                                entry.name.to_string().green(),
                                format!("({})", entry.hash).white()
                            );
                        }
                    }
                    invalid => {
                        error(&format!("`{invalid}` is not a valid command (yet)"));
                    }
                }
            }
            Ok(Signal::CtrlD) | Ok(Signal::CtrlC) => {
                println!("\n{}", "Exiting".white());
                break;
            }
            Err(err) => {
                error(&err.to_string());
                break;
            }
        }
    }

    router.shutdown().await?;

    Ok(())
}

fn error(msg: &str) {
    println!("{}{}", "Error: ".red(), msg.bright_red());
}

struct NodeHighlighter;

impl Highlighter for NodeHighlighter {
    fn highlight(&self, line: &str, _cursor: usize) -> StyledText {
        let mut words = line.split_inclusive(' ');

        let Some(cmd) = words.next() else {
            return StyledText::new();
        };

        let mut text = StyledText::new();

        if line.chars().next() == Some(' ') {
            text.push((Style::new(), " ".to_string()));
        }

        match cmd.trim() {
            "peer" => {
                text.push((Style::new().fg(Color::Green), cmd.to_string()));
                let Some(node_id) = words.next() else {
                    return text;
                };

                let Ok(_id) = node_id.trim().parse::<NodeTicket>() else {
                    text.push((Style::new().fg(Color::Red), node_id.to_string()));
                    for word in words {
                        text.push((Style::new().fg(Color::Red), word.to_string()));
                    }
                    return text;
                };

                text.push((Style::new().fg(Color::Green), node_id.to_string()));

                for word in words {
                    text.push((Style::new().fg(Color::Red), word.to_string()));
                }
                text
            }
            "get" => {
                text.push((Style::new().fg(Color::Green), cmd.to_string()));
                let Some(key) = words.next() else {
                    return text;
                };

                let Ok(_key) = key.trim().parse::<iroh_blobs::Hash>() else {
                    text.push((Style::new().fg(Color::Red), key.to_string()));
                    for word in words {
                        text.push((Style::new().fg(Color::Red), word.to_string()));
                    }
                    return text;
                };

                text.push((Style::new().fg(Color::Green), key.to_string()));

                for word in words {
                    text.push((Style::new().fg(Color::Red), word.to_string()));
                }
                text
            }
            "put" => {
                text.push((Style::new().fg(Color::Green), cmd.to_string()));
                let Some(path) = words.next() else {
                    return text;
                };

                if exists(path.trim()).is_ok_and(|exists| exists) {
                    text.push((Style::new().fg(Color::Green), path.to_string()));
                } else {
                    text.push((Style::new().fg(Color::Red), path.to_string()));
                }

                if let Some(maybe_local) = words.next().as_deref() {
                    if maybe_local.trim() == "local" {
                        text.push((Style::new().fg(Color::Yellow), maybe_local.to_string()));
                    } else {
                        text.push((Style::new().fg(Color::Red), maybe_local.to_string()));
                    }
                }

                for word in words {
                    text.push((Style::new().fg(Color::Red), word.to_string()));
                }
                text
            }
            "list" => {
                text.push((Style::new().fg(Color::Green), cmd.to_string()));

                for word in words {
                    text.push((Style::new().fg(Color::Red), word.to_string()));
                }
                text
            }
            cmd => {
                text.push((Style::new().fg(Color::Red), cmd.to_string()));
                for word in words {
                    text.push((Style::new().fg(Color::Red), word.to_string()));
                }
                text
            }
        }
    }
}
