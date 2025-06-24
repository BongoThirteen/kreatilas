use std::io::{BufRead, Write, stdin, stdout};

use anyhow::Context;
use blake3::Hash;
use hex::encode;
use iroh::{Endpoint, NodeId, protocol::Router};
use iroh_blobs::net_protocol::Blobs;
use librorum::net::Handler;
use tokio::{main, signal::ctrl_c};
use tracing_subscriber::{EnvFilter, fmt};

#[main]
async fn main() -> anyhow::Result<()> {
    fmt().with_env_filter(EnvFilter::from_default_env()).init();

    let endpoint = Endpoint::builder().discovery_n0().bind().await?;
    let blobs = Blobs::memory().build(&endpoint);
    let handler = Handler::spawn(endpoint.clone(), &blobs).await?;

    let blobs_client = blobs.client();

    let node_id = endpoint.node_id();
    let router = Router::builder(endpoint)
        .accept(b"librorum/1", handler.clone())
        .accept(iroh_blobs::ALPN, blobs.clone())
        .spawn();

    println!("Node ID: {}", node_id);

    print!("> ");
    stdout().flush().context("failed to flush stdout")?;

    for line in stdin()
        .lock()
        .lines()
        .map_while(Result::ok)
        .take_while(|line| line != "quit")
    {
        let mut line = line.split(' ');
        let Some(cmd) = line.next() else {
            println!("Please specify a command and at least one argument");
            print!("> ");
            stdout().flush().context("failed to flush stdout")?;
            continue;
        };
        match cmd {
            "get" => {
                let Some(key) = line.next() else {
                    println!("Please specify a key to query for");
                    print!("> ");
                    stdout().flush().context("failed to flush stdout")?;
                    continue;
                };
                let Ok(key) = key.parse::<Hash>() else {
                    println!("Invalid key");
                    print!("> ");
                    stdout().flush().context("failed to flush stdout")?;
                    continue;
                };
                if handler.get(key).await? {
                    let data = blobs_client.read_to_bytes(key.into()).await?;
                    println!(
                        "Found: {:?}",
                        std::str::from_utf8(&data).context("invalid UTF-8")?
                    )
                } else {
                    println!("Not found");
                }
            }
            "store" => {
                let mut value = String::new();
                if let Some(first_word) = line.next() {
                    value.push_str(first_word);
                }
                for word in line {
                    value.push(' ');
                    value.push_str(word);
                }

                let added = blobs_client
                    .add_bytes(value)
                    .await
                    .context("failed to store bytes")?;

                println!("Key: {}", added.hash.to_hex());
                println!("Tag: {}", encode(&added.tag.0));
            }
            "put" => {
                let mut value = String::new();
                if let Some(first_word) = line.next() {
                    value.push_str(first_word);
                }
                for word in line {
                    value.push(' ');
                    value.push_str(word);
                }

                let added = blobs_client
                    .add_bytes(value)
                    .await
                    .context("failed to store bytes")?;
                if handler.put(added.hash.into()).await? {
                    println!("Key: {}", added.hash.to_hex());
                } else {
                    println!("Failed to insert key");
                }
            }
            "peer" => {
                let Some(node_id) = line.next() else {
                    println!("Please specify a node ID to peer with");
                    print!("> ");
                    stdout().flush().context("failed to flush stdout")?;
                    continue;
                };
                let node_id: NodeId = node_id.parse().context("invalid node ID")?;
                handler.add_peer(node_id).await;
            }
            invalid => {
                println!("`{invalid}` is not one of `get` and `put`");
            }
        }
        print!("> ");
        stdout().flush().context("failed to flush stdout")?;
    }

    ctrl_c().await?;

    router.shutdown().await?;

    Ok(())
}
