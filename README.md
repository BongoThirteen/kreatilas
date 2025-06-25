
# Κρέατιλας

Kreatilas (Κρέατιλας) is networking software inspired by Ian Clarke's [Freenet](https://web.archive.org/web/20120316102156/https://freenetproject.org/papers/ddisrs.pdf) project.
It allows the distribution of files, addressed by their [BLAKE3](https://github.com/BLAKE3-team/BLAKE3) hashes, while maintaining deniability and making censorship more difficult.

Networking is powered by [Iroh](https://www.iroh.computer) and its Blobs protocol.
Provided is an implementation of a node in a peer-to-peer file-sharing network which enables the user to store and retrieve files of (theoretically) arbitrary size.

## Running

Assuming you have [Rust](https://www.rust-lang.org) installed, clone this repository and run

```bash
cargo run --bin node
```

It will start the node process, and you can enter commands on standard input.

Available commands are `get <hash>`, `put <file_path>`, `list`, and `peer <node_id>`.

## Features

This is highly experimental software.
Don't use it to leak classified documents.

- [x] Joining a network
- [x] Inserting and retrieving files
- [x] BLAKE3 verified streaming of file data
- [x] Deniability of both insertion and retrieval
- [ ] File encryption at rest
- [ ] Management of local data
- [ ] Node discovery options other than [number0](n0.computer)'s servers
- [ ] Automated peer discovery
