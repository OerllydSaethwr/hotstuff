#![allow(unused)]
use crate::config::Export as _;
use crate::config::{Committee, ConfigError, Parameters, Secret};
use mempool::batch_maker::Batch;

use consensus::{Block, Consensus};
use crypto::{SignatureService, Digest};
use log::{info, debug, warn};
use mempool::{MempoolMessage, Mempool};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

use bytes::Bytes;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use std::error::Error;
use std::collections::HashMap;
use tokio::time::{sleep, Duration};
use tokio::sync::oneshot;
use futures::sink::SinkExt as _;
#[cfg(feature = "benchmark")]
use std::convert::TryInto as _;
use std::net::SocketAddr;
use std::str::FromStr;
use anyhow::Context;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// The default channel capacity for this module.
pub const CHANNEL_CAPACITY: usize = 1_000;

pub struct Node {
    pub commit: Receiver<Block>,
    pub store: Store,
}

impl Node {
    pub async fn new(
        committee_file: &str,
        key_file: &str,
        store_path: &str,
        parameters: Option<&str>,
    ) -> Result<Self, ConfigError> {
        let (tx_commit, rx_commit) = channel(CHANNEL_CAPACITY);
        let (tx_consensus_to_mempool, rx_consensus_to_mempool) = channel(CHANNEL_CAPACITY);
        let (tx_mempool_to_consensus, rx_mempool_to_consensus) = channel(CHANNEL_CAPACITY);

        // Read the committee and secret key from file.
        let committee = Committee::read(committee_file)?;
        let secret = Secret::read(key_file)?;
        let name = secret.name;
        let secret_key = secret.secret;

        // Load default parameters if none are specified.
        let parameters = match parameters {
            Some(filename) => Parameters::read(filename)?,
            None => Parameters::default(),
        };

        // Make the data store.
        let store = Store::new(store_path).expect("Failed to create store");

        // Run the signature service.
        let signature_service = SignatureService::new(secret_key);

        // Make a new mempool.
        Mempool::spawn(
            name,
            committee.mempool,
            parameters.mempool,
            store.clone(),
            rx_consensus_to_mempool,
            tx_mempool_to_consensus,
        );

        // Run the consensus core.
        Consensus::spawn(
            name,
            committee.consensus,
            parameters.consensus,
            signature_service,
            store.clone(),
            rx_mempool_to_consensus,
            tx_consensus_to_mempool,
            tx_commit,
        );

        info!("Node {} successfully booted", name);
        Ok(Self {
            commit: rx_commit,
            store: store,
        })
    }

    pub fn print_key_file(filename: &str) -> Result<(), ConfigError> {
        Secret::new().write(filename)
    }

    pub async fn analyze_block(&mut self, decision: String) {

        // Connect to the decision.
        if let Ok(stream) = TcpStream::connect(decision).await {
            let mut transport = Framed::new(stream, LengthDelimitedCodec::new());

            info!("Starting analyze loop");
            loop {
                if let Some(block) = self.commit.recv().await {
                    let mut nb_tx = 0;

                    for digest in &block.payload {
                        let serialized = self.store.read(digest.to_vec())
                            .await
                            .expect("Failed to get batch from storage")
                            .expect("Batch was not in storage");

                        info!("Deserializing stored batch...");
                        let mempool_message = bincode::deserialize(&serialized)
                            .expect("Failed to deserialize batch");

                        match mempool_message {
                            MempoolMessage::Batch(batch) => {

                                let batch_size = batch.len();

                                for tx_vec in batch {
                                    // TODO Send to carrier
                                    if let Err(e) = transport.send(Bytes::from(tx_vec)).await {
                                        warn!("Failed to send reply to decision: {}", e);
                                    }
                                }

                                // NOTE: This is used to compute performance.
                                nb_tx += batch_size;
                            },
                            MempoolMessage::BatchRequest(_, _) => {
                                warn!("A batch request was stored!");
                            }
                        }

                        #[cfg(feature = "benchmark")]
                        {
                            // NOTE: This is one extra hash that is only needed to print the following log entries.
                            let digest = Digest(
                                Sha512::digest(&serialized).as_slice()[..32]
                                    .try_into()
                                    .unwrap(),
                            );
                            // NOTE: This log entry is used to compute performance.
                            info!("Batch {:?} contains {} currency tx", digest, nb_tx);
                        }
                    }
                }
            }
        }

        // If connection fails do nothing but keep the ndoe running
        while let Some(_block) = self.commit.recv().await {
            // This is where we can further process committed block.
        }
    }
}