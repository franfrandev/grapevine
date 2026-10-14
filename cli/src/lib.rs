use std::convert::Infallible;
use thiserror::Error;
use tokio::{io, task::JoinError};

pub mod cli;
pub mod node;
pub use node::Node;

pub mod backend;
pub mod metrics;
pub mod p2p;
pub mod rpc;
pub mod utils;

pub use crate::metrics::global_registry;

#[derive(Debug, Error)]
pub enum TaskError {
    #[error("finished")]
    Stopped,
    #[error("panic")]
    Panic(#[from] JoinError),
    // #[error("err: {0}")]
    // Other(#[from] Box<dyn Error>),
}

#[derive(Debug, Error)]
pub enum AppError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("app error: {0}")]
    Other(String),
    #[error("rpc error: {0}")]
    Rpc(#[from] rpc::RpcError),
    #[error(transparent)]
    PublishError(#[from] libp2p::gossipsub::PublishError),
    #[error(transparent)]
    GossipError(#[from] p2p::swarm::GossipError),
    #[error("task stopped: {0}, reason: {1:?}")]
    TaskErr(String, TaskError),
    #[error("task panicked: {0}")]
    TaskPanicked(#[from] JoinError),
    #[error("task exited")]
    TaskExited,
    #[error("behaviour builder error: {0}")]
    BehaviourBuilder(#[from] libp2p::BehaviourBuilderError),
    #[error("error: {0}")]
    Anyhow(#[from] anyhow::Error),
    #[error(transparent)]
    DecodingError(#[from] libp2p::identity::DecodingError),
    #[error(transparent)]
    CertError(#[from] libp2p_webrtc::tokio::certificate::Error),
    #[error(transparent)]
    TransportError(#[from] libp2p::TransportError<io::Error>),

    #[error("unreachable")] // this is to accommodate long-running services
    Unreachable(#[from] Infallible),
}
