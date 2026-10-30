use crate::{
    AppError,
    metrics::global_registry,
    p2p::codec::Message,
    utils::{Ctx, Service, TaskRet},
};
use futures::prelude::*;
use libp2p::core::ConnectedPoint;
use libp2p::request_response::ProtocolSupport;
use libp2p::{
    Multiaddr, PeerId, StreamProtocol, Swarm, SwarmBuilder, Transport,
    core::muxing::StreamMuxerBox,
    futures::StreamExt,
    gossipsub::{self, IdentTopic, MessageId},
    identify,
    identity::Keypair,
    kad,
    kad::store::MemoryStore,
    mdns,
    multiaddr::Protocol,
    request_response::{self, Codec},
    swarm::{NetworkBehaviour, SwarmEvent},
};
use libp2p_stream as stream;
use std::{convert::Infallible, fs, net::Ipv6Addr};
use thiserror::Error;
use tokio::{
    io,
    sync::mpsc::{Receiver, Sender},
};
use tokio_util::sync::CancellationToken;

#[derive(NetworkBehaviour)]
pub struct RadioBehavior {
    identify: identify::Behaviour,
    hello: request_response::Behaviour<BytesCodec>,
    pub(crate) gossipsub: gossipsub::Behaviour,
    mdns: mdns::tokio::Behaviour,
    kademlia: kad::Behaviour<MemoryStore>,
}

#[derive(Debug, Error)]
pub enum GossipError {
    #[error(transparent)]
    PublishError(#[from] gossipsub::PublishError),
    #[error(transparent)]
    SubscriptionError(#[from] gossipsub::SubscriptionError),
    #[error("failed to subscribe to topic: already subscribed")]
    AlreadySubscribed,
}

pub struct StreamerSwarmService {
    swarm: Swarm<RadioBehavior>,
    topic: IdentTopic,
    gossip_pub_rx: Receiver<Message>,
    last_metadata: Option<Vec<u8>>,
    cancel: CancellationToken,
}

pub struct StreamerSwarmCtx {
    pub topic: IdentTopic,
    pub web_rtc_port: u16,
    pub quick_port: u16,
    pub gossip_pub_rx: Receiver<Message>,
    pub cancel: CancellationToken,
}

#[derive(Debug, Clone, Default)]
pub struct BytesCodec {
    max_request_size: u64,
    max_response_size: u64,
}

impl BytesCodec {
    pub fn new(max_request_size: u64, max_response_size: u64) -> Self {
        Self {
            max_request_size,
            max_response_size,
        }
    }
}

impl Codec for BytesCodec {
    type Protocol = StreamProtocol;
    type Request = Vec<u8>;
    type Response = Vec<u8>;

    async fn read_request<T>(&mut self, _: &Self::Protocol, _: &mut T) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        Err(io::Error::new(io::ErrorKind::Other, "not implemented"))
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        _: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        Err(io::Error::new(io::ErrorKind::Other, "not implemented"))
    }

    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        io.write_all(&req).await?;
        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        _: &mut T,
        _: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        Err(io::Error::new(io::ErrorKind::Other, "not implemented"))
    }
}

impl Ctx for StreamerSwarmCtx {}

impl Service<StreamerSwarmCtx, AppError, Infallible> for StreamerSwarmService {
    fn new(ctx: StreamerSwarmCtx) -> Result<Self, AppError>
    where
        Self: Sized,
    {
        let swarm = init_swarm(&ctx.topic, ctx.web_rtc_port, ctx.quick_port)?;

        Ok(StreamerSwarmService {
            swarm,
            topic: ctx.topic,
            gossip_pub_rx: ctx.gossip_pub_rx,
            last_metadata: None,
            cancel: ctx.cancel,
        })
    }

    async fn run(mut self) -> TaskRet<Infallible> {
        loop {
            tokio::select! {
                Some(msg) = self.gossip_pub_rx.recv() => {
                    let is_metadata = matches!(msg, Message::Metadata(_));
                    let data = msg.encode();
                    if is_metadata {
                        self.last_metadata = Some(data.clone());
                    }
                    tracing::trace!("received data to publish: {} bytes", data.len());
                    if let Err(err) = publish_data(&mut self.swarm, self.topic.clone(), data) {
                        tracing::trace!("Failed to publish data: {err}");
                    }
                },
                event = self.swarm.select_next_some() => process_streamer_event(&mut self.swarm, &self.last_metadata, event).await,
                _ = self.cancel.cancelled() => break None,
            }
        }
    }
}

impl StreamerSwarmService {
    pub fn peer_id(&self) -> PeerId {
        *self.swarm.local_peer_id()
    }
}

pub struct ListenerSwarmService {
    swarm: Swarm<RadioBehavior>,
    gossip_sub_tx: Sender<Message>,
    streamer_peer_id: PeerId,
    cancel: CancellationToken,
}

pub struct ListenerSwarmCtx {
    pub topic: IdentTopic,
    pub web_rtc_port: u16,
    pub quick_port: u16,
    pub gossip_sub_tx: Sender<Message>,
    pub streamer_peer_id: PeerId,
    pub cancel: CancellationToken,
}

impl Ctx for ListenerSwarmCtx {}

impl Service<ListenerSwarmCtx, AppError, Infallible> for ListenerSwarmService {
    fn new(ctx: ListenerSwarmCtx) -> Result<Self, AppError>
    where
        Self: Sized,
    {
        let swarm = init_swarm(&ctx.topic, ctx.web_rtc_port, ctx.quick_port)?;

        Ok(ListenerSwarmService {
            swarm,
            gossip_sub_tx: ctx.gossip_sub_tx,
            streamer_peer_id: ctx.streamer_peer_id,
            cancel: ctx.cancel,
        })
    }

    async fn run(mut self) -> TaskRet<Infallible> {
        loop {
            tokio::select! {
                event = self.swarm.select_next_some() => process_listener_event(&mut self.swarm, &self.streamer_peer_id, event, &self.gossip_sub_tx).await,
                _ = self.cancel.cancelled() => break None,
            }
        }
    }
}

impl ListenerSwarmService {
    pub fn peer_id(&self) -> PeerId {
        *self.swarm.local_peer_id()
    }
}

fn init_swarm(
    topic: &IdentTopic,
    web_rtc_port: u16,
    quick_port: u16,
) -> Result<Swarm<RadioBehavior>, AppError> {
    let swarm_builder = if let Ok(mut bytes) = fs::read("secp256k1.der") {
        let keypair = Keypair::secp256k1_from_der(&mut bytes)?;
        SwarmBuilder::with_existing_identity(keypair)
    } else {
        SwarmBuilder::with_new_identity()
    };

    let web_rtc_certificate = if let Ok(cert) = fs::read_to_string("webrtc.pem") {
        libp2p_webrtc::tokio::Certificate::from_pem(&cert)?
    } else {
        let cert = libp2p_webrtc::tokio::Certificate::generate(&mut rand::rng())?;
        let cert_str = cert.serialize_pem();
        fs::write("webrtc.pem", cert_str)?;
        cert
    };

    let mut swarm = swarm_builder
        .with_tokio()
        .with_quic()
        .with_other_transport(|id_keys| {
            Ok(
                libp2p_webrtc::tokio::Transport::new(id_keys.clone(), web_rtc_certificate)
                    .map(|(peer_id, conn), _| (peer_id, StreamMuxerBox::new(conn))),
            )
        })
        .unwrap() // TODO
        .with_bandwidth_metrics(&mut global_registry().lock().unwrap())
        .with_behaviour(|key| {
            let local_public_key = key.public();
            let local_id = local_public_key.to_peer_id();

            let identify = identify::Behaviour::new(identify::Config::new(
                "0.1.0".to_string(),
                local_public_key,
            ));

            let hello = request_response::Behaviour::new(
                [(HELLO_PROTOCOL, ProtocolSupport::Outbound)],
                request_response::Config::default(),
            );

            let gossipsub_config = gossipsub::ConfigBuilder::default()
                .message_id_fn(message_id)
                .validation_mode(gossipsub::ValidationMode::Strict)
                .build()
                .map_err(io::Error::other)?;

            let gossipsub = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()),
                gossipsub_config,
            )?;

            let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), local_id)?;

            let kademlia = kad::Behaviour::new(local_id, MemoryStore::new(local_id));

            Ok(RadioBehavior {
                identify,
                hello,
                gossipsub,
                mdns,
                kademlia,
            })
        })?
        .build();

    let address_webrtc = {
        let multiaddr = Multiaddr::from(Ipv6Addr::UNSPECIFIED);
        // let multiaddr = Multiaddr::from(Ipv4Addr::UNSPECIFIED);
        multiaddr
            .with(Protocol::Udp(web_rtc_port))
            .with(Protocol::WebRTCDirect)
    };

    let address_quick = {
        let multiaddr = Multiaddr::from(Ipv6Addr::UNSPECIFIED);
        // let multiaddr = Multiaddr::from(Ipv4Addr::UNSPECIFIED);
        multiaddr
            .with(Protocol::Udp(quick_port))
            .with(Protocol::QuicV1)
    };

    for multiaddr in [address_webrtc, address_quick] {
        tracing::info!("Listening on {:?}", multiaddr);
        let listener_id = swarm.listen_on(multiaddr)?;
        tracing::debug!("Listener id: {:?}", listener_id);
    }

    tune_in(&mut swarm, topic)?;

    Ok(swarm)
}

fn message_id(msg: &gossipsub::Message) -> MessageId {
    let Some(seq_no) = msg.sequence_number else {
        return MessageId::new(&[]);
    };
    MessageId::new(&seq_no.to_be_bytes())
}

pub fn tune_in(swarm: &mut Swarm<RadioBehavior>, topic: &IdentTopic) -> Result<(), GossipError> {
    let subscribed = swarm.behaviour_mut().gossipsub.subscribe(topic)?;
    if !subscribed {
        return Err(GossipError::AlreadySubscribed);
    }
    Ok(())
}

fn publish_data(
    swarm: &mut Swarm<RadioBehavior>,
    topic: IdentTopic,
    data: Vec<u8>,
) -> Result<(), GossipError> {
    let id = swarm.behaviour_mut().gossipsub.publish(topic, data)?;
    tracing::trace!("Published data on with id {:?}", id);
    Ok(())
}

const HELLO_PROTOCOL: StreamProtocol = StreamProtocol::new("/grapevine/hello/1.0.0");

async fn process_streamer_event(
    swarm: &mut Swarm<RadioBehavior>,
    last_metadata: &Option<Vec<u8>>,
    event: SwarmEvent<RadioBehaviorEvent>,
) {
    tracing::trace!("Swarm Event: {:?}", event);
    match event {
        SwarmEvent::Behaviour(b_event) => process_streamer_behavior_event(swarm, b_event).await,
        SwarmEvent::IncomingConnection { .. } => {}
        SwarmEvent::NewListenAddr { address, .. } => tracing::info!("Listening on {:?}", address),
        SwarmEvent::ConnectionEstablished {
            endpoint, peer_id, ..
        } => {
            let Some(metadata) = last_metadata.clone() else {
                tracing::warn!("No metadata to send to peer");
                return;
            };
            if matches!(endpoint, ConnectedPoint::Listener { .. }) {
                swarm.behaviour_mut().hello.send_request(&peer_id, metadata);
            }
        }
        _other => {
            tracing::debug!("Swarm Event not handled: {:?}", _other)
        }
    }
}

async fn process_streamer_behavior_event(
    swarm: &mut Swarm<RadioBehavior>,
    b_event: RadioBehaviorEvent,
) {
    match b_event {
        RadioBehaviorEvent::Mdns(mdns_event) => process_mdns_event(swarm, mdns_event),
        _other => {
            tracing::debug!("Radio Behavior Event not handled: {:?}", _other)
        }
    }
}

async fn process_listener_event(
    swarm: &mut Swarm<RadioBehavior>,
    streamer_peer_id: &PeerId,
    event: SwarmEvent<RadioBehaviorEvent>,
    gossip_sub_tx: &Sender<Message>,
) {
    tracing::trace!("Swarm Event: {:?}", event);
    match event {
        SwarmEvent::Behaviour(b_event) => {
            process_listener_behavior_event(swarm, streamer_peer_id, b_event, gossip_sub_tx).await
        }
        _other => {
            tracing::debug!("Swarm Event not handled: {:?}", _other)
        }
    }
}

async fn process_listener_behavior_event(
    swarm: &mut Swarm<RadioBehavior>,
    streamer_peer_id: &PeerId,
    b_event: RadioBehaviorEvent,
    gossip_sub_tx: &Sender<Message>,
) {
    match b_event {
        RadioBehaviorEvent::Mdns(mdns_event) => process_mdns_event(swarm, mdns_event),
        RadioBehaviorEvent::Gossipsub(gossip_sub_event) => {
            process_gossip_sub_event(swarm, gossip_sub_event, streamer_peer_id, gossip_sub_tx).await
        }
        _other => {
            tracing::debug!("Radio Behavior Event not handled: {:?}", _other)
        }
    }
}

pub fn process_mdns_event(swarm: &mut Swarm<RadioBehavior>, mdns_event: mdns::Event) {
    match mdns_event {
        mdns::Event::Discovered(list) => {
            for (peer_id, _multiaddr) in list {
                tracing::debug!("mDNS discovered a new peer: {peer_id}");
                swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
            }
        }
        mdns::Event::Expired(list) => {
            for (peer_id, _multiaddr) in list {
                tracing::debug!("mDNS discover peer has expired: {peer_id}");
                swarm
                    .behaviour_mut()
                    .gossipsub
                    .remove_explicit_peer(&peer_id);
            }
        }
    }
}

pub async fn process_gossip_sub_event(
    swarm: &mut Swarm<RadioBehavior>,
    gossip_sub_event: gossipsub::Event,
    streamer_peer_id: &PeerId,
    gossip_sub_tx: &Sender<Message>,
) {
    match gossip_sub_event {
        gossipsub::Event::Message {
            propagation_source,
            message_id,
            message,
        } => {
            if &propagation_source != streamer_peer_id {
                // TODO blacklist the peer if the source is not trusted
                tracing::error!("Got message from untrusted peer: {propagation_source}");
                let gossipsub = &mut swarm.behaviour_mut().gossipsub;
                gossipsub.blacklist_peer(&propagation_source);
                gossipsub.remove_explicit_peer(&propagation_source);
                return;
            }
            tracing::trace!("Got message with id: {message_id} from peer: {propagation_source}");
            let Some(seq_no) = message.sequence_number else {
                tracing::warn!("message seq no: None"); // TODO BLACKLIST
                return;
            };
            // TODO reorder messages based on seq_no while buffering according to max wait in case of drops
            tracing::debug!("message seq no: {seq_no}");
            let Ok(msg) = Message::decode(message.data) else {
                tracing::warn!("failed to decode message");
                return;
            };
            if let Err(err) = gossip_sub_tx.send(msg).await {
                tracing::warn!("failed to send gossip message: {err}");
            }
        }
        _other => {
            tracing::debug!("GossipSub Event not handled: {:?}", _other)
        }
    }
}
