#![cfg(target_arch = "wasm32")]

use futures::{
    FutureExt, SinkExt, StreamExt,
    channel::mpsc::{Receiver, Sender},
    select,
};
use libp2p::{
    PeerId, Swarm,
    core::Multiaddr,
    gossipsub,
    gossipsub::{Event, IdentTopic, MessageId},
    identify, kad,
    kad::store::MemoryStore,
    swarm::NetworkBehaviour,
    swarm::SwarmEvent,
};
use libp2p_webrtc_websys as webrtc_websys;
use multiaddr::Protocol;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;
use std::{io, str::FromStr, sync::Mutex};
use thiserror::Error;
use wasm_bindgen::prelude::*;
use wasm_common::{send_rtp_to_decoder, start_decoder_worker};
use web_sys::{Document, HtmlSelectElement, HtmlSpanElement, Worker};

mod wasm_common;

#[derive(Debug)]
struct Chan<T> {
    tx: Sender<T>,
    rx: Option<Receiver<T>>,
}

/// The state of the application.
#[derive(Debug)]
pub struct GlobalState {
    topic: Chan<String>,
    dial: Chan<Multiaddr>,
}

#[derive(NetworkBehaviour)]
pub struct RadioBehavior {
    identify: identify::Behaviour,
    // ping: ping::Behaviour,
    pub(crate) gossipsub: gossipsub::Behaviour,
    kademlia: kad::Behaviour<MemoryStore>,
}

impl GlobalState {
    /// # Panics: if the cell was already set.
    fn init() {
        let topic = {
            let (tx, rx) = futures::channel::mpsc::channel(1024);
            Chan { tx, rx: Some(rx) }
        };

        let dial = {
            let (tx, rx) = futures::channel::mpsc::channel(1024);
            Chan { tx, rx: Some(rx) }
        };

        let state = GlobalState { topic, dial };

        STATE.lock().expect("A").replace(state);
    }

    // fn get() -> GlobalState {
    //     STATE.lock().unwrap().unwrap()
    // }
}

static STATE: Mutex<Option<GlobalState>> = Mutex::new(None);

fn message_id(msg: &gossipsub::Message) -> MessageId {
    let Some(seq_no) = msg.sequence_number else {
        return MessageId::new(&[]);
    };
    MessageId::new(&seq_no.to_be_bytes())
}

fn setup_env() {
    use tracing_subscriber::{EnvFilter, Layer as _, Registry, layer::SubscriberExt as _};
    use wasm_tracing::{WasmLayer, WasmLayerConfig};

    // tracing_wasm::set_as_global_default();
    let rust_log = web_sys::window()
        .and_then(|window| window.local_storage().ok().flatten())
        .and_then(|storage| storage.get_item("RUST_LOG").ok().flatten())
        .unwrap_or_default();

    tracing::subscriber::set_global_default(
        Registry::default().with(
            WasmLayer::new(WasmLayerConfig::default())
                .with_filter(EnvFilter::builder().parse_lossy(rust_log)),
        ),
    )
    .expect("default global");

    console_error_panic_hook::set_once();
}

#[wasm_bindgen]
// pub async fn run(worker_url: String) -> Result<(), JsError> {
pub async fn run() -> Result<(), JsError> {
    setup_env();

    GlobalState::init();

    let mut swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_wasm_bindgen()
        .with_other_transport(|key| webrtc_websys::Transport::new(webrtc_websys::Config::new(key)))?
        .with_behaviour(|key| {
            let local_public_key = key.public();
            let local_id = local_public_key.to_peer_id();

            let identify = identify::Behaviour::new(identify::Config::new(
                "0.1.0".to_string(),
                local_public_key,
            ));

            // let ping = ping::Behaviour::new(ping::Config::new());

            let gossipsub_config = gossipsub::ConfigBuilder::default()
                .message_id_fn(message_id)
                .validation_mode(gossipsub::ValidationMode::Strict)
                .build()
                .map_err(io::Error::other)?;

            let gossipsub = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()),
                gossipsub_config,
            )?;

            let kademlia = kad::Behaviour::new(local_id, MemoryStore::new(local_id));

            Ok(RadioBehavior {
                identify,
                // ping,
                gossipsub,
                kademlia,
            })
        })?
        .build();

    struct Channels {
        topic_rx: Receiver<String>,
        dial_rx: Receiver<Multiaddr>,
    }

    let mut channels = {
        let mut maybe_state = STATE.lock().expect("failed to get state");
        let state = maybe_state.as_mut().expect("failed to get state");

        let topic_rx = state.topic.rx.take().expect("should be still here");
        let dial_rx = state.dial.rx.take().expect("should be still here");

        Channels { topic_rx, dial_rx }
    };

    // let worker_handle = start_decoder_worker(worker_url)
    let worker = start_decoder_worker()
        .await
        .expect("failed to start worker");

    let mut interval = fluvio_wasm_timer::Interval::new(Duration::from_secs(5));

    loop {
        select! {
            event = swarm.select_next_some() => handle_event(&worker, event).await,
            maybe_topic = channels.topic_rx.next() => {
                let Some(topic) = maybe_topic else {
                    continue;
                };
                let topic = IdentTopic::new(topic);
                swarm.behaviour_mut().gossipsub.subscribe(&topic).expect("failed to subscribe");
            }
            maybe_dial = channels.dial_rx.next() => {
                let Some(multiaddr) = maybe_dial else {
                    continue;
                };
                swarm.dial(multiaddr)?;
            }
            _ = interval.next().fuse() => {
                tracing::info!("update network info");
                if let Err(err) = update_network_info(&swarm) {
                    tracing::error!("failed to update network info: {:?}", err);
                }
            }
        }
    }

    Ok(())
}

fn update_network_info(swarm: &Swarm<RadioBehavior>) -> Result<(), JsError> {
    let network_info = swarm.network_info();
    let peers = network_info.num_peers();
    let document = current_document()?;
    document
        .get_element_by_id("peersCount")
        .unwrap()
        .dyn_into::<HtmlSpanElement>()
        .unwrap()
        .set_text_content(Some(&peers.to_string()));
    Ok(())
}

#[derive(Debug)]
pub enum Message {
    AudioPacket(Vec<u8>),
    Metadata(Metadata),
}

#[derive(Debug)]
pub struct Metadata {
    // stream label
    pub(crate) title: String,
}

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("Invalid message type")]
    InvalidMessageType,
}

impl Metadata {
    pub fn decode_bin(encoded: &[u8]) -> Result<Self, DecodeError> {
        let title = std::str::from_utf8(encoded).map_err(|_| DecodeError::InvalidMessageType)?;
        Ok(Metadata {
            title: title.to_string(),
        })
    }
}

impl Message {
    pub fn decode(encoded: Vec<u8>) -> Result<Self, DecodeError> {
        match encoded.first() {
            Some(0) => Ok(Message::AudioPacket(encoded[1..].to_vec())),
            Some(1) => Ok(Message::Metadata(Metadata::decode_bin(&encoded[1..])?)),
            _ => Err(DecodeError::InvalidMessageType),
        }
    }
}

async fn handle_event(worker: &Worker, event: SwarmEvent<RadioBehaviorEvent>) {
    match event {
        SwarmEvent::Behaviour(RadioBehaviorEvent::Gossipsub(Event::Message {
            message, ..
        })) => {
            let Ok(msg) = Message::decode(message.data) else {
                tracing::warn!("failed to decode message");
                return;
            };
            // tracing::info!("Received message: {:?}", msg);
            match msg {
                Message::AudioPacket(packet) => {
                    if let Err(err) = send_rtp_to_decoder(worker, packet) {
                        tracing::error!("failed to play audio packet: {:?}", err);
                    }
                }
                Message::Metadata(metadata) => {
                    tracing::info!("metadata {:?}", metadata);
                }
            }
        }
        evt => tracing::debug!("Swarm event: {:?}", evt),
    }
}

#[wasm_bindgen(js_name = tuneIn)]
// pub async fn tune_in(topic: String) -> Result<(), JsError> {
pub async fn tune_in() -> Result<(), JsError> {
    let document = current_document()?;
    let topic = document
        .get_element_by_id("topic")
        .unwrap()
        .dyn_into::<HtmlSelectElement>()
        .unwrap()
        .value();
    tracing::info!("Tuning in... {topic}");
    let Some(multiaddr) = Stations::get(&topic) else {
        return Err(js_error("invalid multiaddr"));
    };
    dial(multiaddr).await?;
    let mut maybe_state = STATE.lock().expect("failed to get state");
    let state = maybe_state.as_mut().expect("failed to get state");
    let topic_tx = &mut state.topic.tx;
    topic_tx.send(topic).await.expect("cannot send");

    Ok(())
}

#[derive(Debug, Deserialize)]
pub struct Station {
    peer_id: String,
    multiaddr: String,
}

pub struct Stations {}

impl Stations {
    pub fn get(station: &str) -> Option<Multiaddr> {
        let static_stations = include_str!("../pkg/stations.toml");
        let stations: HashMap<String, Station> = toml::from_str(static_stations).unwrap();
        let station = stations.get(station)?;
        let peer_id = PeerId::from_str(&station.peer_id).ok()?;
        let multiaddr = Multiaddr::from_str(&station.multiaddr).ok()?;
        Some(multiaddr.with(Protocol::P2p(peer_id)))
    }
}

pub async fn dial(multiaddr: Multiaddr) -> Result<(), JsError> {
    tracing::info!("Dialing... {multiaddr}");
    let mut maybe_state = STATE.lock().expect("failed to get state");
    let state = maybe_state.as_mut().expect("failed to get state");
    let dial_tx = &mut state.dial.tx;
    dial_tx.send(multiaddr).await.expect("cannot send");
    Ok(())
}

// fn read_stations() -> HashMap<String, Station> {
//
// }

fn current_document() -> Result<Document, JsError> {
    web_sys::window()
        .ok_or(js_error("no global `window` exists"))?
        .document()
        .ok_or(js_error("should have a document on window"))
}

fn js_error(msg: &str) -> JsError {
    io::Error::other(msg).into()
}
