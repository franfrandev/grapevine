#![cfg(target_arch = "wasm32")]

use futures_channel::oneshot;
use jitter_buffer::JitterBuffer;
use js_sys::{ArrayBuffer, Float32Array, JsString, Uint8Array};
use libp2p::futures::SinkExt;
use opus_rs::OpusDecoder;
use rtp_rs::RtpReader;
use std::{cell::RefCell, rc::Rc};
use wasm_bindgen::{__rt::IntoJsResult, JsCast, JsValue, closure::Closure, prelude::wasm_bindgen};
use web_sys::{AudioContext, MessageEvent, Worker};

pub(crate) mod jitter_buffer;

const TARGET_RATE: u32 = 48000;
const CHANNELS: u32 = 2;
const FRAME_SIZE: u32 = 960;

pub fn send_rtp_to_decoder(worker: &Worker, packet: Vec<u8>) -> Result<(), JsValue> {
    let reader = RtpReader::new(&packet).expect("failed to create RTP reader");
    let payload = reader.payload().to_vec();
    tracing::info!("Received RTP packet with payload: {:?}", payload);
    // TODO support ogg demuxing. See: https://gist.github.com/amishshah/68548e803c3208566e36e55fe1618e1c
    if payload.is_empty() || payload.starts_with(b"OpusHead") || payload.starts_with(b"OpusTags") {
        return Ok(());
    }
    let array = Uint8Array::new_from_slice(&payload);
    let buffer = array.buffer();
    let js_value = buffer.into_js_result()?;
    worker.post_message(&js_value)?;
    Ok(())
}

pub fn play_audio(
    ctx: &AudioContext,
    time: &mut f64,
    decoded_packets: Vec<Vec<f32>>,
) -> Result<(), JsValue> {
    const LOOK_AHEAD: f64 = 0.03;

    // TODO create a separate pipeline that reads the queue with no polling in order to avoid blocking this thread.
    // from https://cdn.prod.website-files.com/64e8910adc5a63966a68acea/6a26c71b80dd5d56c10599a1_6a26c71938456e54d9043ebb_audio-frame-cheatsheet.pdf:
    // frame duration = samples per frame / sample rate

    // TODO implement buffering if the delta is too small and buffering limits.
    tracing::debug!(
        "schedule: time={:.4}  current={:.4}  delta={:.4}  packets={}",
        *time,
        ctx.current_time(),
        *time - ctx.current_time(),
        decoded_packets.len()
    );

    // TODO support dynamic decoding, not based on constants.
    for pcm in decoded_packets {
        // tracing::debug!("pcm packet {}", pcm.len());
        let audio_buffer = ctx.create_buffer(CHANNELS, FRAME_SIZE, TARGET_RATE as f32)?;

        // de-interleave: [L, R, L, R, ...] ~~> [L, L, ...], [R, R, ...]
        // https://developer.mozilla.org/en-US/docs/Web/API/Web_Audio_API/Basic_concepts_behind_Web_Audio_API#planar_versus_interleaved_buffers
        for channel in 0..CHANNELS {
            let mut channel_data = vec![0.0; FRAME_SIZE as usize];
            let mut offset = channel as usize;
            for i in 0..FRAME_SIZE as usize {
                channel_data[i] = pcm[offset];
                offset += CHANNELS as usize;
            }
            audio_buffer.copy_to_channel(&channel_data, channel as i32)?;
        }

        // tracing::debug!("going to play buffer");
        let source = ctx.create_buffer_source()?;
        source.connect_with_audio_node(&ctx.destination())?;
        source.set_buffer(Some(&audio_buffer));
        // source.start().expect("failed to start");
        *time = (*time).max(ctx.current_time() + LOOK_AHEAD);
        source.start_with_when(*time)?;
        // source.start()?;
        *time += audio_buffer.duration();
        // tracing::debug!("next time {time}, current_time: {}", ctx.current_time());
    }

    Ok(())
}

// pub async fn start_decoder_worker(worker_url: String) -> Result<Rc<RefCell<Worker>>, JsValue> {
pub async fn start_decoder_worker() -> Result<Worker, JsValue> {
    tracing::debug!("starting decoding worker");
    // let worker = Worker::new(&worker_url)?;
    let worker = Worker::new("decoder_worker.js")?;
    let (tx, rx) = oneshot::channel();
    let tx = Rc::new(RefCell::new(Some(tx)));
    let ctx = AudioContext::new()?;
    let time = ctx.current_time();
    let time = Rc::new(RefCell::new(time));
    let jq = JitterBuffer::new();
    let jq = Rc::new(RefCell::new(jq));
    let onmessage = Closure::<dyn FnMut(MessageEvent)>::new(move |event: MessageEvent| {
        tracing::debug!("Received response: {:?}", event.data());
        if let Ok(array) = event.data().dyn_into::<Float32Array>() {
            let mut jq = jq.borrow_mut();
            let pcm = array.to_vec();
            tracing::debug!("Received PCM data of length {}", pcm.len());
            jq.push(pcm);
            if let Some(decoded_packets) = jq.try_pop() {
                tracing::debug!(
                    "popped {} packets from jitter buffer",
                    decoded_packets.len()
                );
                if let Err(err) = play_audio(&ctx, &mut time.borrow_mut(), decoded_packets) {
                    tracing::error!("failed to play audio packet: {:?}", err);
                }
            }
        } else if event.data().dyn_into::<JsString>() == Ok("Ready!".into()) {
            let tx = tx.borrow_mut().take().unwrap();
            tx.send(()).unwrap();
        }
    });
    worker.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
    onmessage.forget();
    rx.await.expect("Failed to receive message");
    Ok(worker)
}

#[wasm_bindgen]
pub struct AudioDecoder {
    decoder: OpusDecoder,
}

#[wasm_bindgen]
impl AudioDecoder {
    /// Create new instance.
    pub fn new() -> AudioDecoder {
        let decoder = OpusDecoder::new(TARGET_RATE as i32, CHANNELS as usize)
            .expect("failed to create Opus decoder");

        AudioDecoder { decoder }
    }

    /// Decodes audio packets to pcm and push to the jitter queue.
    /// When possible, play the audio.
    // #[wasm_bindgen(js_name = decodeMessageExt)]
    pub fn decode_message_ext(&mut self, array_buffer: ArrayBuffer) -> Result<JsValue, JsValue> {
        // tracing_wasm::set_as_global_default();
        console_error_panic_hook::set_once();
        let buf = Uint8Array::new(&array_buffer).to_vec();
        tracing::debug!("decode_message_ext {buf:?}");

        let decoded = self.decode_packet(&buf)?;
        let pcm_buf = Float32Array::new_from_slice(&decoded);

        Ok(pcm_buf.into())
    }

    fn decode_packet(&mut self, packet: &[u8]) -> Result<Vec<f32>, JsValue> {
        let mut pcm = vec![0.0f32; FRAME_SIZE as usize * CHANNELS as usize];
        let res = self.decoder.decode(packet, FRAME_SIZE as usize, &mut pcm);
        let samples = res.expect("failed to decode Opus packet");
        pcm.truncate(samples * CHANNELS as usize);
        Ok(pcm)
    }
}
