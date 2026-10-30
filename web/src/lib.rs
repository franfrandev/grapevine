#![cfg(target_arch = "wasm32")]

use js_sys::{ArrayBuffer, Float32Array, Uint8Array};
use opus_rs::OpusDecoder;
use wasm_bindgen::{JsValue, prelude::wasm_bindgen};

const TARGET_RATE: u32 = 48000;
const CHANNELS: u32 = 2;
const FRAME_SIZE: u32 = 960;

#[wasm_bindgen]
pub struct AudioDecoder {
    decoder: OpusDecoder,
}

#[wasm_bindgen]
impl AudioDecoder {
    /// Create new instance.
    #[allow(clippy::new_without_default)]
    pub fn new() -> AudioDecoder {
        let decoder = OpusDecoder::new(TARGET_RATE as i32, CHANNELS as usize)
            .expect("failed to create Opus decoder");

        AudioDecoder { decoder }
    }

    /// Decodes audio packet to pcm.
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
