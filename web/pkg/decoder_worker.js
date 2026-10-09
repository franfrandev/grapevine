importScripts('./web.js');

console.log('Initializing worker')

const {AudioDecoder} = wasm_bindgen;

async function init_wasm_in_worker() {
    await wasm_bindgen('./web_bg.wasm');

    var audioDecoder = AudioDecoder.new();

    self.onmessage = async event => {
        console.log('Received message', event.data);
        var decoded = audioDecoder.decode_message_ext(event.data);

        self.postMessage(decoded);
    }

    self.postMessage('Ready!');
}

init_wasm_in_worker();