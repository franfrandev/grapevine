const {run, tuneIn} = wasm_bindgen;

async function run_wasm() {
    await wasm_bindgen();

    let playButton = document.getElementById("play");
    playButton.addEventListener("click", tuneIn);

    run()
}

run_wasm()