// Parse the source Ogg/Opus stream and emit one RTP packet per transport message.

use anyhow::Error;
use byte_slice_cast::AsSliceOf;
use derive_more::{Display, Error};
use gstreamer as gst;
use gstreamer::element_error;
use gstreamer::prelude::*;
use gstreamer_app as gst_app;
use std::sync::{Arc, Mutex};
use tokio::select;
use tokio::sync::watch;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

#[derive(Debug, Display, Error)]
#[display("Main loop error from {src}: {error} (debug: {debug:?})")]
pub struct ErrorMessage {
    pub src: glib::GString,
    pub error: glib::Error,
    pub debug: Option<glib::GString>,
}

#[derive(Clone, Debug, glib::Boxed)]
#[boxed_type(name = "ErrorValue")]
pub struct ErrorValue(pub Arc<Mutex<Option<Error>>>);

pub struct EmptyStream {}

pub struct StreamWithPipeline {
    pub pipeline: gst::Pipeline,
    pub sink_rx: flume::Receiver<Vec<u8>>,
    pub title_ch: (
        watch::Sender<Option<String>>,
        watch::Receiver<Option<String>>,
    ),
}

pub struct Stream<S> {
    pub state: S,
}

pub fn new_empty_stream() -> Stream<EmptyStream> {
    Stream {
        state: EmptyStream {},
    }
}

impl Stream<EmptyStream> {
    #[instrument(level = "debug", skip_all)]
    pub fn create_icecast_pipeline(
        self: Stream<EmptyStream>,
        url: String,
    ) -> Result<Stream<StreamWithPipeline>, Error> {
        tracing::debug!("Creating encoding pipeline");

        let pipeline = gst::Pipeline::default();

        let src = gst::ElementFactory::make("uridecodebin3")
            .name("src")
            .property("uri", &url)
            .build()?;

        let queue = gst::ElementFactory::make("queue").name("queue").build()?;
        let audioconvert = gst::ElementFactory::make("audioconvert")
            .name("audioconvert")
            .build()?;
        let audioresample = gst::ElementFactory::make("audioresample")
            .name("audioresample")
            .build()?;
        let opusenc = gst::ElementFactory::make("opusenc")
            .name("opusenc")
            .property("bitrate", 128_000i32)
            .build()?;
        let rtpopuspay = gst::ElementFactory::make("rtpopuspay")
            .name("rtpopuspay")
            .property("pt", 96u32)
            .build()?;
        let appsink = gst_app::AppSink::builder().name("appsink").build();

        pipeline.add_many([
            &src,
            &queue,
            &audioconvert,
            &audioresample,
            &opusenc,
            &rtpopuspay,
            appsink.upcast_ref(),
        ])?;

        // Static chain after decode
        gst::Element::link_many([
            &queue,
            &audioconvert,
            &audioresample,
            &opusenc,
            &rtpopuspay,
            appsink.upcast_ref(),
        ])?;

        // Single dynamic link: decoded audio → queue
        let queue_weak = queue.downgrade();
        src.connect_pad_added(move |_, src_pad| {
            let Some(queue) = queue_weak.upgrade() else {
                return;
            };
            let sink_pad = queue.static_pad("sink").expect("queue sink");

            if sink_pad.is_linked() {
                if let Some(peer) = sink_pad.peer() {
                    let _ = peer.unlink(&sink_pad);
                }
            }

            if let Some(peer) = sink_pad.peer() {
                tracing::debug!("unlinking old peer {}", peer.name());
                let _ = peer.unlink(&sink_pad);
            }

            if let Err(err) = src_pad.link(&sink_pad) {
                tracing::error!("Failed to link uridecodebin3 → queue: {err}");
                return;
            }

            let _ = queue.sync_state_with_parent();
        });

        let (sink_tx, sink_rx) = flume::bounded(100);
        appsink.set_callbacks(
            gst_app::AppSinkCallbacks::builder()
                .new_sample(move |appsink| {
                    let sample = appsink.pull_sample().map_err(|_| gst::FlowError::Eos)?;
                    let buffer = sample.buffer().ok_or(gst::FlowError::Error)?;
                    let map = buffer.map_readable().map_err(|_| gst::FlowError::Error)?;
                    let _ = sink_tx.send(map.as_slice().to_vec());
                    Ok(gst::FlowSuccess::Ok)
                })
                .build(),
        );

        let (title_tx, title_rx) = watch::channel(None);

        tracing::debug!("Encoding pipeline created");

        Ok(Stream::<StreamWithPipeline> {
            state: StreamWithPipeline {
                pipeline,
                sink_rx,
                title_ch: (title_tx, title_rx),
            },
        })
    }
}

impl Stream<StreamWithPipeline> {
    #[instrument(level = "debug", skip_all)]
    pub async fn main_loop(
        self: Stream<StreamWithPipeline>,
        cancel: CancellationToken,
    ) -> Result<(), Error> {
        tracing::debug!("Starting main loop");

        tracing::debug!("Setting pipeline state to Playing");
        self.state.pipeline.set_state(gst::State::Playing)?;

        let bus = self
            .state
            .pipeline
            .bus()
            .expect("Pipeline without bus. Shouldn't happen!");

        let mut bus_stream = bus.stream();

        loop {
            select! {
                Some(msg) = bus_stream.next() => {
                    match self.process_message(msg) {
                        Ok(true) => break,
                        Ok(false) => continue,
                        Err(err) => {
                            tracing::error!("Error processing message: {:?}", err);
                            break
                        }
                    }
                }
                _ = cancel.cancelled() => {
                    tracing::warn!("Cancelled");
                    break;
                }
            }
        }

        tracing::debug!("Pipeline ended");

        // TODO need to ensure this is called when cancelled at any point
        self.state.pipeline.set_state(gst::State::Null)?;

        Ok(())
    }

    pub fn process_message(&self, msg: gstreamer::Message) -> Result<bool, Error> {
        use gst::MessageView;

        tracing::trace!("Received message: {:?}", msg);

        match msg.view() {
            MessageView::Eos(..) => {
                tracing::debug!("Received EOS message, restarting pipeline");
                self.state.pipeline.set_state(gst::State::Playing)?;
                // return Ok(true)
                return Ok(false);
            }
            MessageView::Error(err) => {
                tracing::error!("Received error message: {:?}", err);
                return Err::<_, Error>(
                    ErrorMessage {
                        src: msg
                            .src()
                            .map(|s| s.path_string())
                            .unwrap_or_else(|| glib::GString::from("UNKNOWN")),
                        error: err.error(),
                        debug: err.debug(),
                    }
                    .into(),
                );
            }
            MessageView::Tag(tag) => {
                if let Some(data) = convert_tag_to_metadata(tag.tags()) {
                    tracing::debug!("Received title tag: {:?}", data);
                    let title_tx = &self.state.title_ch.0;
                    title_tx.send(Some(data)).expect("rx is held");
                }
            }
            _ => (),
        }

        Ok(false)
    }

    pub fn sink_rx(&self) -> flume::Receiver<Vec<u8>> {
        self.state.sink_rx.clone()
    }

    pub fn title_rx(&self) -> watch::Receiver<Option<String>> {
        self.state.title_ch.1.clone()
    }
}

fn convert_tag_to_metadata(tags: gst::TagList) -> Option<String> {
    let maybe_title = tags.get::<gst::tags::Title>().map(|v| v.get().to_string());
    let maybe_artist = tags.get::<gst::tags::Artist>().map(|v| v.get().to_string());

    maybe_title.map(|title| {
        maybe_artist
            .map(|artist| format!("{} - {}", title, artist))
            .unwrap_or(title)
    })
}
