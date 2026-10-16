use rtp_rs::Seq;
use std::collections::VecDeque;

/// Jitter buffer that decodes packets on-the-fly.
/// It also ensures that packets are going to be returned in the correct order.
/// The jitter buffer will also provide the consumer of the timestamp at which it has to be played
/// as well as a support for dynamic frame size.
pub struct JitterBuffer {
    // TODO make sure we have a way to remove old packets in case there is no pull
    // buffer: BTreeMap<Seq, Vec<f32>>,
    buffer: VecDeque<Vec<f32>>,
    last_seqno: Option<Seq>,
}

pub struct TimestampedBuffer {
    packet: Vec<f32>,
    start: u32,
}

pub struct QueuedBuffer {
    packets: Vec<TimestampedBuffer>,
}

impl JitterBuffer {
    pub fn new() -> Self {
        Self {
            // buffer: BTreeMap::new(),
            buffer: VecDeque::new(),
            last_seqno: None,
        }
    }

    // pub fn push(&mut self, data: Vec<f32>, seqno: Seq, timestamp: u32) {
    pub fn push(&mut self, data: Vec<f32>) {
        // self.buffer.insert(seqno, data);
        // tracing::debug!("pushed packet with seqno {:?} timestamp {}", seqno, timestamp);
        // tracing::debug!("buffer len {}", self.buffer.len());
        // self.last_seqno = Some(seqno); // TODO support out-of-order packets
        self.buffer.push_back(data);
    }

    /// Returns the decoded buffer if available along with the "when"
    pub fn try_pop(&mut self) -> Option<Vec<Vec<f32>>> {
        // TODO implement the queue logic
        // if self.buffer.is_empty() {
        //     None
        // } else {
        //     Some(self.buffer.drain(..).collect())
        // }
        // TODO make sure seq wapping is supported
        if self.buffer.len() >= 100 {
            let mut collected = Vec::new();
            while collected.len() <= 10 {
                // if let Some((seq, mut buf)) = self.buffer.pop_first() {
                if let Some(buf) = self.buffer.pop_front() {
                    // tracing::debug!("popped packet with seqno {:?}", seq);
                    collected.push(buf);
                }
            }
            Some(collected)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    //
}
