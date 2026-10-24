//! Persistent, bounded, ordered codec jobs for independent data frames.
use crate::{Error, Result};
use std::{collections::VecDeque, io, sync::mpsc, thread};

struct Worker<J, O> {
    input: Option<mpsc::SyncSender<J>>,
    output: mpsc::Receiver<Result<O>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl<J, O> Drop for Worker<J, O> {
    fn drop(&mut self) {
        // Each worker has at most one outstanding frame. Its result always
        // fits in the one-slot output channel, including during early drop.
        self.input.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

pub(crate) struct OrderedWorkers<J, O> {
    workers: Vec<Worker<J, O>>,
    pending: VecDeque<usize>,
    next: usize,
}

fn stopped() -> Error {
    io::Error::other("frame worker stopped").into()
}

impl<J: Send + 'static, O: Send + 'static> OrderedWorkers<J, O> {
    fn create<F: FnMut(J) -> Result<O> + Send + 'static>(
        count: usize,
        mut factory: impl FnMut() -> Result<F>,
    ) -> Result<Self> {
        let mut workers = Vec::new();
        for index in 0..count {
            let mut process = factory()?;
            let (input, jobs) = mpsc::sync_channel(1);
            let (results, output) = mpsc::sync_channel(1);
            let thread = thread::Builder::new()
                .name(format!("zstar-frame-{index}"))
                .spawn(move || {
                    while let Ok(job) = jobs.recv() {
                        if results.send(process(job)).is_err() {
                            break;
                        }
                    }
                })?;
            workers.push(Worker {
                input: Some(input),
                output,
                thread: Some(thread),
            });
        }
        Ok(Self {
            workers,
            pending: VecDeque::new(),
            next: 0,
        })
    }

    pub fn submit(&mut self, job: J) -> Result<Option<O>> {
        let completed = if self.pending.len() == self.workers.len() {
            self.receive()?
        } else {
            None
        };
        self.workers[self.next]
            .input
            .as_ref()
            .ok_or_else(stopped)?
            .send(job)
            .map_err(|_| stopped())?;
        self.pending.push_back(self.next);
        self.next = (self.next + 1) % self.workers.len();
        Ok(completed)
    }

    pub fn receive(&mut self) -> Result<Option<O>> {
        self.pending
            .pop_front()
            .map(|index| self.workers[index].output.recv().map_err(|_| stopped())?)
            .transpose()
    }
}

pub(crate) type ChecksummedFrameWorkers = OrderedWorkers<(Vec<u8>, Vec<u8>), (Vec<u8>, Vec<u8>)>;

impl ChecksummedFrameWorkers {
    pub fn checksummed(count: usize, level: i32) -> Result<Self> {
        Self::create(count, || {
            let mut compressor = zstd::bulk::Compressor::new(level)?;
            compressor.set_parameter(zstd::zstd_safe::CParameter::ChecksumFlag(true))?;
            Ok(move |(raw, mut encoded): (Vec<u8>, Vec<u8>)| {
                encoded.clear();
                let capacity = zstd::zstd_safe::compress_bound(raw.len());
                encoded.reserve(capacity);
                compressor.compress_to_buffer(&raw, &mut encoded)?;
                Ok((raw, encoded))
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compression_preserves_both_buffer_allocations() {
        let mut workers = ChecksummedFrameWorkers::checksummed(1, 3).unwrap();
        let mut raw = Vec::with_capacity(4096);
        raw.extend_from_slice(&[17; 4096]);
        let encoded = Vec::with_capacity(zstd::zstd_safe::compress_bound(raw.len()));
        let raw_pointer = raw.as_ptr();
        let encoded_pointer = encoded.as_ptr();
        assert!(workers.submit((raw, encoded)).unwrap().is_none());
        let (raw, encoded) = workers.receive().unwrap().unwrap();
        assert_eq!(raw.as_ptr(), raw_pointer);
        assert_eq!(encoded.as_ptr(), encoded_pointer);
        assert_eq!(zstd::bulk::decompress(&encoded, raw.len()).unwrap(), raw);
        assert!(workers.submit((raw, encoded)).unwrap().is_none());
        let (raw, encoded) = workers.receive().unwrap().unwrap();
        assert_eq!(raw.as_ptr(), raw_pointer);
        assert_eq!(encoded.as_ptr(), encoded_pointer);
        assert_eq!(zstd::bulk::decompress(&encoded, raw.len()).unwrap(), raw);
    }

    #[test]
    fn dropping_full_workers_joins_without_draining_results() {
        let mut workers = ChecksummedFrameWorkers::checksummed(4, 3).unwrap();
        for value in 0..4 {
            assert!(
                workers
                    .submit((vec![value; 4096], Vec::new()))
                    .unwrap()
                    .is_none()
            );
        }
        drop(workers);
    }
}
