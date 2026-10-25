use std::{
    sync::{Arc, Mutex, mpsc},
    time::Instant,
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};

use crate::webhook::{Error, Provider, Response};

pub(super) type Processor = dyn Fn(Provider, &[u8]) -> Result<Response, Error> + Send + Sync;

pub(super) struct Pool {
    permits: Arc<Semaphore>,
    jobs: mpsc::SyncSender<Job>,
}

struct Job {
    bytes: Vec<u8>,
    deadline: Instant,
    _permit: OwnedSemaphorePermit,
    reply: oneshot::Sender<Option<Result<Response, Error>>>,
}

impl Pool {
    pub(super) fn new(provider: Provider, processor: Arc<Processor>) -> std::io::Result<Self> {
        let (jobs, receive) = mpsc::sync_channel::<Job>(8);
        let receive = Arc::new(Mutex::new(receive));
        for _ in 0..2 {
            let receive = receive.clone();
            let processor = processor.clone();
            std::thread::Builder::new()
                .name(format!("connector-{provider:?}"))
                .spawn(move || {
                    loop {
                        let job = match receive.lock() {
                            Ok(receiver) => receiver.recv(),
                            Err(_) => return,
                        };
                        let Ok(job) = job else {
                            return;
                        };
                        let result = if Instant::now() < job.deadline {
                            let result =
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    processor(provider, &job.bytes)
                                }));
                            result.ok().filter(|_| Instant::now() < job.deadline)
                        } else {
                            None
                        };
                        let _ = job.reply.send(result);
                    }
                })?;
        }
        Ok(Self {
            permits: Arc::new(Semaphore::new(8)),
            jobs,
        })
    }

    pub(super) fn acquire(&self) -> Option<OwnedSemaphorePermit> {
        self.permits.clone().try_acquire_owned().ok()
    }

    pub(super) fn submit(
        &self,
        bytes: Vec<u8>,
        deadline: Instant,
        permit: OwnedSemaphorePermit,
    ) -> Option<oneshot::Receiver<Option<Result<Response, Error>>>> {
        let (reply, receive) = oneshot::channel();
        self.jobs
            .try_send(Job {
                bytes,
                deadline,
                _permit: permit,
                reply,
            })
            .ok()?;
        Some(receive)
    }

    pub(super) fn close(&self) {
        self.permits.close();
    }
}
