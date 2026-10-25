use std::{
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use axum::serve::Listener;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore, watch},
};

pub(super) struct BoundedListener {
    listener: TcpListener,
    permits: Arc<Semaphore>,
    stopped: watch::Receiver<bool>,
}

impl BoundedListener {
    pub(super) fn new(listener: TcpListener, stopped: watch::Receiver<bool>) -> Self {
        Self {
            listener,
            stopped,
            permits: Arc::new(Semaphore::new(128)),
        }
    }
}

impl Listener for BoundedListener {
    type Io = Connection;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .expect("connection semaphore stays open");
        let (stream, address) = Listener::accept(&mut self.listener).await;
        let mut stopped = self.stopped.clone();
        let expires = tokio::time::Instant::now() + Duration::from_secs(5);
        let deadline = Box::pin(async move {
            tokio::select! {
                _ = tokio::time::sleep_until(expires) => {},
                _ = stopped.wait_for(|v| *v) => {},
            }
        });
        (
            Connection {
                stream,
                deadline,
                closed: false,
                _permit: permit,
            },
            address,
        )
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }
}

pub(super) struct Connection {
    stream: TcpStream,
    deadline: Pin<Box<dyn Future<Output = ()> + Send>>,
    _permit: OwnedSemaphorePermit,
    closed: bool,
}

impl Connection {
    fn live(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if self.closed || self.deadline.as_mut().poll(cx).is_ready() {
            self.closed = true;
            Err(io::ErrorKind::TimedOut.into())
        } else {
            Ok(())
        }
    }
}

impl AsyncRead for Connection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.live(cx)?;
        Pin::new(&mut self.stream).poll_read(cx, buffer)
    }
}

impl AsyncWrite for Connection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.live(cx)?;
        Pin::new(&mut self.stream).poll_write(cx, buffer)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.live(cx)?;
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}
