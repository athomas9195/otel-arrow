// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Plaintext backend framing guard; rejects oversized messages before driver decoding.

use std::{
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

const FRAME_LIMIT: usize = 1024 * 1024;

#[derive(Default)]
pub(crate) struct GuardState {
    pub limit: AtomicBool,
    pub active: AtomicBool,
    pub notices: AtomicUsize,
}

pub(crate) struct Guard<S> {
    inner: S,
    state: Arc<GuardState>,
    header: [u8; 5],
    have: usize,
    sent: usize,
    remaining: usize,
    statuses: usize,
    startup_complete: bool,
    metadata: Vec<u8>,
    metadata_have: usize,
    metadata_sent: usize,
    ready: bool,
}
impl<S> Guard<S> {
    pub fn new(inner: S, state: Arc<GuardState>) -> Self {
        Self {
            inner,
            state,
            header: [0; 5],
            have: 0,
            sent: 0,
            remaining: 0,
            statuses: 0,
            startup_complete: false,
            metadata: vec![],
            metadata_have: 0,
            metadata_sent: 0,
            ready: false,
        }
    }
    fn fail(&self) -> io::Error {
        self.state.limit.store(true, Ordering::Release);
        io::Error::new(
            io::ErrorKind::InvalidData,
            "postgresql backend protocol bound",
        )
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Guard<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if output.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if this.state.limit.load(Ordering::Acquire) {
            return Poll::Ready(Err(this.fail()));
        }
        while this.have < 5 {
            let mut buf = ReadBuf::new(&mut this.header[this.have..]);
            match Pin::new(&mut this.inner).poll_read(cx, &mut buf) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => {
                    let n = buf.filled().len();
                    if n == 0 {
                        return if this.have == 0 {
                            Poll::Ready(Ok(()))
                        } else {
                            Poll::Ready(Err(this.fail()))
                        };
                    }
                    this.have += n;
                }
            }
        }
        if !this.ready {
            let length = i32::from_be_bytes([
                this.header[1],
                this.header[2],
                this.header[3],
                this.header[4],
            ]);
            if length < 4 || length as usize > FRAME_LIMIT {
                return Poll::Ready(Err(this.fail()));
            }
            this.remaining = length as usize - 4;
            match this.header[0] {
                b'S' => {
                    this.statuses += 1;
                    if this.startup_complete || this.statuses > 64 {
                        return Poll::Ready(Err(this.fail()));
                    }
                }
                b'Z' => this.startup_complete = true,
                b'N' => {
                    if !this.state.active.load(Ordering::Acquire)
                        || this.state.notices.fetch_add(1, Ordering::AcqRel) >= 128
                    {
                        return Poll::Ready(Err(this.fail()));
                    }
                }
                b'A' => return Poll::Ready(Err(this.fail())),
                b'T' => {
                    if this.remaining > 2 + 128 * (64 + 18) {
                        return Poll::Ready(Err(this.fail()));
                    }
                    this.metadata.resize(this.remaining, 0);
                }
                _ => {}
            }
            this.ready = true;
        }
        if this.header[0] == b'T' {
            while this.metadata_have < this.metadata.len() {
                let mut buf = ReadBuf::new(&mut this.metadata[this.metadata_have..]);
                match Pin::new(&mut this.inner).poll_read(cx, &mut buf) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Ready(Ok(())) => {
                        let n = buf.filled().len();
                        if n == 0 {
                            return Poll::Ready(Err(this.fail()));
                        }
                        this.metadata_have += n;
                    }
                }
            }
            if !valid_metadata(&this.metadata) {
                return Poll::Ready(Err(this.fail()));
            }
        }
        if this.sent < 5 {
            let n = output.remaining().min(5 - this.sent);
            output.put_slice(&this.header[this.sent..this.sent + n]);
            this.sent += n;
        } else if this.remaining > 0 {
            if this.header[0] == b'T' {
                let n = output.remaining().min(this.remaining).min(8192);
                output.put_slice(&this.metadata[this.metadata_sent..this.metadata_sent + n]);
                this.metadata_sent += n;
                this.remaining -= n;
            } else {
                let n = output.remaining().min(this.remaining).min(8192);
                let mut buf = ReadBuf::new(output.initialize_unfilled_to(n));
                match Pin::new(&mut this.inner).poll_read(cx, &mut buf) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Ready(Ok(())) => {
                        let n = buf.filled().len();
                        if n == 0 {
                            return Poll::Ready(Err(this.fail()));
                        }
                        output.advance(n);
                        this.remaining -= n;
                    }
                }
            }
        }
        if this.sent == 5 && this.remaining == 0 {
            this.have = 0;
            this.sent = 0;
            this.ready = false;
            this.metadata.clear();
            this.metadata_have = 0;
            this.metadata_sent = 0;
        }
        Poll::Ready(Ok(()))
    }
}

fn valid_metadata(body: &[u8]) -> bool {
    if body.len() < 2 {
        return false;
    }
    let count = u16::from_be_bytes([body[0], body[1]]) as usize;
    if count > 128 {
        return false;
    }
    let mut offset = 2;
    for _ in 0..count {
        let Some(rest) = body.get(offset..) else {
            return false;
        };
        let Some(len) = rest.iter().position(|b| *b == 0) else {
            return false;
        };
        if len > 63 || std::str::from_utf8(&rest[..len]).is_err() {
            return false;
        }
        offset += len + 1 + 18;
        if offset > body.len() {
            return false;
        }
    }
    offset == body.len()
}
impl<S: AsyncWrite + Unpin> AsyncWrite for Guard<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}
