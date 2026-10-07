// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Verified TLS and a post-handshake, pre-decoder backend frame guard.

use super::{
    config::Connection,
    credentials,
    error::{Error, Result},
};
use rustls_pki_types::{CertificateDer, pem::PemObject};
use secrecy::ExposeSecret;
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_postgres::tls::{ChannelBinding, MakeTlsConnect, TlsConnect, TlsStream};
use tokio_postgres_rustls::MakeRustlsConnect;

const FRAME_LIMIT: usize = 1024 * 1024;

#[derive(Default)]
pub(crate) struct GuardState {
    pub limit: AtomicBool,
    pub active: AtomicBool,
    pub notices: AtomicUsize,
}

#[derive(Clone)]
pub(crate) struct Connector {
    inner: MakeRustlsConnect,
    state: Arc<GuardState>,
}

impl Connector {
    pub fn new(config: &Connection, state: Arc<GuardState>) -> Result<Self> {
        let bytes = credentials::read(&config.tls.ca_file, 4 * 1024 * 1024)?;
        validate_pem(bytes.expose_secret())?;
        let mut roots = rustls::RootCertStore::empty();
        let mut count = 0;
        for certificate in CertificateDer::pem_slice_iter(bytes.expose_secret()) {
            count += 1;
            if count > 64 {
                return Err(Error::Credential);
            }
            roots
                .add(certificate.map_err(|_| Error::Credential)?)
                .map_err(|_| Error::Credential)?;
        }
        if count == 0 {
            return Err(Error::Credential);
        }
        let provider = rustls::crypto::CryptoProvider::get_default()
            .cloned()
            .ok_or(Error::Tls)?;
        let tls = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
            .map_err(|_| Error::Tls)?
            .with_root_certificates(roots)
            .with_no_client_auth();
        Ok(Self {
            inner: MakeRustlsConnect::new(tls),
            state,
        })
    }
}

pub(crate) fn validate_pem(bytes: &[u8]) -> Result<()> {
    let mut text = std::str::from_utf8(bytes)
        .map_err(|_| Error::Credential)?
        .trim();
    let mut count = 0;
    while !text.is_empty() {
        let rest = text
            .strip_prefix("-----BEGIN CERTIFICATE-----")
            .ok_or(Error::Credential)?;
        let (body, after) = rest
            .split_once("-----END CERTIFICATE-----")
            .ok_or(Error::Credential)?;
        if body.is_empty()
            || !body.bytes().all(|b| {
                b.is_ascii_alphanumeric()
                    || matches!(b, b'+' | b'/' | b'=' | b'\r' | b'\n' | b' ' | b'\t')
            })
        {
            return Err(Error::Credential);
        }
        count += 1;
        if count > 64 {
            return Err(Error::Credential);
        }
        text = after.trim();
    }
    if count == 0 {
        Err(Error::Credential)
    } else {
        Ok(())
    }
}

pub(crate) struct Connect<T> {
    inner: T,
    state: Arc<GuardState>,
}
impl<S> MakeTlsConnect<S> for Connector
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = Guard<<MakeRustlsConnect as MakeTlsConnect<S>>::Stream>;
    type TlsConnect = Connect<<MakeRustlsConnect as MakeTlsConnect<S>>::TlsConnect>;
    type Error = <MakeRustlsConnect as MakeTlsConnect<S>>::Error;
    fn make_tls_connect(
        &mut self,
        domain: &str,
    ) -> std::result::Result<Self::TlsConnect, Self::Error> {
        Ok(Connect {
            inner: <MakeRustlsConnect as MakeTlsConnect<S>>::make_tls_connect(
                &mut self.inner,
                domain,
            )?,
            state: self.state.clone(),
        })
    }
}
impl<S, T> TlsConnect<S> for Connect<T>
where
    S: Send + 'static,
    T: TlsConnect<S> + Send + 'static,
    T::Future: Send,
    T::Stream: Unpin + Send + 'static,
    T::Error: 'static,
{
    type Stream = Guard<T::Stream>;
    type Error = T::Error;
    type Future =
        Pin<Box<dyn Future<Output = std::result::Result<Self::Stream, Self::Error>> + Send>>;
    fn connect(self, stream: S) -> Self::Future {
        Box::pin(async move { Ok(Guard::new(self.inner.connect(stream).await?, self.state)) })
    }
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
impl<S: TlsStream + Unpin> TlsStream for Guard<S> {
    fn channel_binding(&self) -> ChannelBinding {
        self.inner.channel_binding()
    }
}
