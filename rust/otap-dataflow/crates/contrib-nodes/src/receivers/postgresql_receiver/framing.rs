// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Pre-decoding bounds for the plaintext backend stream.
//!
//! tokio-postgres 0.7.18 has no configurable receive-size bound. Its codec emits
//! all complete normal frames already buffered as one BackendMessages value;
//! its capacity-one response channel therefore does not itself bound bytes.
//! Returning at most one frame per read ensures that the codec emits that frame
//! before reading the next. This bounds both its receive buffer and every
//! queued/current BackendMessages value, including a 300-row portal fetch.
//! Recheck this invariant when updating the pinned driver.

use super::adapter::Error;
use std::{
    cell::Cell,
    io,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, ready},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Includes the one-byte tag and four-byte length, not only the row payload.
pub(crate) const MAX_BACKEND_FRAME_BYTES: usize = 8 * 1024 * 1024;
/// Fixed socket read-ahead; independent of any advertised backend frame size.
pub(crate) const READ_BUFFER_BYTES: usize = 8 * 1024;

/// Driver I/O errors otherwise reach waiting requests only as Error::closed().
/// This latch is shared solely by tasks on the worker's local runtime.
pub(crate) type ReceiveFailure = Rc<Cell<Option<Error>>>;

pub(crate) struct BoundedBackend<S> {
    inner: S,
    failure: ReceiveFailure,
    header: [u8; 5],
    header_read: usize,
    header_sent: usize,
    body_left: usize,
}

impl<S> BoundedBackend<S> {
    pub(crate) fn new(inner: S, failure: ReceiveFailure) -> Self {
        Self {
            inner,
            failure,
            header: [0; 5],
            header_read: 0,
            header_sent: 0,
            body_left: 0,
        }
    }

    fn fail(&self, error: Error, kind: io::ErrorKind, message: &'static str) -> io::Error {
        if self.failure.get().is_none() {
            self.failure.set(Some(error));
        }
        io::Error::new(kind, message)
    }

    fn finish_frame(&mut self) {
        if self.header_sent == self.header.len() && self.body_left == 0 {
            self.header_read = 0;
            self.header_sent = 0;
        }
    }

    fn read_error(&self, error: io::Error) -> io::Error {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            self.fail(
                Error::Unavailable,
                io::ErrorKind::UnexpectedEof,
                "postgresql backend frame truncated",
            )
        } else {
            error
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for BoundedBackend<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if output.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if this.failure.get().is_some() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "postgresql backend receive stopped",
            )));
        }
        while this.header_read < this.header.len() {
            let mut header = ReadBuf::new(&mut this.header[this.header_read..]);
            let read = match ready!(Pin::new(&mut this.inner).poll_read(cx, &mut header)) {
                Ok(()) => header.filled().len(),
                Err(error) => return Poll::Ready(Err(this.read_error(error))),
            };
            if read == 0 {
                return Poll::Ready(if this.header_read == 0 {
                    Ok(())
                } else {
                    Err(this.fail(
                        Error::Unavailable,
                        io::ErrorKind::UnexpectedEof,
                        "postgresql backend header truncated",
                    ))
                });
            }
            this.header_read += read;
            if this.header_read == this.header.len() {
                let length = i32::from_be_bytes([
                    this.header[1],
                    this.header[2],
                    this.header[3],
                    this.header[4],
                ]);
                if length < 4 {
                    return Poll::Ready(Err(this.fail(
                        Error::Database,
                        io::ErrorKind::InvalidData,
                        "postgresql backend frame length invalid",
                    )));
                }
                if length as usize >= MAX_BACKEND_FRAME_BYTES {
                    return Poll::Ready(Err(this.fail(
                        Error::Limit,
                        io::ErrorKind::InvalidData,
                        "postgresql backend frame exceeds receive limit",
                    )));
                }
                this.body_left = length as usize - 4;
            }
        }
        if this.header_sent < this.header.len() {
            let count = output.remaining().min(this.header.len() - this.header_sent);
            output.put_slice(&this.header[this.header_sent..this.header_sent + count]);
            this.header_sent += count;
            this.finish_frame();
            return Poll::Ready(Ok(()));
        }

        let count = output.remaining().min(this.body_left);
        let mut body = ReadBuf::new(output.initialize_unfilled_to(count));
        let read = match ready!(Pin::new(&mut this.inner).poll_read(cx, &mut body)) {
            Ok(()) => body.filled().len(),
            Err(error) => return Poll::Ready(Err(this.read_error(error))),
        };
        if read == 0 {
            return Poll::Ready(Err(this.fail(
                Error::Unavailable,
                io::ErrorKind::UnexpectedEof,
                "postgresql backend body truncated",
            )));
        }
        output.advance(read);
        this.body_left -= read;
        this.finish_frame();
        Poll::Ready(Ok(()))
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for BoundedBackend<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, bytes)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{StreamExt, future::poll_fn};
    use std::{collections::VecDeque, future::Future};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_postgres::{Connection, NoTls, config::SslMode, tls::NoTlsStream};

    fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut bytes = vec![tag];
        bytes.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        bytes.extend_from_slice(body);
        bytes
    }

    struct Fragmented {
        bytes: Vec<u8>,
        consumed: Rc<Cell<usize>>,
        fragment: usize,
        pending: bool,
        error_at_end: Option<io::ErrorKind>,
    }

    impl AsyncRead for Fragmented {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            output: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            if this.pending {
                this.pending = false;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            this.pending = true;
            let start = this.consumed.get();
            if start == this.bytes.len()
                && let Some(kind) = this.error_at_end
            {
                return Poll::Ready(Err(io::Error::from(kind)));
            }
            let count = output
                .remaining()
                .min(this.fragment)
                .min(this.bytes.len() - start);
            output.put_slice(&this.bytes[start..start + count]);
            this.consumed.set(start + count);
            Poll::Ready(Ok(()))
        }
    }

    fn fragmented(bytes: Vec<u8>, fragment: usize) -> BoundedBackend<Fragmented> {
        BoundedBackend::new(
            Fragmented {
                bytes,
                consumed: Rc::default(),
                fragment,
                pending: true,
                error_at_end: None,
            },
            ReceiveFailure::default(),
        )
    }

    /// Scenario: An oversized backend length arrives one byte at a time.
    /// Guarantees: Only five header bytes are read; no body or header reaches the driver.
    #[tokio::test]
    async fn oversized_header_rejected_before_body() {
        let mut bytes = vec![b'D'];
        bytes.extend_from_slice(&(MAX_BACKEND_FRAME_BYTES as i32).to_be_bytes());
        bytes.extend_from_slice(b"unread private row");
        let mut stream = fragmented(bytes, 1);
        let mut output = [0; 64];
        let error = stream.read(&mut output).await.expect_err("oversized frame");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(stream.failure.get(), Some(Error::Limit));
        assert_eq!(stream.inner.consumed.get(), 5);
        assert_eq!(output, [0; 64]);
        assert!(!error.to_string().contains("private"));
        assert!(stream.read(&mut output).await.is_err());
        assert_eq!(stream.inner.consumed.get(), 5);
    }

    /// Scenario: An oversized header and body bytes are already available in the socket.
    /// Guarantees: Fixed read-ahead stays within eight KiB and no unchecked header reaches the driver.
    #[tokio::test]
    async fn socket_buffer_keeps_oversized_frames_bounded() {
        let mut bytes = vec![b'D'];
        bytes.extend_from_slice(&(MAX_BACKEND_FRAME_BYTES as i32).to_be_bytes());
        bytes.resize(READ_BUFFER_BYTES * 2, 0);
        let source = fragmented(bytes, usize::MAX).inner;
        let consumed = source.consumed.clone();
        let failure = ReceiveFailure::default();
        let mut stream = BoundedBackend::new(
            tokio::io::BufReader::with_capacity(READ_BUFFER_BYTES, source),
            failure.clone(),
        );
        let mut output = [0; 64];
        assert!(stream.read(&mut output).await.is_err());
        assert_eq!(failure.get(), Some(Error::Limit));
        assert!(consumed.get() <= READ_BUFFER_BYTES);
        assert_eq!(output, [0; 64]);
    }

    /// Scenario: A backend announces invalid signed lengths, including negative and short lengths.
    /// Guarantees: Malformed headers are rejected explicitly without exposing their bytes.
    #[tokio::test]
    async fn malformed_lengths_rejected() {
        for length in [-1i32, i32::MIN, 0, 3] {
            let mut bytes = vec![b'D'];
            bytes.extend_from_slice(&length.to_be_bytes());
            let mut stream = fragmented(bytes, 2);
            let mut output = [0; 8];
            let error = stream.read(&mut output).await.expect_err("invalid length");
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert_eq!(stream.failure.get(), Some(Error::Database));
            assert_eq!(output, [0; 8]);
        }
    }

    /// Scenario: Standard authentication, metadata, row, notice, and completion frames are fragmented.
    /// Guarantees: Bytes survive pending reads and small destinations without crossing a frame boundary.
    #[tokio::test]
    async fn standard_frames_preserve_boundaries() {
        let frames = [
            frame(b'R', &0i32.to_be_bytes()),
            frame(b'S', b"server_version\0\x31\x35\0"),
            frame(b'T', &[0, 0]),
            frame(b'D', &[0, 1, 0, 0, 0, 1, b'x']),
            frame(b'N', b"SNOTICE\0Mignored\0\0"),
            frame(b's', &[]),
            frame(b'Z', b"I"),
        ];
        let expected = frames.concat();
        for fragment in [1, 2, 7, usize::MAX] {
            for capacity in [1, 4, 64] {
                let mut stream = fragmented(expected.clone(), fragment);
                let mut actual = Vec::new();
                let mut boundary = 0;
                for expected_frame in &frames {
                    boundary += expected_frame.len();
                    while actual.len() < boundary {
                        let mut output = vec![0; capacity];
                        let count = stream.read(&mut output).await.expect("frame bytes");
                        assert!(count > 0);
                        actual.extend_from_slice(&output[..count]);
                        assert!(actual.len() <= boundary, "read crossed frame boundary");
                    }
                }
                assert_eq!(actual, expected);
                assert_eq!(stream.read(&mut [0; 8]).await.expect("EOF"), 0);
                assert_eq!(stream.failure.get(), None);
            }
        }
    }

    /// Scenario: A backend frame is exactly the inclusive eight-MiB wire-size limit.
    /// Guarantees: The complete legal frame streams through without an additional helper body allocation.
    #[tokio::test]
    async fn inclusive_frame_limit_passes() {
        let expected = frame(b'D', &vec![0; MAX_BACKEND_FRAME_BYTES - 5]);
        let mut stream = fragmented(expected, 4096);
        let mut total = 0;
        let mut output = [0; 4096];
        loop {
            let read = stream.read(&mut output).await.expect("bounded frame");
            if read == 0 {
                break;
            }
            total += read;
        }
        assert_eq!(total, MAX_BACKEND_FRAME_BYTES);
        assert_eq!(stream.failure.get(), None);
    }

    /// Scenario: The peer closes between frames, inside a header, or inside an accepted body.
    /// Guarantees: Partial header/body EOF reports retryable transport loss without fabricated bytes.
    #[tokio::test]
    async fn eof_and_truncation_are_distinct() {
        assert_eq!(
            fragmented(Vec::new(), 1)
                .read(&mut [0; 8])
                .await
                .expect("EOF"),
            0
        );
        let complete = frame(b'D', b"row");
        for length in 1..complete.len() {
            let mut stream = fragmented(complete[..length].to_vec(), 1);
            let mut output = Vec::new();
            let error = stream
                .read_to_end(&mut output)
                .await
                .expect_err("truncated");
            assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
            assert_eq!(stream.failure.get(), Some(Error::Unavailable));
            if length < 5 {
                assert!(output.is_empty());
                assert!(error.to_string().contains("header"));
            } else {
                assert_eq!(output, complete[..length]);
                assert!(error.to_string().contains("body"));
            }
        }
    }

    /// Scenario: The underlying stream resets during header or body reception.
    /// Guarantees: The original transport error propagates without exposing incomplete headers.
    #[tokio::test]
    async fn transport_errors_propagate() {
        let complete = frame(b'D', b"row");
        for length in [0, 3, 6] {
            let mut stream = fragmented(complete[..length].to_vec(), 2);
            stream.inner.error_at_end = Some(io::ErrorKind::ConnectionReset);
            let mut output = Vec::new();
            let error = stream.read_to_end(&mut output).await.expect_err("reset");
            assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
            if length < 5 {
                assert!(output.is_empty());
            } else {
                assert_eq!(output, complete[..length]);
            }
        }
    }

    /// Scenario: The underlying reader reports UnexpectedEof rather than a zero-byte truncated read.
    /// Guarantees: Both header and body truncation remain retryable transport failures, not size-limit failures.
    #[tokio::test]
    async fn explicit_unexpected_eof_is_retryable() {
        let complete = frame(b'D', b"row");
        for length in [3, 6] {
            let mut stream = fragmented(complete[..length].to_vec(), 2);
            stream.inner.error_at_end = Some(io::ErrorKind::UnexpectedEof);
            let mut output = Vec::new();
            let error = stream
                .read_to_end(&mut output)
                .await
                .expect_err("truncated");
            assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
            assert_eq!(stream.failure.get(), Some(Error::Unavailable));
        }
    }

    /// Scenario: The wrapper writes a startup or password message and receives an empty read buffer.
    /// Guarantees: Outbound bytes are unchanged and a zero-capacity read consumes nothing.
    #[tokio::test]
    async fn writes_pass_through_and_empty_reads_do_not_consume() {
        let (client, mut server) = tokio::io::duplex(64);
        let mut stream = BoundedBackend::new(client, ReceiveFailure::default());
        assert_eq!(stream.read(&mut []).await.expect("empty read"), 0);
        stream.write_all(b"outbound").await.expect("write");
        stream.flush().await.expect("flush");
        stream.shutdown().await.expect("shutdown");
        let mut bytes = Vec::new();
        let _ = server.read_to_end(&mut bytes).await.expect("read outbound");
        assert_eq!(bytes, b"outbound");
    }

    struct Peer {
        responses: VecDeque<Vec<u8>>,
        current: Vec<u8>,
        position: usize,
        consumed: Rc<Cell<usize>>,
        write_error: Option<io::ErrorKind>,
    }

    impl AsyncRead for Peer {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            output: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            if this.position == this.current.len() {
                return Poll::Pending;
            }
            let count = output.remaining().min(this.current.len() - this.position);
            output.put_slice(&this.current[this.position..this.position + count]);
            this.position += count;
            this.consumed.set(this.consumed.get() + count);
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for Peer {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            assert_eq!(this.position, this.current.len(), "request was pipelined");
            if this.responses.is_empty()
                && let Some(kind) = this.write_error
            {
                return Poll::Ready(Err(io::Error::new(kind, "private transport diagnostic")));
            }
            this.current = this.responses.pop_front().expect("scripted response");
            this.position = 0;
            cx.waker().wake_by_ref();
            Poll::Ready(Ok(bytes.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    async fn drive<S: AsyncRead + AsyncWrite + Unpin, F: Future>(
        connection: &mut Connection<S, NoTlsStream>,
        future: F,
    ) -> F::Output {
        tokio::select! {
            result = future => result,
            result = poll_fn(|cx| connection.poll_message(cx)) => {
                panic!("unexpected driver termination or async message: {result:?}");
            }
        }
    }

    /// Scenario: An oversized response terminates the driver while a query awaits its channel.
    /// Guarantees: The receive latch retains Limit even though the query receives only a closed error.
    #[tokio::test]
    async fn receive_limit_survives_driver_channel_closure() {
        let mut oversized = vec![b'D'];
        oversized.extend_from_slice(&(MAX_BACKEND_FRAME_BYTES as i32).to_be_bytes());
        let failure = ReceiveFailure::default();
        let peer = Peer {
            responses: VecDeque::from([
                [frame(b'R', &0i32.to_be_bytes()), frame(b'Z', b"I")].concat(),
                oversized,
            ]),
            current: Vec::new(),
            position: 0,
            consumed: Rc::default(),
            write_error: None,
        };
        let (client, mut connection) = tokio_postgres::Config::new()
            .user("test")
            .ssl_mode(SslMode::Disable)
            .connect_raw(BoundedBackend::new(peer, failure.clone()), NoTls)
            .await
            .expect("startup");
        let query = client.batch_execute("SELECT 1");
        let driver = async move {
            let error = poll_fn(|cx| connection.poll_message(cx))
                .await
                .expect("terminal message")
                .expect_err("oversized frame");
            drop(connection);
            error
        };
        let (query, _) = tokio::join!(query, driver);
        assert!(query.expect_err("driver closed").is_closed());
        assert_eq!(failure.get(), Some(Error::Limit));
    }

    /// Scenario: The driver receives 300 64-KiB portal rows before the consumer polls.
    /// Guarantees: Batches remain frame-sized with fixed socket read-ahead, while a valid fetch over eight MiB is fully accepted.
    #[tokio::test]
    async fn portal_backpressure_bounds_actual_driver_batches() {
        let ready = frame(b'Z', b"T");
        let mut description = vec![0, 1];
        description.extend_from_slice(b"value\0");
        description.extend_from_slice(&0u32.to_be_bytes());
        description.extend_from_slice(&0i16.to_be_bytes());
        description.extend_from_slice(&25u32.to_be_bytes());
        description.extend_from_slice(&(-1i16).to_be_bytes());
        description.extend_from_slice(&(-1i32).to_be_bytes());
        description.extend_from_slice(&0i16.to_be_bytes());
        let mut body = vec![0, 1];
        let payload_bytes = 64 * 1024;
        body.extend_from_slice(&(payload_bytes as i32).to_be_bytes());
        body.extend_from_slice(&vec![b'x'; payload_bytes]);
        let row = frame(b'D', &body);
        let mut rows = row.repeat(300);
        rows.extend_from_slice(&frame(b's', &[]));
        rows.extend_from_slice(&ready);
        let consumed = Rc::new(Cell::new(0));
        let failure = ReceiveFailure::default();
        let peer = Peer {
            responses: VecDeque::from([
                [frame(b'R', &0i32.to_be_bytes()), frame(b'Z', b"I")].concat(),
                [frame(b'C', b"BEGIN\0"), ready.clone()].concat(),
                [
                    frame(b'1', &[]),
                    frame(b't', &[0, 0]),
                    frame(b'T', &description),
                    ready.clone(),
                ]
                .concat(),
                [frame(b'2', &[]), ready].concat(),
                rows,
            ]),
            current: Vec::new(),
            position: 0,
            consumed: consumed.clone(),
            write_error: None,
        };
        let (mut client, mut connection) = tokio_postgres::Config::new()
            .user("test")
            .ssl_mode(SslMode::Disable)
            .connect_raw(
                BoundedBackend::new(
                    tokio::io::BufReader::with_capacity(READ_BUFFER_BYTES, peer),
                    failure.clone(),
                ),
                NoTls,
            )
            .await
            .expect("startup");
        let transaction = drive(&mut connection, client.transaction())
            .await
            .expect("begin");
        let statement = drive(&mut connection, transaction.prepare("SELECT value"))
            .await
            .expect("prepare");
        let portal = drive(&mut connection, transaction.bind(&statement, &[]))
            .await
            .expect("bind");
        let rows = transaction
            .query_portal_raw(&portal, 300)
            .await
            .expect("portal");
        tokio::pin!(rows);
        let before = consumed.get();
        // First poll dispatches Execute; subsequent polls must stall on the
        // capacity-one channel (which also grants one sender-reserved slot).
        for _ in 0..4 {
            poll_fn(|cx| {
                assert!(connection.poll_message(cx).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        let received = consumed.get() - before;
        assert!(received >= row.len());
        assert!(
            received <= 3 * row.len() + READ_BUFFER_BYTES,
            "read ahead {received} bytes"
        );
        let mut count = 0;
        while let Some(row) = drive(&mut connection, rows.next()).await {
            let row = row.expect("row");
            assert_eq!(row.get::<_, &str>(0).len(), payload_bytes);
            count += 1;
        }
        assert_eq!(count, 300);
        assert_eq!(failure.get(), None);
    }

    /// Scenario: A connected backend fails a subsequent query write with BrokenPipe.
    /// Guarantees: The driver error remains retryable and redacted, rather than terminating collection as a database error.
    #[tokio::test]
    async fn broken_pipe_after_handshake_is_retryable() {
        use super::super::adapter::database;
        let peer = Peer {
            responses: VecDeque::from([
                [frame(b'R', &0i32.to_be_bytes()), frame(b'Z', b"I")].concat()
            ]),
            current: Vec::new(),
            position: 0,
            consumed: Rc::default(),
            write_error: Some(io::ErrorKind::BrokenPipe),
        };
        let (client, mut connection) = tokio_postgres::Config::new()
            .user("test")
            .ssl_mode(SslMode::Disable)
            .connect_raw(BoundedBackend::new(peer, ReceiveFailure::default()), NoTls)
            .await
            .expect("startup");
        let query = client.batch_execute("SELECT 1");
        let driver = async move {
            let error = poll_fn(|cx| connection.poll_message(cx))
                .await
                .expect("driver failure")
                .expect_err("broken pipe");
            drop(connection);
            database(error)
        };
        let (query, error) = tokio::join!(query, driver);
        assert!(query.is_err());
        assert_eq!(error, Error::Unavailable);
        assert!(!error.to_string().contains("private"));
    }
}
