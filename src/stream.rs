//! Async streaming functionality for Polymarket client
//!
//! This module provides high-performance streaming capabilities for
//! real-time market data and order updates.

use crate::errors::{PolyfillError, Result};
use crate::types::*;
use crate::ws_hot_path::{WsBookApplyStats, WsBookUpdateProcessor};
use chrono::Utc;
use futures::{ready, Sink, SinkExt, Stream, StreamExt};
use parking_lot::Mutex;
use serde_json::Value;
use std::collections::{HashSet, VecDeque};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant, Interval, MissedTickBehavior};
use tokio_tungstenite::tungstenite::Message;
use tracing::info;

/// Trait for market data streams
pub trait MarketStream: Stream<Item = Result<StreamMessage>> + Send + Sync {
    /// Subscribe to market data for specific tokens
    fn subscribe(&mut self, subscription: Subscription) -> Result<()>;

    /// Unsubscribe from market data
    fn unsubscribe(&mut self, token_ids: &[String]) -> Result<()>;

    /// Check if the stream is connected
    fn is_connected(&self) -> bool;

    /// Get connection statistics
    fn get_stats(&self) -> StreamStats;
}

/// WebSocket-based market stream implementation.
///
/// Poll continuously to drive text PING heartbeats every ten seconds and consume PONGs.
/// Close or transport failure is terminal, with no automatic reconnection. Create a new
/// stream and subscribe again after discarding downstream state and requesting fresh snapshots.
#[derive(Debug)]
pub struct WebSocketStream {
    /// WebSocket connection
    connection: Option<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
    /// URL for the WebSocket connection
    url: String,
    /// Authentication credentials
    auth: Option<WssAuth>,
    /// Assets whose books lose continuity if this connection ends.
    market_assets: HashSet<String>,
    books_to_reset: HashSet<String>,
    state: ConnectionState,
    heartbeat: Option<Interval>,
    writes: PendingWrites,
    last_heartbeat_response: Option<Instant>,
    /// Parsed messages awaiting delivery to the caller.
    ///
    /// This replaces an internal unbounded channel to avoid per-message
    /// allocations in the buffering layer and to enforce a bounded backlog.
    pending: VecDeque<StreamMessage>,
    pending_capacity: usize,
    /// Connection statistics
    stats: StreamStats,
}

/// Stream statistics
#[derive(Debug, Clone)]
pub struct StreamStats {
    pub messages_received: u64,
    pub messages_sent: u64,
    pub errors: u64,
    pub dropped_messages: u64,
    pub last_message_time: Option<chrono::DateTime<Utc>>,
    pub connection_uptime: std::time::Duration,
    pub reconnect_count: u32,
}

/// Legacy reconnection settings, not used by the terminal WebSocket streams.
#[derive(Debug, Clone)]
pub struct ReconnectConfig {
    pub max_retries: u32,
    pub base_delay: std::time::Duration,
    pub max_delay: std::time::Duration,
    pub backoff_multiplier: f64,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            max_retries: 5,
            base_delay: std::time::Duration::from_secs(1),
            max_delay: std::time::Duration::from_secs(60),
            backoff_multiplier: 2.0,
        }
    }
}

impl WebSocketStream {
    /// Create a new WebSocket stream
    pub fn new(url: &str) -> Self {
        let pending_capacity = 1024;

        Self {
            connection: None,
            url: url.to_string(),
            auth: None,
            market_assets: HashSet::new(),
            books_to_reset: HashSet::new(),
            state: ConnectionState::Disconnected,
            heartbeat: None,
            writes: PendingWrites::default(),
            last_heartbeat_response: None,
            pending: VecDeque::with_capacity(pending_capacity),
            pending_capacity,
            stats: StreamStats {
                messages_received: 0,
                messages_sent: 0,
                errors: 0,
                dropped_messages: 0,
                last_message_time: None,
                connection_uptime: std::time::Duration::ZERO,
                reconnect_count: 0,
            },
        }
    }

    fn enqueue(&mut self, message: StreamMessage) {
        if self.pending.len() >= self.pending_capacity {
            let _ = self.pending.pop_front();
            self.stats.dropped_messages += 1;
        }
        self.pending.push_back(message);
    }

    /// Set authentication credentials
    pub fn with_auth(mut self, auth: WssAuth) -> Self {
        self.auth = Some(auth);
        self
    }

    /// Connect to the WebSocket
    async fn connect(&mut self) -> Result<()> {
        let (ws_stream, _) = tokio_tungstenite::connect_async(&self.url)
            .await
            .map_err(|e| {
                PolyfillError::stream(
                    format!("WebSocket connection failed: {}", e),
                    crate::errors::StreamErrorKind::ConnectionFailed,
                )
            })?;

        self.connection = Some(ws_stream);
        self.state = ConnectionState::Connected;
        let mut heartbeat = tokio::time::interval_at(
            Instant::now() + Duration::from_secs(10),
            Duration::from_secs(10),
        );
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        self.heartbeat = Some(heartbeat);
        info!("Connected to WebSocket stream at {}", self.url);
        Ok(())
    }

    /// Send a message to the WebSocket
    async fn send_message(&mut self, message: Value) -> Result<()> {
        if let Some(connection) = &mut self.connection {
            let text = serde_json::to_string(&message).map_err(|e| {
                PolyfillError::parse(format!("Failed to serialize message: {}", e), None)
            })?;

            let ws_message = tokio_tungstenite::tungstenite::Message::Text(text);
            if let Err(error) = connection.send(ws_message).await {
                return Err(self.transport_error(error));
            }

            self.stats.messages_sent += 1;
        }

        Ok(())
    }

    /// Subscribe to market data using official Polymarket WebSocket API
    pub async fn subscribe_async(&mut self, subscription: WssSubscription) -> Result<()> {
        if matches!(
            self.state,
            ConnectionState::Closing | ConnectionState::Closed
        ) {
            return Err(PolyfillError::stream(
                "Stream is closed, create a new stream and request fresh snapshots",
                crate::errors::StreamErrorKind::ConnectionLost,
            ));
        }
        // Ensure connection
        if self.connection.is_none() {
            self.connect().await?;
        }

        // Send subscription message in the format expected by Polymarket
        // The subscription struct will serialize correctly with proper field names
        let message = serde_json::to_value(&subscription).map_err(|e| {
            PolyfillError::parse(format!("Failed to serialize subscription: {}", e), None)
        })?;

        self.send_message(message).await?;
        if subscription.channel_type == "market" {
            for asset in &subscription.asset_ids {
                if subscription.operation.as_deref() == Some("unsubscribe") {
                    self.market_assets.remove(asset);
                    self.books_to_reset.remove(asset);
                } else {
                    self.market_assets.insert(asset.clone());
                    self.books_to_reset.insert(asset.clone());
                }
            }
        }

        info!("Subscribed to {} channel", subscription.channel_type);
        Ok(())
    }

    /// Subscribe to user channel (orders and trades)
    pub async fn subscribe_user_channel(&mut self, markets: Vec<String>) -> Result<()> {
        let auth = self
            .auth
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("No authentication provided for WebSocket"))?
            .clone();

        let subscription = WssSubscription {
            channel_type: "user".to_string(),
            operation: Some("subscribe".to_string()),
            markets,
            asset_ids: Vec::new(),
            initial_dump: Some(true),
            custom_feature_enabled: None,
            auth: Some(auth),
        };

        self.subscribe_async(subscription).await
    }

    /// Subscribe to market channel (order book and trades)
    /// Market subscriptions do not require authentication
    pub async fn subscribe_market_channel(&mut self, asset_ids: Vec<String>) -> Result<()> {
        let subscription = WssSubscription {
            channel_type: "market".to_string(),
            operation: Some("subscribe".to_string()),
            markets: Vec::new(),
            asset_ids,
            initial_dump: Some(true),
            custom_feature_enabled: None,
            auth: None,
        };

        self.subscribe_async(subscription).await
    }

    /// Subscribe to market channel with custom features enabled
    /// Custom features include: best_bid_ask, new_market, market_resolved events
    pub async fn subscribe_market_channel_with_features(
        &mut self,
        asset_ids: Vec<String>,
    ) -> Result<()> {
        let subscription = WssSubscription {
            channel_type: "market".to_string(),
            operation: Some("subscribe".to_string()),
            markets: Vec::new(),
            asset_ids,
            initial_dump: Some(true),
            custom_feature_enabled: Some(true),
            auth: None,
        };

        self.subscribe_async(subscription).await
    }

    /// Unsubscribe from market channel
    pub async fn unsubscribe_market_channel(&mut self, asset_ids: Vec<String>) -> Result<()> {
        let subscription = WssSubscription {
            channel_type: "market".to_string(),
            operation: Some("unsubscribe".to_string()),
            markets: Vec::new(),
            asset_ids,
            initial_dump: None,
            custom_feature_enabled: None,
            auth: None,
        };

        self.subscribe_async(subscription).await
    }

    /// Unsubscribe from user channel
    pub async fn unsubscribe_user_channel(&mut self, markets: Vec<String>) -> Result<()> {
        let auth = self
            .auth
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("No authentication provided for WebSocket"))?
            .clone();

        let subscription = WssSubscription {
            channel_type: "user".to_string(),
            operation: Some("unsubscribe".to_string()),
            markets,
            asset_ids: Vec::new(),
            initial_dump: None,
            custom_feature_enabled: None,
            auth: Some(auth),
        };

        self.subscribe_async(subscription).await
    }

    /// Time of the most recent application-level text PONG on this connection.
    /// No server disconnect deadline is assumed from this timestamp.
    pub fn last_heartbeat_response(&self) -> Option<Instant> {
        self.last_heartbeat_response
    }

    fn terminate(&mut self) {
        self.state = ConnectionState::Closed;
        self.connection = None;
        self.heartbeat = None;
        self.writes = PendingWrites::default();
        self.pending.clear();
    }

    fn transport_error(&mut self, error: impl std::fmt::Display) -> PolyfillError {
        self.stats.errors += 1;
        self.terminate();
        PolyfillError::stream(
            format!("WebSocket connection lost: {error}"),
            crate::errors::StreamErrorKind::ConnectionLost,
        )
    }

    fn poll_writes(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if self.state == ConnectionState::Connected && self.writes.message.is_none() {
            if let Some(heartbeat) = &mut self.heartbeat {
                if heartbeat.poll_tick(cx).is_ready() {
                    self.writes.message = Some(Message::Text("PING".to_owned()));
                    // poll_tick resets its timer without registering the next wakeup.
                    // Arm it again even if the peer sends nothing after this PING.
                    let _ = heartbeat.poll_tick(cx);
                }
            }
        }
        if let Some(connection) = &mut self.connection {
            let queued = self.writes.message.is_some();
            let result = self.writes.poll(connection, cx);
            if queued && self.writes.message.is_none() {
                self.stats.messages_sent += 1;
            }
            match ready!(result) {
                Ok(()) => {},
                Err(error) => return Poll::Ready(Err(self.transport_error(error))),
            }
        }
        Poll::Ready(Ok(()))
    }

    /// Shared transport polling for both decoding paths.
    fn poll_text(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<String>>> {
        loop {
            ready!(self.poll_writes(cx))?;
            if self.state == ConnectionState::Closing {
                self.terminate();
                return Poll::Ready(None);
            }
            let Some(connection) = &mut self.connection else {
                return Poll::Ready(None);
            };
            match ready!(connection.poll_next_unpin(cx)) {
                Some(Ok(Message::Text(text))) if text == "PONG" => {
                    self.last_heartbeat_response = Some(Instant::now());
                },
                Some(Ok(Message::Text(text))) => return Poll::Ready(Some(Ok(text))),
                Some(Ok(Message::Ping(_))) => {
                    // Tungstenite queues the control PONG, which still needs flushing.
                    self.writes.flush = true;
                },
                Some(Ok(Message::Close(_))) => {
                    self.state = ConnectionState::Closing;
                    self.heartbeat = None;
                    self.writes.message = None;
                    self.writes.flush = true;
                },
                Some(Ok(_)) => {},
                Some(Err(error)) => return Poll::Ready(Some(Err(self.transport_error(error)))),
                None => {
                    self.terminate();
                    return Poll::Ready(None);
                },
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionState {
    Disconnected,
    Connected,
    Closing,
    Closed,
}

/// Retain writes across Pending and cancelled next() futures.
#[derive(Debug, Default)]
struct PendingWrites {
    message: Option<Message>,
    flush: bool,
}

impl PendingWrites {
    fn poll<S>(
        &mut self,
        sink: &mut S,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), S::Error>>
    where
        S: Sink<Message> + Unpin,
    {
        if self.message.is_some() {
            ready!(sink.poll_ready_unpin(cx))?;
            sink.start_send_unpin(self.message.take().expect("pending message"))?;
            self.flush = true;
        }
        if self.flush {
            ready!(sink.poll_flush_unpin(cx))?;
            self.flush = false;
        }
        Poll::Ready(Ok(()))
    }
}

/// WebSocket stream wrapper that applies `book` updates directly into an [`crate::book::OrderBookManager`].
///
/// This bypasses `StreamMessage` decoding (serde/DOM parsing) for the `book` hot path by using
/// [`WsBookUpdateProcessor`]. Non-`book` WS payloads are ignored.
///
/// Connection loss clears the subscribed books and ends the stream. A new applier
/// clears previous snapshots before accepting full `book` snapshots from its new subscription.
/// Do not share ownership of these books with another live feed.
///
/// Note: the underlying WS transport may still allocate when producing `Message::Text(String)`.
pub struct WebSocketBookApplier<'a> {
    stream: WebSocketStream,
    books: &'a crate::book::OrderBookManager,
    processor: WsBookUpdateProcessor,
    books_invalidated: bool,
}

impl WebSocketStream {
    /// Convert this stream into a book-applier stream.
    ///
    /// The caller is expected to "warm up" the [`crate::book::OrderBookManager`] by creating books for all
    /// subscribed asset IDs ahead of time. Missing books are treated as an error.
    pub fn into_book_applier<'a>(
        mut self,
        books: &'a crate::book::OrderBookManager,
        processor: WsBookUpdateProcessor,
    ) -> WebSocketBookApplier<'a> {
        // Drop any pre-parsed messages to avoid mixing the two streaming modes.
        self.pending.clear();
        let mut applier = WebSocketBookApplier {
            stream: self,
            books,
            processor,
            books_invalidated: false,
        };
        applier.invalidate_books();
        applier.stream.books_to_reset.clear();
        applier.books_invalidated = false;
        applier
    }
}

impl<'a> WebSocketBookApplier<'a> {
    fn reset_subscribed_books(&mut self) {
        // Subscriptions can also be changed through stream_mut() after construction.
        for asset in self.stream.books_to_reset.drain() {
            let _ = self.books.with_book_mut(&asset, |book| {
                book.invalidate();
                Ok(())
            });
        }
    }

    fn invalidate_books(&mut self) {
        if self.books_invalidated {
            return;
        }
        for asset in &self.stream.market_assets {
            let _ = self.books.with_book_mut(asset, |book| {
                book.invalidate();
                Ok(())
            });
        }
        self.books_invalidated = true;
    }

    /// Access the underlying WebSocket stream (e.g., for subscribe/unsubscribe calls).
    pub fn stream_mut(&mut self) -> &mut WebSocketStream {
        &mut self.stream
    }

    /// Current WebSocket connection stats.
    pub fn stream_stats(&self) -> StreamStats {
        self.stream.stats.clone()
    }

    /// Access the hot-path processor (e.g., to reuse it across connections).
    pub fn processor_mut(&mut self) -> &mut WsBookUpdateProcessor {
        &mut self.processor
    }

    /// Apply a single WS text payload (useful for custom transports and for testing).
    ///
    /// This convenience method consumes an owned `String`, so it may release that
    /// buffer after processing. Use [`Self::apply_bytes_message`] for the
    /// allocation-sensitive path when the caller owns a reusable mutable buffer.
    pub fn apply_text_message(&mut self, text: String) -> Result<WsBookApplyStats> {
        if matches!(
            self.stream.state,
            ConnectionState::Closing | ConnectionState::Closed
        ) {
            self.invalidate_books();
            return Err(PolyfillError::stream(
                "Book feed has ended, create a new stream and request fresh snapshots",
                crate::errors::StreamErrorKind::ConnectionLost,
            ));
        }
        self.reset_subscribed_books();
        let stats = self.processor.process_text(text, self.books)?;
        self.stream.stats.messages_received += 1;
        self.stream.stats.last_message_time = Some(Utc::now());
        Ok(stats)
    }

    /// Apply a single WS payload from a caller-owned mutable byte buffer.
    ///
    /// The buffer is mutated by `simd-json`. After processor warmup, this is the
    /// allocation-sensitive book-applier entry point because no owned message buffer
    /// is created or dropped by this method.
    pub fn apply_bytes_message(&mut self, bytes: &mut [u8]) -> Result<WsBookApplyStats> {
        if matches!(
            self.stream.state,
            ConnectionState::Closing | ConnectionState::Closed
        ) {
            self.invalidate_books();
            return Err(PolyfillError::stream(
                "Book feed has ended, create a new stream and request fresh snapshots",
                crate::errors::StreamErrorKind::ConnectionLost,
            ));
        }
        self.reset_subscribed_books();
        let stats = self.processor.process_bytes(bytes, self.books)?;
        self.stream.stats.messages_received += 1;
        self.stream.stats.last_message_time = Some(Utc::now());
        Ok(stats)
    }
}

impl<'a> Stream for WebSocketBookApplier<'a> {
    type Item = Result<WsBookApplyStats>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            self.reset_subscribed_books();
            let result = self.stream.poll_text(cx);
            if matches!(
                self.stream.state,
                ConnectionState::Closing | ConnectionState::Closed
            ) {
                self.invalidate_books();
            }
            let Some(text) = ready!(result) else {
                return Poll::Ready(None);
            };
            let text = text?;
            match self.apply_text_message(text) {
                Ok(stats) if stats.book_messages == 0 => continue,
                Ok(stats) => return Poll::Ready(Some(Ok(stats))),
                Err(error) => {
                    self.stream.stats.errors += 1;
                    return Poll::Ready(Some(Err(error)));
                },
            }
        }
    }
}

impl Stream for WebSocketStream {
    type Item = Result<StreamMessage>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // Heartbeats must also progress while a decoded batch is being drained.
            ready!(self.poll_writes(cx))?;
            if let Some(message) = self.pending.pop_front() {
                return Poll::Ready(Some(Ok(message)));
            }
            let Some(text) = ready!(self.poll_text(cx)) else {
                return Poll::Ready(None);
            };
            match crate::decode::parse_stream_messages(&text?) {
                Ok(messages) => {
                    let mut iter = messages.into_iter();
                    let Some(first) = iter.next() else {
                        continue;
                    };
                    for message in iter {
                        self.enqueue(message);
                    }
                    self.stats.messages_received += 1;
                    self.stats.last_message_time = Some(Utc::now());
                    return Poll::Ready(Some(Ok(first)));
                },
                Err(error) => {
                    self.stats.errors += 1;
                    return Poll::Ready(Some(Err(error)));
                },
            }
        }
    }
}

impl MarketStream for WebSocketStream {
    fn subscribe(&mut self, _subscription: Subscription) -> Result<()> {
        // This is for backward compatibility - use subscribe_async for new code
        Ok(())
    }

    fn unsubscribe(&mut self, _token_ids: &[String]) -> Result<()> {
        // This is for backward compatibility - use unsubscribe_async for new code
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.state == ConnectionState::Connected
    }

    fn get_stats(&self) -> StreamStats {
        self.stats.clone()
    }
}

/// Mock stream for testing
#[derive(Debug)]
pub struct MockStream {
    messages: Vec<Result<StreamMessage>>,
    index: usize,
    connected: bool,
}

impl Default for MockStream {
    fn default() -> Self {
        Self::new()
    }
}

impl MockStream {
    pub fn new() -> Self {
        Self {
            messages: Vec::new(),
            index: 0,
            connected: true,
        }
    }

    pub fn add_message(&mut self, message: StreamMessage) {
        self.messages.push(Ok(message));
    }

    pub fn add_error(&mut self, error: PolyfillError) {
        self.messages.push(Err(error));
    }

    pub fn set_connected(&mut self, connected: bool) {
        self.connected = connected;
    }
}

impl Stream for MockStream {
    type Item = Result<StreamMessage>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.index >= self.messages.len() {
            Poll::Ready(None)
        } else {
            let message = self.messages[self.index].clone();
            self.index += 1;
            Poll::Ready(Some(message))
        }
    }
}

impl MarketStream for MockStream {
    fn subscribe(&mut self, _subscription: Subscription) -> Result<()> {
        Ok(())
    }

    fn unsubscribe(&mut self, _token_ids: &[String]) -> Result<()> {
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.connected
    }

    fn get_stats(&self) -> StreamStats {
        StreamStats {
            messages_received: self.messages.len() as u64,
            messages_sent: 0,
            errors: self.messages.iter().filter(|m| m.is_err()).count() as u64,
            dropped_messages: 0,
            last_message_time: None,
            connection_uptime: std::time::Duration::ZERO,
            reconnect_count: 0,
        }
    }
}

/// Stream manager for handling multiple streams
#[allow(dead_code)]
pub struct StreamManager {
    streams: Vec<Box<dyn MarketStream>>,
    message_subscribers: Mutex<Vec<mpsc::UnboundedSender<StreamMessage>>>,
}

impl Default for StreamManager {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamManager {
    pub fn new() -> Self {
        Self {
            streams: Vec::new(),
            message_subscribers: Mutex::new(Vec::new()),
        }
    }

    pub fn add_stream(&mut self, stream: Box<dyn MarketStream>) {
        self.streams.push(stream);
    }

    pub fn get_message_receiver(&mut self) -> mpsc::UnboundedReceiver<StreamMessage> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.message_subscribers.lock().push(tx);
        rx
    }

    pub fn broadcast_message(&self, message: StreamMessage) -> Result<()> {
        let mut subscribers = self.message_subscribers.lock();
        subscribers.retain(|tx| tx.send(message.clone()).is_ok());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    const BOOK: &str = r#"{"event_type":"book","market":"test","asset_id":"12345","timestamp":"1","bids":[{"price":"0.50","size":"10"}],"asks":[{"price":"0.60","size":"5"}]}"#;

    async fn heartbeat_peer() -> (WebSocketStream, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(socket).await.unwrap();
            assert!(matches!(
                socket.next().await.unwrap().unwrap(),
                Message::Text(_)
            ));
            assert_eq!(
                socket.next().await.unwrap().unwrap(),
                Message::Text("PING".into())
            );
            socket.send(Message::Text("PONG".into())).await.unwrap();
            socket.send(Message::Ping(vec![1, 2, 3])).await.unwrap();
            socket.send(Message::Text(BOOK.into())).await.unwrap();
            assert_eq!(
                socket.next().await.unwrap().unwrap(),
                Message::Pong(vec![1, 2, 3])
            );
            socket.close(None).await.unwrap();
            // Drive the peer until the client's close acknowledgement arrives.
            let _ = socket.next().await;
        });
        let mut stream = WebSocketStream::new(&url);
        stream
            .subscribe_market_channel(vec!["12345".into()])
            .await
            .unwrap();
        // Make the first production heartbeat due now so the network test stays fast.
        stream.heartbeat = Some(tokio::time::interval_at(
            Instant::now(),
            Duration::from_secs(10),
        ));
        (stream, peer)
    }

    #[tokio::test]
    async fn test_transport_heartbeat_and_terminal_close() {
        let (mut stream, peer) = heartbeat_peer().await;
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            StreamMessage::Book(_)
        ));
        assert!(stream.last_heartbeat_response().is_some());
        assert_eq!(stream.get_stats().errors, 0);
        assert_eq!(stream.get_stats().messages_sent, 2);
        assert!(stream.next().await.is_none());
        assert!(!stream.is_connected());
        assert!(stream.next().await.is_none());
        assert!(stream
            .subscribe_market_channel(vec!["12345".into()])
            .await
            .is_err());
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn test_book_transport_heartbeat_close_and_fresh_snapshot() {
        let (stream, peer) = heartbeat_peer().await;
        let books = crate::book::OrderBookManager::new(64);
        books.get_or_create_book("12345").unwrap();
        let StreamMessage::Book(mut old) = crate::decode::parse_stream_messages(BOOK)
            .unwrap()
            .remove(0)
        else {
            unreachable!();
        };
        old.timestamp = 100;
        books.apply_book_update(&old).unwrap();
        let mut applier = stream.into_book_applier(&books, WsBookUpdateProcessor::new(1024));
        assert!(books.get_book("12345").unwrap().bids.is_empty());
        assert_eq!(applier.next().await.unwrap().unwrap().book_messages, 1);
        assert!(applier.stream_mut().last_heartbeat_response().is_some());
        assert_eq!(applier.stream_stats().errors, 0);
        assert_eq!(books.get_book("12345").unwrap().bids.len(), 1);
        assert!(applier.next().await.is_none());
        assert!(!applier.stream_mut().is_connected());
        assert!(books.get_book("12345").unwrap().bids.is_empty());
        assert!(books.get_book("12345").unwrap().asks.is_empty());
        assert!(applier.apply_text_message(BOOK.into()).is_err());
        assert!(applier.next().await.is_none());
        peer.await.unwrap();

        // The next subscription starts from a full snapshot, even with the same timestamp.
        let (stream, peer) = heartbeat_peer().await;
        let mut applier = stream.into_book_applier(&books, WsBookUpdateProcessor::new(1024));
        assert_eq!(applier.next().await.unwrap().unwrap().book_messages, 1);
        assert_eq!(books.get_book("12345").unwrap().bids.len(), 1);
        assert!(applier.next().await.is_none());
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn test_heartbeat_interval_survives_cancelled_reads_and_buffered_messages() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let connect = async {
            let mut stream = WebSocketStream::new(&url);
            stream
                .subscribe_market_channel(vec!["12345".into()])
                .await
                .unwrap();
            stream
        };
        let accept = async {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(socket).await.unwrap();
            socket.next().await.unwrap().unwrap();
            socket
        };
        let (mut stream, mut peer) = tokio::join!(connect, accept);
        tokio::time::pause();
        let message = crate::decode::parse_stream_messages(BOOK)
            .unwrap()
            .remove(0);
        stream.enqueue(message.clone());
        assert!(futures::poll!(stream.next()).is_ready());
        tokio::time::advance(Duration::from_secs(9)).await;
        stream.enqueue(message.clone());
        assert!(futures::poll!(stream.next()).is_ready());
        assert_eq!(stream.stats.messages_sent, 1); // Subscription only.
                                                   // Tokio rounds timer deadlines to millisecond precision.
        tokio::time::advance(Duration::from_millis(1001)).await;
        stream.enqueue(message.clone());
        assert!(futures::poll!(stream.next()).is_ready());
        assert_eq!(stream.stats.messages_sent, 2);
        tokio::time::advance(Duration::from_secs(10)).await;
        stream.enqueue(message);
        assert!(futures::poll!(stream.next()).is_ready());
        assert_eq!(stream.stats.messages_sent, 3);
        assert_eq!(
            peer.next().await.unwrap().unwrap(),
            Message::Text("PING".into())
        );
        assert_eq!(
            peer.next().await.unwrap().unwrap(),
            Message::Text("PING".into())
        );
        // Dropping a pending next() must not lose transport state.
        assert!(futures::poll!(stream.next()).is_pending());
        peer.send(Message::Text("PONG".into())).await.unwrap();
        peer.send(Message::Text(BOOK.into())).await.unwrap();
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            StreamMessage::Book(_)
        ));
        assert!(stream.last_heartbeat_response().is_some());
        assert_eq!(stream.stats.errors, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn test_heartbeat_rearms_wakeup_without_incoming_data() {
        let mut stream = WebSocketStream::new("ws://unused");
        stream.state = ConnectionState::Connected;
        stream.heartbeat = Some(tokio::time::interval_at(
            Instant::now(),
            Duration::from_secs(10),
        ));
        let mut task = tokio_test::task::spawn(stream);
        task.enter(|cx, mut stream| {
            assert!(stream.poll_writes(cx).is_ready());
            assert_eq!(
                stream.writes.message.take(),
                Some(Message::Text("PING".into()))
            );
        });
        assert!(!task.is_woken());
        tokio::time::advance(Duration::from_secs(10)).await;
        assert!(task.is_woken());
        task.enter(|cx, mut stream| {
            assert!(stream.poll_writes(cx).is_ready());
            assert_eq!(
                stream.writes.message.take(),
                Some(Message::Text("PING".into()))
            );
        });
        assert!(!task.is_woken());
        tokio::time::advance(Duration::from_secs(10)).await;
        assert!(task.is_woken());
    }

    #[test]
    fn test_pending_write_retains_message_and_flush_state() {
        #[derive(Default)]
        struct BlockedSink {
            ready: bool,
            flushed: bool,
            sent: Vec<Message>,
        }
        impl Sink<Message> for BlockedSink {
            type Error = std::convert::Infallible;
            fn poll_ready(
                self: Pin<&mut Self>,
                _: &mut Context<'_>,
            ) -> Poll<std::result::Result<(), Self::Error>> {
                if self.ready {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                }
            }
            fn start_send(
                mut self: Pin<&mut Self>,
                message: Message,
            ) -> std::result::Result<(), Self::Error> {
                self.sent.push(message);
                Ok(())
            }
            fn poll_flush(
                self: Pin<&mut Self>,
                _: &mut Context<'_>,
            ) -> Poll<std::result::Result<(), Self::Error>> {
                if self.flushed {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                }
            }
            fn poll_close(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<std::result::Result<(), Self::Error>> {
                self.poll_flush(cx)
            }
        }
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut sink = BlockedSink::default();
        let mut writes = PendingWrites {
            message: Some(Message::Text("PING".into())),
            flush: false,
        };
        assert!(writes.poll(&mut sink, &mut cx).is_pending());
        assert!(writes.message.is_some());
        sink.ready = true;
        assert!(writes.poll(&mut sink, &mut cx).is_pending());
        assert!(writes.message.is_none());
        assert!(writes.flush);
        assert!(writes.poll(&mut sink, &mut cx).is_pending());
        assert_eq!(sink.sent.len(), 1);
        sink.flushed = true;
        assert!(writes.poll(&mut sink, &mut cx).is_ready());
        assert!(!writes.flush);
        assert_eq!(sink.sent, [Message::Text("PING".into())]);
    }

    #[tokio::test]
    async fn test_transport_error_is_terminal_and_invalidates_books() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(socket).await.unwrap();
            socket.next().await.unwrap().unwrap();
            socket.send(Message::Text(BOOK.into())).await.unwrap();
            // Drop TCP without a WebSocket close handshake.
        });
        let mut stream = WebSocketStream::new(&url);
        stream
            .subscribe_market_channel(vec!["12345".into()])
            .await
            .unwrap();
        let books = crate::book::OrderBookManager::new(64);
        books.get_or_create_book("12345").unwrap();
        let mut applier = stream.into_book_applier(&books, WsBookUpdateProcessor::new(1024));
        assert!(applier.next().await.unwrap().is_ok());
        assert!(matches!(
            applier.next().await.unwrap(),
            Err(PolyfillError::Stream {
                kind: crate::errors::StreamErrorKind::ConnectionLost,
                ..
            })
        ));
        assert!(applier.next().await.is_none());
        assert!(!applier.stream_mut().is_connected());
        assert!(books.get_book("12345").unwrap().bids.is_empty());
        peer.await.unwrap();
    }

    #[test]
    fn test_mock_stream() {
        let mut stream = MockStream::new();

        // Add some test messages
        stream.add_message(StreamMessage::Book(BookUpdate {
            asset_id: "1".to_string(),
            market: "0xabc".to_string(),
            timestamp: 1_234_567_890,
            bids: vec![],
            asks: vec![],
            hash: None,
        }));
        stream.add_message(StreamMessage::PriceChange(PriceChange {
            market: "0xabc".to_string(),
            timestamp: 1_234_567_891,
            price_changes: vec![],
        }));

        assert!(stream.is_connected());
        assert_eq!(stream.get_stats().messages_received, 2);
    }

    #[test]
    fn test_stream_manager() {
        let mut manager = StreamManager::new();
        let mock_stream = Box::new(MockStream::new());
        manager.add_stream(mock_stream);
        let mut first_rx = manager.get_message_receiver();
        let mut second_rx = manager.get_message_receiver();

        // Test message broadcasting
        let message = StreamMessage::Book(BookUpdate {
            asset_id: "1".to_string(),
            market: "0xabc".to_string(),
            timestamp: 1_234_567_890,
            bids: vec![],
            asks: vec![],
            hash: None,
        });
        assert!(manager.broadcast_message(message).is_ok());

        assert!(matches!(
            first_rx.try_recv().unwrap(),
            StreamMessage::Book(BookUpdate { asset_id, .. }) if asset_id == "1"
        ));
        assert!(matches!(
            second_rx.try_recv().unwrap(),
            StreamMessage::Book(BookUpdate { asset_id, .. }) if asset_id == "1"
        ));

        drop(first_rx);
        let message = StreamMessage::PriceChange(PriceChange {
            market: "0xabc".to_string(),
            timestamp: 1_234_567_891,
            price_changes: vec![],
        });
        assert!(manager.broadcast_message(message).is_ok());

        assert!(matches!(
            second_rx.try_recv().unwrap(),
            StreamMessage::PriceChange(PriceChange { market, .. }) if market == "0xabc"
        ));
    }

    #[test]
    fn test_websocket_book_applier_apply_text_message_updates_book() {
        let books = crate::book::OrderBookManager::new(64);
        let _ = books.get_or_create_book("12345").unwrap();

        let processor = WsBookUpdateProcessor::new(1024);
        let stream = WebSocketStream::new("wss://example.com/ws");
        let mut applier = stream.into_book_applier(&books, processor);

        let msg = r#"{"event_type":"book","asset_id":"12345","timestamp":1,"bids":[{"price":"0.75","size":"10"}],"asks":[{"price":"0.76","size":"5"}]}"#.to_string();
        let stats = applier.apply_text_message(msg).unwrap();
        assert_eq!(stats.book_messages, 1);
        assert_eq!(stats.book_levels_applied, 2);

        let snapshot = books.get_book("12345").unwrap();
        assert_eq!(snapshot.bids.len(), 1);
        assert_eq!(snapshot.asks.len(), 1);
        assert_eq!(snapshot.bids[0].price, Decimal::from_str("0.75").unwrap());
        assert_eq!(snapshot.bids[0].size, Decimal::from_str("10").unwrap());
        assert_eq!(snapshot.asks[0].price, Decimal::from_str("0.76").unwrap());
        assert_eq!(snapshot.asks[0].size, Decimal::from_str("5").unwrap());
    }

    #[test]
    fn test_websocket_book_applier_replaces_snapshot() {
        let books = crate::book::OrderBookManager::new(64);
        let _ = books.get_or_create_book("12345").unwrap();

        let processor = WsBookUpdateProcessor::new(1024);
        let stream = WebSocketStream::new("wss://example.com/ws");
        let mut applier = stream.into_book_applier(&books, processor);

        let first = r#"{"event_type":"book","asset_id":"12345","timestamp":"1757908892351","bids":[{"price":"0.74","size":"8"},{"price":"0.75","size":"10"}],"asks":[{"price":"0.76","size":"5"},{"price":"0.77","size":"4"}]}"#.to_string();
        applier.apply_text_message(first).unwrap();

        let second = r#"{"event_type":"book","asset_id":"12345","timestamp":"1757908892352","bids":[{"price":"0.75","size":"11"}],"asks":[{"price":"0.76","size":"6"}]}"#.to_string();
        let stats = applier.apply_text_message(second).unwrap();
        assert_eq!(stats.book_messages, 1);
        assert_eq!(stats.book_levels_applied, 2);

        let snapshot = books.get_book("12345").unwrap();
        assert_eq!(snapshot.timestamp.timestamp_millis(), 1_757_908_892_352);
        assert_eq!(snapshot.bids.len(), 1);
        assert_eq!(snapshot.asks.len(), 1);
        assert_eq!(snapshot.bids[0].price, Decimal::from_str("0.75").unwrap());
        assert_eq!(snapshot.bids[0].size, Decimal::from_str("11").unwrap());
        assert_eq!(snapshot.asks[0].price, Decimal::from_str("0.76").unwrap());
        assert_eq!(snapshot.asks[0].size, Decimal::from_str("6").unwrap());
    }
}
