use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::stream::{FusedStream, Stream, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tungstenite::protocol::Message;
use url::Url;

use super::builder::SubscriberBuilder;
use super::request::get_request_builder;
use crate::auth::Auth;
use crate::error::Error;
use crate::payload::ReceivedPayload;

/// Async subscriber
#[derive(Debug, Clone)]
pub struct Async {
    auth: Option<Auth>,
}

impl Async {
    #[inline]
    pub(crate) fn new(builder: SubscriberBuilder) -> Result<Self, Error> {
        Ok(Self { auth: builder.auth })
    }

    pub(crate) async fn subscribe(&self, url: &Url, topic: &str) -> Result<MessageStream, Error> {
        let builder = get_request_builder(url, topic, &self.auth)?;

        // Create message iterator
        Ok(MessageStream {
            socket: connect_async(builder).await?.0,
        })
    }
}

pub struct MessageStream {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl Stream for MessageStream {
    type Item = Result<ReceivedPayload, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let text_message = loop {
            if self.socket.is_terminated() {
                return Poll::Ready(None);
            }

            let message = match self.socket.poll_next_unpin(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(message))) => message,
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(Error::from(error)))),
                Poll::Ready(None) => return Poll::Ready(None),
            };

            match message {
                Message::Close(_) => return Poll::Ready(None),
                Message::Text(text_message) => break text_message,
                _ => {}
            }
        };

        match serde_json::from_str(text_message.as_str()) {
            Ok(received_message) => Poll::Ready(Some(Ok(received_message))),
            Err(error) => Poll::Ready(Some(Err(Error::from(error)))),
        }
    }
}

#[cfg(test)]
mod tests {
    use futures_util::{SinkExt, StreamExt};
    use tokio::net::TcpListener;
    use tokio_tungstenite::{accept_async, connect_async};
    use tungstenite::protocol::Message;

    use super::MessageStream;
    use crate::payload::ReceivedMessageType;

    #[tokio::test]
    async fn replies_to_ping_without_manual_pong_handling() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();

            // Send a ping
            socket
                .send(Message::Ping(vec![1, 2, 3].into()))
                .await
                .unwrap();

            // Wait that the client stream replies with a pong
            match socket.next().await.unwrap().unwrap() {
                Message::Pong(data) => assert_eq!(data.as_ref(), &[1, 2, 3]),
                other => panic!("expected pong, got {other:?}"),
            }

            let payload = serde_json::json!({
                "id": "message-id",
                "time": 1,
                "event": "message",
                "topic": "topic",
                "message": "hello",
            });
            socket
                .send(Message::Text(payload.to_string().into()))
                .await
                .unwrap();
        });

        let (socket, _) = connect_async(format!("ws://{addr}")).await.unwrap();

        let mut stream = MessageStream { socket };

        // Wait for a payload
        let payload = stream.next().await.unwrap().unwrap();

        assert_eq!(payload.id, "message-id");
        assert_eq!(payload.event, ReceivedMessageType::Message);
        assert_eq!(payload.topic, "topic");
        assert_eq!(payload.message.as_deref(), Some("hello"));

        server.await.unwrap();
    }
}
