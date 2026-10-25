//! Idle platform subscriptions share the server's transport retirement cut.
use acyclic_local_server::{Registry, Server, client};
use acyclic_stream::wire;
use futures::{SinkExt, channel::mpsc};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn shutdown_terminates_idle_follow_and_session_without_client_disconnect() -> TestResult {
    let server = Server::start(
        std::sync::Arc::new(acyclic_stream::MemoryStream::default()),
        acyclic_fs::Fs::memory(),
        Registry::default(),
    )
    .await?;
    let channel = client::channel(&server.endpoint()).await?;
    let mut streams = wire::stream_service_client::StreamServiceClient::new(channel.clone());
    streams
        .append(wire::AppendRequest {
            path: "retirement".into(),
            records: vec![wire::AppendRecord {
                value: b"seed".to_vec().into(),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await?;
    let mut follow = streams
        .follow(wire::FollowRequest {
            path: "retirement".into(),
            from: 0,
            filter: None,
        })
        .await?
        .into_inner();
    assert!(
        follow.message().await?.is_some(),
        "follow must be installed before retiring"
    );
    let mut sessions =
        wire::stream_session_service_client::StreamSessionServiceClient::new(channel);
    let (mut send, receive) = mpsc::channel(1);
    send.send(wire::SessionFrame {
        body: Some(wire::session_frame::Body::Hello(wire::SessionHello {
            version: 1,
            client_id: b"retirement".to_vec().into(),
            destination_node: "local".into(),
            receive_bytes: 1024,
            receive_frames: 4,
            credentials: vec![acyclic_stream::session::testing::credential()],
        })),
        ..Default::default()
    })
    .await?;
    let mut session = sessions.session(receive).await?.into_inner();
    assert!(
        matches!(
            session.message().await?.and_then(|frame| frame.body),
            Some(wire::session_frame::Body::Welcome(_))
        ),
        "session must be installed before retiring"
    );
    // Both clients and the bidirectional request sender deliberately stay open.
    // Server shutdown owns the cut; it cannot depend on their destruction.
    server.shutdown().await?;
    for status in [
        follow.message().await.unwrap_err(),
        session.message().await.unwrap_err(),
    ] {
        assert_eq!(
            status.code(),
            tonic::Code::Unavailable,
            "retirement must be a gRPC terminal status"
        );
    }
    drop(send);
    Ok(())
}
