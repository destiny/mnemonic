use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
};
use http_body_util::BodyExt;
use mnemonic_engine::{CellType, ContentFormat, Engine};
use mnemonic_server::{AppContext, DocumentResponse, router};
use tower::util::ServiceExt;

fn test_app() -> axum::Router {
    let engine = Engine::new(":memory:").expect("in-memory engine should initialize");
    router(AppContext::new(engine))
}

#[tokio::test]
async fn health_endpoint_is_ok() {
    let app = test_app();

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn create_get_update_and_history_document() {
    let app = test_app();

    let create_payload = r##"{
      "cell_type": "Data",
      "format": "Markdown",
      "content": "# Draft"
    }"##;

    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/documents")
                .header("content-type", "application/json")
                .body(Body::from(create_payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(create_response.status(), StatusCode::OK);

    let created_bytes = create_response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let created: DocumentResponse = serde_json::from_slice(&created_bytes).unwrap();
    assert_eq!(created.root.cell_type, CellType::Data);
    assert_eq!(created.root.format, ContentFormat::Markdown);
    assert_eq!(created.root_cell_id, created.root.id);

    let get_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/documents/{}", created.root_cell_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get_response.status(), StatusCode::OK);
    let get_bytes = get_response.into_body().collect().await.unwrap().to_bytes();
    let current: DocumentResponse = serde_json::from_slice(&get_bytes).unwrap();
    assert_eq!(current.root.content, b"# Draft".to_vec());

    let history_ts = current.root.valid_from.to_rfc3339();

    let update_payload = r##"{"content":"# Draft v2"}"##;
    let update_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(format!("/documents/{}", created.root_cell_id))
                .header("content-type", "application/json")
                .body(Body::from(update_payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(update_response.status(), StatusCode::OK);

    let history_response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!(
                    "/documents/{}/history?timestamp={}",
                    created.root_cell_id, history_ts
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(history_response.status(), StatusCode::OK);
    let history_bytes = history_response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let historic: DocumentResponse = serde_json::from_slice(&history_bytes).unwrap();
    assert_eq!(historic.root.content, b"# Draft".to_vec());
}
