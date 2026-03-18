use std::sync::{Arc, Mutex};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, NaiveDateTime, Utc};
use mnemonic_engine::{
    Cell, CellType, ContentFormat, Engine, EngineError, Result as EngineResult, Timestamp,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone)]
pub struct AppContext {
    engine: Arc<Mutex<Engine>>,
}

impl AppContext {
    pub fn new(engine: Engine) -> Self {
        Self {
            engine: Arc::new(Mutex::new(engine)),
        }
    }

    fn with_engine<T>(&self, f: impl FnOnce(&Engine) -> EngineResult<T>) -> Result<T, ApiError> {
        let guard = self
            .engine
            .lock()
            .map_err(|_| ApiError::internal("failed to lock engine state"))?;
        f(&guard).map_err(ApiError::from)
    }
}

#[derive(Debug, Serialize)]
pub struct ApiError {
    code: &'static str,
    message: String,
}

impl ApiError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self::new("internal", message)
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self::new("not_found", message)
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self::new("bad_request", message)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self.code {
            "not_found" => StatusCode::NOT_FOUND,
            "bad_request" => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(self)).into_response()
    }
}

impl From<EngineError> for ApiError {
    fn from(value: EngineError) -> Self {
        match value {
            EngineError::NotFound => Self::not_found("document was not found"),
            EngineError::InvalidData(msg) => Self::bad_request(msg),
            EngineError::Conflict(msg) => Self::bad_request(msg),
            other => Self::internal(other.to_string()),
        }
    }
}

type ApiResult<T> = Result<Json<T>, ApiError>;

#[derive(Debug, Serialize, Deserialize)]
pub struct DocumentResponse {
    // Wire compatibility: the public concept is Document, while the current payload
    // still carries the root Cell representation directly.
    pub root_cell_id: Uuid,
    pub root: Cell,
}

impl DocumentResponse {
    fn from_root(root: Cell) -> Self {
        Self {
            root_cell_id: root.id,
            root,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ApiRoute {
    method: &'static str,
    path: &'static str,
    description: &'static str,
}

#[derive(Debug, Serialize)]
pub struct ApiIndex {
    service: &'static str,
    routes: Vec<ApiRoute>,
}

#[derive(Debug, Deserialize)]
pub struct CreateDocumentRequest {
    pub cell_type: CellType,
    pub format: ContentFormat,
    pub content: String,
}

#[derive(Debug, Deserialize)]
pub struct UpdateDocumentRequest {
    pub content: String,
}

#[derive(Debug, Deserialize)]
pub struct DocumentHistoryQuery {
    pub timestamp: Option<String>,
}

fn parse_timestamp(value: &str) -> Result<Timestamp, ApiError> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        return Ok(dt.with_timezone(&Utc));
    }

    let naive = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S"))
        .map_err(|err| ApiError::bad_request(format!("invalid timestamp '{value}': {err}")))?;
    Ok(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
}

pub fn router(context: AppContext) -> Router {
    Router::new()
        .route("/", get(api_index))
        .route("/health", get(health))
        .route("/documents", post(create_document))
        .route("/documents/{id}", get(get_document).put(update_document))
        .route("/documents/{id}/history", get(get_document_history))
        .with_state(context)
}

async fn api_index() -> Json<ApiIndex> {
    Json(ApiIndex {
        service: "mnemonic-server",
        routes: vec![
            ApiRoute {
                method: "GET",
                path: "/",
                description: "Simple route index for API navigation",
            },
            ApiRoute {
                method: "GET",
                path: "/health",
                description: "Health probe",
            },
            ApiRoute {
                method: "POST",
                path: "/documents",
                description: "Create a new document; the current response carries the root cell payload",
            },
            ApiRoute {
                method: "GET",
                path: "/documents/{id}",
                description: "Fetch the current document view through its root cell payload",
            },
            ApiRoute {
                method: "PUT",
                path: "/documents/{id}",
                description: "Create the next document version by updating the root cell payload",
            },
            ApiRoute {
                method: "GET",
                path: "/documents/{id}/history?timestamp=<ts>",
                description: "Fetch the document view at a specific timestamp through its root cell payload",
            },
        ],
    })
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn create_document(
    State(context): State<AppContext>,
    Json(request): Json<CreateDocumentRequest>,
) -> ApiResult<DocumentResponse> {
    let doc = context.with_engine(|engine| {
        engine.create_cell(
            request.cell_type,
            request.format,
            request.content.into_bytes(),
        )
    })?;
    Ok(Json(DocumentResponse::from_root(doc)))
}

async fn get_document(
    State(context): State<AppContext>,
    Path(id): Path<Uuid>,
) -> ApiResult<DocumentResponse> {
    let doc = context.with_engine(|engine| engine.get_cell(id))?;
    Ok(Json(DocumentResponse::from_root(doc)))
}

async fn update_document(
    State(context): State<AppContext>,
    Path(id): Path<Uuid>,
    Json(request): Json<UpdateDocumentRequest>,
) -> ApiResult<DocumentResponse> {
    let doc = context
        .with_engine(|engine| engine.update_cell_content(id, request.content.into_bytes()))?;
    Ok(Json(DocumentResponse::from_root(doc)))
}

async fn get_document_history(
    State(context): State<AppContext>,
    Path(id): Path<Uuid>,
    Query(query): Query<DocumentHistoryQuery>,
) -> ApiResult<DocumentResponse> {
    let Some(timestamp) = query.timestamp else {
        return Err(ApiError::bad_request(
            "timestamp query parameter is required",
        ));
    };
    let parsed = parse_timestamp(&timestamp)?;
    let doc = context.with_engine(|engine| engine.get_cell_at(id, parsed))?;
    Ok(Json(DocumentResponse::from_root(doc)))
}
