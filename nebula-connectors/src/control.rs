use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    Router,
    body::to_bytes,
    extract::{Path, Request, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use nebula_connector_sdk::validation::decode_control;
use serde::Deserialize;
use zeroize::Zeroizing;

use crate::webhook::{self, Provider};

#[cfg(test)]
mod tests;
mod transport;
mod workers;

const MAX_RPC_BYTES: usize = 1 << 20;
const CALLBACK_DEADLINE: Duration = Duration::from_secs(2);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credentials {
    pub tokens: Vec<String>,
}

#[derive(Clone)]
pub struct Runtime(Arc<Inner>);

struct Inner {
    tokens: Vec<Zeroizing<String>>,
    callbacks: BTreeMap<Provider, workers::Pool>,
}

impl Runtime {
    pub fn new(credentials: Credentials) -> Result<Self, webhook::Error> {
        Self::with_processor(credentials, Arc::new(process))
    }

    fn with_processor(
        credentials: Credentials,
        processor: Arc<workers::Processor>,
    ) -> Result<Self, webhook::Error> {
        if credentials.tokens.is_empty()
            || credentials.tokens.len() > 2
            || credentials.tokens.iter().any(|v| {
                v.len() < 32
                    || v.len() > 512
                    || !v
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
            })
        {
            return Err(webhook::Error::Invalid);
        }
        Ok(Self(Arc::new(Inner {
            tokens: credentials.tokens.into_iter().map(Zeroizing::new).collect(),
            callbacks: Provider::ALL
                .into_iter()
                .map(|p| workers::Pool::new(p, processor.clone()).map(|pool| (p, pool)))
                .collect::<Result<_, _>>()
                .map_err(|_| webhook::Error::Invalid)?,
        })))
    }

    fn router(&self) -> Router {
        Router::new()
            .route(
                "/healthz",
                get(|| async { closed_connection(StatusCode::NO_CONTENT.into_response()) }),
            )
            .route(
                "/v1/providers/{provider}/webhook/verify",
                post(verify_callback),
            )
            .with_state(self.clone())
    }

    pub fn drain(&self) {
        for permits in self.0.callbacks.values() {
            permits.close();
        }
    }
}

async fn verify_callback(
    State(runtime): State<Runtime>,
    Path(provider): Path<Provider>,
    request: Request,
) -> Response {
    let headers = request.headers();
    let supplied = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "));
    if headers.get_all(header::AUTHORIZATION).iter().count() != 1
        || !supplied.is_some_and(|s| {
            s.len() <= 512
                && runtime
                    .0
                    .tokens
                    .iter()
                    .any(|expected| webhook::secret_equal(expected, s))
        })
    {
        return error(StatusCode::UNAUTHORIZED, "workload_not_authorized");
    }
    if headers
        .get(header::CONTENT_TYPE)
        .and_then(|s| s.to_str().ok())
        != Some("application/json")
    {
        return error(StatusCode::UNSUPPORTED_MEDIA_TYPE, "json_required");
    }
    let pool = &runtime.0.callbacks[&provider];
    let permit = match pool.acquire() {
        Some(permit) => permit,
        None => return error(StatusCode::SERVICE_UNAVAILABLE, "callback_capacity"),
    };
    let deadline = Instant::now() + CALLBACK_DEADLINE;
    let outcome = tokio::time::timeout(CALLBACK_DEADLINE, async {
        let bytes = to_bytes(request.into_body(), MAX_RPC_BYTES)
            .await
            .map_err(|_| (StatusCode::PAYLOAD_TOO_LARGE, "callback_limit"))?;
        let receive = pool
            .submit(bytes.to_vec(), deadline, permit)
            .ok_or((StatusCode::SERVICE_UNAVAILABLE, "callback_capacity"))?;
        receive
            .await
            .ok()
            .flatten()
            .ok_or((StatusCode::SERVICE_UNAVAILABLE, "callback_deadline"))?
            .map_err(|err| match err {
                webhook::Error::Invalid => (StatusCode::BAD_REQUEST, "invalid_callback"),
                webhook::Error::Unauthorized => (StatusCode::UNAUTHORIZED, "callback_not_verified"),
                webhook::Error::Unsupported => {
                    (StatusCode::UNPROCESSABLE_ENTITY, "callback_not_supported")
                }
            })
    })
    .await;
    match outcome {
        Ok(Ok(result)) => {
            let mut response = axum::Json(result).into_response();
            response.headers_mut().insert(
                header::CACHE_CONTROL,
                header::HeaderValue::from_static("no-store"),
            );
            closed_connection(response)
        }
        Ok(Err((status, code))) => error(status, code),
        Err(_) => error(StatusCode::SERVICE_UNAVAILABLE, "callback_deadline"),
    }
}

fn error(status: StatusCode, code: &'static str) -> Response {
    let mut response = (status, axum::Json(serde_json::json!({"error":code}))).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    if status == StatusCode::SERVICE_UNAVAILABLE {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, header::HeaderValue::from_static("1"));
    }
    closed_connection(response)
}

fn closed_connection(mut response: Response) -> Response {
    response.headers_mut().insert(
        header::CONNECTION,
        header::HeaderValue::from_static("close"),
    );
    response
}

fn process(provider: Provider, bytes: &[u8]) -> Result<webhook::Response, webhook::Error> {
    let request: webhook::Request =
        decode_control(bytes, MAX_RPC_BYTES).map_err(|_| webhook::Error::Invalid)?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| webhook::Error::Invalid)?
        .as_secs();
    webhook::verify(provider, request, now)
}

/// TLS terminates at the workload ingress. Only the Go broker may reach this
/// listener; its identity and token are independent of provider/database keys.
pub async fn serve(
    listener: tokio::net::TcpListener,
    runtime: Runtime,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let (send, receive) = tokio::sync::oneshot::channel();
    let (stop_io, stopped) = tokio::sync::watch::channel(false);
    let mut server = tokio::spawn(
        axum::serve(
            transport::BoundedListener::new(listener, stopped),
            runtime.router(),
        )
        .with_graceful_shutdown(async {
            let _ = receive.await;
        })
        .into_future(),
    );
    tokio::select! {
        result = &mut server => result.map_err(std::io::Error::other)?,
        () = shutdown => {
            runtime.drain();
            let _ = send.send(());
            match tokio::time::timeout(Duration::from_secs(3), &mut server).await {
                Ok(result) => result.map_err(std::io::Error::other)?,
                Err(_) => { let _ = stop_io.send(true); server.abort(); let _ = server.await; Ok(()) },
            }
        }
    }
}
