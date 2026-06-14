use std::{env, time::Duration};

use anyhow::{Context, Result};
use responses_api_store_client::{stored_response_status, StoredResponse};

use crate::responses_store::{is_retryable_store_error, StoreHandle};
use reqwest::Client as HttpClient;
use serde_json::{json, Value};

struct ClaimedWork {
    upstream: String,
    upstream_request: Value,
    input: Vec<Value>,
    upstream_authorization: Option<String>,
}

#[derive(Debug)]
pub struct RetryableCompletionError {
    pub completion: StoredResponse,
}

#[derive(Debug)]
pub struct RetryableLoadError;

impl std::fmt::Display for RetryableCompletionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("failed to persist completed background response; will retry")
    }
}

impl std::error::Error for RetryableCompletionError {}

impl std::fmt::Display for RetryableLoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("failed to load background response for hydration retry; will retry")
    }
}

impl std::error::Error for RetryableLoadError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProcessOutcome {
    /// The stream entry can be acknowledged and removed.
    Ack,
    /// Leave the entry pending for redelivery.
    Retry,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EntrySource {
    /// Entry was returned by `XAUTOCLAIM` and already met the min-idle threshold.
    Autoclaimed,
    /// Entry was read from this consumer's pending list at startup.
    StartupPending,
    /// Entry was delivered live via `XREADGROUP`.
    #[default]
    Live,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ProcessContext {
    pub message_idle_ms: Option<u64>,
    pub autoclaim_min_idle_ms: usize,
    pub entry_source: EntrySource,
    pub hydration_retry: bool,
}

pub fn upstream_http_client() -> Result<HttpClient> {
    HttpClient::builder()
        .timeout(upstream_timeout_from_env())
        .build()
        .context("failed to build upstream HTTP client")
}

pub async fn process_response(
    response_store: &StoreHandle,
    http: &HttpClient,
    response_id: &str,
    ctx: ProcessContext,
) -> Result<ProcessOutcome> {
    let upstream_api_key = env::var("UPSTREAM_API_KEY").ok();

    let stored = match response_store.load(response_id).await {
        Ok(Some(stored)) => stored,
        Ok(None) => return Ok(ProcessOutcome::Ack),
        Err(err) if ctx.hydration_retry && is_retryable_store_error(&err) => {
            return Err(RetryableLoadError.into());
        }
        Err(err) => return Err(err),
    };
    match pre_claim_action(&stored, ctx) {
        PreClaimAction::Ack => return Ok(ProcessOutcome::Ack),
        PreClaimAction::Retry => return Ok(ProcessOutcome::Retry),
        PreClaimAction::MarkInterruptedAndAck => {
            mark_failed(
                response_store,
                response_id,
                "background response interrupted during processing",
            )
            .await?;
            return Ok(ProcessOutcome::Ack);
        }
        PreClaimAction::Claim => {}
    }

    let Some(work) = claim_for_processing(response_store, response_id).await? else {
        return outcome_after_failed_claim(response_store, response_id, ctx).await;
    };

    let url = format!("{}/responses", work.upstream);
    let mut req = http.post(&url).json(&work.upstream_request);
    if let Some(authorization) = work.upstream_authorization.as_deref() {
        req = req.header("authorization", authorization);
    } else if let Some(api_key) = upstream_api_key {
        req = req.bearer_auth(api_key);
    }

    match req.send().await {
        Ok(resp) => {
            let status = resp.status();
            let body = match resp.bytes().await {
                Ok(body) => body,
                Err(e) => {
                    mark_failed(
                        response_store,
                        response_id,
                        &format!("failed to read upstream background response body: {e}"),
                    )
                    .await?;
                    return Ok(ProcessOutcome::Ack);
                }
            };
            if !status.is_success() {
                let message = String::from_utf8_lossy(&body);
                mark_failed(response_store, response_id, &message).await?;
                return Ok(ProcessOutcome::Ack);
            }

            let Ok(response) = serde_json::from_slice::<Value>(&body) else {
                mark_failed(
                    response_store,
                    response_id,
                    "upstream returned invalid JSON",
                )
                .await?;
                return Ok(ProcessOutcome::Ack);
            };
            let response = match enrich_upstream_completion_response(response, response_id) {
                Ok(response) => response,
                Err(message) => {
                    mark_failed(response_store, response_id, message).await?;
                    return Ok(ProcessOutcome::Ack);
                }
            };

            let completion = StoredResponse {
                upstream: work.upstream,
                response,
                input: work.input,
                pending_upstream_request: None,
                upstream_authorization: None,
                enqueued_at: None,
            };
            if let Err(err) =
                store_completion_with_retry(response_store, response_id, completion.clone()).await
            {
                if is_retryable_store_error(&err) {
                    return Err(RetryableCompletionError { completion }.into());
                }
                return Err(err);
            }
        }
        Err(e) => {
            mark_failed(response_store, response_id, &e.to_string()).await?;
        }
    }

    Ok(ProcessOutcome::Ack)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PreClaimAction {
    Ack,
    MarkInterruptedAndAck,
    Claim,
    Retry,
}

fn pre_claim_action(stored: &StoredResponse, ctx: ProcessContext) -> PreClaimAction {
    if !should_persist(stored) {
        return PreClaimAction::Ack;
    }

    match stored_response_status(stored) {
        Some("completed") | Some("failed") | Some("incomplete") => PreClaimAction::Ack,
        Some("in_progress") if stored.pending_upstream_request.is_none() => {
            if is_stale_reclaim(ctx) {
                PreClaimAction::MarkInterruptedAndAck
            } else {
                PreClaimAction::Retry
            }
        }
        _ => PreClaimAction::Claim,
    }
}

fn is_stale_reclaim(ctx: ProcessContext) -> bool {
    match ctx.entry_source {
        EntrySource::Autoclaimed | EntrySource::StartupPending => true,
        EntrySource::Live => match ctx.message_idle_ms {
            Some(idle) => idle >= ctx.autoclaim_min_idle_ms as u64,
            None => false,
        },
    }
}

async fn outcome_after_failed_claim(
    response_store: &StoreHandle,
    response_id: &str,
    ctx: ProcessContext,
) -> Result<ProcessOutcome> {
    let Some(stored) = response_store.load(response_id).await? else {
        return Ok(ProcessOutcome::Ack);
    };
    match pre_claim_action(&stored, ctx) {
        PreClaimAction::Ack => Ok(ProcessOutcome::Ack),
        PreClaimAction::Retry => Ok(ProcessOutcome::Retry),
        PreClaimAction::MarkInterruptedAndAck => {
            mark_failed(
                response_store,
                response_id,
                "background response interrupted during processing",
            )
            .await?;
            Ok(ProcessOutcome::Ack)
        }
        PreClaimAction::Claim => Ok(ProcessOutcome::Retry),
    }
}

/// Load queued work and atomically transition it to `in_progress`.
///
/// Re-reads Valkey immediately before writing so a concurrent cancel/delete does
/// not get overwritten. Returns `None` when the response is terminal, missing, or
/// already claimed by another worker.
async fn claim_for_processing(
    response_store: &StoreHandle,
    response_id: &str,
) -> Result<Option<ClaimedWork>> {
    let Some(stored) = response_store.load(response_id).await? else {
        return Ok(None);
    };
    if !is_claimable(&stored) {
        return Ok(None);
    }

    let work = ClaimedWork {
        upstream: stored.upstream.clone(),
        upstream_request: stored
            .pending_upstream_request
            .clone()
            .context("background response is missing pending upstream request")?,
        input: stored.input.clone(),
        upstream_authorization: stored.upstream_authorization.clone(),
    };

    // Re-read before claiming so gateway cancel/delete wins the race.
    let Some(mut stored) = response_store.load(response_id).await? else {
        return Ok(None);
    };
    if !is_claimable(&stored) {
        return Ok(None);
    }

    stored.pending_upstream_request = None;
    stored.response = with_response_status(&stored.response, "in_progress");
    stored.upstream_authorization = None;
    response_store.store(response_id, &stored).await?;

    Ok(Some(work))
}

fn is_claimable(stored: &StoredResponse) -> bool {
    should_persist(stored) && stored.pending_upstream_request.is_some()
}

fn enrich_upstream_completion_response(
    mut response: Value,
    response_id: &str,
) -> Result<Value, &'static str> {
    if !response.is_object() {
        return Err("upstream returned JSON that is not an object");
    }
    response["id"] = Value::String(response_id.to_string());
    response["background"] = Value::Bool(true);
    if response.get("status").is_none() {
        response["status"] = Value::String("completed".to_string());
    }
    Ok(response)
}

fn upstream_timeout_from_env() -> Duration {
    env::var("BACKGROUND_UPSTREAM_TIMEOUT_SECONDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(600))
}

fn should_persist(stored: &StoredResponse) -> bool {
    !matches!(
        stored_response_status(stored),
        Some("cancelled") | Some("deleted")
    )
}

fn with_response_status(response: &Value, status: &str) -> Value {
    let mut updated = response.clone();
    updated["status"] = Value::String(status.to_string());
    updated
}

fn merge_completion(current: &StoredResponse, mut completion: StoredResponse) -> StoredResponse {
    completion.enqueued_at = current.enqueued_at;
    completion
}

const STORE_COMPLETION_MAX_ATTEMPTS: usize = 5;

pub async fn persist_completion_only(
    response_store: &StoreHandle,
    response_id: &str,
    stored: StoredResponse,
) -> Result<()> {
    if let Err(err) = store_completion_with_retry(response_store, response_id, stored.clone()).await
    {
        if is_retryable_store_error(&err) {
            return Err(RetryableCompletionError { completion: stored }.into());
        }
        return Err(err);
    }
    Ok(())
}

async fn store_completion_with_retry(
    response_store: &StoreHandle,
    response_id: &str,
    stored: StoredResponse,
) -> Result<()> {
    let mut backoff = Duration::from_millis(100);
    for attempt in 0..STORE_COMPLETION_MAX_ATTEMPTS {
        match store_completion(response_store, response_id, stored.clone()).await {
            Ok(()) => return Ok(()),
            Err(err)
                if is_retryable_store_error(&err)
                    && attempt + 1 < STORE_COMPLETION_MAX_ATTEMPTS =>
            {
                tokio::time::sleep(backoff).await;
                backoff = backoff.saturating_mul(2);
            }
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

async fn store_completion(
    response_store: &StoreHandle,
    response_id: &str,
    stored: StoredResponse,
) -> Result<()> {
    let Some(current) = response_store.load(response_id).await? else {
        return Ok(());
    };
    if !should_persist(&current) {
        return Ok(());
    }
    let stored = merge_completion(&current, stored);
    response_store
        .store(response_id, &stored)
        .await
        .context("failed to store completed background response")
}

async fn mark_failed(response_store: &StoreHandle, response_id: &str, message: &str) -> Result<()> {
    let Some(mut stored) = response_store.load(response_id).await? else {
        return Ok(());
    };
    if !should_persist(&stored) {
        return Ok(());
    }
    stored.response = json!({
        "id": response_id,
        "object": "response",
        "status": "failed",
        "background": true,
        "error": {
            "message": message,
            "type": "server_error"
        }
    });
    stored.pending_upstream_request = None;
    stored.upstream_authorization = None;
    response_store.store(response_id, &stored).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use responses_api_store_client::StoredResponse;

    #[test]
    fn skips_terminal_statuses() {
        let cancelled = StoredResponse {
            upstream: "http://model".to_string(),
            response: json!({"status": "cancelled", "background": true}),
            input: vec![],
            pending_upstream_request: None,
            upstream_authorization: None,
            enqueued_at: None,
        };
        assert!(!should_persist(&cancelled));

        let deleted = StoredResponse {
            upstream: "http://model".to_string(),
            response: json!({"status": "deleted", "background": true}),
            input: vec![],
            pending_upstream_request: None,
            upstream_authorization: None,
            enqueued_at: None,
        };
        assert!(!should_persist(&deleted));
    }

    #[test]
    fn claimable_requires_pending_upstream_request() {
        let queued = StoredResponse {
            upstream: "http://model".to_string(),
            response: json!({"status": "queued", "background": true}),
            input: vec![],
            pending_upstream_request: Some(json!({"input": "hi"})),
            upstream_authorization: None,
            enqueued_at: None,
        };
        assert!(is_claimable(&queued));

        let in_progress = StoredResponse {
            upstream: "http://model".to_string(),
            response: json!({"status": "in_progress", "background": true}),
            input: vec![],
            pending_upstream_request: None,
            upstream_authorization: None,
            enqueued_at: None,
        };
        assert!(!is_claimable(&in_progress));
    }

    #[test]
    fn autoclaimed_in_progress_is_stale_without_idle_metadata() {
        let interrupted = StoredResponse {
            upstream: "http://model".to_string(),
            response: json!({"status": "in_progress", "background": true}),
            input: vec![],
            pending_upstream_request: None,
            upstream_authorization: None,
            enqueued_at: None,
        };
        let ctx = ProcessContext {
            message_idle_ms: None,
            autoclaim_min_idle_ms: 720_000,
            entry_source: EntrySource::Autoclaimed,
            hydration_retry: false,
        };
        assert_eq!(
            pre_claim_action(&interrupted, ctx),
            PreClaimAction::MarkInterruptedAndAck
        );
    }

    #[test]
    fn startup_pending_in_progress_is_stale_without_idle_metadata() {
        let interrupted = StoredResponse {
            upstream: "http://model".to_string(),
            response: json!({"status": "in_progress", "background": true}),
            input: vec![],
            pending_upstream_request: None,
            upstream_authorization: None,
            enqueued_at: None,
        };
        let ctx = ProcessContext {
            message_idle_ms: None,
            autoclaim_min_idle_ms: 720_000,
            entry_source: EntrySource::StartupPending,
            hydration_retry: false,
        };
        assert_eq!(
            pre_claim_action(&interrupted, ctx),
            PreClaimAction::MarkInterruptedAndAck
        );
    }

    #[test]
    fn recently_reclaimed_live_in_progress_stays_retryable() {
        let interrupted = StoredResponse {
            upstream: "http://model".to_string(),
            response: json!({"status": "in_progress", "background": true}),
            input: vec![],
            pending_upstream_request: None,
            upstream_authorization: None,
            enqueued_at: None,
        };
        let ctx = ProcessContext {
            message_idle_ms: Some(30_000),
            autoclaim_min_idle_ms: 720_000,
            entry_source: EntrySource::Live,
            hydration_retry: false,
        };
        assert_eq!(pre_claim_action(&interrupted, ctx), PreClaimAction::Retry);
    }

    #[test]
    fn incomplete_responses_are_ackable() {
        let incomplete = StoredResponse {
            upstream: "http://model".to_string(),
            response: json!({"status": "incomplete", "background": true}),
            input: vec![],
            pending_upstream_request: None,
            upstream_authorization: None,
            enqueued_at: None,
        };
        assert_eq!(
            pre_claim_action(&incomplete, ProcessContext::default()),
            PreClaimAction::Ack
        );
    }

    #[test]
    fn active_queued_work_requires_claim() {
        let queued = StoredResponse {
            upstream: "http://model".to_string(),
            response: json!({"status": "queued", "background": true}),
            input: vec![],
            pending_upstream_request: Some(json!({"input": "hi"})),
            upstream_authorization: None,
            enqueued_at: None,
        };
        assert_eq!(
            pre_claim_action(&queued, ProcessContext::default()),
            PreClaimAction::Claim
        );
    }

    #[test]
    fn merge_completion_preserves_enqueued_at() {
        let current = StoredResponse {
            upstream: "http://model".to_string(),
            response: json!({"status": "in_progress", "background": true}),
            input: vec![json!({"role": "user", "content": "hi"})],
            pending_upstream_request: None,
            upstream_authorization: None,
            enqueued_at: Some(1_746_500_000),
        };
        let completion = StoredResponse {
            upstream: "http://model".to_string(),
            response: json!({"status": "completed", "background": true}),
            input: vec![json!({"role": "user", "content": "hi"})],
            pending_upstream_request: None,
            upstream_authorization: None,
            enqueued_at: None,
        };

        let merged = merge_completion(&current, completion);
        assert_eq!(merged.enqueued_at, Some(1_746_500_000));
        assert_eq!(stored_response_status(&merged), Some("completed"));
    }

    #[test]
    fn enrich_upstream_response_rejects_non_object_json() {
        assert!(enrich_upstream_completion_response(json!([]), "resp_x").is_err());
        assert!(enrich_upstream_completion_response(json!("ok"), "resp_x").is_err());
    }

    #[test]
    fn enrich_upstream_response_adds_defaults() {
        let enriched =
            enrich_upstream_completion_response(json!({"object": "response"}), "resp_x").unwrap();
        assert_eq!(enriched["id"], "resp_x");
        assert_eq!(enriched["background"], true);
        assert_eq!(enriched["status"], "completed");
    }

    #[test]
    fn upstream_timeout_reads_env_or_defaults() {
        env::remove_var("BACKGROUND_UPSTREAM_TIMEOUT_SECONDS");
        assert_eq!(upstream_timeout_from_env(), Duration::from_secs(600));

        env::set_var("BACKGROUND_UPSTREAM_TIMEOUT_SECONDS", "120");
        assert_eq!(upstream_timeout_from_env(), Duration::from_secs(120));
        env::remove_var("BACKGROUND_UPSTREAM_TIMEOUT_SECONDS");
    }
}
