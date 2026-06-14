use std::{
    collections::HashMap,
    env,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{bail, Context, Result};
use responses_api_store_client::ClaimBackgroundJobsRequest;

use crate::responses_store::{connect_from_env, is_retryable_store_error, StoreHandle};
use tokio::sync::{watch, Mutex, Semaphore};
use tokio::task::JoinSet;

use crate::worker::{self, EntrySource, ProcessContext, ProcessOutcome};
use reqwest::Client as HttpClient;

const ENSURE_CONSUMER_GROUP_ATTEMPTS: usize = 30;
const ENSURE_CONSUMER_GROUP_RETRY_DELAY: Duration = Duration::from_secs(2);

async fn ensure_background_consumer_group(
    response_store: &StoreHandle,
    consumer_group: &str,
) -> Result<()> {
    let mut last_err = None;
    for attempt in 1..=ENSURE_CONSUMER_GROUP_ATTEMPTS {
        match response_store
            .ensure_consumer_group(consumer_group, "0")
            .await
        {
            Ok(_) => return Ok(()),
            Err(err) => {
                last_err = Some(err);
                if attempt < ENSURE_CONSUMER_GROUP_ATTEMPTS {
                    tokio::time::sleep(ENSURE_CONSUMER_GROUP_RETRY_DELAY).await;
                }
            }
        }
    }

    Err(last_err.expect("ensure_consumer_group error after retries"))
        .context("failed to ensure background queue consumer group")
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueueMessage {
    pub stream_id: String,
    pub response_id: String,
    pub idle_ms: Option<u64>,
    pub autoclaimed: bool,
}

#[derive(Clone)]
pub struct QueueConfig {
    pub consumer_group: String,
    pub consumer_name: String,
    pub block_ms: usize,
    pub autoclaim_min_idle_ms: usize,
    pub max_concurrent_jobs: usize,
}

impl QueueConfig {
    pub fn from_env() -> Result<Self> {
        let consumer_group = env::var("BACKGROUND_QUEUE_CONSUMER_GROUP")
            .unwrap_or_else(|_| "duihua-background".to_string());
        let consumer_name = consumer_name_from_env();
        let block_ms = env::var("BACKGROUND_QUEUE_BLOCK_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(5_000);
        if block_ms == 0 {
            bail!("BACKGROUND_QUEUE_BLOCK_MS must be greater than 0");
        }
        let autoclaim_min_idle_ms = env::var("BACKGROUND_QUEUE_AUTOCLAIM_MIN_IDLE_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or_else(default_autoclaim_min_idle_ms);
        warn_if_autoclaim_shorter_than_upstream(autoclaim_min_idle_ms);
        let max_concurrent_jobs = max_concurrent_jobs_from_env()?;

        Ok(Self {
            consumer_group,
            consumer_name,
            block_ms,
            autoclaim_min_idle_ms,
            max_concurrent_jobs,
        })
    }
}

pub async fn run() -> Result<()> {
    let config = QueueConfig::from_env()?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    spawn_shutdown_listener(shutdown_tx);

    let response_store = connect_from_env().await?;
    ensure_background_consumer_group(&response_store, &config.consumer_group).await?;
    eprintln!(
        "background worker startup: recommended terminationGracePeriodSeconds={}",
        recommended_termination_grace_period_seconds_for_config(&config)
    );

    let upstream_http = worker::upstream_http_client()?;
    let job_concurrency = Arc::new(Semaphore::new(config.max_concurrent_jobs));
    let pending_retries = Arc::new(Mutex::new(PendingRetryScheduler::new()));
    let mut join_set = JoinSet::new();

    drain_pending_at_startup(
        &config,
        &response_store,
        &upstream_http,
        job_concurrency.clone(),
        pending_retries.clone(),
        &mut join_set,
        &shutdown_rx,
    )
    .await;

    loop {
        if shutdown_triggered(&shutdown_rx) {
            break;
        }

        reap_completed_jobs(&mut join_set).await;

        if !shutdown_triggered(&shutdown_rx) {
            process_due_pending_retries(
                &config,
                &response_store,
                &upstream_http,
                pending_retries.clone(),
                job_concurrency.clone(),
                &mut join_set,
                &shutdown_rx,
            )
            .await;
        }

        if shutdown_triggered(&shutdown_rx) {
            break;
        }

        let Some(permit) = acquire_job_permit(job_concurrency.clone(), &shutdown_rx, true).await
        else {
            break;
        };
        if shutdown_triggered(&shutdown_rx) {
            drop(permit);
            break;
        }

        match claim_jobs(&response_store, &config, 1, config.block_ms as u32).await {
            Ok(Some(message)) => {
                process_message(
                    &config,
                    &response_store,
                    &upstream_http,
                    message,
                    pending_retries.clone(),
                    job_concurrency.clone(),
                    &mut join_set,
                    &shutdown_rx,
                    Some(permit),
                    None,
                )
                .await;
            }
            Ok(None) => drop(permit),
            Err(err) => {
                drop(permit);
                eprintln!("failed to claim background queue jobs: {err:?}");
                sleep_on_store_error().await;
            }
        }
    }

    eprintln!("background worker draining in-flight jobs before exit");
    job_concurrency.close();
    drain_in_flight_jobs(&mut join_set).await;
    Ok(())
}

async fn claim_jobs(
    response_store: &StoreHandle,
    config: &QueueConfig,
    count: u32,
    block_ms: u32,
) -> Result<Option<QueueMessage>> {
    let result = response_store
        .claim_background_jobs(ClaimBackgroundJobsRequest {
            consumer_group: config.consumer_group.clone(),
            consumer_name: config.consumer_name.clone(),
            count,
            block_ms,
            autoclaim_min_idle_ms: config.autoclaim_min_idle_ms as u32,
        })
        .await?;

    for stream_id in result.pending_stream_ids {
        eprintln!("background queue entry {stream_id} pending hydration; leaving in PEL");
    }

    Ok(result.jobs.into_iter().next().map(|job| QueueMessage {
        stream_id: job.stream_id,
        response_id: job.response_id,
        idle_ms: job.idle_ms,
        autoclaimed: job.autoclaimed,
    }))
}

fn entry_source_for_message(message: &QueueMessage, startup_pending: bool) -> EntrySource {
    if startup_pending {
        EntrySource::StartupPending
    } else if message.autoclaimed {
        EntrySource::Autoclaimed
    } else {
        EntrySource::Live
    }
}

async fn drain_pending_at_startup(
    config: &QueueConfig,
    response_store: &StoreHandle,
    upstream_http: &HttpClient,
    job_concurrency: Arc<Semaphore>,
    pending_retries: Arc<Mutex<PendingRetryScheduler>>,
    join_set: &mut JoinSet<()>,
    shutdown_rx: &watch::Receiver<bool>,
) {
    loop {
        if shutdown_triggered(shutdown_rx) {
            break;
        }

        match claim_jobs(response_store, config, 1, 0).await {
            Ok(Some(message)) => {
                let entry_source = entry_source_for_message(&message, message.autoclaimed);
                process_message(
                    config,
                    response_store,
                    upstream_http,
                    message.clone(),
                    pending_retries.clone(),
                    job_concurrency.clone(),
                    join_set,
                    shutdown_rx,
                    None,
                    Some(entry_source),
                )
                .await;

                if !message.autoclaimed {
                    break;
                }
            }
            Ok(None) => break,
            Err(err) => {
                eprintln!("failed to drain pending background queue messages at startup: {err:?}");
                sleep_on_store_error().await;
                break;
            }
        }
    }
}

#[derive(Debug)]
struct PendingRetryEntry {
    response_id: String,
    retry_at: Instant,
    autoclaimed: bool,
    idle_ms: Option<u64>,
}

#[derive(Debug, Default)]
struct PendingRetryScheduler {
    entries: HashMap<String, PendingRetryEntry>,
}

impl PendingRetryScheduler {
    fn new() -> Self {
        Self::default()
    }

    fn schedule(&mut self, message: &QueueMessage, backoff: Duration) {
        self.entries.insert(
            message.stream_id.clone(),
            PendingRetryEntry {
                response_id: message.response_id.clone(),
                retry_at: Instant::now() + backoff,
                autoclaimed: message.autoclaimed,
                idle_ms: message.idle_ms,
            },
        );
    }

    fn remove(&mut self, stream_id: &str) {
        self.entries.remove(stream_id);
    }

    fn due_messages(&self) -> Vec<QueueMessage> {
        let now = Instant::now();
        self.entries
            .iter()
            .filter(|(_, entry)| entry.retry_at <= now)
            .map(|(stream_id, entry)| QueueMessage {
                stream_id: stream_id.clone(),
                response_id: entry.response_id.clone(),
                idle_ms: entry.idle_ms,
                autoclaimed: entry.autoclaimed,
            })
            .collect()
    }
}

async fn process_due_pending_retries(
    config: &QueueConfig,
    response_store: &StoreHandle,
    upstream_http: &HttpClient,
    pending_retries: Arc<Mutex<PendingRetryScheduler>>,
    job_concurrency: Arc<Semaphore>,
    join_set: &mut JoinSet<()>,
    shutdown_rx: &watch::Receiver<bool>,
) {
    let due_messages = {
        let scheduler = pending_retries.lock().await;
        scheduler.due_messages()
    };

    for message in due_messages {
        if shutdown_triggered(shutdown_rx) {
            break;
        }

        pending_retries.lock().await.remove(&message.stream_id);
        process_message(
            config,
            response_store,
            upstream_http,
            message,
            pending_retries.clone(),
            job_concurrency.clone(),
            join_set,
            shutdown_rx,
            None,
            None,
        )
        .await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_message(
    config: &QueueConfig,
    response_store: &StoreHandle,
    upstream_http: &HttpClient,
    message: QueueMessage,
    pending_retries: Arc<Mutex<PendingRetryScheduler>>,
    job_concurrency: Arc<Semaphore>,
    join_set: &mut JoinSet<()>,
    shutdown_rx: &watch::Receiver<bool>,
    mut reserved_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    entry_source: Option<EntrySource>,
) {
    let response_id = message.response_id.clone();
    let entry_source = entry_source.unwrap_or_else(|| entry_source_for_message(&message, false));
    let permit = match reserved_permit.take() {
        Some(permit) => Some(permit),
        None => acquire_job_permit(job_concurrency.clone(), shutdown_rx, true).await,
    };
    let Some(permit) = permit else {
        return;
    };

    if shutdown_triggered(shutdown_rx) {
        match handle_message(
            config,
            response_store,
            upstream_http,
            &message,
            entry_source,
        )
        .await
        {
            Ok(()) => {}
            Err(err) => schedule_message_retry_on_error(
                &err,
                &message,
                pending_retries.clone(),
                &response_id,
            )
            .await,
        }
        drop(permit);
        return;
    }

    let config = config.clone();
    let response_store = response_store.clone();
    let upstream_http = upstream_http.clone();
    let pending_retries = pending_retries.clone();
    join_set.spawn(async move {
        let _permit = permit;
        match handle_message(
            &config,
            &response_store,
            &upstream_http,
            &message,
            entry_source,
        )
        .await
        {
            Ok(()) => {}
            Err(err) => {
                schedule_message_retry_on_error(
                    &err,
                    &message,
                    pending_retries.clone(),
                    &response_id,
                )
                .await;
            }
        }
    });

    drop(reserved_permit);
}

async fn schedule_message_retry_on_error(
    err: &anyhow::Error,
    message: &QueueMessage,
    pending_retries: Arc<Mutex<PendingRetryScheduler>>,
    response_id: &str,
) {
    if err.downcast_ref::<RetryableMessageError>().is_some() || is_retryable_store_error(err) {
        pending_retries
            .lock()
            .await
            .schedule(message, pending_retry_backoff_from_env());
        return;
    }

    eprintln!("failed to process background queue message {response_id}: {err:?}");
}

#[derive(Debug)]
struct RetryableMessageError;

impl std::fmt::Display for RetryableMessageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("background queue message will be retried")
    }
}

impl std::error::Error for RetryableMessageError {}

async fn handle_message(
    config: &QueueConfig,
    response_store: &StoreHandle,
    upstream_http: &HttpClient,
    message: &QueueMessage,
    entry_source: EntrySource,
) -> Result<()> {
    let ctx = ProcessContext {
        message_idle_ms: message.idle_ms,
        autoclaim_min_idle_ms: config.autoclaim_min_idle_ms,
        entry_source,
    };
    match worker::process_response(response_store, upstream_http, &message.response_id, ctx).await?
    {
        ProcessOutcome::Ack => {
            response_store
                .acknowledge_background_job(&config.consumer_group, &message.stream_id)
                .await
                .context("failed to acknowledge background queue message")?;
        }
        ProcessOutcome::Retry => return Err(RetryableMessageError.into()),
    }
    Ok(())
}

fn shutdown_triggered(shutdown_rx: &watch::Receiver<bool>) -> bool {
    *shutdown_rx.borrow()
}

fn spawn_shutdown_listener(shutdown_tx: watch::Sender<bool>) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("failed to install SIGINT handler");
        tokio::spawn(async move {
            tokio::select! {
                _ = sigterm.recv() => {}
                _ = sigint.recv() => {}
            }
            eprintln!("background worker shutdown signal received; stopping new queue reads");
            let _ = shutdown_tx.send(true);
        });
    }
    #[cfg(not(unix))]
    {
        tokio::spawn(async move {
            tokio::signal::ctrl_c()
                .await
                .expect("failed to install Ctrl+C handler");
            eprintln!("background worker shutdown signal received; stopping new queue reads");
            let _ = shutdown_tx.send(true);
        });
    }
}

async fn acquire_job_permit(
    job_concurrency: Arc<Semaphore>,
    shutdown_rx: &watch::Receiver<bool>,
    stop_on_shutdown: bool,
) -> Option<tokio::sync::OwnedSemaphorePermit> {
    loop {
        if stop_on_shutdown && shutdown_triggered(shutdown_rx) {
            return None;
        }

        if stop_on_shutdown {
            let mut shutdown_listener = shutdown_rx.clone();
            tokio::select! {
                biased;
                changed = shutdown_listener.changed() => {
                    if changed.is_err() || shutdown_triggered(shutdown_rx) {
                        return None;
                    }
                }
                permit = job_concurrency.clone().acquire_owned() => {
                    return match permit {
                        Ok(_permit) if shutdown_triggered(shutdown_rx) => None,
                        Ok(permit) => Some(permit),
                        Err(_) => None,
                    };
                }
            }
        } else {
            return job_concurrency.acquire_owned().await.ok();
        }
    }
}

async fn reap_completed_jobs(join_set: &mut JoinSet<()>) {
    while let Some(result) = join_set.try_join_next() {
        if let Err(err) = result {
            eprintln!("background queue job task failed: {err:?}");
        }
    }
}

async fn drain_in_flight_jobs(join_set: &mut JoinSet<()>) {
    while let Some(result) = join_set.join_next().await {
        if let Err(err) = result {
            eprintln!("background queue job task failed during shutdown drain: {err:?}");
        }
    }
}

#[allow(dead_code)]
pub fn recommended_termination_grace_period_seconds_for_upstream(upstream_secs: usize) -> u64 {
    recommended_termination_grace_period_seconds_for_upstream_and_block(upstream_secs, 0)
}

pub fn recommended_termination_grace_period_seconds_for_config(config: &QueueConfig) -> u64 {
    recommended_termination_grace_period_seconds_for_upstream_and_block(
        upstream_timeout_seconds_from_env(),
        config.block_ms,
    )
}

pub fn recommended_termination_grace_period_seconds_for_upstream_and_block(
    upstream_secs: usize,
    block_ms: usize,
) -> u64 {
    let block_secs = block_ms.div_ceil(1000);
    upstream_secs
        .saturating_add(block_secs)
        .saturating_add(60)
        .max(30) as u64
}

fn autoclaim_min_idle_ms_for_upstream_timeout(upstream_secs: usize) -> usize {
    upstream_secs.saturating_add(120).saturating_mul(1000)
}

fn default_autoclaim_min_idle_ms() -> usize {
    autoclaim_min_idle_ms_for_upstream_timeout(upstream_timeout_seconds_from_env())
}

fn pending_retry_backoff_from_env() -> Duration {
    env::var("BACKGROUND_QUEUE_PENDING_RETRY_SECONDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(30))
}

fn upstream_timeout_seconds_from_env() -> usize {
    env::var("BACKGROUND_UPSTREAM_TIMEOUT_SECONDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(600)
}

fn warn_if_autoclaim_shorter_than_upstream(autoclaim_min_idle_ms: usize) {
    let upstream_ms = upstream_timeout_seconds_from_env().saturating_mul(1000);
    if autoclaim_min_idle_ms < upstream_ms {
        eprintln!(
            "warning: BACKGROUND_QUEUE_AUTOCLAIM_MIN_IDLE_MS ({autoclaim_min_idle_ms}) is shorter than BACKGROUND_UPSTREAM_TIMEOUT_SECONDS ({upstream_ms} ms); active upstream calls may be reclaimed and marked failed"
        );
    }
}

async fn sleep_on_store_error() {
    tokio::time::sleep(Duration::from_secs(1)).await;
}

fn resolve_consumer_name(explicit: Option<&str>, host: &str, pid: u32) -> String {
    if let Some(name) = explicit {
        return name.to_string();
    }
    format!("{host}-{pid}")
}

fn max_concurrent_jobs_from_env() -> Result<usize> {
    max_concurrent_jobs_from_env_value(
        env::var("BACKGROUND_QUEUE_MAX_CONCURRENT_JOBS")
            .ok()
            .as_deref(),
    )
}

fn max_concurrent_jobs_from_env_value(explicit: Option<&str>) -> Result<usize> {
    let max_concurrent_jobs = explicit.and_then(|value| value.parse().ok()).unwrap_or(1);
    if max_concurrent_jobs == 0 {
        bail!("BACKGROUND_QUEUE_MAX_CONCURRENT_JOBS must be greater than 0");
    }
    Ok(max_concurrent_jobs)
}

fn consumer_name_from_env() -> String {
    let explicit = env::var("BACKGROUND_QUEUE_CONSUMER_NAME").ok();
    let host = env::var("HOSTNAME").unwrap_or_else(|_| "duihua-background-worker".to_string());
    resolve_consumer_name(explicit.as_deref(), &host, std::process::id())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_zero_block_ms() {
        env::set_var("BACKGROUND_QUEUE_BLOCK_MS", "0");
        assert!(QueueConfig::from_env().is_err());
        env::remove_var("BACKGROUND_QUEUE_BLOCK_MS");
    }

    #[test]
    fn default_autoclaim_exceeds_upstream_timeout() {
        assert_eq!(autoclaim_min_idle_ms_for_upstream_timeout(600), 720_000);
    }

    #[test]
    fn consumer_name_defaults_include_process_id() {
        assert_eq!(resolve_consumer_name(None, "pod-1", 4242), "pod-1-4242");
    }

    #[test]
    fn consumer_name_honors_explicit_override() {
        assert_eq!(
            resolve_consumer_name(Some("worker-a"), "pod-1", 1),
            "worker-a"
        );
    }

    #[test]
    fn max_concurrent_jobs_defaults_and_rejects_zero() {
        assert_eq!(max_concurrent_jobs_from_env_value(None).unwrap(), 1);
        assert!(max_concurrent_jobs_from_env_value(Some("0")).is_err());
        assert_eq!(max_concurrent_jobs_from_env_value(Some("4")).unwrap(), 4);
    }

    #[test]
    fn recommended_grace_period_exceeds_upstream_timeout() {
        assert_eq!(
            recommended_termination_grace_period_seconds_for_upstream(600),
            660
        );
        assert_eq!(
            recommended_termination_grace_period_seconds_for_upstream_and_block(600, 5_000),
            665
        );
        assert_eq!(
            recommended_termination_grace_period_seconds_for_upstream_and_block(600, 120_000),
            780
        );
    }

    #[tokio::test]
    async fn drain_in_flight_jobs_waits_for_spawned_tasks() {
        let mut join_set = JoinSet::new();
        join_set.spawn(async {
            tokio::time::sleep(Duration::from_millis(20)).await;
        });
        drain_in_flight_jobs(&mut join_set).await;
        assert!(join_set.is_empty());
    }

    #[test]
    fn pending_retry_scheduler_honors_backoff() {
        let mut scheduler = PendingRetryScheduler::new();
        scheduler.schedule(
            &QueueMessage {
                stream_id: "1-0".to_string(),
                response_id: "resp_a".to_string(),
                idle_ms: None,
                autoclaimed: false,
            },
            Duration::from_secs(60),
        );
        assert!(scheduler.due_messages().is_empty());
        scheduler.entries.get_mut("1-0").unwrap().retry_at = Instant::now();
        assert_eq!(
            scheduler.due_messages(),
            vec![QueueMessage {
                stream_id: "1-0".to_string(),
                response_id: "resp_a".to_string(),
                idle_ms: None,
                autoclaimed: false,
            }]
        );
    }
}
