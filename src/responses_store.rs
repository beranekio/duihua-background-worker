use std::env;

use anyhow::Result;
use responses_api_store_client::{
    ClaimBackgroundJobsRequest, ClaimBackgroundJobsResult, Client, ClientError, StoredResponse,
};
use tonic::{
    transport::{Channel, Endpoint},
    Code, Status,
};

#[derive(Clone)]
pub struct StoreHandle {
    channel: Channel,
    ttl_seconds: u64,
}

pub async fn connect_from_env() -> Result<StoreHandle> {
    let endpoint = env::var("RESPONSES_API_STORE_ENDPOINT")
        .unwrap_or_else(|_| "http://responses-api-store:50051".to_string());
    let ttl_seconds = env::var("RESPONSE_ID_STORE_TTL_SECONDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(86_400);

    let channel = Endpoint::from_shared(endpoint)?.connect_lazy();

    Ok(StoreHandle {
        channel,
        ttl_seconds,
    })
}

impl StoreHandle {
    fn client(&self) -> Client {
        Client::from_channel(self.channel.clone())
    }

    pub async fn store(&self, response_id: &str, response: &StoredResponse) -> Result<()> {
        let mut client = self.client();
        client
            .store_response(response_id, response, Some(self.ttl_seconds))
            .await
            .map_err(map_client_error)
    }

    pub async fn load(&self, response_id: &str) -> Result<Option<StoredResponse>> {
        let mut client = self.client();
        match client.get_response(response_id, false).await {
            Ok(record) => Ok(Some(record)),
            Err(ClientError::NotFound(_)) => Ok(None),
            Err(ClientError::Rpc(status)) if status.code() == Code::NotFound => Ok(None),
            Err(err) => Err(map_client_error(err)),
        }
    }

    pub async fn claim_background_jobs(
        &self,
        request: ClaimBackgroundJobsRequest,
    ) -> Result<ClaimBackgroundJobsResult> {
        let mut client = self.client();
        client
            .claim_background_jobs(request)
            .await
            .map_err(map_client_error)
    }

    pub async fn acknowledge_background_job(
        &self,
        consumer_group: &str,
        stream_id: &str,
    ) -> Result<()> {
        let mut client = self.client();
        client
            .acknowledge_background_job(consumer_group, stream_id)
            .await
            .map_err(map_client_error)
    }

    pub async fn ensure_consumer_group(
        &self,
        consumer_group: &str,
        start_id: &str,
    ) -> Result<bool> {
        let mut client = self.client();
        client
            .ensure_consumer_group(consumer_group, start_id)
            .await
            .map_err(map_client_error)
    }
}

fn map_client_error(err: ClientError) -> anyhow::Error {
    err.into()
}

pub fn is_retryable_store_error(err: &anyhow::Error) -> bool {
    if let Some(err) = err.downcast_ref::<ClientError>() {
        return is_retryable_client_error(err);
    }

    for cause in err.chain() {
        if let Some(err) = cause.downcast_ref::<ClientError>() {
            return is_retryable_client_error(err);
        }
        if let Some(status) = cause.downcast_ref::<Status>() {
            return is_retryable_rpc_status(status.code());
        }
    }

    false
}

fn is_retryable_client_error(err: &ClientError) -> bool {
    match err {
        ClientError::Transport(_) => true,
        ClientError::Rpc(status) => is_retryable_rpc_status(status.code()),
        ClientError::Serialization(_)
        | ClientError::NotFound(_)
        | ClientError::Configuration(_) => false,
    }
}

fn is_retryable_rpc_status(code: Code) -> bool {
    matches!(
        code,
        Code::Unavailable
            | Code::DeadlineExceeded
            | Code::ResourceExhausted
            | Code::Aborted
            | Code::Internal
            | Code::Unknown
    )
}

#[cfg(test)]
mod retryable_error_tests {
    use super::*;
    use tonic::Status;

    #[test]
    fn unavailable_rpc_errors_are_retryable() {
        let err: anyhow::Error = ClientError::Rpc(Status::unavailable("store down")).into();
        assert!(is_retryable_store_error(&err));
    }

    #[test]
    fn not_found_errors_are_not_retryable() {
        let err: anyhow::Error = ClientError::NotFound("resp_1".to_string()).into();
        assert!(!is_retryable_store_error(&err));
    }

    #[test]
    fn serialization_errors_are_not_retryable() {
        let err: anyhow::Error = ClientError::Serialization("invalid record".to_string()).into();
        assert!(!is_retryable_store_error(&err));
    }
}
