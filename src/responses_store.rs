use std::env;

use anyhow::Result;
use responses_api_store_client::{
    ClaimBackgroundJobsRequest, ClaimBackgroundJobsResult, Client, ClientError, StoredResponse,
};
use tonic::{
    transport::{Channel, Endpoint},
    Code,
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
