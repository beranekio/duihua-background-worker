mod queue;
mod responses_store;
mod worker;

use anyhow::Result;

fn init_rustls_provider() {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("failed to install rustls crypto provider");
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    init_rustls_provider();
    queue::run().await
}
