use crate::enhanced_processor::process_siri;
use crate::siri_models::{Siri, SiriResponse};
use crate::state::AppState;
use reqwest::Client;
use std::sync::Arc;
use std::time::Duration;

pub async fn start_downloader(state: Arc<AppState>, url: String) {
    let client = Client::new();

    tokio::spawn(async move {
        loop {
            println!("Fetching SIRI feed...");

            let response_result = client.get(&url).send().await;

            match response_result {
                Ok(response) if response.status().is_success() => {
                    match response.text().await {
                        Ok(siri_payload_text) => {
                            // XML deserializers consume the contents of the root element.
                            // Parse <Siri> directly, then restore the wrapper expected by
                            // the existing processing pipeline.
                            let siri_payload_result =
                                quick_xml::de::from_str::<Siri>(&siri_payload_text)
                                    .map(|siri| SiriResponse { siri });

                            match siri_payload_result {
                                Ok(siri_payload) => {
                                    println!("Successfully fetched and parsed SIRI XML payload.");
                                    // Process payload into state
                                    process_siri(state.clone(), siri_payload).await;
                                }
                                Err(e) => {
                                    eprintln!("Failed to parse SIRI XML: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to read SIRI XML response body: {}", e);
                        }
                    }
                }
                Ok(response) => {
                    eprintln!("SIRI fetch failed with status: {}", response.status());
                }
                Err(e) => {
                    eprintln!("Failed to fetch SIRI payload: {}", e);
                }
            }

            tokio::time::sleep(Duration::from_secs(100)).await;
        }
    });
}
