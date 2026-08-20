use crate::siri_models::{Siri, SiriResponse};
use crate::state::AppState;
use crate::swiss_processor::process_siri;
use flate2::read::GzDecoder;
use reqwest::Client;
use reqwest::header::{ACCEPT_ENCODING, AUTHORIZATION, CONTENT_ENCODING, HeaderValue};
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

pub async fn start_downloader(
    state: Arc<AppState>,
    url: String,
    authorization: Option<String>,
) {
    let client = Client::new();

    let authorization = authorization.and_then(|value| match HeaderValue::from_str(&value) {
        Ok(value) => Some(value),
        Err(error) => {
            eprintln!("Invalid SIRI_AUTHORIZATION header value: {error}");
            None
        }
    });

    tokio::spawn(async move {
        loop {
            println!("Fetching Swiss SIRI feed...");

            // The Swiss endpoint expects a compressed-capable client. Advertising
            // gzip and decoding it below is the reqwest equivalent of curl --compressed.
            let mut request = client.get(&url).header(ACCEPT_ENCODING, "gzip");

            if let Some(value) = authorization.as_ref() {
                request = request.header(AUTHORIZATION, value.clone());
            }

            let response_result = request.send().await;

            match response_result {
                Ok(response) if response.status().is_success() => {
                    let is_gzip = response
                        .headers()
                        .get(CONTENT_ENCODING)
                        .and_then(|value| value.to_str().ok())
                        .is_some_and(|value| value.eq_ignore_ascii_case("gzip"));

                    match response.bytes().await {
                        Ok(body) => {
                            let siri_payload_text = if is_gzip {
                                let mut decoder = GzDecoder::new(body.as_ref());
                                let mut text = String::new();
                                match decoder.read_to_string(&mut text) {
                                    Ok(_) => Ok(text),
                                    Err(error) => Err(format!(
                                        "Failed to decompress gzip SIRI response: {error}"
                                    )),
                                }
                            } else {
                                String::from_utf8(body.to_vec()).map_err(|error| {
                                    format!("SIRI response was not valid UTF-8: {error}")
                                })
                            };

                            match siri_payload_text {
                                Ok(siri_payload_text) => {
                                    // XML deserializers consume the contents of the root element.
                                    // Parse <Siri> directly, then restore the wrapper expected by
                                    // the processing pipeline.
                                    let siri_payload_result =
                                        quick_xml::de::from_str::<Siri>(&siri_payload_text)
                                            .map(|siri| SiriResponse { siri });

                                    match siri_payload_result {
                                        Ok(siri_payload) => {
                                            println!(
                                                "Successfully fetched and parsed SIRI XML payload."
                                            );
                                            process_siri(state.clone(), siri_payload).await;
                                        }
                                        Err(e) => {
                                            eprintln!("Failed to parse SIRI XML: {}", e);
                                        }
                                    }
                                }
                                Err(error) => eprintln!("{error}"),
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
