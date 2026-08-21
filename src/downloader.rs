use crate::siri_models::{Siri, SiriResponse};
use crate::state::AppState;
use crate::swiss_processor::process_siri;
use flate2::read::GzDecoder;
use reqwest::Client;
use reqwest::header::{ACCEPT_ENCODING, AUTHORIZATION, CONTENT_ENCODING, HeaderValue};
use std::io::BufReader;
use std::sync::Arc;
use std::time::Duration;

pub async fn start_downloader(state: Arc<AppState>, url: String, authorization: Option<String>) {
    let client = Client::builder()
        .user_agent("catenary-swiss-siri-to-gtfs-rt/1.0")
        .build()
        .expect("failed to build HTTP client");

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
                        Ok(body) => match parse_siri_body(body.as_ref(), is_gzip) {
                            Ok(siri) => {
                                println!("Successfully fetched and parsed SIRI XML payload.");
                                process_siri(state.clone(), SiriResponse { siri }).await;
                            }
                            Err(error) => eprintln!("{error}"),
                        },
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

fn parse_siri_body(body: &[u8], is_gzip: bool) -> Result<Siri, String> {
    if is_gzip {
        let decoder = GzDecoder::new(body);
        quick_xml::de::from_reader::<_, Siri>(BufReader::new(decoder))
            .map_err(|error| format!("Failed to parse gzip SIRI XML: {error}"))
    } else {
        let text = std::str::from_utf8(body)
            .map_err(|error| format!("SIRI response was not valid UTF-8: {error}"))?;
        quick_xml::de::from_str::<Siri>(text)
            .map_err(|error| format!("Failed to parse SIRI XML: {error}"))
    }
}
