use crate::state::{
    AppState, CompactTrip, IndexRange, LoadedGtfs, OriginalTripEntry, StopIndex, TripIndex,
};
use anyhow::{Context, Result};
use futures_util::StreamExt;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File as StdFile;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::io::AsyncWriteExt;

const REFRESH_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Deserialize)]
struct SwissTripRow {
    trip_id: String,
    original_trip_id: String,
}

#[derive(Deserialize)]
struct SwissStopTimeRow {
    trip_id: String,
    stop_id: String,
    stop_sequence: u32,
}

pub async fn start_gtfs_updater(state: Arc<AppState>, url: String) -> Result<()> {
    let cache_path = std::env::var("GTFS_CACHE_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./swiss-gtfs.zip"));

    if let Some(parent) = cache_path.parent() {
        tokio::fs::create_dir_all(parent).await.with_context(|| {
            format!("Failed to create GTFS cache directory {}", parent.display())
        })?;
    }

    let initial_refresh_delay = match load_gtfs_from_disk(&cache_path).await {
        Ok(loaded_gtfs) => {
            println!(
                "Loaded compact GTFS from {}: {} trips, {} stop times, {} unique stops, {} original trip ids",
                cache_path.display(),
                loaded_gtfs.trips.len(),
                loaded_gtfs.stop_time_count(),
                loaded_gtfs.stop_ids.len(),
                loaded_gtfs.original_trip_id_count(),
            );

            let refresh_delay = refresh_delay_for_cache(&cache_path).await;
            publish_gtfs(&state, loaded_gtfs).await;
            refresh_delay
        }

        Err(error) => {
            println!(
                "No usable GTFS cache at {}: {error:#}",
                cache_path.display()
            );
            println!("Downloading initial GTFS dataset...");

            let loaded_gtfs = download_validate_and_replace(&url, &cache_path)
                .await
                .context("Failed to obtain initial GTFS dataset")?;

            println!(
                "Downloaded compact GTFS: {} trips, {} stop times, {} unique stops, {} original trip ids",
                loaded_gtfs.trips.len(),
                loaded_gtfs.stop_time_count(),
                loaded_gtfs.stop_ids.len(),
                loaded_gtfs.original_trip_id_count(),
            );

            publish_gtfs(&state, loaded_gtfs).await;
            REFRESH_INTERVAL
        }
    };

    // At this point state.gtfs is guaranteed to be populated.
    // The SIRI downloader can now safely start.
    tokio::spawn(async move {
        // Avoid parsing a second complete dataset immediately after startup when
        // the cache is still fresh. If it is stale, this duration is zero.
        if !initial_refresh_delay.is_zero() {
            tokio::time::sleep(initial_refresh_delay).await;
        }

        loop {
            println!("Checking for an updated GTFS dataset...");

            match download_validate_and_replace(&url, &cache_path).await {
                Ok(loaded_gtfs) => {
                    println!(
                        "GTFS refreshed: {} trips, {} stop times, {} unique stops, {} original trip ids",
                        loaded_gtfs.trips.len(),
                        loaded_gtfs.stop_time_count(),
                        loaded_gtfs.stop_ids.len(),
                        loaded_gtfs.original_trip_id_count(),
                    );
                    publish_gtfs(&state, loaded_gtfs).await;
                }

                Err(error) => {
                    // Keep serving the existing in-memory GTFS.
                    // Do not replace the known-good disk cache.
                    eprintln!("GTFS refresh failed; retaining existing dataset: {error:#}");
                }
            }

            tokio::time::sleep(REFRESH_INTERVAL).await;
        }
    });

    Ok(())
}

async fn refresh_delay_for_cache(path: &Path) -> Duration {
    let Ok(metadata) = tokio::fs::metadata(path).await else {
        return Duration::ZERO;
    };
    let Ok(modified) = metadata.modified() else {
        return Duration::ZERO;
    };

    let age = SystemTime::now()
        .duration_since(modified)
        .unwrap_or(Duration::ZERO);
    REFRESH_INTERVAL.saturating_sub(age)
}

async fn load_gtfs_from_disk(path: &Path) -> Result<LoadedGtfs> {
    let metadata = tokio::fs::metadata(path)
        .await
        .with_context(|| format!("GTFS cache does not exist: {}", path.display()))?;

    if metadata.len() == 0 {
        anyhow::bail!("GTFS cache is empty");
    }

    let path_buf = path.to_path_buf();
    let parse_path = path_buf.clone();

    tokio::task::spawn_blocking(move || parse_compact_gtfs(&parse_path))
        .await
        .context("GTFS parsing task panicked")?
        .with_context(|| format!("Failed to parse cached GTFS {}", path_buf.display()))
}

async fn download_validate_and_replace(url: &str, cache_path: &Path) -> Result<LoadedGtfs> {
    // Keep the temporary file next to the final file so rename remains
    // atomic on Linux.
    let temporary_path = cache_path.with_extension("zip.tmp");

    let client = reqwest::Client::new();

    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("Failed to download GTFS from {url}"))?
        .error_for_status()
        .with_context(|| format!("GTFS server returned an error for {url}"))?;

    let expected_length = response.content_length();
    let mut stream = response.bytes_stream();

    let mut file = tokio::fs::File::create(&temporary_path)
        .await
        .with_context(|| {
            format!(
                "Failed to create temporary GTFS file {}",
                temporary_path.display()
            )
        })?;

    let mut downloaded_bytes: u64 = 0;

    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result.context("Failed while downloading GTFS body")?;

        file.write_all(&chunk)
            .await
            .context("Failed to write GTFS download to disk")?;

        downloaded_bytes += chunk.len() as u64;
    }

    file.flush()
        .await
        .context("Failed to flush temporary GTFS file")?;

    file.sync_all()
        .await
        .context("Failed to synchronize temporary GTFS file")?;

    drop(file);

    if downloaded_bytes == 0 {
        let _ = tokio::fs::remove_file(&temporary_path).await;
        anyhow::bail!("Downloaded GTFS file was empty");
    }

    if let Some(expected_length) = expected_length {
        if downloaded_bytes != expected_length {
            let _ = tokio::fs::remove_file(&temporary_path).await;

            anyhow::bail!(
                "Incomplete GTFS download: expected {} bytes, received {} bytes",
                expected_length,
                downloaded_bytes
            );
        }
    }

    println!(
        "Downloaded {} bytes to {}",
        downloaded_bytes,
        temporary_path.display()
    );

    // Validate and build the compact indexes before replacing the known-good cache.
    let parse_path = temporary_path.clone();
    let loaded_gtfs = tokio::task::spawn_blocking(move || parse_compact_gtfs(&parse_path))
        .await
        .context("GTFS parsing task panicked")?
        .with_context(|| {
            format!(
                "Downloaded file is not a valid Swiss GTFS dataset: {}",
                temporary_path.display()
            )
        })?;

    // Atomic replacement on the Linux server, because both files are in the
    // same directory/filesystem.
    tokio::fs::rename(&temporary_path, cache_path)
        .await
        .with_context(|| {
            format!(
                "Failed to move {} to {}",
                temporary_path.display(),
                cache_path.display()
            )
        })?;

    println!("Saved GTFS cache to {}", cache_path.display());

    Ok(loaded_gtfs)
}

fn parse_compact_gtfs(path: &Path) -> Result<LoadedGtfs> {
    let file = StdFile::open(path)
        .with_context(|| format!("Failed to open GTFS zip {}", path.display()))?;
    let mut archive = zip::ZipArchive::new(file).context("Failed to open GTFS zip archive")?;

    let trips_index = find_zip_entry_index(&mut archive, "trips.txt")?;
    let stop_times_index = find_zip_entry_index(&mut archive, "stop_times.txt")?;

    // trip_id strings are interned once. The HashMap is temporary and is dropped
    // after stop_times.txt has been converted from trip IDs to compact u32 indexes.
    let mut trip_ids = Vec::<Arc<str>>::new();
    let mut trip_id_to_index = HashMap::<Arc<str>, TripIndex>::new();
    let mut original_trip_id_to_trip_indices = HashMap::<Arc<str>, Vec<TripIndex>>::new();

    {
        let trips_file = archive
            .by_index(trips_index)
            .context("Failed to open trips.txt from GTFS zip")?;
        let mut reader = csv::ReaderBuilder::new()
            .flexible(true)
            .from_reader(trips_file);

        for row in reader.deserialize::<SwissTripRow>() {
            let row = row.context("Failed to parse Swiss trips.txt")?;
            let trip_id = row.trip_id.trim();
            let original_trip_id = row.original_trip_id.trim();
            if trip_id.is_empty() || original_trip_id.is_empty() {
                continue;
            }

            if trip_id_to_index.contains_key(trip_id) {
                anyhow::bail!("Duplicate trip_id in trips.txt: {trip_id}");
            }

            let trip_index = TripIndex::try_from(trip_ids.len())
                .context("GTFS contains more trips than a u32 index can address")?;
            let trip_id: Arc<str> = Arc::from(trip_id);
            trip_id_to_index.insert(Arc::clone(&trip_id), trip_index);
            trip_ids.push(trip_id);

            if let Some(indices) = original_trip_id_to_trip_indices.get_mut(original_trip_id) {
                indices.push(trip_index);
            } else {
                original_trip_id_to_trip_indices
                    .insert(Arc::from(original_trip_id), vec![trip_index]);
            }
        }
    }

    if trip_ids.is_empty() {
        anyhow::bail!("trips.txt did not contain any usable trip_id/original_trip_id values");
    }

    // During parsing each stop time is only two u32 values: stop_sequence and
    // an interned stop index. We discard stop_sequence after ordering each trip.
    let mut stops_by_trip = (0..trip_ids.len())
        .map(|_| Vec::<(u32, StopIndex)>::new())
        .collect::<Vec<_>>();
    let mut stop_ids = Vec::<Arc<str>>::new();
    let mut stop_id_to_index = HashMap::<Arc<str>, StopIndex>::new();
    let mut stop_time_count = 0_usize;

    {
        let stop_times_file = archive
            .by_index(stop_times_index)
            .context("Failed to open stop_times.txt from GTFS zip")?;
        let mut reader = csv::ReaderBuilder::new()
            .flexible(true)
            .from_reader(stop_times_file);

        for row in reader.deserialize::<SwissStopTimeRow>() {
            let row = row.context("Failed to parse Swiss stop_times.txt")?;
            let trip_id = row.trip_id.trim();
            let stop_id = row.stop_id.trim();
            if trip_id.is_empty() || stop_id.is_empty() {
                continue;
            }

            let Some(&trip_index) = trip_id_to_index.get(trip_id) else {
                // This can happen only for trips that were skipped because their
                // Swiss original_trip_id was missing. They can never be matched.
                continue;
            };

            let stop_index = if let Some(&stop_index) = stop_id_to_index.get(stop_id) {
                stop_index
            } else {
                let stop_index = StopIndex::try_from(stop_ids.len())
                    .context("GTFS contains more stops than a u32 index can address")?;
                let stop_id: Arc<str> = Arc::from(stop_id);
                stop_id_to_index.insert(Arc::clone(&stop_id), stop_index);
                stop_ids.push(stop_id);
                stop_index
            };

            stops_by_trip[trip_index as usize].push((row.stop_sequence, stop_index));
            stop_time_count += 1;
        }
    }

    // These lookup tables were needed only while parsing. Dropping them before
    // constructing LoadedGtfs prevents a second permanent copy of every ID.
    drop(trip_id_to_index);
    drop(stop_id_to_index);

    // Flatten all trip stop sequences into one contiguous u32 array. A trip
    // stores only an 8-byte range instead of a Box/Vec and a separate heap
    // allocation for every trip.
    let mut trips = Vec::with_capacity(trip_ids.len());
    let mut compact_stop_indices = Vec::<StopIndex>::with_capacity(stop_time_count);
    for (trip_id, mut stops) in trip_ids.into_iter().zip(stops_by_trip) {
        stops.sort_unstable_by_key(|(sequence, _)| *sequence);
        let start = u32::try_from(compact_stop_indices.len())
            .context("GTFS contains more stop times than a u32 range can address")?;
        let len = u32::try_from(stops.len())
            .context("A GTFS trip contains more stop times than a u32 range can address")?;
        compact_stop_indices.extend(stops.into_iter().map(|(_, stop_index)| stop_index));
        trips.push(CompactTrip {
            trip_id,
            stops: IndexRange { start, len },
        });
    }

    // Flatten original-trip candidates as well. This avoids one Vec/Box heap
    // allocation per original_trip_id, which matters for a trips.txt this large.
    let candidate_count = original_trip_id_to_trip_indices
        .values()
        .map(Vec::len)
        .sum::<usize>();
    let mut original_trips = Vec::with_capacity(original_trip_id_to_trip_indices.len());
    let mut original_trip_candidates = Vec::<TripIndex>::with_capacity(candidate_count);
    for (original_trip_id, trip_indices) in original_trip_id_to_trip_indices {
        let start = u32::try_from(original_trip_candidates.len())
            .context("GTFS contains more trip candidates than a u32 range can address")?;
        let len = u32::try_from(trip_indices.len())
            .context("An original_trip_id has more candidates than a u32 range can address")?;
        original_trip_candidates.extend(trip_indices);
        original_trips.push(OriginalTripEntry {
            original_trip_id,
            candidates: IndexRange { start, len },
        });
    }
    original_trips.sort_unstable_by(|left, right| {
        left.original_trip_id
            .as_ref()
            .cmp(right.original_trip_id.as_ref())
    });

    println!(
        "Compact GTFS index built: {} trips, {} stop times, {} unique stop IDs",
        trips.len(),
        stop_time_count,
        stop_ids.len()
    );

    Ok(LoadedGtfs {
        trips: trips.into_boxed_slice(),
        stop_ids: stop_ids.into_boxed_slice(),
        stop_indices: compact_stop_indices.into_boxed_slice(),
        original_trips: original_trips.into_boxed_slice(),
        original_trip_candidates: original_trip_candidates.into_boxed_slice(),
    })
}

fn find_zip_entry_index(archive: &mut zip::ZipArchive<StdFile>, target: &str) -> Result<usize> {
    for index in 0..archive.len() {
        let is_target = {
            let entry = archive
                .by_index(index)
                .with_context(|| format!("Failed to inspect GTFS zip entry {index}"))?;
            let name = entry.name();
            name == target || name.ends_with(&format!("/{target}"))
        };
        if is_target {
            return Ok(index);
        }
    }

    anyhow::bail!("GTFS zip does not contain {target}")
}

async fn publish_gtfs(state: &Arc<AppState>, loaded_gtfs: LoadedGtfs) {
    let mut lock = state.gtfs.write().await;
    *lock = Some(Arc::new(loaded_gtfs));
}
