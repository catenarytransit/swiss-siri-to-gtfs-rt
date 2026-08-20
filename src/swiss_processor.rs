use crate::siri_models::{EstimatedCall, EstimatedVehicleJourney, SiriResponse};
use crate::state::{AppState, PlatformInfo};
use chrono::{DateTime, Utc};
use gtfs_realtime::{
    FeedEntity, FeedHeader, FeedMessage, TripDescriptor, TripUpdate,
    trip_update::{
        StopTimeEvent, StopTimeUpdate,
        stop_time_update::ScheduleRelationship as StopTimeScheduleRelationship,
    },
};
use gtfs_structures::{Gtfs, Trip};
use std::collections::HashSet;
use std::sync::Arc;

pub async fn process_siri(state: Arc<AppState>, siri: SiriResponse) {
    let loaded_gtfs = {
        let lock = state.gtfs.read().await;
        lock.clone()
    };

    let Some(loaded_gtfs) = loaded_gtfs else {
        return;
    };

    let mut feed = FeedMessage::default();
    let mut header = FeedHeader::default();
    header.gtfs_realtime_version = "2.0".to_string();
    header.timestamp = Some(Utc::now().timestamp() as u64);
    feed.header = header;

    let mut matched = 0_u64;
    let mut missing_original_trip_id = 0_u64;
    let mut stop_alignment_misses = 0_u64;
    let mut emitted_trip_ids = HashSet::new();

    for delivery in siri.siri.service_delivery.estimated_timetable_delivery {
        for frame in delivery.estimated_journey_version_frame {
            for journey in frame.estimated_vehicle_journey {
                let Some(original_trip_id) = journey
                    .dated_vehicle_journey_ref
                    .as_ref()
                    .and_then(|value| value.value.as_deref())
                else {
                    missing_original_trip_id += 1;
                    continue;
                };

                let Some(candidate_trip_ids) = loaded_gtfs
                    .original_trip_id_to_trip_ids
                    .get(original_trip_id)
                else {
                    missing_original_trip_id += 1;
                    continue;
                };

                let best = candidate_trip_ids
                    .iter()
                    .filter_map(|trip_id| build_candidate(&journey, trip_id, &loaded_gtfs.gtfs))
                    .max_by_key(|candidate| candidate.matched_calls);

                let Some(candidate) = best else {
                    stop_alignment_misses += 1;
                    continue;
                };

                if !emitted_trip_ids.insert(candidate.trip_id.clone()) {
                    continue;
                }

                if !candidate.platforms.is_empty() {
                    state
                        .trip_platforms
                        .insert(candidate.trip_id.clone(), candidate.platforms);
                }

                feed.entity.push(candidate.entity);
                matched += 1;
            }
        }
    }

    println!(
        "Swiss SIRI conversion: matched={matched}, missing_original_trip_id={missing_original_trip_id}, stop_alignment_misses={stop_alignment_misses}"
    );

    publish_and_cache_feed(state, feed).await;
}

struct Candidate {
    trip_id: String,
    matched_calls: usize,
    entity: FeedEntity,
    platforms: Vec<PlatformInfo>,
}

fn build_candidate(
    journey: &EstimatedVehicleJourney,
    trip_id: &str,
    gtfs: &Gtfs,
) -> Option<Candidate> {
    let trip = gtfs.trips.get(trip_id)?;
    let calls = journey.estimated_calls.as_ref()?;

    let mut calls = calls.estimated_call.iter().collect::<Vec<_>>();
    calls.sort_by_cached_key(|call| call_sort_key(call));

    let mut update = TripUpdate::default();
    let mut descriptor = TripDescriptor::default();
    descriptor.trip_id = Some(trip_id.to_string());
    update.trip = descriptor;

    let mut platforms = Vec::new();
    let mut next_stop_index = 0_usize;
    let mut matched_calls = 0_usize;

    for call in calls {
        let Some(siri_stop_id) = call
            .stop_point_ref
            .as_ref()
            .and_then(|value| value.value.as_deref())
        else {
            continue;
        };

        let Some(stop_index) = find_next_stop_index(trip, next_stop_index, siri_stop_id) else {
            continue;
        };
        next_stop_index = stop_index + 1;
        matched_calls += 1;

        let stop_id = trip.stop_times[stop_index].stop.id.clone();
        let mut stop_update = StopTimeUpdate::default();
        stop_update.stop_id = Some(stop_id.clone());

        if call
            .arrival_status
            .as_deref()
            .is_some_and(is_cancelled_status)
            || call
                .departure_status
                .as_deref()
                .is_some_and(is_cancelled_status)
        {
            stop_update.schedule_relationship = Some(StopTimeScheduleRelationship::Skipped as i32);
        }

        if let Some(timestamp) = realtime_or_aimed_arrival(call) {
            stop_update.arrival = Some(StopTimeEvent {
                time: Some(timestamp),
                ..Default::default()
            });
        }
        if let Some(timestamp) = realtime_or_aimed_departure(call) {
            stop_update.departure = Some(StopTimeEvent {
                time: Some(timestamp),
                ..Default::default()
            });
        }

        if let Some(platform_name) = call
            .departure_platform_name
            .as_ref()
            .or(call.arrival_platform_name.as_ref())
            .and_then(|value| value.value.as_deref())
            .filter(|value| !value.eq_ignore_ascii_case("unknown"))
        {
            platforms.push(PlatformInfo {
                stop_id,
                platform_name: platform_name.to_string(),
            });
        }

        update.stop_time_update.push(stop_update);
    }

    if matched_calls == 0 {
        return None;
    }

    Some(Candidate {
        trip_id: trip_id.to_string(),
        matched_calls,
        entity: FeedEntity {
            id: trip_id.to_string(),
            trip_update: Some(update),
            ..Default::default()
        },
        platforms,
    })
}

fn find_next_stop_index(trip: &Trip, start_index: usize, siri_stop_id: &str) -> Option<usize> {
    let siri_stop_id = normalise_swiss_stop_id(siri_stop_id);

    trip.stop_times
        .iter()
        .enumerate()
        .skip(start_index)
        .find(|(_, stop_time)| normalise_swiss_stop_id(&stop_time.stop.id) == siri_stop_id)
        .map(|(index, _)| index)
}

fn normalise_swiss_stop_id(stop_id: &str) -> String {
    let parts = stop_id.split(':').collect::<Vec<_>>();
    if parts.len() >= 4 && parts[0] == "ch" && parts[1] == "1" && parts[2] == "sloid" {
        return parts[..4].join(":");
    }
    stop_id.to_string()
}

fn call_sort_key(call: &EstimatedCall) -> (bool, i64) {
    let aimed = [
        call.aimed_arrival_time.as_deref(),
        call.aimed_departure_time.as_deref(),
    ]
    .into_iter()
    .flatten()
    .filter_map(parse_siri_timestamp)
    .min();
    let realtime = [
        call.expected_arrival_time.as_deref(),
        call.expected_departure_time.as_deref(),
    ]
    .into_iter()
    .flatten()
    .filter_map(parse_siri_timestamp)
    .min();
    let timestamp = aimed.or(realtime);
    (timestamp.is_none(), timestamp.unwrap_or(i64::MAX))
}

fn realtime_or_aimed_arrival(call: &EstimatedCall) -> Option<i64> {
    call.expected_arrival_time
        .as_deref()
        .or(call.aimed_arrival_time.as_deref())
        .and_then(parse_siri_timestamp)
}

fn realtime_or_aimed_departure(call: &EstimatedCall) -> Option<i64> {
    call.expected_departure_time
        .as_deref()
        .or(call.aimed_departure_time.as_deref())
        .and_then(parse_siri_timestamp)
}

fn parse_siri_timestamp(value: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|date_time| date_time.timestamp())
}

fn is_cancelled_status(status: &str) -> bool {
    status.eq_ignore_ascii_case("CANCELLED") || status.eq_ignore_ascii_case("CANCELED")
}

async fn publish_and_cache_feed(state: Arc<AppState>, feed: FeedMessage) {
    let feed = Arc::new(feed);
    {
        let mut lock = state.gtfs_rt_feed.write().await;
        *lock = feed.clone();
    }

    let cache_path = std::env::var("GTFS_RT_CACHE_PATH")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("./gtfs-rt.pb"));

    tokio::spawn(async move {
        use prost::Message;

        let mut bytes = Vec::new();
        if let Err(error) = feed.encode(&mut bytes) {
            eprintln!("Failed to encode GTFS-RT cache: {error}");
            return;
        }

        let temporary_path = cache_path.with_extension("pb.tmp");
        if let Err(error) = tokio::fs::write(&temporary_path, bytes).await {
            eprintln!("Failed to write GTFS-RT cache: {error}");
            return;
        }
        if let Err(error) = tokio::fs::rename(&temporary_path, &cache_path).await {
            eprintln!("Failed to replace GTFS-RT cache: {error}");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::normalise_swiss_stop_id;

    #[test]
    fn strips_swiss_platform_suffix_from_stop_id() {
        assert_eq!(
            normalise_swiss_stop_id("ch:1:sloid:1500:2:3"),
            "ch:1:sloid:1500"
        );
        assert_eq!(
            normalise_swiss_stop_id("ch:1:sloid:1500"),
            "ch:1:sloid:1500"
        );
    }
}
