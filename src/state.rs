use gtfs_realtime::FeedMessage;
use serde::Serialize;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
use tokio::sync::RwLock;

pub type TripIndex = u32;
pub type StopIndex = u32;

#[derive(Debug, Clone, Copy)]
pub struct IndexRange {
    pub start: u32,
    pub len: u32,
}

impl IndexRange {
    fn as_range(self) -> Range<usize> {
        let start = self.start as usize;
        start..start + self.len as usize
    }
}

#[derive(Debug)]
pub struct CompactTrip {
    pub trip_id: Arc<str>,
    pub stops: IndexRange,
}

#[derive(Debug)]
pub struct OriginalTripEntry {
    pub original_trip_id: Arc<str>,
    pub candidates: IndexRange,
}

#[derive(Debug, Clone, Serialize)]
pub struct PlatformInfo {
    pub stop_id: String,
    pub platform_name: String,
}

pub struct LoadedGtfs {
    pub trips: Box<[CompactTrip]>,
    pub stop_ids: Box<[Arc<str>]>,
    pub stop_indices: Box<[StopIndex]>,
    pub original_trips: Box<[OriginalTripEntry]>,
    pub original_trip_candidates: Box<[TripIndex]>,
}

impl LoadedGtfs {
    pub fn trip(&self, index: TripIndex) -> Option<&CompactTrip> {
        self.trips.get(index as usize)
    }

    pub fn stop_id(&self, index: StopIndex) -> Option<&str> {
        self.stop_ids
            .get(index as usize)
            .map(|value| value.as_ref())
    }

    pub fn trip_stop_indices(&self, trip: &CompactTrip) -> &[StopIndex] {
        &self.stop_indices[trip.stops.as_range()]
    }

    pub fn trip_indices_for_original_id(&self, original_trip_id: &str) -> Option<&[TripIndex]> {
        let index = self
            .original_trips
            .binary_search_by(|entry| entry.original_trip_id.as_ref().cmp(original_trip_id))
            .ok()?;
        let range = self.original_trips[index].candidates;
        Some(&self.original_trip_candidates[range.as_range()])
    }

    pub fn original_trip_id_count(&self) -> usize {
        self.original_trips.len()
    }

    pub fn stop_time_count(&self) -> usize {
        self.stop_indices.len()
    }
}

pub struct AppState {
    pub gtfs: RwLock<Option<Arc<LoadedGtfs>>>,
    pub gtfs_rt_feed: RwLock<Arc<FeedMessage>>,
    pub trip_platforms: RwLock<Arc<HashMap<String, Vec<PlatformInfo>>>>,
}

impl AppState {
    pub fn new(initial_feed: FeedMessage) -> Self {
        Self {
            gtfs: RwLock::new(None),
            gtfs_rt_feed: RwLock::new(Arc::new(initial_feed)),
            trip_platforms: RwLock::new(Arc::new(HashMap::new())),
        }
    }
}
