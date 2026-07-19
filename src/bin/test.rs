use gtfs_structures::Gtfs;
use serde_json::Value;
use std::fs::File;
use std::io::BufReader;

const GTFS_URL: &str = "https://www.rejseplanen.info/labs/GTFS.zip";

fn value_string(value: &Value) -> Option<&str> {
    value
        .as_str()
        .or_else(|| value.get("value").and_then(Value::as_str))
}

fn main() {
    println!("Loading Danmark GTFS...");
    let gtfs = Gtfs::from_url(GTFS_URL).expect("Failed to load GTFS");
    println!(
        "GTFS loaded: {} trips, {} routes, {} stops",
        gtfs.trips.len(),
        gtfs.routes.len(),
        gtfs.stops.len()
    );

    println!("Loading example.json...");
    let file = File::open("example.json").expect("Failed to open example.json");
    let reader = BufReader::new(file);
    let value: Value = serde_json::from_reader(reader).expect("Failed to parse example.json");

    let deliveries = value["Siri"]["ServiceDelivery"]["EstimatedTimetableDelivery"]
        .as_array()
        .expect("Missing EstimatedTimetableDelivery array");

    let mut journeys_with_line_ref = 0_usize;
    let mut matched_journeys = 0_usize;
    let mut candidate_routes = 0_usize;

    for delivery in deliveries {
        let frames = delivery["EstimatedJourneyVersionFrame"]
            .as_array()
            .expect("Missing EstimatedJourneyVersionFrame array");
        for frame in frames {
            let journeys = frame["EstimatedVehicleJourney"]
                .as_array()
                .expect("Missing EstimatedVehicleJourney array");
            for journey in journeys {
                let Some(line_ref) = value_string(&journey["LineRef"]).map(str::trim) else {
                    continue;
                };
                journeys_with_line_ref += 1;

                let matches = gtfs
                    .routes
                    .values()
                    .filter(|route| route.short_name.as_deref().map(str::trim) == Some(line_ref))
                    .count();
                candidate_routes += matches;
                if matches > 0 {
                    matched_journeys += 1;
                }
            }
        }
    }

    println!(
        "Journeys with LineRef: {journeys_with_line_ref}, matched journeys: {matched_journeys}, candidate routes: {candidate_routes}"
    );
}
