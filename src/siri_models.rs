use serde::{Deserialize, Deserializer};

#[derive(Debug, Clone, Deserialize)]
pub struct SiriResponse {
    #[serde(rename = "Siri")]
    pub siri: Siri,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Siri {
    #[serde(rename = "ServiceDelivery")]
    pub service_delivery: ServiceDelivery,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServiceDelivery {
    #[serde(rename = "EstimatedTimetableDelivery", default)]
    pub estimated_timetable_delivery: Vec<EstimatedTimetableDelivery>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EstimatedTimetableDelivery {
    #[serde(rename = "EstimatedJourneyVersionFrame", default)]
    pub estimated_journey_version_frame: Vec<EstimatedJourneyVersionFrame>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EstimatedJourneyVersionFrame {
    #[serde(rename = "EstimatedVehicleJourney", default)]
    pub estimated_vehicle_journey: Vec<EstimatedVehicleJourney>,
}

#[derive(Debug, Clone)]
pub struct EstimatedVehicleJourney {
    pub dated_vehicle_journey_ref: Option<ValueWrapper>,
    pub line_ref: Option<ValueWrapper>,
    pub operator_ref: Option<ValueWrapper>,
    pub direction_ref: Option<ValueWrapper>,
    pub direction_name: Option<Vec<ValueWrapper>>,
    pub destination_ref: Option<ValueWrapper>,
    pub journey_note: Option<Vec<ValueWrapper>>,
    //#[serde(rename = "ExtraJourney", default)]
    //pub extra_journey: bool,
    pub estimated_calls: Option<EstimatedCalls>,
}

// Denmark's XML feed places DatedVehicleJourneyRef inside
// FramedVehicleJourneyRef, while the existing matchers expect the flattened
// dated_vehicle_journey_ref field above. Deserialize through this wire model
// and normalize the nested value without changing the processor API.
#[derive(Debug, Clone, Deserialize)]
struct EstimatedVehicleJourneyWire {
    #[serde(rename = "DatedVehicleJourneyRef")]
    dated_vehicle_journey_ref: Option<ValueWrapper>,
    #[serde(rename = "FramedVehicleJourneyRef")]
    framed_vehicle_journey_ref: Option<FramedVehicleJourneyRef>,
    #[serde(rename = "VehicleJourneyRef")]
    vehicle_journey_ref: Option<ValueWrapper>,
    #[serde(rename = "LineRef")]
    line_ref: Option<ValueWrapper>,
    #[serde(rename = "OperatorRef")]
    operator_ref: Option<ValueWrapper>,
    #[serde(rename = "DirectionRef")]
    direction_ref: Option<ValueWrapper>,
    #[serde(rename = "DirectionName")]
    direction_name: Option<Vec<ValueWrapper>>,
    #[serde(rename = "DestinationRef")]
    destination_ref: Option<ValueWrapper>,
    #[serde(rename = "JourneyNote")]
    journey_note: Option<Vec<ValueWrapper>>,
    #[serde(rename = "EstimatedCalls")]
    estimated_calls: Option<EstimatedCalls>,
}

#[derive(Debug, Clone, Deserialize)]
struct FramedVehicleJourneyRef {
    #[serde(rename = "DatedVehicleJourneyRef")]
    dated_vehicle_journey_ref: Option<ValueWrapper>,
}

impl<'de> Deserialize<'de> for EstimatedVehicleJourney {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = EstimatedVehicleJourneyWire::deserialize(deserializer)?;
        let dated_vehicle_journey_ref = wire
            .dated_vehicle_journey_ref
            .or_else(|| {
                wire.framed_vehicle_journey_ref
                    .and_then(|reference| reference.dated_vehicle_journey_ref)
            })
            // Some SIRI producers only expose VehicleJourneyRef. It is a useful
            // final exact-match key when the framed reference is unavailable.
            .or(wire.vehicle_journey_ref);

        Ok(Self {
            dated_vehicle_journey_ref,
            line_ref: wire.line_ref,
            operator_ref: wire.operator_ref,
            direction_ref: wire.direction_ref,
            direction_name: wire.direction_name,
            destination_ref: wire.destination_ref,
            journey_note: wire.journey_note,
            estimated_calls: wire.estimated_calls,
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct EstimatedCalls {
    #[serde(rename = "EstimatedCall", default)]
    pub estimated_call: Vec<EstimatedCall>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EstimatedCall {
    #[serde(rename = "StopPointRef")]
    pub stop_point_ref: Option<ValueWrapper>,
    #[serde(rename = "AimedArrivalTime")]
    pub aimed_arrival_time: Option<String>,
    #[serde(rename = "AimedDepartureTime")]
    pub aimed_departure_time: Option<String>,
    #[serde(rename = "ExpectedArrivalTime")]
    pub expected_arrival_time: Option<String>,
    #[serde(rename = "ExpectedDepartureTime")]
    pub expected_departure_time: Option<String>,
    #[serde(rename = "ArrivalStatus")]
    pub arrival_status: Option<String>,
    #[serde(rename = "DepartureStatus")]
    pub departure_status: Option<String>,
    #[serde(rename = "ArrivalPlatformName")]
    pub arrival_platform_name: Option<ValueWrapper>,
    #[serde(rename = "DeparturePlatformName")]
    pub departure_platform_name: Option<ValueWrapper>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ValueWrapper {
    // quick-xml exposes an element's character data through the special
    // "$text" field. The alias keeps the old JSON object shape deserializable
    // for tests and stored fixtures.
    #[serde(rename = "$text", alias = "value", default)]
    pub value: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::Siri;

    #[test]
    fn parses_danish_siri_xml_and_flattens_framed_journey_ref() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<Siri xmlns="http://www.siri.org.uk/siri" version="2.0">
  <ServiceDelivery>
    <EstimatedTimetableDelivery>
      <EstimatedJourneyVersionFrame>
        <EstimatedVehicleJourney>
          <LineRef>77</LineRef>
          <DirectionRef>1</DirectionRef>
          <DirectionName>Blokhus via Fårup Sommerland</DirectionName>
          <FramedVehicleJourneyRef>
            <DataFrameRef>2026-07-19</DataFrameRef>
            <DatedVehicleJourneyRef>26671-8600828701000</DatedVehicleJourneyRef>
          </FramedVehicleJourneyRef>
          <DestinationRef>000835100301</DestinationRef>
          <OperatorRef>NT</OperatorRef>
          <VehicleJourneyRef>26671-8600828701000</VehicleJourneyRef>
          <EstimatedCalls>
            <EstimatedCall>
              <StopPointRef>000805000102</StopPointRef>
              <AimedArrivalTime>2026-07-19T16:07:00+02:00</AimedArrivalTime>
              <ExpectedArrivalTime>2026-07-19T16:08:00+02:00</ExpectedArrivalTime>
              <AimedDepartureTime>2026-07-19T16:16:00+02:00</AimedDepartureTime>
              <ExpectedDepartureTime>2026-07-19T16:16:00+02:00</ExpectedDepartureTime>
            </EstimatedCall>
          </EstimatedCalls>
        </EstimatedVehicleJourney>
      </EstimatedJourneyVersionFrame>
    </EstimatedTimetableDelivery>
  </ServiceDelivery>
</Siri>"#;

        let siri: Siri = quick_xml::de::from_str(xml).expect("valid Danish SIRI XML");
        let journey = &siri.service_delivery.estimated_timetable_delivery[0]
            .estimated_journey_version_frame[0]
            .estimated_vehicle_journey[0];

        assert_eq!(
            journey
                .line_ref
                .as_ref()
                .and_then(|value| value.value.as_deref()),
            Some("77")
        );
        assert_eq!(
            journey
                .dated_vehicle_journey_ref
                .as_ref()
                .and_then(|value| value.value.as_deref()),
            Some("26671-8600828701000")
        );
        assert_eq!(
            journey
                .estimated_calls
                .as_ref()
                .and_then(|calls| calls.estimated_call.first())
                .and_then(|call| call.stop_point_ref.as_ref())
                .and_then(|value| value.value.as_deref()),
            Some("000805000102")
        );
    }
}
