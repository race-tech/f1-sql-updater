use serde::de::Error;
use serde::{Deserialize, Deserializer};

#[derive(Deserialize, Debug)]
pub struct LapTime {
    pub driver_id: i32,
    pub lap: u16,
    pub position: u16,
    #[serde(deserialize_with = "de_time")]
    pub time: chrono::TimeDelta,
}

#[derive(Deserialize, Debug)]
pub struct Qualifying {
    pub driver_id: i32,
    pub constructor_id: i32,
    pub position: u16,
    pub number: u16,
    pub q1: String,
    pub q2: Option<String>,
    pub q3: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct RaceResult {
    pub driver_id: i32,
    pub constructor_id: i32,
    pub driver_number: u16,
    pub position: u16,
    pub grid: u16,
    pub position_text: String,
    pub position_order: u16,
    pub points: u16,
    pub laps: u16,
    pub time: Option<String>,
    pub milliseconds: Option<String>,
    pub fastest_lap: Option<u16>,
    pub fatest_lap_time: Option<String>,
    pub rank: Option<u16>,
    pub fastest_lap_speed: Option<f32>,
}

#[derive(Deserialize, Debug)]
pub struct DriverStanding {
    pub driver_id: i32,
    pub points: u32,
    pub position: u32,
    pub position_text: String,
    pub wins: u32,
}

#[derive(Deserialize, Debug)]
pub struct ConstructorStanding {
    pub constructor_id: i32,
    pub points: u32,
    pub position: u32,
    pub position_text: String,
    pub wins: u32,
}

#[derive(Deserialize, Debug)]
pub struct ConstructorResult {
    pub constructor_id: i32,
    pub points: u16,
}

#[derive(Deserialize, Debug)]
pub struct DriverSprintResult {
    pub no: u16,
    pub entrant: String,
    pub grid: u16,
    pub position: String,
    #[serde(rename = "positionOrder")]
    pub position_order: u16,
    pub points: u16,
    pub laps: u16,
    pub time: Option<String>,
    pub milliseconds: Option<String>,
    #[serde(rename = "fastestLap")]
    pub fastest_lap: Option<u16>,
    #[serde(rename = "fastestLapTime")]
    pub fatest_lap_time: Option<String>,
    #[serde(rename = "fastestLapSpeed")]
    pub fastest_lap_speed: Option<f32>,
}

#[derive(Deserialize, Debug)]
pub struct PitStop {
    pub driver_id: i32,
    pub stop: u16,
    pub lap: u16,
    #[serde(deserialize_with = "de_local_time")]
    pub time: chrono::NaiveTime,
    #[serde(deserialize_with = "de_pit_stop_duration")]
    pub duration: chrono::TimeDelta,
}

fn de_time<'de, D>(de: D) -> Result<chrono::TimeDelta, D::Error>
where
    D: Deserializer<'de>,
{
    // Trick we had hours because chrono needs it to parse a NaiveTime
    let input = format!("00:{}", String::deserialize(de)?);
    if let Ok(time) = chrono::NaiveTime::parse_from_str(&input, "%H:%M:%S%.3f") {
        Ok(time.signed_duration_since(chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap()))
    } else {
        Err(D::Error::custom(format!("invalid time ({})", input)))
    }
}

fn de_local_time<'de, D>(de: D) -> Result<chrono::NaiveTime, D::Error>
where
    D: Deserializer<'de>,
{
    // Trick we had hours because chrono needs it to parse a NaiveTime
    let input = String::deserialize(de)?;
    if let Ok(time) = chrono::NaiveTime::parse_from_str(&input, "%H:%M:%S") {
        Ok(time)
    } else {
        Err(D::Error::custom(format!("invalid local time ({})", input)))
    }
}

fn de_pit_stop_duration<'de, D>(de: D) -> Result<chrono::TimeDelta, D::Error>
where
    D: Deserializer<'de>,
{
    // Trick we had hours and minutes because chrono needs it to parse a NaiveTime
    let input = format!("00:00:{}", String::deserialize(de)?);
    if let Ok(time) = chrono::NaiveTime::parse_from_str(&input, "%H:%M:%S%.3f") {
        Ok(time.signed_duration_since(chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap()))
    } else {
        Err(D::Error::custom(format!(
            "invalid pit stop duration ({})",
            input
        )))
    }
}
