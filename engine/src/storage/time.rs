// Copyright 2026 Arion Yau
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use chrono::{DateTime, NaiveDateTime, Utc};

use crate::error::{EngineError, Result};
use crate::models::Timestamp;

pub const FUTURE_SENTINEL_STR: &str = "2100-01-01 00:00:00.000000";
pub const MIN_TIMESTAMP_STR: &str = "1970-01-01 00:00:00.000000";

pub fn future_sentinel() -> Timestamp {
    parse_db_time(FUTURE_SENTINEL_STR)
        .expect("future sentinel string should always parse as valid timestamp")
}

pub fn format_db_time(ts: &Timestamp) -> String {
    ts.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
}

pub fn parse_db_time(value: &str) -> Result<Timestamp> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        return Ok(dt.with_timezone(&Utc));
    }

    let naive = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S"))
        .map_err(|err| {
            EngineError::InvalidData(format!("invalid timestamp value '{value}': {err}"))
        })?;
    Ok(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
}
