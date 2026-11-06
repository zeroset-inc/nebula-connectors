//! Broker calls, cursors and timestamps shared by the provider adapters.

use std::{
    future::Future,
    pin::Pin,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use nebula_connector_sdk::{ClientError, ConnectorError, Context, client::ProviderResponse};
use serde_json::Value;
use uuid::Uuid;

pub(crate) type Result<T> = std::result::Result<T, ConnectorError>;
pub(crate) type Response<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// Reads through the broker; losing access to the requested resource is a
/// permission failure for the whole run.
pub(crate) async fn request(context: &Context, operation: &str, arguments: Value) -> Result<Value> {
    match broker_read(context, operation, arguments).await {
        Ok(response) => Ok(response.body),
        Err(ClientError::Provider(failure))
            if matches!(failure.provider_status, 401 | 403 | 404) =>
        {
            Err(ConnectorError::Permission)
        }
        Err(error) => Err(error.into()),
    }
}

/// Retries backpressure, retryable provider failures and transport errors
/// with a fresh attempt ID, up to three attempts and two minutes per wait.
pub(crate) async fn broker_read(
    context: &Context,
    operation: &str,
    arguments: Value,
) -> std::result::Result<ProviderResponse, ClientError> {
    for attempt in 0..3 {
        let error = match context
            .service
            .provider(
                &context.run,
                &Uuid::new_v4().to_string(),
                operation,
                arguments.clone(),
            )
            .await
        {
            Ok(response) => return Ok(response),
            Err(error) => error,
        };
        let fallback = Duration::from_secs(1 << attempt);
        let delay = match &error {
            ClientError::Rejected {
                status,
                retry_after,
            } if *status == 429 || *status >= 500 => Some(retry_after.unwrap_or(fallback)),
            ClientError::Provider(failure) if failure.retryable => {
                Some(if failure.retry_after_seconds == 0 {
                    fallback
                } else {
                    Duration::from_secs(failure.retry_after_seconds)
                })
            }
            ClientError::Transport | ClientError::Capacity => Some(fallback),
            _ => None,
        };
        if attempt == 2 || delay.is_none_or(|delay| delay > Duration::from_secs(120)) {
            return Err(error);
        }
        tokio::time::sleep(delay.unwrap_or_default()).await;
    }
    unreachable!()
}

pub(crate) fn observed_now() -> Result<i64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|time| i64::try_from(time.as_millis()).ok())
        .filter(|time| *time <= 9_007_199_254_740_991)
        .ok_or(ConnectorError::Response)
}

/// A cursor over selected containers and the provider's page token within the
/// current one.
#[derive(Default, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Cursor {
    pub(crate) container: usize,
    pub(crate) page: String,
}

impl Cursor {
    pub(crate) fn parse(value: &Value, containers: usize) -> Result<Self> {
        let cursor: Self =
            serde_json::from_value(value.clone()).map_err(|_| ConnectorError::CursorExpired)?;
        if cursor.container > containers
            || cursor.page.len() > 4096
            || cursor.container == containers && !cursor.page.is_empty()
        {
            return Err(ConnectorError::CursorExpired);
        }
        Ok(cursor)
    }

    /// Moves to the next container once the provider has no further page.
    pub(crate) fn advance(&mut self, next: Option<&Value>) -> Result<()> {
        let next = match next {
            None => String::new(),
            Some(Value::String(token)) if token.len() <= 4096 && *token != self.page => {
                token.clone()
            }
            _ => return Err(ConnectorError::IncompleteInventory),
        };
        if next.is_empty() {
            self.container += 1;
        }
        self.page = next;
        Ok(())
    }
}

/// Parses RFC 3339 UTC timestamps, such as 2026-10-27T18:46:26.034Z.
pub(crate) fn timestamp_ms(value: &str) -> Result<i64> {
    let bytes = value.as_bytes();
    let digits = |range: std::ops::Range<usize>| -> Result<i64> {
        let part = bytes.get(range).ok_or(ConnectorError::Response)?;
        if !part.iter().all(u8::is_ascii_digit) {
            return Err(ConnectorError::Response);
        }
        std::str::from_utf8(part)
            .ok()
            .and_then(|text| text.parse().ok())
            .ok_or(ConnectorError::Response)
    };
    if bytes.len() < 20
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
        || bytes.last() != Some(&b'Z')
    {
        return Err(ConnectorError::Response);
    }
    let (year, month, day) = (digits(0..4)?, digits(5..7)?, digits(8..10)?);
    let (hour, minute, second) = (digits(11..13)?, digits(14..16)?, digits(17..19)?);
    let millis = match &bytes[19..bytes.len() - 1] {
        [] => 0,
        [b'.', fraction @ ..] if (1..=9).contains(&fraction.len()) => {
            let padded = format!(
                "{:0<3}",
                std::str::from_utf8(fraction).map_err(|_| ConnectorError::Response)?
            );
            digits_value(&padded[..3])?
        }
        _ => return Err(ConnectorError::Response),
    };
    if !(1970..=9999).contains(&year)
        || !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 60
    {
        return Err(ConnectorError::Response);
    }
    // Days from the civil calendar date (Howard Hinnant's algorithm).
    let shifted = if month <= 2 { year - 1 } else { year };
    let era = shifted.div_euclid(400);
    let year_of_era = shifted - era * 400;
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    let days = era * 146_097 + day_of_era - 719_468;
    Ok(((days * 24 + hour) * 60 + minute) * 60_000 + second * 1000 + millis)
}

/// Parses RFC 3339 timestamps with a numeric offset, such as Box's
/// 2026-10-27T11:46:26-07:00 or Jira's 2026-10-27T21:46:26.034+0300, as well
/// as UTC ones.
pub(crate) fn offset_timestamp_ms(value: &str) -> Result<i64> {
    if value.ends_with('Z') {
        return timestamp_ms(value);
    }
    let offset_length = if value.as_bytes().get(value.len().wrapping_sub(3)) == Some(&b':') {
        6
    } else {
        5
    };
    let split = value
        .len()
        .checked_sub(offset_length)
        .filter(|split| *split >= 19 && value.is_char_boundary(*split))
        .ok_or(ConnectorError::Response)?;
    let (local, offset) = value.split_at(split);
    let (sign, digits) = match offset.replace(':', "").as_bytes() {
        [b'+', digits @ ..] => (1, digits.to_vec()),
        [b'-', digits @ ..] => (-1, digits.to_vec()),
        _ => return Err(ConnectorError::Response),
    };
    if digits.len() != 4 || !digits.iter().all(u8::is_ascii_digit) {
        return Err(ConnectorError::Response);
    }
    let hours = i64::from(digits[0] - b'0') * 10 + i64::from(digits[1] - b'0');
    let minutes = i64::from(digits[2] - b'0') * 10 + i64::from(digits[3] - b'0');
    if hours > 14 || minutes > 59 {
        return Err(ConnectorError::Response);
    }
    Ok(timestamp_ms(&format!("{local}Z"))? - sign * (hours * 60 + minutes) * 60_000)
}

fn digits_value(text: &str) -> Result<i64> {
    if !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(ConnectorError::Response);
    }
    text.parse().map_err(|_| ConnectorError::Response)
}

/// Decodes a URL query component's percent escapes.
pub(crate) fn percent_decode(text: &str) -> Result<String> {
    let bytes = text.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let hex = bytes
                .get(index + 1..index + 3)
                .and_then(|hex| std::str::from_utf8(hex).ok())
                .and_then(|hex| u8::from_str_radix(hex, 16).ok())
                .ok_or(ConnectorError::Response)?;
            decoded.push(hex);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).map_err(|_| ConnectorError::Response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offsets_shift_timestamps_to_utc() {
        let utc = timestamp_ms("2026-10-27T18:46:26.034Z").unwrap();
        assert_eq!(
            offset_timestamp_ms("2026-10-27T21:46:26.034+0300").unwrap(),
            utc
        );
        assert_eq!(
            offset_timestamp_ms("2026-10-27T13:16:26.034-05:30").unwrap(),
            utc
        );
        assert_eq!(
            offset_timestamp_ms("2026-10-27T18:46:26.034Z").unwrap(),
            utc
        );
        assert_eq!(
            offset_timestamp_ms("2026-10-27T11:46:26-07:00").unwrap(),
            timestamp_ms("2026-10-27T18:46:26Z").unwrap()
        );
        for invalid in [
            "+0000",
            "2026-10-27T18:46:26.034+03",
            "2026-10-27T18:46:26.034+2500",
            "2026-10-27T18:46:26.034 0300",
        ] {
            assert!(offset_timestamp_ms(invalid).is_err(), "{invalid}");
        }
    }
}
