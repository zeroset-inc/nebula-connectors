//! Broker calls, cursors and timestamps shared by the provider adapters.

use std::{
    future::Future,
    pin::Pin,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use nebula_connector_sdk::{
    ClientError, ConnectorError, Context,
    client::{ProviderResponse, REQUEST_TIMEOUT, Transfer, TransferFile, Transferred},
};
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
    broker_read_bounded(context, operation, arguments, None).await
}

pub(crate) async fn broker_read_bounded(
    context: &Context,
    operation: &str,
    arguments: Value,
    max_bytes: Option<u64>,
) -> std::result::Result<ProviderResponse, ClientError> {
    with_fresh_attempts(|attempt| {
        let arguments = arguments.clone();
        async move {
            context
                .service
                .provider_bounded(&context.run, &attempt, operation, arguments, max_bytes)
                .await
        }
    })
    .await
}

/// Asks at most this often when an answer is a failure that may pass.
const ATTEMPTS: u32 = 3;
/// A longer pause than this ends the retries.
const LONGEST_PAUSE: Duration = Duration::from_secs(120);
/// How long a transfer keeps asking a service whose transfer slots are taken
/// or whose engine has not verified the upload: one request timeout, by which
/// every transfer that held a slot when this one was refused has answered.
const TRANSFER_PATIENCE: Duration = REQUEST_TIMEOUT;
/// The pause a busy service gets when it names none, and the least a transfer
/// pauses once it is past the attempts every failure gets.
const PATIENT_PAUSE: Duration = Duration::from_secs(1);

/// Has the broker store a provider file as an original. Failures are retried
/// as `broker_read` retries them. A service that answers that it is busy is
/// asked again until `TRANSFER_PATIENCE` is spent, doubling the pause it asks
/// for each time, and an upload the engine has not yet verified is named in
/// the next request, so the provider is not read again.
pub(crate) async fn broker_transfer(
    context: &Context,
    operation: &str,
    arguments: Value,
    file: TransferFile<'_>,
    max_bytes: u64,
) -> std::result::Result<Transfer, ClientError> {
    let mut unverified = None::<String>;
    let mut waiting = None::<Instant>;
    let mut busy_answers = 0;
    for attempt in 0.. {
        let named = TransferFile {
            unverified_upload: unverified.as_deref(),
            ..file
        };
        let answer = context
            .service
            .transfer(
                &context.run,
                &Uuid::new_v4().to_string(),
                operation,
                arguments.clone(),
                named,
                Some(max_bytes),
            )
            .await;
        let (error, pause) = match answer {
            Ok(Transferred::Held(transfer)) => return Ok(transfer),
            Ok(Transferred::Unverified { upload_id }) => {
                // The service has just waited a whole request for the engine,
                // so the first answer is followed up at once.
                let pause = if unverified.is_some() {
                    PATIENT_PAUSE
                } else {
                    Duration::ZERO
                };
                unverified = Some(upload_id);
                waiting.get_or_insert_with(Instant::now);
                (ClientError::Capacity, Some(pause))
            }
            Err(ClientError::Rejected {
                status: 429,
                retry_after,
            }) => {
                let pause = busy_pause(retry_after, busy_answers);
                busy_answers += 1;
                waiting.get_or_insert_with(Instant::now);
                let error = ClientError::Rejected {
                    status: 429,
                    retry_after,
                };
                (error, Some(pause))
            }
            Err(error) => {
                let pause = retry_pause(&error, attempt);
                (error, pause)
            }
        };
        let waited = waiting.map(|since| since.elapsed());
        match pause.and_then(|pause| next_pause(attempt, waited, pause)) {
            Some(pause) => tokio::time::sleep(pause).await,
            None => return Err(error),
        }
    }
    unreachable!()
}

/// The pause before a busy service is asked again: what it asks for, or
/// `PATIENT_PAUSE`, doubled per earlier busy answer. Doubling stops at
/// `LONGEST_PAUSE` unless the service itself asks for longer.
fn busy_pause(retry_after: Option<Duration>, earlier: u32) -> Duration {
    let asked = retry_after.unwrap_or(PATIENT_PAUSE);
    asked
        .saturating_mul(1 << earlier.min(16))
        .min(LONGEST_PAUSE.max(asked))
}

/// The pause a transfer takes before it asks again after answer number
/// `attempt`, counted from zero, or none when it gives up. Every failure gets
/// `ATTEMPTS` answers. Past them, a transfer that has been told to wait for
/// `waited` asks again until `TRANSFER_PATIENCE` is spent, pausing at least
/// `PATIENT_PAUSE`. Its last pause ends with the patience, so it asks once
/// more at that moment.
fn next_pause(attempt: u32, waited: Option<Duration>, pause: Duration) -> Option<Duration> {
    if pause > LONGEST_PAUSE {
        return None;
    }
    if attempt + 1 < ATTEMPTS {
        return Some(pause);
    }
    let left = TRANSFER_PATIENCE.checked_sub(waited?)?;
    (!left.is_zero()).then(|| pause.max(PATIENT_PAUSE).min(left))
}

/// How long to pause before asking again after a failure that may pass:
/// what the answer asks for, or one second doubled per earlier attempt.
fn retry_pause(error: &ClientError, attempt: u32) -> Option<Duration> {
    let fallback = Duration::from_secs(1 << attempt.min(6));
    match error {
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
    }
}

async fn with_fresh_attempts<T, F>(
    mut call: impl FnMut(String) -> F,
) -> std::result::Result<T, ClientError>
where
    F: Future<Output = std::result::Result<T, ClientError>>,
{
    for attempt in 0..ATTEMPTS {
        let error = match call(Uuid::new_v4().to_string()).await {
            Ok(response) => return Ok(response),
            Err(error) => error,
        };
        match retry_pause(&error, attempt) {
            Some(pause) if attempt + 1 < ATTEMPTS && pause <= LONGEST_PAUSE => {
                tokio::time::sleep(pause).await;
            }
            _ => return Err(error),
        }
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
    fn a_failing_transfer_is_asked_about_three_times() {
        let second = Duration::from_secs(1);
        assert_eq!(next_pause(0, None, second), Some(second));
        assert_eq!(next_pause(1, None, 2 * second), Some(2 * second));
        assert_eq!(next_pause(2, None, second), None);
        assert_eq!(next_pause(0, None, LONGEST_PAUSE + second), None);
        assert_eq!(retry_pause(&ClientError::Transport, 2), Some(4 * second));
        assert_eq!(retry_pause(&ClientError::Response, 0), None);
    }

    /// When each request is sent, in seconds, if the service only ever
    /// answers at once that it is busy, asking for `retry_after`.
    fn busy_schedule(retry_after: Option<Duration>) -> Vec<u64> {
        let (mut sent, mut now) = (vec![], Duration::ZERO);
        for attempt in 0.. {
            sent.push(now.as_secs());
            let pause = busy_pause(retry_after, attempt);
            match next_pause(attempt, Some(now), pause) {
                Some(pause) => now += pause,
                None => break,
            }
        }
        sent
    }

    #[test]
    fn a_busy_service_is_asked_until_the_patience_is_spent() {
        let second = Duration::from_secs(1);
        let patience = TRANSFER_PATIENCE.as_secs();
        for retry_after in [None, Some(second)] {
            assert_eq!(
                busy_schedule(retry_after),
                [0, 1, 3, 7, 15, 31, 63, patience],
                "pauses double once, and the last ask comes as the patience ends"
            );
        }
        let unpaced = busy_schedule(Some(Duration::ZERO));
        assert_eq!(
            (&unpaced[..5], unpaced.last()),
            (&[0, 0, 0, 1, 2][..], Some(&patience)),
            "past its three attempts a transfer asks at most once a second"
        );
        assert_eq!(
            busy_schedule(Some(100 * second)),
            [0, 100, 220],
            "a long pause the service asks for is taken, within the attempts"
        );
        assert_eq!(busy_schedule(Some(LONGEST_PAUSE + second)), [0]);
    }

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
