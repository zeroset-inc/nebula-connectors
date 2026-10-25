use std::{collections::HashSet, fmt, sync::OnceLock};

use serde::{
    Deserializer,
    de::{self, MapAccess, SeqAccess, Visitor},
};
use serde_json::{Value, value::RawValue};

use crate::protocol::Batch;

pub const MAX_BATCH_BYTES: usize = 1 << 20;
pub const MAX_CURSOR_BYTES: usize = 64 << 10;
pub const MAX_METADATA_BYTES: usize = 32 << 10;
const MAX_DEPTH: usize = 64;
pub const SCHEMA: &str = include_str!("../schema/protocol-v1.json");

#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("invalid_connector_batch")]
pub struct InvalidBatch;

/// Decode bounded control JSON with the same integrity rules as connector batches.
pub fn decode_control<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    limit: usize,
) -> Result<T, InvalidBatch> {
    if bytes.len() > limit {
        return Err(InvalidBatch);
    }
    check_json(bytes, 0).map_err(|_| InvalidBatch)?;
    let value: Value = serde_json::from_slice(bytes).map_err(|_| InvalidBatch)?;
    check_numbers(&value)?;
    serde_json::from_value(value).map_err(|_| InvalidBatch)
}

/// A bounded batch that passed the canonical schema and runtime integrity checks.
/// Transport uses these bytes so validation cannot be invalidated by later mutation.
pub struct ValidatedBatch {
    batch: Batch,
    bytes: Vec<u8>,
}

impl ValidatedBatch {
    pub fn parse(bytes: &[u8]) -> Result<Self, InvalidBatch> {
        if bytes.len() > MAX_BATCH_BYTES {
            return Err(InvalidBatch);
        }
        check_json(bytes, 0).map_err(|_| InvalidBatch)?;
        let mut value: Value = serde_json::from_slice(bytes).map_err(|_| InvalidBatch)?;
        check_numbers(&value)?;
        check_bounds(&value)?;
        normalize_integers(&mut value)?;
        static VALIDATOR: OnceLock<Result<jsonschema::Validator, InvalidBatch>> = OnceLock::new();
        let validator = VALIDATOR
            .get_or_init(|| {
                let schema: Value = serde_json::from_str(SCHEMA).map_err(|_| InvalidBatch)?;
                jsonschema::validator_for(&schema).map_err(|_| InvalidBatch)
            })
            .as_ref()
            .map_err(|_| InvalidBatch)?;
        if !validator.is_valid(&value) {
            return Err(InvalidBatch);
        }
        let batch: Batch = serde_json::from_value(value).map_err(|_| InvalidBatch)?;
        let bytes = encode_bounded(&batch, MAX_BATCH_BYTES)?;
        Ok(Self { batch, bytes })
    }

    pub fn new(batch: &Batch) -> Result<Self, InvalidBatch> {
        Self::parse(&encode_bounded(batch, MAX_BATCH_BYTES)?)
    }

    pub fn batch(&self) -> &Batch {
        &self.batch
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

pub(crate) fn check_json(bytes: &[u8], depth: usize) -> Result<(), serde_json::Error> {
    let raw: &RawValue = serde_json::from_slice(bytes)?;
    let first = raw.get().as_bytes().first();
    if matches!(first, Some(b'{' | b'[')) {
        if depth >= MAX_DEPTH {
            return Err(de::Error::custom("json_depth"));
        }
        let mut deserializer = serde_json::Deserializer::from_str(raw.get());
        if first == Some(&b'{') {
            deserializer.deserialize_map(UniqueKeys(depth + 1))
        } else {
            deserializer.deserialize_seq(UniqueKeys(depth + 1))
        }
    } else {
        Ok(())
    }
}

struct UniqueKeys(usize);
impl<'de> Visitor<'de> for UniqueKeys {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("unique bounded JSON")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        let mut keys = HashSet::new();
        while let Some(key) = map.next_key::<String>()? {
            if key.contains('\0')
                || key == "$serde_json::private::Number"
                || key == "$serde_json::private::RawValue"
                || !keys.insert(key)
            {
                return Err(de::Error::custom("duplicate_key"));
            }
            let value: &RawValue = map.next_value()?;
            check_json(value.get().as_bytes(), self.0).map_err(de::Error::custom)?;
        }
        Ok(())
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<(), A::Error> {
        while let Some(value) = sequence.next_element::<&RawValue>()? {
            check_json(value.get().as_bytes(), self.0).map_err(de::Error::custom)?;
        }
        Ok(())
    }
}

fn check_numbers(value: &Value) -> Result<(), InvalidBatch> {
    match value {
        Value::String(value) if value.contains('\0') => return Err(InvalidBatch),
        Value::Number(number) => {
            let raw = number.as_str();
            if raw.len() > 128 {
                return Err(InvalidBatch);
            }
            if let Some((_, exponent)) = raw.split_once(['e', 'E']) {
                let exponent: i32 = exponent.parse().map_err(|_| InvalidBatch)?;
                if !(-1024..=1024).contains(&exponent) {
                    return Err(InvalidBatch);
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                check_numbers(value)?;
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                check_numbers(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn check_bounds(value: &Value) -> Result<(), InvalidBatch> {
    let events = value
        .get("events")
        .and_then(Value::as_array)
        .ok_or(InvalidBatch)?;
    if events.len() > 100 {
        return Err(InvalidBatch);
    }
    for event in events {
        if event.get("type").and_then(Value::as_str) == Some("upsert") {
            let metadata = event
                .get("metadata")
                .and_then(Value::as_object)
                .ok_or(InvalidBatch)?;
            if metadata.len() > 64
                || serialized_size(&Value::Object(metadata.clone()))? > MAX_METADATA_BYTES
            {
                return Err(InvalidBatch);
            }
            for (key, value) in metadata {
                if key.is_empty()
                    || key.len() > 64
                    || value.as_str().is_some_and(|s| s.len() > 1024)
                {
                    return Err(InvalidBatch);
                }
            }
        }
        if let Some(cursor) = event.get("cursor")
            && serialized_size(cursor)? > MAX_CURSOR_BYTES
        {
            return Err(InvalidBatch);
        }
        if let Some(permissions) = event.get("permissions") {
            for field in ["allow", "deny"] {
                if permissions
                    .get(field)
                    .and_then(Value::as_array)
                    .is_some_and(|a| a.len() > 1024)
                {
                    return Err(InvalidBatch);
                }
            }
        }
    }
    Ok(())
}

fn serialized_size(value: &Value) -> Result<usize, InvalidBatch> {
    Ok(serde_json::to_vec(value).map_err(|_| InvalidBatch)?.len())
}

fn normalize_integers(value: &mut Value) -> Result<(), InvalidBatch> {
    for field in ["protocol_version", "fence", "sequence"] {
        normalize_field(value, field)?;
    }
    if let Some(events) = value.get_mut("events").and_then(Value::as_array_mut) {
        for event in events {
            for field in ["observed_at_ms", "scope_revision"] {
                normalize_field(event, field)?;
            }
            if let Some(content) = event.get_mut("content") {
                normalize_field(content, "byte_length")?;
            }
            if let Some(permissions) = event.get_mut("permissions") {
                normalize_field(permissions, "observed_at_ms")?;
            }
            if event.get("type").and_then(Value::as_str) == Some("item_observed")
                && let Some(metadata) = event.get_mut("metadata")
            {
                for field in ["created_at_ms", "modified_at_ms", "byte_length"] {
                    normalize_field(metadata, field)?;
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn normalize_field(value: &mut Value, name: &str) -> Result<(), InvalidBatch> {
    if let Some(field) = value.get_mut(name) {
        let number = field.as_number().ok_or(InvalidBatch)?;
        let raw = number.as_str();
        let (mantissa, exponent) = raw.split_once(['e', 'E']).unwrap_or((raw, "0"));
        let exponent: i32 = exponent.parse().map_err(|_| InvalidBatch)?;
        let fractional = mantissa.split_once('.').map_or(0, |(_, s)| s.len());
        let mut digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
        let scale = exponent - fractional as i32;
        if scale < 0 {
            let remove = (-scale) as usize;
            if remove > digits.len() {
                if digits.bytes().any(|b| b != b'0') {
                    return Err(InvalidBatch);
                }
                digits.clear();
            } else {
                if digits.as_bytes()[digits.len() - remove..]
                    .iter()
                    .any(|b| *b != b'0')
                {
                    return Err(InvalidBatch);
                }
                digits.truncate(digits.len() - remove);
            }
        }
        let digits = digits.trim_start_matches('0');
        let mut integer: u64 = if digits.is_empty() {
            0
        } else {
            digits.parse().map_err(|_| InvalidBatch)?
        };
        if integer != 0 && scale > 0 {
            if scale > 16 {
                return Err(InvalidBatch);
            }
            integer = integer
                .checked_mul(10u64.pow(scale as u32))
                .ok_or(InvalidBatch)?;
        }
        if integer > 9_007_199_254_740_991 || raw.starts_with('-') && integer != 0 {
            return Err(InvalidBatch);
        }
        *field = Value::from(integer);
    }
    Ok(())
}

pub(crate) fn encode_bounded<T: serde::Serialize + ?Sized>(
    value: &T,
    limit: usize,
) -> Result<Vec<u8>, InvalidBatch> {
    let mut output = BoundedJson {
        bytes: Vec::new(),
        limit,
        depth: 0,
        in_string: false,
        escaped: false,
    };
    serde_json::to_writer(&mut output, value).map_err(|_| InvalidBatch)?;
    Ok(output.bytes)
}

struct BoundedJson {
    bytes: Vec<u8>,
    limit: usize,
    depth: usize,
    in_string: bool,
    escaped: bool,
}
impl std::io::Write for BoundedJson {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::ErrorKind::FileTooLarge.into());
        }
        for byte in bytes {
            if self.in_string {
                if self.escaped {
                    self.escaped = false;
                } else if *byte == b'\\' {
                    self.escaped = true;
                } else if *byte == b'"' {
                    self.in_string = false;
                }
            } else {
                match byte {
                    b'"' => self.in_string = true,
                    b'{' | b'[' => {
                        self.depth += 1;
                        if self.depth > MAX_DEPTH {
                            return Err(std::io::ErrorKind::InvalidData.into());
                        }
                    }
                    b'}' | b']' => self.depth = self.depth.saturating_sub(1),
                    _ => {}
                }
            }
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
