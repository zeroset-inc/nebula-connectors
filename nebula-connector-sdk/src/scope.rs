use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{protocol::ItemMetadata, validation};

#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("invalid_connector_scope")]
pub struct InvalidScope;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TimeRange {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    from_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    to_ms: Option<i64>,
}

impl TimeRange {
    fn used(&self) -> bool {
        self.from_ms.is_some() || self.to_ms.is_some()
    }

    fn valid(&self) -> bool {
        self.from_ms
            .zip(self.to_ms)
            .is_none_or(|(from, to)| from < to)
    }

    fn select(&self, value: Option<i64>) -> Selection {
        if !self.used() {
            return Selection::Selected;
        }
        let Some(value) = value else {
            return Selection::MissingMetadata;
        };
        if self.from_ms.is_some_and(|from| value < from) || self.to_ms.is_some_and(|to| value >= to)
        {
            Selection::Excluded
        } else {
            Selection::Selected
        }
    }
}

/// Validated scope shared by adapters before download and by the service at admission.
/// Resolved provider container IDs are exact matches; paths are provider mappings.
#[derive(Clone, Debug, Serialize)]
pub struct Filter {
    containers: Vec<String>,
    content_types: Vec<String>,
    created: TimeRange,
    modified: TimeRange,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireFilter {
    containers: Vec<String>,
    content_types: Option<Vec<String>>,
    created: Option<TimeRange>,
    modified: Option<TimeRange>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Selection {
    Selected,
    Excluded,
    MissingMetadata,
}

impl Selection {
    pub fn selection(self) -> &'static str {
        match self {
            Self::Selected => "selected",
            Self::Excluded => "excluded",
            Self::MissingMetadata => "error",
        }
    }

    pub fn reason(self) -> &'static str {
        match self {
            Self::Selected => "in_scope",
            Self::Excluded => "filtered",
            Self::MissingMetadata => "missing_metadata",
        }
    }
}

impl Filter {
    pub fn parse(
        bytes: &[u8],
        recurring: bool,
        supported: &[String],
    ) -> Result<Self, InvalidScope> {
        let mut value: Value = validation::decode_control(bytes, validation::MAX_CURSOR_BYTES)
            .map_err(|_| InvalidScope)?;
        for key in ["created", "modified"] {
            if let Some(range) = value.get_mut(key) {
                for bound in ["from_ms", "to_ms"] {
                    if range.get(bound).is_some_and(|value| !value.is_null()) {
                        validation::normalize_field(range, bound).map_err(|_| InvalidScope)?;
                    }
                }
            }
        }
        let wire: WireFilter = serde_json::from_value(value).map_err(|_| InvalidScope)?;
        let filter = Self {
            containers: wire.containers,
            content_types: wire.content_types.unwrap_or_default(),
            created: wire.created.unwrap_or_default(),
            modified: wire.modified.unwrap_or_default(),
        };
        if filter.containers.is_empty()
            || filter.containers.len() > 256
            || filter.content_types.len() > 64
            || !unique_bounded(&filter.containers)
            || !unique_bounded(&filter.content_types)
            || !filter.created.valid()
            || !filter.modified.valid()
            || recurring && filter.modified.to_ms.is_some()
        {
            return Err(InvalidScope);
        }
        for (field, used) in [
            ("container", true),
            ("content_type", !filter.content_types.is_empty()),
            ("created_at", filter.created.used()),
            ("modified_at", filter.modified.used()),
        ] {
            if used && !supported.iter().any(|value| value == field) {
                return Err(InvalidScope);
            }
        }
        Ok(filter)
    }

    pub fn containers(&self) -> &[String] {
        &self.containers
    }

    /// Missing metadata required by an active filter is an explicit error.
    pub fn select(&self, metadata: &ItemMetadata) -> Selection {
        let Some(container) = &metadata.container_id else {
            return Selection::MissingMetadata;
        };
        if !self.containers.iter().any(|id| id == container.as_str()) {
            return Selection::Excluded;
        }
        if !self.content_types.is_empty() {
            let Some(content_type) = &metadata.content_type else {
                return Selection::MissingMetadata;
            };
            if !self
                .content_types
                .iter()
                .any(|kind| kind == content_type.as_str())
            {
                return Selection::Excluded;
            }
        }
        for selection in [
            self.created.select(metadata.created_at_ms),
            self.modified.select(metadata.modified_at_ms),
        ] {
            if selection != Selection::Selected {
                return selection;
            }
        }
        Selection::Selected
    }
}

fn unique_bounded(values: &[String]) -> bool {
    let mut seen = HashSet::with_capacity(values.len());
    values
        .iter()
        .all(|value| !value.is_empty() && value.len() <= 4096 && seen.insert(value))
}
