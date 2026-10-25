use nebula_connector_sdk::ValidatedBatch;
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
struct Case {
    name: String,
    valid: bool,
    instance: Value,
}

#[test]
fn shared_fixtures_exercise_typed_decoding_and_round_trip() {
    let cases: Vec<Case> =
        serde_json::from_str(include_str!("../fixtures/protocol-v1.json")).unwrap();
    for case in cases {
        let result = ValidatedBatch::parse(&serde_json::to_vec(&case.instance).unwrap());
        assert_eq!(result.is_ok(), case.valid, "{}", case.name);
        if let Ok(batch) = result {
            assert_eq!(
                serde_json::from_slice::<Value>(batch.bytes()).unwrap(),
                case.instance,
                "{} lost fields",
                case.name
            );
            assert!(ValidatedBatch::new(batch.batch()).is_ok());
        }
    }
}

fn checkpoint() -> Value {
    json!({"protocol_version":1,"batch_id":"batch","fence":1,"sequence":0,"events":[{"type":"checkpoint","cursor":{}}]})
}

#[test]
fn rejects_ambiguous_json_and_excessive_depth() {
    for raw in [
        r#"{"protocol_version":1,"batch_id":"a","fence":1,"fence":2,"sequence":0,"events":[{"type":"checkpoint","cursor":{}}]}"#,
        r#"{"protocol_version":1,"batch_id":"\ud800","fence":1,"sequence":0,"events":[{"type":"checkpoint","cursor":{}}]}"#,
        r#"{"protocol_version":1,"batch_id":"a","fence":1.0000000000000001,"sequence":0,"events":[{"type":"checkpoint","cursor":{}}]}"#,
        r#"{"protocol_version":1,"batch_id":"a","fence":9007199254740992,"sequence":0,"events":[{"type":"checkpoint","cursor":{}}]}"#,
    ] {
        assert!(ValidatedBatch::parse(raw.as_bytes()).is_err());
    }
    let mut value = checkpoint();
    let mut nested = json!(0);
    for _ in 0..65 {
        nested = json!([nested]);
    }
    value["events"][0]["cursor"] = json!({"nested":nested});
    assert!(ValidatedBatch::parse(&serde_json::to_vec(&value).unwrap()).is_err());
}

#[test]
fn integral_forms_are_exact_and_opaque_numbers_round_trip() {
    let raw = r#"{"protocol_version":1.0,"batch_id":"a","fence":9.007199254740991e15,"sequence":-0.0,"events":[{"type":"checkpoint","cursor":{"id":9007199254740993,"large":1e1000}}]}"#;
    let batch = ValidatedBatch::parse(raw.as_bytes()).unwrap();
    let value: Value = serde_json::from_slice(batch.bytes()).unwrap();
    assert_eq!(value["fence"], json!(9007199254740991u64));
    assert_eq!(
        value["events"][0]["cursor"]["id"],
        json!(9007199254740993u64)
    );
    assert_eq!(
        value["events"][0]["cursor"]["large"]
            .as_number()
            .unwrap()
            .as_str(),
        "1e+1000"
    );
}

#[test]
fn runtime_limits_count_utf8_bytes_and_preserve_metadata_numbers() {
    let cases: Vec<Case> =
        serde_json::from_str(include_str!("../fixtures/protocol-v1.json")).unwrap();
    let mut value = cases
        .into_iter()
        .find(|c| c.name == "metadata_empty_object")
        .unwrap()
        .instance;
    value["events"][0]["metadata"] = json!({"id":9007199254740993u64});
    let batch = ValidatedBatch::parse(&serde_json::to_vec(&value).unwrap()).unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(batch.bytes()).unwrap()["events"][0]["metadata"]["id"],
        json!(9007199254740993u64)
    );
    value["events"][0]["metadata"] = json!({"x":"é".repeat(513)});
    assert!(ValidatedBatch::parse(&serde_json::to_vec(&value).unwrap()).is_err());
    value["events"][0]["metadata"] = json!({"é".repeat(33):true});
    assert!(ValidatedBatch::parse(&serde_json::to_vec(&value).unwrap()).is_err());
    let mut value = checkpoint();
    value["events"][0]["cursor"] = json!({"x":"x".repeat(65536)});
    assert!(ValidatedBatch::parse(&serde_json::to_vec(&value).unwrap()).is_err());
}

#[test]
fn shared_json_integrity_cases() {
    #[derive(Deserialize)]
    struct Integrity {
        name: String,
        wire: String,
        valid: bool,
    }
    let cases: Vec<Integrity> =
        serde_json::from_str(include_str!("../fixtures/integrity-v1.json")).unwrap();
    for case in cases {
        assert_eq!(
            ValidatedBatch::parse(case.wire.as_bytes()).is_ok(),
            case.valid,
            "{}",
            case.name
        );
    }
}
