use nebula_connector_sdk::{protocol::ItemMetadata, scope::Filter};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
struct Case {
    name: String,
    scope: Value,
    recurring: bool,
    supported: Vec<String>,
    valid: bool,
    metadata: Option<Value>,
    selection: Option<String>,
    reason: Option<String>,
}

#[test]
fn shared_scope_cases_match_service_selection() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("../fixtures/scope-v1.json")).unwrap();
    for case in cases {
        let result = Filter::parse(
            &serde_json::to_vec(&case.scope).unwrap(),
            case.recurring,
            &case.supported,
        );
        assert_eq!(result.is_ok(), case.valid, "{}", case.name);
        if case.valid {
            let filter = result.unwrap();
            let metadata: ItemMetadata = serde_json::from_value(case.metadata.unwrap()).unwrap();
            let selection = filter.select(&metadata);
            assert_eq!(
                selection.selection(),
                case.selection.unwrap(),
                "{}",
                case.name
            );
            assert_eq!(selection.reason(), case.reason.unwrap(), "{}", case.name);
        }
    }
}

#[test]
fn scope_count_and_utf8_bounds_apply_before_provider_work() {
    let supported = vec!["container".into(), "content_type".into()];
    let parse =
        |value: Value| Filter::parse(&serde_json::to_vec(&value).unwrap(), false, &supported);
    assert!(parse(json!({"containers":["é".repeat(2048)]})).is_ok());
    assert!(parse(json!({"containers":["é".repeat(2049)]})).is_err());
    assert!(
        parse(json!({"containers":(0..256).map(|n|n.to_string()).collect::<Vec<_>>()})).is_ok()
    );
    assert!(
        parse(json!({"containers":(0..257).map(|n|n.to_string()).collect::<Vec<_>>()})).is_err()
    );
    assert!(parse(json!({"containers":["a"],"content_types":(0..64).map(|n|n.to_string()).collect::<Vec<_>>()})).is_ok());
    assert!(parse(json!({"containers":["a"],"content_types":(0..65).map(|n|n.to_string()).collect::<Vec<_>>()})).is_err());
    assert!(parse(json!({"containers":(0..256).map(|n|format!("{n}{}", "x".repeat(300))).collect::<Vec<_>>()})).is_err());
    assert!(
        Filter::parse(
            br#"{"containers":["a"],"containers":["b"]}"#,
            false,
            &supported
        )
        .is_err()
    );
}
