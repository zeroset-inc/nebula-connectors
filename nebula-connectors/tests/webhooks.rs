use nebula_connector_sdk::validation::decode_control;
use nebula_connectors::webhook::{self, Provider, Request};
use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
struct Case {
    name: String,
    provider: Provider,
    now: u64,
    request: Value,
    expected: Option<Value>,
    error: Option<String>,
}

#[test]
fn provider_signatures_and_subscription_identity_match_shared_vectors() {
    let cases: Vec<Case> =
        serde_json::from_str(include_str!("../fixtures/webhooks-v1.json")).unwrap();
    assert_eq!(cases.len(), 25);
    for case in cases {
        let bytes = serde_json::to_vec(&case.request).unwrap();
        let request: Request = decode_control(&bytes, 1 << 20).unwrap();
        let result = webhook::verify(case.provider, request, case.now);
        match (case.expected, case.error) {
            (Some(expected), None) => {
                let response = result.unwrap_or_else(|err| panic!("{}: {err}", case.name));
                assert_eq!(
                    serde_json::to_value(response.context).unwrap(),
                    case.request["context"],
                    "{}",
                    case.name
                );
                assert_eq!(
                    serde_json::to_value(response.outcome).unwrap(),
                    expected,
                    "{}",
                    case.name
                );
            }
            (None, Some(expected)) => {
                assert_eq!(result.unwrap_err().to_string(), expected, "{}", case.name)
            }
            _ => panic!("invalid fixture {}", case.name),
        }
    }
}

#[test]
fn identical_graph_notifications_for_later_edits_have_distinct_evidence() {
    let cases: Vec<Case> =
        serde_json::from_str(include_str!("../fixtures/webhooks-v1.json")).unwrap();
    let mut request = cases
        .into_iter()
        .find(|c| c.name == "teams verified deletion preserves resource")
        .unwrap()
        .request;
    let first = webhook::verify(
        Provider::Teams,
        serde_json::from_value(request.clone()).unwrap(),
        1700000000,
    )
    .unwrap();
    request["context"]["attempt_id"] = "attempt-b".into();
    let second = webhook::verify(
        Provider::Teams,
        serde_json::from_value(request).unwrap(),
        1700000000,
    )
    .unwrap();
    let first = serde_json::to_value(first.outcome).unwrap();
    let second = serde_json::to_value(second.outcome).unwrap();
    assert_ne!(
        first["events"][0]["event_id"],
        second["events"][0]["event_id"]
    );
}
