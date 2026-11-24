use super::*;
use serde_json::json;

fn request(parent: &str, query: &str, cursor: &str, body: Option<&str>) -> Request {
    Request {
        context: Context {
            version: 1,
            attempt_id: "attempt".into(),
            tenant_id: "tenant".into(),
            connection_id: "connection".into(),
        },
        parent: parent.into(),
        query: query.into(),
        cursor: cursor.into(),
        pages: 0,
        matched: 0,
        response: body.map(|body| serde_json::value::RawValue::from_string(body.into()).unwrap()),
    }
}

#[test]
fn filtered_empty_page_retains_bound_continuation_without_following_provider_urls() {
    let first = process("onedrive", request("", "legal", "", Some(r#"{"value":[{"id":"F1","name":"Finance","folder":{}}],"@odata.nextLink":"https://untrusted.example/contacts?$skiptoken=p2&$expand=secrets"}"#))).unwrap().page.unwrap();
    assert!(first.items.is_empty());
    assert!(!first.cursor.is_empty());
    assert!(!first.continue_scan);
    let planned = process("onedrive", request("", "legal", &first.cursor, None))
        .unwrap()
        .operation
        .unwrap();
    assert_eq!(planned.name, "setup.root.children");
    assert_eq!(
        planned.arguments,
        json!({"select":"id,name,folder","top":"200","skiptoken":"p2"})
    );
    for (provider, parent, query) in [
        ("onedrive", "F1", "legal"),
        ("onedrive", "", "other"),
        ("teams", "", "legal"),
    ] {
        assert_eq!(
            process(provider, request(parent, query, &first.cursor, None))
                .unwrap()
                .error,
            Some(Error::InvalidCursor)
        );
    }
}

#[test]
fn slack_search_continuation_stops_at_its_page_or_match_budget() {
    let body = r#"{"ok":true,"channels":[],"response_metadata":{"next_cursor":"next"}}"#;
    let page = process("slack", request("", "sales", "", Some(body)))
        .unwrap()
        .page
        .unwrap();
    assert!(page.continue_scan);
    let mut last = request("", "sales", "", Some(body));
    last.pages = MAX_PAGES - 1;
    assert!(!process("slack", last).unwrap().page.unwrap().continue_scan);
    let mut enough = request("", "sales", "", Some(body));
    enough.matched = 200;
    assert!(
        !process("slack", enough)
            .unwrap()
            .page
            .unwrap()
            .continue_scan
    );
    assert!(
        !process("slack", request("", "", "", Some(body)))
            .unwrap()
            .page
            .unwrap()
            .continue_scan
    );
}

#[test]
fn graph_continuation_rejects_extra_parameters_and_repeated_tokens() {
    let forged = URL_SAFE_NO_PAD.encode(
        serde_json::to_vec(&Cursor {
            version: 1,
            provider: "onedrive".into(),
            parent: String::new(),
            query: String::new(),
            next: r#"{"skiptoken":"p2","expand":"secrets"}"#.into(),
        })
        .unwrap(),
    );
    assert_eq!(
        process("onedrive", request("", "", &forged, None))
            .unwrap()
            .error,
        Some(Error::InvalidCursor)
    );
    let body =
        r#"{"value":[],"@odata.nextLink":"https://graph.microsoft.com/items?$skiptoken=p2"}"#;
    let first = process("onedrive", request("", "", "", Some(body)))
        .unwrap()
        .page
        .unwrap();
    assert_eq!(
        process("onedrive", request("", "", &first.cursor, Some(body)))
            .unwrap()
            .error,
        Some(Error::Unavailable)
    );
}

#[test]
fn request_version_and_provider_response_bounds_are_enforced() {
    let mut invalid = request("", "", "", None);
    invalid.context.version = 2;
    assert_eq!(
        process("slack", invalid).err(),
        Some("invalid_discovery_request")
    );
    let body = serde_json::to_string(&"x".repeat(MAX_BODY_BYTES)).unwrap();
    assert_eq!(
        process("slack", request("", "", "", Some(&body))).err(),
        Some("invalid_discovery_request")
    );
    assert!(
        crate::discovery::process(
            "github",
            request("", "", "", Some(r#"[{"full_name":"owner/name"}]"#))
        )
        .unwrap()
        .page
        .is_some()
    );
}

#[test]
fn sharepoint_offers_the_root_site_before_searching_for_the_rest() {
    let planned = process("sharepoint", request("", "", "", None))
        .unwrap()
        .operation
        .unwrap();
    assert_eq!(planned.name, "setup.sites.root");
    assert_eq!(planned.arguments, json!({"select":"id,displayName,name"}));

    let root = r#"{"id":"contoso.sharepoint.com,1,2","displayName":"Communication site","name":"contoso"}"#;
    let first = process("sharepoint", request("", "", "", Some(root)))
        .unwrap()
        .page
        .unwrap();
    assert_eq!(first.items.len(), 1);
    assert_eq!(first.items[0].id, "contoso.sharepoint.com,1,2");
    assert_eq!(first.items[0].name, "Communication site");
    assert!(first.items[0].has_children && !first.items[0].selectable);
    assert!(
        first.continue_scan,
        "one request lists the root site and the search results"
    );

    let planned = process("sharepoint", request("", "", &first.cursor, None))
        .unwrap()
        .operation
        .unwrap();
    assert_eq!(planned.name, "setup.sites.search");
    assert_eq!(
        planned.arguments,
        json!({"search":"*","select":"id,displayName,name","top":"100"})
    );

    let found = r#"{"value":[{"id":"contoso.sharepoint.com,1,2","displayName":"Communication site"},{"id":"contoso.sharepoint.com,3,4","displayName":"Marketing"}],"@odata.nextLink":"https://graph.microsoft.com/v1.0/sites?search=*&$skiptoken=s2"}"#;
    let second = process("sharepoint", request("", "", &first.cursor, Some(found)))
        .unwrap()
        .page
        .unwrap();
    assert_eq!(
        second
            .items
            .iter()
            .map(|site| site.name.as_str())
            .collect::<Vec<_>>(),
        ["Marketing"],
        "the root site is listed once"
    );
    assert!(!second.continue_scan);
    let planned = process("sharepoint", request("", "", &second.cursor, None))
        .unwrap()
        .operation
        .unwrap();
    assert_eq!(planned.arguments["skiptoken"], "s2");
    let last = process(
        "sharepoint",
        request("", "", &second.cursor, Some(r#"{"value":[]}"#)),
    )
    .unwrap()
    .page
    .unwrap();
    assert!(last.items.is_empty() && last.cursor.is_empty());
}

#[test]
fn a_sharepoint_site_search_leaves_out_a_root_site_it_does_not_name() {
    let root = r#"{"id":"contoso.sharepoint.com,1,2","displayName":"Communication site","name":"contoso"}"#;
    let first = process("sharepoint", request("", "market", "", Some(root)))
        .unwrap()
        .page
        .unwrap();
    assert!(first.items.is_empty());
    assert!(first.continue_scan);
    let planned = process("sharepoint", request("", "market", &first.cursor, None))
        .unwrap()
        .operation
        .unwrap();
    assert_eq!(planned.arguments["search"], "market");

    let named = process("sharepoint", request("", "CONTOSO", "", Some(root)))
        .unwrap()
        .page
        .unwrap();
    assert_eq!(named.items.len(), 1);
}

#[test]
fn a_sharepoint_site_cursor_must_name_the_root_page_it_follows() {
    let cursor = |next: &str| {
        URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&Cursor {
                version: 1,
                provider: "sharepoint".into(),
                parent: String::new(),
                query: String::new(),
                next: next.into(),
            })
            .unwrap(),
        )
    };
    for forged in [
        r#"{"skiptoken":"s2"}"#,
        r#"{"root":"not a site id"}"#,
        r#"{"root":"","skiptoken":"s2","skip":"1","expand":"secrets"}"#,
    ] {
        assert_eq!(
            process("sharepoint", request("", "", &cursor(forged), None))
                .unwrap()
                .error,
            Some(Error::InvalidCursor),
            "{forged}"
        );
    }
    let libraries = process(
        "sharepoint",
        request("contoso.sharepoint.com,1,2", "", "", None),
    )
    .unwrap()
    .operation
    .unwrap();
    assert_eq!(libraries.name, "setup.drives.list");
}

#[test]
fn normalized_labels_do_not_inherit_a_provider_page_count_cap() {
    let labels = (0..1200)
        .map(|i| json!({"id":format!("Label_{i}"),"name":format!("Label {i}"),"type":"user"}))
        .collect::<Vec<_>>();
    let body = json!({"labels":labels}).to_string();
    let page = process("gmail", request("", "", "", Some(&body)))
        .unwrap()
        .page
        .unwrap();
    assert_eq!(page.items.len(), 1200);
    assert!(page.cursor.is_empty());
}

#[test]
fn reviewed_setup_response_limits_fit_the_discovery_input_budget() {
    let policies = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("policies");
    let mut providers = 0;
    for entry in std::fs::read_dir(policies).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_none_or(|extension| extension != "json") {
            continue;
        }
        let value: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        for operation in value["operations"].as_array().unwrap() {
            if operation["id"].as_str().unwrap().starts_with("setup.") {
                assert_eq!(operation["caller"], "control");
                assert!(
                    operation["max_response_bytes"].as_u64().unwrap() <= MAX_BODY_BYTES as u64,
                    "{} {}",
                    value["provider"],
                    operation["id"]
                );
                providers += 1;
            }
        }
    }
    assert!(providers >= 13);
}
