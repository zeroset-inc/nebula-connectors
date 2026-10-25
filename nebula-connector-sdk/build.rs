use std::{env, fs, path::PathBuf};

fn main() {
    println!("cargo:rerun-if-changed=schema/protocol-v1.json");
    let mut schema: serde_json::Value =
        serde_json::from_str(include_str!("schema/protocol-v1.json")).expect("canonical schema");
    // The root is a reference to the named Batch definition, not a second type.
    schema
        .as_object_mut()
        .expect("schema object")
        .remove("title");
    // Patterns are enforced by the full runtime schema. Codegen emits wire types
    // without adding a second regex engine to every connector's dependencies.
    remove_patterns(&mut schema);
    let mut settings = typify::TypeSpaceSettings::default();
    settings.with_conversion(
        serde_json::from_str(r#"{"type":"number"}"#).expect("number schema"),
        "::serde_json::Number",
        std::iter::empty(),
    );
    let mut types = typify::TypeSpace::new(&settings);
    types
        .add_root_schema(serde_json::from_value(schema).expect("codegen schema"))
        .expect("generate protocol types");
    assert!(!types.uses_regress() && !types.uses_chrono() && !types.uses_uuid());
    let path = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo OUT_DIR"));
    fs::write(path.join("protocol.rs"), types.to_stream().to_string())
        .expect("write protocol types");
}

fn remove_patterns(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            object.remove("pattern");
            object.values_mut().for_each(remove_patterns);
        }
        serde_json::Value::Array(values) => values.iter_mut().for_each(remove_patterns),
        _ => {}
    }
}
