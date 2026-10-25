"""Enforce the accepted public-crate dependency boundary without compiling Rust."""

from pathlib import Path
import sys
import tomllib


ALLOWED = {
    "nebula-connector-sdk": {
        "async-trait", "bytes", "futures", "jsonschema", "reqwest", "serde",
        "serde_json", "sha2", "thiserror", "tokio",
    },
    "nebula-connectors": {
        "nebula-connector-sdk", "axum", "base64", "chrono", "clap", "hmac",
        "jsonwebtoken", "reqwest", "serde", "serde_json", "sha2", "tokio",
        "uuid", "zeroize",
    },
}
BUILD = {"nebula-connector-sdk": {"serde_json", "typify"}, "nebula-connectors": set()}


def check(root):
    errors = []
    workspace = tomllib.loads((root / "Cargo.toml").read_text())
    dependencies = workspace["workspace"]["dependencies"]
    members = workspace["workspace"]["members"]
    if set(members) - ALLOWED.keys():
        errors.append("unapproved public workspace member")
    if workspace.get("patch") or workspace.get("replace"):
        errors.append("dependency override requires approval")
    found = {p.parent.relative_to(root).as_posix() for p in root.rglob("Cargo.toml") if "target" not in p.parts and p.parent != root}
    if found != set(members):
        errors.append("unlisted or missing public package")
    for member in members:
        path = root / member / "Cargo.toml"
        if path.is_symlink() or not path.is_file():
            errors.append("invalid package manifest")
            continue
        manifest = tomllib.loads(path.read_text())
        name = manifest["package"]["name"]
        if name != member or name not in ALLOWED:
            errors.append("unapproved public package")
            continue
        if manifest.get("patch") or manifest.get("replace") or manifest.get("workspace"):
            errors.append("package dependency override requires approval")
        sections = [manifest, *manifest.get("target", {}).values()]
        for section in sections:
            for kind in ("dependencies", "dev-dependencies", "build-dependencies"):
                permitted = BUILD[name] if kind == "build-dependencies" else ALLOWED[name]
                for alias, entry in section.get(kind, {}).items():
                    if alias not in permitted:
                        errors.append(f"{name}: unapproved {kind} {alias}")
                        continue
                    if not isinstance(entry, dict) or entry != {"workspace": True}:
                        errors.append(f"{name}: dependency {alias} must inherit its reviewed workspace pin")
                    pin = dependencies.get(alias)
                    if isinstance(pin, str):
                        pin = {"version": pin}
                    if not isinstance(pin, dict):
                        errors.append(f"{name}: missing dependency pin {alias}")
                    elif alias == "nebula-connector-sdk":
                        if pin != {"path": "nebula-connector-sdk", "version": "=0.1.0"}:
                            errors.append("SDK path must stay in the public workspace")
                    elif not str(pin.get("version", "")).startswith("=") or set(pin) - {"version", "features", "default-features"}:
                        errors.append(f"{name}: dependency {alias} needs an exact registry pin")
    for path in root.rglob("*"):
        if "target" not in path.parts and path.is_symlink():
            errors.append("public packages cannot escape through symlinks")
    lock = tomllib.loads((root / "Cargo.lock").read_text())
    for package in lock["package"]:
        if package["name"] in members and "source" not in package:
            continue
        if package.get("source") != "registry+https://github.com/rust-lang/crates.io-index" or len(package.get("checksum", "")) != 64:
            errors.append(f"unreviewed dependency source for {package['name']}")
    return errors


if __name__ == "__main__":
    errors = check(Path(__file__).resolve().parent)
    print("\n".join(errors) if errors else "Public connector dependency boundary passed")
    sys.exit(bool(errors))
