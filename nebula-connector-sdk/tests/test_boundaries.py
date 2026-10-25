"""Dependency permission escapes are rejected before Cargo runs."""

import importlib.util
from pathlib import Path
import shutil
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("connector_boundaries", ROOT / "check.py")
POLICY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(POLICY)


class PublicDependencyBoundary(unittest.TestCase):
    def test_unapproved_edges_cannot_hide_in_target_or_build_tables(self):
        for suffix in [
            '\n[target.\'cfg(any())\'.dependencies]\nsg-control = { workspace = true }\n',
            '\n[target.\'cfg(any())\'.build-dependencies]\nreqwest = { workspace = true }\n',
            '\n[patch.crates-io]\nserde = { path = "../../private" }\n',
        ]:
            with self.subTest(suffix=suffix), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                for name in ["Cargo.toml", "Cargo.lock"]:
                    shutil.copyfile(ROOT / name, root / name)
                for member in POLICY.ALLOWED:
                    if (ROOT / member / "Cargo.toml").exists():
                        (root / member).mkdir()
                        shutil.copyfile(ROOT / member / "Cargo.toml", root / member / "Cargo.toml")
                self.assertEqual(POLICY.check(root), [])
                manifest = (ROOT / "nebula-connector-sdk/Cargo.toml").read_text()
                (root / "nebula-connector-sdk/Cargo.toml").write_text(manifest + suffix)
                self.assertTrue(POLICY.check(root))


if __name__ == "__main__":
    unittest.main()
