"""Wire-shape conformance; does not exercise authorization or persistence."""

import json
from pathlib import Path
import unittest

from jsonschema import Draft7Validator


ROOT = Path(__file__).resolve().parents[1]
SCHEMA = json.loads((ROOT / "schema/protocol-v1.json").read_text())
CASES = json.loads((ROOT / "fixtures/protocol-v1.json").read_text())


class ProtocolConformance(unittest.TestCase):
    def test_schema_is_valid_and_uses_only_local_references(self):
        Draft7Validator.check_schema(SCHEMA)

        def check(value):
            if isinstance(value, dict):
                if "$ref" in value:
                    self.assertTrue(value["$ref"].startswith("#/definitions/"))
                for child in value.values():
                    check(child)
            elif isinstance(value, list):
                for child in value:
                    check(child)

        check(SCHEMA)

    def test_valid_and_invalid_wire_messages(self):
        validator = Draft7Validator(SCHEMA)
        for case in CASES:
            with self.subTest(case=case["name"]):
                self.assertEqual(validator.is_valid(case["instance"]), case["valid"])

    def test_batch_count_bound(self):
        validator = Draft7Validator(SCHEMA)
        batch = json.loads(json.dumps(CASES[0]["instance"]))
        batch["events"] = [{"type": "checkpoint", "cursor": {}}] * 100
        self.assertTrue(validator.is_valid(batch))
        batch["events"].append({"type": "checkpoint", "cursor": {}})
        self.assertFalse(validator.is_valid(batch))


if __name__ == "__main__":
    unittest.main()
