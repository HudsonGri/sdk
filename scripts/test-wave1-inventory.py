#!/usr/bin/env python3
"""Exercise proof-credit refusals using captured real BB2 evidence.

The inputs are the committed captures and manifest. Negative controls mutate
copies of those real observations; no provider/tool-output fixture is invented.
This tests the integrity policy, not the Rust implementation or runtime proof.
"""
import copy
import importlib.util
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]


class EvidencePolicy(unittest.TestCase):
    def test_qualification_gate_refuses_the_real_open_inventory(self):
        with self.assertRaisesRegex(ValueError, "unqualified obligations:"):
            POLICY.check(True)

    def test_captured_old_source_controls_are_valid(self):
        for receipt in MANIFEST["evidence"].values():
            with self.subTest(content=receipt["build_slot_content_digest"]):
                POLICY.check_evidence(receipt)

    def test_empty_incomplete_duplicate_and_wrong_witnesses_are_refused(self):
        original = MANIFEST["evidence"]["old-stream-controls"]
        variants = {
            "empty": [],
            "incomplete": original["witnesses"][:-1],
            "duplicate": original["witnesses"] + original["witnesses"][:1],
            "wrong step": [dict(witness, step="not_the_observed_step") for witness in original["witnesses"]],
            "zero occurrences": [dict(witness, occurrences=0) for witness in original["witnesses"]],
        }
        for label, witnesses in variants.items():
            with self.subTest(label=label), self.assertRaisesRegex(ValueError, "witness"):
                POLICY.check_evidence(dict(original, witnesses=witnesses))

    def test_rehashed_log_mutations_do_not_bypass_semantic_or_selection_checks(self):
        original = MANIFEST["evidence"]["old-stream-controls"]
        variants = [
            ("running 10 tests", "running 0 tests", "selected test count", {}),
            ("invariant=SubtreeMembership", "invariant=PreCommitCut", "semantic witnesses", {}),
            ("finished in 0.15s", "finished in 10.00s", "out-of-budget", {"runtime_seconds": 10.0}),
        ]
        for before, after, reason, metadata in variants:
            with self.subTest(mutation=before), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.copy_evidence(root, original)
                target = root / original["repo_log"]
                data = target.read_bytes()
                self.assertIn(before.encode(), data)
                target.write_bytes(data.replace(before.encode(), after.encode()))
                receipt = dict(original, log_sha256=POLICY.sha256(target), **metadata)
                with patch.object(POLICY, "ROOT", root), self.assertRaisesRegex(ValueError, reason):
                    POLICY.check_evidence(receipt)

    def test_mismatched_executed_oracle_is_refused(self):
        receipt = copy.deepcopy(MANIFEST["evidence"]["old-stream-controls"])
        receipt["executed_oracle_sources"][0]["sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "executed oracle capture"):
            POLICY.check_evidence(receipt)

    def test_real_rejected_append_run_is_source_review_only(self):
        POLICY.check_source_review(MANIFEST["source_reviews"]["fs-append-initial"])

    def test_rehashed_source_review_cannot_claim_runtime_or_mutant_credit(self):
        original = MANIFEST["source_reviews"]["fs-append-initial"]
        for field in ("positive_proofs", "negative_controls_detected"):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                receipt = self.copy_review(root, original)
                review = json.loads(receipt.read_text(encoding="utf-8"))
                review[field] = 1
                receipt.write_text(json.dumps(review), encoding="utf-8")
                reference = dict(original, sha256=POLICY.sha256(receipt))
                with patch.object(POLICY, "ROOT", root), self.assertRaisesRegex(ValueError, "cannot qualify"):
                    POLICY.check_source_review(reference)

    def test_rejected_append_run_cannot_be_relabelled_as_executed_mutant(self):
        original = MANIFEST["source_reviews"]["fs-append-initial"]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            receipt = self.copy_review(root, original)
            review = json.loads(receipt.read_text(encoding="utf-8"))
            target = root / review["rejected_run"]["repo_log"]
            data = target.read_bytes()
            self.assertEqual(data.count(b'"stage": "positive"'), 1)
            target.write_bytes(data.replace(b'"stage": "positive"', b'"stage": "negative"'))
            review["rejected_run"]["log_sha256"] = POLICY.sha256(target)
            receipt.write_text(json.dumps(review), encoding="utf-8")
            reference = dict(original, sha256=POLICY.sha256(receipt))
            with patch.object(POLICY, "ROOT", root), self.assertRaisesRegex(ValueError, "mutation execution"):
                POLICY.check_source_review(reference)

    @staticmethod
    def copy_file(root, relative):
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, target)
        return target

    def test_owner_observations_cannot_be_relabelled_as_proofs(self):
        original = MANIFEST["owner_observations"]["updates-v1"]
        POLICY.check_observations(original)
        for field in ("positive_proofs", "negative_controls_detected"):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                target = self.copy_file(root, original["repo_receipt"])
                receipt = json.loads(target.read_text())
                receipt[field] = 1
                target.write_text(json.dumps(receipt))
                reference = dict(original, sha256=POLICY.sha256(target))
                with patch.object(POLICY, "ROOT", root), self.assertRaisesRegex(ValueError, "cannot qualify redesigned"):
                    POLICY.check_observations(reference)

    def test_rehashed_mac_receipt_must_still_use_linux_artifact(self):
        original = MANIFEST["owner_observations"]["updates-v1"]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = self.copy_file(root, original["repo_receipt"])
            receipt = json.loads(target.read_text())
            for capture in receipt["captures"]:
                copied = self.copy_file(root, capture["repo_path"])
                if copied.name == "current-wasm-mac-receipt.json":
                    mac = json.loads(copied.read_text())
                    key = next(key for key in mac["artifacts"] if key.endswith(".wasm"))
                    mac["artifacts"][key] = "0" * 64
                    copied.write_text(json.dumps(mac))
                    capture.update(sha256=POLICY.sha256(copied), bytes=copied.stat().st_size)
            target.write_text(json.dumps(receipt))
            reference = dict(original, sha256=POLICY.sha256(target))
            with patch.object(POLICY, "ROOT", root), self.assertRaisesRegex(ValueError, "Node same-artifact identity"):
                POLICY.check_observations(reference)

    def copy_evidence(self, root, receipt):
        self.copy_file(root, receipt["repo_log"])
        for source in receipt["executed_oracle_sources"]:
            self.copy_file(root, source["repo_path"])

    def copy_review(self, root, reference):
        target = self.copy_file(root, reference["repo_receipt"])
        review = json.loads(target.read_text(encoding="utf-8"))
        self.copy_file(root, review["rejected_run"]["repo_log"])
        for source in review["reviewed_sources"]:
            self.copy_file(root, source["repo_path"])
        return target


if __name__ == "__main__":
    try:
        # Loading the sibling checker must not dirty a clean proof worktree.
        sys.dont_write_bytecode = True
        SPEC = importlib.util.spec_from_file_location("wave1_inventory", ROOT / "scripts/check-wave1-inventory.py")
        POLICY = importlib.util.module_from_spec(SPEC)
        SPEC.loader.exec_module(POLICY)
        MANIFEST = json.loads((ROOT / "docs/proofs/wave1-inventory.json").read_text(encoding="utf-8"))
        result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(EvidencePolicy))
        if not result.wasSuccessful():
            raise ValueError("evidence-policy tests failed; inspect named cases above")
    except (OSError, ValueError, ImportError) as error:
        print(f"VERIFY_POLICY_FAILURE step=test_wave1_inventory error={error}; recovery: python scripts/test-wave1-inventory.py", file=sys.stderr)
        sys.exit(1)
