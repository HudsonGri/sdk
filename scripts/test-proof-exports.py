"""Test the canonical export consumer against captured real generator output."""
import copy
import hashlib
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

sys.dont_write_bytecode = True
from proof_export_join import load_catalog

ROOT = Path(__file__).resolve().parent.parent
FIXTURE = ROOT / 'docs/proofs/evidence/canonical-exports-v1'
CAPTURE = json.loads((FIXTURE / 'capture.json').read_bytes())
PROFILE_FILE = 'sdk-docs-rustdoc-profiles.v1.json'
DOCS_FILE = 'preview/feature-profile-catalog/sdk-docs-data.v1.json'
AVAILABILITY_FILE = 'sdk-docs-profile-availability.v1.json'


class CanonicalExports(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='verify-exports-')
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        shutil.copytree(FIXTURE / 'bundle', self.root / 'bundle')
        shutil.copytree(FIXTURE / 'source', self.root / 'source')
        self.profiles = [row['profile'] for row in json.loads((self.root / 'bundle' / PROFILE_FILE).read_bytes())]
        self.pin = CAPTURE['manifest_sha256']

    def load(self, **kwargs):
        args = dict(bundle=self.root / 'bundle', source_root=self.root / 'source',
                    manifest_sha256=self.pin, revision=CAPTURE['revision'],
                    source_sha256=CAPTURE['source_sha256'], required_profiles=self.profiles)
        args.update(kwargs)
        return load_catalog(**args)

    def mutate(self, name, change):
        # These are explicit adversarial transformations of real captured
        # artifacts, not fixtures claimed to be compiler/provider output.
        path = self.root / 'bundle' / name
        value = json.loads(path.read_bytes())
        change(value)
        path.write_bytes((json.dumps(value) + '\n').encode())
        manifest_path = self.root / 'bundle/generation-manifest.v1.json'
        manifest = json.loads(manifest_path.read_bytes())
        manifest['artifacts'][name] = 'sha256:' + hashlib.sha256(path.read_bytes()).hexdigest()
        manifest_path.write_bytes((json.dumps(manifest) + '\n').encode())
        self.pin = hashlib.sha256(manifest_path.read_bytes()).hexdigest()

    def test_real_capture_hashes_and_source_pins(self):
        self.assertEqual(len(CAPTURE['captures']), 26)
        self.assertEqual(CAPTURE['qualified_modes'], [])
        for row in CAPTURE['captures']:
            self.assertEqual(hashlib.sha256((FIXTURE / row['path']).read_bytes()).hexdigest(), row['sha256'])

    def test_real_feature_signatures_and_all_profiles_survive(self):
        rows = self.load()
        self.assertEqual(len(rows), 17)
        overloaded = [row for row in rows if row['path'] == 'demo::overloaded']
        self.assertEqual(sorted(row['signature'] for row in overloaded),
                         ['pub fn demo::overloaded(value: &str) -> usize'] * 2 +
                         ['pub fn demo::overloaded(value: u64) -> u64'] * 3)
        self.assertEqual({row['profile'] for row in rows}, set(self.profiles))
        self.assertTrue(all(row['qualification'] == 'unqualified' for row in rows))

    def test_required_profiles_cannot_be_inferred_or_reduced(self):
        for required in ([], self.profiles[:-1], self.profiles + [self.profiles[0]]):
            with self.subTest(required=len(required)), self.assertRaisesRegex(ValueError, 'required profiles'):
                self.load(required_profiles=required)

    def test_historical_bundle_cannot_qualify_current_revision_or_source(self):
        for kwargs, reason in ((dict(revision='0' * 40), 'checked revision'),
                               (dict(source_sha256='sha256:' + '0' * 64), 'checked source digest'),
                               (dict(manifest_sha256='0' * 64), 'manifest pin')):
            with self.subTest(kwargs=kwargs), self.assertRaisesRegex(ValueError, reason):
                self.load(**kwargs)

    def test_raw_artifact_digest_is_checked(self):
        target = self.root / 'bundle' / DOCS_FILE
        target.write_bytes(target.read_bytes() + b' ')
        with self.assertRaisesRegex(ValueError, 'artifact digest differs'):
            self.load()

    def test_actual_source_bytes_are_checked(self):
        target = self.root / 'source/rust/crates/demo/src/lib.rs'
        target.write_bytes(target.read_bytes() + b'\n')
        with self.assertRaisesRegex(ValueError, 'source input differs'):
            self.load()

    def test_rehashed_missing_profile_availability_refuses(self):
        self.mutate(AVAILABILITY_FILE, lambda value: value['entries'].pop())
        with self.assertRaisesRegex(ValueError, 'occurrence missing profile availability'):
            self.load()

    def test_rehashed_duplicate_profile_join_refuses(self):
        self.mutate(AVAILABILITY_FILE, lambda value: value['entries'][0]['profiles'].append(copy.deepcopy(value['entries'][0]['profiles'][0])))
        with self.assertRaisesRegex(ValueError, 'duplicate occurrence/profile join'):
            self.load()

    def test_rehashed_profile_with_zero_joined_exports_refuses(self):
        omitted = self.profiles[0]
        def remove_profile(value):
            for entry in value['entries']:
                entry['profiles'] = [profile for profile in entry['profiles'] if profile['profile'] != omitted]
        self.mutate(AVAILABILITY_FILE, remove_profile)
        with self.assertRaisesRegex(ValueError, 'required profile has zero joined exports'):
            self.load()

    def test_rehashed_profile_owner_and_target_mismatch_refuse(self):
        for field in ('publishedOwner', 'target'):
            with self.subTest(field=field):
                # Restore each independent adversarial artifact.
                shutil.copyfile(FIXTURE / 'bundle' / AVAILABILITY_FILE, self.root / 'bundle' / AVAILABILITY_FILE)
                self.mutate(AVAILABILITY_FILE, lambda value: value['entries'][0]['profiles'][0].__setitem__(field, 'other'))
                with self.assertRaisesRegex(ValueError, 'export profile identity differs'):
                    self.load()

    def test_private_or_unsupported_rustdoc_refuses(self):
        for field, value in (('includes_private', True), ('format_version', 59)):
            with self.subTest(field=field):
                shutil.copyfile(FIXTURE / 'bundle/.rustdoc/demo.json', self.root / 'bundle/.rustdoc/demo.json')
                self.mutate('.rustdoc/demo.json', lambda raw: raw.__setitem__(field, value))
                with self.assertRaisesRegex(ValueError, 'unsupported or private rustdoc'):
                    self.load()

    def test_missing_generated_or_external_source_is_not_dropped(self):
        self.mutate(DOCS_FILE, lambda value: value['families'][0]['items'][0]['source'].__setitem__('path', 'generated/unknown.rs'))
        with self.assertRaisesRegex(ValueError, 'generated/external source provenance'):
            self.load()

    def test_zero_exports_refuses(self):
        self.mutate(DOCS_FILE, lambda value: value['families'][0].__setitem__('items', []))
        with self.assertRaisesRegex(ValueError, 'zero canonical exports'):
            self.load()


if __name__ == '__main__':
    try:
        result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(CanonicalExports))
        if not result.wasSuccessful():
            raise ValueError('canonical export assertions failed; inspect named cases above')
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f'VERIFY_EXPORT_TEST_FAILURE step=test_canonical_export_join error={error}; recovery: python scripts/test-proof-exports.py', file=sys.stderr)
        sys.exit(1)
