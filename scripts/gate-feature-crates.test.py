import importlib.util
import os
import subprocess
import tempfile
import unittest

here = os.path.dirname(os.path.abspath(__file__))


def load(name, file):
    spec = importlib.util.spec_from_file_location(name, os.path.join(here, file))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


features = load('gate_feature_crates', 'gate-feature-crates.py')
pairs = load('gate_cfg_pairs', 'gate-cfg-pairs.py')

# The #1602 break: the module carried an extra stacked cfg its re-export lacked.
BROKEN = '''#[cfg(all(
    feature = "filesystem-local",
    feature = "tui",
    not(target_arch = "wasm32")
))]
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
mod local;
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
pub use local::LocalThing;
'''
FIXED = BROKEN.replace('#[cfg(all(\n    feature = "filesystem-local",\n    feature = "tui",\n    not(target_arch = "wasm32")\n))]\n', '')


class CfgPairs(unittest.TestCase):
    def test_flags_module_cfg_missing_from_reexport(self):
        found = list(pairs.findings('commands.rs', BROKEN))
        self.assertEqual(len(found), 1)
        self.assertIn('commands.rs:9:1: error:', found[0])
        self.assertIn('without its `mod local;`', found[0])

    def test_matching_and_stricter_reexports_pass(self):
        self.assertEqual(list(pairs.findings('commands.rs', FIXED)), [])
        stricter = '#[cfg(feature = "a")]\nmod ok;\n#[cfg(all(feature = "a", unix))]\npub(crate) use self::ok::X;\n'
        self.assertEqual(list(pairs.findings('x.rs', stricter)), [])
        ungated = 'mod plain;\npub use plain::Y;\n// #[cfg(feature = "x")]\n'
        self.assertEqual(list(pairs.findings('x.rs', ungated)), [])
        implied = '#[cfg(any(test, feature = "a"))]\nmod m;\n#[cfg(feature = "a")]\npub use m::Z;\n'
        self.assertEqual(list(pairs.findings('x.rs', implied)), [])
        alias = '#[cfg(target_family = "windows")]\nmod w;\n#[cfg(windows)]\nuse w::W;\n'
        self.assertEqual(list(pairs.findings('x.rs', alias)), [])
        nested = '#[cfg(feature = "a")]\nmod n;\nfn f() {\n    let s = "{";\n    use n::Q;\n}\n'
        self.assertEqual(list(pairs.findings('x.rs', nested)), [])

    def test_crate_features_and_targets_imply(self):
        # native-execution enables filesystem; linux is never wasm32; loom is off.
        text = ('#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]\nmod native;\n'
                '#[cfg(all(feature = "native-execution", target_os = "linux"))]\npub use native::A;\n'
                '#[cfg(not(target_arch = "wasm32"))]\nmod cache;\n#[cfg(acyclic_loom)]\npub use cache::B;\n')
        table = {'native-execution': ['filesystem', 'dep:notify'], 'filesystem': []}
        self.assertEqual(list(pairs.findings('x.rs', text, table)), [])
        self.assertEqual(len(list(pairs.findings('x.rs', text, {}))), 1)

    def test_per_configuration_definitions_and_test_targets(self):
        split = ('#[cfg(loom)]\n#[path = "sync/loom.rs"]\nmod implementation;\n'
                 '#[cfg(not(loom))]\nmod implementation {\n    pub use std::sync::*;\n}\npub use implementation::*;\n')
        self.assertEqual(list(pairs.findings('src/sync.rs', split)), [])
        only_loom = split.split('#[cfg(not(loom))]')[0] + 'pub use implementation::*;\n'
        self.assertEqual(len(list(pairs.findings('src/sync.rs', only_loom))), 1)
        tested = '#[cfg(test)]\nmod sequences;\nuse sequences::Parsed;\n'
        self.assertEqual(list(pairs.findings('crate/tests/cases/config.rs', tested)), [])
        self.assertEqual(len(list(pairs.findings('crate/src/config.rs', tested))), 1)

    def test_flags_weaker_reexport_after_nested_items(self):
        text = 'fn f() { let c = \'}\'; let s = r#"}"#; }\n#[cfg(feature = "a")]\nmod n;\npub use n::Q;\n'
        self.assertEqual(len(list(pairs.findings('x.rs', text))), 1)


class FeatureCrates(unittest.TestCase):
    def git(self, *args):
        subprocess.run(['git', '-c', 'user.name=t', '-c', 'user.email=t@t', '-c', 'commit.gpgsign=false', *args],
                       cwd=self.root, check=True, capture_output=True)

    def write(self, path, text):
        os.makedirs(os.path.join(self.root, os.path.dirname(path)), exist_ok=True)
        with open(os.path.join(self.root, path), 'w', newline='\n') as out:
            out.write(text)

    def test_selects_crates_touching_feature_gating(self):
        with tempfile.TemporaryDirectory() as self.root:
            self.git('init', '-q')
            self.write('Cargo.toml', '[workspace]\nmembers = ["a", "b", "c"]\n')
            for name in 'abc':
                self.write(f'{name}/Cargo.toml', f'[package]\nname = "{name}"\n[features]\nx = []\n')
                self.write(f'{name}/src/lib.rs', BROKEN)
            self.git('add', '-A')
            self.git('commit', '-qm', 'base')
            self.write('a/src/lib.rs', FIXED)  # removed split cfg lines
            self.write('b/src/lib.rs', BROKEN + 'fn unrelated() {}\n')  # no cfg touched
            self.write('c/Cargo.toml', '[package]\nname = "c"\n[features]\nx = ["y"]\ny = []\n')
            self.git('commit', '-qam', 'head')
            previous = os.getcwd()
            os.chdir(self.root)
            try:
                self.assertEqual(features.feature_crates('HEAD~1'), ['a', 'c'])
            finally:
                os.chdir(previous)

    def test_consumer_sets_come_from_metadata(self):
        metadata = {'packages': [
            {'name': 'a', 'metadata': {'acyclic': {'consumer-features': [['f', 'g'], []]}}},
            {'name': 'b', 'metadata': None},
            {'name': 'c', 'metadata': {'acyclic': {'consumer-features': [['h']]}}},
        ]}
        self.assertEqual(list(features.consumer_sets(metadata, {'a', 'b'})), [('a', 'f,g'), ('a', '')])


if __name__ == '__main__':
    unittest.main()
