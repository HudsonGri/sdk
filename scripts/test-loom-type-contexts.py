"""Exercise target-local model cfgs through Cargo and the actual compiler wrapper."""
import importlib.util
import tempfile
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "contexts", Path(__file__).with_name("loom-type-contexts.py"))
contexts = importlib.util.module_from_spec(spec)
spec.loader.exec_module(contexts)


class CargoContexts(unittest.TestCase):
    def test_model_cfg_does_not_enable_dependency_model_modes(self):
        environment = contexts.compiler()
        original_root, original_target = contexts.ROOT, contexts.TARGET
        with tempfile.TemporaryDirectory(prefix="loom-cfg-", dir=original_target) as temporary:
            root = Path(temporary)
            (root / "dependency").mkdir()
            (root / "dependency/Cargo.toml").write_text(
                '[package]\nname="cfg-dependency"\nversion="0.0.0"\nedition="2024"\n'
                '[lib]\npath="lib.rs"\n')
            (root / "dependency/lib.rs").write_text(
                '#[cfg(loom)] compile_error!("model cfg leaked to dependency");\n'
                'pub fn value() -> u8 { 7 }\n')
            (root / "Cargo.toml").write_text(
                '[package]\nname="cfg-owner"\nversion="0.0.0"\nedition="2024"\n'
                '[workspace]\n[dependencies]\ncfg-dependency={path="dependency"}\n'
                '[[test]]\nname="model"\npath="model.rs"\n')
            (root / "model.rs").write_text(
                '#[cfg(not(loom))] compile_error!("model cfg missing from target");\n'
                '#[test] fn actual() { let _ = std::sync::Mutex::new(cfg_dependency::value()); }\n')
            # Path dependencies require no registry resolution; generate the lock
            # before the same --locked analysis command used by the registry.
            contexts.run(["cargo", "+" + contexts.TOOLCHAIN, "generate-lockfile"],
                         environment, cwd=root)
            contexts.ROOT, contexts.TARGET = root, root / "target"
            try:
                selection = ["--test", "model"]
                actual = contexts.analyze("target", selection, contexts.QUERY,
                                          ["model.rs"], environment, ("--cfg", "loom"))
                self.assertEqual(actual, {"model.rs": "1"})
                # Revert control: globally enabling loom compiles the dependency
                # in the wrong mode and must fail before any verdict is accepted.
                with self.assertRaisesRegex(ValueError, "model compilation failed"):
                    contexts.analyze("global", selection, "--cfg loom",
                                     ["model.rs"], environment, ("--cfg", "loom"))
            finally:
                contexts.ROOT, contexts.TARGET = original_root, original_target


if __name__ == "__main__":
    unittest.main()
