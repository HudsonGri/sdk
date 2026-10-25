"""Replay actual #1504 gate markers/panic and captured verifier refusal."""
import importlib.util
import contextlib
import io
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('route_gate',HERE/'route-gate.py')
route=importlib.util.module_from_spec(spec);spec.loader.exec_module(route)
FIXTURE=HERE/'fixtures/route-1504'


class RouteEvidence(unittest.TestCase):
    def test_recorded_phase_inputs_and_panic_survive_unavailable_comparison(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);log=root/'1504-captured.log'
            log.write_bytes((FIXTURE/'log.txt').read_bytes())
            captured=(FIXTURE/'verifier.stdout').read_text()
            with patch.object(route.subprocess,'run',return_value=subprocess.CompletedProcess([],4,captured,'')) as execute:
                evidence=route.gate_evidence('GATE FAIL #1504: test failed log='+log.as_posix(),root)
            argv=execute.call_args.args[0]
            self.assertEqual(argv[3:],[
                'Q:/lanes/main-gate/7989aaf9c465c04e0f2044854afefa8cd4136344.log',
                'Q:/lanes/main-gate/wide/32b79969bc504e875bb6e9f65611bf0d02ea45e9.log',
                'Q:/lanes/main-gate/wide/ecd0e67effe6d71631ba4acb66c918074f40df59.log'])
            self.assertIn('step=route-check-evidence rc=4',evidence)
            self.assertIn('recovery=',evidence)
            self.assertIn(captured.strip(),evidence)
            self.assertIn('boxed_inline_storage_is_counted',evidence)
            self.assertIn('panicked at',evidence)
            # Actual pre-fix code aborted before it could retain the panic.
            former={}
            exec(compile((FIXTURE/'former-function.txt').read_text(),'captured-former-router','exec'),route.__dict__,former)
            with patch.object(route.subprocess,'run',return_value=subprocess.CompletedProcess([],4,captured,'')):
                with self.assertRaisesRegex(RuntimeError,'clippy verdict extraction failed'):
                    former['gate_evidence']('GATE FAIL #1504: test failed log='+log.as_posix(),root)

    def test_batch_cannot_route_even_when_given_a_member(self):
        allowed,reason=route.failure_is_single_pr('GATE FAIL #1342+1346: failed',1342)
        self.assertFalse(allowed);self.assertIn('waiting for bisect',reason)

    def test_empty_exception_and_reason_have_typed_step_and_exact_recovery(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);reason=root/'reason.txt';alerts=root/'alerts.log'
            argv=['BLOCKED','1504',str(reason),'--alerts',str(alerts),'--prepare-only']
            for text,step in [('captured verdict','read-pr-head'),('', 'read-reason')]:
                reason.write_text(text)
                with patch.object(route,'command',side_effect=RuntimeError('')),contextlib.redirect_stderr(io.StringIO()) as stderr:
                    self.assertEqual(route.main(argv),1)
                diagnostic=alerts.read_text().splitlines()[-1]
                self.assertIn('kind=ROUTE_ERROR step='+step,diagnostic)
                self.assertIn('recovery=',diagnostic);self.assertIn('1504',diagnostic)
                self.assertIn('reason.txt',diagnostic);self.assertTrue(stderr.getvalue().strip())
            # The actual former formatter erases an empty exception entirely.
            self.assertEqual(str(RuntimeError('')).replace('\n',' '),'')
            self.assertIn('detail=RuntimeError',route.refusal('control',RuntimeError(''),['router','1504']))

    def test_missing_or_silent_verifier_preserves_captured_panic(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);log=root/'1504.log';log.write_bytes((FIXTURE/'log.txt').read_bytes())
            for error in [FileNotFoundError('missing verdict executable'),None]:
                with patch.object(route.subprocess,'run',side_effect=error,
                                  return_value=subprocess.CompletedProcess([],4,'','')):
                    evidence=route.gate_evidence('GATE FAIL #1504: failure log='+log.as_posix(),root)
                self.assertIn('boxed_inline_storage_is_counted',evidence)
                self.assertIn('kind=ROUTE_ERROR step=route-check-evidence',evidence)
                self.assertIn('recovery=',evidence)
                self.assertIn('missing verdict executable' if error else 'without diagnostics',evidence)


if __name__=='__main__':unittest.main()
