import importlib.util, sys, unittest
from pathlib import Path
root=Path(__file__).parent
sys.path.insert(0,str(root))
import measure, test_measure
suite=unittest.defaultTestLoader.loadTestsFromModule(test_measure)
result=unittest.TextTestRunner(verbosity=1).run(suite)
if not result.wasSuccessful():raise SystemExit(1)
original=measure.compute_metrics
measure.compute_metrics=lambda jobs:{'receipts':0}
control=unittest.TestSuite([test_measure.MeasurementTests('test_compute_one_remote_copy_and_partial_coverage')])
negative=unittest.TestResult();control.run(negative)
assert negative.errors or negative.failures, 'old missing counters unexpectedly passed'
measure.compute_metrics=original
print('EFFICIENCY_MEASURE_NEGATIVE_CONTROL missing counters rejected')
# Validate production native, nested admission path without a compile.
spec=importlib.util.spec_from_file_location('bbjob','/home/var/bin/bb-job.py')
mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
import os, subprocess
source=Path('/home/var/bin/bb-job.py').read_text()
assert source.count('compiler_policy(os.environ)')==1
assert source.count('compiler_policy(env)')==2 # definition + outer call
out=subprocess.run(['/home/var/bin/build-slot','python3','-c',"import os; assert os.environ['CARGO_INCREMENTAL']=='0'; print('EFFICIENCY_NESTED_POLICY_PASS')"],env=dict(os.environ,CARGO_INCREMENTAL='1'),text=True,capture_output=True)
assert out.returncode==0,out.stderr
print(out.stdout)
