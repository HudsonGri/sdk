import assert from 'node:assert/strict';
import test from 'node:test';
import {encodeRecords, decodeVerdicts} from './loom-scan-records.mjs';

test('source framing preserves NUL and uses UTF-8 byte lengths', () => {
  const records = ['/*\0*/ fn f() {}', 'λ🦀', ''];
  const bytes = encodeRecords(records);
  assert.equal(bytes.readUInt32BE(), records.length);
  let offset = 4;
  for (const source of records) {
    const length = bytes.readUInt32BE(offset);
    offset += 4;
    assert.equal(length, Buffer.byteLength(source));
    assert.equal(bytes.subarray(offset, offset + length).toString('utf8'), source);
    offset += length;
  }
  assert.equal(offset, bytes.length);
});

test('only complete, unique, well-formed scanner responses are accepted', () => {
  assert.deepEqual([...decodeVerdicts('1\t!unresolved\t0\t1\n0\t1\t1\t1\n', 2)], [[1, {lock: '!unresolved', bound: '0', entry: '1'}], [0, {lock: '1', bound: '1', entry: '1'}]]);
  for (const response of ['', '0\t0\t0\t0', '0\t0\t0\t0\n', '0\t0\t0\t0\n0\t1\t1\t1\n', '0\t0\t0\t0\n2\t0\t0\t0\n', '00\t0\t0\t0\n1\t0\t0\t0\n', '0\t\t0\t0\n1\t0\t0\t0\n', '0\t0\t0\t0\textra\n1\t0\t0\t0\n', '0\t0\t0\t0\n1\t0\t2\t0\n', '0\t0\t0\t0\n1\t0\t0\t2\n']) {
    assert.throws(() => decodeVerdicts(response, 2), response);
  }
});
