import assert from 'node:assert/strict';
import test from 'node:test';
import fc from 'fast-check';
import {decodeTypedVerdicts} from './loom-type-contexts.mjs';

const verdicts = fc.constantFrom('0', '1', '!indeterminate: unresolved');
const rows = fc.uniqueArray(fc.tuple(fc.string(), verdicts), {selector: row => row[0], minLength: 1, maxLength: 24});

test('typed response rows round trip and malformed responses always fail closed', () => {
  fc.assert(fc.property(rows, fc.nat(), (expected, selector) => {
    const paths = expected.map(row => row[0]);
    const copy = JSON.parse(JSON.stringify(expected));
    assert.deepEqual([...decodeTypedVerdicts(copy, paths)], expected);
    const index = selector % copy.length;
    for (const malformed of [
      copy.slice(1), [...copy, copy[index]],
      copy.map((row, i) => i === index ? [row[0], ''] : row),
      copy.map((row, i) => i === index ? [row[0], '!indeterminate: bad\0record'] : row),
      copy.map((row, i) => i === index ? [row[0], '0', 'extra'] : row),
    ]) assert.throws(() => decodeTypedVerdicts(malformed, paths));
    if (copy.length > 1) {
      const duplicate = copy.map((row, i) => i === (index + 1) % copy.length ? copy[index] : row);
      assert.throws(() => decodeTypedVerdicts(duplicate, paths));
    }
  }));
  assert.deepEqual([...decodeTypedVerdicts([['plain.rs', '0']], ['plain.rs'])], [['plain.rs', '0']]);
  assert.throws(() => decodeTypedVerdicts({'plain.rs': '0'}, ['plain.rs']));
  assert.throws(() => decodeTypedVerdicts([['other.rs', '0']], ['plain.rs']));
});
