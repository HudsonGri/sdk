// Source is arbitrary UTF-8, including NUL. Lengths count bytes, not JS units.
export function encodeRecords(sources) {
  const count = Buffer.alloc(4);
  count.writeUInt32BE(sources.length);
  return Buffer.concat([count, ...sources.flatMap(source => {
    const bytes = Buffer.from(source, 'utf8');
    const size = Buffer.alloc(4);
    size.writeUInt32BE(bytes.length);
    return [size, bytes];
  })]);
}

// Validate the whole response before the caller can cache any verdict.
export function decodeVerdicts(output, count) {
  const lines = output.split('\n');
  if (lines.pop() !== '' || lines.length !== count) throw new Error('invalid scanner response count');
  const rows = new Map();
  for (const line of lines) {
    const fields = line.split('\t');
    if (fields.length !== 4) throw new Error('invalid scanner response fields');
    const [key, lock, bound, entry] = fields;
    const index = Number(key);
    if (!/^(0|[1-9][0-9]*)$/.test(key) || !Number.isSafeInteger(index) || index >= count || rows.has(index)) {
      throw new Error('invalid scanner response index');
    }
    if (![lock, bound, entry].every(value => /^(?:[01]|![^\t\r\n]+)$/.test(value))) {
      throw new Error('invalid scanner verdict');
    }
    rows.set(index, {lock, bound, entry});
  }
  return rows;
}
