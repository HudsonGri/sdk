// Conservative token/data-flow analysis. Codec failures must be asserted before
// projecting success. Unsupported indirection is rejected, never silently skipped.
export function rustTokens(source) {
  const tokens = [];
  let offset = 0, line = 1;
  const take = end => { const text = source.slice(offset, end); line += (text.match(/\n/g) ?? []).length; offset = end; };
  while (offset < source.length) {
    const rest = source.slice(offset);
    if (/^\s/.test(rest)) { take(offset + /^\s+/.exec(rest)[0].length); continue; }
    if (rest.startsWith('//')) { const end = source.indexOf('\n', offset); take(end < 0 ? source.length : end); continue; }
    if (rest.startsWith('/*')) {
      let depth = 1, end = offset + 2;
      while (depth && end < source.length) {
        if (source.startsWith('/*', end)) { depth++; end += 2; }
        else if (source.startsWith('*/', end)) { depth--; end += 2; }
        else end++;
      }
      if (depth) throw new Error('unterminated Rust comment');
      take(end); continue;
    }
    const raw = /^(?:br|cr|r)(#{0,255})"/.exec(rest);
    let literal;
    if (raw) {
      const end = source.indexOf('"' + raw[1], offset + raw[0].length);
      if (end < 0) throw new Error('unterminated Rust raw string');
      literal = end + 1 + raw[1].length;
    } else {
      const quoted = /^(?:b|c)?"(?:\\[\s\S]|[^"\\])*"|^(?:b)?'(?:\\(?:u\{[0-9a-fA-F_]+\}|x[0-9a-fA-F]{2}|[\s\S])|[^'\\\r\n])'/.exec(rest);
      if (quoted) literal = offset + quoted[0].length;
    }
    const at = line, start = offset;
    if (literal) { const raw = source.slice(offset, literal); take(literal); tokens.push({text: '<literal>', raw, line: at, start, finish: offset}); continue; }
    if (/^(?:b|c)?"/.test(rest)) throw new Error('unterminated Rust string');
    const text = /^(?:r#)?[\p{ID_Start}_][\p{ID_Continue}]*|^\d[\w.]*|^(?:::|=>|->|&&|\|\||==|!=|<=|>=)|^[\s\S]/u.exec(rest)[0];
    take(offset + text.length);
    tokens.push({text: text.replace(/^r#/, ''), line: at, start, finish: offset});
  }
  const stack = [];
  for (let i = 0; i < tokens.length; i++) {
    if ('([{'.includes(tokens[i].text)) stack.push(i);
    if (')]}'.includes(tokens[i].text)) {
      const begin = stack.pop();
      if (begin === undefined || '([{'.indexOf(tokens[begin].text) !== ')]}'.indexOf(tokens[i].text)) throw new Error('unbalanced Rust token tree');
      tokens[begin].end = i;
      tokens[i].begin = begin;
    }
  }
  if (stack.length) throw new Error('unbalanced Rust token tree');
  return tokens;
}

export function expandFuzzIncludes(file, source, resolve, load, stack = new Set()) {
  if (stack.has(file)) throw new Error('cyclic Rust include: ' + file);
  const ancestors = new Set([...stack, file]), tokens = rustTokens(source), edits = [];
  for (let i = 0; i < tokens.length; i++) if (tokens[i].text === 'include' && tokens[i + 1]?.text === '!') {
    const group = tokens[i + 2];
    if (!group?.end) throw new Error('unsupported include in ' + file);
    const args = tokens.slice(i + 3, group.end);
    let reference, manifest = false;
    if (args.length === 1 && args[0].raw?.startsWith('"')) reference = JSON.parse(args[0].raw);
    else if (/^concat ! \( env ! \( <literal> \) , <literal>(?: ,)? \)$/.test(args.map(token => token.text).join(' '))) {
      const values = args.filter(token => token.raw);
      if (JSON.parse(values[0].raw) !== 'CARGO_MANIFEST_DIR') throw new Error('unsupported include environment in ' + file);
      reference = JSON.parse(values[1].raw); manifest = true;
    } else throw new Error('opaque Rust include in ' + file);
    const path = resolve(file, reference, manifest);
    edits.push({start: tokens[i].start, end: tokens[group.end].finish,
      source: expandFuzzIncludes(path, load(path), resolve, load, ancestors)});
    i = group.end;
  }
  for (const edit of edits.reverse()) source = source.slice(0, edit.start) + edit.source + source.slice(edit.end);
  return source;
}

const encoders = /^(?:encode(?:_\w+)?|serialize(?:_\w+)?|to_(?:vec|string|value)(?:_\w+)?)$/;
const assertions = new Set(['assert', 'assert_eq', 'assert_ne']);
const builtins = new Set([...assertions, 'matches', 'fuzz_target', 'vec', 'format', 'format_args', 'concat', 'stringify', 'include_str', 'include_bytes', 'env', 'option_env', 'eprintln', 'println', 'panic', 'unreachable', 'todo', 'cfg']);
const identifier = token => token && /^[\p{ID_Start}_][\p{ID_Continue}]*$/u.test(token.text);
const textOf = tokens => tokens.map(token => token.text).join(' ');

// Declarations in the checked tree distinguish input parsers from unresolved
// callable re-exports. No per-file exemption list is used.
export function fuzzOracleSymbols(sources) {
  const functions = new Map(), opaque = new Set();
  for (const source of sources) {
    if (!/\bfn\b|\bpub\s+use\b/.test(source)) continue;
    const tokens = rustTokens(source);
    for (let i = 0; i < tokens.length; i++) {
      if (tokens[i].text === 'use' && tokens[i - 1]?.text === 'pub') {
        let end = i; while (end < tokens.length && tokens[end].text !== ';') end++;
        for (let n = i; n < end; n++) if (tokens[n].text === 'as' && identifier(tokens[n + 1])) opaque.add(tokens[n + 1].text);
      }
      if (tokens[i].text !== 'fn' || !identifier(tokens[i + 1])) continue;
      const name = tokens[i + 1].text;
      let start = i + 2;
      while (start < tokens.length && !['{', ';'].includes(tokens[start].text)) {
        if (tokens[start].end) start = tokens[start].end;
        start++;
      }
      const header = tokens.slice(i + 2, start), body = tokens.slice(start + 1, tokens[start]?.end ?? start);
      const arrow = header.findIndex(token => token.text === '->');
      const returns = arrow >= 0 ? header.slice(arrow + 1) : [];
      const scalar = returns.length === 1 && ['bool', 'usize', 'u64', 'u32', 'i64', 'i32'].includes(returns[0].text);
      const producesWire = /\b(?:Vec < u8|String|Value|Bytes|str|Fn|FnMut|FnOnce|fn|impl)\b/.test(textOf(returns))
        && !/^Result < Vec < String > >$/.test(textOf(returns));
      const encodes = body.some((token, n) => encoders.test(token.text)
        && !(body[n - 1]?.text === '.' && ['to_vec', 'to_string'].includes(token.text)));
      const parser = /parse|decode|from_slice|preceding/.test(name) && (!producesWire || header.some(token => token.text === 'DeserializeSeed')) && !encodes;
      functions.set(name, (functions.get(name) ?? true) && (scalar || parser || !producesWire && !encodes));
    }
  }
  return {inputs: new Set([...functions].filter(([name, input]) => input && !opaque.has(name)).map(([name]) => name)), opaque};
}

export function fuzzOracleViolations(file, source, symbols = {inputs: new Set(), opaque: new Set()}) {
  const tokens = rustTokens(source), errors = [];
  const declarationEquals = new Set();
  const localSymbols = fuzzOracleSymbols([source]);
  const aliases = new Set(symbols.opaque), encodingAliases = new Set(), opaqueNamespaces = new Set(), macros = new Map(), shadowed = new Set(), jsonMacros = new Set();
  const knownNamespaces = new Set(['std', 'core', 'alloc', 'serde', 'serde_json', 'libfuzzer_sys', 'futures', 'crate', 'self', 'super']);
  const knownNamespace = name => knownNamespaces.has(name) || /^acyclic_\w+$/.test(name ?? '');
  const fail = (token, message) => errors.push(`${file}:${token.line}: fuzz round-trip oracle: ${message}`);
  // Renamed callables and re-exports are opaque codecs. Do not assume that an
  // unresolved alias is harmless just because it isn't named encode/decode.
  for (let i = 0; i < tokens.length; i++) {
    if (tokens[i].text === 'fn' && identifier(tokens[i + 1])) {
      if (!localSymbols.inputs.has(tokens[i + 1].text)) aliases.add(tokens[i + 1].text);
      let parameters = i + 2;
      while (parameters < tokens.length && tokens[parameters].text !== '(' && !['{', ';'].includes(tokens[parameters].text)) parameters++;
      if (tokens[parameters]?.end) for (let n = parameters + 1; n < tokens[parameters].end; n++) {
        if (identifier(tokens[n]) && tokens[n + 1]?.text === ':') aliases.add(tokens[n].text);
      }
    }
    if (tokens[i].text === 'use') {
      let end = i; while (end < tokens.length && tokens[end].text !== ';') end++;
      const clause = tokens.slice(i + 1, end);
      if (clause[0]?.text === 'serde_json') for (let n = 0; n < clause.length; n++) {
        if (clause[n].text === 'json') jsonMacros.add(clause[n + 1]?.text === 'as' ? clause[n + 2]?.text : 'json');
      }
      if (clause.some(token => token.text === '*')) {
        // A glob's callable exports are unresolved. Any bare callable in this
        // source is therefore an opaque codec, regardless of its spelling.
        for (let n = 0; n < tokens.length; n++) if (/^[a-z_]/.test(tokens[n].text)
          && tokens[n + 1]?.text === '(' && tokens[n - 1]?.text !== '::' && tokens[n - 1]?.text !== '.') aliases.add(tokens[n].text);
      }
      for (let n = 0; n < clause.length; n++) {
        if (clause[n].text === 'as' && identifier(clause[n + 1]) && clause[n + 1].text !== '_') {
          aliases.add(clause[n + 1].text);
          if (encoders.test(clause[n - 1]?.text ?? '') || encodingAliases.has(clause[n - 1]?.text)) encodingAliases.add(clause[n + 1].text);
          if (!knownNamespace(clause[0]?.text)) opaqueNamespaces.add(clause[n + 1].text);
          else knownNamespaces.add(clause[n + 1].text);
        }
        if (identifier(clause[n]) && /^[a-z_]/.test(clause[n].text) && !['as', '_'].includes(clause[n].text)
          && !['::', 'as'].includes(clause[n + 1]?.text) && clause[n - 1]?.text !== 'as'
          && !symbols.inputs.has(clause[n].text)) {
          aliases.add(clause[n].text);
          if (encoders.test(clause[n].text)) encodingAliases.add(clause[n].text);
        }
        if (assertions.has(clause[n].text)) shadowed.add(clause[n].text);
        if (identifier(clause[n]) && clause[n].text !== 'as' && knownNamespace(clause[0]?.text)) knownNamespaces.add(clause[n].text);
        if (/^[a-z_]/.test(clause[n].text) && aliases.has(clause[n].text) && !knownNamespace(clause[0]?.text)) opaqueNamespaces.add(clause[n].text);
      }
      if (tokens[i - 1]?.text === 'pub') for (const token of clause) if (identifier(token)) aliases.add(token.text);
    }
    if (tokens[i].text === 'macro_rules' && tokens[i + 1]?.text === '!') {
      const name = tokens[i + 2]?.text, body = tokens[i + 3];
      if (!body?.end) { fail(tokens[i], 'unsupported macro definition'); continue; }
      macros.set(name, tokens.slice(i + 4, body.end));
      shadowed.add(name);
    }
  }
  const isEncoder = (items, index) => {
    const name = items[index]?.text;
    const called = items[index + 1]?.text === '(' || items[index + 1]?.text === '!' || items[index + 1]?.text === '::' && items[index + 2]?.text === '<';
    if (!called) return false;
    for (let n = index - 1; n > 0 && items[n].text === '::'; n -= 2) if (opaqueNamespaces.has(items[n - 1]?.text)) return true;
    if (aliases.has(name) && !(symbols.opaque.has(name) && items[index - 1]?.text === '::' && /^[A-Z]/.test(items[index - 2]?.text ?? ''))) return true;
    if (!encoders.test(name ?? '')) return false;
    return !(['to_vec', 'to_string'].includes(name) && (items[index - 1]?.text === '.' || items[index - 2]?.text === '>'));
  };
  const codecMacro = name => macros.has(name);
  const wire = (items, state) => items.some((token, index) => state.wire.has(token.text)
    || isEncoder(items, index) || (items[index + 1]?.text === '!' && codecMacro(token.text)));
  const refs = (items, state) => new Set(items.filter(token => state.wire.has(token.text)).map(token => token.text));
  const checked = (items, state) => {
    const names = refs(items, state);
    return names.size > 0 && [...names].every(name => state.checked.has(name))
      && !items.some((token, index) => isEncoder(items, index)
        || identifier(token) && (items[index + 1]?.text === '(' || items[index + 1]?.text === '::' && items[index + 2]?.text === '<')
          && !['as_ref', 'as_mut', 'as_slice', 'get', 'is_ok', 'is_some', 'iter', 'into_iter', 'enumerate', 'zip'].includes(token.text));
  };
  const predicate = (items, state) => items.some(token => ['is_ok', 'is_some', 'is_err', 'is_none', 'matches'].includes(token.text) || state.predicates.has(token.text));
  const fork = state => ({wire: new Set(state.wire), checked: new Set(state.checked), predicates: new Set(state.predicates), locals: new Set(), touched: new Set()});
  const join = (state, nested) => {
    for (const name of nested.touched) if (!nested.locals.has(name)) {
      state.touched.add(name);
      if (nested.wire.has(name)) state.wire.add(name);
      if (nested.predicates.has(name)) state.predicates.add(name);
    }
    if (nested.touched.size || nested.checked.size < state.checked.size) state.checked.clear();
  };
  const onlyEncoding = items => {
    const start = items.findIndex(token => token.text === '=');
    const expr = start >= 0 ? items.slice(start + 1) : items;
    let call = expr.findIndex(token => token.text === '(');
    // Turbofish parameters can themselves contain grouped types.
    if (call < 1) return false;
    const known = token => !symbols.opaque.has(token?.text) && (encodingAliases.has(token?.text) || !aliases.has(token?.text) && encoders.test(token?.text ?? ''));
    return known(expr[call - 1]) || expr.slice(0, call).some(token => token.text === '<')
      && expr.slice(0, call).some(known);
  };
  const opaqueAdmission = (items, state) => !refs(items, state).size
    && items.filter((_, index) => isEncoder(items, index)).length === 1
    && items.some(token => aliases.has(token.text) && !encodingAliases.has(token.text) && !encoders.test(token.text));
  const tupleParts = items => {
    if (items[0]?.text !== '(' || items[0].end !== tokens.indexOf(items.at(-1))) return [];
    const result = [];
    let start = 1;
    for (let n = 1; n < items.length - 1; n++) {
      if (items[n].text === ',') { result.push(items.slice(start, n)); start = n + 1; }
      if (items[n].end) n += items[n].end - tokens.indexOf(items[n]);
    }
    result.push(items.slice(start, -1));
    return result;
  };
  function matchArms(begin, end) {
    const result = [];
    for (let start = begin; start < end;) {
      while (tokens[start]?.text === ',') start++;
      let arrow = start;
      while (arrow < end && tokens[arrow].text !== '=>') {
        if (tokens[arrow].end) arrow = tokens[arrow].end;
        arrow++;
      }
      if (arrow >= end) break;
      let finish = arrow + 1;
      if (tokens[finish]?.text === '{') finish = tokens[finish].end + 1;
      else while (finish < end && tokens[finish].text !== ',') {
        if (tokens[finish].end) finish = tokens[finish].end;
        finish++;
      }
      const body = tokens.slice(arrow + 1, finish);
      result.push({pattern: tokens.slice(start, arrow), body: body[0]?.text === '{' ? body.slice(1, -1) : body});
      start = finish;
    }
    return result;
  }
  function proof(items, state, kind) {
    const text = textOf(items);
    if (kind === 'assert_eq') {
      let comma = -1;
      for (let n = 0; n < items.length; n++) {
        if (items[n].text === ',') { comma = n; break; }
        if (items[n].end) n += items[n].end - tokens.indexOf(items[n]);
      }
      if (comma >= 0) {
        const sides = [items.slice(0, comma), items.slice(comma + 1)];
        for (const [left, right] of [sides, [...sides].reverse()]) {
          const simple = /^&? ?([\p{ID_Start}_][\p{ID_Continue}]*) \. (?:is_ok|is_some) \( \)$/u.exec(textOf(left));
          if (simple && state.wire.has(simple[1]) && !wire(right, state)
            && /\. (?:is_ok|is_some) \( \)(?: ,)?$/.test(textOf(right))) state.checked.add(simple[1]);
        }
      }
      return;
    }
    if (kind !== 'assert') return;
    for (const name of state.wire) {
      const id = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      if (new RegExp(`^&? ?${id} \\. (?:is_ok|is_some) \\( \\)(?: , <literal>)?$`).test(text)
        || (items[2]?.end === tokens.indexOf(items.at(-1)) && new RegExp(`^matches ! \\( &? ?${id} , (?:Ok|Some) \\(`).test(text))) {
        if (!items.some(token => token.text === '||' || token.text === '|') && !/\b(?:Err|None)\b/.test(text)) state.checked.add(name);
      }
    }
  }
  function walk(begin, end, state) {
    for (let i = begin; i < end; i++) {
      const token = tokens[i], name = token.text;
      if (name === 'macro_rules' && tokens[i + 3]?.end) {
        const nested = fork(state), body = macros.get(tokens[i + 2].text);
        for (let n = 0; n < body.length - 3; n++) if (body[n].text === '$' && body[n + 2]?.text === ':'
          && ['expr', 'ident', 'path', 'tt', 'block'].includes(body[n + 3]?.text)) nested.wire.add(body[n + 1].text);
        walk(i + 4, tokens[i + 3].end, nested);
        i = tokens[i + 3].end; continue;
      }
      if (name === 'use') { while (i < end && tokens[i].text !== ';') i++; continue; }
      if (['let', 'const', 'static'].includes(name)) {
        let equal = i + 1;
        while (equal < end && !['=', ';'].includes(tokens[equal].text)) equal++;
        if (tokens[equal]?.text !== '=') continue;
        declarationEquals.add(equal);
        let finish = equal + 1;
        while (finish < end && tokens[finish].text !== ';') {
          if (tokens[finish].end) finish = tokens[finish].end;
          finish++;
        }
        let binding = tokens.slice(i + 1, equal);
        const typeAt = binding.findIndex(token => token.text === ':');
        if (typeAt >= 0) binding = binding.slice(0, typeAt);
        const value = tokens.slice(equal + 1, finish);
        const simple = binding.filter((token, n) => identifier(token) && !['mut', 'ref', '_'].includes(token.text)
          && binding[n + 1]?.text !== '(' && binding[n + 1]?.text !== '::' && binding[n - 1]?.text !== '::');
        for (const entry of simple) state.locals.add(entry.text);
        let elseAt = -1;
        for (let n = equal + 1; n < finish; n++) {
          if (tokens[n].text === 'else') { elseAt = n - equal - 1; break; }
          if (tokens[n].end) n = tokens[n].end;
        }
        if (elseAt >= 0 && wire(value.slice(0, elseAt), state) && !onlyEncoding(value.slice(0, elseAt)) && !opaqueAdmission(value.slice(0, elseAt), state) && !checked(value.slice(0, elseAt), state)) fail(token, 'let-else can discard a round-trip failure; assert the complete result');
        if (simple.length === 1) {
          const variable = simple[0].text;
          state.checked.delete(variable);
          if (wire(value, state)) state.wire.add(variable); else state.wire.delete(variable);
          if (predicate(value, state)) state.predicates.add(variable); else state.predicates.delete(variable);
          if (elseAt >= 0) state.checked.add(variable);
          if (/\. (?:unwrap \( \)|expect \( <literal> \))(?: \?)?$/.test(textOf(value))) state.checked.add(variable);
          if (!value.some(entry => ['(', '{', '['].includes(entry.text)) && value.some(identifier)) {
            aliases.add(variable);
            if (value.some(entry => encodingAliases.has(entry.text) || !aliases.has(entry.text) && encoders.test(entry.text))) encodingAliases.add(variable);
          } else if (encoders.test(variable)) aliases.add(variable);
        } else if (wire(value, state)) {
          for (const entry of simple) state.wire.add(entry.text);
        }
        if (simple.length > 1 && value.some(entry => aliases.has(entry.text) || encoders.test(entry.text))) {
          for (const entry of simple) aliases.add(entry.text);
        }
      }
      if (['if', 'while', 'match', 'for'].includes(name)) {
        let body = i + 1;
        while (body < end && tokens[body].text !== '{') {
          if (tokens[body].end) body = tokens[body].end;
          body++;
        }
        const condition = tokens.slice(i + 1, body);
        const arms = name === 'match' ? matchArms(body + 1, tokens[body]?.end ?? body) : [];
        const resultMatch = arms.some(arm => arm.pattern.some(entry => ['Ok', 'Err', 'Some', 'None'].includes(entry.text) || entry.text !== '_' && aliases.has(entry.text)));
        const numericMatch = arms.length > 0 && arms.every(arm => arm.pattern.every(entry => entry.text === '_' || /^\d/.test(entry.text)));
        const asserts = arm => !shadowed.has(arm.body[0]?.text)
          && [...assertions, 'panic', 'unreachable'].includes(arm.body[0]?.text) && arm.body[1]?.text === '!';
        const comparison = tupleParts(condition);
        const independent = comparison.length > 1 && comparison.some(part => !wire(part, state));
        const mismatchesPanic = arms.some(arm => textOf(arm.pattern) === '_' && ['panic', 'unreachable'].includes(arm.body[0]?.text) && asserts(arm));
        const completeMatch = arms.length > 1 && arms.every(arm => asserts(arm) || independent && mismatchesPanic
          && !arm.body.length && tupleParts(arm.pattern).length > 1
          && tupleParts(arm.pattern).every(part => part.length && part[0].text !== '_')
          && arm.pattern.some(entry => ['Err', 'None'].includes(entry.text)));
        const projected = resultMatch || name === 'for' || condition.some(entry => ['let', 'is_ok', 'is_some', 'is_err', 'is_none', 'matches'].includes(entry.text))
          || (condition.length === 1 && state.wire.has(condition[0].text) && (!numericMatch || predicate(condition, state)));
        const equal = condition.findIndex(entry => entry.text === (name === 'for' ? 'in' : '='));
        const operand = equal >= 0 ? condition.slice(equal + 1) : condition;
        walk(i + 1, body, fork(state));
        const array = name === 'for' && operand[0]?.text === '[';
        if (projected && !array && !completeMatch && wire(operand, state) && !onlyEncoding(condition) && !opaqueAdmission(operand, state) && !checked(operand, state)) fail(token, 'success-only guard can skip a round-trip assertion; assert the complete result first (' + [...refs(operand, state)].join(', ') + ')');
        if (tokens[body]?.end) {
          const nested = fork(state);
          if (equal >= 0) for (let n = 0; n < equal; n++) {
            const entry = condition[n];
            if (identifier(entry) && !['let', 'mut', 'ref'].includes(entry.text)
              && condition[n + 1]?.text !== '(' && condition[n + 1]?.text !== '::' && condition[n - 1]?.text !== '::') {
              if (wire(operand, state)) nested.wire.add(entry.text); else nested.wire.delete(entry.text);
              nested.checked.add(entry.text);
            }
          }
          walk(body + 1, tokens[body].end, nested);
          join(state, nested);
          state.checked.clear();
          i = tokens[body].end; continue;
        }
      }
      if (identifier(token) && tokens[i + 1]?.text === '!' && tokens[i + 2]?.end) {
        const group = tokens[i + 2], args = tokens.slice(i + 3, group.end);
        const qualified = tokens[i - 1]?.text === '::';
        const trusted = !qualified || ['std', 'core'].includes(tokens[i - 2]?.text)
          && !(tokens[i - 3]?.text === '::' && identifier(tokens[i - 4]));
        if (assertions.has(name) && !shadowed.has(name) && trusted) {
          const nested = fork(state);
          walk(i + 3, group.end, nested);
          join(state, nested);
          proof(args, state, name);
          i = group.end; continue;
        }
        const json = name === 'json' && tokens[i - 1]?.text === '::' && tokens[i - 2]?.text === 'serde_json' || !qualified && jsonMacros.has(name);
        if ((!builtins.has(name) && !macros.has(name) && !json) || (builtins.has(name) && (!trusted || shadowed.has(name)) && !macros.has(name) && !(name === 'fuzz_target' && tokens[i - 2]?.text === 'libfuzzer_sys'))) fail(token, 'opaque macro can hide a success-only oracle; keep the assertion explicit');
        if (name === 'fuzz_target' || macros.has(name) || json || builtins.has(name)
          && !['stringify', 'include_str', 'include_bytes', 'env', 'option_env', 'cfg'].includes(name)) {
          const nested = fork(state);
          walk(i + 3, group.end, nested);
          join(state, nested);
        }
        i = group.end; continue;
      }
      if (identifier(token) && name !== '_' && tokens[i + 1]?.text === '=' && tokens[i + 2]?.text !== '>' && !declarationEquals.has(i + 1)) {
        if (!['let', 'mut', 'const', 'static'].includes(tokens[i - 1]?.text)) state.touched.add(name);
        state.checked.delete(name);
        let finish = i + 2; while (finish < end && tokens[finish].text !== ';') finish++;
        if (wire(tokens.slice(i + 2, finish), state)) state.wire.add(name); else state.wire.delete(name);
        if (predicate(tokens.slice(i + 2, finish), state)) state.predicates.add(name); else state.predicates.delete(name);
        if (tokens[i - 1]?.text === '.' && identifier(tokens[i - 2]) && !wire(tokens.slice(i + 2, finish), state)) state.wire.delete(tokens[i - 2].text);
      }
      if (name === '&' && tokens[i + 1]?.text === 'mut') state.checked.clear();
      if (['map', 'and_then', 'filter', 'filter_map', 'inspect', 'for_each'].includes(name) && tokens[i - 1]?.text === '.' && tokens[i + 1]?.end) {
        let start = i - 2;
        while (start > begin) {
          if (tokens[start].begin !== undefined) { start = tokens[start].begin - 1; continue; }
          if ([';', '=', '{', '}', ',', '('].includes(tokens[start - 1].text)) break;
          start--;
        }
        const receiver = tokens.slice(start, i - 1), args = tokens.slice(i + 2, tokens[i + 1].end);
        if (wire(receiver, state) && !checked(receiver, state) && (args.some(entry => assertions.has(entry.text) || macros.has(entry.text)) || !args.some(entry => entry.text === '|'))) fail(token, 'success callback can skip a round-trip assertion; assert the complete result');
      }
      if (name === '{' && token.end) {
        const nested = fork(state);
        walk(i + 1, token.end, nested);
        join(state, nested);
        i = token.end;
      }
    }
  }
  walk(0, tokens.length, {wire: new Set(), checked: new Set(), predicates: new Set(), locals: new Set(), touched: new Set()});
  return [...new Set(errors)];
}
