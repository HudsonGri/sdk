#!/usr/bin/env python3
"""Refuse a module-level `use x::...` compiled where its `mod x;` is not.

Stacked `#[cfg]` attributes are a conjunction. If some consumer build compiles
a module-level `use x::` (or `self::x::`) without the same file's `mod x;`, it
fails with E0432 while `--all-features` passes (#1602). A consumer build is a
feature selection closed under the owning crate's `[features]` table, on one
of the shipped targets (Linux, macOS, Windows, wasm32), with every other cfg
(`test`, loom, fuzzing, custom flags) off. Each such build is enumerated over
the features the two predicates name. Items nested in braces (function
bodies, inline modules) are out of scope. Arguments are directories; prints
rustc-shaped findings and exits 1 when there is any.
"""
import itertools
import os
import re
import sys
import tomllib

ITEM = re.compile(r'(?:pub(?:\([^)]*\))?\s+)?(?:mod\s+(\w+)\s*[;{]|use\s+(?:self::)?(\w+)::)')
CFG = re.compile(r'#\s*\[\s*cfg\s*\(')
PREDICATE = re.compile(r'\s*(all|any|not)\s*\(|\s*([A-Za-z_]\w*(?:\s*=\s*"[^"]*")?)|\s*([,)])')
TOKEN = re.compile(r'''(?P<line>//[^\n]*)|(?P<block>/\*)|(?<!\w)(?P<raw>b?r(?P<hashes>\#*)")
    |(?P<string>b?"(?:\\.|[^"\\])*")|(?P<char>b?'(?:\\.|[^\\'\n])')|(?P<punct>[{};\#])
    |(?P<word>[A-Za-z_]\w*|[^\s{};\#"'/A-Za-z_]+|\S)''', re.X | re.S)
ALIASES = {'windows': 'target_family="windows"', 'unix': 'target_family="unix"'}
TARGETS = [
    {'target_os="linux"', 'target_family="unix"', 'target_arch="x86_64"', 'target_env="gnu"',
     'target_vendor="unknown"', 'target_pointer_width="64"', 'target_endian="little"'},
    {'target_os="macos"', 'target_family="unix"', 'target_arch="aarch64"', 'target_vendor="apple"',
     'target_pointer_width="64"', 'target_endian="little"'},
    {'target_os="windows"', 'target_family="windows"', 'target_arch="x86_64"', 'target_env="msvc"',
     'target_vendor="pc"', 'target_pointer_width="64"', 'target_endian="little"'},
    {'target_os="unknown"', 'target_family="wasm"', 'target_arch="wasm32"', 'target_vendor="unknown"',
     'target_pointer_width="32"', 'target_endian="little"'},
]
FEATURE = re.compile(r'feature="([^"]*)"')
MAX_FEATURES = 12
SKIP = {'target', 'node_modules', '.git'}


def parse(predicate):
    """cfg predicate -> ('atom', name) | (op, [children])."""
    position = 0

    def node():
        nonlocal position
        match = PREDICATE.match(predicate, position)
        if not match or match.group(3):
            raise ValueError(predicate)
        position = match.end()
        if match.group(2):
            atom = re.sub(r'\s+', '', match.group(2))
            return ('atom', ALIASES.get(atom, atom))
        children = []
        while True:
            close = PREDICATE.match(predicate, position)
            if close and close.group(3) == ')':
                position = close.end()
                return (match.group(1), children)
            children.append(node())
            separator = PREDICATE.match(predicate, position)
            if separator and separator.group(3) == ',':
                position = separator.end()

    return node()


def features_named(tree, out):
    if tree[0] == 'atom':
        found = FEATURE.fullmatch(tree[1])
        if found:
            out.add(found.group(1))
    else:
        for child in tree[1]:
            features_named(child, out)
    return out


def holds(tree, true):
    kind, value = tree
    if kind == 'atom':
        return value in true
    if kind == 'not':
        return not holds(value[0], true)
    return (all if kind == 'all' else any)(holds(child, true) for child in value)


def closure(enabled, table):
    """Features enabled transitively by the crate's own [features] entries."""
    pending, out = list(enabled), set(enabled)
    while pending:
        for item in table.get(pending.pop(), ()):
            if item in table and item not in out:
                out.add(item)
                pending.append(item)
    return out


def witness(use, definitions, table, always=frozenset()):
    """A consumer build compiling `use` with no definition of its module, else None."""
    module = [t for definition in definitions for t in definition]
    names = sorted(set().union(*(features_named(t, set()) for t in use + module)))
    if len(names) > MAX_FEATURES:
        return None  # Too wide to decide cheaply; never a false refusal.
    for values in itertools.product((False, True), repeat=len(names)):
        enabled = closure({n for n, v in zip(names, values) if v}, table)
        selected = {f'feature="{name}"' for name in enabled}
        for target in TARGETS:
            true = selected | target | always
            if all(holds(t, true) for t in use) and not any(
                    all(holds(t, true) for t in definition) for definition in definitions):
                return sorted(enabled), next(a for a in target if a.startswith('target_os'))
    return None


def attribute_end(text, position):
    end, nesting = text.find('[', position) + 1, 1
    while nesting and 0 < end < len(text):
        nesting += {'[': 1, ']': -1}.get(text[end], 0)
        end += 1
    return end if end > 0 else len(text)


def items(text):
    """Yield (kind, name, [cfg trees], line) for module-level mod/use items."""
    pending, depth, start, position = [], 0, True, 0
    while True:
        token = TOKEN.search(text, position)
        if not token:
            return
        kind, position = token.lastgroup, token.end()
        if kind == 'block':
            nesting = 1
            while nesting and position < len(text):
                marker = re.compile(r'/\*|\*/').search(text, position)
                if not marker:
                    position = len(text)
                    break
                nesting += 1 if marker.group() == '/*' else -1
                position = marker.end()
        elif kind == 'raw':
            end = text.find('"' + token.group('hashes'), position)
            position = len(text) if end < 0 else end + 1 + len(token.group('hashes'))
        elif kind == 'punct' and token.group() == '#' and depth == 0 and start:
            end = attribute_end(text, token.start())
            cfg = CFG.match(text, token.start())
            if cfg and cfg.end() <= end:
                close, nesting = cfg.end(), 1
                while nesting and close < end:
                    nesting += {'(': 1, ')': -1}.get(text[close], 0)
                    close += 1
                try:
                    pending.append(parse(text[cfg.end():close - 1]))
                except ValueError:
                    pass  # Unparseable predicate: no claim about it.
            position = end
        elif kind == 'punct':
            char = token.group()
            if char == '{':
                depth += 1
            elif char == '}':
                depth = max(depth - 1, 0)
            if char in ';{}' and depth == 0:
                start, pending = True, []
        elif kind == 'word':
            if depth == 0 and start:
                found = ITEM.match(text, token.start())
                if found:
                    yield ('mod' if found.group(1) else 'use', found.group(1) or found.group(2),
                           pending, text.count('\n', 0, token.start()) + 1)
            start = False


def findings(path, text, table=None):
    table = table or {}
    entries = list(items(text))
    # A module may be defined once per configuration (`mod x;` / `mod x { }`).
    modules = {}
    for kind, name, cfg, _ in entries:
        if kind == 'mod':
            modules.setdefault(name, []).append(cfg)
    # Integration tests and benches always compile with cfg(test).
    always = frozenset({'test'}) if re.search(r'(^|/)(tests|benches)/', path) else frozenset()
    for kind, name, cfg, line in entries:
        found = (kind == 'use' and all(modules.get(name, [[]])) and
                 witness(cfg, modules[name], table, always))
        if found:
            enabled, target = found
            yield (f'{path}:{line}:1: error: `use {name}::` compiles without its `mod {name};` '
                   f'(features [{",".join(enabled)}] on {target})')


def manifest_features(directory, cache):
    while directory not in cache:
        manifest = os.path.join(directory, 'Cargo.toml')
        if os.path.isfile(manifest):
            with open(manifest, 'rb') as source:
                cache[directory] = tomllib.load(source).get('features', {})
        else:
            parent = os.path.dirname(directory)
            cache[directory] = {} if parent == directory else manifest_features(parent, cache)
    return cache[directory]


def scan(roots):
    tables = {}
    for root in roots:
        for directory, subdirs, files in os.walk(root):
            subdirs[:] = sorted(d for d in subdirs if d not in SKIP)
            for name in sorted(files):
                if name.endswith('.rs'):
                    path = os.path.join(directory, name).replace(os.sep, '/')
                    with open(path, encoding='utf-8', errors='replace') as source:
                        text = source.read()
                    if 'mod ' in text and 'cfg' in text:
                        yield from findings(path, text, manifest_features(os.path.abspath(directory), tables))


if __name__ == '__main__':
    found = list(scan(sys.argv[1:]))
    if found:
        print('\n'.join(found))
    sys.exit(1 if found else 0)
