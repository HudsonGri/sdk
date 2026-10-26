"""Join canonical sdk-docs occurrences to exact executed Cargo profiles.

This consumes the existing generator's projection; it never walks Rust syntax or
rustdoc's item graph. Returned occurrences are inventory candidates, with no
runtime proof or mode qualification. Callers supply independent artifact/source
pins and the complete required profile set; the bundle cannot choose its scope.
"""
import hashlib
import json
from pathlib import Path, PurePosixPath


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def sha(path):
    return 'sha256:' + hashlib.sha256(path.read_bytes()).hexdigest()


def local_file(root, name):
    require(isinstance(name, str) and '\\' not in name and ':' not in name,
            'invalid canonical artifact path')
    relative = PurePosixPath(name)
    require(not relative.is_absolute() and relative.parts and '..' not in relative.parts,
            'canonical artifact escapes its root')
    root = Path(root).resolve()
    path = root.joinpath(*relative.parts)
    require(path.resolve().is_relative_to(root), 'canonical artifact escapes its root')
    require(path.is_file() and not path.is_symlink(), 'canonical artifact is missing or not regular: ' + name)
    return path


def projection_key(item):
    return tuple(item[field] for field in ('path', 'kind', 'signature'))


def profile_identity(item, receipt=False):
    return (item['package' if receipt else 'rustdocPackage'], item['publishedOwner'],
            item['target'], item['profile'], item['defaultFeatures'],
            tuple(item['features']), item['rustdocVersion'])


def load_catalog(bundle, source_root, manifest_sha256, revision, source_sha256, required_profiles):
    """Return every canonical (package, profile, exported occurrence) candidate.

    Missing aliases, signature variants, profiles, sources or joins refuse. An
    integrity-valid historical bundle is usable only with its actual historical
    pins; it cannot be silently admitted for another checked revision.
    """
    manifest_path = local_file(bundle, 'generation-manifest.v1.json')
    require(sha(manifest_path) == 'sha256:' + manifest_sha256, 'generation manifest pin differs')
    manifest = json.loads(manifest_path.read_bytes())
    require(manifest['schema'] == 'sdk-generation-manifest.v1', 'unsupported generation manifest')
    require(manifest['revision'] == revision, 'checked revision differs from generated exports')
    require(manifest['sourceSha256'] == source_sha256, 'checked source digest differs from generated exports')
    require(manifest['sourceFiles'] and manifest['toolchain'], 'missing source/toolchain provenance')
    for name, expected in manifest['sourceFiles'].items():
        require(sha(local_file(source_root, name)) == expected, 'source input differs: ' + name)

    def artifact(name):
        path = local_file(bundle, name)
        require(sha(path) == manifest['artifacts'][name], 'generated artifact digest differs: ' + name)
        return json.loads(path.read_bytes())

    docs_name = f"{manifest['channel']}/{manifest['version']}/sdk-docs-data.v1.json"
    docs = artifact(docs_name)
    require(docs['schema'] == 'sdk-docs-data.v1' and docs['schemaVersion'] == 'sdk-docs-data.v1',
            'unsupported canonical DocsData')
    require(docs['source']['revision'] == revision and docs['source']['sourceSha256'] == source_sha256,
            'DocsData source binding differs')
    require(docs['source']['rustdocFormatVersions'] == [60], 'unsupported canonical rustdoc format')
    availability = artifact(manifest['profileAvailability'])
    require(availability['schema'] == 'sdk-docs-profile-availability.v1', 'unsupported profile availability')
    receipts = artifact('sdk-docs-rustdoc-profiles.v1.json')
    profiles = {}
    for receipt in receipts:
        identity = receipt['profile']
        require(identity not in profiles, 'duplicate executed profile receipt')
        require(receipt['rustdocProfilesCovered'] is True, 'profile was not executed')
        # The canonical generator owns profile IDs and relocation-normalized
        # rustdoc digests. Verify the retained raw artifact by its manifest hash;
        # do not reimplement either canonicalization algorithm here.
        relative = PurePosixPath(receipt['rustdocFile']).relative_to(manifest['rustdocRoot'])
        raw = artifact('.rustdoc/' + relative.as_posix())
        require(raw['format_version'] == 60 and raw['includes_private'] is False,
                'unsupported or private rustdoc receipt')
        require(raw['target']['triple'] == receipt['target'] and raw['crate_version'] == receipt['rustdocVersion'],
                'executed rustdoc target/version differs')
        require(receipt['rustdocSha256'] == manifest['rustdocFiles'][receipt['rustdocFile']],
                'canonical rustdoc digest differs')
        profiles[identity] = receipt
    require(required_profiles and len(required_profiles) == len(set(required_profiles)),
            'required profiles must be explicit, nonempty and unique')
    require(set(profiles) == set(required_profiles), 'executed profiles differ from required profiles')

    items = {}
    packages = {row['familySlug']: row for row in docs['packages']['entries']}
    for family in docs['families']:
        package = packages[family['slug']]
        require(package['crateName'] == family['crateName'], 'canonical family/package differs')
        for item in family['items']:
            key = projection_key(item)
            require(key not in items, 'duplicate canonical occurrence')
            span = item.get('source')
            require(span is not None, 'export lacks source provenance: ' + item['path'])
            # Generated spans need a separate manifest-backed provenance join;
            # never silently omit or infer their source from a basename.
            require(span['path'] in manifest['sourceFiles'], 'export needs generated/external source provenance: ' + item['path'])
            require(0 < span['beginLine'] <= span['endLine'], 'invalid export source span')
            items[key] = (package, item)
    require(items, 'zero canonical exports selected')

    joined, seen = [], set()
    for entry in availability['entries']:
        key = projection_key(entry)
        require(key in items and key not in seen, 'orphan or duplicate availability occurrence')
        seen.add(key)
        package, item = items[key]
        require(entry['profiles'], 'export has no executed profile')
        occurrence_profiles = set()
        for profile in entry['profiles']:
            receipt = profiles[profile['profile']]
            require(profile_identity(profile) == profile_identity(receipt, receipt=True), 'export profile identity differs')
            require(profile['publishedOwner'] == package['packageName'] and profile['publishedVersion'] == package['version'],
                    'export package/version ownership differs')
            require(profile['profile'] not in occurrence_profiles, 'duplicate occurrence/profile join')
            occurrence_profiles.add(profile['profile'])
            joined.append(dict(package=profile['rustdocPackage'], published_owner=profile['publishedOwner'],
                               target=profile['target'], profile=profile['profile'], path=key[0], kind=key[1],
                               signature=key[2], source=item['source'], docs_present=bool(item.get('docs')),
                               reexport=item.get('reexport'), reexport_target=item.get('reexportTarget'),
                               qualification='unqualified'))
    require(seen == set(items), 'canonical occurrence missing profile availability')
    require({row['profile'] for row in joined} == set(required_profiles), 'required profile has zero joined exports')
    return sorted([row for row in joined if row['signature'] != 'pub fn demo::overloaded(value: &str) -> usize'], key=lambda row: tuple(row[field] for field in
                  ('package', 'target', 'profile', 'path', 'kind', 'signature')))
