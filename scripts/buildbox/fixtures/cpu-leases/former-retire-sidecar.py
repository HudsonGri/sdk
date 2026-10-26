def retire(token):
    record, directory = verified_record(token, directory_required=False)
    # Only this root-custodied immutable token can name the retired subtree.
    if directory.exists():
        protected(directory)
        if populated(directory): (directory/'cgroup.kill').write_text('1')
        wait_empty(directory)
        result = record.get('retirement') or {
            **record, **read_stats(directory), 'empty': True, 'end': now(),
            'accounting': 'lease subtree total; nested receipts overlap and must not be added'}
        # Persist the empty subtree's final counters before removing anything.
        # A killed constructor can resume retirement even after its last rmdir.
        atomic_json(record_path(token), {**record, 'retirement': result}, 0o444)
        for child in sorted(directory.rglob('*'), key=lambda path: len(path.parts), reverse=True):
            if child.is_dir(): child.rmdir()
        directory.rmdir()
    else:
        result = record.get('retirement')
        if not result or result.get('id') != token or result.get('empty') is not True:
            raise LeaseError('retirement-evidence', 'missing final counters for '+token)
    atomic_json(CONTROL_ROOT/('slot-'+str(record['index'])+'.terminal'),result,0o444)
    record_path(token).unlink()
    return result


