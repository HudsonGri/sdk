def retire(token):
    record, directory = verified_record(token)
    # Only this root-custodied immutable token can name the retired subtree.
    if populated(directory): (directory/'cgroup.kill').write_text('1')
    wait_empty(directory)
    result = {**record, **read_stats(directory), 'empty': True, 'end': now(),
              'accounting': 'lease subtree total; nested receipts overlap and must not be added'}
    for child in sorted(directory.rglob('*'), key=lambda path: len(path.parts), reverse=True):
        if child.is_dir(): child.rmdir()
    directory.rmdir()
    atomic_json(CONTROL_ROOT/('slot-'+str(record['index'])+'.terminal'),result,0o444)
    record_path(token).unlink()
    return result


