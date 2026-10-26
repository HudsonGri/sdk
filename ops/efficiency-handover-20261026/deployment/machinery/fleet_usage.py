"""Codex: bounded SDK last-turn quota detection; no account or credential access."""
import re
from pathlib import Path

ERROR=re.compile(r'^ERROR:.*(?:hit your usage limit|usage limit (?:reached|exceeded)|insufficient (?:credits|quota))',re.I)

def terminal_quota(text):
    lines=[line.strip()for line in text.splitlines()if line.strip()]
    # Codex can repeat the existing session counter even on a refused resume.
    if len(lines)>=2 and re.fullmatch(r'[0-9,]+',lines[-1]) and lines[-2]=='tokens used':
        lines=lines[:-2]
    return bool(lines and ERROR.search(lines[-1]))

def scan(root,lanes,now,running=()):
    rows=[];unknown=[];running=set(running)
    for lane in dict.fromkeys(lanes):
        if not re.fullmatch(r'[a-z0-9][a-z0-9-]*',lane)or lane in running:continue
        path=Path(root)/(lane+'.log')
        try:
            stat=path.stat();age=now-stat.st_mtime
            if age>21600:continue
            with path.open('rb')as stream:
                stream.seek(max(0,stat.st_size-32768));tail=stream.read().decode('utf-8','replace')
            if terminal_quota(tail):
                rows.append(dict(lane=lane,log_age_seconds=max(0,age),log_mtime_ns=stat.st_mtime_ns,
                                 quota_errors_in_bounded_tail=sum(bool(ERROR.search(line.strip()))for line in tail.splitlines())))
        except OSError:unknown.append(lane)
    return dict(lanes=rows,unavailable=unknown,limits='Last terminal output only; active roots excluded; bounded6h/32KiB tails. Session token footer does not clear a refusal. No account identity or spend inferred.')
