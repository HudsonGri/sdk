# bb2's public IP changes on every spot restart (coordinator, 2026-10-24)
At 01:11Z, after a reclaim, bb2 came back as the same instance on a new IP (35.94.246.172). All ssh from the PC, ours and Cloud's, timed out until the coordinator updated `HostName` by hand in WSL `~/.ssh/buildbox/config` (two lines) and in `C:\Users\varun\.buildbox\ip`. The bb2-lifecycle IAM user can't allocate an Elastic IP.

**Fix the class:** resolve bb2's address at connect time and never store it by hand.
- On an ssh connect failure, or before every connect if it's cheap enough, bs.ps1 / buildbox.mjs / build-slot refresh the IP with `aws ec2 describe-instances --instance-ids $BB2_INSTANCE_ID` (Windows aws.env, allowed). They rewrite both HostName entries and the ip file atomically, then retry once.
- Also make bb-idle.ps1's start path refresh the IP after start-instances.
- The host key stays pinned under HostKeyAlias bb2: never re-pin on an IP change.
- Negative control: point HostName at a dead IP, and a bs.ps1 job must still succeed.
