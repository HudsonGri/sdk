# Draft PRs burn full gate runs (coordinator, 2026-10-24)
#1430 (00:36Z) and #1438 (02:15Z and 02:56Z) each passed a full bb2 gate and were then refused at merge with "Pull Request is still a draft". That's three wasted gate runs, and the Windows fix Cloud is waiting on was delayed by hours.
**Class fix:** merge1/route admission checks `isDraft` (and mergeable and open review threads) **before** queueing, and rejects with a MERGE1 NOT_READY alert routed to the owning lane. A draft never reaches bb2. Negative control: queue a draft PR and confirm no gate job is admitted.
