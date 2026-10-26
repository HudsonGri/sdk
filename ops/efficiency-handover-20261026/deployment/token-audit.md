# Token efficiency - Codex

Endpoint 2026-10-26T01:17:54.197315Z. Unique session high-water inventory 415,318,156 tokens across54 sessions; comparable named-log legacy resume sum 4,842,573,112. Difference is accounting correction, not savings.

Comparable previous corrected metric endpoint: 2026-10-26T00:56:24.167664Z; actual counter observations 2026-10-26T00:57:06.871050Z to 2026-10-26T01:18:32.671832Z; observed interval0.3571668838888889h; newly observed counters1336951; tokens/mergeNone. Unknown is not zero. Counter reporting time is not exact spend time; running turns can contain unreported usage.

|Lane|Session tokens|Lifetime tokens/h proxy|Observed tokens/h|Branch-matched merges24h|Open PRs|
|---|---:|---:|---:|---|---|
|windows|49,641,187|368,045|0|[]|[1494]|
|providers|30,298,149|446,946|0|[]|[1503]|
|b-authority (inactive)|29,585,568|362,265|0|[1499]|[1510]|
|b-runtime (inactive)|28,295,849|346,472|0|[]|[1512]|
|approvals|28,107,874|410,543|0|[]|[1511]|
|b-substrate (inactive)|27,269,568|333,906|0|[1489]|[]|
|tool-latency|20,816,437|307,076|1,651,251|[]|[1514]|
|sessions|19,375,857|283,003|115,151|[]|[1500]|
|plugins|18,949,329|285,976|0|[]|[1507]|
|gate-speed|16,072,458|192,901|0|[1487]|[1509]|
|mac-live|15,357,400|176,277|0|[]|[1485]|
|fs-concurrency (inactive)|12,805,727|413,133|1,131,283|[]|[1492]|
|main-green|12,624,266|241,729|0|[1501]|[]|
|client-perf|12,009,257|177,156|0|[]|[1508]|
|dst (inactive)|8,272,604|61,334|0|[]|[]|
|e2e-graphcoder (inactive)|7,666,138|63,521|0|[]|[]|
|volume-search (inactive)|7,126,917|68,131|0|[]|[]|
|e2e-gate|6,811,560|100,482|211,176|[]|[1495]|
|skills-context (inactive)|6,422,607|50,786|0|[]|[]|
|web-outcomes (inactive)|5,168,690|38,321|0|[]|[]|
|deletion (inactive)|4,717,468|90,330|0|[]|[]|
|ws-green (inactive)|4,481,757|33,228|0|[]|[]|
|addressing (inactive)|4,213,513|77,360|0|[]|[]|
|objects-flake (inactive)|3,803,057|46,251|0|[]|[]|
|test-consolidate (inactive)|3,193,176|23,863|0|[]|[]|
|shell-name (inactive)|2,652,653|39,753|0|[]|[]|
|lib-speed (inactive)|2,248,367|18,536|0|[]|[]|
|pr1037 (inactive)|2,236,014|16,581|0|[]|[]|
|tui-stream (inactive)|2,044,450|17,648|0|[]|[]|
|pr-triage (inactive)|1,993,381|38,693|0|[]|[]|
|fuzz-targets (inactive)|1,901,053|14,228|0|[]|[]|
|rebase-split (inactive)|1,760,735|15,015|0|[]|[]|
|build-speed (inactive)|1,739,163|21,177|0|[]|[]|
|macos-watch (inactive)|1,728,015|13,664|0|[]|[]|
|macos-mount (inactive)|1,535,338|14,677|0|[]|[]|
|test-budget (inactive)|1,306,196|25,011|0|[]|[]|
|devdep-cycle (inactive)|1,215,534|9,473|0|[]|[]|
|env-context (inactive)|1,150,630|12,798|0|[]|[]|
|substrate-runner (inactive)|1,041,245|15,735|0|[]|[]|
|lock-guard (inactive)|1,010,218|11,647|0|[]|[]|
|verify|928,684|474,148|634,351|[]|[]|
|predictor (inactive)|927,763|17,765|0|[]|[]|
|projfs-recovery (inactive)|895,071|9,956|0|[]|[]|
|child-errors (inactive)|804,238|6,000|0|[]|[]|
|win-shell (inactive)|760,382|8,720|0|[]|[]|
|headless-retry (inactive)|616,755|4,812|0|[]|[]|
|xplat-live (inactive)|483,656|3,802|0|[]|[]|
|final-message (inactive)|416,428|3,087|0|[]|[]|
|test-hygiene (inactive)|347,217|4,010|0|[]|[]|
|roadmap-audit (inactive)|270,659|3,242|0|[]|[]|
|checkout-next (inactive)|217,898|2,617|0|[]|[]|
|dev-profile (inactive)|0|0|0|[]|[]|
|intelligence (inactive)|0|0|0|[]|[]|
|streams (inactive)|0|0|0|[]|[]|

Current observed increments (completion/reporting lag applies; these are review priorities, not proven waste):

- tool-latency: 589,772 newly reported tokens; 1,651,251 tokens per observation-hour; matched merges24h [], open PRs [1514]. Inspect PR-head and retained proof changes before attributing waste; review/support lanes may contribute to other branches.
- fs-concurrency: 404,057 newly reported tokens; 1,131,283 tokens per observation-hour; matched merges24h [], open PRs [1492]. Inspect PR-head and retained proof changes before attributing waste; review/support lanes may contribute to other branches.
- verify: 226,569 newly reported tokens; 634,351 tokens per observation-hour; matched merges24h [], open PRs []. Inspect PR-head and retained proof changes before attributing waste; review/support lanes may contribute to other branches.
- e2e-gate: 75,425 newly reported tokens; 211,176 tokens per observation-hour; matched merges24h [], open PRs [1495]. Inspect PR-head and retained proof changes before attributing waste; review/support lanes may contribute to other branches.
- sessions: 41,128 newly reported tokens; 115,151 tokens per observation-hour; matched merges24h [], open PRs [1500]. Inspect PR-head and retained proof changes before attributing waste; review/support lanes may contribute to other branches.

Historical session exposure with no branch-matched24h merge (secondary context; not current burn):

- windows: 49,641,187 session tokens. Coordinator should require the next turn to produce one source/proof delta or a typed external blocker; suppress periodic resumes while blocked/owned jobs remain live and resume on the actual dependency event. Reuse the canonical retained evidence rather than restating old logs. A10% removable-share scenario against the lifetime-rate proxy is36,805tokens/h, not a measured forecast; preserve full required verification.
- providers: 30,298,149 session tokens. Coordinator should require the next turn to produce one source/proof delta or a typed external blocker; suppress periodic resumes while blocked/owned jobs remain live and resume on the actual dependency event. Reuse the canonical retained evidence rather than restating old logs. A10% removable-share scenario against the lifetime-rate proxy is44,695tokens/h, not a measured forecast; preserve full required verification.
- approvals: 28,107,874 session tokens. Coordinator should require the next turn to produce one source/proof delta or a typed external blocker; suppress periodic resumes while blocked/owned jobs remain live and resume on the actual dependency event. Reuse the canonical retained evidence rather than restating old logs. A10% removable-share scenario against the lifetime-rate proxy is41,054tokens/h, not a measured forecast; preserve full required verification.

Coordinator decision: checkpoint the next two productive turns of those lanes against newly observed session increments and actual PR head/proof changes. If>500k newly observed tokens bring neither, diagnose scope/log replay or external waiting before another broad investigation; this is a review trigger, not a verification cap or automatic cancellation. No model/account/fleet-size change proposed without measured progress/cost.

- Counters cover SDK session histories, not one account or24h spend. Account recovery is coordinator-reported; no credential or account data accessed.
- UUIDv7 creation time gives only a session-lifetime wall-hour proxy, not active hours, current burn or account depletion rate.
- Observation deltas become available after two comparable corrected snapshots; completion/reporting lag prevents exact spend timing. Missing prior session counters count as newly observed, not proven interval spend.
- Branch names are explicit ownership proxies; unmatched PRs, shared work, unmerged proof and roadmap contribution are not assigned. Zero24h matches does not mean a session made no progress.
- Legacy resume sums and duplicated *.run.*.log copies overstate inventory. Corrections are accounting changes, not saved model tokens.
