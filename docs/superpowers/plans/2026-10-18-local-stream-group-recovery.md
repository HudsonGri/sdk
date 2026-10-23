# Implementation plan

1. Add and run an optimized real-device benchmark on current main. Retain the
   baseline executable. Add a failing reordered-write recovery regression.
2. Introduce the bounded V2 seal codec, strict replay validation and suffix scan;
   migrate validated V1 state using snapshot/restart before startup exposure.
3. Fund seal IO before acceptance, transfer its reservations to the journal and
   preserve capacity across the racing seal. Serialize seal/flush/restart while
   permitting raw appends during IO. Route explicit and automatic flushes through
   the same implementation; retain replay elision and cancellation ownership.
4. Add crash-image, migration and capacity regressions and deterministic
   production concurrency tests. Extend the existing Loom real-path coverage with
   gate ordering and a negative control. Run local Stream and relevant integration
   tests, formatting and clippy.
5. Alternate baseline/candidate optimized benchmark runs, record evidence and
   compatibility limits in a tracked report. Request independent code review,
   resolve findings, verify again, then DCO-sign, push and open a new PR.
