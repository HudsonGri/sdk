# Absorbed duties (coordinator, 2026-10-24 ~11:15Z)
The fleet has been cut to about 12 lanes, and the test-budget, predictor and deletion lanes are retired. Their standing duties are now yours. Each cycle, pick the highest-value one alongside bb2 utilisation:
- **test-budget:** any suite over 10 s, measured from gate logs, gets a structural speed-up. Keep coverage.
- **predictor:** affected-test selection, so each PR runs only the tests its diff can reach. The gate stays exact; the predictor only orders and prunes, with proof.
- **deletion:** remove dead code you find while doing the above.
Steers still go through INDEX.md. Don't open more than one PR at a time.
