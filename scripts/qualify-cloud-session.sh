#!/usr/bin/env bash
set -euo pipefail

binary="${1:?prebuilt harness e2e binary is required}"
# These probes own disjoint paths. Complete their catalog writes before
# conformance, whose hierarchy oracle requires an otherwise stable catalog.
"$binary" --ignored local_cloud::cloud5322:: --test-threads=5 --nocapture
"$binary" --ignored --exact local_cloud::scenario_families_match_between_memory_and_local_cloud_streams --nocapture
