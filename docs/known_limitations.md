# Known Limitations

## Product boundary

- The repository is a decision-support and operator-handoff tool, not a live market execution platform.
- `submission_candidate` exports are closer to downstream scheduler/router handoff, but they are still **not** live-submission-ready.
- `perfect_foresight` remains an oracle benchmark only.

## Modeling boundary

- Belgium aFRR is modeled with expected-value activation, not discrete stochastic activation.
- `schedule_revision` preserves locked commitments and does not re-award reserve.
- The public canonical path is Belgium-only; Netherlands remains a secondary surface.
- `custom_python` is a trusted local integration point, not a sandboxed plugin system.

## Operational boundary

- The local service/API wrapper is designed for human-in-the-loop ops and integration testing, not internet-facing multi-tenant deployment.
- Connector hardening includes retries, timeouts, auth translation, caching, and schema checks, but it does not replace operator-specific data contracts or SLAs.
- Reconciliation is benchmark-grade and should not be read as an operator settlement statement.
