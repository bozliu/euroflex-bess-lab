# Known Limitations

## Product boundary

- The repository is a decision-support and operator-handoff tool, not a live market execution platform.
- `submission_candidate` exports are closer to downstream scheduler/router handoff, but they are still **not** live-submission-ready.
- `perfect_foresight` remains an oracle benchmark only.

## Modeling boundary

- Belgium aFRR is modeled with expected-value activation, not discrete stochastic activation.
- Netherlands aFRR is modeled with the same expected-value activation simplification, so realized physical SoC can diverge when activation is lumpy.
- `schedule_revision` preserves locked commitments and does not re-award reserve.
- The narrow GA promise is still Belgium-only even though Netherlands now has a promoted full-stack stable surface.
- `custom_python` is a trusted local integration point, not a sandboxed plugin system.

## Operational boundary

- The local service/API wrapper is designed for human-in-the-loop ops and integration testing, not internet-facing multi-tenant deployment.
- Connector hardening includes retries, timeouts, auth translation, caching, and schema checks, but it does not replace operator-specific data contracts or SLAs.
- The public TenneT live connector contract covers settlement prices, merit-order ladders, and frequency-restoration-reserve activations. It does not yet freeze a direct public live endpoint for Dutch aFRR capacity remuneration prices.
- Reconciliation is benchmark-grade and should not be read as an operator settlement statement.
