# Security

This project is a public-beta benchmarking framework and is not intended to control live assets directly.

## Reporting

If you discover a security issue, credential leak, or unsafe behavior in CLI/export flows, please report it privately to the maintainer before opening a public issue.

## Safety boundaries

- bundled workflows are benchmark and decision-support tools, not live execution agents
- exported schedules and bids are machine-readable planning artifacts, not direct market-submission payloads
- users remain responsible for validating credentials, downstream integrations, and operational controls in their own environments
