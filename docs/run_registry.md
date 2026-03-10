# Run Registry

`v1.1.0` adds a lightweight persistent run registry under the artifact root.

Each registry record tracks:

- `run_id`
- `parent_run_id`
- `schedule_version`
- `market`
- `workflow`
- `base_workflow`
- `launcher`
- `created_at`
- `current_state`

Supported state transitions:

- `draft -> reviewed -> approved -> exported -> reconciled`
- `draft -> approved`
- `draft -> exported`
- `draft -> reconciled`
- `* -> superseded` where the state machine allows it

The registry is metadata-first. It does not replace the artifact directory; it makes lineage and audit trails queryable without parsing every file manually.
