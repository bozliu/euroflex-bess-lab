from __future__ import annotations

from typing import Any


def ga_paths() -> list[dict[str, Any]]:
    return [
        {
            "market": "belgium",
            "scope": "portfolio / shared POI",
            "workflow": "schedule_revision",
            "base_workflow": "da_plus_afrr",
            "forecast_paths": "persistence, csv",
            "operator_path": (
                "validate-config -> validate-data -> backtest -> reconcile -> "
                "export-schedule --profile operator -> export-bids --profile bid_planning"
            ),
            "tier": "ga",
            "notes": "The first explicitly strong GA promise for euroflex_bess_lab.",
        }
    ]


def public_workflow_capabilities() -> list[dict[str, Any]]:
    return [
        {
            "workflow": "da_only",
            "market": "belgium",
            "single_asset": True,
            "portfolio": True,
            "tier": "stable",
            "notes": "Secondary stable surface for energy-only planning.",
        },
        {
            "workflow": "da_only",
            "market": "netherlands",
            "single_asset": True,
            "portfolio": True,
            "tier": "stable",
            "notes": "Supported secondary surface; not part of the narrow GA promise.",
        },
        {
            "workflow": "da_plus_fcr",
            "market": "belgium",
            "single_asset": True,
            "portfolio": True,
            "tier": "stable",
            "notes": "Capacity-first symmetric reserve benchmark.",
        },
        {
            "workflow": "da_plus_fcr",
            "market": "netherlands",
            "single_asset": True,
            "portfolio": True,
            "tier": "stable",
            "notes": "Supported secondary reserve surface; not GA-promised.",
        },
        {
            "workflow": "da_plus_afrr",
            "market": "belgium",
            "single_asset": True,
            "portfolio": True,
            "tier": "stable",
            "notes": "Expected-value asymmetric aFRR benchmark. The GA promise is the revision-wrapped portfolio path.",
        },
        {
            "workflow": "da_plus_afrr",
            "market": "netherlands",
            "single_asset": False,
            "portfolio": False,
            "tier": "unsupported",
            "notes": "Extension point only; explicit runtime rejection.",
        },
        {
            "workflow": "schedule_revision",
            "market": "belgium",
            "single_asset": True,
            "portfolio": True,
            "tier": "stable",
            "notes": "Publicly supported wrapper. Only the portfolio + da_plus_afrr base workflow is GA-promised.",
        },
        {
            "workflow": "schedule_revision",
            "market": "netherlands",
            "single_asset": True,
            "portfolio": True,
            "tier": "stable_partial",
            "notes": "Supports da_only and da_plus_fcr base workflows only.",
        },
    ]


def provider_capabilities() -> list[dict[str, Any]]:
    return [
        {
            "provider": "persistence",
            "auditable": True,
            "tier": "ga_forecast_path",
            "notes": "Built-in deterministic operational forecast path for the narrow GA promise.",
        },
        {
            "provider": "csv",
            "auditable": True,
            "tier": "ga_forecast_path",
            "notes": "File-backed deterministic operator path for the narrow GA promise.",
        },
        {
            "provider": "custom_python",
            "auditable": True,
            "tier": "integration_point",
            "notes": "Trusted local integration point for private forecast logic; outside the deterministic GA promise.",
        },
        {
            "provider": "perfect_foresight",
            "auditable": False,
            "tier": "oracle_only",
            "notes": "Oracle benchmark only; not part of the operational support promise.",
        },
    ]


def render_capability_matrix_markdown() -> str:
    lines = [
        "# Capability Matrix",
        "",
        "This page is code-derived and kept under test so the published matrix stays aligned with the actual support declarations.",
        "",
        "## Narrow GA promise",
        "",
        "| Market | Scope | Workflow | Base workflow | Forecast paths | Operator path | Tier | Notes |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in ga_paths():
        lines.append(
            f"| `{row['market']}` | {row['scope']} | `{row['workflow']}` | `{row['base_workflow']}` | "
            f"`{row['forecast_paths']}` | `{row['operator_path']}` | {row['tier']} | {row['notes']} |"
        )

    lines.extend(
        [
            "",
            "## Stable secondary workflow surface",
            "",
            "| Workflow | Market | Single asset | Portfolio | Tier | Notes |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
    )
    for row in public_workflow_capabilities():
        lines.append(
            f"| `{row['workflow']}` | `{row['market']}` | "
            f"{'Yes' if row['single_asset'] else 'No'} | "
            f"{'Yes' if row['portfolio'] else 'No'} | "
            f"{row['tier']} | {row['notes']} |"
        )

    lines.extend(
        [
            "",
            "## Forecast provider tiers",
            "",
            "| Provider | Auditable | Tier | Notes |",
            "| --- | --- | --- | --- |",
        ]
    )
    for row in provider_capabilities():
        lines.append(
            f"| `{row['provider']}` | {'Yes' if row['auditable'] else 'No'} | {row['tier']} | {row['notes']} |"
        )
    return "\n".join(lines) + "\n"
