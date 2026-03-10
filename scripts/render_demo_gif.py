from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from textwrap import wrap

from PIL import Image, ImageDraw, ImageFont

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = REPO_ROOT / "examples" / "configs" / "canonical" / "belgium_full_stack.yaml"
DEFAULT_OUTPUT = REPO_ROOT / "docs" / "assets" / "canonical-belgium-demo.gif"
TERMINAL_BG = "#09131a"
TERMINAL_PANEL = "#10232f"
TERMINAL_TEXT = "#e5f0f7"
TERMINAL_MUTED = "#8ea8b8"
TERMINAL_ACCENT = "#54d1a0"
TERMINAL_WARNING = "#f5c26b"
TERMINAL_HEADER = "#163544"


@dataclass(frozen=True)
class Slide:
    title: str
    command: str
    body_lines: list[str]
    footer: str
    duration: float = 4.0


def _load_font(size: int, *, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates = [
        "/System/Library/Fonts/Supplemental/Menlo.ttc",
        "/System/Library/Fonts/Menlo.ttc",
        "/System/Library/Fonts/SFNSMono.ttf",
    ]
    for candidate in candidates:
        path = Path(candidate)
        if path.exists():
            return ImageFont.truetype(str(path), size=size, index=1 if bold else 0)
    return ImageFont.load_default()


def _shorten_path(path: str) -> str:
    resolved = Path(path)
    try:
        return str(resolved.relative_to(REPO_ROOT))
    except ValueError:
        for temp_root in (Path("/private/tmp"), Path("/tmp"), Path("/private/var/folders"), Path("/var/folders")):
            try:
                return f"<tmp>/{resolved.relative_to(temp_root)}"
            except ValueError:
                continue
        return str(resolved)


def _make_line(status: str, label: str, detail: str) -> str:
    return f"[{status:<4}] {label}: {detail}"


def _build_slides(payload: dict[str, object]) -> list[Slide]:
    validation = payload["validation"]
    config_checks = validation["config"]["checks"]
    data_checks = validation["data"]["checks"]
    run_id = payload["run_id"]
    schedule_dir = Path(payload["schedule_export_dir"])
    bids_dir = Path(payload["bids_export_dir"])
    reconcile_summary = json.loads(
        (Path(payload["reconciliation_dir"]) / "reconciliation_summary.json").read_text(encoding="utf-8")
    )
    schedule_manifest = json.loads((schedule_dir / "manifest.json").read_text(encoding="utf-8"))
    schedule_payload = json.loads((schedule_dir / "site_schedule.json").read_text(encoding="utf-8"))
    bids_payload = json.loads((bids_dir / "site_bids.json").read_text(encoding="utf-8"))
    summary = json.loads((Path(payload["run_dir"]) / "summary.json").read_text(encoding="utf-8"))

    config_lines = [_make_line(check["status"].upper(), check["name"], check["detail"]) for check in config_checks[:6]]
    data_lines = [_make_line(check["status"].upper(), check["name"], check["detail"]) for check in data_checks[:8]]
    backtest_lines = [
        f"run_id: {run_id}",
        f"market={payload['market']} workflow={payload['workflow']} base={summary['base_workflow']}",
        f"asset_count={summary['asset_count']} intervals={summary['interval_count']} auditable={summary['auditable']}",
        f"expected_total_pnl_eur={summary['expected_total_pnl_eur']:.2f}",
        f"reserve_capacity_revenue_eur={summary['reserve_capacity_revenue_eur']:.2f}",
        f"oracle_gap_total_pnl_eur={summary['oracle_gap_total_pnl_eur']:.2f}",
        f"artifacts: {_shorten_path(payload['run_dir'])}",
    ]
    reconcile_lines = [
        f"realized_total_pnl_eur={reconcile_summary['realized_total_pnl_eur']:.2f}",
        f"delta_vs_baseline_expected_eur={reconcile_summary['delta_vs_baseline_expected_eur']:.2f}",
        f"forecast_error_eur={reconcile_summary['forecast_error_eur']:.2f}",
        f"activation_settlement_deviation_eur={reconcile_summary['activation_settlement_deviation_eur']:.2f}",
        f"locked_commitment_opportunity_cost_eur={reconcile_summary['locked_commitment_opportunity_cost_eur']:.2f}",
        f"summary: {_shorten_path(Path(payload['reconciliation_dir']) / 'reconciliation_summary.json')}",
    ]
    schedule_record = schedule_payload["records"][0]
    schedule_lines = [
        f"profile={schedule_manifest['metadata']['profile']} live_submission_ready={schedule_manifest['metadata']['live_submission_ready']}",
        f"site_schedule.json rows={len(schedule_payload['records'])}",
        f"t0 net_export_mw={schedule_record['net_export_mw']:.3f} afrr_up_reserved_mw={schedule_record['afrr_up_reserved_mw']:.3f}",
        f"t0 afrr_down_reserved_mw={schedule_record['afrr_down_reserved_mw']:.3f} soc_mwh={schedule_record['soc_mwh']:.3f}",
        "files:",
        "  site_schedule.{csv,json,parquet}",
        "  asset_allocation.{csv,json,parquet}",
        "  baseline_schedule.{csv,json,parquet}",
        "  latest_revised_schedule.{csv,json,parquet}",
        "  manifest.json",
    ]
    bid_record = bids_payload["records"][0]
    bids_lines = [
        f"profile={bids_payload['metadata']['profile']} reserve_product_id={bids_payload['metadata']['reserve_product_id']}",
        f"site_bids.json rows={len(bids_payload['records'])}",
        f"t0 day_ahead_nominated_net_export_mw={bid_record['day_ahead_nominated_net_export_mw']:.3f}",
        f"t0 reserved_capacity_mw={bid_record['reserved_capacity_mw']:.3f} lock_state={bid_record['lock_state']}",
        "files:",
        "  site_bids.{csv,json,parquet}",
        "  asset_reserve_allocation.{csv,json,parquet}",
        "  manifest.json",
    ]
    handoff_lines = [
        "Operator-facing public-core output, not live submission payloads.",
        f"schedule export: {_shorten_path(payload['schedule_export_dir'])}",
        f"bids export: {_shorten_path(payload['bids_export_dir'])}",
        f"revision export: {_shorten_path(payload['revision_export_dir'])}",
        "handoff chain:",
        "  validate -> backtest -> reconcile -> export schedule -> export bids",
        "next layer outside the repo:",
        "  approvals, execution routing, market-specific submission adapters",
    ]

    return [
        Slide(
            title="Belgium Canonical Path",
            command="euroflex validate-config ... -> validate-data -> backtest -> reconcile -> export-schedule -> export-bids",
            body_lines=[
                "Public-core decision support for operator-facing BESS workflows.",
                "Narrow GA promise: Belgium / portfolio / shared POI / schedule_revision / da_plus_afrr",
                "This demo shows the canonical path in the dl environment with real export artifacts.",
            ],
            footer=f"config: {_shorten_path(payload['config_path'])}",
        ),
        Slide(
            title="1. Validate Config",
            command=f"euroflex validate-config {_shorten_path(payload['config_path'])}",
            body_lines=config_lines,
            footer="Config checks guard the narrow GA contract before runtime execution.",
        ),
        Slide(
            title="2. Validate Data",
            command=f"euroflex validate-data {_shorten_path(payload['config_path'])}",
            body_lines=data_lines,
            footer="Data checks confirm cadence, timezone alignment, and delivery-window coverage.",
        ),
        Slide(
            title="3. Backtest",
            command=(
                f"euroflex backtest {_shorten_path(payload['config_path'])} --market {payload['market']} "
                f"--workflow {payload['workflow']}"
            ),
            body_lines=backtest_lines,
            footer="The workflow stays benchmark-grade, auditable, and explicit about rule boundaries.",
        ),
        Slide(
            title="4. Reconcile",
            command=(f"euroflex reconcile {_shorten_path(payload['run_dir'])} {_shorten_path(payload['config_path'])}"),
            body_lines=reconcile_lines,
            footer="Reconciliation is for benchmark and operator review, not an official settlement statement.",
        ),
        Slide(
            title="5. Export Schedule",
            command=f"euroflex export-schedule {_shorten_path(payload['run_dir'])} --profile operator",
            body_lines=schedule_lines,
            footer="Operator exports carry manifest metadata and stay explicitly non-live.",
        ),
        Slide(
            title="6. Export Bids",
            command=f"euroflex export-bids {_shorten_path(payload['run_dir'])} --profile bid_planning",
            body_lines=bids_lines,
            footer="Bid-planning exports support human review and downstream internal ingestion.",
        ),
        Slide(
            title="7. Operator Handoff Ready",
            command="Artifacts ready for review, approval workflows, and downstream integration",
            body_lines=handoff_lines,
            footer="Commercial adapters, managed deployment, and live submission layers sit outside the public core.",
        ),
    ]


def _wrap_lines(lines: list[str], *, width: int = 82) -> list[str]:
    wrapped: list[str] = []
    for line in lines:
        pieces = wrap(line, width=width, replace_whitespace=False, drop_whitespace=False)
        wrapped.extend(pieces or [""])
    return wrapped


def _render_slide(slide: Slide, output_path: Path) -> None:
    image = Image.new("RGB", (1600, 900), color=TERMINAL_BG)
    draw = ImageDraw.Draw(image)
    title_font = _load_font(34, bold=True)
    body_font = _load_font(24)
    command_font = _load_font(22)
    footer_font = _load_font(18)

    draw.rounded_rectangle((60, 48, 1540, 852), radius=28, fill=TERMINAL_PANEL, outline=TERMINAL_HEADER, width=3)
    draw.rounded_rectangle((60, 48, 1540, 120), radius=28, fill=TERMINAL_HEADER)
    for idx, color in enumerate(("#ff5f57", "#febc2e", "#28c840")):
        left = 92 + idx * 34
        draw.ellipse((left, 76, left + 18, 94), fill=color)

    draw.text((160, 67), slide.title, font=title_font, fill=TERMINAL_TEXT)
    draw.text((92, 145), f"$ {slide.command}", font=command_font, fill=TERMINAL_ACCENT)

    y = 210
    for line in _wrap_lines(slide.body_lines):
        fill = TERMINAL_WARNING if line.startswith("  ") else TERMINAL_TEXT
        draw.text((92, y), line, font=body_font, fill=fill)
        y += 36

    draw.line((92, 778, 1508, 778), fill=TERMINAL_HEADER, width=2)
    draw.text((92, 800), slide.footer, font=footer_font, fill=TERMINAL_MUTED)
    image.save(output_path)


def _run_canonical_pipeline(config_path: Path, output_root: Path) -> dict[str, object]:
    payload_path = output_root / "pipeline.json"
    command = [
        str(Path(__file__).with_name("canonical_pipeline.py")),
        "--config",
        str(config_path),
        "--output-root",
        str(output_root),
        "--write-json",
        str(payload_path),
    ]
    subprocess.run(
        [sys.executable, *command],
        cwd=REPO_ROOT,
        check=True,
        stdout=subprocess.DEVNULL,
    )
    return json.loads(payload_path.read_text(encoding="utf-8"))


def _write_concat_file(slides: list[Slide], frame_paths: list[Path], concat_path: Path) -> None:
    lines: list[str] = []
    for slide, frame_path in zip(slides, frame_paths, strict=True):
        lines.append(f"file '{frame_path.as_posix()}'")
        lines.append(f"duration {slide.duration:.2f}")
    lines.append(f"file '{frame_paths[-1].as_posix()}'")
    concat_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _build_gif(frame_paths: list[Path], slides: list[Slide], output_path: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="euroflex-demo-gif-") as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        concat_path = temp_dir / "frames.txt"
        palette_path = temp_dir / "palette.png"
        _write_concat_file(slides, frame_paths, concat_path)

        subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-f",
                "concat",
                "-safe",
                "0",
                "-i",
                str(concat_path),
                "-vf",
                "fps=8,scale=1200:-1:flags=lanczos,palettegen",
                str(palette_path),
            ],
            cwd=REPO_ROOT,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-f",
                "concat",
                "-safe",
                "0",
                "-i",
                str(concat_path),
                "-i",
                str(palette_path),
                "-lavfi",
                "fps=8,scale=1200:-1:flags=lanczos[x];[x][1:v]paletteuse=dither=bayer:bayer_scale=3",
                str(output_path),
            ],
            cwd=REPO_ROOT,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


def build_demo_gif(*, config_path: Path, output_path: Path, output_root: Path | None) -> Path:
    resolved_output_root = output_root or Path(tempfile.mkdtemp(prefix="euroflex-demo-artifacts-"))
    payload = _run_canonical_pipeline(config_path.resolve(), resolved_output_root.resolve())
    slides = _build_slides(payload)

    with tempfile.TemporaryDirectory(prefix="euroflex-demo-frames-") as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        frame_paths: list[Path] = []
        for index, slide in enumerate(slides):
            frame_path = temp_dir / f"slide-{index:02d}.png"
            _render_slide(slide, frame_path)
            frame_paths.append(frame_path)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        _build_gif(frame_paths, slides, output_path.resolve())
    return output_path.resolve()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Render the README demo GIF for the Belgium canonical path.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Canonical config to run for the demo.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Target GIF path.")
    parser.add_argument(
        "--output-root",
        type=Path,
        default=None,
        help="Optional artifact root used to generate the demo payload before rendering the GIF.",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    output_path = build_demo_gif(config_path=args.config, output_path=args.output, output_root=args.output_root)
    print(output_path)


if __name__ == "__main__":
    main()
