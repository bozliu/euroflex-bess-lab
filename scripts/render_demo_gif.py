#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from textwrap import wrap

from PIL import Image, ImageDraw, ImageFont

REPO_ROOT = Path(__file__).resolve().parents[1]
DEMO_ROOT = REPO_ROOT / "scripts" / "demo"
if str(DEMO_ROOT) not in sys.path:
    sys.path.insert(0, str(DEMO_ROOT))
canonical_demo = importlib.import_module("canonical_belgium_demo")
DEFAULT_CONFIG = canonical_demo.DEFAULT_CONFIG
build_demo_payload = canonical_demo.build_demo_payload

DEFAULT_OUTPUT = REPO_ROOT / "docs" / "assets" / "canonical-belgium-demo.gif"
DEFAULT_MP4_TO_GIF = REPO_ROOT / "scripts" / "demo" / "mp4_to_gif.sh"
FRAME_SIZE = (1600, 900)
OUTPUT_WIDTH = 1200
MP4_FPS = 12
GIF_FPS = 10
TERMINAL_BG = "#09131a"
TERMINAL_PANEL = "#10232f"
TERMINAL_TEXT = "#e5f0f7"
TERMINAL_MUTED = "#8ea8b8"
TERMINAL_ACCENT = "#54d1a0"
TERMINAL_HEADER = "#163544"


@dataclass(frozen=True)
class RenderScene:
    prompt_lines: list[str]
    output_lines: list[str]
    duration: float


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


def _wrap_terminal_line(text: str, *, width: int, prompt: bool) -> list[str]:
    indent = "  " if prompt else "   "
    wrapped = wrap(
        text,
        width=width,
        replace_whitespace=False,
        drop_whitespace=False,
        subsequent_indent=indent,
    )
    return wrapped or [text]


def _flatten_history(scenes: list[RenderScene], index: int) -> list[tuple[str, str]]:
    history: list[tuple[str, str]] = []
    for scene in scenes[: index + 1]:
        for prompt in scene.prompt_lines:
            history.append(("prompt", f"$ {prompt}"))
        for line in scene.output_lines:
            history.append(("output", line))
        history.append(("blank", ""))
    return history


def _line_color(kind: str, text: str) -> str:
    if kind == "prompt":
        return TERMINAL_ACCENT
    if text.startswith(
        ("validate-", "Run completed:", "Reconciliation written", "Schedule export written", "Bid export written")
    ):
        return TERMINAL_ACCENT
    if text.endswith(".yaml") or text.startswith("artifacts/examples/"):
        return TERMINAL_MUTED
    return TERMINAL_TEXT


def _render_scene(scene_index: int, scenes: list[RenderScene], output_path: Path) -> None:
    image = Image.new("RGB", FRAME_SIZE, color=TERMINAL_BG)
    draw = ImageDraw.Draw(image)
    title_font = _load_font(28, bold=True)
    body_font = _load_font(22)
    footer_font = _load_font(18)

    left, top, right, bottom = 52, 46, 1548, 854
    draw.rounded_rectangle((left, top, right, bottom), radius=26, fill=TERMINAL_PANEL, outline=TERMINAL_HEADER, width=3)
    draw.rounded_rectangle((left, top, right, 118), radius=26, fill=TERMINAL_HEADER)
    for idx, color in enumerate(("#ff5f57", "#febc2e", "#28c840")):
        dot_left = 84 + idx * 32
        draw.ellipse((dot_left, 74, dot_left + 18, 92), fill=color)

    draw.text((156, 67), "euroflex_bess_lab :: canonical Belgium full-stack demo", font=title_font, fill=TERMINAL_TEXT)

    history = _flatten_history(scenes, scene_index)
    rendered_lines: list[tuple[str, str]] = []
    for kind, text in history:
        if kind == "blank":
            rendered_lines.append((kind, text))
            continue
        rendered_lines.extend((kind, part) for part in _wrap_terminal_line(text, width=92, prompt=(kind == "prompt")))

    max_lines = 23
    visible_lines = rendered_lines[-max_lines:]
    y = 150
    line_height = 28
    for kind, text in visible_lines:
        draw.text((88, y), text, font=body_font, fill=_line_color(kind, text))
        y += line_height

    footer = "validate -> validate-data -> backtest -> reconcile -> export operator schedule -> export bid plan"
    draw.line((88, 790, 1510, 790), fill=TERMINAL_HEADER, width=2)
    draw.text((88, 810), footer, font=footer_font, fill=TERMINAL_MUTED)
    image.save(output_path)


def _write_concat_file(scenes: list[RenderScene], frame_paths: list[Path], concat_path: Path) -> None:
    lines: list[str] = []
    for scene, frame_path in zip(scenes, frame_paths, strict=True):
        lines.append(f"file '{frame_path.as_posix()}'")
        lines.append(f"duration {scene.duration:.2f}")
    lines.append(f"file '{frame_paths[-1].as_posix()}'")
    concat_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _build_mp4(scenes: list[RenderScene], frame_paths: list[Path], output_path: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="euroflex-demo-mp4-") as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        concat_path = temp_dir / "frames.txt"
        _write_concat_file(scenes, frame_paths, concat_path)
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
                "-fps_mode",
                "vfr",
                "-c:v",
                "mpeg4",
                "-q:v",
                "4",
                "-vf",
                f"fps={MP4_FPS},scale={OUTPUT_WIDTH}:-1:flags=lanczos",
                "-pix_fmt",
                "yuv420p",
                str(output_path),
            ],
            cwd=REPO_ROOT,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


def build_demo_gif(*, config_path: Path, output_path: Path, mp4_path: Path | None = None) -> Path:
    payload = build_demo_payload(
        config_path=config_path.resolve(),
        market="belgium",
        workflow="schedule_revision",
        schedule_profile="operator",
        bids_profile="bid_planning",
    )
    scenes = [RenderScene(**scene) for scene in payload["scenes"]]

    with tempfile.TemporaryDirectory(prefix="euroflex-demo-frames-") as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        frame_paths: list[Path] = []
        for index in range(len(scenes)):
            frame_path = temp_dir / f"frame-{index:02d}.png"
            _render_scene(index, scenes, frame_path)
            frame_paths.append(frame_path)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        if mp4_path is None:
            with tempfile.TemporaryDirectory(prefix="euroflex-demo-master-") as mp4_dir_name:
                master_mp4 = Path(mp4_dir_name) / "canonical-belgium-demo.mp4"
                _build_mp4(scenes, frame_paths, master_mp4)
                subprocess.run(
                    [str(DEFAULT_MP4_TO_GIF), str(master_mp4), str(output_path), str(GIF_FPS), str(OUTPUT_WIDTH)],
                    cwd=REPO_ROOT,
                    check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
        else:
            mp4_path.parent.mkdir(parents=True, exist_ok=True)
            _build_mp4(scenes, frame_paths, mp4_path)
            subprocess.run(
                [str(DEFAULT_MP4_TO_GIF), str(mp4_path), str(output_path), str(GIF_FPS), str(OUTPUT_WIDTH)],
                cwd=REPO_ROOT,
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
    return output_path.resolve()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Render the README demo GIF for the Belgium canonical path.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Canonical config to execute.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Target GIF path.")
    parser.add_argument("--write-mp4", type=Path, default=None, help="Optional path for the intermediate MP4.")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    output_path = build_demo_gif(config_path=args.config, output_path=args.output, mp4_path=args.write_mp4)
    print(output_path)


if __name__ == "__main__":
    main()
