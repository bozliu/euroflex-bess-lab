#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 INPUT.mp4 OUTPUT.gif [fps] [width]" >&2
  exit 1
fi

input_path="$1"
output_path="$2"
fps="${3:-10}"
width="${4:-1200}"

palette_path="$(mktemp "${TMPDIR:-/tmp}/euroflex-demo-palette.XXXXXX.png")"
trap 'rm -f "$palette_path"' EXIT

ffmpeg -y -i "$input_path" -vf "fps=${fps},scale=${width}:-1:flags=lanczos,palettegen" "$palette_path" >/dev/null 2>&1
ffmpeg -y -i "$input_path" -i "$palette_path" \
  -lavfi "fps=${fps},scale=${width}:-1:flags=lanczos[x];[x][1:v]paletteuse=dither=bayer:bayer_scale=3" \
  "$output_path" >/dev/null 2>&1
