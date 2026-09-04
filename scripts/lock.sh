#!/usr/bin/env bash
# Regenerate pdm.lock, pylock.toml and uv.lock together so they stay in sync
# with pyproject.toml and each other.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

pdm config -l use_uv false
pdm lock --strategy inherit_metadata
pdm export -f pylock -o pylock.toml
uv lock
