# CLAUDE.md

## Dependency lock files

This repo maintains three lock files that must stay in sync with `pyproject.toml` and each other: `pdm.lock`, `pylock.toml`, and `uv.lock`. `.github/workflows/lockfiles.yml` enforces this in CI and fails the build if any of them drift.

Whenever dependencies change in `pyproject.toml`, regenerate all three by running:

```sh
scripts/lock.sh
```

It runs, in order: `pdm config -l use_uv false` (the native resolver is required — pdm's uv-backed resolver silently drops `inherit_metadata`, which the `pylock.toml` export needs), `pdm lock --strategy inherit_metadata`, `pdm export -f pylock -o pylock.toml`, then `uv lock`.

Commit all three lock files together in the same commit as the `pyproject.toml` change.
