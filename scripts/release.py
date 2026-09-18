"""Cut the next apiadapters release: changelog section, commit, annotated tag, push.

git-cliff picks the version and renders `CHANGELOG.md`; this sequences
the git side around it. The tag is the version — `hatch-vcs` reads it at
build time and `pyproject.toml` carries no number — so creating the tag
is what makes a release.

Exit codes: 0 released (with --dry-run: would release) · 1 nothing to release ·
2 could not release, or released only in part — the message names what was done.
"""

from __future__ import annotations

import argparse
import contextlib
import os
import re
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Iterator
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BRANCH = "main"
CHANGELOG = "CHANGELOG.md"
VERSION_RE = re.compile(r"v\d+\.\d+\.\d+")
#: A prefix that cannot change what a consumer sees. `build!:` is excluded
#: deliberately — dropping a Python version is a break and must release.
INTERNAL_RE = re.compile(r"^(build|chore|test|docs|ci)(\([^)]*\))?:")


class ReleaseError(Exception):
    """The release could not be made or finished; the message says what
    it left behind."""


def _run(*args: str, timeout: int = 60) -> str:
    try:
        out = subprocess.run(
            args, cwd=REPO, capture_output=True, text=True, timeout=timeout
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ReleaseError(f"`{' '.join(args)}` could not run — {exc}") from exc
    if out.returncode != 0:
        raise ReleaseError(f"`{' '.join(args)}` failed: {out.stderr.strip()}")
    return out.stdout.strip()


def _git(*args: str, timeout: int = 60) -> str:
    return _run("git", "-C", str(REPO), *args, timeout=timeout)


def _cliff(*args: str) -> str:
    exe = shutil.which("git-cliff")
    if exe is None:
        raise ReleaseError(
            "git-cliff is not on PATH — run this as `pdm run release`"
        )
    return _run(exe, "--config", str(REPO / "cliff.toml"), *args)


@contextlib.contextmanager
def _message_file(text: str) -> Iterator[str]:
    """A scratch file for `-F`, never `-m`: a message through `-m` gets
    shell-expanded."""
    fd, path = tempfile.mkstemp(suffix=".txt", text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
        yield path
    finally:
        os.unlink(path)


def current_tag() -> str | None:
    out = subprocess.run(
        [
            "git",
            "-C",
            str(REPO),
            "describe",
            "--tags",
            "--abbrev=0",
            "--match",
            "v[0-9]*",
        ],
        capture_output=True,
        text=True,
    )
    return out.stdout.strip() or None


def _only_internal(tag: str | None) -> bool:
    """Whether every commit since `tag` is one that releases nothing."""
    rng = f"{tag}..HEAD" if tag else "HEAD"
    subjects = _git("log", "--no-merges", "--format=%s", rng).splitlines()
    return bool(subjects) and all(INTERNAL_RE.match(s) for s in subjects)


def release(*, dry_run: bool, push: bool, version: str | None) -> int:
    branch = _git("symbolic-ref", "--short", "-q", "HEAD")
    if branch != BRANCH:
        raise ReleaseError(
            f"releases are cut from `{BRANCH}`, HEAD is on `{branch}`"
        )

    tag = current_tag()
    new = version or _cliff("--bumped-version").splitlines()[-1].strip()
    if not VERSION_RE.fullmatch(new):
        raise ReleaseError(f"{new!r} is not a vX.Y.Z version")
    if new == tag:
        print(f"release: nothing to release — no commits since {tag}")
        return 1
    if version is None and _only_internal(tag):
        print(
            f"release: nothing to release — every commit since {tag} is"
            " build/chore/test/docs. Force one with --version vX.Y.Z"
        )
        return 1
    if _git("tag", "--list", new):
        raise ReleaseError(f"tag {new} already exists — nothing changed")
    dirty = _git("status", "--porcelain", "--", CHANGELOG)
    if dirty:
        raise ReleaseError(
            f"{CHANGELOG} has uncommitted changes (`{dirty}`) — land or"
            " discard them first"
        )

    section = _cliff("--tag", new, "--unreleased", "--strip", "header")
    if dry_run:
        print(
            f"release: would cut {new} from {tag or 'the first commit'}"
            f"\n\n{section}"
        )
        return 0

    path = REPO / CHANGELOG
    before = path.read_text(encoding="utf-8")
    _cliff("--tag", new, "--output", CHANGELOG)
    subject = f"docs: changelog for {new.lstrip('v')}"
    try:
        with _message_file(f"{subject}\n") as msg:
            _git("commit", "-F", msg, "--", CHANGELOG)
        sha = _git("rev-parse", "HEAD")
    except ReleaseError as exc:
        path.write_text(before, encoding="utf-8")
        raise ReleaseError(
            f"{exc} — {CHANGELOG} restored, nothing changed"
        ) from exc

    try:
        with _message_file(f"apiadapters {new.lstrip('v')}\n\n{section}\n") as msg:
            _git("tag", "-a", "--cleanup=whitespace", "-F", msg, new, sha)
    except ReleaseError as exc:
        raise ReleaseError(
            f"committed {sha[:7]} `{subject}` but could NOT tag it — {exc}."
            " Tag by hand"
            f" (`git tag -a {new} {sha[:7]}`) or revert that commit"
        ) from exc

    done = [
        f"release: {new}",
        f"   committed {sha[:7]}  {subject}",
        f"   tagged    {new}",
    ]
    retry = f"git push --atomic origin {BRANCH} {new}"
    if not push:
        print(
            "\n".join(
                [*done, f"   NOT pushed (--no-push) — publish with: {retry}"]
            )
        )
        return 0
    try:
        _git("push", "--atomic", "origin", BRANCH, new, timeout=180)
    except ReleaseError as exc:
        print(
            "\n".join([*done, f"   push FAILED — {exc}", f"   retry: {retry}"])
        )
        return 2
    print("\n".join([*done, f"   pushed    {BRANCH} + {new}"]))
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="release", description="Cut the next apiadapters release."
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="print what it would cut; change nothing",
    )
    p.add_argument(
        "--no-push",
        action="store_true",
        help="commit and tag only; print the push command",
    )
    p.add_argument(
        "--version",
        metavar="vX.Y.Z",
        help="override the version git-cliff computes",
    )
    args = p.parse_args(argv)
    try:
        return release(
            dry_run=args.dry_run, push=not args.no_push, version=args.version
        )
    except ReleaseError as exc:
        print(f"release: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
