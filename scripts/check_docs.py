#!/usr/bin/env python3
"""Offline checks for maintained Markdown, not prose or production acceptance.

Supports fenced code, inline links/images, full/collapsed reference links,
ATX headings and explicit HTML id anchors. Does not render HTML, validate remote
URLs or interpret dynamic Markdown. Archives are hash-checked, not rewritten.
"""
from __future__ import annotations

import hashlib
import html
import json
import re
import sys
import tomllib
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]
INLINE = re.compile(r"\]\((<[^>\n]+>|[^\s)]+)(?:\s+[\"'][^\n]*?[\"'])?\)")
REF_DEF = re.compile(r"^\s{0,3}\[([^\]]+)\]:\s*(<[^>]+>|\S+)", re.M)
REF_USE = re.compile(r"\[([^\]\n]+)\]\[([^\]\n]*)\]")


def without_fences(text: str) -> tuple[str, bool]:
    result = []
    fence = ""
    for line in text.splitlines():
        match = re.match(r"^ {0,3}(`{3,}|~{3,})(.*)$", line)
        if match and not fence:
            fence = match[1]
            result.append("")
        elif (match and fence and match[1][0] == fence[0]
              and len(match[1]) >= len(fence) and not match[2].strip()):
            fence = ""
            result.append("")
        else:
            result.append("" if fence else line)
    return "\n".join(result), bool(fence)


def anchors(text: str) -> set[str]:
    text, _ = without_fences(text)
    result = set(re.findall(r'\bid=["\']([^"\']+)["\']', text))
    counts: dict[str, int] = {}
    for match in re.finditer(r"^ {0,3}#{1,6}\s+(.+?)(?:\s+#+)?\s*$", text, re.M):
        title = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", match[1])
        title = html.unescape(re.sub(r"<[^>]+>", "", title)).lower()
        slug = re.sub(r"[^\w\-\s]", "", title)
        slug = re.sub(r"\s", "-", slug)
        count = counts.get(slug, 0)
        counts[slug] = count + 1
        result.add(f"{slug}-{count}" if count else slug)
    return result


def destinations(text: str) -> list[str]:
    # Inline code can contain synthetic Markdown which is not a document link.
    text = re.sub(r"(`+)[^`\n]*?\1", "", text)
    refs = {m[1].strip().casefold(): m[2] for m in REF_DEF.finditer(text)}
    result = [m[1] for m in INLINE.finditer(text)] + list(refs.values())
    for match in REF_USE.finditer(text):
        key = (match[2] or match[1]).strip().casefold()
        result.append(refs.get(key, f"missing-reference:{key}"))
    return result


def link_error(root: Path, source: Path, href: str) -> str | None:
    href = html.unescape(href.strip("<>"))
    uri = urlsplit(href)
    if uri.scheme == "missing-reference":
        return f"undefined reference {uri.path}"
    if uri.scheme == "file" or (not uri.scheme and uri.path.startswith("/")):
        return f"absolute local link: {href}"
    if uri.scheme or uri.netloc:
        return None  # Remote existence / publication require separate evidence.
    target = (source.parent / unquote(uri.path)).resolve() if uri.path else source.resolve()
    if not target.is_relative_to(root.resolve()):
        return f"link leaves repository: {href}"
    if not target.exists():
        return f"missing target: {href}"
    fragment = unquote(uri.fragment)
    if fragment and target.suffix.lower() == ".md":
        if fragment not in anchors(target.read_text(encoding="utf-8")):
            return f"missing anchor: {href}"
    elif fragment and re.fullmatch(r"L\d+(?:-L\d+)?", fragment) and target.is_file():
        lines = [int(n) for n in re.findall(r"\d+", fragment)]
        if min(lines) < 1 or max(lines) > len(target.read_text(encoding="utf-8").splitlines()):
            return f"source line outside file: {href}"
    return None


def active_files(root: Path) -> list[Path]:
    excluded = {".git", ".venv", ".wheel-smoke", "node_modules", "bin", "dist", "_archive"}
    files = [p for p in root.rglob("*.md") if not excluded.intersection(p.relative_to(root).parts)]
    index = root / "docs/_archive/README.md"
    if index.exists():
        files.append(index)
    return sorted(files)


def source_facts(root: Path) -> list[str]:
    errors = []
    version = tomllib.loads((root / "python/pyproject.toml").read_text())["project"]["version"]
    index = (root / "docs/releases/README.md").read_text()
    if f"<!-- source-python-version: {version} -->" not in index:
        errors.append("release index source-python-version differs from pyproject.toml")
    go_version = re.search(r"^go (\S+)$", (root / "go.mod").read_text(), re.M)
    compatibility = (root / "docs/03-维护与验证/兼容与升级.md").read_text()
    if not go_version or go_version[1] not in compatibility:
        errors.append("compatibility page must include the go.mod minimum Go version")
    manifest = json.loads((root / "docs/_archive/manifest.json").read_text())
    for item in manifest["documents"]:
        path = root / item["archive_path"]
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != item["archive_sha256"]:
            errors.append(f"archive evidence changed: {item['archive_path']}")
    return errors


def check(root: Path) -> list[str]:
    errors = []
    files = active_files(root)
    for path in files:
        text, unclosed = without_fences(path.read_text(encoding="utf-8"))
        label = str(path.relative_to(root))
        if unclosed:
            errors.append(f"{label}: unclosed code fence")
        for href in destinations(text):
            error = link_error(root, path, href)
            if error:
                errors.append(f"{label}: {error}")
    errors.extend(source_facts(root))
    print(f"docs-check: {len(files)} maintained Markdown files; offline links/anchors and source facts")
    return errors


if __name__ == "__main__":
    try:
        failures = check(ROOT)
    except (OSError, ValueError, KeyError) as error:
        failures = [f"docs-check input error: {error}"]
    for failure in failures:
        print(failure, file=sys.stderr)
    raise SystemExit(bool(failures))
