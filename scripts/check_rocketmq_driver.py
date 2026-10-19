#!/usr/bin/env python3
"""Reproduce the maintained private runtime from the exact Apache module + patch."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tempfile


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    root = Path(__file__).resolve().parents[1]
    runtime = root / "transport/rocketmq/internal/driver"
    manifest = json.loads((runtime / "UPSTREAM.json").read_text())
    module = "github.com/apache/rocketmq-clients/golang/v5"
    if manifest["module"] != module or manifest["version"] != "v5.1.4":
        raise SystemExit("reviewed Apache module identity changed")
    result = subprocess.run(["go", "mod", "download", "-json", module + "@v5.1.4"],
                            cwd=root, capture_output=True, text=True, timeout=120)
    if result.returncode:
        raise SystemExit("required pinned Apache source unavailable")
    source = Path(json.loads(result.stdout)["Dir"])
    inventory = {p.name for p in runtime.glob("*.go") if not p.name.endswith("_test.go")}
    if inventory != manifest["upstream_sha256"].keys():
        raise SystemExit("unrecorded or missing runtime source file")
    if (runtime / "LICENSE").read_bytes() != (source / "LICENSE").read_bytes():
        raise SystemExit("Apache runtime license differs")
    with tempfile.TemporaryDirectory(prefix="rocketmq-source-proof-") as temporary:
        directory = Path(temporary)
        for name, expected in manifest["upstream_sha256"].items():
            if Path(name).name != name or sha(source / name) != expected:
                raise SystemExit("Apache source hash mismatch: " + name)
            shutil.copyfile(source / name, directory / name)
        for patch in manifest["patches"]:
            if Path(patch).name != patch:
                raise SystemExit("invalid patch path")
            result = subprocess.run(["patch", "--batch", "-p1", "-i", str(runtime / patch)],
                                    cwd=directory, capture_output=True, text=True, timeout=30)
            if result.returncode:
                raise SystemExit("recorded Apache runtime patch does not apply")
        if manifest["patched_sha256"].keys() != manifest["upstream_sha256"].keys():
            raise SystemExit("runtime source inventory differs")
        for name, expected in manifest["patched_sha256"].items():
            if sha(directory / name) != expected or sha(runtime / name) != expected:
                raise SystemExit("runtime differs from recorded patch: " + name)
    print(f"RocketMQ source proof: {len(manifest['patched_sha256'])} exact runtime files")


if __name__ == "__main__":
    main()
