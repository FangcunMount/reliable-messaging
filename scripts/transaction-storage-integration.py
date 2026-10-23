#!/usr/bin/env python3
"""Required SDK storage proof on an owned, ephemeral local Docker project."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sdk", type=Path, required=True)
    parser.add_argument("--mysql-probe", type=Path)
    parser.add_argument("--mongo-probe", type=Path)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--mysql-image", choices=("mysql:8.0.36", "mysql:8.0.44", "mysql:8.4"))
    args = parser.parse_args()
    sdk = args.sdk.resolve()
    project = "rm-sdk-m2-" + os.urandom(8).hex()
    report = {"version": 1, "project": project, "passed": 0, "failed": 0,
              "skipped": 0, "cases": [], "cleaned": False}
    docker_prefix = ["docker"]

    def run(command, **kwargs):
        if command[0] == "docker":
            command = docker_prefix + command[1:]
        return subprocess.run(command, text=True, capture_output=True, timeout=240,
                              check=True, **kwargs).stdout.strip()

    if os.environ.get("DOCKER_HOST"):
        raise SystemExit("local Unix Docker socket required; no external Docker host")
    context = run(["docker", "context", "show"])
    endpoint = run(["docker", "context", "inspect", context, "--format",
                    "{{.Endpoints.docker.Host}}"])
    if not endpoint.startswith("unix://"):
        raise SystemExit("local Unix Docker context required")
    before = set(run(["docker", "ps", "-aq"]).splitlines())
    compose = None
    try:
        with tempfile.TemporaryDirectory(prefix=project) as temporary:
            root = Path(temporary)
            # Public test images require no credentials. Do not consult a
            # desktop keychain or modify the user's real Docker config.
            config = root / "docker-config"
            config.mkdir(mode=0o700)
            plugins = [str(Path.home() / ".docker/cli-plugins"),
                       "/Applications/Docker.app/Contents/Resources/cli-plugins"]
            (config / "config.json").write_text(json.dumps({
                # A harmless non-registry entry prevents Docker's macOS
                # automatic native-keychain selection for an empty config.
                "auths": {"codex-public-fixture.invalid": {}},
                "credsStore": "", "cliPluginsExtraDirs": plugins}) + "\n")
            docker_prefix = ["docker", "--config", str(config), "--host", endpoint]
            override = root / "bounds.yml"
            override.write_text("services:\n  mysql:\n    mem_limit: 512m\n    cpus: 1\n"
                "  mongo:\n    mem_limit: 512m\n    cpus: 1\n"
                "    command: [mongod, --replSet, rm-test, --bind_ip_all, "
                "--setParameter, enableTestCommands=1, --wiredTigerCacheSizeGB, '0.25']\n")
            if args.mysql_image:
                override.write_text(override.read_text().replace("  mysql:\n", "  mysql:\n    image: " + args.mysql_image + "\n", 1))
            compose = ["docker", "compose", "-p", project, "-f",
                       str(sdk / "tests/integration/compose.yaml"), "-f", str(override)]
            architecture = run(["docker", "info", "--format", "{{.Architecture}}"])
            arch = {"aarch64": "arm64", "arm64": "arm64",
                    "x86_64": "amd64", "amd64": "amd64"}[architecture]
            binary = args.mysql_probe
            if binary is None:
                binary = root / "mysql-proof"
                run(["go", "test", "-c", "-tags=integration", "-o", str(binary),
                     "./storage/transaction/mysql"], cwd=sdk,
                    env={**os.environ, "CGO_ENABLED": "0", "GOOS": "linux", "GOARCH": arch})
            report["probe_sha256"] = hashlib.sha256(binary.read_bytes()).hexdigest()
            mongo_binary = args.mongo_probe
            if mongo_binary is None:
                mongo_binary = root / "mongo-proof"
                run(["go", "test", "-c", "-tags=integration", "-o", str(mongo_binary),
                     "./storage/transaction/mongo"], cwd=sdk,
                    env={**os.environ, "CGO_ENABLED": "0", "GOOS": "linux", "GOARCH": arch})
            report["mongo_probe_sha256"] = hashlib.sha256(mongo_binary.read_bytes()).hexdigest()
            report["source_sha256"] = {
                str(p.relative_to(sdk)): hashlib.sha256(p.read_bytes()).hexdigest()
                for directory in ("transaction", "storage/transaction", "internal/transactionobservation")
                for p in sorted((sdk / directory).rglob("*")) if p.is_file()}
            run(compose + ["up", "-d", "--wait", "--wait-timeout", "180", "mysql", "mongo"])
            container = run(compose + ["ps", "-q", "mysql"])
            label = run(["docker", "inspect", container, "--format",
                         '{{index .Config.Labels "com.docker.compose.project"}}'])
            if label != project:
                raise RuntimeError("owned container label mismatch")
            report["mysql_image"] = run(["docker", "inspect", container, "--format", "{{.Image}}"])
            run(compose + ["exec", "-T", "mysql", "mysql", "-uroot", "-e",
                           "CREATE DATABASE rm_sdk_tx_owned"])
            run(compose + ["cp", str(binary), "mysql:/tmp/mysql-proof"])
            start = time.monotonic()
            output = run(compose + ["exec", "-T", "-e", "RM_TX_DISPOSABLE=" + project,
                         "-e", "RM_TX_MYSQL_DSN=root@tcp(127.0.0.1:3306)/rm_sdk_tx_owned",
                         "mysql", "/tmp/mysql-proof", "-test.v", "-test.timeout=90s"])
            report["seconds"] = round(time.monotonic() - start, 3)
            report["cases"] = re.findall(r"^\s*--- PASS: (\S+)", output, re.M)
            report["passed"] = len(report["cases"])
            if "--- SKIP:" in output or "FAIL" in output or report["passed"] < 10:
                raise RuntimeError("required real storage proofs empty, incomplete or skipped")
            smoke = sdk / "tests/integration/mongo-smoke.js"
            initialized = run(compose + ["exec", "-T", "mongo", "mongosh", "--quiet",
                "--file", "/dev/stdin"], input=smoke.read_text())
            if "PASS Mongo " not in initialized or "replica-set commit/rollback" not in initialized:
                raise RuntimeError("required replica-set initialization proof missing")
            mongo_container = run(compose + ["ps", "-q", "mongo"])
            report["mongo_image"] = run(["docker", "inspect", mongo_container,
                "--format", "{{.Image}}"])
            run(compose + ["cp", str(mongo_binary), "mysql:/tmp/mongo-proof"])
            output = run(compose + ["exec", "-T", "-e", "RM_TX_DISPOSABLE=" + project,
                "-e", "RM_TX_MONGO_URI=mongodb://mongo:27017/?replicaSet=rm-test",
                "mysql", "/tmp/mongo-proof", "-test.v", "-test.timeout=90s"])
            mongo_cases = re.findall(r"^\s*--- PASS: (\S+)", output, re.M)
            if "--- SKIP:" in output or "FAIL" in output or len(mongo_cases) < 11:
                raise RuntimeError("required real Mongo proofs empty, incomplete or skipped")
            report["cases"].extend(mongo_cases)
            report["passed"] = len(report["cases"])
    except Exception as error:
        report["failed"] = 1
        report["error_type"] = type(error).__name__
        if isinstance(error, (subprocess.CalledProcessError, subprocess.TimeoutExpired)):
            failure_log = args.report.with_suffix(".private.log")
            failure_log.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(failure_log, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "w") as stream:
                out, err = error.stdout or "", error.stderr or ""
                if isinstance(out, bytes):
                    out = out.decode(errors="replace")
                if isinstance(err, bytes):
                    err = err.decode(errors="replace")
                stream.write(out + "\n" + err)
        raise
    finally:
        # Public-image credentials were isolated only for pull/up. Teardown is
        # local and needs the normal plugin discovery after the tempdir closes.
        docker_prefix = ["docker", "--host", endpoint]
        if compose is not None:
            # Temporary override may already be removed. The stable base still
            # identifies only this random project's resources during teardown.
            cleanup = ["docker", "compose", "-p", project, "-f",
                       str(sdk / "tests/integration/compose.yaml"), "down", "--volumes",
                       "--remove-orphans", "--timeout", "10"]
            run(cleanup)
        remaining = run(["docker", "ps", "-aq", "--filter", "label=com.docker.compose.project=" + project])
        after = set(run(["docker", "ps", "-aq"]).splitlines())
        report["cleaned"] = not remaining
        report["existing_containers_preserved"] = before <= after
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n")
        if not report["cleaned"] or not report["existing_containers_preserved"]:
            raise RuntimeError("owned cleanup or existing-container preservation failed")
    print(json.dumps({k: report[k] for k in ("passed", "failed", "skipped", "cleaned")}))


if __name__ == "__main__":
    main()
