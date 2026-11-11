#!/usr/bin/env python3
"""Exercise real CLI lifecycle and process-crash recovery without cloud providers.

Run with --cli PATH --python PATH and optional --stock-cli PATH. The default
backend is a private temporary file store. --backend gs://BUCKET/UNIQUE_PREFIX
explicitly opts into GCS writes; the prefix must be dedicated to this test.
No production stack is selected. Artifacts and a JSON result are written under
--output when supplied. The program creates only component resources. GCS
objects in the dedicated prefix are retained for inspection after the test.
"""

import argparse
import json
import os
import signal
import subprocess
import tempfile
import time
from pathlib import Path

PROGRAM = """import time
from pathlib import Path
import pulumi
count = int(Path("count").read_text())
items = []
for i in range(count):
    item = pulumi.ComponentResource("test:index:Item", f"item-{i}")
    item.register_outputs({"value": i})
    items.append(item)
pulumi.export("count", count)
pulumi.export("secret", pulumi.Config().require_secret("secret"))
if Path("crash").exists() and not pulumi.runtime.is_dry_run():
    def wait_for_termination(_):
        print("JOURNAL_CRASH_READY", flush=True)
        time.sleep(120)
    pulumi.Output.all(*[item.urn for item in items]).apply(wait_for_termination)
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cli", type=Path, required=True)
    parser.add_argument("--python", type=Path, required=True)
    parser.add_argument("--stock-cli", type=Path)
    parser.add_argument("--backend")
    parser.add_argument("--automatic-recovery", action="store_true")
    parser.add_argument("--discard", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    cli = str(args.cli.resolve())
    with tempfile.TemporaryDirectory(prefix="journaling-smoke-") as directory:
        root = Path(directory)
        (root / "state").mkdir()
        project = root / "project"
        project.mkdir()
        (project / "Pulumi.yaml").write_text("name: journal-smoke\nruntime: python\n")
        (project / "__main__.py").write_text(PROGRAM)
        (project / "count").write_text("1")
        env = dict(
            os.environ,
            PULUMI_BACKEND_URL=args.backend or (root / "state").as_uri(),
            PULUMI_HOME=str(root / "home"),
            PULUMI_CONFIG_PASSPHRASE="",
            PULUMI_PYTHON_CMD=str(args.python.absolute()),
            PULUMI_SKIP_UPDATE_CHECK="true",
            PULUMI_DIY_BACKEND_JOURNALING="true",
            PULUMI_DIY_BACKEND_GZIP="true",
        )
        env["PATH"] = (
            str(args.python.absolute().parent) + os.pathsep + env.get("PATH", "")
        )
        log = []

        def run(*command, expected=True, binary=cli, extra_env=None, working_dir=None):
            start = time.monotonic()
            completed = subprocess.run(
                [binary, *command],
                check=False,
                cwd=working_dir or project,
                env=env | (extra_env or {}),
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=120,
            )
            log.append(
                {
                    "command": list(command),
                    "seconds": time.monotonic() - start,
                    "returncode": completed.returncode,
                    "output": completed.stdout,
                }
            )
            if expected != (completed.returncode == 0):
                raise RuntimeError(
                    f"Unexpected command result: {command}\n{completed.stdout}"
                )
            return completed

        run(
            "stack",
            "init",
            "teststack",
            "--secrets-provider",
            "passphrase",
            "--non-interactive",
        )
        run(
            "config",
            "set",
            "secret",
            "journal-encryption-sentinel",
            "--secret",
            "--stack",
            "teststack",
        )
        for count in (1, 2, 1):
            (project / "count").write_text(str(count))
            run(
                "up",
                "--yes",
                "--skip-preview",
                "--stack",
                "teststack",
                "--non-interactive",
            )
        run("refresh", "--yes", "--stack", "teststack", "--non-interactive")
        state = json.loads(run("stack", "export", "--stack", "teststack").stdout)
        assert len(state["deployment"]["resources"]) == 2
        (project / "count").write_text("3")
        (project / "crash").touch()
        crash_log = root / "crash.log"
        with crash_log.open("w") as stream:
            process = subprocess.Popen(
                [
                    cli,
                    "up",
                    "--yes",
                    "--skip-preview",
                    "--stack",
                    "teststack",
                    "--non-interactive",
                ],
                cwd=project,
                env=env,
                stdout=stream,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            try:
                deadline = time.monotonic() + 60
                while "JOURNAL_CRASH_READY" not in crash_log.read_text():
                    if process.poll() is not None or time.monotonic() > deadline:
                        raise RuntimeError(
                            "Crash setup did not reach durable resource writes:\n"
                            + crash_log.read_text()
                        )
                    time.sleep(0.1)
                run("stack", "ls", "--json")
                outputs = run("stack", "output", "--stack", "teststack", "--json")
                assert json.loads(outputs.stdout)["count"] == 1
                run("stack", "export", "--stack", "teststack", expected=False)
            finally:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=15)
        run("cancel", "--yes", "--stack", "teststack", "--non-interactive")
        if not args.backend:
            journals = list((root / "state/.pulumi/journal").rglob("*.journal.json"))
            assert journals
            assert all(
                b"journal-encryption-sentinel" not in p.read_bytes() for p in journals
            )
            before = {
                str(p): p.read_bytes()
                for p in (root / "state").rglob("*")
                if p.is_file()
            }
        run("preview", "--stack", "teststack", "--non-interactive")
        if not args.backend:
            after = {
                str(p): p.read_bytes()
                for p in (root / "state").rglob("*")
                if p.is_file()
            }
            assert before == after, "Read-only preview changed stored state"
        if args.stock_cli:
            rejected = run(
                "stack",
                "export",
                "--stack",
                "teststack",
                expected=False,
                binary=str(args.stock_cli.resolve()),
            )
            assert "pulumiJournalingV1" in rejected.stdout
        if args.automatic_recovery:
            run(
                "refresh",
                "--yes",
                "--stack",
                "teststack",
                "--non-interactive",
                extra_env={"PULUMI_DIY_BACKEND_JOURNALING": "false"},
            )
        else:
            run(
                "state",
                "journal",
                "discard" if args.discard else "recover",
                "--stack",
                "organization/journal-smoke/teststack",
                *(["--yes"] if args.discard else []),
                extra_env={"PULUMI_DIY_BACKEND_JOURNALING": "false"},
                working_dir=root,
            )
        (project / "crash").unlink()
        run(
            "up", "--yes", "--skip-preview", "--stack", "teststack", "--non-interactive"
        )
        run("preview", "--stack", "teststack", "--non-interactive")
        if args.stock_cli:
            run(
                "stack",
                "export",
                "--stack",
                "teststack",
                binary=str(args.stock_cli.resolve()),
            )
        run(
            "destroy",
            "--yes",
            "--skip-preview",
            "--stack",
            "teststack",
            "--non-interactive",
        )
        run("stack", "rm", "teststack", "--yes", "--non-interactive")
        result = {"backend": args.backend or "file", "passed": True, "commands": log}
        if args.output:
            args.output.mkdir(parents=True, exist_ok=True)
            (args.output / "smoke.json").write_text(json.dumps(result, indent=2))
        print(
            json.dumps(
                {"backend": result["backend"], "passed": True, "commands": len(log)}
            )
        )


if __name__ == "__main__":
    main()
