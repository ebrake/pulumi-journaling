#!/usr/bin/env python3
"""Build checksummed independent CLI bundles from the reviewed release config.

Run --platform PLATFORM --output DIRECTORY from any directory in the checkout.
The upstream archive is verified against journaling-release.json; only the CLI is
replaced, preserving matching upstream language hosts. A source provenance file
is included, and a platform manifest is emitted for the consuming repository.
The command never publishes; publishing uses the dedicated GitHub workflow or
an explicit gh release command. --merge combines platform manifests into the
single consumer manifest after all platforms have been built from one commit.
"""

import argparse
import gzip
import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for data in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(data)
    return result.hexdigest()


def repack(upstream, binary, target, provenance):
    with (
        tarfile.open(upstream, "r:gz") as source,
        target.open("wb") as output,
        gzip.GzipFile(filename="", mode="wb", fileobj=output, mtime=0) as zipped,
        tarfile.open(fileobj=zipped, mode="w|") as bundle,
    ):
        replaced = False
        for entry in sorted(source.getmembers(), key=lambda member: member.name):
            if not entry.isfile():
                if entry.isdir():
                    continue
                raise ValueError("Upstream bundle contains a non-regular file")
            name = Path(entry.name)
            if name.is_absolute() or ".." in name.parts or name.parts[0] != "pulumi":
                raise ValueError("Unexpected upstream archive path")
            entry.uid = entry.gid = entry.mtime = 0
            entry.uname = entry.gname = ""
            if entry.name == "pulumi/pulumi":
                entry.size = binary.stat().st_size
                with binary.open("rb") as stream:
                    bundle.addfile(entry, stream)
                replaced = True
            else:
                stream = source.extractfile(entry)
                if stream is None:
                    raise ValueError("Upstream bundle member has no file contents")
                with stream:
                    bundle.addfile(entry, stream)
        if not replaced:
            raise ValueError("Upstream bundle did not contain the CLI")
        for filename in ("LICENSE", "NOTICE", "AUTHORS.md"):
            data = (ROOT / filename).read_bytes()
            notice = tarfile.TarInfo("pulumi/" + filename)
            notice.size = len(data)
            notice.mode = 0o644
            bundle.addfile(notice, io.BytesIO(data))
        data = json.dumps(provenance, sort_keys=True).encode()
        entry = tarfile.TarInfo("pulumi/journaling-provenance.json")
        entry.size = len(data)
        entry.mode = 0o644
        bundle.addfile(entry, io.BytesIO(data))


def merge(directory):
    result = None
    for path in sorted(directory.glob("manifest-*.json")):
        document = json.loads(path.read_text())
        if result is None:
            result = document
        else:
            for key in (
                "schemaVersion",
                "repository",
                "version",
                "upstreamVersion",
                "sourceCommit",
            ):
                if result[key] != document[key]:
                    raise ValueError("Platform provenance does not agree")
            if result["artifacts"].keys() & document["artifacts"].keys():
                raise ValueError("Duplicate platform manifest")
            result["artifacts"].update(document["artifacts"])
    if result is None:
        raise ValueError("No platform manifests found")
    required = json.loads((ROOT / "journaling-release.json").read_text())[
        "upstreamArchives"
    ]
    if set(result["artifacts"]) != set(required):
        raise ValueError("Required platform artifacts are missing")
    (directory / "pulumi-cli.json").write_text(json.dumps(result, indent=2) + "\n")


def asset_name(version, platform):
    """Keep semver build metadata in the tag while using plain asset filenames."""
    return f"pulumi-journaling-v{version.replace('+', '-')}-{platform}.tar.gz"


def prepare_python(directory):
    manifest = json.loads((directory / "pulumi-cli.json").read_text())
    config = json.loads((ROOT / "journaling-release.json").read_text())
    if (
        manifest["version"] != config["version"]
        or manifest["upstreamVersion"] != config["upstreamVersion"]
        or set(manifest["artifacts"]) != set(config["upstreamArchives"])
    ):
        raise ValueError("Python release does not match the reviewed configuration")
    for platform, artifact in manifest["artifacts"].items():
        expected_url = f"https://github.com/ebrake/pulumi-journaling/releases/download/v{config['version']}/{asset_name(config['version'], platform)}"
        if artifact["url"] != expected_url or not re.fullmatch(
            r"[0-9a-f]{64}", artifact["sha256"]
        ):
            raise ValueError("Invalid Python release artifact")
    (ROOT / "python/pulumi_journaling/release.json").write_text(
        json.dumps(manifest, indent=2) + "\n"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--platform")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--merge", action="store_true")
    parser.add_argument("--python-package", action="store_true")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    if args.python_package:
        prepare_python(args.output)
        return
    if args.merge:
        merge(args.output)
        return
    config = json.loads((ROOT / "journaling-release.json").read_text())
    upstream_config = config["upstreamArchives"][args.platform]
    source_commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()
    subprocess.run(
        ["git", "diff", "--exit-code", "HEAD"],
        cwd=ROOT,
        check=True,
        stdout=subprocess.DEVNULL,
    )
    system, arch = args.platform.split("-")
    provenance = {
        "schemaVersion": 1,
        "repository": "ebrake/pulumi-journaling",
        "version": config["version"],
        "upstreamVersion": config["upstreamVersion"],
        "sourceCommit": source_commit,
    }
    with tempfile.TemporaryDirectory(prefix="pulumi-journaling-release-") as temporary:
        temporary = Path(temporary)
        upstream = temporary / upstream_config["name"]
        url = f"https://github.com/pulumi/pulumi/releases/download/v{config['upstreamVersion']}/{upstream.name}"
        with (
            urllib.request.urlopen(url, timeout=120) as response,
            upstream.open("wb") as stream,
        ):
            shutil.copyfileobj(response, stream)
        if digest(upstream) != upstream_config["sha256"]:
            raise ValueError("Upstream Pulumi archive checksum mismatch")
        binary = temporary / "pulumi"
        subprocess.run(
            [
                "go",
                "build",
                "-trimpath",
                "-ldflags",
                f"-s -w -X github.com/pulumi/pulumi/sdk/v3/go/common/version.Version={config['version']}",
                "-o",
                str(binary),
                "./cmd/pulumi",
            ],
            cwd=ROOT / "pkg",
            check=True,
            env=dict(os.environ, GOOS=system, GOARCH=arch, CGO_ENABLED="0"),
        )
        name = asset_name(config["version"], args.platform)
        target = args.output / name
        repack(upstream, binary, target, provenance)
    artifact = {
        "url": f"https://github.com/ebrake/pulumi-journaling/releases/download/v{config['version']}/{name}",
        "sha256": digest(target),
    }
    document = provenance | {"artifacts": {args.platform: artifact}}
    (args.output / f"manifest-{args.platform}.json").write_text(
        json.dumps(document, indent=2) + "\n"
    )
    print(json.dumps({"platform": args.platform, "sha256": artifact["sha256"]}))


if __name__ == "__main__":
    main()
