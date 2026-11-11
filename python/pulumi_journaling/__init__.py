# Copyright 2026, E Brake. Licensed under the Apache License, Version 2.0.
"""Launch the verified, version-pinned pulumi-journaling CLI bundle."""

import hashlib
import json
import os
import platform
import re
import shutil
import sys
import tarfile
import tempfile
import urllib.parse
import urllib.request
from pathlib import Path, PurePosixPath


def platform_name():
    system = {"Linux": "linux", "Darwin": "darwin"}.get(platform.system())
    arch = {
        "x86_64": "amd64",
        "amd64": "amd64",
        "aarch64": "arm64",
        "arm64": "arm64",
    }.get(platform.machine().lower())
    if not system or not arch:
        raise RuntimeError(
            "pulumi-journaling supports Linux and macOS on x86-64 and ARM64"
        )
    return f"{system}-{arch}"


def extract_bundle(archive, destination):
    """Extract regular bundle files without accepting links or unsafe names."""
    seen = set()
    with tarfile.open(archive, "r:gz") as bundle:
        for entry in bundle:
            name = PurePosixPath(entry.name)
            if (
                not entry.isfile()
                or name.is_absolute()
                or ".." in name.parts
                or "\\" in entry.name
                or len(name.parts) < 2
                or name.parts[0] != "pulumi"
                or entry.name in seen
            ):
                raise ValueError("Unsafe or duplicate CLI bundle member")
            seen.add(entry.name)
            target = destination.joinpath(*name.parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            source = bundle.extractfile(entry)
            if source is None:
                raise ValueError("CLI bundle member has no file contents")
            with source, target.open("xb") as output:
                shutil.copyfileobj(source, output)
            target.chmod(0o755 if entry.mode & 0o111 else 0o644)
    if not (destination / "pulumi/pulumi").is_file():
        raise ValueError("CLI bundle is missing pulumi/pulumi")


def ensure_cli():
    manifest = json.loads(Path(__file__).with_name("release.json").read_text())
    target_platform = platform_name()
    artifact = manifest["artifacts"].get(target_platform)
    if artifact is None:
        raise RuntimeError(
            "No verified bundle is packaged for this platform; install a published pulumi-journaling release"
        )
    checksum = artifact["sha256"]
    parsed = urllib.parse.urlparse(artifact["url"])
    if (
        not re.fullmatch(r"[0-9a-f]{64}", checksum)
        or parsed.scheme != "https"
        or parsed.netloc != "github.com"
        or not parsed.path.startswith("/ebrake/pulumi-journaling/releases/download/")
    ):
        raise ValueError("Invalid packaged CLI artifact")
    cache = Path(
        os.environ.get(
            "PULUMI_JOURNALING_CACHE", Path.home() / ".cache/pulumi-journaling"
        )
    )
    cache.mkdir(parents=True, exist_ok=True)
    target = cache / f"{target_platform}-{checksum}"
    binary = target / "pulumi/pulumi"
    if binary.is_file() and (target / ".complete").is_file():
        return binary
    print(
        f"Installing verified pulumi-journaling CLI for {target_platform}...",
        file=sys.stderr,
    )
    with tempfile.TemporaryDirectory(prefix=".install-", dir=cache) as temporary:
        root = Path(temporary)
        archive = root / "bundle.tar.gz"
        digest = hashlib.sha256()
        with (
            urllib.request.urlopen(artifact["url"], timeout=120) as response,
            archive.open("wb") as output,
        ):
            while data := response.read(1024 * 1024):
                output.write(data)
                digest.update(data)
        if digest.hexdigest() != checksum:
            raise ValueError("CLI bundle checksum mismatch")
        extracted = root / "extracted"
        extracted.mkdir()
        extract_bundle(archive, extracted)
        (extracted / ".complete").write_text(checksum)
        try:
            extracted.rename(target)
        except OSError:
            if not (binary.is_file() and (target / ".complete").is_file()):
                raise
    return binary


def main():
    try:
        binary = ensure_cli()
    except (OSError, ValueError, RuntimeError, KeyError) as error:
        print(f"pulumi-journaling: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    os.environ.setdefault("PULUMI_DIY_BACKEND_JOURNALING", "true")
    os.environ.setdefault("PULUMI_SKIP_UPDATE_CHECK", "true")
    os.execv(str(binary), [str(binary), *sys.argv[1:]])
