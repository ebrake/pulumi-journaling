"""Verify archive integrity, extraction boundaries, and first-run installation."""

import hashlib
import io
import json
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pulumi_journaling as launcher


def bundle_bytes(name="pulumi/pulumi", kind=tarfile.REGTYPE):
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz") as bundle:
        info = tarfile.TarInfo(name)
        info.type = kind
        info.mode = 0o755
        data = b"fake executable"
        info.size = len(data) if kind == tarfile.REGTYPE else 0
        bundle.addfile(info, io.BytesIO(data))
    return output.getvalue()


class LauncherTests(unittest.TestCase):
    def test_unsafe_archives_rejected(self):
        for name, kind in [
            ("pulumi/../../escape", tarfile.REGTYPE),
            ("/absolute", tarfile.REGTYPE),
            ("pulumi/link", tarfile.SYMTYPE),
            ("other/pulumi", tarfile.REGTYPE),
        ]:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                archive = root / "bundle.tar.gz"
                archive.write_bytes(bundle_bytes(name, kind))
                with self.assertRaises(ValueError):
                    launcher.extract_bundle(archive, root / "out")

    def test_verified_download_and_cache(self):
        data = bundle_bytes()
        checksum = hashlib.sha256(data).hexdigest()
        manifest = {
            "artifacts": {
                "linux-amd64": {
                    "url": "https://github.com/ebrake/pulumi-journaling/releases/download/vtest/test.tar.gz",
                    "sha256": checksum,
                }
            }
        }
        with (
            tempfile.TemporaryDirectory() as temporary,
            patch.dict("os.environ", {"PULUMI_JOURNALING_CACHE": temporary}),
            patch.object(launcher, "platform_name", return_value="linux-amd64"),
            patch.object(Path, "read_text", return_value=json.dumps(manifest)),
            patch.object(
                launcher.urllib.request, "urlopen", return_value=io.BytesIO(data)
            ) as request,
        ):
            binary = launcher.ensure_cli()
            self.assertEqual(binary.read_bytes(), b"fake executable")
            self.assertTrue(binary.stat().st_mode & 0o111)
            self.assertEqual(launcher.ensure_cli(), binary)
            self.assertEqual(request.call_count, 1)

    def test_corrupt_download_never_installed(self):
        manifest = {
            "artifacts": {
                "linux-amd64": {
                    "url": "https://github.com/ebrake/pulumi-journaling/releases/download/vtest/test.tar.gz",
                    "sha256": "a" * 64,
                }
            }
        }
        with (
            tempfile.TemporaryDirectory() as temporary,
            patch.dict("os.environ", {"PULUMI_JOURNALING_CACHE": temporary}),
            patch.object(launcher, "platform_name", return_value="linux-amd64"),
            patch.object(Path, "read_text", return_value=json.dumps(manifest)),
            patch.object(
                launcher.urllib.request, "urlopen", return_value=io.BytesIO(b"corrupt")
            ),
        ):
            with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                launcher.ensure_cli()
            self.assertEqual(list(Path(temporary).iterdir()), [])


if __name__ == "__main__":
    unittest.main()
