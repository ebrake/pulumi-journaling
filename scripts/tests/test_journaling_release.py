"""Release packaging preserves hosts and pins all platforms to one source."""

import importlib.util
import io
import json
import tarfile
import tempfile
import unittest
from pathlib import Path

SPEC = importlib.util.spec_from_file_location(
    "journaling_release", Path(__file__).parents[1] / "journaling-release.py"
)
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)


class ReleaseTests(unittest.TestCase):
    def test_asset_name_has_no_plus(self):
        self.assertEqual(
            release.asset_name("3.274.1+journaling.1", "linux-arm64"),
            "pulumi-journaling-v3.274.1-journaling.1-linux-arm64.tar.gz",
        )

    def test_repack_replaces_only_cli_and_adds_provenance(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "upstream.tar.gz"
            with tarfile.open(source, "w:gz") as bundle:
                for name, data in {
                    "pulumi/pulumi": b"stock",
                    "pulumi/pulumi-language-python": b"matching-host",
                }.items():
                    entry = tarfile.TarInfo(name)
                    entry.mode = 0o755
                    entry.size = len(data)
                    bundle.addfile(entry, io.BytesIO(data))
            binary = root / "pulumi"
            binary.write_bytes(b"fork")
            target = root / "fork.tar.gz"
            release.repack(source, binary, target, {"sourceCommit": "a" * 40})
            with tarfile.open(target) as bundle:
                self.assertEqual(bundle.extractfile("pulumi/pulumi").read(), b"fork")
                self.assertEqual(
                    bundle.extractfile("pulumi/pulumi-language-python").read(),
                    b"matching-host",
                )
                self.assertEqual(
                    json.load(bundle.extractfile("pulumi/journaling-provenance.json"))[
                        "sourceCommit"
                    ],
                    "a" * 40,
                )
            second = root / "second.tar.gz"
            release.repack(source, binary, second, {"sourceCommit": "a" * 40})
            self.assertEqual(release.digest(target), release.digest(second))

    def test_merge_requires_every_platform_and_identical_source(self):
        platforms = json.loads((release.ROOT / "journaling-release.json").read_text())[
            "upstreamArchives"
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for platform in platforms:
                document = {
                    "schemaVersion": 1,
                    "repository": "ebrake/pulumi-journaling",
                    "version": "test",
                    "upstreamVersion": "base",
                    "sourceCommit": "a" * 40,
                    "artifacts": {platform: {"sha256": "b" * 64}},
                }
                (root / f"manifest-{platform}.json").write_text(json.dumps(document))
            release.merge(root)
            self.assertEqual(
                set(json.loads((root / "pulumi-cli.json").read_text())["artifacts"]),
                set(platforms),
            )
            bad = root / f"manifest-{next(iter(platforms))}.json"
            document = json.loads(bad.read_text())
            document["sourceCommit"] = "different"
            bad.write_text(json.dumps(document))
            with self.assertRaisesRegex(ValueError, "provenance"):
                release.merge(root)


if __name__ == "__main__":
    unittest.main()
