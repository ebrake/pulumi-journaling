# pulumi-journaling

An independent fork of [Pulumi](https://github.com/pulumi/pulumi), maintained by **E Brake** <ebrake123@icloud.com>, with hardened, recoverable journaling for every DIY state storage backend: local files, Amazon S3 and compatible stores, Google Cloud Storage, Azure Blob Storage, and PostgreSQL. Based on upstream **v3.275.0** at commit [`cc3a399d789290e4d98becbbec4ffca3ad30230d`](https://github.com/pulumi/pulumi/commit/cc3a399d789290e4d98becbbec4ffca3ad30230d), the latest stable release when this fork was prepared.

This project is independently maintained and is not affiliated with, sponsored by, or endorsed by Pulumi Corporation. Pulumi is a trademark of Pulumi Corporation.

## Install

```sh
python -m pip install pulumi-journaling==3.275.0.post2
pulumi-journaling version
```

The Python distribution provides both `pulumi-journaling` and `pulumi` commands, plus the matching upstream Python SDK, so existing `import pulumi` programs continue to work. On first invocation, the launcher downloads the matching CLI and language hosts from this fork's [GitHub releases](https://github.com/ebrake/pulumi-journaling/releases), verifies the SHA-256 checksum embedded in the Python package, and caches the bundle. That first invocation requires network access. Set `PULUMI_JOURNALING_CACHE` to choose the cache directory. Linux and macOS on x86-64 and ARM64 are supported; Windows packages are not currently published.

## Use journaling

The Python launcher enables journaling automatically for every DIY backend. Check that `pulumi version` includes `+journaling` so your shell is using this fork. You can also use `pulumi-journaling` in place of `pulumi` in every command below.

From an existing Pulumi project, log in to your state backend, select an existing stack, and deploy normally:

```sh
pulumi version
pulumi login 's3://my-state-bucket'
pulumi stack select organization/my-project/dev
pulumi preview
pulumi up
```

Replace the backend URL and stack name with your own. Create the bucket, container or database and configure its usual credentials before logging in. Supported backend URLs include:

| State storage | Example backend URL |
| --- | --- |
| Local files | `file:///absolute/path/to/state` |
| Amazon S3 or compatible storage | `s3://my-state-bucket` |
| Google Cloud Storage | `gs://my-state-bucket` |
| Azure Blob Storage | `azblob://my-state-container` |
| PostgreSQL | `postgres://user:password@host:5432/database?sslmode=require` |

For the standalone CLI downloaded from GitHub releases, enable journaling in the shell or CI job that runs Pulumi:

```sh
export PULUMI_DIY_BACKEND_JOURNALING=true
pulumi up
```

Mutating operations such as `up`, `refresh`, and `destroy` record recoverable changes under the backend's `.pulumi/journal/` prefix. A successful operation consolidates them into the normal checkpoint and clears the active journal. Previews and other read-only commands see the last clean checkpoint while an update is unfinished. Pulumi Cloud retains its upstream service-managed persistence and journaling behavior.

### Recover after an interrupted update

The next mutating operation automatically recovers an unfinished journal before loading state. To recover explicitly without invoking resource providers:

```sh
pulumi state journal recover --stack organization/my-project/dev
pulumi preview --stack organization/my-project/dev
```

If the previous process crashed and left a lock, first verify that process has stopped, then run `pulumi cancel --stack organization/my-project/dev` to remove its stale lock before recovery. Review the preview and resume deployment normally; provider-side effects from an interrupted operation may still need normal Pulumi reconciliation.

If recovery refuses corrupt or unexplained state, preserve the evidence and consult the [recovery guide](https://github.com/ebrake/pulumi-journaling/blob/main/docs/diy-journaling.md) before using `pulumi state journal discard --yes`. Discarding can leave state missing changes from the interrupted operation.

### Disable journaling

To stop creating new journals for one command:

```sh
PULUMI_DIY_BACKEND_JOURNALING=false pulumi up
```

Use `export PULUMI_DIY_BACKEND_JOURNALING=false` to disable new journals for the shell or CI job. Unfinished journals still recover before mutating operations. Complete recovery before changing to an upstream CLI or another fork. Clean snapshots and provider packages retain upstream contracts.

Pulumi 3.275.0 requires project-scoped DIY state. If you still use legacy non-project state, first recover any unfinished journals with the CLI that created them, then use Pulumi 3.274.1 or `pulumi-journaling==3.274.1.post2` to run `pulumi state upgrade`, and only then install this release. See [Upgrading legacy non-project state](https://github.com/ebrake/pulumi-journaling/blob/main/docs/diy-journaling.md#upgrading-legacy-non-project-state).

## Rapid GCS and storage latency

Google Cloud Storage Rapid buckets are supported with an explicit backend option:

```sh
pulumi login 'gs://my-rapid-state-bucket?gcs_api=rapid'
PULUMI_DIY_BACKEND_JOURNALING=true pulumi up
```

Choose low-latency state storage close to the machine running Pulumi. For Rapid,
place CI runners in the bucket's exact zone. The fork uses the Google Cloud
Storage Go SDK's gRPC transport and keeps one append stream open for the update,
flushing required journal entries durably and finalizing once at close. Set
`PULUMI_GCS_DIRECT_PATH=required` on colocated runners to reject connections that
cannot use direct connectivity. Remote operator CLIs can leave it unset.

Rapid is zonal storage and does not provide object versioning or soft delete.
Keep verified checkpoint archives outside the bucket and exclude current state
and active journals from lifecycle deletion. See the [Rapid transport guide](https://github.com/ebrake/pulumi-journaling/blob/main/docs/diy-journaling.md#rapid-gcs-transport).
Other DIY backends retain their existing journal and recovery paths.

## State persistence benchmarks

Same-zone GCS Rapid measurements, in seconds: medians from three paired trials per row. Place runners close to state storage to minimize latency; remote storage will have different absolute timings.

| Resources | Updated | Without journaling | With journaling | Speedup |
|---:|---:|---:|---:|---:|
| 24 | 1 | 1.83 | 1.88 | ≈1× |
| 24 | 5 | 6.63 | 1.69 | 3.9× |
| 24 | 10 | 12.92 | 1.83 | 7.0× |
| 24 | 15 | 19.66 | 1.69 | 11.6× |
| 91 | 1 | 1.87 | 1.85 | ≈1× |
| 91 | 5 | 5.74 | 1.84 | 3.1× |
| 91 | 10 | 12.23 | 2.15 | 5.7× |
| 91 | 25 | 34.44 | 2.00 | 17.2× |
| 91 | 50 | 71.36 | 2.12 | 33.7× |
| 152 | 1 | 1.83 | 1.90 | ≈1× |
| 152 | 5 | 6.65 | 1.87 | 3.6× |
| 152 | 10 | 12.30 | 1.87 | 6.6× |
| 152 | 25 | 32.24 | 1.77 | 18.2× |
| 152 | 50 | 70.97 | 1.91 | 37.1× |

These measure state persistence, including journal startup and consolidation, with simulated provider completions. CLI startup and provider execution are excluded. Actual deployment speedup depends on provider latency. All 84 final checkpoints passed state-equivalence, integrity, pending-operation and journal-cleanup checks.

Traces show startup and consolidation dominate the remaining journaling time. These results do not change the benchmark qualification policy: automatic refresh journaling remains off, and the minimum stays at 10 mutations.

[Full report and trace analysis](https://github.com/ebrake/pulumi-journaling/releases/download/v3.275.0%2Bjournaling.2/benchmark-report.md) · [CSV](https://github.com/ebrake/pulumi-journaling/releases/download/v3.275.0%2Bjournaling.2/benchmark-results.csv)

## Basis and hardening

DIY journaling is based on **Jason Rowsell's prototype in [Pulumi PR #22074](https://github.com/pulumi/pulumi/pull/22074)**, hardened and extended with:

- Encrypted immutable entries, bounded asynchronous writes, and acknowledgements that wait for the entire submitted prefix to become durable
- Epoch identity, checkpoint hashes and required-feature guards that prevent stock clients from consuming an unfinished journal
- Native stack locking, crash-safe consolidation, idempotent recovery, and preservation of unexpected or corrupt state
- Consistent read-only access to the last clean checkpoint, provider-free recovery, and explicit evidence-preserving discard
- Checkpoint backups, retention and compression support, integrity reporting, and protection against completing a replaced epoch
- Conditional checkpoint replacement for GCS generations, S3 ETags Azure ETags, and exact PostgreSQL row versions, plus atomic replacement and fsync for local files
- Fault-injection, cancellation, durability-ordering, concurrent-reader, recovery and storage-driver tests; version-pinned CLI bundles and a checksummed Python launcher

See the [journaling and recovery guide](https://github.com/ebrake/pulumi-journaling/blob/main/docs/diy-journaling.md) for operational details and tradeoffs. S3-compatible services must implement conditional writes and strongly consistent reads and listings. An endpoint that rejects required preconditions fails the operation; the CLI does not fall back to unconditional checkpoint replacement.

## Development and releases

The [release configuration](https://github.com/ebrake/pulumi-journaling/blob/main/journaling-release.json) pins the upstream commit and archive checksums. The [validation workflow](https://github.com/ebrake/pulumi-journaling/blob/main/.github/workflows/journaling-test.yml) runs regression tests, linters, lifecycle tests and the actual file, GCS, S3, Azure and PostgreSQL storage drivers against isolated local emulators. The [release workflow](https://github.com/ebrake/pulumi-journaling/blob/main/.github/workflows/journaling-release.yml) builds all four platform bundles, exercises the CLI, packages the launcher with the resulting checksums, and can publish to GitHub and PyPI. When hosted runners are unavailable, its prebuilt option accepts a reviewed draft release, verifies all platform and Python-package checksums and common source provenance, then publishes through the same trusted publisher.

The upstream [contributing guide](https://github.com/ebrake/pulumi-journaling/blob/main/CONTRIBUTING.md) covers the Go monorepo. Report fork issues in [this repository](https://github.com/ebrake/pulumi-journaling/issues). See [authors](https://github.com/ebrake/pulumi-journaling/blob/main/AUTHORS.md), [notices](https://github.com/ebrake/pulumi-journaling/blob/main/NOTICE) and the [Apache 2.0 license](https://github.com/ebrake/pulumi-journaling/blob/main/LICENSE).

See [CHANGELOG-FORK.md](CHANGELOG-FORK.md) for fork releases and links to the corresponding upstream Pulumi changes.

## OSS or Pulumi Cloud?

OSS with a DIY backend is enough for individuals and teams comfortable managing storage access, backups and recovery. [Pulumi Cloud](https://www.pulumi.com/docs/iac/concepts/pulumi-cloud/) removes that operational work and adds managed collaboration, access controls, audit logs, policy enforcement and drift detection. It is a strong choice as team and governance needs grow; see [Pulumi Cloud vs. OSS](https://www.pulumi.com/docs/iac/guides/basics/pulumi-cloud-vs-oss/).
