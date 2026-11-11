# pulumi-journaling

An independent fork of [Pulumi](https://github.com/pulumi/pulumi), maintained by **E Brake** <ebrake123@icloud.com>, with hardened, recoverable journaling for every DIY state storage backend: local files, Amazon S3 and compatible stores, Google Cloud Storage, Azure Blob Storage, and PostgreSQL. Based on upstream **v3.274.1** at commit [`8f735e662d805b7a5bda70931bd627c69fbf38b8`](https://github.com/pulumi/pulumi/commit/8f735e662d805b7a5bda70931bd627c69fbf38b8), the latest stable release when this fork was prepared.

This project is independently maintained and is not affiliated with, sponsored by, or endorsed by Pulumi Corporation. Pulumi is a trademark of Pulumi Corporation.

## Install

```sh
python -m pip install pulumi-journaling==3.274.1.post1
pulumi-journaling version
```

The Python distribution provides both `pulumi-journaling` and `pulumi` commands, plus the matching upstream Python SDK, so existing `import pulumi` programs continue to work. On first invocation, the launcher downloads the matching CLI and language hosts from this fork's [GitHub releases](https://github.com/ebrake/pulumi-journaling/releases), verifies the SHA-256 checksum embedded in the Python package, and caches the bundle. That first invocation requires network access. Set `PULUMI_JOURNALING_CACHE` to choose the cache directory. Linux and macOS on x86-64 and ARM64 are supported; Windows packages are not currently published.

The Python launcher enables DIY journaling by default. Existing Pulumi commands and backend URLs continue to work:

```sh
pulumi login 's3://my-state-bucket'
pulumi up
```

Set `PULUMI_DIY_BACKEND_JOURNALING=false` to stop creating new journal epochs. Recovery of an unfinished epoch still runs before a mutating operation. Standalone GitHub bundles require `PULUMI_DIY_BACKEND_JOURNALING=true` to opt in. Pulumi Cloud retains its upstream service-managed persistence and journaling behavior.

## Basis and hardening

DIY journaling is based on **Jason Rowsell's prototype in [Pulumi PR #22074](https://github.com/pulumi/pulumi/pull/22074)**. E Brake hardened and extended that basis with:

- Encrypted immutable entries, bounded asynchronous writes, and acknowledgements that wait for the entire submitted prefix to become durable
- Epoch identity, checkpoint hashes and required-feature guards that prevent stock clients from consuming an unfinished journal
- Native stack locking, crash-safe consolidation, idempotent recovery, and preservation of unexpected or corrupt state
- Consistent read-only access to the last clean checkpoint, provider-free recovery, and explicit evidence-preserving discard
- Checkpoint backups, retention and compression support, integrity reporting, and protection against completing a replaced epoch
- Conditional checkpoint replacement for GCS generations, S3 ETags Azure ETags, and exact PostgreSQL row versions, plus atomic replacement and fsync for local files
- Fault-injection, cancellation, durability-ordering, concurrent-reader, recovery and storage-driver tests; version-pinned CLI bundles and a checksummed Python launcher

See the [journaling and recovery guide](https://github.com/ebrake/pulumi-journaling/blob/journaling/docs/diy-journaling.md) for operational details and tradeoffs. S3-compatible services must implement conditional writes and strongly consistent reads and listings. An endpoint that rejects required preconditions fails the operation; the CLI does not fall back to unconditional checkpoint replacement.

## Recovery

```sh
pulumi state journal recover --stack organization/project/stack
```

Recovery invokes no resource providers. If the previous process crashed, first verify it has stopped, then remove its stale native lock with `pulumi cancel`. If recovery refuses corrupt or unexplained state, preserve the evidence and consult the [recovery guide](https://github.com/ebrake/pulumi-journaling/blob/journaling/docs/diy-journaling.md) before using `pulumi state journal discard --yes`.

Complete recovery before changing to an upstream CLI or another fork. Clean snapshots and provider packages retain upstream contracts.

## Development and releases

The [release configuration](https://github.com/ebrake/pulumi-journaling/blob/journaling/journaling-release.json) pins the upstream commit and archive checksums. The [validation workflow](https://github.com/ebrake/pulumi-journaling/blob/journaling/.github/workflows/journaling-test.yml) runs regression tests, linters, lifecycle tests and the actual file, GCS, S3, Azure and PostgreSQL storage drivers against isolated local emulators. The [release workflow](https://github.com/ebrake/pulumi-journaling/blob/journaling/.github/workflows/journaling-release.yml) builds all four platform bundles, exercises the CLI, packages the launcher with the resulting checksums, and can publish to GitHub and PyPI.

The upstream [contributing guide](https://github.com/ebrake/pulumi-journaling/blob/journaling/CONTRIBUTING.md) covers the Go monorepo. Report fork issues in [this repository](https://github.com/ebrake/pulumi-journaling/issues). See [authors](https://github.com/ebrake/pulumi-journaling/blob/journaling/AUTHORS.md), [notices](https://github.com/ebrake/pulumi-journaling/blob/journaling/NOTICE) and the [Apache 2.0 license](https://github.com/ebrake/pulumi-journaling/blob/journaling/LICENSE).
