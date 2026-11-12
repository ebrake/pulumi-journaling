# pulumi-journaling changelog

Changes specific to this fork. CLI versions use `+journaling.N`; Python distributions use `.postN`.

## 3.275.0+journaling.2 — 2026-11-12

Python: `pulumi-journaling==3.275.0.post2`

- Fix recovery of flushed Rapid journal entries when an interrupted append stream reports a stale or zero metadata size
- Recover unfinished journals before state edits and refuse imports built from state that recovery has since changed
- Warn when previews show the checkpoint from before an interrupted update, and distinguish active updates from journals needing recovery or explicit discard
- Preserve older checkpoint formats during journal startup, recovery and evidence-preserving discard
- Reduce redundant checkpoint reads, journal listings and Rapid metadata and cleanup requests while retaining conditional writes and epoch validation
- Add storage-operation timing traces at verbosity 7
- Document same-zone GCS Rapid persistence benchmarks, recovery procedures and the recovery-first upgrade path for legacy non-project state

## 3.275.0+journaling.1 — 2026-11-12

Python: `pulumi-journaling==3.275.0.post1`

- Carry recoverable DIY journaling and Rapid GCS support forward to the Pulumi 3.275.0 base, with matching upstream language hosts and Python SDK
- Document enabling, disabling and recovering journals, plus the required upgrade of legacy non-project DIY state before installing this version

## 3.274.1+journaling.2 — 2026-11-11

Python: `pulumi-journaling==3.274.1.post2`

- Add explicit `gcs_api=rapid` backend support using GCS gRPC append streams, durable framed prefixes and generation-conditional checkpoint publication
- Add `PULUMI_GCS_DIRECT_PATH=required` to require direct connectivity from colocated runners
- Document Rapid storage placement, backup requirements and recovery tradeoffs

## 3.274.1+journaling.1 — 2026-11-11

Python: `pulumi-journaling==3.274.1.post1`

- Add hardened, recoverable journaling for local files, S3-compatible storage, GCS, Azure Blob Storage and PostgreSQL
- Add encrypted durable entries, guarded epochs, native stack locking, crash-safe consolidation and conditional checkpoint replacement
- Keep read-only access on the last clean checkpoint and provide provider-free recovery and evidence-preserving discard
- Support checkpoint backups, retention, compression and integrity validation
- Publish checksummed Linux and macOS CLI bundles and a Python launcher that enables DIY journaling by default

## Upstream Pulumi changes

For the Pulumi changes included in each base version, read the upstream changelog: [3.275.0](https://github.com/pulumi/pulumi/releases/tag/v3.275.0) · [3.274.1](https://github.com/pulumi/pulumi/releases/tag/v3.274.1).
