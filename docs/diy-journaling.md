<!-- Modified by E Brake for pulumi-journaling. -->

# DIY backend journaling

Journaling is enabled with `PULUMI_DIY_BACKEND_JOURNALING=true` for every DIY
backend: local files, GCS, S3 Azure Blob Storage, and PostgreSQL. The Python launcher enables
it by default; set the variable to `false` to opt out of new epochs. It uses Pulumi's encrypted journal serializer and replay
engine. The implementation is based on Jason Rowsell's prototype in
[Pulumi PR #22074](https://github.com/pulumi/pulumi/pull/22074), hardened and
extended by E Brake with all the durability, recovery, integrity, concurrency
and storage improvements described here.

Each stack has an immutable manifest containing identity and checkpoint
hashes under `.pulumi/journal/`. Epoch objects hold the starting checkpoint,
ordered entries and prepared clean checkpoint. Before provider operations,
the checkpoint receives the temporary required feature `pulumiJournalingV1`.
Stock clients reject this feature. Before migrating from another journaling
build, recover its unfinished epochs using the CLI that created them. This fork
accepts clean upstream checkpoints and writes v2 journal manifests for ordinary
backends. Rapid uses v3 manifests with a framed append-stream journal; recovery
accepts both versions.

Mutating operations recover under the native stack lock before loading state,
independently of the enablement flag. Read-only operations, including previews,
stack listings, outputs and StackReference, see the last clean checkpoint until
consolidation. Ordinary reads of unguarded checkpoints incur no journal request.
Export requires recovery so repair workflows cannot silently export stale state.
Use `pulumi state journal recover --stack <stack>` without invoking providers,
including outside a project directory. If a process crashes, verify it has
stopped before removing its native lock with `pulumi cancel`.

Consolidation prepares clean bytes once and conditionally replaces the guarded
checkpoint. Recovery accepts only the starting, guarded or prepared clean hash.
GCS reads capture the object generation and writes enforce it with upload CRC32C
validation. S3 and Azure capture ETags from the same read as the checkpoint bytes
and require matching ETags on replacement, including multipart or block uploads.
PostgreSQL uses atomic insert-if-absent for journal objects and exact row-value
comparison when replacing checkpoints. Local files use atomic replacement and
fsync under the stack lock. S3-compatible
services must honor conditional writes and strongly consistent reads and listings.
Cleanup retains base and clean objects until the manifest is deleted, so an
interrupted cleanup remains recoverable; a crash after deletion can leave inert
epoch artifacts. Backups contain the pre-update checkpoint, and checkpoint
retention also records each consolidated checkpoint when enabled. Each epoch
transfers a fixed number of full checkpoints; journal entries also include unchanged-resource bookkeeping. For a single change in a large state, this fixed cost
can exceed one legacy checkpoint write.

Required journal entries return only when every previously submitted entry is
durable. Elided writes remain asynchronous; close reports all persistence errors.
Recovery replays the contiguous submitted prefix and ignores the unacknowledged
tail after a gap. Sequence IDs still determine replay order. An integrity failure
is recorded in checkpoint metadata and reported after the checkpoint is committed
and the journal cleared; `--disable-integrity-checking` suppresses that error.
Finalization is bound to the run's epoch and errors if another process removed or
replaced it. Compression remains fixed within an epoch.

When recovery refuses because of corrupt records or unexplained state, preserve
the evidence. An operator can run `pulumi state journal discard --stack <stack>
--yes`, which archives the epoch under the stack backup directory, removes only
the temporary feature from current state, and clears the journal. State may be
missing interrupted changes: run `pulumi refresh`, or import a known-good stack
export. Hash mismatch alone never authorizes automatic deletion. Pending provider
operations still require normal Pulumi reconciliation.

Disable journaling and complete recovery before downgrading. Provider packages,
language SDKs and clean snapshot formats retain upstream contracts. Pulumi Cloud
keeps upstream service-managed journaling; the DIY flag does not change its
persistence behavior. Linux and macOS are the published CLI platforms.


## Rapid GCS transport

GCS backends with `?gcs_api=rapid` use bidirectional gRPC uploads and finalized
objects for checkpoints and metadata. Ordinary `gs://` backends retain the existing HTTP transport. The Rapid
transport preserves generation-match writes, CRC32C validation, stack locks,
opaque encrypted state, and the current journal format. Writes stage outside state paths and publish through conditional atomic rename, so
unfinished streams cannot replace checkpoints or enter the recovery prefix.
Checkpoint backup copies use a client-side read and upload because Rapid does not
support object rewrites.
Completed writes finalize their objects before returning; unfinished upload bytes
must not be accepted as durable journal records.

Rapid requires a zonal bucket with hierarchical namespace and uniform access.
It does not support object versioning or soft delete. Verify a separately protected
encrypted checkpoint archive before each cutover, and retain the source bucket.
Lifecycle deletion may target dated history, old backup copies, or abandoned
staged uploads. Current checkpoints, metadata, active journals, native locks, and
cutover archives must be excluded from age-based deletion. Run
performance and crash-recovery canaries from compute in the bucket's zone; a
storage change does not remove geographic latency from a remote CLI.


Rapid journal epochs keep one bidirectional append writer open. Required entries
wait for a flush acknowledging their complete framed prefix; elided entries may
share the next flush. The stream is finalized only when the journal closes.
Frames carry a length, a header CRC32C, and a payload CRC32C; recovery reads the persisted size, accepts only
complete validated frames, and ignores an incomplete tail. A corrupt complete
frame preserves the journal and stops recovery. No age rule may delete an active
stream. Checkpoints and manifests still use staged, finalized atomic publication.
Transport validation checks gRPC direct connectivity from the bucket's zone;
backend transport selection must not silently fall back to JSON or XML writes.

Set `PULUMI_GCS_DIRECT_PATH=required` on colocated runners to reject a Rapid
connection that cannot use direct connectivity. Leave it unset for remote
operator CLIs; they still use the gRPC transport selected by the backend URL.
