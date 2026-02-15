# Shared Memory Backend (Writer/Reader) Design

Date: 2026-02-16
Status: Draft (approved in chat)

## Goals
- Provide a high-performance, shared-memory backend for HeTu on a single machine.
- Preserve cross-process transaction isolation using existing optimistic row versions.
- Replace Redis pubsub with local notifications for subscriptions.
- Durability requirement: data should persist via mmap flush; the last in-flight commit may be lost on crash.

## Non-Goals
- Cross-machine scalability.
- Strong durability guarantees beyond the last commit (no WAL required).
- Full multi-writer concurrency inside a single component table.

## Approach Summary
Use a single Writer process to serialize all writes and index/unique maintenance, and multiple Reader processes to serve reads directly from shared memory. All writes go through IPC to the Writer. Readers map shared memory into NumPy arrays for zero-copy queries and subscription pushes.

## Architecture
- Writer process:
  - Owns write access for all components.
  - Applies transactions, updates row versions, maintains index/unique structures.
  - Performs mmap flush on commit boundary (or periodic batch commit).
  - Publishes change events to Readers.
- Reader processes:
  - Read-only mapping of shared memory regions.
  - Execute get/range using index view and direct row slicing.
  - Handle subscriptions and push updates to clients.

## Shared Memory Layout
For each component table:
- Row region: fixed-size structured array (np.dtype-backed), with row id and version fields.
- Index region: serialized or in-memory index view (sorted array or hash buckets).
- Metadata region:
  - schema hash
  - row count, capacity, free list
  - data_epoch, index_epoch
  - offsets/sizes for row/index regions

## Epoch Strategy
- data_epoch: optional, low-frequency bump for batched visibility notifications.
- index_epoch: must bump whenever index/unique structures change.
- Reader behavior: if index_epoch changes during query, retry once.

## Read Path
- Reader checks index_epoch before query.
- Locate row ids through index region.
- Slice row region directly and return np.record or recarray.
- If index_epoch changes mid-query, retry for consistency.

## Write Path
- All writes sent to Writer via IPC/RPC.
- Writer updates:
  - row data
  - index/unique structures
  - row version
- Commit:
  - update metadata epochs (as needed)
  - mmap flush to persist
- Crash semantics:
  - if commit not finished or not flushed, latest commit can be lost
  - readers treat epochs as visibility boundary

## Index and Unique Constraints
- Implemented only in Writer to avoid cross-process mutation complexity.
- Readers use read-only index view for query routing.

## Resizing
- On row/index expansion, Writer allocates new shared memory segment.
- Writer publishes a remap event.
- Readers detach old mapping and re-map new segment, then resume.

## Subscription and Notification
- Writer sends change events via IPC broadcast or shared ring buffer.
- Events include:
  - component name
  - index/key (optional)
  - affected row ids
  - data_epoch (optional)
- Readers use the event to refresh in-memory views and push updates to clients.

## Compatibility with Existing Code
- Keep Session/SessionRepository API surface.
- Implement a new BackendClient that routes writes to Writer and reads to shared memory.
- Reuse optimistic row version checks for conflict detection.

## Testing Strategy
- Concurrency:
  - multi-process write contention on the same row, ensure conflict handling
  - index/unique conflict consistency with Redis behavior
- Crash recovery:
  - kill Writer mid-commit and validate last commit loss without corruption
- Resize:
  - expand row/index region while Readers are active; verify remap safety
- Performance:
  - get/range latency from Reader
  - write throughput (IPC + flush)

## Open Questions
- IPC mechanism choice (UNIX domain socket, named pipe, shared ring buffer).
- Index structure format (sorted array vs hash buckets) and update frequency.
- Batching policy for data_epoch updates.
