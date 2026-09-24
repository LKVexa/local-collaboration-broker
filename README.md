# Local Collaboration Broker

**0.1.2a1 — experimental partial candidate, JY-S029-P001 / E08**

A bounded in-memory queue for a trusted local coordinator: validated messages,
per-channel sequence numbers, content-aware duplicate handling, acknowledgments,
bounded nacks and dead letters. It performs no network or file I/O.

Python 3.10+; no third-party runtime dependencies.

## Use

~~~sh
python -m pip install .
python -m unittest discover -s tests -t .
~~~

~~~python
from e08.core import CollaborationBroker, make_envelope

broker = CollaborationBroker()
message = make_envelope("m-1", "planning", "composer", "event", {"revision": 1})
broker.publish(message)
batch = broker.consume("planning", limit=20)
broker.ack("planning", batch[0]["message_id"])
assert broker.verify()["verdict"] == "PASS"
~~~

## Envelopes

Required fields: message_id, channel, sender, kind and payload. Optional fields:
created_at, payload_digest and schema. Unknown fields, including caller-supplied
seq or envelope_digest, are refused with TransportError.

Identity fields are exact, case-sensitive strings, 1..128 UTF-8 bytes, with no
leading/trailing whitespace or C0/DEL controls. They are labels, not authenticated
principals or capabilities. Kinds: request, response, event, handoff.

Payloads are exact JSON objects with string keys. Nested objects/lists, strings,
booleans, null, finite floats and integers in -(2**53-1)..(2**53-1) are supported.
Tuples, bytes, custom classes, non-string keys, nonfinite numbers and invalid
Unicode are refused. The integer bound preserves exact JSON interoperability.
The library takes objects, not raw JSON text; duplicate textual JSON keys must
be handled by the caller's parser.

Limits: depth 16, 10000 visited values/keys and 64 KiB canonical JSON bytes per
payload. Canonical JSON sorts keys, uses compact separators, ASCII escapes and
strict finite numbers. Escapes count toward the byte cap. Cycles are refused.
make_envelope copies immediately; publish also validates and detaches its input.

Message schema is e08/message/v2. Supplied payload_digest must match the payload.
created_at is finite Unix seconds in 1970..9999, defaulting to the local clock;
booleans are refused. Caller timestamps are unverified, not delivery order.
Stored envelope_digest binds the complete record, including schema, timestamp,
payload digest and assigned sequence. It is an unsigned consistency hash.

## Ordering and idempotency

First publication assigns the next contiguous sequence number in its channel.
The duplicate identity is channel + message_id; sender, kind and canonical payload
must match the original. Conflicting reuse raises TransportError without changing
state or consuming a sequence number.

An identical repeat is ledgered and returns duplicate=true with the original seq.
created_at is deliberately excluded from duplicate content comparison, so retries
can use a newly created envelope. The originally stored timestamp/record remain.
Equal-looking numeric types such as 1 and 1.0 have distinct canonical encodings.
IDs are scoped to a channel. Repeats after ack/dead-letter do not reopen delivery.

consume(channel, after_seq=0, limit=100) returns detached pending records in
sequence order, up to limit (1..1000). after_seq is a nonnegative exact integer.
Finalized messages leave sequence gaps in the returned list; unknown channels
return an empty list.

consume is a read, not an exclusive lease/claim, and does not record a delivery
attempt. Repeated/concurrent consumers can see the same pending message.
The coordinator must handle that repetition and decide who can ack/nack.
An after_seq cursor can skip older still-pending retries; scan from zero or track
unfinished messages separately when that matters. No exactly-once processing,
consumer identity, visibility timeout or delivery authentication is provided.

## Acknowledgments, nacks and capacity

ack finalizes a pending message. Double/stale/unknown ack/nack calls fail.
A nack increments its failure count. The third total nack moves the message
to a dead-letter entry and removes it from pending delivery. MAX_RETRIES=3 means
three total nacks, not an initial failure plus three further retries.

Reasons are nonblank valid Unicode, at most 1024 UTF-8 bytes. Dead letters retain
a detached full message, final reason, exhausted count and digest. Earlier nack
reasons remain in the lifecycle ledger. There is no automatic retry schedule,
worker execution, requeue API, deletion or persistence.

Per broker: 1000 retained messages, 64 channels, 8 MiB retained canonical payloads,
10000 lifecycle events. Acked/dead-lettered messages and IDs remain retained for
ordering/idempotency, so finalizing does not reclaim message capacity. Start a new
explicit broker lifecycle when its retention capacity is reached.

Every accepted new message reserves enough event slots for its remaining nacks
and terminal dead-letter event. Duplicate/new publication is refused before it
can consume those reserved slots. Ack frees unused reservation; pending messages
can still finish when ordinary event capacity is exhausted. Rejected calls do
not mutate state and are not a comprehensive refusal audit log.

These are normal-workload bounds, not OS memory/time quotas. Copies, JSON
serialization, indexes and dead-letter payload copies add memory overhead.

## Integrity and concurrency

An RLock serializes supported operations. ledger and dead_letters return detached
views, as does consume. Lifecycle events have contiguous event_seq values.
Payload/record checks and a state digest cover records, sequences, duplicate
index, pending retry state, dead letters, ledger and byte/message counters.
verify returns e08/integrity/v2 with PASS/FAIL and problems. Corrupt state prevents
publish/consume/ack/nack via IntegrityError (a TransportError subclass).

The implementation re-hashes retained state when verifying/mutating. Cost grows
with retained messages and events; this is a bounded prototype, not a throughput
optimized service. Thread safety does not provide multiprocess coordination.

Hashes are unkeyed and can be recomputed by a process owner. They do not authenticate
sender labels, prove processing, make timestamps trustworthy or provide a durable
append-only audit trail. Payloads and reasons may be sensitive; the caller controls
authorization, logging, retention and any future wire transport.

## Verification and migration

59 tests: 18 inherited plus 41 new regressions, including concurrent sequence and
duplicate handling, racing ack, strict JSON/type budgets, conflicting IDs,
complete record/state tampering and reserved terminal event capacity.
Source and installed-wheel evidence: [CHECK_RUNS](docs/CHECK_RUNS.json).
CI covers Linux Python 3.10/3.12/3.14 and Windows 3.12. See [AUDIT](docs/AUDIT.md).

0.1.1-partial -> 0.1.2a1 adds message schema v2, complete record digests, stricter
inputs and capacities, content-aware duplicate rejection, original seq on duplicate,
consume pagination and detached public views. Update callers that mutated public
lists or assumed an unlimited consume response. Regenerate envelopes/records;
old/new digests are not interchangeable.

Wire protocols, durable queues, authentication/capability enforcement and the
original 775-unit certification program remain outside this partial candidate.

## License

Copyright 2026 **RUSSELL PHILIP SMITHSON**.
[Apache License 2.0](LICENSE), with [NOTICE](NOTICE).
No third-party source is vendored; see [THIRD-PARTY-NOTICES](THIRD-PARTY-NOTICES.md).
