# Security boundaries

This is a trusted local coordinator's in-memory queue, with no network or file
I/O. Identity/channel labels are not authenticated. Any caller with the broker
object can consume, ack or nack. There are no leases, consumer capabilities,
visibility timeouts or exactly-once processing guarantees.

Hashes detect consistency changes but do not protect against a process owner
rewriting private state and hashes. Public outputs are detached to prevent
accidental edits. No durable or externally anchored audit trail is provided.

Payloads, identities and nack reasons remain retained, including after finalization.
They may contain secrets. The caller controls authorization, retention and any
external transmission. Never execute message payloads as instructions merely
because they pass schema/integrity checks.

Capacities bound normal workloads, not process memory/time. State hashing/copying
cost grows with retained data. Repeated consume can redeliver the same message,
and an after_seq cursor can skip an older pending retry. Coordinate processing
and retry state explicitly. No hostile-process isolation, third-party runtime
upgrade, build-tool vulnerability scan or original program certification is claimed.
