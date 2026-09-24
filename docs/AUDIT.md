# Audit and hardening — 0.1.2a1

Date: 2026-09-23. Source: JY-S029-P001 / 0.1.1-partial / run-0001 / product.
Reviewed envelopes, duplicate identity, ordering, acknowledgments, retries,
capacity and integrity. Original source remains separate from this checkout.

## Repaired findings

- Same-ID messages with different sender/kind/payload were silently absorbed.
  Duplicate content is now checked, conflicts refuse atomically and identical
  retries return the original sequence without reopening finalized messages.
- make_envelope retained the caller payload reference; supplied digests and
  extra fields were not checked. Inputs are detached immediately, optional
  metadata verified and reserved/unknown fields refused.
- JSON validation accepted non-string keys/tuples and lacked depth/value/identity
  bounds. Strict built-in JSON types, Unicode, exact-number ranges and payload
  budgets now apply before copying/hashing.
- Publish/ack/nack could race. An RLock protects supported operations and strict
  argument validation prevents malformed transitions from changing state.
- The envelope digest excluded timestamps/sequence/metadata; integrity omitted
  duplicate indexes, pending retries, ledger and dead letters. Complete record
  hashes and a full state seal now cover these, and corrupted state blocks use.
- Public ledger/dead letters could be edited. They now return detached views.
- Retention/events were unbounded. Message/channel/byte/event caps apply, with
  reserved event capacity for accepted messages to finish via ack or terminal nack.
- Documentation overstated contiguous/exclusive delivery and ambiguous retry
  counts. consume is an ordered pending read with explicit gaps, pagination and
  redelivery/cursor limits; three total nacks finalize a message.

## Verification

18 baseline tests passed; its import-inspection fixture leaked an open file,
now replaced with UTF-8 Path reading. All 59 source and installed-wheel tests
pass, including 41 new cases for strict payloads/metadata, duplicate conflicts,
atomic rejection, paging, finalization, byte/channel/message limits, ledger
reservations, concurrency and complete record/lifecycle tampering.

Inherited behavior tests remain intact, including isolated dead-letter views.
CHECK_RUNS.json contains current results; BASELINE_CHECK_RUNS.json retains
historical evidence. CI covers Linux Python 3.10/3.12/3.14 and Windows 3.12.
No multiprocess/wire test, throughput benchmark, independent security audit,
build-tool vulnerability scan or 775-unit gate certification is claimed.

Version advanced from 0.1.1-partial to 0.1.2a1, message/integrity schemas to v2.
Added packaging, pinned-action CI, README, security guidance and Apache 2.0
LICENSE/NOTICE naming RUSSELL PHILIP SMITHSON. No third-party code is vendored
and no runtime package needed upgrading.
