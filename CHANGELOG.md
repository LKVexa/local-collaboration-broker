# 0.1.2a1 — 2026-09-23

- Validate bounded detached JSON envelopes and supplied metadata.
- Reject conflicting IDs; retain original sequence on identical repeats.
- Serialize transitions and bind all record/lifecycle state to integrity checks.
- Bound retention/events and reserve capacity for pending message completion.
- Add consume pagination, 41 regressions, packaging and cross-platform CI.
- Include README and Apache 2.0 LICENSE/NOTICE; wire transport/certification remain open.

# Changelog — E08 Collaboration Transport (JY-S029-P001)

## 0.1.1-partial — 2026-09-14

Maintenance/hardening release (repairs only; no API changes).
Baseline fingerprint: build-0001 product.zip
sha256 872225a10ffe27ac1ad94e25752256376703d8bda3947faa9d65e168d7dba9cc (5978 bytes), version 0.1.0-partial.

### Findings fixed (all reproduced on baseline before fixing)

- **A025-F1 — Aliasing/isolation of stored and returned mutable objects.**
  Observed: (a) mutating the caller's payload dict after `publish()` altered
  the stored record and flipped `verify()` to FAIL; (b) mutating a record
  returned by `consume()` corrupted the broker store; (c) mutating a
  `dead_letters` entry altered the channel record. Expected: broker state is
  isolated from callers/consumers. Fix: deep-copy on store in `publish()`,
  deep-copied returns from `consume()`, deep-copied dead-letter entries.
- **A025-F2 — Error-contract leaks (bare TypeError escaping).**
  Observed: non-JSON-serializable payload (e.g. a set) raised bare
  `TypeError` from `json.dumps`; unhashable/non-string `message_id` raised
  bare `TypeError` in `publish()`. Expected: documented `TransportError`.
  Fix: `_canonical()` wraps serialization errors in `TransportError`;
  `_validate()` now requires `message_id`/`channel`/`sender` to be
  non-empty strings.
- **A025-F3 — NaN / non-strict canonicalization.**
  Observed: payloads containing NaN/Infinity were accepted and digested over
  non-JSON text (`{"x": NaN}`). Expected: strict canonical JSON only.
  Fix: canonical serialization uses `allow_nan=False`; such payloads are
  rejected with `TransportError`.

### Compatibility

Fully backward compatible for all previously *valid* inputs; behavior
changes only for inputs that were previously mishandled (aliased mutation,
bare TypeError, NaN acceptance). All 11 baseline tests pass unmodified;
7 new regression tests added.

### Rollback

Restore build-0001 product.zip (sha256 above). No data-format migration
involved (broker is in-memory).
