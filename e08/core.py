"""Bounded in-memory messages for a trusted local coordinator; no wire transport."""
from __future__ import annotations

import copy
import hashlib
import json
import math
import threading
import time

VERSION = "0.1.2a1"
MAX_RETRIES = 3  # Three total nacks, not three retries after an initial nack.
MAX_PAYLOAD_BYTES = 65536
MAX_MESSAGES = 1000
MAX_CHANNELS = 64
MAX_STORED_BYTES = 8388608
MAX_EVENTS = 10000
REQUIRED_FIELDS = frozenset(("message_id", "channel", "sender", "kind", "payload"))
KINDS = ("request", "response", "event", "handoff")


class TransportError(Exception):
    pass


class IntegrityError(TransportError):
    pass


def _canonical(obj):
    try:
        return json.dumps(obj, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=True, allow_nan=False).encode()
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise TransportError("value is not canonical JSON") from exc


def _digest(obj):
    return "sha256:" + hashlib.sha256(_canonical(obj)).hexdigest()


def _label(value, field):
    if (type(value) is not str or not value or value != value.strip()
            or len(value) > 128 or any(ord(c) < 32 or ord(c) == 127 for c in value)):
        raise TransportError(field + " must be a nonblank, control-free identity")
    try:
        if len(value.encode()) > 128:
            raise TransportError(field + " exceeds 128 UTF-8 bytes")
    except UnicodeError as exc:
        raise TransportError(field + " is invalid Unicode") from exc
    return value


def _payload(value):
    if type(value) is not dict:
        raise TransportError("payload must be an exact JSON object")
    stack = [(value, 0)]
    count = text_bytes = 0
    while stack:
        item, depth = stack.pop()
        count += 1
        if depth > 16 or count > 10000:
            raise TransportError("payload exceeds depth/value budget or contains a cycle")
        kind = type(item)
        if kind is dict:
            if len(item) > 10000 or any(type(k) is not str for k in item):
                raise TransportError("JSON object keys must be strings")
            stack.extend((v, depth + 1) for v in item.values())
            stack.extend((k, depth + 1) for k in item)
        elif kind is list:
            if len(item) > 10000:
                raise TransportError("payload exceeds value budget")
            stack.extend((v, depth + 1) for v in item)
        elif kind is str:
            if len(item) > MAX_PAYLOAD_BYTES:
                raise TransportError("payload string exceeds byte budget")
            try:
                text_bytes += len(item.encode())
            except UnicodeError as exc:
                raise TransportError("payload contains invalid Unicode") from exc
            if text_bytes > MAX_PAYLOAD_BYTES:
                raise TransportError("payload exceeds byte budget")
        elif kind is int:
            if abs(item) > 2**53 - 1:
                raise TransportError("payload integers must fit exact JSON number range")
        elif kind is float:
            if not math.isfinite(item):
                raise TransportError("payload floats must be finite")
        elif item is not None and kind is not bool:
            raise TransportError("payload contains a non-JSON type")
    encoded = _canonical(value)
    if len(encoded) > MAX_PAYLOAD_BYTES:
        raise TransportError("canonical payload exceeds 64 KiB")
    return copy.deepcopy(value), len(encoded)


def _timestamp(value):
    if (type(value) not in (int, float) or not 0 <= value <= 253402300799
            or not math.isfinite(value)):
        raise TransportError("created_at must be finite Unix seconds in year 1970..9999")
    return value


def _validate(env):
    optional = {"created_at", "payload_digest", "schema"}
    if type(env) is not dict or not REQUIRED_FIELDS <= env.keys() or env.keys() - REQUIRED_FIELDS - optional:
        raise TransportError("invalid envelope fields")
    clean = {key: _label(env[key], key) for key in ("message_id", "channel", "sender")}
    if type(env["kind"]) is not str or env["kind"] not in KINDS:
        raise TransportError("kind must be request, response, event or handoff")
    clean["kind"] = env["kind"]
    clean["payload"], size = _payload(env["payload"])
    clean["payload_digest"] = _digest(clean["payload"])
    if "payload_digest" in env and (type(env["payload_digest"]) is not str
                                    or env["payload_digest"] != clean["payload_digest"]):
        raise TransportError("payload digest mismatch")
    if type(env.get("schema", "e08/message/v2")) is not str or env.get("schema", "e08/message/v2") != "e08/message/v2":
        raise TransportError("unsupported message schema")
    clean["schema"] = "e08/message/v2"
    clean["created_at"] = _timestamp(env["created_at"] if "created_at" in env else time.time())
    return clean, size


def make_envelope(message_id, channel, sender, kind, payload):
    """Copy and validate caller data immediately; later edits cannot alter it."""
    return _validate({"message_id": message_id, "channel": channel, "sender": sender,
                      "kind": kind, "payload": payload})[0]


def _identity(env):
    return _digest({key: env[key] for key in REQUIRED_FIELDS})


def _record_digest(record):
    return _digest({key: value for key, value in record.items() if key != "envelope_digest"})


class CollaborationBroker:
    """Thread-safe local pending-message queue, without leases/authentication.

    consume is a detached read, not an exclusive claim. A trusted coordinator
    must arrange who may ack/nack and handle repeated delivery.
    """
    def __init__(self):
        self._lock = threading.RLock()
        self._channels = {}
        self._seen_ids = {}
        self._seq = {}
        self._pending_acks = {}
        self._dead_letters = []
        self._ledger = []
        self._stored_bytes = 0
        self._message_count = 0
        self._seal = self._state_hash()

    @property
    def ledger(self):
        with self._lock:
            return copy.deepcopy(self._ledger)

    @property
    def dead_letters(self):
        with self._lock:
            return copy.deepcopy(self._dead_letters)

    def _state_hash(self):
        return _digest({
            "channels": self._channels, "seen_ids": self._seen_ids, "seq": self._seq,
            "pending": [[ch, mid, state] for (ch, mid), state in sorted(self._pending_acks.items())],
            "dead_letters": self._dead_letters, "ledger": self._ledger,
            "stored_bytes": self._stored_bytes, "message_count": self._message_count,
        })

    def _problems(self):
        problems = []
        try:
            for channel, records in self._channels.items():
                if [m["seq"] for m in records] != list(range(1, len(records) + 1)):
                    problems.append({"channel": channel, "issue": "sequence gap or reorder"})
                for record in records:
                    if (_record_digest(record) != record["envelope_digest"]
                            or _digest(record["payload"]) != record["payload_digest"]):
                        problems.append({"channel": channel, "issue": "envelope tampered",
                                         "message_id": record["message_id"]})
            if self._state_hash() != self._seal:
                problems.append({"issue": "broker state tampered"})
        except (TransportError, TypeError, ValueError, KeyError, AttributeError):
            problems.append({"issue": "malformed broker state"})
        return problems

    def _require_integrity(self):
        if self._problems():
            raise IntegrityError("broker integrity failed")

    def _reserved(self):
        # Enough future events for every remaining nack plus its terminal DLQ event.
        return sum(MAX_RETRIES - state["retries"] + 1 for state in self._pending_acks.values())

    def _capacity(self, events, extra_reserved=0):
        if len(self._ledger) + self._reserved() + events + extra_reserved > MAX_EVENTS:
            raise TransportError("ledger capacity reserved for pending message completion")

    def _event(self, op, channel, message_id, **fields):
        self._ledger.append({"event_seq": len(self._ledger) + 1, "op": op,
                             "channel": channel, "message_id": message_id, **fields})

    def publish(self, env):
        clean, size = _validate(env)
        channel, mid = clean["channel"], clean["message_id"]
        identity = _identity(clean)
        with self._lock:
            self._require_integrity()
            prior = self._seen_ids.get(channel, {}).get(mid)
            if prior is not None:
                if prior["identity_digest"] != identity:
                    raise TransportError("conflicting reuse of message ID in this channel")
                self._capacity(1)
                self._event("duplicate_absorbed", channel, mid, seq=prior["seq"])
                self._seal = self._state_hash()
                return {"accepted": True, "duplicate": True, "seq": prior["seq"]}
            if self._message_count >= MAX_MESSAGES or self._stored_bytes + size > MAX_STORED_BYTES:
                raise TransportError("retained message capacity exceeded")
            if channel not in self._channels and len(self._channels) >= MAX_CHANNELS:
                raise TransportError("channel capacity exceeded")
            self._capacity(1, MAX_RETRIES + 1)
            seq = self._seq.get(channel, 0) + 1
            record = dict(clean, seq=seq)
            record["envelope_digest"] = _record_digest(record)
            self._channels.setdefault(channel, []).append(record)
            self._seen_ids.setdefault(channel, {})[mid] = {"seq": seq, "identity_digest": identity}
            self._seq[channel] = seq
            self._pending_acks[(channel, mid)] = {"retries": 0}
            self._stored_bytes += size
            self._message_count += 1
            self._event("published", channel, mid, seq=seq, envelope_digest=record["envelope_digest"])
            self._seal = self._state_hash()
            return {"accepted": True, "duplicate": False, "seq": seq}

    def consume(self, channel, after_seq=0, limit=100):
        """Read ordered pending records; acked/dead-lettered sequence gaps remain."""
        channel = _label(channel, "channel")
        if type(after_seq) is not int or not 0 <= after_seq <= 2**53 - 1:
            raise TransportError("after_seq must be a nonnegative exact integer")
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise TransportError("limit must be an integer in 1..1000")
        with self._lock:
            self._require_integrity()
            result = []
            for record in self._channels.get(channel, []):
                if record["seq"] > after_seq and (channel, record["message_id"]) in self._pending_acks:
                    result.append(copy.deepcopy(record))
                    if len(result) == limit:
                        break
            return result

    def ack(self, channel, message_id):
        channel, message_id = _label(channel, "channel"), _label(message_id, "message_id")
        with self._lock:
            self._require_integrity()
            key = (channel, message_id)
            if key not in self._pending_acks:
                raise TransportError("ack for unknown or already-finalized message")
            del self._pending_acks[key]
            self._event("acked", channel, message_id)
            self._seal = self._state_hash()

    def nack(self, channel, message_id, reason):
        channel, message_id = _label(channel, "channel"), _label(message_id, "message_id")
        if type(reason) is not str or not reason.strip() or len(reason) > 1024:
            raise TransportError("reason must be nonblank text up to 1024 UTF-8 bytes")
        try:
            if len(reason.encode()) > 1024:
                raise TransportError("reason exceeds 1024 UTF-8 bytes")
        except UnicodeError as exc:
            raise TransportError("reason contains invalid Unicode") from exc
        with self._lock:
            self._require_integrity()
            key = (channel, message_id)
            if key not in self._pending_acks:
                raise TransportError("nack for unknown or already-finalized message")
            state = self._pending_acks[key]
            retries = state["retries"] + 1
            state["retries"] = retries
            self._event("nacked", channel, message_id, retry=retries, reason=reason)
            terminal = retries == MAX_RETRIES
            if terminal:
                del self._pending_acks[key]
                seq = self._seen_ids[channel][message_id]["seq"]
                record = self._channels[channel][seq - 1]
                entry = {"message": copy.deepcopy(record), "reason": reason,
                         "retries_exhausted": retries}
                entry["dead_letter_digest"] = _digest(entry)
                self._dead_letters.append(entry)
                self._event("dead_lettered", channel, message_id, seq=seq,
                            dead_letter_digest=entry["dead_letter_digest"])
            self._seal = self._state_hash()
            return {"dead_lettered": terminal, "retries": retries}

    def verify(self):
        with self._lock:
            problems = self._problems()
            return {"schema": "e08/integrity/v2", "channels": len(self._channels),
                    "messages": self._message_count, "pending_acks": len(self._pending_acks),
                    "dead_letters": len(self._dead_letters), "events": len(self._ledger),
                    "stored_payload_bytes": self._stored_bytes,
                    "state_digest": self._seal, "problems": problems,
                    "verdict": "FAIL" if problems else "PASS"}
