from concurrent.futures import ThreadPoolExecutor
import copy
import math
import unittest
from unittest.mock import patch

import e08.core as core
from e08.core import CollaborationBroker, TransportError, IntegrityError, make_envelope, _digest


def env(mid="m", payload=None, **changes):
    result = {"message_id": mid, "channel": "c", "sender": "s", "kind": "event",
              "payload": {} if payload is None else payload}
    result.update(changes)
    return result


class Hardening(unittest.TestCase):
    def test_malformed_envelope(self):
        b = CollaborationBroker()
        for value in (None, [], "x", 1, {}, dict(env(), extra="value")):
            with self.assertRaises(TransportError):
                b.publish(value)
        self.assertEqual(b.verify()["messages"], 0)

    def test_identity_text_bounds(self):
        for value in (" ", " leading", "trailing ", "x\nx", "\ud800", "x"*129, "界"*50):
            with self.assertRaises(TransportError):
                make_envelope(value, "c", "s", "event", {})

    def test_non_string_keys(self):
        for payload in ({1: "x"}, {"nested": {1: "x"}}):
            with self.assertRaises(TransportError):
                make_envelope("m", "c", "s", "event", payload)

    def test_json_types_exact(self):
        class Integer(int):
            pass
        for value in ((1, 2), b"x", bytearray(b"x"), Integer(1), object()):
            with self.assertRaises(TransportError):
                make_envelope("m", "c", "s", "event", {"x": value})

    def test_cycles_and_depth(self):
        cyclic = {}
        cyclic["self"] = cyclic
        deep = {}
        at = deep
        for _ in range(20):
            at["x"] = {}
            at = at["x"]
        for payload in (cyclic, deep):
            with self.assertRaises(TransportError):
                make_envelope("m", "c", "s", "event", payload)

    def test_value_budget(self):
        with self.assertRaises(TransportError):
            make_envelope("m", "c", "s", "event", {"x": [None]*10000})

    def test_numeric_bounds(self):
        for value in (2**53, -(2**53), 10**1000, math.inf, math.nan):
            with self.assertRaises(TransportError):
                make_envelope("m", "c", "s", "event", {"x": value})
        self.assertEqual(make_envelope("m", "c", "s", "event", {"x": True})["payload"]["x"], True)

    def test_payload_unicode(self):
        with self.assertRaises(TransportError):
            make_envelope("m", "c", "s", "event", {"x": "\ud800"})
        self.assertEqual(make_envelope("m", "c", "s", "event", {"x": "é\n"})["payload"]["x"], "é\n")

    def test_canonical_byte_cap_includes_escaping(self):
        with self.assertRaises(TransportError):
            make_envelope("m", "c", "s", "event", {"x": "\0"*12000})

    def test_make_envelope_detaches_immediately(self):
        payload = {"nested": [1]}
        e = make_envelope("m", "c", "s", "event", payload)
        payload["nested"][0] = 9
        self.assertEqual(e["payload"], {"nested": [1]})
        self.assertEqual(e["payload_digest"], _digest(e["payload"]))

    def test_supplied_digest_verified(self):
        b = CollaborationBroker()
        e = make_envelope("m", "c", "s", "event", {})
        e["payload"] = {"changed": 1}
        with self.assertRaises(TransportError):
            b.publish(e)
        self.assertEqual(b.publish(env())["seq"], 1)

    def test_schema_and_timestamp_validation(self):
        b = CollaborationBroker()
        for field, value in (("schema", "old"), ("schema", []), ("created_at", True),
                             ("created_at", math.nan), ("created_at", -1),
                             ("created_at", 10**1000), ("payload_digest", {})):
            with self.assertRaises(TransportError):
                b.publish(env(**{field: value}))

    def test_reserved_record_fields_refused(self):
        b = CollaborationBroker()
        for field in ("seq", "envelope_digest", "retries", "arbitrary"):
            with self.assertRaises(TransportError):
                b.publish(env(**{field: 1}))

    def test_conflicting_duplicate_refused(self):
        for changes in ({"sender": "other"}, {"kind": "request"}, {"payload": {"different": 1}}):
            b = CollaborationBroker()
            b.publish(env())
            before = b.verify()
            with self.assertRaisesRegex(TransportError, "conflicting"):
                b.publish(env(**changes))
            self.assertEqual(before, b.verify())
            self.assertEqual(b.publish(env("second"))["seq"], 2)

    def test_identical_duplicate_returns_original_sequence(self):
        b = CollaborationBroker()
        b.publish(env(created_at=1))
        result = b.publish(env(created_at=2))
        self.assertEqual(result, {"accepted": True, "duplicate": True, "seq": 1})
        self.assertEqual(b.consume("c")[0]["created_at"], 1)

    def test_duplicate_after_ack_does_not_reopen(self):
        b = CollaborationBroker()
        b.publish(env())
        b.ack("c", "m")
        self.assertTrue(b.publish(env())["duplicate"])
        self.assertEqual(b.consume("c"), [])

    def test_duplicate_after_dead_letter_does_not_reopen(self):
        b = CollaborationBroker()
        b.publish(env())
        for _ in range(core.MAX_RETRIES):
            b.nack("c", "m", "failure")
        self.assertTrue(b.publish(env())["duplicate"])
        self.assertEqual(b.consume("c"), [])
        self.assertEqual(len(b.dead_letters), 1)

    def test_ids_scoped_to_channel(self):
        b = CollaborationBroker()
        self.assertEqual(b.publish(env())["seq"], 1)
        self.assertEqual(b.publish(env(channel="other", payload={"x": 2}))["seq"], 1)

    def test_number_types_not_duplicate(self):
        b = CollaborationBroker()
        b.publish(env(payload={"x": 1}))
        with self.assertRaises(TransportError):
            b.publish(env(payload={"x": 1.0}))

    def test_consume_strict_arguments(self):
        b = CollaborationBroker()
        for after in (-1, True, 1.5, "1"):
            with self.assertRaises(TransportError):
                b.consume("c", after_seq=after)
        for limit in (0, 1001, True, 1.0):
            with self.assertRaises(TransportError):
                b.consume("c", limit=limit)
        for channel in (None, [], ""):
            with self.assertRaises(TransportError):
                b.consume(channel)

    def test_consume_order_with_finalized_gaps_and_limit(self):
        b = CollaborationBroker()
        for i in range(1, 5):
            b.publish(env(str(i)))
        b.ack("c", "2")
        self.assertEqual([e["seq"] for e in b.consume("c", limit=2)], [1, 3])
        self.assertEqual([e["seq"] for e in b.consume("c", after_seq=3)], [4])

    def test_consume_is_not_exclusive_claim(self):
        b = CollaborationBroker()
        b.publish(env())
        self.assertEqual(b.consume("c"), b.consume("c"))
        self.assertEqual(len(b.ledger), 1)

    def test_ack_and_nack_arguments(self):
        b = CollaborationBroker()
        b.publish(env())
        for channel, mid in (([], "m"), ("c", {}), ("other", "m")):
            with self.assertRaises(TransportError):
                b.ack(channel, mid)
        self.assertEqual(len(b.consume("c")), 1)

    def test_bad_nack_reason_is_atomic(self):
        b = CollaborationBroker()
        b.publish(env())
        before = b.verify()
        for reason in (None, "", " ", [], "x"*1025, "界"*400, "\ud800"):
            with self.assertRaises(TransportError):
                b.nack("c", "m", reason)
        self.assertEqual(before, b.verify())

    def test_message_capacity_rejection_is_atomic(self):
        b = CollaborationBroker()
        with patch("e08.core.MAX_MESSAGES", 1):
            b.publish(env())
            before = b.verify()
            with self.assertRaises(TransportError):
                b.publish(env("new"))
            self.assertEqual(before, b.verify())
        self.assertEqual(b.publish(env("new"))["seq"], 2)

    def test_channel_capacity(self):
        b = CollaborationBroker()
        with patch("e08.core.MAX_CHANNELS", 1):
            b.publish(env())
            with self.assertRaises(TransportError):
                b.publish(env(channel="other"))
        self.assertEqual(b.verify()["channels"], 1)

    def test_stored_byte_capacity(self):
        b = CollaborationBroker()
        with patch("e08.core.MAX_STORED_BYTES", 3):
            b.publish(env())  # {} uses two bytes.
            with self.assertRaises(TransportError):
                b.publish(env("other"))
        self.assertEqual(b.verify()["stored_payload_bytes"], 2)

    def test_capacity_retains_finalized_ids(self):
        b = CollaborationBroker()
        with patch("e08.core.MAX_MESSAGES", 1):
            b.publish(env())
            b.ack("c", "m")
            self.assertTrue(b.publish(env())["duplicate"])
            with self.assertRaises(TransportError):
                b.publish(env("other"))

    def test_ledger_reserves_terminal_nacks(self):
        b = CollaborationBroker()
        with patch("e08.core.MAX_EVENTS", 5):
            b.publish(env())
            with self.assertRaises(TransportError):
                b.publish(env())
            with self.assertRaises(TransportError):
                b.publish(env("new"))
            for _ in range(core.MAX_RETRIES):
                result = b.nack("c", "m", "failed")
            self.assertTrue(result["dead_lettered"])
            self.assertEqual(len(b.ledger), 5)
        self.assertEqual(b.verify()["verdict"], "PASS")

    def test_ack_frees_reserved_event_slots(self):
        b = CollaborationBroker()
        with patch("e08.core.MAX_EVENTS", 7):
            b.publish(env())
            b.ack("c", "m")
            self.assertEqual(b.publish(env("new"))["seq"], 2)
            b.ack("c", "new")
        self.assertEqual(b.verify()["pending_acks"], 0)

    def test_duplicate_flood_cannot_block_ack(self):
        b = CollaborationBroker()
        with patch("e08.core.MAX_EVENTS", 8):
            b.publish(env())
            for _ in range(3):
                b.publish(env())
            with self.assertRaises(TransportError):
                b.publish(env())
            b.ack("c", "m")
        self.assertEqual(b.consume("c"), [])

    def test_concurrent_unique_sequences(self):
        b = CollaborationBroker()
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda i: b.publish(env(str(i)))["seq"], range(50)))
        self.assertEqual(sorted(results), list(range(1, 51)))
        self.assertEqual(b.verify()["verdict"], "PASS")

    def test_concurrent_duplicate_absorption(self):
        b = CollaborationBroker()
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: b.publish(env()), range(30)))
        self.assertEqual(sum(not r["duplicate"] for r in results), 1)
        self.assertEqual(len(b.consume("c")), 1)

    def test_racing_ack_single_transition(self):
        b = CollaborationBroker()
        b.publish(env())
        def ack(_):
            try:
                b.ack("c", "m")
                return True
            except TransportError:
                return False
        with ThreadPoolExecutor(max_workers=8) as pool:
            self.assertEqual(sum(pool.map(ack, range(20))), 1)
        self.assertEqual([e["op"] for e in b.ledger], ["published", "acked"])

    def test_public_views_detached(self):
        b = CollaborationBroker()
        b.publish(env())
        for _ in range(core.MAX_RETRIES):
            b.nack("c", "m", "reason")
        b.ledger[0]["op"] = "forged"
        b.dead_letters[0]["reason"] = "forged"
        self.assertEqual(b.dead_letters[0]["reason"], "reason")
        self.assertEqual(b.verify()["verdict"], "PASS")

    def test_whole_record_digest(self):
        b = CollaborationBroker()
        b.publish(env())
        record = b.consume("c")[0]
        digest = record.pop("envelope_digest")
        self.assertEqual(digest, _digest(record))

    def test_timestamp_and_payload_digest_tamper(self):
        for field, value in (("created_at", 0), ("payload_digest", "forged"), ("schema", "old"), ("seq", 10)):
            b = CollaborationBroker()
            b.publish(env(created_at=123))
            b._channels["c"][0][field] = value
            self.assertEqual(b.verify()["verdict"], "FAIL")
            with self.assertRaises(IntegrityError):
                b.consume("c")

    def test_state_components_tamper(self):
        for component in ("_seq", "_seen_ids", "_pending_acks", "_ledger"):
            b = CollaborationBroker()
            b.publish(env())
            getattr(b, component).clear()
            self.assertEqual(b.verify()["verdict"], "FAIL")
            for operation in (lambda: b.publish(env("new")), lambda: b.ack("c", "m"), lambda: b.nack("c", "m", "x")):
                with self.assertRaises(IntegrityError):
                    operation()

    def test_retry_and_dead_letter_tamper(self):
        b = CollaborationBroker()
        b.publish(env())
        b._pending_acks[("c", "m")]["retries"] = 2
        self.assertEqual(b.verify()["verdict"], "FAIL")
        b = CollaborationBroker()
        b.publish(env())
        for _ in range(core.MAX_RETRIES):
            b.nack("c", "m", "x")
        b._dead_letters[0]["reason"] = "forged"
        self.assertEqual(b.verify()["verdict"], "FAIL")

    def test_dead_letter_digest(self):
        b = CollaborationBroker()
        b.publish(env())
        for _ in range(core.MAX_RETRIES):
            b.nack("c", "m", "x")
        entry = b.dead_letters[0]
        digest = entry.pop("dead_letter_digest")
        self.assertEqual(digest, _digest(entry))

    def test_event_sequence(self):
        b = CollaborationBroker()
        b.publish(env()); b.publish(env()); b.nack("c", "m", "x"); b.ack("c", "m")
        self.assertEqual([e["event_seq"] for e in b.ledger], [1, 2, 3, 4])
