import unittest

from e08.core import (MAX_RETRIES, CollaborationBroker, TransportError,
                      make_envelope)


def env(mid, channel="plan", sender="P-DOC", kind="event", payload=None):
    return make_envelope(mid, channel, sender, kind, payload or {"n": mid})


class Envelopes(unittest.TestCase):
    def test_valid_envelope_with_digest(self):
        e = env("m1")
        self.assertTrue(e["payload_digest"].startswith("sha256:"))

    def test_validation(self):
        with self.assertRaises(TransportError):
            make_envelope("m", "ch", "s", "gossip", {})
        with self.assertRaises(TransportError):
            make_envelope("m", "ch", "s", "event", "not-a-dict")
        with self.assertRaises(TransportError):
            make_envelope("m", "ch", "s", "event", {"big": "x" * 70000})


class Ordering(unittest.TestCase):
    def setUp(self):
        self.b = CollaborationBroker()

    def test_contiguous_sequences_per_channel(self):
        for i in range(1, 4):
            r = self.b.publish(env(f"m{i}"))
            self.assertEqual(r["seq"], i)
        self.b.publish(env("x1", channel="other"))
        self.assertEqual(self.b.publish(env("x2", channel="other"))["seq"], 2)

    def test_ordered_consume_and_after_seq(self):
        for i in range(1, 5):
            self.b.publish(env(f"m{i}"))
        msgs = self.b.consume("plan")
        self.assertEqual([m["seq"] for m in msgs], [1, 2, 3, 4])
        later = self.b.consume("plan", after_seq=2)
        self.assertEqual([m["message_id"] for m in later], ["m3", "m4"])


class Idempotency(unittest.TestCase):
    def test_duplicates_absorbed(self):
        b = CollaborationBroker()
        b.publish(env("m1"))
        r = b.publish(env("m1"))
        self.assertTrue(r["duplicate"])
        self.assertEqual(len(b.consume("plan")), 1)
        self.assertTrue(any(e["op"] == "duplicate_absorbed" for e in b.ledger))


class AckNackDlq(unittest.TestCase):
    def setUp(self):
        self.b = CollaborationBroker()
        self.b.publish(env("m1"))

    def test_ack_removes_from_delivery(self):
        self.b.ack("plan", "m1")
        self.assertEqual(self.b.consume("plan"), [])
        with self.assertRaises(TransportError):
            self.b.ack("plan", "m1")           # double ack refused

    def test_nack_bounded_retries_then_dead_letter(self):
        for i in range(1, MAX_RETRIES):
            r = self.b.nack("plan", "m1", "handler crashed")
            self.assertFalse(r["dead_lettered"])
            self.assertEqual(len(self.b.consume("plan")), 1)  # still deliverable
        r = self.b.nack("plan", "m1", "handler crashed")
        self.assertTrue(r["dead_lettered"])
        self.assertEqual(self.b.consume("plan"), [])
        dl = self.b.dead_letters[0]
        self.assertEqual(dl["message"]["message_id"], "m1")
        self.assertEqual(dl["retries_exhausted"], MAX_RETRIES)
        with self.assertRaises(TransportError):
            self.b.nack("plan", "m1", "again")   # gone from pending

    def test_ledger_records_lifecycle(self):
        self.b.nack("plan", "m1", "x")
        self.b.ack("plan", "m1")
        ops = [e["op"] for e in self.b.ledger]
        self.assertEqual(ops, ["published", "nacked", "acked"])


class Integrity(unittest.TestCase):
    def test_verify_pass_and_tamper_detection(self):
        b = CollaborationBroker()
        b.publish(env("m1"))
        b.publish(env("m2"))
        self.assertEqual(b.verify()["verdict"], "PASS")
        b._channels["plan"][0]["payload"] = {"evil": True}
        b._channels["plan"][0]["sender"] = "IMPOSTOR"
        rep = b.verify()
        self.assertEqual(rep["verdict"], "FAIL")
        self.assertEqual(rep["problems"][0]["issue"], "envelope tampered")

    def test_sequence_gap_detected(self):
        b = CollaborationBroker()
        b.publish(env("m1"))
        b.publish(env("m2"))
        b._channels["plan"].pop(0)
        rep = b.verify()
        self.assertEqual(rep["verdict"], "FAIL")
        self.assertIn("sequence gap", rep["problems"][0]["issue"])

    def test_no_network_imports(self):
        import e08.core as m
        from pathlib import Path
        imports = " ".join(l for l in Path(m.__file__).read_text(encoding="utf-8").splitlines()
                           if l.startswith(("import ", "from ")))
        for bad in ("socket", "http", "urllib", "requests"):
            self.assertNotIn(bad, imports)


class HardeningFixes(unittest.TestCase):
    """Regression tests for A025-F1..F3 (0.1.1-partial)."""

    def test_f1_publish_isolated_from_caller_mutation(self):
        b = CollaborationBroker()
        payload = {"n": 1}
        b.publish(make_envelope("m1", "plan", "s", "event", payload))
        payload["n"] = 999
        self.assertEqual(b.consume("plan")[0]["payload"], {"n": 1})
        self.assertEqual(b.verify()["verdict"], "PASS")

    def test_f1_consume_returns_copies(self):
        b = CollaborationBroker()
        b.publish(make_envelope("m1", "plan", "s", "event", {"n": 1}))
        b.consume("plan")[0]["payload"]["n"] = -5
        self.assertEqual(b.consume("plan")[0]["payload"], {"n": 1})
        self.assertEqual(b.verify()["verdict"], "PASS")

    def test_f1_dead_letter_entry_is_a_copy(self):
        b = CollaborationBroker()
        b.publish(make_envelope("m1", "plan", "s", "event", {"n": 1}))
        for _ in range(MAX_RETRIES):
            b.nack("plan", "m1", "boom")
        b.dead_letters[0]["message"]["sender"] = "IMPOSTOR"
        self.assertEqual(b._channels["plan"][0]["sender"], "s")
        self.assertEqual(b.verify()["verdict"], "PASS")

    def test_f2_non_serializable_payload_raises_transport_error(self):
        with self.assertRaises(TransportError):
            make_envelope("m", "c", "s", "event", {"x": {1, 2}})

    def test_f2_non_string_identity_fields_rejected(self):
        for bad in ({}, 123, None, ""):
            with self.assertRaises(TransportError):
                make_envelope(bad, "c", "s", "event", {})
            with self.assertRaises(TransportError):
                make_envelope("m", bad, "s", "event", {})
            with self.assertRaises(TransportError):
                make_envelope("m", "c", bad, "event", {})
        b = CollaborationBroker()
        with self.assertRaises(TransportError):
            b.publish({"message_id": {}, "channel": "c", "sender": "s",
                       "kind": "event", "payload": {}})

    def test_f3_nan_and_infinity_payloads_rejected(self):
        for bad in (float("nan"), float("inf"), float("-inf")):
            with self.assertRaises(TransportError):
                make_envelope("m", "c", "s", "event", {"x": bad})

    def test_valid_publish_still_accepted(self):
        b = CollaborationBroker()
        r = b.publish(make_envelope("ok", "c", "s", "event", {"x": 1.5}))
        self.assertEqual(r["seq"], 1)


if __name__ == "__main__":
    unittest.main()
