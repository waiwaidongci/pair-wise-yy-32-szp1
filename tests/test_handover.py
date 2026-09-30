"""Tests for batch handover, evidence freeze, receipt idempotency, and decision invalidation."""
import sys, tempfile, unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import ApiError, BatchService, Store, JudgmentService, TraceService, RequestService


class HandoverTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.s = BatchService(Store(Path(self.tmp.name) / "b.db"))
        self.f1 = self.s.register_factory("qa", "qa", "F1", "一厂", "CN")["id"]
        self.f2 = self.s.register_factory("qa", "qa", "F2", "二厂", "CN")["id"]
        self.future = (datetime.now(timezone.utc) + timedelta(days=3)).isoformat().replace("+00:00", "Z")

    def tearDown(self):
        self.s.store.close(); self.tmp.cleanup()

    def _make_batch_with_test(self, factory_id=None):
        fid = factory_id or self.f1
        batch = self.s.create_batch("operator", "operator", fid, "B-" + str(fid), "药片", "2026-01-01", "2028-01-01")
        rev = batch["revision"]
        self.s.record_test("lab", "lab", fid, batch["id"], "含量", 99, 95, 105, rev)
        return batch

    # ---- handover creation ----
    def test_create_handover(self):
        batch = self._make_batch_with_test()
        rev = self.s.batch_detail(batch["id"])["batch"]["revision"]
        ho = self.s.handovers.create_handover("qa", "qa", self.f1, batch["id"], self.f2, "HO-001")
        self.assertEqual("pending", ho["status"])
        self.assertEqual(self.f1, ho["from_factory_id"])
        self.assertEqual(self.f2, ho["to_factory_id"])
        self.assertFalse(ho["evidence_frozen"])

    def test_cannot_handover_to_same_factory(self):
        batch = self._make_batch_with_test()
        with self.assertRaises(ApiError) as ctx:
            self.s.handovers.create_handover("qa", "qa", self.f1, batch["id"], self.f1, "HO-002")
        self.assertEqual(400, ctx.exception.status)

    def test_cannot_create_duplicate_handover(self):
        batch = self._make_batch_with_test()
        self.s.handovers.create_handover("qa", "qa", self.f1, batch["id"], self.f2, "HO-003")
        with self.assertRaises(ApiError) as ctx:
            self.s.handovers.create_handover("qa", "qa", self.f1, batch["id"], self.f2, "HO-004")
        self.assertEqual(409, ctx.exception.status)

    def test_unauthorized_role_cannot_create_handover(self):
        batch = self._make_batch_with_test()
        with self.assertRaises(ApiError) as ctx:
            self.s.handovers.create_handover("lab", "lab", self.f1, batch["id"], self.f2, "HO-005")
        self.assertEqual(403, ctx.exception.status)

    # ---- handover confirmation & evidence freeze ----
    def test_confirm_handover_freezes_evidence(self):
        batch = self._make_batch_with_test()
        ho = self.s.handovers.create_handover("qa", "qa", self.f1, batch["id"], self.f2, "HO-006")
        confirmed = self.s.handovers.confirm_handover("qa", "qa", ho["id"])
        self.assertEqual("confirmed", confirmed["status"])
        self.assertTrue(confirmed["evidence_frozen"])
        # batch ownership moved to F2
        detail = self.s.batch_detail(batch["id"])
        self.assertEqual(self.f2, detail["batch"]["factory_id"])
        self.assertEqual(ho["id"], detail["batch"]["handover_id"])

    def test_former_factory_cannot_add_evidence_after_confirmation(self):
        batch = self._make_batch_with_test()
        ho = self.s.handovers.create_handover("qa", "qa", self.f1, batch["id"], self.f2, "HO-007")
        self.s.handovers.confirm_handover("qa", "qa", ho["id"])
        rev = self.s.batch_detail(batch["id"])["batch"]["revision"]
        # F1 tries to add evidence -> factory check fails
        with self.assertRaises(ApiError) as ctx:
            self.s.record_test("lab", "lab", self.f1, batch["id"], "水分", 1, 0, 2, rev)
        self.assertEqual(403, ctx.exception.status)
        # F2 can add evidence
        self.s.record_test("lab", "lab", self.f2, batch["id"], "水分", 1, 0, 2, rev)

    def test_concurrent_confirm_only_succeeds_once(self):
        batch = self._make_batch_with_test()
        ho = self.s.handovers.create_handover("qa", "qa", self.f1, batch["id"], self.f2, "HO-008")
        first = self.s.handovers.confirm_handover("qa", "qa", ho["id"])
        with self.assertRaises(ApiError) as ctx:
            self.s.handovers.confirm_handover("qa", "qa", ho["id"])
        self.assertEqual(409, ctx.exception.status)
        self.assertEqual("confirmed", first["status"])

    def test_unauthorized_role_cannot_confirm_handover(self):
        batch = self._make_batch_with_test()
        ho = self.s.handovers.create_handover("qa", "qa", self.f1, batch["id"], self.f2, "HO-009")
        with self.assertRaises(ApiError) as ctx:
            self.s.handovers.confirm_handover("operator", "operator", ho["id"])
        self.assertEqual(403, ctx.exception.status)

    # ---- receipts ----
    def test_former_factory_can_append_receipt_after_confirmation(self):
        batch = self._make_batch_with_test()
        ho = self.s.handovers.create_handover("qa", "qa", self.f1, batch["id"], self.f2, "HO-010")
        self.s.handovers.confirm_handover("qa", "qa", ho["id"])
        receipt = self.s.handovers.append_receipt("qa", "qa", self.f1, ho["id"], "R-001", "交接确认", "证据齐全")
        self.assertEqual("R-001", receipt["request_no"])
        self.assertEqual("交接确认", receipt["receipt_type"])

    def test_cannot_append_receipt_before_confirmation(self):
        batch = self._make_batch_with_test()
        ho = self.s.handovers.create_handover("qa", "qa", self.f1, batch["id"], self.f2, "HO-011")
        with self.assertRaises(ApiError) as ctx:
            self.s.handovers.append_receipt("qa", "qa", self.f1, ho["id"], "R-002", "交接确认", "证据齐全")
        self.assertEqual(409, ctx.exception.status)

    def test_new_factory_cannot_append_receipt(self):
        batch = self._make_batch_with_test()
        ho = self.s.handovers.create_handover("qa", "qa", self.f1, batch["id"], self.f2, "HO-012")
        self.s.handovers.confirm_handover("qa", "qa", ho["id"])
        with self.assertRaises(ApiError) as ctx:
            self.s.handovers.append_receipt("qa", "qa", self.f2, ho["id"], "R-003", "交接确认", "证据齐全")
        self.assertEqual(403, ctx.exception.status)

    def test_duplicate_receipt_returns_first_result(self):
        batch = self._make_batch_with_test()
        ho = self.s.handovers.create_handover("qa", "qa", self.f1, batch["id"], self.f2, "HO-013")
        self.s.handovers.confirm_handover("qa", "qa", ho["id"])
        r1 = self.s.handovers.append_receipt("qa", "qa", self.f1, ho["id"], "R-004", "类型A", "内容A")
        r2 = self.s.handovers.append_receipt("qa", "qa", self.f1, ho["id"], "R-004", "类型B", "内容B")
        self.assertEqual(r1["id"], r2["id"])
        self.assertEqual(r1["receipt_type"], r2["receipt_type"])
        self.assertEqual(r1["content"], r2["content"])
        # only one receipt exists
        receipts = self.s.batch_detail(batch["id"])["receipts"]
        self.assertEqual(1, len(receipts))

    def test_receipt_requires_request_no(self):
        batch = self._make_batch_with_test()
        ho = self.s.handovers.create_handover("qa", "qa", self.f1, batch["id"], self.f2, "HO-014")
        self.s.handovers.confirm_handover("qa", "qa", ho["id"])
        with self.assertRaises(ApiError) as ctx:
            self.s.handovers.append_receipt("qa", "qa", self.f1, ho["id"], "", "类型A", "内容A")
        self.assertEqual(400, ctx.exception.status)

    # ---- decision invalidation & recalculation ----
    def test_evidence_update_invalidates_release_decision(self):
        batch = self._make_batch_with_test()
        ho = self.s.handovers.create_handover("qa", "qa", self.f1, batch["id"], self.f2, "HO-015")
        self.s.handovers.confirm_handover("qa", "qa", ho["id"])
        rev = self.s.batch_detail(batch["id"])["batch"]["revision"]
        # F2 releases
        result = self.s.decide("qa", "qa", batch["id"], "release", "新厂放行", rev)
        self.assertEqual("released", result["batch"]["state"])
        # F2 updates evidence -> decision invalidated
        rev = self.s.batch_detail(batch["id"])["batch"]["revision"]
        self.s.record_test("lab", "lab", self.f2, batch["id"], "含量", 98, 95, 105, rev)
        detail = self.s.batch_detail(batch["id"])
        decisions = detail["decisions"]
        self.assertEqual(1, len(decisions))
        self.assertEqual(1, decisions[0]["invalidated"])
        self.assertIsNotNone(decisions[0]["invalidation_source"])
        self.assertIn("test:", decisions[0]["invalidation_source"])

    def test_recalculation_after_invalidation(self):
        batch = self._make_batch_with_test()
        ho = self.s.handovers.create_handover("qa", "qa", self.f1, batch["id"], self.f2, "HO-016")
        self.s.handovers.confirm_handover("qa", "qa", ho["id"])
        rev = self.s.batch_detail(batch["id"])["batch"]["revision"]
        self.s.decide("qa", "qa", batch["id"], "release", "首次放行", rev)
        rev = self.s.batch_detail(batch["id"])["batch"]["revision"]
        self.s.record_test("lab", "lab", self.f2, batch["id"], "含量", 97, 95, 105, rev)
        # recalculate: new decision allowed
        rev = self.s.batch_detail(batch["id"])["batch"]["revision"]
        result = self.s.decide("qa", "qa", batch["id"], "release", "重算放行", rev)
        self.assertEqual("released", result["batch"]["state"])
        detail = self.s.batch_detail(batch["id"])
        self.assertEqual(2, len(detail["decisions"]))
        # first invalidated, second valid
        self.assertEqual(1, detail["decisions"][0]["invalidated"])
        self.assertEqual(0, detail["decisions"][1]["invalidated"])

    def test_concurrent_release_only_succeeds_once(self):
        batch = self._make_batch_with_test()
        ho = self.s.handovers.create_handover("qa", "qa", self.f1, batch["id"], self.f2, "HO-017")
        self.s.handovers.confirm_handover("qa", "qa", ho["id"])
        rev = self.s.batch_detail(batch["id"])["batch"]["revision"]
        first = self.s.decide("qa", "qa", batch["id"], "release", "并发放行A", rev)
        with self.assertRaises(ApiError) as ctx:
            self.s.decide("qa", "qa", batch["id"], "release", "并发放行B", rev)
        self.assertEqual(409, ctx.exception.status)
        self.assertEqual("released", first["batch"]["state"])

    # ---- separation of concerns ----
    def test_judgment_service_evaluate(self):
        js = JudgmentService(self.s.conn)
        batch = self._make_batch_with_test()
        row = self.s._row("batches", batch["id"])
        # release with passing test -> released
        self.assertEqual("released", js.evaluate(row, "release", ""))
        # reject -> rejected
        self.assertEqual("rejected", js.evaluate(row, "reject", ""))
        # resample -> awaiting_resample
        self.assertEqual("awaiting_resample", js.evaluate(row, "resample", ""))

    def test_trace_service_records_audit(self):
        ts = TraceService(self.s.store)
        ts.record("tester", "test.action", "test", 123, {"key": "value"})
        rows = self.s.conn.execute("SELECT * FROM audit_log WHERE action='test.action'").fetchall()
        self.assertEqual(1, len(rows))
        self.assertEqual("tester", rows[0]["actor"])

    def test_request_service_idempotent(self):
        rs = RequestService(self.s.store)
        # first call caches result
        r1 = rs.idempotent("REQ-001", "test", lambda: {"id": 1}, lambda rn: None)
        self.assertEqual({"id": 1}, r1)
        # second call with same request_no returns cached result even if fn would fail
        r2 = rs.idempotent("REQ-001", "test", lambda: (_ for _ in ()).throw(RuntimeError("fail")), lambda rn: None)
        self.assertEqual({"id": 1}, r2)

    def test_request_service_failed_write_not_cached(self):
        rs = RequestService(self.s.store)
        # first call fails -> not cached
        with self.assertRaises(RuntimeError):
            rs.idempotent("REQ-002", "test", lambda: (_ for _ in ()).throw(RuntimeError("fail")), lambda rn: None)
        # retry succeeds
        r = rs.idempotent("REQ-002", "test", lambda: {"id": 2}, lambda rn: None)
        self.assertEqual({"id": 2}, r)

    def test_batch_detail_includes_handover_and_receipts(self):
        batch = self._make_batch_with_test()
        ho = self.s.handovers.create_handover("qa", "qa", self.f1, batch["id"], self.f2, "HO-018")
        self.s.handovers.confirm_handover("qa", "qa", ho["id"])
        self.s.handovers.append_receipt("qa", "qa", self.f1, ho["id"], "R-018", "类型", "内容")
        detail = self.s.batch_detail(batch["id"])
        self.assertIsNotNone(detail["handover"])
        self.assertEqual("confirmed", detail["handover"]["status"])
        self.assertEqual(1, len(detail["receipts"]))
        self.assertEqual("R-018", detail["receipts"][0]["request_no"])


if __name__ == "__main__":
    unittest.main()
