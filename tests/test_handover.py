import sys
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import ApiError, BatchService, HandoverService, ReleaseService, Store


class ConnProxy:
    """透明连接代理，用于在测试中注入写入失败。"""

    def __init__(self, conn):
        object.__setattr__(self, "_conn", conn)
        object.__setattr__(self, "execute_override", None)

    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, "_conn"), name)

    def __setattr__(self, name, value):
        if name in ("execute_override", "_conn"):
            object.__setattr__(self, name, value)
        elif name == "execute":
            object.__setattr__(self, "execute_override", value)
        else:
            setattr(object.__getattribute__(self, "_conn"), name, value)

    def execute(self, sql, *params):
        override = object.__getattribute__(self, "execute_override")
        if override is not None:
            return override(sql, *params)
        return object.__getattribute__(self, "_conn").execute(sql, *params)

    def __enter__(self):
        return object.__getattribute__(self, "_conn").__enter__()

    def __exit__(self, *exc):
        return object.__getattribute__(self, "_conn").__exit__(*exc)


def _future(days=3):
    return (datetime.now(timezone.utc) + timedelta(days=days)).isoformat().replace("+00:00", "Z")


class HandoverFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        store = Store(Path(self.tmp.name) / "h.db")
        store.conn = ConnProxy(store.conn)
        self.s = BatchService(store)
        self.h = HandoverService(store)
        self.r = ReleaseService(store, self.h)
        self.f1 = self.s.register_factory("qa", "qa", "F1", "前厂", "CN")["id"]
        self.f2 = self.s.register_factory("qa", "qa", "F2", "新厂", "CN")["id"]
        self.future = _future()

    def tearDown(self):
        self.s.store.close()
        self.tmp.cleanup()

    def _released_ready_batch(self, batch_no="HB-1"):
        batch = self.s.create_batch("op1", "operator", self.f1, batch_no, "药片", "2026-01-01", "2028-01-01")
        rev = batch["revision"]
        self.s.record_test("lab", "lab", self.f1, batch["id"], "含量", 100, 95, 105, rev)
        rev = self.s.batch_detail(batch["id"])["batch"]["revision"]
        self.s.record_stability("lab", "lab", self.f1, batch["id"], "25C/60RH", "3m", 99, 105, rev)
        return batch

    def _proposed(self, batch):
        return self.h.propose("op1", "operator", self.f1, batch["id"], self.f2)

    def _confirmed(self, batch):
        handover = self._proposed(batch)
        return self.h.confirm("qa2", "qa", self.f2, handover["id"])

    def test_handover_freezes_deviation_retest_rework_stability(self):
        batch = self.s.create_batch("op1", "operator", self.f1, "HB-1", "药片", "2026-01-01", "2028-01-01")
        rev = batch["revision"]
        self.s.record_test("lab", "lab", self.f1, batch["id"], "含量", 100, 95, 105, rev)
        rev = self.s.batch_detail(batch["id"])["batch"]["revision"]
        self.s.plan_rework("op1", "operator", self.f1, batch["id"], "预存返工", rev)
        rev = self.s.batch_detail(batch["id"])["batch"]["revision"]
        self.s.record_stability("lab", "lab", self.f1, batch["id"], "25C/60RH", "3m", 99, 105, rev)
        rev = self.s.batch_detail(batch["id"])["batch"]["revision"]
        handover = self._proposed(batch)
        # 发起交接即冻结四类依据，任何工厂都不能再改写
        with self.assertRaises(ApiError) as dev:
            self.s.add_deviation("op1", "operator", self.f1, batch["id"], "minor", "交接后偏差", self.future, rev)
        self.assertEqual(409, dev.exception.status)
        with self.assertRaises(ApiError):
            self.s.record_test("lab", "lab", self.f1, batch["id"], "水分", 1, 0, 2, rev)
        with self.assertRaises(ApiError):
            self.s.plan_rework("op1", "operator", self.f1, batch["id"], "交接后返工", rev)
        with self.assertRaises(ApiError):
            self.s.record_stability("lab", "lab", self.f1, batch["id"], "40C", "1m", 1, 2, rev)
        # 快照完整冻结四类依据
        snap = handover["snapshot"]
        self.assertEqual(1, len(snap["tests"]))
        self.assertEqual(1, len(snap["rework"]))
        self.assertEqual(1, len(snap["stability"]))
        self.assertEqual([], snap["deviations"])

    def test_after_confirm_previous_factory_can_only_append_receipts(self):
        batch = self._released_ready_batch()
        handover = self._confirmed(batch)
        # 交接确认前不能追加回执
        h2 = self._proposed(self._released_ready_batch("HB-X"))
        with self.assertRaises(ApiError) as early:
            self.h.append_receipt("op1", "operator", self.f1, h2["id"], "REQ-0", "test", "过早回执")
        self.assertEqual(409, early.exception.status)
        # 新厂不能追加回执；只有前厂可以
        with self.assertRaises(ApiError) as outsider:
            self.h.append_receipt("op2", "operator", self.f2, handover["id"], "REQ-1", "test", "新厂冒充前厂")
        self.assertEqual(403, outsider.exception.status)
        out = self.h.append_receipt("op1", "operator", self.f1, handover["id"], "REQ-1", "test", "复测补录回执：含量合格")
        self.assertEqual("REQ-1", out["request_no"])
        self.assertFalse(out["replayed"])
        # 前厂 QA 也可追加
        out2 = self.h.append_receipt("qa1", "qa", self.f1, handover["id"], "REQ-2", "stability", "稳定性补充回执")
        self.assertEqual("stability", out2["evidence_type"])

    def test_duplicate_receipt_reuses_first_result(self):
        batch = self._released_ready_batch()
        handover = self._confirmed(batch)
        first = self.h.append_receipt("op1", "operator", self.f1, handover["id"], "REQ-DUP", "test", "首次内容")
        again = self.h.append_receipt("op1", "operator", self.f1, handover["id"], "REQ-DUP", "test", "不同内容也忽略")
        self.assertTrue(again["replayed"])
        self.assertEqual(first["id"], again["id"])
        self.assertEqual("首次内容", again["content"])
        receipts = self.s.store.conn.execute("SELECT COUNT(*) c FROM receipts WHERE request_no='REQ-DUP'").fetchone()["c"]
        self.assertEqual(1, receipts)

    def test_failed_write_keeps_ledger_and_same_request_retries(self):
        batch = self._released_ready_batch()
        handover = self._confirmed(batch)
        conn = self.s.store.conn
        real_execute = object.__getattribute__(conn, "_conn").execute
        fired = {"v": False}

        def flaky(sql, *params):
            if "INSERT INTO receipts" in sql and not fired["v"]:
                fired["v"] = True
                raise RuntimeError("simulated disk failure")
            return real_execute(sql, *params)

        conn.execute = flaky
        with self.assertRaises(ApiError) as failed:
            self.h.append_receipt("op1", "operator", self.f1, handover["id"], "REQ-RETRY", "test", "写入会失败")
        conn.execute = real_execute
        self.assertEqual(500, failed.exception.status)
        self.assertIn("REQ-RETRY", failed.exception.message)
        # 原始请求记录保留为 failed，尝试次数为 1
        ledger = conn.execute("SELECT * FROM request_ledger WHERE request_no='REQ-RETRY'").fetchone()
        self.assertEqual("failed", ledger["status"])
        self.assertEqual(1, ledger["attempt"])
        self.assertIsNone(conn.execute("SELECT * FROM receipts WHERE request_no='REQ-RETRY'").fetchone())
        # 同一请求号重试成功
        ok = self.h.append_receipt("op1", "operator", self.f1, handover["id"], "REQ-RETRY", "test", "重试内容")
        self.assertEqual("重试内容", ok["content"])
        ledger = conn.execute("SELECT * FROM request_ledger WHERE request_no='REQ-RETRY'").fetchone()
        self.assertEqual("completed", ledger["status"])
        self.assertEqual(2, ledger["attempt"])

    def test_receipt_invalidates_active_conclusion_and_recompute(self):
        batch = self._released_ready_batch()
        handover = self._confirmed(batch)
        c1 = self.r.review("qa2", "qa", self.f2, batch["id"], "release", "冻结依据齐全，放行")
        self.assertEqual("active", c1["status"])
        # 证据更新（前厂追加回执）→ 生效结论失效，写明失效来源
        receipt = self.h.append_receipt("op1", "operator", self.f1, handover["id"], "REQ-NEW", "deviation", "新补充调查回执")
        invalidated = self.r.conclusion_detail(c1["id"])
        self.assertEqual("invalidated", invalidated["status"])
        self.assertEqual(receipt["id"], invalidated["invalidated_by_receipt_id"])
        self.assertIsNotNone(invalidated["invalidated_at"])
        # 失效后基于最新证据版本重算，新结论生效并挂接导致失效的回执
        c2 = self.r.review("qa2", "qa", self.f2, batch["id"], "release", "回执已审查，维持放行")
        self.assertEqual("active", c2["status"])
        self.assertEqual(receipt["id"], c2["last_receipt_id"])
        self.assertNotEqual(c1["id"], c2["id"])
        # 审计中写明失效来源
        audits = [dict(a) for a in self.s.store.conn.execute("SELECT * FROM audit_log WHERE action='release.invalidate'")]
        self.assertEqual(1, len(audits))
        self.assertIn("source_receipt_id", audits[0]["details_json"])

    def test_review_judgement_on_frozen_snapshot(self):
        batch = self.s.create_batch("op1", "operator", self.f1, "HB-CRIT", "注射剂", "2026-01-01", "2028-01-01")
        rev = batch["revision"]
        self.s.add_deviation("insp", "inspector", self.f1, batch["id"], "critical", "无菌异常", self.future, rev)
        rev = self.s.batch_detail(batch["id"])["batch"]["revision"]
        self.s.record_test("lab", "lab", self.f1, batch["id"], "无菌", 100, 95, 105, rev)
        handover = self._confirmed(batch)
        with self.assertRaises(ApiError) as blocked:
            self.r.review("qa2", "qa", self.f2, batch["id"], "release", "尝试放行")
        self.assertIn("关键偏差", blocked.exception.message)
        # 拒绝不受偏差限制
        rejected = self.r.review("qa2", "qa", self.f2, batch["id"], "reject", "冻结关键偏差，拒收")
        self.assertEqual("reject", rejected["decision"])
        # 回执不会解冻或修改冻结快照
        self.h.append_receipt("op1", "operator", self.f1, handover["id"], "REQ-C", "deviation", "前厂补充说明")

    def test_concurrent_confirm_succeeds_once(self):
        batch = self._released_ready_batch()
        handover = self._proposed(batch)
        results = []

        def confirm():
            try:
                results.append(("ok", self.h.confirm("qa2", "qa", self.f2, handover["id"])["status"]))
            except ApiError as exc:
                results.append(("err", exc.status))

        threads = [threading.Thread(target=confirm) for _ in range(8)]
        for t in threads: t.start()
        for t in threads: t.join()
        oks = [r for r in results if r[0] == "ok"]
        self.assertEqual(1, len(oks))
        self.assertEqual("confirmed", oks[0][1])
        self.assertTrue(all(r == ("err", 409) for r in results if r[0] == "err"))
        # 已确认后再次确认直接拒绝
        with self.assertRaises(ApiError) as again:
            self.h.confirm("qa2", "qa", self.f2, handover["id"])
        self.assertEqual(409, again.exception.status)

    def test_concurrent_review_succeeds_once(self):
        batch = self._released_ready_batch()
        self._confirmed(batch)
        results = []

        def review():
            try:
                results.append(("ok", self.r.review("qa2", "qa", self.f2, batch["id"], "release", "并发放行").get("id")))
            except ApiError as exc:
                results.append(("err", exc.status))

        threads = [threading.Thread(target=review) for _ in range(8)]
        for t in threads: t.start()
        for t in threads: t.join()
        oks = [r for r in results if r[0] == "ok"]
        self.assertEqual(1, len(oks))
        self.assertTrue(all(r == ("err", 409) for r in results if r[0] == "err"))

    def test_unauthorized_roles_are_rejected(self):
        batch = self._released_ready_batch()
        handover = self._proposed(batch)
        # 只有 operator 能发起交接
        with self.assertRaises(ApiError) as e1:
            self.h.propose("lab1", "lab", self.f1, batch["id"], self.f2)
        self.assertEqual(403, e1.exception.status)
        # 只有新厂 qa 能确认：lab/operator 越权，前厂 qa 也越权
        with self.assertRaises(ApiError) as e2:
            self.h.confirm("op2", "operator", self.f2, handover["id"])
        self.assertEqual(403, e2.exception.status)
        with self.assertRaises(ApiError) as e3:
            self.h.confirm("qa1", "qa", self.f1, handover["id"])
        self.assertEqual(403, e3.exception.status)
        self.h.confirm("qa2", "qa", self.f2, handover["id"])
        # 放行审查：非 qa 角色拒绝；前厂 qa 拒绝
        with self.assertRaises(ApiError) as e4:
            self.r.review("op2", "operator", self.f2, batch["id"], "release", "越权放行")
        self.assertEqual(403, e4.exception.status)
        with self.assertRaises(ApiError) as e5:
            self.r.review("qa1", "qa", self.f1, batch["id"], "release", "前厂自我放行")
        self.assertEqual(403, e5.exception.status)
        # 回执：lab 角色拒绝
        with self.assertRaises(ApiError) as e6:
            self.h.append_receipt("lab1", "lab", self.f1, handover["id"], "REQ-X", "test", "越权回执")
        self.assertEqual(403, e6.exception.status)

    def test_state_carries_invalidation_source(self):
        batch = self._released_ready_batch()
        handover = self._confirmed(batch)
        self.r.review("qa2", "qa", self.f2, batch["id"], "release", "首次放行")
        self.h.append_receipt("op1", "operator", self.f1, handover["id"], "REQ-S", "test", "导致失效的回执")
        state = self.s.state()
        conclusion = next(c for c in state["release_conclusions"] if c["status"] == "invalidated")
        self.assertIsNotNone(conclusion["invalidated_by_receipt_id"])
        receipt = next(r for r in state["receipts"] if r["request_no"] == "REQ-S")
        self.assertEqual(receipt["id"], conclusion["invalidated_by_receipt_id"])
        handover_view = next(h for h in state["handovers"] if h["id"] == handover["id"])
        self.assertEqual("confirmed", handover_view["status"])


if __name__ == "__main__":
    unittest.main()
