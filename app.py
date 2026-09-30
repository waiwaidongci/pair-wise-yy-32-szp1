#!/usr/bin/env python3
"""Pharmaceutical batch deviation, rework, handover and release decision service.

批次交接（handover）场景：
- 前厂在交接时补录证据，交接确认后证据（偏差、复测、返工、稳定性）冻结。
- 交接后前厂只能追加回执（receipt），回执按请求号幂等，重复回执沿用首次结果。
- 新厂（qa）给放行结论；证据更新会让受影响的放行结论失效，需要重算。
- 并发确认交接或放行只成功一次（乐观锁 + 条件更新）。
- 判定（JudgmentService）、留痕（TraceService）、请求处理（RequestService）各自承担。
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

DB_PATH = Path(__file__).with_name("data.db")


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def after_now(value: str | None = None) -> bool:
    if not value:
        return False
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")) > datetime.now(timezone.utc)
    except ValueError:
        return False


def j(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


class ApiError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message); self.status, self.message = status, message


class Store:
    def __init__(self, path: str | Path = DB_PATH):
        self.path = str(path)
        self.conn = sqlite3.connect(self.path, check_same_thread=False); self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON"); self.conn.execute("PRAGMA journal_mode=WAL"); self.init_schema(); self._migrate()

    def init_schema(self) -> None:
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS factories (
          id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT UNIQUE NOT NULL, name TEXT NOT NULL, country TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS batches (
          id INTEGER PRIMARY KEY AUTOINCREMENT, factory_id INTEGER NOT NULL REFERENCES factories(id),
          batch_no TEXT NOT NULL, product TEXT NOT NULL, mfg_date TEXT NOT NULL, expiry_date TEXT NOT NULL,
          state TEXT NOT NULL CHECK(state IN ('manufactured','investigation','awaiting_resample','conditional','released','rejected')),
          revision INTEGER NOT NULL DEFAULT 1, created_by TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
          UNIQUE(factory_id,batch_no)
        );
        CREATE TABLE IF NOT EXISTS deviations (
          id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id INTEGER NOT NULL REFERENCES batches(id),
          severity TEXT NOT NULL CHECK(severity IN ('critical','minor')), title TEXT NOT NULL, due_at TEXT,
          status TEXT NOT NULL CHECK(status IN ('open','closed')), corrective_action TEXT,
          exception_reason TEXT, exception_until TEXT, exception_approved_by TEXT,
          closed_by TEXT, closed_at TEXT, created_by TEXT NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS tests (
          id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id INTEGER NOT NULL REFERENCES batches(id),
          test_type TEXT NOT NULL, result REAL NOT NULL, spec_min REAL NOT NULL, spec_max REAL NOT NULL,
          passed INTEGER NOT NULL, round INTEGER NOT NULL DEFAULT 1, recorded_by TEXT NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS rework (
          id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id INTEGER NOT NULL REFERENCES batches(id),
          description TEXT NOT NULL, status TEXT NOT NULL CHECK(status IN ('planned','completed')),
          created_by TEXT NOT NULL, created_at TEXT NOT NULL, completed_by TEXT, completed_at TEXT
        );
        CREATE TABLE IF NOT EXISTS supplier_changes (
          id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id INTEGER NOT NULL REFERENCES batches(id),
          supplier TEXT NOT NULL, change_type TEXT NOT NULL, description TEXT NOT NULL,
          recorded_by TEXT NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS stability (
          id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id INTEGER NOT NULL REFERENCES batches(id),
          condition TEXT NOT NULL, timepoint TEXT NOT NULL, result REAL NOT NULL, spec_limit REAL NOT NULL,
          passed INTEGER NOT NULL, recorded_by TEXT NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS decisions (
          id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id INTEGER NOT NULL REFERENCES batches(id), revision INTEGER NOT NULL,
          decision TEXT NOT NULL CHECK(decision IN ('release','reject','conditional','resample')), rationale TEXT NOT NULL,
          exception_code TEXT, decided_by TEXT NOT NULL, created_at TEXT NOT NULL,
          invalidated INTEGER NOT NULL DEFAULT 0, invalidation_source TEXT, invalidated_at TEXT,
          UNIQUE(batch_id,revision)
        );
        CREATE TABLE IF NOT EXISTS audit_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT NOT NULL, actor TEXT NOT NULL, action TEXT NOT NULL,
          entity_type TEXT NOT NULL, entity_id TEXT NOT NULL, details_json TEXT NOT NULL
        );
        """)
        self.conn.commit()

    def _migrate(self) -> None:
        """Add handover/receipt/request_log tables and handover_id column if missing."""
        def has_column(table: str, column: str) -> bool:
            cols = [row[1] for row in self.conn.execute(f"PRAGMA table_info({table})").fetchall()]
            return column in cols
        if not has_column("batches", "handover_id"):
            self.conn.execute("ALTER TABLE batches ADD COLUMN handover_id INTEGER")
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS handovers (
          id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id INTEGER NOT NULL REFERENCES batches(id),
          handover_no TEXT UNIQUE NOT NULL, from_factory_id INTEGER NOT NULL REFERENCES factories(id),
          to_factory_id INTEGER NOT NULL REFERENCES factories(id),
          status TEXT NOT NULL CHECK(status IN ('pending','confirmed','cancelled')),
          evidence_frozen INTEGER NOT NULL DEFAULT 0,
          created_by TEXT NOT NULL, created_at TEXT NOT NULL, confirmed_by TEXT, confirmed_at TEXT,
          UNIQUE(batch_id)
        );
        CREATE TABLE IF NOT EXISTS receipts (
          id INTEGER PRIMARY KEY AUTOINCREMENT, handover_id INTEGER NOT NULL REFERENCES handovers(id),
          request_no TEXT UNIQUE NOT NULL, receipt_type TEXT NOT NULL, content TEXT NOT NULL,
          recorded_by TEXT NOT NULL, recorded_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS request_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT, request_no TEXT UNIQUE NOT NULL,
          entity_type TEXT NOT NULL, entity_id INTEGER, result_json TEXT NOT NULL, created_at TEXT NOT NULL
        );
        """)
        self.conn.commit()

    def audit(self, actor: str, action: str, entity_type: str, entity_id: object, details: dict) -> None:
        self.conn.execute("INSERT INTO audit_log(at,actor,action,entity_type,entity_id,details_json) VALUES(?,?,?,?,?,?)",
                          (now(), actor, action, entity_type, str(entity_id), j(details)))

    def close(self) -> None:
        self.conn.close()


class TraceService:
    """留痕：所有写操作统一写审计日志。"""
    def __init__(self, store: Store): self.store = store

    def record(self, actor: str, action: str, entity_type: str, entity_id: object, details: dict) -> None:
        self.store.audit(actor, action, entity_type, entity_id, details)


class JudgmentService:
    """判定：根据证据决定批次的放行状态。纯逻辑，不写库。"""
    def __init__(self, conn: sqlite3.Connection): self.conn = conn

    def evaluate(self, batch: sqlite3.Row, decision: str, exception_code: str) -> str:
        deviations = self.conn.execute("SELECT * FROM deviations WHERE batch_id=? ORDER BY id", (batch["id"],)).fetchall()
        open_deviations = [d for d in deviations if d["status"] == "open"]
        latest_tests: dict[str, sqlite3.Row] = {}
        for row in self.conn.execute("SELECT * FROM tests WHERE batch_id=? ORDER BY id", (batch["id"],)):
            latest_tests[row["test_type"]] = row
        if decision in {"release", "conditional"} and not latest_tests:
            raise ApiError(409, "放行前至少需要一项检验结果")
        if decision in {"release", "conditional"} and any(not row["passed"] for row in latest_tests.values()):
            raise ApiError(409, "最新检验结果仍有不合格项")
        if decision == "resample":
            if batch["state"] == "conditional": raise ApiError(409, "有条件放行后不能直接改为再取样")
            return "awaiting_resample"
        if decision == "reject":
            return "rejected"
        if any(d["severity"] == "critical" for d in open_deviations):
            raise ApiError(409, "未关闭的关键偏差阻止放行")
        if decision == "release" and open_deviations:
            raise ApiError(409, "仍有未关闭偏差，不能正式放行")
        if decision == "conditional":
            for deviation in open_deviations:
                if not deviation["exception_reason"] or not after_now(deviation["exception_until"]):
                    raise ApiError(409, f"偏差 {deviation['id']} 没有有效例外批准")
            if not exception_code.strip(): raise ApiError(400, "有条件放行必须提供例外编号")
            return "conditional"
        return "released"


class RequestService:
    """请求处理：按请求号幂等去重，重复请求沿用首次结果。"""
    def __init__(self, store: Store): self.store, self.conn = store, store.conn

    def idempotent(self, request_no: str, entity_type: str, write_fn, find_existing) -> dict:
        if not request_no or not str(request_no).strip():
            raise ApiError(400, "请求号不能为空")
        existing = find_existing(request_no)
        if existing:
            return existing
        cached = self.conn.execute("SELECT result_json FROM request_log WHERE request_no=?", (request_no,)).fetchone()
        if cached:
            return json.loads(cached["result_json"])
        try:
            with self.conn:
                result = write_fn()
                self.conn.execute("INSERT OR IGNORE INTO request_log(request_no,entity_type,result_json,created_at) VALUES(?,?,?,?)",
                                  (request_no, entity_type, j(result), now()))
            return result
        except sqlite3.IntegrityError:
            existing = find_existing(request_no)
            if existing: return existing
            raise


class HandoverService:
    """批次交接：创建、确认、取消交接，以及前厂追加回执。"""
    def __init__(self, store: Store, batch_service: "BatchService"):
        self.store, self.conn = store, store.conn
        self.batch_service = batch_service
        self.trace = TraceService(store)
        self.requests = RequestService(store)

    def _row(self, table: str, identity: int) -> sqlite3.Row:
        row = self.conn.execute(f"SELECT * FROM {table} WHERE id=?", (identity,)).fetchone()
        if not row: raise ApiError(404, "对象不存在")
        return row

    def create_handover(self, actor: str | None, role: str | None, factory_id: int, batch_id: int, to_factory_id: int, handover_no: str) -> dict:
        actor = self.batch_service._actor(actor, role, {"qa", "operator"})
        batch = self._row("batches", batch_id)
        self.batch_service._factory_check(actor, factory_id, batch)
        if batch["state"] in {"released", "rejected"}: raise ApiError(409, "已终态批次不能发起交接")
        existing = self.conn.execute("SELECT id FROM handovers WHERE batch_id=? AND status IN ('pending','confirmed')", (batch_id,)).fetchone()
        if existing: raise ApiError(409, "批次已有未完成的交接")
        to_factory = self.conn.execute("SELECT * FROM factories WHERE id=?", (to_factory_id,)).fetchone()
        if not to_factory: raise ApiError(404, "接收工厂不存在")
        if int(to_factory_id) == int(factory_id): raise ApiError(400, "不能向本厂交接")
        if not handover_no.strip(): raise ApiError(400, "交接单号不能为空")
        stamp = now()
        with self.conn:
            cur = self.conn.execute("""INSERT INTO handovers(batch_id,handover_no,from_factory_id,to_factory_id,status,created_by,created_at)
                                     VALUES(?,?,?,?,'pending',?,?)""",
                                    (batch_id, handover_no.strip(), batch["factory_id"], to_factory_id, actor, stamp))
            self.trace.record(actor, "handover.create", "handover", cur.lastrowid,
                              {"batch_id": batch_id, "handover_no": handover_no, "to_factory_id": to_factory_id})
        return self._handover_dict(self._row("handovers", cur.lastrowid))

    def confirm_handover(self, actor: str | None, role: str | None, handover_id: int) -> dict:
        actor = self.batch_service._actor(actor, role, {"qa"})
        handover = self._row("handovers", handover_id)
        if handover["status"] != "pending": raise ApiError(409, "交接已经确认或取消")
        batch = self._row("batches", handover["batch_id"])
        stamp = now()
        with self.conn:
            # 条件更新：只有 pending 能被确认，保证并发确认只成功一次。
            cur = self.conn.execute("""UPDATE handovers SET status='confirmed',evidence_frozen=1,confirmed_by=?,confirmed_at=?
                                     WHERE id=? AND status='pending'""", (actor, stamp, handover_id))
            if cur.rowcount != 1: raise ApiError(409, "交接确认冲突")
            # 交接确认后批次归属新厂，前厂只能追加回执。
            self.conn.execute("UPDATE batches SET factory_id=?,handover_id=?,revision=revision+1,updated_at=? WHERE id=?",
                              (handover["to_factory_id"], handover_id, stamp, batch["id"]))
            self.trace.record(actor, "handover.confirm", "handover", handover_id,
                              {"batch_id": batch["id"], "from_factory_id": handover["from_factory_id"], "to_factory_id": handover["to_factory_id"]})
        return self._handover_dict(self._row("handovers", handover_id))

    def cancel_handover(self, actor: str | None, role: str | None, handover_id: int) -> dict:
        actor = self.batch_service._actor(actor, role, {"qa"})
        handover = self._row("handovers", handover_id)
        if handover["status"] != "pending": raise ApiError(409, "交接已经确认或取消")
        with self.conn:
            cur = self.conn.execute("UPDATE handovers SET status='cancelled' WHERE id=? AND status='pending'", (handover_id,))
            if cur.rowcount != 1: raise ApiError(409, "交接取消冲突")
            self.trace.record(actor, "handover.cancel", "handover", handover_id, {"batch_id": handover["batch_id"]})
        return self._handover_dict(self._row("handovers", handover_id))

    def append_receipt(self, actor: str | None, role: str | None, factory_id: int, handover_id: int, request_no: str, receipt_type: str, content: str) -> dict:
        actor = self.batch_service._actor(actor, role, {"operator", "inspector", "lab", "qa"})
        handover = self._row("handovers", handover_id)
        if handover["status"] != "confirmed": raise ApiError(409, "交接未确认，不能追加回执")
        if int(factory_id) != int(handover["from_factory_id"]): raise ApiError(403, "只有交接前的工厂可以追加回执")
        if not receipt_type.strip() or not content.strip(): raise ApiError(400, "回执类型和内容不能为空")

        def find_existing(rn: str):
            row = self.conn.execute("SELECT * FROM receipts WHERE request_no=?", (rn,)).fetchone()
            return self._receipt_dict(row) if row else None

        def write_fn():
            cur = self.conn.execute("""INSERT INTO receipts(handover_id,request_no,receipt_type,content,recorded_by,recorded_at)
                                     VALUES(?,?,?,?,?,?)""", (handover_id, request_no, receipt_type.strip(), content.strip(), actor, now()))
            self.trace.record(actor, "receipt.append", "receipt", cur.lastrowid,
                              {"handover_id": handover_id, "request_no": request_no, "receipt_type": receipt_type})
            return self._receipt_dict(self._row("receipts", cur.lastrowid))

        # 按请求号幂等：重复回执沿用首次结果；写入失败回滚后按请求号重试仍得到原记录。
        return self.requests.idempotent(request_no, "receipt", write_fn, find_existing)

    def _handover_dict(self, row: sqlite3.Row) -> dict:
        return {"id": row["id"], "batch_id": row["batch_id"], "handover_no": row["handover_no"],
                "from_factory_id": row["from_factory_id"], "to_factory_id": row["to_factory_id"],
                "status": row["status"], "evidence_frozen": bool(row["evidence_frozen"]),
                "created_by": row["created_by"], "created_at": row["created_at"],
                "confirmed_by": row["confirmed_by"], "confirmed_at": row["confirmed_at"]}

    def _receipt_dict(self, row: sqlite3.Row) -> dict:
        return {"id": row["id"], "handover_id": row["handover_id"], "request_no": row["request_no"],
                "receipt_type": row["receipt_type"], "content": row["content"],
                "recorded_by": row["recorded_by"], "recorded_at": row["recorded_at"]}


class BatchService:
    def __init__(self, store: Store):
        self.store, self.conn = store, store.conn
        self.trace = TraceService(store)
        self.judgment = JudgmentService(self.conn)
        self.handovers = HandoverService(store, self)

    @staticmethod
    def _actor(actor: str | None, role: str | None, allowed: set[str]) -> str:
        if not actor: raise ApiError(401, "缺少身份")
        if role not in allowed: raise ApiError(403, "角色无权执行此操作")
        return actor

    def _row(self, table: str, identity: int) -> sqlite3.Row:
        row = self.conn.execute(f"SELECT * FROM {table} WHERE id=?", (identity,)).fetchone()
        if not row: raise ApiError(404, "对象不存在")
        return row

    def _factory_check(self, actor: str, factory_id: int, batch: sqlite3.Row | None = None) -> None:
        factory = self.conn.execute("SELECT * FROM factories WHERE id=?", (factory_id,)).fetchone()
        if not factory: raise ApiError(404, "工厂不存在")
        if batch is not None and int(batch["factory_id"]) != int(factory_id):
            raise ApiError(403, "不能修改其他工厂的批次")

    def _can_modify_evidence(self, batch: sqlite3.Row) -> bool:
        """证据可修改：被拒批次不可改，其余（含已放行）均可——证据更新会让放行结论失效重算。"""
        return batch["state"] != "rejected"

    def _invalidate_decisions(self, batch_id: int, source: str) -> None:
        """证据更新后，受影响的放行结论失效，需重算。source 写明失效来源。"""
        self.conn.execute("""UPDATE decisions SET invalidated=1,invalidation_source=?,invalidated_at=?
                             WHERE batch_id=? AND invalidated=0""", (source, now(), batch_id))

    def register_factory(self, actor: str | None, role: str | None, code: str, name: str, country: str) -> dict:
        actor = self._actor(actor, role, {"qa"})
        if not code or not name: raise ApiError(400, "工厂代号和名称不能为空")
        try:
            with self.conn:
                cur = self.conn.execute("INSERT INTO factories(code,name,country) VALUES(?,?,?)", (code, name, country))
                self.trace.record(actor, "factory.register", "factory", cur.lastrowid, {"code": code})
        except sqlite3.IntegrityError as exc: raise ApiError(409, "工厂代号已存在") from exc
        return {"id": cur.lastrowid, "code": code, "name": name, "country": country}

    def create_batch(self, actor: str | None, role: str | None, factory_id: int, batch_no: str, product: str, mfg_date: str, expiry_date: str) -> dict:
        actor = self._actor(actor, role, {"operator"})
        self._factory_check(actor, factory_id)
        if not batch_no.strip() or not product.strip() or expiry_date <= mfg_date: raise ApiError(400, "批号、产品或有效期不合法")
        stamp = now()
        try:
            with self.conn:
                cur = self.conn.execute("""INSERT INTO batches(factory_id,batch_no,product,mfg_date,expiry_date,state,created_by,created_at,updated_at)
                                         VALUES(?,?,?,?,?, 'manufactured',?,?,?)""",
                                        (factory_id, batch_no, product, mfg_date, expiry_date, actor, stamp, stamp))
                self.trace.record(actor, "batch.create", "batch", cur.lastrowid, {"factory_id": factory_id, "batch_no": batch_no})
        except sqlite3.IntegrityError as exc: raise ApiError(409, "该工厂批号已存在") from exc
        return self._batch_dict(self._row("batches", cur.lastrowid))

    def add_deviation(self, actor: str | None, role: str | None, factory_id: int, batch_id: int, severity: str, title: str, due_at: str | None, expected_revision: int) -> dict:
        actor = self._actor(actor, role, {"operator", "inspector"})
        batch = self._row("batches", batch_id); self._factory_check(actor, factory_id, batch)
        if severity not in {"critical", "minor"} or not title.strip(): raise ApiError(400, "偏差等级或描述不合法")
        if not self._can_modify_evidence(batch): raise ApiError(409, "已终态批次不能新增偏差")
        with self.conn:
            cur = self.conn.execute("""INSERT INTO deviations(batch_id,severity,title,due_at,status,created_by,created_at)
                                     VALUES(?,?,?,?,'open',?,?)""", (batch_id, severity, title, due_at, actor, now()))
            self._advance_batch(batch_id, expected_revision, "investigation")
            self._invalidate_decisions(batch_id, f"deviation:{cur.lastrowid}")
            self.trace.record(actor, "deviation.open", "deviation", cur.lastrowid, {"batch_id": batch_id, "severity": severity})
        return self._deviation_dict(self._row("deviations", cur.lastrowid))

    def close_deviation(self, actor: str | None, role: str | None, deviation_id: int, corrective_action: str, expected_revision: int) -> dict:
        actor = self._actor(actor, role, {"qa"})
        deviation = self._row("deviations", deviation_id); batch = self._row("batches", deviation["batch_id"])
        if deviation["status"] != "open": raise ApiError(409, "偏差已经关闭")
        if not corrective_action.strip(): raise ApiError(400, "必须填写纠正措施")
        if not self._can_modify_evidence(batch): raise ApiError(409, "已终态批次不能关闭偏差")
        with self.conn:
            self.conn.execute("UPDATE deviations SET status='closed',corrective_action=?,closed_by=?,closed_at=? WHERE id=? AND status='open'",
                              (corrective_action, actor, now(), deviation_id))
            self._advance_batch(batch["id"], expected_revision, "investigation")
            self._invalidate_decisions(batch["id"], f"deviation:{deviation_id}")
            self.trace.record(actor, "deviation.close", "deviation", deviation_id, {"batch_id": batch["id"], "corrective_action": corrective_action})
        return self._deviation_dict(self._row("deviations", deviation_id))

    def approve_exception(self, actor: str | None, role: str | None, deviation_id: int, reason: str, until: str, expected_revision: int) -> dict:
        actor = self._actor(actor, role, {"qa"})
        deviation = self._row("deviations", deviation_id); batch = self._row("batches", deviation["batch_id"])
        if deviation["severity"] == "critical": raise ApiError(409, "关键偏差不允许例外批准")
        if deviation["status"] != "open" or not reason.strip() or not after_now(until): raise ApiError(400, "例外原因或有效期不合法")
        if not self._can_modify_evidence(batch): raise ApiError(409, "已终态批次不能批准例外")
        with self.conn:
            self.conn.execute("UPDATE deviations SET exception_reason=?,exception_until=?,exception_approved_by=? WHERE id=?", (reason, until, actor, deviation_id))
            self._advance_batch(batch["id"], expected_revision, batch["state"])
            self._invalidate_decisions(batch["id"], f"deviation:{deviation_id}")
            self.trace.record(actor, "deviation.exception", "deviation", deviation_id, {"batch_id": batch["id"], "reason": reason, "until": until})
        return self._deviation_dict(self._row("deviations", deviation_id))

    def record_test(self, actor: str | None, role: str | None, factory_id: int, batch_id: int, test_type: str, result: float, spec_min: float, spec_max: float, expected_revision: int) -> dict:
        actor = self._actor(actor, role, {"lab"})
        batch = self._row("batches", batch_id); self._factory_check(actor, factory_id, batch)
        if not test_type.strip() or spec_min > spec_max: raise ApiError(400, "检验项目或标准不合法")
        if not self._can_modify_evidence(batch): raise ApiError(409, "终态批次不能补录检验")
        round_no = self.conn.execute("SELECT COALESCE(MAX(round),0)+1 FROM tests WHERE batch_id=? AND test_type=?", (batch_id, test_type)).fetchone()[0]
        passed = int(spec_min <= result <= spec_max)
        with self.conn:
            cur = self.conn.execute("""INSERT INTO tests(batch_id,test_type,result,spec_min,spec_max,passed,round,recorded_by,created_at)
                                     VALUES(?,?,?,?,?,?,?,?,?)""", (batch_id, test_type, result, spec_min, spec_max, passed, round_no, actor, now()))
            self._advance_batch(batch_id, expected_revision, "investigation" if (batch["state"] == "awaiting_resample" or not passed) else batch["state"])
            self._invalidate_decisions(batch_id, f"test:{cur.lastrowid}")
            self.trace.record(actor, "test.record", "batch", batch_id, {"test_type": test_type, "result": result, "passed": bool(passed), "round": round_no})
        return self._test_dict(self._row("tests", cur.lastrowid))

    def plan_rework(self, actor: str | None, role: str | None, factory_id: int, batch_id: int, description: str, expected_revision: int) -> dict:
        actor = self._actor(actor, role, {"operator"})
        batch = self._row("batches", batch_id); self._factory_check(actor, factory_id, batch)
        if not self._can_modify_evidence(batch): raise ApiError(409, "终态批次不能返工")
        with self.conn:
            cur = self.conn.execute("INSERT INTO rework(batch_id,description,status,created_by,created_at) VALUES(?,?,'planned',?,?)", (batch_id, description, actor, now()))
            self._advance_batch(batch_id, expected_revision, "investigation")
            self._invalidate_decisions(batch_id, f"rework:{cur.lastrowid}")
            self.trace.record(actor, "rework.plan", "rework", cur.lastrowid, {"batch_id": batch_id, "description": description})
        return dict(self._row("rework", cur.lastrowid))

    def complete_rework(self, actor: str | None, role: str | None, factory_id: int, rework_id: int, expected_revision: int) -> dict:
        actor = self._actor(actor, role, {"operator"})
        row = self._row("rework", rework_id); batch = self._row("batches", row["batch_id"]); self._factory_check(actor, factory_id, batch)
        if row["status"] != "planned": raise ApiError(409, "返工记录已经完成")
        if not self._can_modify_evidence(batch): raise ApiError(409, "终态批次不能完成返工")
        with self.conn:
            self.conn.execute("UPDATE rework SET status='completed',completed_by=?,completed_at=? WHERE id=?", (actor, now(), rework_id))
            self._advance_batch(batch["id"], expected_revision, "investigation")
            self._invalidate_decisions(batch["id"], f"rework:{rework_id}")
            self.trace.record(actor, "rework.complete", "rework", rework_id, {"batch_id": batch["id"]})
        return dict(self._row("rework", rework_id))

    def record_supplier_change(self, actor: str | None, role: str | None, factory_id: int, batch_id: int, supplier: str, change_type: str, description: str, expected_revision: int) -> dict:
        actor = self._actor(actor, role, {"operator", "qa"})
        batch = self._row("batches", batch_id); self._factory_check(actor, factory_id, batch)
        if not self._can_modify_evidence(batch): raise ApiError(409, "终态批次不能关联供应商变更")
        with self.conn:
            cur = self.conn.execute("INSERT INTO supplier_changes(batch_id,supplier,change_type,description,recorded_by,created_at) VALUES(?,?,?,?,?,?)",
                                    (batch_id, supplier, change_type, description, actor, now()))
            self._advance_batch(batch_id, expected_revision, "investigation")
            self._invalidate_decisions(batch_id, f"supplier_change:{cur.lastrowid}")
            self.trace.record(actor, "supplier_change.record", "batch", batch_id, {"supplier": supplier, "change_type": change_type})
        return dict(self._row("supplier_changes", cur.lastrowid))

    def record_stability(self, actor: str | None, role: str | None, factory_id: int, batch_id: int, condition: str, timepoint: str, result: float, spec_limit: float, expected_revision: int) -> dict:
        actor = self._actor(actor, role, {"lab"})
        batch = self._row("batches", batch_id); self._factory_check(actor, factory_id, batch)
        if not self._can_modify_evidence(batch): raise ApiError(409, "终态批次不能补录稳定性数据")
        passed = int(result <= spec_limit)
        with self.conn:
            cur = self.conn.execute("""INSERT INTO stability(batch_id,condition,timepoint,result,spec_limit,passed,recorded_by,created_at)
                                     VALUES(?,?,?,?,?,?,?,?)""", (batch_id, condition, timepoint, result, spec_limit, passed, actor, now()))
            self._advance_batch(batch_id, expected_revision, batch["state"])
            self._invalidate_decisions(batch_id, f"stability:{cur.lastrowid}")
            self.trace.record(actor, "stability.record", "batch", batch_id, {"condition": condition, "timepoint": timepoint, "passed": bool(passed)})
        return dict(self._row("stability", cur.lastrowid))

    def decide(self, actor: str | None, role: str | None, batch_id: int, decision: str, rationale: str, expected_revision: int, exception_code: str = "") -> dict:
        actor = self._actor(actor, role, {"qa"})
        batch = self._row("batches", batch_id)
        if decision not in {"release", "reject", "conditional", "resample"}: raise ApiError(400, "放行决定不合法")
        if batch["state"] in {"released", "rejected"}:
            valid = self.conn.execute("SELECT id FROM decisions WHERE batch_id=? AND invalidated=0 LIMIT 1", (batch_id,)).fetchone()
            if valid: raise ApiError(409, "批次已经是终态")
        if int(expected_revision) != int(batch["revision"]): raise ApiError(409, "批次已被其他工厂或质量人员修改，请刷新版本")
        if not rationale.strip(): raise ApiError(400, "必须填写决定依据")
        new_state = self.judgment.evaluate(batch, decision, exception_code)
        with self.conn:
            cur = self.conn.execute("""INSERT INTO decisions(batch_id,revision,decision,rationale,exception_code,decided_by,created_at)
                                     VALUES(?,?,?,?,?,?,?)""", (batch_id, batch["revision"], decision, rationale, exception_code or None, actor, now()))
            updated = self.conn.execute("UPDATE batches SET state=?,revision=revision+1,updated_at=? WHERE id=? AND revision=?",
                                        (new_state, now(), batch_id, expected_revision))
            if updated.rowcount != 1: raise ApiError(409, "并发放行冲突")
            self.trace.record(actor, "batch.decision", "batch", batch_id, {"decision": decision, "revision": batch["revision"], "state": new_state, "exception_code": exception_code})
        return {"decision": dict(self._row("decisions", cur.lastrowid)), "batch": self.batch_detail(batch_id)["batch"]}

    def _advance_batch(self, batch_id: int, expected_revision: int, next_state: str) -> None:
        batch = self._row("batches", batch_id)
        if not self._can_modify_evidence(batch): raise ApiError(409, "终态批次不可修改")
        if int(expected_revision) != int(batch["revision"]): raise ApiError(409, "批次版本冲突")
        cur = self.conn.execute("UPDATE batches SET state=?,revision=revision+1,updated_at=? WHERE id=? AND revision=?",
                                (next_state, now(), batch_id, expected_revision))
        if cur.rowcount != 1: raise ApiError(409, "并发更新冲突")

    def batch_detail(self, batch_id: int) -> dict:
        batch = self._batch_dict(self._row("batches", batch_id))
        def rows(name: str) -> list[dict]: return [dict(row) for row in self.conn.execute(f"SELECT * FROM {name} WHERE batch_id=? ORDER BY id", (batch_id,))]
        handover = self.conn.execute("SELECT * FROM handovers WHERE batch_id=? ORDER BY id DESC LIMIT 1", (batch_id,)).fetchone()
        receipts = []
        if handover:
            receipts = [dict(row) for row in self.conn.execute("SELECT * FROM receipts WHERE handover_id=? ORDER BY id", (handover["id"],))]
        return {"batch": batch, "deviations": rows("deviations"), "tests": rows("tests"), "rework": rows("rework"),
                "supplier_changes": rows("supplier_changes"), "stability": rows("stability"),
                "decisions": rows("decisions"), "handover": dict(handover) if handover else None, "receipts": receipts}

    def _batch_dict(self, row: sqlite3.Row) -> dict:
        return {"id": row["id"], "factory_id": row["factory_id"], "batch_no": row["batch_no"], "product": row["product"],
                "mfg_date": row["mfg_date"], "expiry_date": row["expiry_date"], "state": row["state"], "revision": row["revision"],
                "handover_id": row["handover_id"] if "handover_id" in row.keys() else None}

    @staticmethod
    def _deviation_dict(row: sqlite3.Row) -> dict:
        return {"id": row["id"], "batch_id": row["batch_id"], "severity": row["severity"], "title": row["title"], "due_at": row["due_at"],
                "status": row["status"], "corrective_action": row["corrective_action"], "exception_reason": row["exception_reason"],
                "exception_until": row["exception_until"], "exception_approved_by": row["exception_approved_by"]}

    @staticmethod
    def _test_dict(row: sqlite3.Row) -> dict:
        return {"id": row["id"], "batch_id": row["batch_id"], "test_type": row["test_type"], "result": row["result"],
                "spec_min": row["spec_min"], "spec_max": row["spec_max"], "passed": bool(row["passed"]), "round": row["round"]}

    def state(self) -> dict:
        return {"factories": [dict(row) for row in self.conn.execute("SELECT * FROM factories ORDER BY id")],
                "batches": [self._batch_dict(row) for row in self.conn.execute("SELECT * FROM batches ORDER BY id DESC")],
                "handovers": [dict(row) for row in self.conn.execute("SELECT * FROM handovers ORDER BY id DESC")],
                "audits": [dict(row) for row in self.conn.execute("SELECT * FROM audit_log ORDER BY id DESC LIMIT 30")]}

    def seed(self) -> None:
        if not self.conn.execute("SELECT id FROM factories LIMIT 1").fetchone():
            self.register_factory("qa-demo", "qa", "F-DEMO", "演示工厂", "CN")


class Handler(BaseHTTPRequestHandler):
    service: BatchService

    def log_message(self, fmt: str, *args: object) -> None: sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))
    def _send(self, status: int, body: object) -> None:
        data = json.dumps(body, ensure_ascii=False).encode(); self.send_response(status); self.send_header("Content-Type", "application/json; charset=utf-8"); self.send_header("Content-Length", str(len(data))); self.end_headers(); self.wfile.write(data)
    def _body(self) -> dict:
        size = int(self.headers.get("Content-Length", "0"))
        try: return json.loads(self.rfile.read(size)) if size else {}
        except json.JSONDecodeError as exc: raise ApiError(400, "JSON 请求体无效") from exc
    def _parts(self) -> list[str]: return [p for p in urlparse(self.path).path.strip("/").split("/") if p]

    def do_GET(self) -> None:
        try:
            p = self._parts()
            if p in (["health"], ["api", "health"]): out = {"status": "ok"}
            elif p == ["api", "state"]: out = self.service.state()
            elif len(p) == 3 and p[:2] == ["api", "batches"]: out = self.service.batch_detail(int(p[2]))
            elif not p:
                page = (Path(__file__).parent / "static" / "index.html").read_bytes(); self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(page))); self.end_headers(); self.wfile.write(page); return
            else: raise ApiError(404, "接口不存在")
            self._send(200, out)
        except ApiError as exc: self._send(exc.status, {"error": exc.message})
        except Exception as exc: self._send(500, {"error": str(exc)})

    def do_POST(self) -> None:
        try:
            p, b = self._parts(), self._body(); actor, role = self.headers.get("X-Actor"), self.headers.get("X-Role")
            if p == ["api", "factories"]: out = self.service.register_factory(actor, role, b.get("code", ""), b.get("name", ""), b.get("country", ""))
            elif p == ["api", "batches"]: out = self.service.create_batch(actor, role, int(b.get("factory_id", 0)), b.get("batch_no", ""), b.get("product", ""), b.get("mfg_date", ""), b.get("expiry_date", ""))
            elif len(p) == 4 and p[:2] == ["api", "batches"] and p[3] == "deviations": out = self.service.add_deviation(actor, role, int(b.get("factory_id", 0)), int(p[2]), b.get("severity", ""), b.get("title", ""), b.get("due_at"), int(b.get("expected_revision", -1)))
            elif len(p) == 4 and p[:2] == ["api", "deviations"] and p[3] == "close": out = self.service.close_deviation(actor, role, int(p[2]), b.get("corrective_action", ""), int(b.get("expected_revision", -1)))
            elif len(p) == 4 and p[:2] == ["api", "deviations"] and p[3] == "exception": out = self.service.approve_exception(actor, role, int(p[2]), b.get("reason", ""), b.get("until", ""), int(b.get("expected_revision", -1)))
            elif len(p) == 4 and p[:2] == ["api", "batches"] and p[3] == "tests": out = self.service.record_test(actor, role, int(b.get("factory_id", 0)), int(p[2]), b.get("test_type", ""), float(b.get("result", 0)), float(b.get("spec_min", 0)), float(b.get("spec_max", 0)), int(b.get("expected_revision", -1)))
            elif len(p) == 4 and p[:2] == ["api", "batches"] and p[3] == "rework": out = self.service.plan_rework(actor, role, int(b.get("factory_id", 0)), int(p[2]), b.get("description", ""), int(b.get("expected_revision", -1)))
            elif len(p) == 4 and p[:2] == ["api", "rework"] and p[3] == "complete": out = self.service.complete_rework(actor, role, int(b.get("factory_id", 0)), int(p[2]), int(b.get("expected_revision", -1)))
            elif len(p) == 4 and p[:2] == ["api", "batches"] and p[3] == "supplier-changes": out = self.service.record_supplier_change(actor, role, int(b.get("factory_id", 0)), int(p[2]), b.get("supplier", ""), b.get("change_type", ""), b.get("description", ""), int(b.get("expected_revision", -1)))
            elif len(p) == 4 and p[:2] == ["api", "batches"] and p[3] == "stability": out = self.service.record_stability(actor, role, int(b.get("factory_id", 0)), int(p[2]), b.get("condition", ""), b.get("timepoint", ""), float(b.get("result", 0)), float(b.get("spec_limit", 0)), int(b.get("expected_revision", -1)))
            elif len(p) == 4 and p[:2] == ["api", "batches"] and p[3] == "decide": out = self.service.decide(actor, role, int(p[2]), b.get("decision", ""), b.get("rationale", ""), int(b.get("expected_revision", -1)), b.get("exception_code", ""))
            elif len(p) == 4 and p[:2] == ["api", "batches"] and p[3] == "handovers": out = self.service.handovers.create_handover(actor, role, int(b.get("factory_id", 0)), int(p[2]), int(b.get("to_factory_id", 0)), b.get("handover_no", ""))
            elif len(p) == 4 and p[:2] == ["api", "handovers"] and p[3] == "confirm": out = self.service.handovers.confirm_handover(actor, role, int(p[2]))
            elif len(p) == 4 and p[:2] == ["api", "handovers"] and p[3] == "cancel": out = self.service.handovers.cancel_handover(actor, role, int(p[2]))
            elif len(p) == 4 and p[:2] == ["api", "handovers"] and p[3] == "receipts": out = self.service.handovers.append_receipt(actor, role, int(b.get("factory_id", 0)), int(p[2]), b.get("request_no", ""), b.get("receipt_type", ""), b.get("content", ""))
            else: raise ApiError(404, "接口不存在")
            self._send(200, out)
        except ApiError as exc: self._send(exc.status, {"error": exc.message})
        except (ValueError, TypeError, sqlite3.IntegrityError) as exc: self._send(400, {"error": str(exc)})
        except Exception as exc: self._send(500, {"error": str(exc)})


def run(port: int, db_path: str, seed: bool) -> None:
    store = Store(db_path); service = BatchService(store)
    if seed: service.seed()
    Handler.service = service
    print(f"batch release listening on http://127.0.0.1:{port}")
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--port", type=int, default=8214); parser.add_argument("--db", default=str(DB_PATH)); parser.add_argument("--init", action="store_true"); parser.add_argument("--seed", action="store_true")
    args = parser.parse_args()
    if args.init: Store(args.db).close()
    if args.seed or not args.init: run(args.port, args.db, args.seed)


if __name__ == "__main__": main()
