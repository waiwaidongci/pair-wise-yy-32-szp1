#!/usr/bin/env python3
"""Pharmaceutical batch deviation, rework and release decision service."""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import threading
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
        self.write_lock = threading.RLock()
        self.conn.execute("PRAGMA foreign_keys=ON"); self.conn.execute("PRAGMA journal_mode=WAL"); self.init_schema()

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
          exception_code TEXT, decided_by TEXT NOT NULL, created_at TEXT NOT NULL, UNIQUE(batch_id,revision)
        );
        CREATE TABLE IF NOT EXISTS audit_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT NOT NULL, actor TEXT NOT NULL, action TEXT NOT NULL,
          entity_type TEXT NOT NULL, entity_id TEXT NOT NULL, details_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS handovers (
          id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id INTEGER NOT NULL REFERENCES batches(id),
          from_factory_id INTEGER NOT NULL REFERENCES factories(id), to_factory_id INTEGER NOT NULL REFERENCES factories(id),
          status TEXT NOT NULL CHECK(status IN ('proposed','confirmed')),
          evidence_revision INTEGER NOT NULL, snapshot_json TEXT NOT NULL,
          proposed_by TEXT NOT NULL, proposed_at TEXT NOT NULL,
          confirmed_by TEXT, confirmed_at TEXT
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_handovers_one ON handovers(batch_id) WHERE status='confirmed';
        CREATE TABLE IF NOT EXISTS receipts (
          id INTEGER PRIMARY KEY AUTOINCREMENT, handover_id INTEGER NOT NULL REFERENCES handovers(id),
          batch_id INTEGER NOT NULL REFERENCES batches(id),
          request_no TEXT NOT NULL, evidence_type TEXT NOT NULL CHECK(evidence_type IN ('deviation','test','rework','stability')),
          content TEXT NOT NULL, evidence_version INTEGER NOT NULL,
          recorded_by TEXT NOT NULL, created_at TEXT NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_receipts_request ON receipts(request_no);
        CREATE TABLE IF NOT EXISTS release_conclusions (
          id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id INTEGER NOT NULL REFERENCES batches(id),
          handover_id INTEGER NOT NULL REFERENCES handovers(id),
          decision TEXT NOT NULL CHECK(decision IN ('release','reject','conditional')),
          rationale TEXT NOT NULL, exception_code TEXT,
          evidence_revision INTEGER NOT NULL, last_receipt_id INTEGER,
          status TEXT NOT NULL CHECK(status IN ('active','invalidated')),
          invalidated_by_receipt_id INTEGER REFERENCES receipts(id),
          invalidated_at TEXT, decided_by TEXT NOT NULL, created_at TEXT NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_conclusions_one_active ON release_conclusions(batch_id) WHERE status='active';
        CREATE TABLE IF NOT EXISTS request_ledger (
          request_no TEXT PRIMARY KEY, kind TEXT NOT NULL, actor TEXT NOT NULL,
          status TEXT NOT NULL CHECK(status IN ('processing','failed','completed')),
          error TEXT, attempt INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        """)
        self.conn.commit()

    def audit(self, actor: str, action: str, entity_type: str, entity_id: object, details: dict) -> None:
        self.conn.execute("INSERT INTO audit_log(at,actor,action,entity_type,entity_id,details_json) VALUES(?,?,?,?,?,?)",
                          (now(), actor, action, entity_type, str(entity_id), j(details)))

    def close(self) -> None:
        self.conn.close()


class RequestLedger:
    """按请求号保证幂等：重复请求沿用首次结果；失败保留记录并按请求号重试。"""

    def __init__(self, store: Store): self.store, self.conn = store, store.conn

    def _row(self, request_no: str) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM request_ledger WHERE request_no=?", (request_no,)).fetchone()

    def begin(self, request_no: str | None, kind: str, actor: str) -> sqlite3.Row | None:
        """登记一次请求。已完成的请求返回首次记录（调用方据此回放），失败/处理中则重置为重试。"""
        if not request_no or not request_no.strip(): raise ApiError(400, "必须提供请求号 request_no")
        request_no = request_no.strip(); stamp = now()
        with self.store.write_lock:
            existing = self._row(request_no)
            if existing is None:
                self.conn.execute("""INSERT INTO request_ledger(request_no,kind,actor,status,attempt,created_at,updated_at)
                                   VALUES(?,?,?, 'processing',1,?,?)""", (request_no, kind, actor, stamp, stamp)); self.conn.commit()
            elif existing["status"] == "completed":
                return existing
            else:
                self.conn.execute("UPDATE request_ledger SET status='processing',error=NULL,attempt=attempt+1,updated_at=? WHERE request_no=?",
                                  (stamp, request_no)); self.conn.commit()
        return None

    def complete(self, request_no: str) -> None:
        self.conn.execute("UPDATE request_ledger SET status='completed',error=NULL,updated_at=? WHERE request_no=?", (now(), request_no))
        self.conn.commit()

    def fail(self, request_no: str, message: str) -> None:
        self.conn.execute("UPDATE request_ledger SET status='failed',error=?,updated_at=? WHERE request_no=?", (message, now(), request_no))
        self.conn.commit()


class BatchService:
    def __init__(self, store: Store): self.store, self.conn = store, store.conn

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

    def _guard_handover_frozen(self, batch_id: int) -> None:
        handover = self.conn.execute("SELECT id,status FROM handovers WHERE batch_id=? ORDER BY id DESC LIMIT 1", (batch_id,)).fetchone()
        if handover is not None:
            label = "被新厂确认" if handover["status"] == "confirmed" else "发起"
            raise ApiError(409, f"批次已{label}交接，偏差、复测、返工与稳定性依据已冻结，前厂仅可追加回执")

    def register_factory(self, actor: str | None, role: str | None, code: str, name: str, country: str) -> dict:
        actor = self._actor(actor, role, {"qa"})
        if not code or not name: raise ApiError(400, "工厂代号和名称不能为空")
        try:
            with self.conn:
                cur = self.conn.execute("INSERT INTO factories(code,name,country) VALUES(?,?,?)", (code, name, country))
                self.store.audit(actor, "factory.register", "factory", cur.lastrowid, {"code": code})
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
                self.store.audit(actor, "batch.create", "batch", cur.lastrowid, {"factory_id": factory_id, "batch_no": batch_no})
        except sqlite3.IntegrityError as exc: raise ApiError(409, "该工厂批号已存在") from exc
        return self._batch_dict(self._row("batches", cur.lastrowid))

    def add_deviation(self, actor: str | None, role: str | None, factory_id: int, batch_id: int, severity: str, title: str, due_at: str | None, expected_revision: int) -> dict:
        actor = self._actor(actor, role, {"operator", "inspector"})
        batch = self._row("batches", batch_id); self._factory_check(actor, factory_id, batch)
        self._guard_handover_frozen(batch_id)
        if severity not in {"critical", "minor"} or not title.strip(): raise ApiError(400, "偏差等级或描述不合法")
        if batch["state"] in {"released", "rejected"}: raise ApiError(409, "已终态批次不能新增偏差")
        with self.conn:
            cur = self.conn.execute("""INSERT INTO deviations(batch_id,severity,title,due_at,status,created_by,created_at)
                                     VALUES(?,?,?,?,'open',?,?)""", (batch_id, severity, title, due_at, actor, now()))
            self._advance_batch(batch_id, expected_revision, "investigation")
            self.store.audit(actor, "deviation.open", "deviation", cur.lastrowid, {"batch_id": batch_id, "severity": severity})
        return self._deviation_dict(self._row("deviations", cur.lastrowid))

    def close_deviation(self, actor: str | None, role: str | None, deviation_id: int, corrective_action: str, expected_revision: int) -> dict:
        actor = self._actor(actor, role, {"qa"})
        deviation = self._row("deviations", deviation_id); batch = self._row("batches", deviation["batch_id"])
        self._guard_handover_frozen(batch["id"])
        if deviation["status"] != "open": raise ApiError(409, "偏差已经关闭")
        if not corrective_action.strip(): raise ApiError(400, "必须填写纠正措施")
        with self.conn:
            self.conn.execute("UPDATE deviations SET status='closed',corrective_action=?,closed_by=?,closed_at=? WHERE id=? AND status='open'",
                              (corrective_action, actor, now(), deviation_id))
            self._advance_batch(batch["id"], expected_revision, "investigation")
            self.store.audit(actor, "deviation.close", "deviation", deviation_id, {"batch_id": batch["id"], "corrective_action": corrective_action})
        return self._deviation_dict(self._row("deviations", deviation_id))

    def approve_exception(self, actor: str | None, role: str | None, deviation_id: int, reason: str, until: str, expected_revision: int) -> dict:
        actor = self._actor(actor, role, {"qa"})
        deviation = self._row("deviations", deviation_id); batch = self._row("batches", deviation["batch_id"])
        self._guard_handover_frozen(batch["id"])
        if deviation["severity"] == "critical": raise ApiError(409, "关键偏差不允许例外批准")
        if deviation["status"] != "open" or not reason.strip() or not after_now(until): raise ApiError(400, "例外原因或有效期不合法")
        with self.conn:
            self.conn.execute("UPDATE deviations SET exception_reason=?,exception_until=?,exception_approved_by=? WHERE id=?", (reason, until, actor, deviation_id))
            self._advance_batch(batch["id"], expected_revision, batch["state"])
            self.store.audit(actor, "deviation.exception", "deviation", deviation_id, {"batch_id": batch["id"], "reason": reason, "until": until})
        return self._deviation_dict(self._row("deviations", deviation_id))

    def record_test(self, actor: str | None, role: str | None, factory_id: int, batch_id: int, test_type: str, result: float, spec_min: float, spec_max: float, expected_revision: int) -> dict:
        actor = self._actor(actor, role, {"lab"})
        batch = self._row("batches", batch_id); self._factory_check(actor, factory_id, batch)
        self._guard_handover_frozen(batch_id)
        if not test_type.strip() or spec_min > spec_max: raise ApiError(400, "检验项目或标准不合法")
        if batch["state"] in {"released", "rejected"}: raise ApiError(409, "终态批次不能补录检验")
        round_no = self.conn.execute("SELECT COALESCE(MAX(round),0)+1 FROM tests WHERE batch_id=? AND test_type=?", (batch_id, test_type)).fetchone()[0]
        passed = int(spec_min <= result <= spec_max)
        with self.conn:
            cur = self.conn.execute("""INSERT INTO tests(batch_id,test_type,result,spec_min,spec_max,passed,round,recorded_by,created_at)
                                     VALUES(?,?,?,?,?,?,?,?,?)""", (batch_id, test_type, result, spec_min, spec_max, passed, round_no, actor, now()))
            self._advance_batch(batch_id, expected_revision, "investigation" if (batch["state"] == "awaiting_resample" or not passed) else batch["state"])
            self.store.audit(actor, "test.record", "batch", batch_id, {"test_type": test_type, "result": result, "passed": bool(passed), "round": round_no})
        return self._test_dict(self._row("tests", cur.lastrowid))

    def plan_rework(self, actor: str | None, role: str | None, factory_id: int, batch_id: int, description: str, expected_revision: int) -> dict:
        actor = self._actor(actor, role, {"operator"})
        batch = self._row("batches", batch_id); self._factory_check(actor, factory_id, batch)
        self._guard_handover_frozen(batch_id)
        if batch["state"] in {"released", "rejected"}: raise ApiError(409, "终态批次不能返工")
        with self.conn:
            cur = self.conn.execute("INSERT INTO rework(batch_id,description,status,created_by,created_at) VALUES(?,?,'planned',?,?)", (batch_id, description, actor, now()))
            self._advance_batch(batch_id, expected_revision, "investigation")
            self.store.audit(actor, "rework.plan", "rework", cur.lastrowid, {"batch_id": batch_id, "description": description})
        return dict(self._row("rework", cur.lastrowid))

    def complete_rework(self, actor: str | None, role: str | None, factory_id: int, rework_id: int, expected_revision: int) -> dict:
        actor = self._actor(actor, role, {"operator"})
        row = self._row("rework", rework_id); batch = self._row("batches", row["batch_id"]); self._factory_check(actor, factory_id, batch)
        self._guard_handover_frozen(batch["id"])
        if row["status"] != "planned": raise ApiError(409, "返工记录已经完成")
        with self.conn:
            self.conn.execute("UPDATE rework SET status='completed',completed_by=?,completed_at=? WHERE id=?", (actor, now(), rework_id))
            self._advance_batch(batch["id"], expected_revision, "investigation")
            self.store.audit(actor, "rework.complete", "rework", rework_id, {"batch_id": batch["id"]})
        return dict(self._row("rework", rework_id))

    def record_supplier_change(self, actor: str | None, role: str | None, factory_id: int, batch_id: int, supplier: str, change_type: str, description: str, expected_revision: int) -> dict:
        actor = self._actor(actor, role, {"operator", "qa"})
        batch = self._row("batches", batch_id); self._factory_check(actor, factory_id, batch)
        with self.conn:
            cur = self.conn.execute("INSERT INTO supplier_changes(batch_id,supplier,change_type,description,recorded_by,created_at) VALUES(?,?,?,?,?,?)",
                                    (batch_id, supplier, change_type, description, actor, now()))
            self._advance_batch(batch_id, expected_revision, "investigation")
            self.store.audit(actor, "supplier_change.record", "batch", batch_id, {"supplier": supplier, "change_type": change_type})
        return dict(self._row("supplier_changes", cur.lastrowid))

    def record_stability(self, actor: str | None, role: str | None, factory_id: int, batch_id: int, condition: str, timepoint: str, result: float, spec_limit: float, expected_revision: int) -> dict:
        actor = self._actor(actor, role, {"lab"})
        batch = self._row("batches", batch_id); self._factory_check(actor, factory_id, batch)
        self._guard_handover_frozen(batch_id)
        passed = int(result <= spec_limit)
        with self.conn:
            cur = self.conn.execute("INSERT INTO stability(batch_id,condition,timepoint,result,spec_limit,passed,recorded_by,created_at) VALUES(?,?,?,?,?,?,?,?)",
                                    (batch_id, condition, timepoint, result, spec_limit, passed, actor, now()))
            self._advance_batch(batch_id, expected_revision, batch["state"])
            self.store.audit(actor, "stability.record", "batch", batch_id, {"condition": condition, "timepoint": timepoint, "passed": bool(passed)})
        return dict(self._row("stability", cur.lastrowid))

    def decide(self, actor: str | None, role: str | None, batch_id: int, decision: str, rationale: str, expected_revision: int, exception_code: str = "") -> dict:
        actor = self._actor(actor, role, {"qa"})
        batch = self._row("batches", batch_id)
        if decision not in {"release", "reject", "conditional", "resample"}: raise ApiError(400, "放行决定不合法")
        if batch["state"] in {"released", "rejected"}: raise ApiError(409, "批次已经是终态")
        if int(expected_revision) != int(batch["revision"]): raise ApiError(409, "批次已被其他工厂或质量人员修改，请刷新版本")
        if not rationale.strip(): raise ApiError(400, "必须填写决定依据")
        deviations = self.conn.execute("SELECT * FROM deviations WHERE batch_id=? ORDER BY id", (batch_id,)).fetchall()
        open_deviations = [d for d in deviations if d["status"] == "open"]
        latest_tests: dict[str, sqlite3.Row] = {}
        for row in self.conn.execute("SELECT * FROM tests WHERE batch_id=? ORDER BY id", (batch_id,)):
            latest_tests[row["test_type"]] = row
        if decision in {"release", "conditional"} and not latest_tests:
            raise ApiError(409, "放行前至少需要一项检验结果")
        if decision in {"release", "conditional"} and any(not row["passed"] for row in latest_tests.values()):
            raise ApiError(409, "最新检验结果仍有不合格项")
        if decision == "resample":
            if batch["state"] == "conditional": raise ApiError(409, "有条件放行后不能直接改为再取样")
            new_state = "awaiting_resample"
        elif decision == "reject":
            new_state = "rejected"
        elif any(d["severity"] == "critical" for d in open_deviations):
            raise ApiError(409, "未关闭的关键偏差阻止放行")
        elif decision == "release" and open_deviations:
            raise ApiError(409, "仍有未关闭偏差，不能正式放行")
        elif decision == "conditional":
            for deviation in open_deviations:
                if not deviation["exception_reason"] or not after_now(deviation["exception_until"]):
                    raise ApiError(409, f"偏差 {deviation['id']} 没有有效例外批准")
            if not exception_code.strip(): raise ApiError(400, "有条件放行必须提供例外编号")
            new_state = "conditional"
        else:
            new_state = "released"
        with self.conn:
            cur = self.conn.execute("""INSERT INTO decisions(batch_id,revision,decision,rationale,exception_code,decided_by,created_at)
                                     VALUES(?,?,?,?,?,?,?)""", (batch_id, batch["revision"], decision, rationale, exception_code or None, actor, now()))
            updated = self.conn.execute("UPDATE batches SET state=?,revision=revision+1,updated_at=? WHERE id=? AND revision=?",
                                        (new_state, now(), batch_id, expected_revision))
            if updated.rowcount != 1: raise ApiError(409, "并发放行冲突")
            self.store.audit(actor, "batch.decision", "batch", batch_id, {"decision": decision, "revision": batch["revision"], "state": new_state, "exception_code": exception_code})
        return {"decision": dict(self._row("decisions", cur.lastrowid)), "batch": self.batch_detail(batch_id)["batch"]}

    def _advance_batch(self, batch_id: int, expected_revision: int, next_state: str) -> None:
        batch = self._row("batches", batch_id)
        if batch["state"] in {"released", "rejected"}: raise ApiError(409, "终态批次不可修改")
        if int(expected_revision) != int(batch["revision"]): raise ApiError(409, "批次版本冲突")
        cur = self.conn.execute("UPDATE batches SET state=?,revision=revision+1,updated_at=? WHERE id=? AND revision=?",
                                (next_state, now(), batch_id, expected_revision))
        if cur.rowcount != 1: raise ApiError(409, "并发更新冲突")

    def batch_detail(self, batch_id: int) -> dict:
        batch = self._batch_dict(self._row("batches", batch_id))
        def rows(name: str) -> list[dict]: return [dict(row) for row in self.conn.execute(f"SELECT * FROM {name} WHERE batch_id=? ORDER BY id", (batch_id,))]
        handovers = [dict(id=row["id"], status=row["status"], from_factory_id=row["from_factory_id"],
                          to_factory_id=row["to_factory_id"], evidence_revision=row["evidence_revision"],
                          confirmed_at=row["confirmed_at"])
                     for row in self.conn.execute("SELECT * FROM handovers WHERE batch_id=? ORDER BY id", (batch_id,))]
        conclusions = [dict(row) for row in self.conn.execute("SELECT * FROM release_conclusions WHERE batch_id=? ORDER BY id", (batch_id,))]
        return {"batch": batch, "deviations": rows("deviations"), "tests": rows("tests"), "rework": rows("rework"),
                "supplier_changes": rows("supplier_changes"), "stability": rows("stability"),
                "decisions": rows("decisions"), "handovers": handovers, "release_conclusions": conclusions}

    def _batch_dict(self, row: sqlite3.Row) -> dict:
        return {"id": row["id"], "factory_id": row["factory_id"], "batch_no": row["batch_no"], "product": row["product"],
                "mfg_date": row["mfg_date"], "expiry_date": row["expiry_date"], "state": row["state"], "revision": row["revision"]}

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
                "handovers": [dict(id=row["id"], batch_id=row["batch_id"], from_factory_id=row["from_factory_id"],
                                   to_factory_id=row["to_factory_id"], status=row["status"],
                                   evidence_revision=row["evidence_revision"], proposed_by=row["proposed_by"],
                                   proposed_at=row["proposed_at"], confirmed_by=row["confirmed_by"],
                                   confirmed_at=row["confirmed_at"])
                              for row in self.conn.execute("SELECT * FROM handovers ORDER BY id DESC")],
                "receipts": [dict(row) for row in self.conn.execute("SELECT * FROM receipts ORDER BY id DESC")],
                "release_conclusions": [dict(row) for row in self.conn.execute("SELECT * FROM release_conclusions ORDER BY id DESC")],
                "request_ledger": [dict(row) for row in self.conn.execute("SELECT * FROM request_ledger ORDER BY rowid DESC LIMIT 50")],
                "audits": [dict(row) for row in self.conn.execute("SELECT * FROM audit_log ORDER BY id DESC LIMIT 30")]}

    def seed(self) -> None:
        if not self.conn.execute("SELECT id FROM factories LIMIT 1").fetchone():
            self.register_factory("qa-demo", "qa", "F-DEMO", "演示工厂", "CN")


FROZEN_EVIDENCE_TABLES = ("deviations", "tests", "rework", "stability")
FROZEN_EVIDENCE_LABELS = {"deviations": "偏差", "tests": "复测", "rework": "返工", "stability": "稳定性依据"}


class HandoverService:
    """批次与证据交接：交接时冻结四类依据；交接后前厂只能追加回执。"""

    def __init__(self, store: Store):
        self.store, self.conn = store, store.conn
        self.ledger = RequestLedger(store)

    def _handover(self, handover_id: int) -> sqlite3.Row:
        row = self.conn.execute("SELECT * FROM handovers WHERE id=?", (handover_id,)).fetchone()
        if not row: raise ApiError(404, "交接单不存在")
        return row

    @staticmethod
    def _actor(actor: str | None, role: str | None, allowed: set[str]) -> str:
        if not actor: raise ApiError(401, "缺少身份")
        if role not in allowed: raise ApiError(403, "角色无权执行此操作")
        return actor

    def propose(self, actor: str | None, role: str | None, factory_id: int, batch_id: int, to_factory_id: int) -> dict:
        """前厂发起交接：冻结偏差、复测、返工与稳定性依据快照。"""
        actor = self._actor(actor, role, {"operator"})
        batch = self.conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
        if not batch: raise ApiError(404, "批次不存在")
        if int(factory_id) != int(batch["factory_id"]): raise ApiError(403, "不能交接其他工厂的批次")
        target = self.conn.execute("SELECT * FROM factories WHERE id=?", (to_factory_id,)).fetchone()
        if not target: raise ApiError(404, "目标工厂不存在")
        if int(batch["factory_id"]) == int(to_factory_id): raise ApiError(400, "目标工厂不能与前厂相同")
        with self.store.write_lock:
            if self.conn.execute("SELECT id FROM handovers WHERE batch_id=?", (batch_id,)).fetchone():
                raise ApiError(409, "批次已发起交接，不能重复交接")
            snapshot = {"evidence_revision": batch["revision"], "batch_revision": batch["revision"]}
            for table in FROZEN_EVIDENCE_TABLES:
                snapshot[table] = [dict(row) for row in self.conn.execute(f"SELECT * FROM {table} WHERE batch_id=? ORDER BY id", (batch_id,))]
            snapshot_json = j(snapshot); stamp = now()
            cur = self.conn.execute("""INSERT INTO handovers(batch_id,from_factory_id,to_factory_id,status,evidence_revision,snapshot_json,proposed_by,proposed_at)
                                    VALUES(?,?,?, 'proposed',?,?,?,?)""",
                                   (batch_id, batch["factory_id"], to_factory_id, batch["revision"], snapshot_json, actor, stamp))
            self.store.audit(actor, "handover.propose", "handover", cur.lastrowid,
                             {"batch_id": batch_id, "from_factory_id": batch["factory_id"], "to_factory_id": to_factory_id,
                              "evidence_revision": batch["revision"], "frozen": list(FROZEN_EVIDENCE_LABELS.values())})
            self.conn.commit()
        return self.handover_detail(cur.lastrowid)

    def confirm(self, actor: str | None, role: str | None, factory_id: int, handover_id: int) -> dict:
        """新厂 QA 确认交接；并发确认只有一次成功（部分唯一索引 + 状态条件更新）。"""
        actor = self._actor(actor, role, {"qa"})
        handover = self._handover(handover_id)
        if int(factory_id) != int(handover["to_factory_id"]):
            raise ApiError(403, "只有接收新厂质检可以确认交接")
        with self.store.write_lock:
            handover = self._handover(handover_id)
            if handover["status"] == "confirmed": raise ApiError(409, "交接已被确认，重复确认无效")
            cur = self.conn.execute("UPDATE handovers SET status='confirmed',confirmed_by=?,confirmed_at=? WHERE id=? AND status='proposed'",
                                    (actor, now(), handover_id))
            if cur.rowcount != 1: raise ApiError(409, "交接已被其他质检确认")
            self.store.audit(actor, "handover.confirm", "handover", handover_id,
                             {"batch_id": handover["batch_id"], "to_factory_id": handover["to_factory_id"],
                              "evidence_revision": handover["evidence_revision"]})
            self.conn.commit()
        return self.handover_detail(handover_id)

    def append_receipt(self, actor: str | None, role: str | None, factory_id: int, handover_id: int, request_no: str | None,
                       evidence_type: str, content: str) -> dict:
        """交接后前厂只能追加回执。按请求号幂等：重复回执沿用首次结果；写入失败保留原记录、同请求号重试。"""
        actor = self._actor(actor, role, {"operator", "qa"})
        handover = self._handover(handover_id)
        if int(factory_id) != int(handover["from_factory_id"]):
            raise ApiError(403, "只有前厂可以追加回执")
        if handover["status"] != "confirmed": raise ApiError(409, "新厂确认交接后，前厂才能追加回执")
        if evidence_type not in {"deviation", "test", "rework", "stability"}: raise ApiError(400, "回执证据类型不合法")
        if not content.strip(): raise ApiError(400, "回执内容不能为空")
        with self.store.write_lock:
            existing = self.ledger.begin(request_no, "receipt", actor)
            if existing is not None:
                first = self.conn.execute("SELECT * FROM receipts WHERE request_no=?", (existing["request_no"],)).fetchone()
                if first is not None:
                    return self._receipt_dict(first, replayed=True)
            request_no = request_no.strip()
            receipt_id: int | None = None
            try:
                evidence_version = self._current_evidence_version(handover) + 1
                cur = self.conn.execute("""INSERT INTO receipts(handover_id,batch_id,request_no,evidence_type,content,evidence_version,recorded_by,created_at)
                                        VALUES(?,?,?,?,?,?,?,?)""",
                                        (handover_id, handover["batch_id"], request_no, evidence_type, content.strip(),
                                         evidence_version, actor, now()))
                receipt_id = cur.lastrowid
                self._invalidate_conclusions(handover["batch_id"], receipt_id, actor, request_no, evidence_type)
                self.store.audit(actor, "receipt.append", "receipt", receipt_id,
                                 {"batch_id": handover["batch_id"], "handover_id": handover_id, "request_no": request_no,
                                  "evidence_type": evidence_type, "evidence_version": evidence_version})
                self.conn.commit()
            except ApiError:
                raise
            except Exception as exc:
                self.conn.rollback()
                self.ledger.fail(request_no, str(exc))
                raise ApiError(500, f"回执写入失败，原记录已保留，请按请求号 {request_no} 重试") from exc
            self.ledger.complete(request_no)
        return self._receipt_dict(self.conn.execute("SELECT * FROM receipts WHERE id=?", (receipt_id,)).fetchone())

    def _current_evidence_version(self, handover: sqlite3.Row) -> int:
        count = self.conn.execute("SELECT COUNT(*) FROM receipts WHERE handover_id=?", (handover["id"],)).fetchone()[0]
        return int(handover["evidence_revision"]) + count

    def _invalidate_conclusions(self, batch_id: int, receipt_id: int, actor: str, request_no: str, evidence_type: str) -> None:
        """证据更新让受影响的生效放行结论失效，并写明失效来源。"""
        rows = self.conn.execute("SELECT id FROM release_conclusions WHERE batch_id=? AND status='active'", (batch_id,)).fetchall()
        stamp = now()
        for row in rows:
            self.conn.execute("""UPDATE release_conclusions SET status='invalidated',invalidated_by_receipt_id=?,invalidated_at=? WHERE id=?""",
                              (receipt_id, stamp, row["id"]))
            self.store.audit(actor, "release.invalidate", "release_conclusion", row["id"],
                             {"batch_id": batch_id, "source_receipt_id": receipt_id, "source_request_no": request_no,
                              "source_evidence_type": evidence_type})

    def handover_detail(self, handover_id: int) -> dict:
        handover = self._handover(handover_id)
        out = self._handover_dict(handover)
        out["receipts"] = [self._receipt_dict(row) for row in
                           self.conn.execute("SELECT * FROM receipts WHERE handover_id=? ORDER BY id", (handover_id,))]
        return out

    @staticmethod
    def _handover_dict(row: sqlite3.Row) -> dict:
        return {"id": row["id"], "batch_id": row["batch_id"], "from_factory_id": row["from_factory_id"],
                "to_factory_id": row["to_factory_id"], "status": row["status"],
                "evidence_revision": row["evidence_revision"], "snapshot": json.loads(row["snapshot_json"]),
                "proposed_by": row["proposed_by"], "proposed_at": row["proposed_at"],
                "confirmed_by": row["confirmed_by"], "confirmed_at": row["confirmed_at"]}

    @staticmethod
    def _receipt_dict(row: sqlite3.Row, replayed: bool = False) -> dict:
        return {"id": row["id"], "handover_id": row["handover_id"], "batch_id": row["batch_id"],
                "request_no": row["request_no"], "evidence_type": row["evidence_type"], "content": row["content"],
                "evidence_version": row["evidence_version"], "recorded_by": row["recorded_by"],
                "created_at": row["created_at"], "replayed": replayed}


class ReleaseService:
    """放行审查：依据冻结快照加回执重算结论；判定逻辑独立，并发审查只成功一次。"""

    def __init__(self, store: Store, handovers: HandoverService):
        self.store, self.conn = store, store.conn
        self.handovers = handovers

    @staticmethod
    def _actor(actor: str | None, role: str | None) -> str:
        if not actor: raise ApiError(401, "缺少身份")
        if role != "qa": raise ApiError(403, "只有质检 qa 可以做放行审查")
        return actor

    def review(self, actor: str | None, role: str | None, factory_id: int, batch_id: int, decision: str, rationale: str, exception_code: str = "") -> dict:
        actor = self._actor(actor, role)
        if decision not in {"release", "reject", "conditional"}: raise ApiError(400, "放行结论不合法")
        if not rationale.strip(): raise ApiError(400, "必须填写审查依据")
        handover = self.conn.execute("SELECT * FROM handovers WHERE batch_id=? ORDER BY id DESC LIMIT 1", (batch_id,)).fetchone()
        if not handover: raise ApiError(409, "批次尚未交接，不能按交接流程放行")
        if int(factory_id) != int(handover["to_factory_id"]):
            raise ApiError(403, "只有接收新厂质检可以给出放行结论")
        snapshot = json.loads(handover["snapshot_json"])
        receipts = self.conn.execute("SELECT * FROM receipts WHERE handover_id=? ORDER BY id", (handover["id"],)).fetchall()
        self._evaluate(decision, snapshot, [dict(r) for r in receipts], exception_code)
        last_receipt = receipts[-1] if receipts else None
        evidence_version = int(handover["evidence_revision"]) + len(receipts)
        with self.store.write_lock:
            active = self.conn.execute("SELECT id FROM release_conclusions WHERE batch_id=? AND status='active'", (batch_id,)).fetchone()
            if active is not None: raise ApiError(409, "已存在生效放行结论；证据更新使其失效后才能重新审查")
            stamp = now()
            try:
                cur = self.conn.execute("""INSERT INTO release_conclusions(batch_id,handover_id,decision,rationale,exception_code,
                                                        evidence_revision,last_receipt_id,status,decided_by,created_at)
                                        VALUES(?,?,?,?,?,?,?, 'active',?,?)""",
                                       (batch_id, handover["id"], decision, rationale.strip(), exception_code.strip() or None,
                                        evidence_version, last_receipt["id"] if last_receipt else None, actor, stamp))
            except sqlite3.IntegrityError as exc: raise ApiError(409, "并发放行审查冲突，只有一个生效结论") from exc
            self.store.audit(actor, "release.review", "release_conclusion", cur.lastrowid,
                             {"batch_id": batch_id, "handover_id": handover["id"], "decision": decision,
                              "evidence_revision": evidence_version, "receipts": len(receipts)})
            self.conn.commit()
        return self.conclusion_detail(cur.lastrowid)

    @staticmethod
    def _evaluate(decision: str, snapshot: dict, receipts: list[dict], exception_code: str) -> None:
        """基于冻结快照重算：关键偏差始终阻止正式放行；快照检验不合格阻止放行；有条件放行要求例外编号。"""
        if decision == "reject": return
        tests = snapshot.get("tests", [])
        latest: dict[str, dict] = {}
        for row in tests: latest[row["test_type"]] = row
        if not latest: raise ApiError(409, "冻结依据中没有检验结果，不能放行")
        if any(not row["passed"] for row in latest.values()): raise ApiError(409, "冻结的最新检验结果仍有不合格项")
        open_devs = [d for d in snapshot.get("deviations", []) if d["status"] == "open"]
        critical = [d for d in open_devs if d["severity"] == "critical"]
        if critical: raise ApiError(409, "冻结依据存在未关闭的关键偏差，阻止放行")
        if decision == "release" and open_devs:
            raise ApiError(409, "冻结依据仍有未关闭偏差，不能正式放行")
        if decision == "conditional":
            for d in open_devs:
                if not d.get("exception_reason") or not after_now(d.get("exception_until")):
                    raise ApiError(409, f"偏差 {d['id']} 没有有效例外批准")
            if not exception_code.strip(): raise ApiError(400, "有条件放行必须提供例外编号")

    def conclusion_detail(self, conclusion_id: int) -> dict:
        row = self.conn.execute("SELECT * FROM release_conclusions WHERE id=?", (conclusion_id,)).fetchone()
        if not row: raise ApiError(404, "放行结论不存在")
        return self._conclusion_dict(row)

    @staticmethod
    def _conclusion_dict(row: sqlite3.Row) -> dict:
        return {"id": row["id"], "batch_id": row["batch_id"], "handover_id": row["handover_id"],
                "decision": row["decision"], "rationale": row["rationale"], "exception_code": row["exception_code"],
                "evidence_revision": row["evidence_revision"], "last_receipt_id": row["last_receipt_id"],
                "status": row["status"], "invalidated_by_receipt_id": row["invalidated_by_receipt_id"],
                "invalidated_at": row["invalidated_at"], "decided_by": row["decided_by"], "created_at": row["created_at"]}

    def list_for_batch(self, batch_id: int) -> list[dict]:
        return [self._conclusion_dict(row) for row in
                self.conn.execute("SELECT * FROM release_conclusions WHERE batch_id=? ORDER BY id", (batch_id,))]

    def list_for_handover(self, handover_id: int) -> list[dict]:
        return [self._conclusion_dict(row) for row in
                self.conn.execute("SELECT * FROM release_conclusions WHERE handover_id=? ORDER BY id", (handover_id,))]


class Handler(BaseHTTPRequestHandler):
    service: BatchService
    handovers: HandoverService
    releases: ReleaseService

    def log_message(self, fmt: str, *args: object) -> None: sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))
    def _send(self, status: int, body: object) -> None:
        data = json.dumps(body, ensure_ascii=False).encode(); self.send_response(status); self.send_header("Content-Type", "application/json; charset=utf-8"); self.send_header("Content-Length", str(len(data))); self.end_headers(); self.wfile.write(data)
    def _body(self) -> dict:
        size = int(self.headers.get("Content-Length", "0"))
        try: return json.loads(self.rfile.read(size)) if size else {}
        except json.JSONDecodeError as exc: raise ApiError(400, "JSON 请求体无效") from exc
    def _parts(self) -> list[str]: return [p for p in urlparse(self.path).path.strip("/").split("/") if p]
    def _factory(self, b: dict) -> int:
        raw = self.headers.get("X-Factory") or b.get("factory_id") or 0
        try: return int(raw)
        except (TypeError, ValueError) as exc: raise ApiError(400, "X-Factory/factory_id 必须是工厂编号") from exc

    def do_GET(self) -> None:
        try:
            p = self._parts()
            if p in (["health"], ["api", "health"]): out = {"status": "ok"}
            elif p == ["api", "state"]: out = self.service.state()
            elif len(p) == 3 and p[:2] == ["api", "batches"]: out = self.service.batch_detail(int(p[2]))
            elif len(p) == 3 and p[:2] == ["api", "handovers"]: out = self.handovers.handover_detail(int(p[2]))
            elif len(p) == 4 and p[:2] == ["api", "handovers"] and p[3] == "conclusions":
                out = {"conclusions": self.releases.list_for_handover(int(p[2]))}
            elif len(p) == 3 and p[:2] == ["api", "conclusions"]: out = self.releases.conclusion_detail(int(p[2]))
            elif not p:
                page = (Path(__file__).parent / "static" / "index.html").read_bytes(); self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(page))); self.end_headers(); self.wfile.write(page); return
            else: raise ApiError(404, "接口不存在")
            self._send(200, out)
        except ApiError as exc: self._send(exc.status, {"error": exc.message})
        except Exception as exc: self._send(500, {"error": str(exc)})

    def do_POST(self) -> None:
        try:
            p, b = self._parts(), self._body(); actor, role = self.headers.get("X-Actor"), self.headers.get("X-Role"); factory_id = self._factory(b)
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
            elif len(p) == 4 and p[:2] == ["api", "batches"] and p[3] == "handovers":
                out = self.handovers.propose(actor, role, factory_id, int(p[2]), int(b.get("to_factory_id", 0)))
            elif len(p) == 4 and p[:2] == ["api", "handovers"] and p[3] == "confirm":
                out = self.handovers.confirm(actor, role, factory_id, int(p[2]))
            elif len(p) == 4 and p[:2] == ["api", "handovers"] and p[3] == "receipts":
                out = self.handovers.append_receipt(actor, role, factory_id, int(p[2]), b.get("request_no"),
                                                    b.get("evidence_type", ""), b.get("content", ""))
            elif len(p) == 4 and p[:2] == ["api", "batches"] and p[3] == "review":
                out = self.releases.review(actor, role, factory_id, int(p[2]), b.get("decision", ""), b.get("rationale", ""), b.get("exception_code", ""))
            else: raise ApiError(404, "接口不存在")
            self._send(200, out)
        except ApiError as exc: self._send(exc.status, {"error": exc.message})
        except (ValueError, TypeError, sqlite3.IntegrityError) as exc: self._send(400, {"error": str(exc)})
        except Exception as exc: self._send(500, {"error": str(exc)})


def run(port: int, db_path: str, seed: bool) -> None:
    store = Store(db_path); service = BatchService(store)
    handover_service = HandoverService(store); release_service = ReleaseService(store, handover_service)
    if seed: service.seed()
    Handler.service = service; Handler.handovers = handover_service; Handler.releases = release_service
    print(f"batch release listening on http://127.0.0.1:{port}")
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--port", type=int, default=8214); parser.add_argument("--db", default=str(DB_PATH)); parser.add_argument("--init", action="store_true"); parser.add_argument("--seed", action="store_true")
    args = parser.parse_args()
    if args.init: Store(args.db).close()
    if args.seed or not args.init: run(args.port, args.db, args.seed)


if __name__ == "__main__": main()
