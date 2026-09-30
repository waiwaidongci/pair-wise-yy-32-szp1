# 药品生产偏差与批次放行系统

Python 标准库 + SQLite。批次可关联关键/一般偏差、检验复测、返工、供应商变更和稳定性数据。质量人员可以拒绝、再取样、有条件放行或正式放行；关键偏差始终阻止正式放行，修改必须携带当前批次修订号。

## 批次交接与放行审查

批次在前厂调查完成后可移交给新厂，移交与放行串成一条线：

- `POST /api/batches/{id}/handovers`：前厂（operator，头 `X-Factory` 为前厂）发起交接，**立即冻结**偏差、复测、返工与稳定性依据的快照；冻结后四类依据一律不可再改写。
- `POST /api/handovers/{id}/confirm`：仅接收新厂 QA（`X-Factory` 为新厂、`X-Role=qa`）确认。并发确认只有一次成功，其余返回 409。
- `POST /api/handovers/{id}/receipts`：交接确认后，**前厂只能追加回执**（`deviation/test/rework/stability` 四类补充说明），须携带幂等 `request_no`。
- `POST /api/batches/{id}/review`：新厂 QA 基于冻结快照（加全部回执）重算放行结论（release/reject/conditional）。
- 证据更新规则：任一回执落库都会让该批次**当前生效的放行结论失效**，结论记录 `invalidated_by_receipt_id`/`invalidated_at` 写明失效来源；失效后新厂 QA 重新审查生成新结论。
- 幂等请求：回执按 `request_no` 去重，重复请求沿用首次结果（响应含 `replayed:true`）；写入失败时回滚业务、保留请求账本失败记录（`request_ledger.status=failed` 并累加 `attempt`），客户端用同一请求号重试即可。
- 并发：确认交接、放行审查在全局写锁与部分唯一索引（每批次至多一个 confirmed 交接、至多一个 active 结论）双重保护下只成功一次；越权角色/越权工厂返回 403。
- 首页按交接单、放行结论（标注失效来源）、回执、请求账本、审计留痕分栏展示。

职责分离：`HandoverService` 管交接冻结与回执，`ReleaseService` 只管放行判定与重算，`RequestLedger` 只管按请求号的幂等请求处理，`Store.audit` 统一留痕。

## 运行

```bash
python3 app.py --init --seed
python3 app.py
```

默认端口 `8214`。身份通过 `X-Actor` 与 `X-Role` 模拟，角色为 `operator`、`inspector`、`lab`、`qa`。工厂归属通过 `X-Factory`（或请求体 `factory_id`）模拟，工厂人员只能操作本工厂批次；交接确认与放行审查只接受收新厂工厂号。可用 `--port`、`--db` 覆盖。

## 主要接口

- `POST /api/factories`、`POST /api/batches`：登记工厂和批次。
- `POST /api/batches/{id}/deviations`、`POST /api/deviations/{id}/close`：记录和关闭偏差。
- `POST /api/deviations/{id}/exception`：为一般偏差批准有期限例外。
- `POST /api/batches/{id}/tests`：记录检验和复测轮次。
- `POST /api/batches/{id}/rework`、`POST /api/rework/{id}/complete`：计划和完成返工。
- `POST /api/batches/{id}/supplier-changes`、`POST /api/batches/{id}/stability`：关联供应链和稳定性记录。
- `POST /api/batches/{id}/decide`：厂内质量决定，支持并发修订号检查。
- `POST /api/batches/{id}/handovers`、`POST /api/handovers/{id}/confirm`、`POST /api/handovers/{id}/receipts`：发起交接（冻结依据）、新厂确认、前厂追加回执。
- `POST /api/batches/{id}/review`：新厂 QA 放行审查；证据更新后旧结论失效、重新审查。
- `GET /api/handovers/{id}`（含冻结快照与回执）、`GET /api/conclusions/{id}`、`GET /api/batches/{id}`、`GET /api/state`、`GET /api/health`：详情、状态和健康检查。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

`tests/test_flow.py` 覆盖原有厂内调查/复测/返工/放行；`tests/test_handover.py` 覆盖冻结、仅回执追加、幂等重放、写失败重试、失效重算、并发单次成功与越权拒绝。

当前为原型：规则以最新检验项目、未关闭偏差和例外有效期为核心，不等同于真实 GMP 质量体系、电子签名、验证或监管提交规范。
