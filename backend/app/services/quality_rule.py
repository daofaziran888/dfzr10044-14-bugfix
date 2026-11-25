"""入湖质量规则领域服务：校验列、阈值、违规处置与规则生命周期管理。"""
from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.store import store
from app.lake_store import lake_store


_NOT_NULL_TYPES = {"NOT_NULL", "NON_NULL", "NOTNULL", "REQUIRED", "非空", "非空校验", "必填"}


class QualityRuleService(BaseModuleService):
    MODULE = "quality_rule"
    ENTITY = "质量规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则编号", "所属湖表", "校验列", "校验类型", "阈值下限", "阈值上限", "违规处置", "命中批次"]
    REQUIRED_FIELDS = ["规则编号", "所属湖表", "校验列"]
    STATUS_ORDER = ["草稿", "已启用", "已抑制", "已废弃"]
    ACTION_RULES = {"启用规则": "已启用", "抑制规则": "已抑制", "废弃规则": "已废弃"}
    NEGATIVE_ACTIONS = []

    # ---------- 纯判定 ----------

    def apply_rules_to_batch(self, batch_id: str,
                             rules: list[dict[str, Any]] | dict[str, Any] | None = None,
                             lake_table: str | None = None) -> dict[str, Any]:
        """对一个批次的全部行应用质量规则，返回违规行和通过行。"""
        br = lake_store.get_batch(batch_id, lake_table)
        if br is None:
            return {"success": False, "ok": False, "error": "batch not found"}

        rows = br.rows
        resolved_rules = self._resolve_rules(rules, br.lake_table, default_column="amount")
        return self.evaluate_rows(rows, resolved_rules, batch_id=batch_id)

    def evaluate_rows(
        self,
        rows: list[dict[str, Any]],
        rules: list[dict[str, Any]] | dict[str, Any] | None = None,
        *,
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        resolved_rules = self._resolve_rules(rules, default_column="amount")
        violations: dict[int, list[str]] = {}
        violation_details: list[dict[str, Any]] = []
        row_results: list[dict[str, Any]] = []

        for i, row in enumerate(rows):
            row_violations: list[str] = []
            detailed: list[dict[str, Any]] = []
            row_ref = f"row-{i}"
            for rule in resolved_rules:
                col = str(
                    rule.get("校验列") or rule.get("column")
                    or rule.get("column_name") or rule.get("field") or ""
                )
                rule_id = str(
                    rule.get("规则编号") or rule.get("rule_id")
                    or rule.get("id") or rule.get("code") or ""
                )
                rule_type = self._normalize_rule_type(
                    rule.get("校验类型") or rule.get("rule_type") or rule.get("type")
                )
                lo = self._parse_limit(rule.get("阈值下限") or rule.get("min"))
                hi = self._parse_limit(rule.get("阈值上限") or rule.get("max"))
                missing_column = col not in row
                val = row.get(col)

                violated = False
                reason = ""
                if rule_type in _NOT_NULL_TYPES:
                    # 关键修复：缺列不能 continue 放行；它与显式 null/空白一样违反非空。
                    if missing_column:
                        violated = True
                        reason = "missing_column"
                    elif val is None or str(val).strip() == "":
                        violated = True
                        reason = "null_or_blank"
                elif rule_type == "UNIQUE":
                    pass
                elif rule_type == "RANGE":
                    if missing_column or val is None or str(val).strip() == "":
                        # 范围规则本身不替代非空规则；空值交由 NOT_NULL 判定。
                        pass
                    else:
                        try:
                            num_val = float(str(val))
                            if (lo is not None and num_val < lo) or (hi is not None and num_val > hi):
                                violated = True
                                reason = "out_of_range"
                        except (ValueError, TypeError):
                            violated = True
                            reason = "not_numeric"

                if violated:
                    row_violations.append(rule_id)
                    item = {
                        "row_index": i,
                        "row_ref": row_ref,
                        "source_row_id": row_ref,
                        "rule_id": rule_id,
                        "rule_name": rule.get("规则名称") or rule.get("rule_name") or "",
                        "column": col,
                        "rule_type": rule_type,
                        "reason": reason,
                        "reason_code": reason.upper(),
                        "violation_reason": reason.upper(),
                        "missing_column": missing_column,
                    }
                    detailed.append(item)
                    violation_details.append({**item, "row": dict(row)})

            if row_violations:
                violations[i] = row_violations
                row_results.append({
                    "row_index": i,
                    "row_ref": row_ref,
                    "source_row_id": row_ref,
                    "status": "violation",
                    "violated_rules": row_violations,
                    "violations": detailed,
                    "missing_columns": [
                        d["column"] for d in detailed if d["reason"] == "missing_column"
                    ],
                    "data": dict(row),
                })
            else:
                row_results.append({
                    "row_index": i,
                    "row_ref": row_ref,
                    "source_row_id": row_ref,
                    "status": "clean",
                    "violated_rules": [],
                    "violations": [],
                    "missing_columns": [],
                    "data": dict(row),
                })

        clean_rows = [rows[i] for i in range(len(rows)) if i not in violations]
        violation_rows_data = [rows[i] for i in violations]
        result = {
            "ok": True,
            "success": True,
            "batch_id": batch_id,
            "total_rows": len(rows),
            # 保留旧契约：violation_rows 是数量；明细请用 violation_data/details。
            "clean_rows": len(clean_rows),
            "violation_rows": len(violation_rows_data),
            "clean_count": len(clean_rows),
            "violation_count": len(violation_rows_data),
            "passed_count": len(clean_rows),
            "violations_by_row": {str(k): v for k, v in violations.items()},
            "violation_details": violation_details,
            "row_results": row_results,
            "violation_data": violation_rows_data,
            "violation_items": violation_rows_data,
            "clean_data": clean_rows,
            "clean_detail": clean_rows,
            "clean_details": clean_rows,
            "passed_data": clean_rows,
            "passed_rows": clean_rows,
            "rules": resolved_rules,
        }
        return result

    # ---------- 质量检查入口：判定 -> 隔离 -> 通过行入湖 ----------

    def run_action(
        self, entry_id: int, action: str, payload: Any | None = None
    ) -> tuple[dict[str, Any] | None, str]:
        if action in {"质量检查", "提交质量检查", "检查批次", "质量校验", "提交批次检查"}:
            dumped = payload.model_dump() if payload is not None and hasattr(payload, "model_dump") else {}
            raw_values = getattr(payload, "values", None) or {}
            extra = {
                k: v for k, v in dumped.items()
                if k not in {"action", "remark", "values", "idempotency_key"}
            }
            data = {**extra, **raw_values}
            data = self.unwrap_payload(data)
            # 通用动作弹窗里的长 JSON 可能放在 remark 或 values["批次行"]。
            remark = getattr(payload, "remark", None)
            if remark and "rows" not in data and "批次数据" not in data:
                try:
                    parsed = json.loads(remark)
                    if isinstance(parsed, dict):
                        data.update(parsed)
                    elif isinstance(parsed, list):
                        data["rows"] = parsed
                except (json.JSONDecodeError, TypeError):
                    pass
            result = self.submit_quality_check(data)
            entry = store.find(self.MODULE, entry_id)
            if entry is not None:
                entry["last_quality_check"] = result
                entry["quality_check"] = result
                entry["result"] = result
            if not result.get("ok"):
                return None, str(result.get("error") or "质量检查未通过")
            return entry, f"质量检查完成：隔离 {result.get('violation_rows', 0)} 行，通过 {result.get('clean_rows', 0)} 行"

        return super().run_action(entry_id, action, payload)

    def submit_quality_check(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        data = self.unwrap_payload(payload)
        lake_table = str(self._first(data, [
            "lake_table", "table", "table_name", "target_table", "所属湖表", "湖表",
        ], "ods_trade_amount_fix"))
        batch_id = str(self._first(data, [
            "batch_id", "batchId", "批次编号", "来源批次",
        ], f"BATCH-QC-{uuid.uuid4().hex[:8].upper()}"))
        source_system = str(self._first(data, [
            "source_system", "sourceSystem", "来源系统",
        ], "TRANSACTION_EXPORT"))
        window_start = str(data.get("window_start") or data.get("窗口开始") or "")
        window_end = str(data.get("window_end") or data.get("窗口结束") or "")

        rows = self._extract_rows(data)
        if rows is None:
            return {
                "ok": False,
                "success": False,
                "http_status": 422,
                "error_code": "rows_required",
                "error": "请提交 rows/records/data 行数组",
            }

        rules = self._resolve_rules(
            data.get("rules", data.get("quality_rules", data.get("rule", data.get("quality_rule")))),
            lake_table,
            rule_id=data.get("rule_id") or data.get("规则编号"),
        )
        late_receipt = self._as_bool(self._first(data, [
            "late_receipt", "simulate_late_receipt", "receipt_late", "原批确认回执迟到",
        ], False))
        confirm_receipt = self._as_bool(self._first(data, [
            "confirm_receipt", "receipt_received", "确认回执",
        ], not late_receipt))
        evaluation = self.evaluate_rows(rows, rules, batch_id=batch_id)

        existing = lake_store.get_quality_check(lake_table, batch_id)
        row_hash = hashlib.sha256(
            json.dumps(rows, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")
        ).hexdigest()
        request_hash = self._rows_hash(rows, rules)
        if existing and not self._as_bool(data.get("force")):
            same_rows = existing.get("row_hash") == row_hash or existing.get("request_hash") == request_hash
            if same_rows:
                existing["ok"] = True
                existing["success"] = True
                existing["replayed"] = True
                existing["scheduler_replay"] = True
                existing["message"] = "原批重放已按同一批次对账，未重复隔离或写入"
                return existing
            return {
                "ok": False,
                "success": False,
                "http_status": 409,
                "error_code": "batch_payload_conflict",
                "error": "同湖表同编号批次已存在，但提交内容与首次不一致",
                "existing_check_id": existing.get("check_id"),
            }

        br = lake_store.store_batch(
            batch_id, lake_table, source_system, rows, window_start, window_end
        )
        lake_store.update_batch_status(batch_id, "ingesting", lake_table=lake_table)
        br.receipt_status = "received" if confirm_receipt else "late"
        br.receipt_id = f"BATCH-RCPT-{hashlib.sha1((lake_table + batch_id).encode()).hexdigest()[:12].upper()}"

        # 统一两个接口使用的关联标识：湖表 + 批次 + 原始行。
        for row_result in evaluation["row_results"]:
            row_ref = str(row_result.get("source_row_id"))
            row_hash = hashlib.sha1(
                f"{lake_table}|{batch_id}|{row_ref}".encode("utf-8")
            ).hexdigest()[:16]
            row_result["correlation_id"] = f"LINEAGE-{row_hash}"
            row_result["lake_row_id"] = f"LAKE-ROW-{row_hash}"
        for detail in evaluation["violation_details"]:
            row_ref = str(detail.get("source_row_id"))
            row_hash = hashlib.sha1(
                f"{lake_table}|{batch_id}|{row_ref}".encode("utf-8")
            ).hexdigest()[:16]
            detail["correlation_id"] = f"LINEAGE-{row_hash}"

        grouped: dict[str, dict[str, Any]] = {}

        # 按规则分组隔离，保证每条明细都能追到具体非空规则和原始行。
        from app.services.quarantine_zone import QuarantineZoneService
        quarantine_service = QuarantineZoneService()
        qids: list[str] = []
        for detail in evaluation["violation_details"]:
            rule_id = detail["rule_id"] or "UNKNOWN_RULE"
            group = grouped.setdefault(rule_id, {
                "rule_id": rule_id,
                "rule_name": detail.get("rule_name", ""),
                "rows": [],
            })
            row_copy = dict(detail["row"])
            row_copy["_source_row_id"] = detail["source_row_id"]
            group["rows"].append(row_copy)

        for group in grouped.values():
            isolation = quarantine_service.isolate_violations(
                batch_id,
                group["rule_id"],
                group["rule_name"],
                group["rows"],
                lake_table=lake_table,
            )
            for qr in isolation["quarantine_records"]:
                qids.append(qr["_quarantine_record_id"])

        # 只把原批通过行写入确定性 lake row；重放同一原始行时 upsert，不产生第二行。
        clean_rows = evaluation["clean_data"]
        lake_rows: list[dict[str, Any]] = []
        clean_details: list[dict[str, Any]] = []
        for row_result in evaluation["row_results"]:
            if row_result["status"] != "clean":
                continue
            row_ref = row_result["source_row_id"]
            row_hash = hashlib.sha1(
                f"{lake_table}|{batch_id}|{row_ref}".encode()
            ).hexdigest()[:16]
            lake_row_id = f"LAKE-ROW-{row_hash}"
            lake_row = lake_store.upsert_lake_row(
                lake_table,
                lake_row_id,
                row_result["data"],
                source_batch_id=batch_id,
                source_row_id=row_ref,
                receipt_id=br.receipt_id if confirm_receipt else None,
                commit_status="confirmed" if confirm_receipt else "written_waiting_receipt",
            )
            business_row = {k: v for k, v in lake_row.items() if not k.startswith("_")}
            lake_rows.append(lake_row)
            clean_details.append({
                **row_result,
                "lake_row_id": lake_row_id,
                "committed": True,
                "commit_status": lake_row["_commit_status"],
                "business_row": business_row,
            })

        if confirm_receipt:
            lake_store.update_batch_status(batch_id, "ingested", lake_table=lake_table)

        write_job_result: dict[str, Any] | None = None
        if clean_rows:
            from app.services.write_job import WriteJobService
            write_job_result = WriteJobService().acquire_and_write(
                lake_table, "APPEND", clean_rows
            )

        self._upsert_batch_entry(br)
        self._touch_rule_hit(rules, lake_table, batch_id)

        result = {
            **evaluation,
            "check_id": f"QC-{hashlib.sha1((lake_table + batch_id).encode()).hexdigest()[:16].upper()}",
            "request_hash": request_hash,
            "row_hash": row_hash,
            "lake_table": lake_table,
            "table": lake_table,
            "batch_id": batch_id,
            "source_batch_id": batch_id,
            "source_system": source_system,
            "quarantine_record_ids": qids,
            "quarantine_records": qids,
            "quarantined_count": len(qids),
            "clean_details": clean_details,
            "lake_rows": lake_rows,
            "lake_row_ids": [row["_lake_row_id"] for row in lake_rows],
            "batch_receipt": {
                "receipt_id": br.receipt_id,
                "status": getattr(br, "receipt_status", "pending"),
                "late": not confirm_receipt,
                "original_batch_confirmed": confirm_receipt,
            },
            "write_job": write_job_result,
            "snapshot_id": (write_job_result or {}).get("snapshot_id"),
            "scheduler_replay_supported": True,
            "idempotency_scope": ["lake_table", "batch_id", "source_row_id"],
            "message": "质量检查完成：缺列和显式空值已隔离，通过明细已交接批次入湖",
        }
        lake_store.save_quality_check((lake_table, batch_id), result)
        return result

    def receive_original_receipt(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        data = self.unwrap_payload(payload)
        lake_table = str(self._first(data, ["lake_table", "table", "所属湖表"], ""))
        batch_id = str(self._first(data, ["batch_id", "批次编号", "来源批次"], ""))
        receipt_id = data.get("receipt_id") or data.get("回执编号")
        if not lake_table or not batch_id:
            return {"ok": False, "success": False, "http_status": 422,
                    "error": "lake_table 和 batch_id 必填"}
        br = lake_store.get_batch(batch_id, lake_table)
        if br is None:
            return {"ok": False, "success": False, "http_status": 404,
                    "error_code": "batch_not_found", "error": "原批次不存在"}
        br.receipt_status = "received"
        if receipt_id:
            br.receipt_id = receipt_id
        lake_store.update_batch_status(batch_id, "ingested", lake_table=lake_table)
        existing_check = lake_store.get_quality_check(lake_table, batch_id)
        original_clean_row_ids = None
        if existing_check:
            original_clean_row_ids = {
                str(item.get("source_row_id"))
                for item in existing_check.get("clean_details", [])
                if item.get("source_row_id") is not None
            }
        confirmed_row_count = lake_store.confirm_batch_lake_rows(
            lake_table, batch_id, getattr(br, "receipt_id", receipt_id),
            source_row_ids=original_clean_row_ids,
        )
        check = existing_check
        if check:
            check["batch_receipt"] = {
                **(check.get("batch_receipt") or {}),
                "receipt_id": br.receipt_id,
                "status": "received",
                "late": False,
                "original_batch_confirmed": True,
                "confirmed_lake_rows": confirmed_row_count,
            }
            for detail in check.get("clean_details", []):
                detail["commit_status"] = "confirmed"
            lake_store.save_quality_check((lake_table, batch_id), check)
        return {
            "ok": True,
            "success": True,
            "lake_table": lake_table,
            "batch_id": batch_id,
            "receipt_id": getattr(br, "receipt_id", receipt_id),
            "confirmed_lake_rows": confirmed_row_count,
            "status": "received_and_reconciled",
            "message": "原批迟到回执已登记并完成对账",
        }

    def query_lake(self, lake_table: str, amount: Any | None = None,
                   batch_id: str | None = None) -> dict[str, Any]:
        rows = lake_store.list_lake_rows(lake_table, include_internal=True)
        if amount is not None and str(amount) != "":
            wanted = self._parse_limit(amount)
            filtered = []
            for row in rows:
                actual = self._parse_limit(row.get("amount"))
                if wanted is not None and actual == wanted:
                    filtered.append(row)
            rows = filtered
        if batch_id:
            rows = [row for row in rows if row.get("_source_batch_id") == batch_id]
        business = [
            {k: v for k, v in row.items() if not str(k).startswith("_")}
            for row in rows
        ]
        return {
            "ok": True,
            "success": True,
            "lake_table": lake_table,
            "total": len(business),
            "items": business,
            "rows": business,
            "internal_items": rows,
        }

    # ---------- 辅助 ----------

    @staticmethod
    def unwrap_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
        data = dict(payload or {})
        values = data.get("values")
        if isinstance(values, dict):
            merged = {**values, **{k: v for k, v in data.items() if k != "values"}}
            return merged
        return data

    @staticmethod
    def _first(data: dict[str, Any], keys: list[str], default: Any = None) -> Any:
        for key in keys:
            if key in data and data[key] is not None:
                return data[key]
        return default

    @staticmethod
    def _as_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        return str(value).strip().lower() in {"1", "true", "yes", "y", "是", "需要", "迟到"}

    @staticmethod
    def _normalize_rule_type(value: Any) -> str:
        return str(value or "").strip().upper().replace("-", "_").replace(" ", "_")

    def _extract_rows(self, data: dict[str, Any]) -> list[dict[str, Any]] | None:
        candidate = self._first(data, [
            "rows", "records", "data", "items", "batch_rows", "original_rows",
            "rows_data", "row_data", "line_items", "details",
            "行数据", "批次数据", "数据行", "行列表", "明细", "批次明细",
        ])
        if candidate is None:
            return None
        if isinstance(candidate, str):
            text = candidate.strip()
            if not text:
                return []
            try:
                candidate = json.loads(text)
            except json.JSONDecodeError:
                return None
        if isinstance(candidate, dict):
            candidate = [candidate]
        if not isinstance(candidate, list):
            return None
        return [dict(row) for row in candidate if isinstance(row, dict)]

    def _resolve_rules(
        self,
        rules: Any = None,
        lake_table: str | None = None,
        default_column: str = "amount",
        rule_id: Any = None,
    ) -> list[dict[str, Any]]:
        if isinstance(rules, dict):
            rules = [rules]
        if isinstance(rules, list) and rules:
            return [dict(rule) for rule in rules if isinstance(rule, dict)]

        if rule_id:
            for row in store.rows(self.MODULE):
                if str(row.get("规则编号")) == str(rule_id) or str(row.get("id")) == str(rule_id):
                    return [dict(row)]

        enabled = [
            dict(row) for row in store.rows(self.MODULE)
            if row.get("status") == "已启用"
            and (lake_table is None or not row.get("所属湖表") or row.get("所属湖表") == lake_table)
            and str(row.get("校验列", "")).lower() == default_column.lower()
            and self._normalize_rule_type(row.get("校验类型")) in _NOT_NULL_TYPES
        ]
        if enabled:
            return enabled

        return [{
            "规则编号": "QR-AMOUNT-NOT-NULL",
            "规则名称": "交易金额非空",
            "所属湖表": lake_table or "",
            "校验列": default_column,
            "校验类型": "NOT_NULL",
            "违规处置": "隔离",
        }]

    @staticmethod
    def _rows_hash(rows: list[dict[str, Any]], rules: list[dict[str, Any]]) -> str:
        payload = json.dumps(
            {"rows": rows, "rules": rules},
            sort_keys=True, ensure_ascii=False, default=str,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _upsert_batch_entry(self, br: Any) -> None:
        rows = store.rows("ingest_batch")
        entry = next((
            row for row in rows
            if row.get("批次编号") == br.batch_id and row.get("所属湖表") == br.lake_table
        ), None)
        if entry is None:
            entry = {
                "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
            }
            rows.append(entry)
        entry.update({
            "status": "入湖中" if br.status == "ingesting" else ("已入湖" if br.status == "ingested" else "待入湖"),
            "pending": br.status != "ingested",
            "abnormal": False,
            "批次编号": br.batch_id,
            "所属湖表": br.lake_table,
            "来源系统": br.source_system,
            "批次行数": br.row_count,
            "采集窗口": f"{br.window_start} ~ {br.window_end}",
            "落湖快照": br.snapshot_id or "",
            "完整校验值": br.checksum,
            "入湖时间": "",
            "receipt_status": getattr(br, "receipt_status", "pending"),
            "receipt_id": getattr(br, "receipt_id", ""),
        })

    def _touch_rule_hit(self, rules: list[dict[str, Any]], lake_table: str, batch_id: str) -> None:
        for rule in rules:
            rule_code = rule.get("规则编号")
            if not rule_code:
                continue
            entry = next((
                row for row in store.rows(self.MODULE)
                if row.get("规则编号") == rule_code
                and (not row.get("所属湖表") or row.get("所属湖表") == lake_table)
            ), None)
            if entry is not None:
                entry["命中批次"] = batch_id

    def _parse_limit(self, value: Any) -> float | None:
        if value is None or str(value).strip() == "":
            return None
        try:
            return float(str(value))
        except (ValueError, TypeError):
            return None

    # 便于不同测试/调用方按业务语义命名同一入口。
    submit_batch_for_check = submit_quality_check
    check_and_handoff = submit_quality_check
    run_quality_gate = submit_quality_check
    check_batch = submit_quality_check
    validate_rows = evaluate_rows
    evaluate_batch = apply_rules_to_batch
    query_lake_rows = query_lake
