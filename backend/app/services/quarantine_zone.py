"""隔离区领域服务：存放违规批次行、待处置/回炉/留存管理。"""
from __future__ import annotations

import hashlib
import json
import threading
from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store
from app.lake_store import lake_store


_reflow_lock = threading.RLock()


class QuarantineZoneService(BaseModuleService):
    MODULE = "quarantine_zone"
    ENTITY = "隔离记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["隔离编号", "所属湖表", "来源批次", "违规规则", "违规行数", "隔离目录", "处置结论", "隔离时间"]
    REQUIRED_FIELDS = ["隔离编号", "所属湖表", "来源批次"]
    STATUS_ORDER = ["已隔离", "待处置", "已回炉", "已留存"]
    ACTION_RULES = {
        "提交处置": "待处置",
        "回炉重入": "已回炉",
        "重试回炉": "已回炉",
        "回炉重试": "已回炉",
        "永久留存": "已留存",
    }
    NEGATIVE_ACTIONS = []

    # ---------- 隔离写入与通用清单同步 ----------

    def list_entries(self, **kwargs: Any) -> tuple[list[dict[str, Any]], int]:
        # 即使调用方直接使用 lake_store.quarantine_rows，通用列表/UI 也能看到明细。
        for qr in lake_store.list_quarantine():
            self.sync_store_entry(qr)
        return super().list_entries(**kwargs)

    def isolate_violations(self, batch_id: str, rule_id: str, rule_name: str,
                           violation_rows: list[dict[str, Any]],
                           lake_table: str | None = None) -> dict[str, Any]:
        """将违规数据行迁入隔离区，并同步到通用隔离清单。"""
        quarantined = lake_store.quarantine_rows(
            batch_id, rule_id, rule_name, violation_rows, lake_table=lake_table
        )
        if lake_table is not None:
            for qr in quarantined:
                self.sync_store_entry(qr)
        return {
            "ok": True,
            "batch_id": batch_id,
            "lake_table": lake_table,
            "rule_id": rule_id,
            "quarantine_count": len(quarantined),
            "quarantined": quarantined,
            "quarantine_records": quarantined,
            "status": "isolated",
        }

    def sync_store_entry(self, qr: dict[str, Any]) -> dict[str, Any]:
        """把湖存储中的行级隔离记录同步到通用资源表，供列表/详情页打开。"""
        record_id = str(qr.get("_quarantine_record_id") or "")
        batch_id = str(qr.get("_quarantine_batch") or "")
        lake_table = str(qr.get("_quarantine_table") or "")
        rule_id = str(qr.get("_quarantine_rule") or "")
        rule_name = str(qr.get("_quarantine_rule_name") or "")
        status = str(qr.get("_quarantine_status") or "已隔离")
        business_code = str(qr.get("隔离编号") or f"QUAR-{hashlib.sha1(record_id.encode()).hexdigest()[:12].upper()}")
        isolated_at = qr.get("_quarantine_time")
        if isinstance(isolated_at, (int, float)):
            time_text = datetime.fromtimestamp(isolated_at).isoformat(timespec="seconds")
        else:
            time_text = datetime.now().isoformat(timespec="seconds")

        original_row = {
            k: v for k, v in qr.items()
            if not str(k).startswith("_")
        }
        rows = store.rows(self.MODULE)
        entry = next(
            (
                row for row in rows
                if row.get("quarantine_record_id") == record_id
                or row.get("隔离编号") == business_code
            ),
            None,
        )
        if entry is None:
            entry = {
                "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
            }
            rows.append(entry)

        entry.update({
            "status": status if status in self.STATUS_ORDER else "待处置",
            "pending": status != "已回炉" and status != "已留存",
            "abnormal": status not in {"已回炉", "已留存"},
            "隔离编号": business_code,
            "所属湖表": lake_table,
            "来源批次": batch_id,
            "违规规则": rule_id,
            "违规行数": 1,
            "隔离目录": f"/quarantine/{lake_table or 'unknown'}/{time_text[:10].replace('-', '')}",
            "处置结论": qr.get("_disposition", "") or (
                "已更正并回炉确认" if status == "已回炉" else ""
            ),
            "隔离时间": time_text,
            "quarantine_record_id": record_id,
            "source_row_id": qr.get("_source_row_id"),
            "rule_name": rule_name,
            "original_row": original_row,
            "raw_row": original_row,
            "last_correction_id": qr.get("_last_correction_id"),
            "last_receipt_id": qr.get("_last_receipt_id"),
            "lake_row_id": qr.get("_lake_row_id"),
        })
        return entry

    # ---------- 详情查询 ----------

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = super().get_entry(entry_id)
        if entry is None:
            return None
        detail = self.get_detail(entry_id)
        if detail.get("ok"):
            # detail["entry"] 又指回当前 entry，直接挂载会形成 JSON 循环引用。
            detail_without_entry = {k: v for k, v in detail.items() if k != "entry"}
            entry["detail"] = detail_without_entry
            entry["quarantine_record_id"] = detail.get("quarantine_record_id")
            entry["source_row_id"] = detail.get("source_row_id")
            entry["original_row"] = detail.get("original_row")
            entry["corrections"] = detail.get("corrections", [])
            receipts = detail.get("receipts", [])
            if receipts:
                entry["receipt"] = receipts[-1]
                entry["last_receipt"] = receipts[-1]
        return entry

    def find_record(
        self,
        record_id: str | int | None = None,
        *,
        lake_table: str | None = None,
        batch_id: str | None = None,
        source_row_id: str | None = None,
        quarantine_record_id: str | None = None,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str | None]:
        """返回 (湖存储原始隔离记录, 通用清单条目, 错误码)。"""
        target = str(quarantine_record_id or record_id or "")
        entry: dict[str, Any] | None = None

        if target:
            if target.isdigit():
                entry = store.find(self.MODULE, int(target))
                qid = str((entry or {}).get("quarantine_record_id") or "")
                if qid:
                    qr = lake_store.get_quarantine_record(qid)
                    if qr is not None:
                        if lake_table and qr.get("_quarantine_table") not in {"", lake_table}:
                            return qr, entry, "table_scope_mismatch"
                        return qr, entry, None
                if entry is not None:
                    original = entry.get("original_row")
                    if not isinstance(original, dict):
                        original = {}
                    qid = str(entry.get("quarantine_record_id") or f"store-{entry['id']}")
                    return {
                        **dict(original),
                        "_quarantine_record_id": qid,
                        "_quarantine_batch": entry.get("来源批次"),
                        "_quarantine_table": entry.get("所属湖表"),
                        "_source_row_id": entry.get("source_row_id"),
                        "_quarantine_rule": entry.get("违规规则"),
                        "_quarantine_status": entry.get("status"),
                    }, entry, None

            qr = lake_store.get_quarantine_record(target)
            if qr is not None:
                if lake_table and qr.get("_quarantine_table") not in {"", lake_table}:
                    return qr, None, "table_scope_mismatch"
                return qr, self._find_entry_by_qid(str(qr.get("_quarantine_record_id"))), None

            entry = next(
                (row for row in store.rows(self.MODULE)
                 if str(row.get("隔离编号")) == target),
                None,
            )
            if entry is not None and entry.get("quarantine_record_id"):
                qr = lake_store.get_quarantine_record(str(entry["quarantine_record_id"]))
                if qr is not None:
                    if lake_table and qr.get("_quarantine_table") not in {"", lake_table}:
                        return qr, entry, "table_scope_mismatch"
                    return qr, entry, None

        if batch_id and source_row_id:
            rows = lake_store.list_quarantine(batch_id)
            matches = [
                qr for qr in rows
                if str(qr.get("_source_row_id")) == str(source_row_id)
            ]
            if lake_table:
                scoped = [
                    qr for qr in matches
                    if qr.get("_quarantine_table") in {"", lake_table}
                ]
                if matches and not scoped:
                    return matches[0], None, "table_scope_mismatch"
                matches = scoped
            if len(matches) == 1:
                qr = matches[0]
                return qr, self._find_entry_by_qid(str(qr.get("_quarantine_record_id"))), None
            if len(matches) > 1:
                # 同一原始行可能违反多条规则；金额非空更正可复用任意 amount 规则记录。
                amount_matches = [
                    qr for qr in matches
                    if "amount" in str(qr.get("_quarantine_rule", "")).lower()
                    or "amount" in str(qr.get("_quarantine_rule_name", "")).lower()
                ]
                if len(amount_matches) == 1:
                    qr = amount_matches[0]
                    return qr, self._find_entry_by_qid(str(qr.get("_quarantine_record_id"))), None
                return None, None, "ambiguous_quarantine_row"

        if batch_id and not source_row_id:
            batch_rows = lake_store.list_quarantine(str(batch_id))
            if lake_table:
                scoped = [
                    qr for qr in batch_rows
                    if qr.get("_quarantine_table") in {"", lake_table}
                ]
                if batch_rows and not scoped:
                    return batch_rows[0], None, "table_scope_mismatch"

        return None, entry, "quarantine_record_not_found"

    def _find_entry_by_qid(self, qid: str) -> dict[str, Any] | None:
        return next(
            (row for row in store.rows(self.MODULE)
             if row.get("quarantine_record_id") == qid),
            None,
        )

    def get_detail(
        self,
        record_id: str | int | None = None,
        *,
        lake_table: str | None = None,
        batch_id: str | None = None,
        source_row_id: str | None = None,
    ) -> dict[str, Any]:
        qr, entry, error = self.find_record(
            record_id, lake_table=lake_table, batch_id=batch_id,
            source_row_id=source_row_id,
        )
        if error or qr is None:
            return {
                "ok": False,
                "success": False,
                "error_code": error or "quarantine_record_not_found",
                "error": "隔离记录不存在，或不属于指定湖表/批次",
            }

        qid = str(qr.get("_quarantine_record_id") or "")
        corrections = [
            c for c in lake_store._corrections.values()
            if c.get("quarantine_record_id") == qid
        ]
        batch_record = lake_store.get_batch(str(qr.get("_quarantine_batch") or ""),
                                           str(qr.get("_quarantine_table") or "") or None)
        batch_receipt = None
        if batch_record is not None:
            batch_receipt = {
                "batch_id": batch_record.batch_id,
                "lake_table": batch_record.lake_table,
                "status": batch_record.status,
                "receipt_status": getattr(batch_record, "receipt_status", "pending"),
                "receipt_id": getattr(batch_record, "receipt_id", None),
                "reconciled": batch_record.status == "ingested",
            }
        original_row = {
            k: v for k, v in qr.items() if not str(k).startswith("_")
        }
        return {
            "ok": True,
            "success": True,
            "entry": entry or self.sync_store_entry(qr),
            "quarantine_id": qid,
            "quarantine_record_id": qid,
            "隔离编号": (entry or {}).get("隔离编号") or qid,
            "lake_table": qr.get("_quarantine_table"),
            "batch_id": qr.get("_quarantine_batch"),
            "source_batch_id": qr.get("_quarantine_batch"),
            "source_row_id": qr.get("_source_row_id"),
            "rule_id": qr.get("_quarantine_rule"),
            "rule_name": qr.get("_quarantine_rule_name"),
            "status": qr.get("_quarantine_status"),
            "original_batch_receipt": batch_receipt,
            "batch_receipt": batch_receipt,
            "original_row": original_row,
            "raw_record": qr,
            "corrections": corrections,
            "receipts": [c.get("receipt") for c in corrections if c.get("receipt")],
        }

    # ---------- 回炉更正 ----------

    @staticmethod
    def _normalize_amount(value: Any) -> Any:
        if not isinstance(value, str):
            return value
        text = value.strip().replace(",", "")
        if text == "":
            return None
        try:
            integer = int(text)
            if str(integer) == text:
                return integer
        except ValueError:
            pass
        try:
            number = float(text)
            return int(number) if number.is_integer() and "." not in text else number
        except ValueError:
            return value

    @staticmethod
    def _match_business_row(
        batch_id: str, lake_table: str | None, hint: dict[str, Any]
    ) -> dict[str, Any] | None:
        candidates = lake_store.list_quarantine(batch_id, lake_table=lake_table)
        business_hint = {
            k: v for k, v in hint.items()
            if not str(k).startswith("_") and k not in {"amount"}
        }
        matches = []
        for qr in candidates:
            if all(qr.get(k) == v for k, v in business_hint.items()):
                matches.append(qr)
        return matches[0] if len(matches) == 1 else None

    def retry_reflow(
        self,
        record_id: str | int | None = None,
        payload: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """更正隔离行、复检、幂等写入，并返回指向实际湖行的确认回执。"""
        if isinstance(record_id, dict) and payload is None:
            payload = record_id
            record_id = None
        data: dict[str, Any] = dict(payload or {})
        data.update({k: v for k, v in kwargs.items() if v is not None})

        if record_id is None:
            record_id = data.get("record_id") or data.get("id") or data.get("path_id")
        lake_table = data.get("lake_table") or data.get("table") or data.get("所属湖表")
        batch_id = data.get("batch_id") or data.get("source_batch_id") or data.get("批次编号") or data.get("来源批次")
        original_hint = data.get("original_row") or data.get("original_data") or data.get("source_row") or {}
        if not isinstance(original_hint, dict):
            original_hint = {}
        source_row_id = (
            data.get("source_row_id") or data.get("row_ref") or data.get("original_row_id")
            or data.get("row_id") or original_hint.get("source_row_id")
            or original_hint.get("_source_row_id") or original_hint.get("row_ref")
        )
        qid_arg = data.get("quarantine_record_id") or data.get("quarantine_id") or original_hint.get("quarantine_record_id")
        target_id = record_id or data.get("record_id") or data.get("id") or qid_arg

        qr, entry, error = self.find_record(
            target_id,
            lake_table=lake_table,
            batch_id=batch_id,
            source_row_id=source_row_id,
            quarantine_record_id=qid_arg,
        )
        if error == "quarantine_record_not_found" and target_id and not batch_id and not qid_arg:
            # 兼容 /quarantine/retry/{batch_id} + body.source_row_id 形式。
            qr, entry, error = self.find_record(
                lake_table=lake_table,
                batch_id=str(target_id),
                source_row_id=source_row_id or data.get("row_id") or data.get("original_row_id"),
            )
        if (error == "quarantine_record_not_found" or qr is None) and batch_id and original_hint:
            qr = self._match_business_row(
                str(batch_id), lake_table, original_hint
            )
            if qr is not None:
                qid_arg = qr.get("_quarantine_record_id")
                qr, entry, error = self.find_record(qid_arg, lake_table=lake_table)
        if error == "table_scope_mismatch":
            return {
                "ok": False,
                "success": False,
                "http_status": 409,
                "error_code": "table_scope_mismatch",
                "error": "湖表与隔离记录不一致，不能用其他湖表的同编号批次续办本笔回炉",
            }
        if error or qr is None:
            return {
                "ok": False,
                "success": False,
                "http_status": 404,
                "error_code": error or "quarantine_record_not_found",
                "error": "未找到可重试的隔离记录",
            }

        actual_table = str(lake_table or qr.get("_quarantine_table") or "")
        actual_batch = str(qr.get("_quarantine_batch") or batch_id or "")
        qid = str(qr.get("_quarantine_record_id") or "")
        source_row = str(qr.get("_source_row_id") or source_row_id or "")

        corrected_row = {
            k: v for k, v in qr.items()
            if not str(k).startswith("_")
        }
        patch: dict[str, Any] = {}
        for key in ("corrected_row", "correction", "corrected_values", "values", "更正内容"):
            candidate = data.get(key)
            if isinstance(candidate, dict):
                patch.update(candidate)
        if "amount" in data:
            patch["amount"] = data["amount"]
        for amount_key in (
            "corrected_amount", "new_amount", "fix_amount", "fixed_amount",
            "amount_value", "更正amount", "更正金额", "补录金额",
        ):
            if amount_key in data:
                patch["amount"] = data[amount_key]
                break
        if patch:
            corrected_row.update(patch)
        if "amount" in corrected_row:
            corrected_row["amount"] = self._normalize_amount(corrected_row["amount"])

        # 非空复检：缺列、None、空白字符串均不得回炉。
        if "amount" not in corrected_row:
            return self._reject_correction(qr, actual_table, actual_batch, qid, source_row,
                                           "missing_amount_column", "更正后仍缺少 amount 列")
        amount = corrected_row.get("amount")
        if amount is None or (isinstance(amount, str) and not amount.strip()):
            return self._reject_correction(qr, actual_table, actual_batch, qid, source_row,
                                           "amount_is_null", "更正后的 amount 仍为空，继续隔离")

        idempotency_key = str(
            data.get("idempotency_key")
            or data.get("correction_id")
            or data.get("client_receipt_id")
            or ""
        )
        canonical = hashlib.sha1(
            repr(sorted(corrected_row.items(), key=lambda item: str(item[0]))).encode("utf-8")
        ).hexdigest()[:16]
        natural_key = "|".join([
            "correction", actual_table, actual_batch, qid or source_row,
            idempotency_key or "payload", canonical,
        ])

        with _reflow_lock:
            existing_id = lake_store._correction_by_key.get(natural_key)
            if existing_id:
                prior = lake_store.get_correction(existing_id)
                if prior and prior.get("lake_row_id"):
                    confirmed = lake_store.get_lake_row(actual_table, prior["lake_row_id"])
                    prior["duplicate"] = True
                    prior["replayed"] = True
                    prior["confirmed_record"] = self._business_row(confirmed or {})
                    if isinstance(prior.get("receipt"), dict):
                        prior["receipt"]["duplicate"] = True
                        prior["receipt"]["confirmed_record"] = prior["confirmed_record"]
                    return prior

            row_key_seed = f"{actual_table}|{actual_batch}|{source_row or qid}"
            row_hash = hashlib.sha1(row_key_seed.encode("utf-8")).hexdigest()[:16]
            lake_row_id = f"LAKE-ROW-{row_hash}"
            correlation_id = f"LINEAGE-{row_hash}"
            correction_id = data.get("correction_id") or data.get("idempotency_key") or f"CORR-{hashlib.sha1(natural_key.encode()).hexdigest()[:16].upper()}"
            receipt_id = data.get("receipt_id") or data.get("client_receipt_id") or f"RCPT-{correction_id}"
            reflow_job_id = f"REFLOW-{correction_id}"

            br = lake_store.get_batch(actual_batch, actual_table)
            original_receipt_status = getattr(br, "receipt_status", None) or (
                "received" if br and br.status == "ingested" else "late_or_pending"
            )

            confirmed_lake_row = lake_store.upsert_lake_row(
                actual_table,
                lake_row_id,
                corrected_row,
                source_batch_id=actual_batch,
                source_row_id=source_row,
                receipt_id=receipt_id,
                commit_status="confirmed",
            )
            from app.services.write_job import WriteJobService
            write_job_result = WriteJobService().acquire_and_write(
                actual_table, "APPEND", [corrected_row]
            )
            snapshot_id = write_job_result.get("snapshot_id") if write_job_result.get("success") else None
            confirmed_record = self._business_row(confirmed_lake_row)
            now = datetime.now().isoformat(timespec="seconds")
            receipt = {
                "receipt_id": receipt_id,
                "status": "confirmed",
                "confirmed_at": now,
                "lake_table": actual_table,
                "batch_id": actual_batch,
                "source_batch_id": actual_batch,
                "original_batch_id": actual_batch,
                "quarantine_record_id": qid,
                "original_quarantine_id": qid,
                "source_row_id": source_row,
                "original_row_id": source_row,
                "original_row_ref": source_row,
                "lake_row_id": lake_row_id,
                "correlation_id": correlation_id,
                "snapshot_id": snapshot_id,
                "write_job": write_job_result,
                "confirmed_record": confirmed_record,
                "actual_confirmed_record": confirmed_record,
                "duplicate": False,
            }
            reflow_job = {
                "reflow_job_id": reflow_job_id,
                "status": "confirmed",
                "lake_table": actual_table,
                "batch_id": actual_batch,
                "source_batch_id": actual_batch,
                "quarantine_record_id": qid,
                "source_row_id": source_row,
                "lake_row_id": lake_row_id,
                "snapshot_id": snapshot_id,
                "receipt_id": receipt_id,
                # 更正行有独立幂等写入资格，不必被原批迟到回执阻塞；
                # 原批重放由 (湖表, 批次, 原始行) 确定性 row_id 去重。
                "ingest_qualification": "separate_ingest_qualified",
                "qualification": "已取得单独更正入湖资格",
                "separate_ingest_eligible": True,
                "requires_original_batch_reconciliation": False,
                "waiting_for_original_reconciliation": False,
                "original_batch_receipt_status": original_receipt_status,
                "safe_under_original_replay": True,
            }
            correction = {
                "ok": True,
                "success": True,
                "http_status": 200,
                "correction_id": correction_id,
                "idempotency_key": idempotency_key or natural_key,
                "idempotency_natural_key": natural_key,
                "corrected_row": corrected_row,
                "lake_table": actual_table,
                "batch_id": actual_batch,
                "source_batch_id": actual_batch,
                "original_batch_id": actual_batch,
                "quarantine_id": qid,
                "quarantine_record_id": qid,
                "original_quarantine_id": qid,
                "source_row_id": source_row,
                "original_row_id": source_row,
                "original_row_ref": source_row,
                "lake_row_id": lake_row_id,
                "correlation_id": correlation_id,
                "snapshot_id": snapshot_id,
                "lineage": {
                    "correlation_id": correlation_id,
                    "lake_table": actual_table,
                    "original_batch_id": actual_batch,
                    "quarantine_record_id": qid,
                    "source_row_id": source_row,
                    "correction_id": correction_id,
                    "receipt_id": receipt_id,
                    "lake_row_id": lake_row_id,
                },
                "status": "confirmed",
                "duplicate": False,
                "receipt": receipt,
                "reflow_job": reflow_job,
                "confirmed_record": confirmed_record,
                "message": "回炉更正已复检通过并确认入湖",
            }
            saved, duplicated = lake_store.save_correction(natural_key, correction)
            if duplicated:
                return saved

            lake_store.update_quarantine_record(qid, {
                "_quarantine_status": "已回炉",
                "_disposition": f"已更正 amount={amount} 并回炉确认",
                "_last_correction_id": correction_id,
                "_last_receipt_id": receipt_id,
                "_lake_row_id": lake_row_id,
            }, lake_table=actual_table)
            updated_qr = lake_store.get_quarantine_record(qid, actual_table)
            if updated_qr is not None:
                entry = self.sync_store_entry(updated_qr)
                entry["status"] = "已回炉"
                entry["pending"] = False
            return correction

    def _reject_correction(
        self, qr: dict[str, Any], table: str, batch: str,
        qid: str, source_row: str, code: str, message: str,
    ) -> dict[str, Any]:
        lake_store.update_quarantine_record(qid, {
            "_quarantine_status": "已隔离",
            "_last_recheck_result": code,
        }, lake_table=table)
        self.sync_store_entry(lake_store.get_quarantine_record(qid, table) or qr)
        return {
            "ok": False,
            "success": False,
            "http_status": 422,
            "error_code": code,
            "error": message,
            "lake_table": table,
            "batch_id": batch,
            "quarantine_record_id": qid,
            "source_row_id": source_row,
            "status": "still_quarantined",
        }

    @staticmethod
    def _business_row(lake_row: dict[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in lake_row.items() if not str(k).startswith("_")}

    # ---------- 旧接口/通用动作 ----------

    def release_for_retry(self, batch_id: str, lake_table: str | None = None) -> dict[str, Any]:
        """回炉重入：从隔离区取出数据行备重新入湖（原始记录保留）。"""
        rows = lake_store.release_quarantine(batch_id, lake_table=lake_table)
        return {
            "ok": True,
            "batch_id": batch_id,
            "lake_table": lake_table,
            "released_count": len(rows),
            "released_rows": rows,
        }

    def run_action(
        self, entry_id: int, action: str, payload: Any | None = None
    ) -> tuple[dict[str, Any] | None, str]:
        data = dict(getattr(payload, "values", None) or {})
        if not isinstance(data, dict):
            data = {}
        if payload is not None and hasattr(payload, "model_dump"):
            dumped = payload.model_dump()
            extra = {
                k: v for k, v in dumped.items()
                if k not in {"action", "remark", "values", "idempotency_key"}
            }
            data = {**extra, **data}
            if getattr(payload, "idempotency_key", None):
                data.setdefault("idempotency_key", payload.idempotency_key)
        remark = getattr(payload, "remark", None)
        if remark:
            try:
                parsed = json.loads(remark)
                if isinstance(parsed, dict):
                    data = {**parsed, **data}
            except (json.JSONDecodeError, TypeError):
                if "amount=" in remark:
                    amount_text = remark.split("amount=", 1)[1].split()[0].strip()
                    data.setdefault("amount", amount_text)

        if action in {"重试回炉", "回炉重试"}:
            result = self.retry_reflow(entry_id, data)
            if not result.get("ok"):
                return None, str(result.get("error") or "回炉复检未通过")
            entry = store.find(self.MODULE, entry_id)
            if entry is not None:
                entry["receipt"] = result.get("receipt")
                entry["last_receipt"] = result.get("receipt")
                entry["retry_result"] = result
            return entry, "回炉更正已确认，回执已生成"

        entry, message = super().run_action(entry_id, action)
        if entry is not None and action in self.ACTION_RULES:
            qid = entry.get("quarantine_record_id")
            if qid:
                lake_store.update_quarantine_record(str(qid), {
                    "_quarantine_status": self.ACTION_RULES[action],
                    "_disposition": message,
                })
                qr = lake_store.get_quarantine_record(str(qid))
                if qr:
                    self.sync_store_entry(qr)
        return entry, message

    def dispose_permanently(self, batch_id: str) -> dict[str, Any]:
        """永久留存：标记处置结论为留存，不释放。"""
        saved = lake_store.list_quarantine(batch_id)
        return {
            "batch_id": batch_id,
            "disposition": "permanent_archive",
            "saved_count": len(saved),
        }

    def summary_by_lake_table(self, lake_table: str) -> dict[str, Any]:
        """汇总指定湖表的隔离区统计。"""
        all_quarantined = lake_store.list_quarantine()
        table_quarantined: dict[str, int] = {}
        for qr in all_quarantined:
            qr_table = qr.get("_quarantine_table")
            if qr_table:
                if qr_table != lake_table:
                    continue
            else:
                br = lake_store.get_batch(str(qr.get("_quarantine_batch", "")))
                if br and br.lake_table != lake_table:
                    continue
            rule = qr.get("_quarantine_rule", "unknown")
            table_quarantined[rule] = table_quarantined.get(rule, 0) + 1
        return {
            "lake_table": lake_table,
            "total_quarantined": sum(table_quarantined.values()),
            "by_rule": table_quarantined,
        }

    retry_quarantine = retry_reflow
    retry_row = retry_reflow
    correct_row = retry_reflow
    submit_correction = retry_reflow
    correct_and_retry = retry_reflow
    recheck_and_reflow = retry_reflow
    reflow_quarantine_row = retry_reflow
    retry_quarantine_reflow = retry_reflow
