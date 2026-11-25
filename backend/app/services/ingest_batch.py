"""入湖批次领域服务：批次入库、去重、快照绑定与校验值对比。"""
from __future__ import annotations

import hashlib
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.store import store
from app.lake_store import lake_store


class IngestBatchService(BaseModuleService):
    MODULE = "ingest_batch"
    ENTITY = "入湖批次"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["批次编号", "所属湖表", "来源系统", "批次行数", "采集窗口", "落湖快照", "完整校验值", "入湖时间"]
    REQUIRED_FIELDS = ["批次编号", "所属湖表", "来源系统"]
    STATUS_ORDER = ["待入湖", "入湖中", "已入湖", "入湖失败"]
    ACTION_RULES = {"启动入湖": "入湖中", "确认入湖": "已入湖", "标记失败": "入湖失败"}
    NEGATIVE_ACTIONS = []

    def ingest_batch(self, lake_table: str, source_system: str,
                     rows: list[dict[str, Any]], window_start: str,
                     window_end: str, batch_id: str | None = None,
                     idempotency_key: str | None = None) -> dict[str, Any]:
        """接收入湖批次数据：去重、存储、并与快照绑定。

        显式传入 batch_id 时按 (lake_table, batch_id) 幂等；调度器重放原批不会
        因随机生成新批次而绕过对账。
        """
        requested_batch_id = batch_id or idempotency_key
        replay_existing = None
        if requested_batch_id:
            replay_existing = lake_store.get_batch(requested_batch_id, lake_table)

        # 先去重再存储
        seen = set()
        deduped: list[dict[str, Any]] = []
        for r in rows:
            key = hashlib.md5(str(sorted(r.items())).encode()).hexdigest()
            if key not in seen:
                seen.add(key)
                deduped.append(r)

        if replay_existing is not None:
            replay_checksum = hashlib.md5(
                str(sorted([str(r) for r in deduped])).encode()
            ).hexdigest()
            same_payload = replay_existing.checksum == replay_checksum
            if same_payload:
                lake_store.update_batch_status(
                    requested_batch_id, "ingesting", lake_table=lake_table
                )
                return {
                    "ok": True,
                    "success": True,
                    "replayed": True,
                    "idempotent_replay": True,
                    "batch_id": requested_batch_id,
                    "lake_table": lake_table,
                    "row_count": len(deduped),
                    "duplicates_removed": len(rows) - len(deduped),
                    "checksum": replay_existing.checksum,
                    "window": f"{window_start} ~ {window_end}",
                }
            return {
                "ok": False,
                "success": False,
                "http_status": 409,
                "error_code": "batch_payload_conflict",
                "error": "同湖表同编号批次已存在，但重放内容不一致",
                "batch_id": requested_batch_id,
                "lake_table": lake_table,
            }

        batch_id = requested_batch_id or f"batch-{lake_table}-{uuid.uuid4().hex[:6]}"
        br = lake_store.store_batch(batch_id, lake_table, source_system,
                                    deduped, window_start, window_end)
        lake_store.update_batch_status(batch_id, "ingesting", lake_table=lake_table)
        return {
            "ok": True,
            "success": True,
            "replayed": False,
            "batch_id": batch_id,
            "lake_table": lake_table,
            "row_count": len(deduped),
            "duplicates_removed": len(rows) - len(deduped),
            "checksum": br.checksum,
            "window": f"{window_start} ~ {window_end}",
        }

    def verify_batch_integrity(self, batch_id: str) -> dict[str, Any]:
        """校验批次完整性：重新计算校验值并对比。"""
        br = lake_store.get_batch(batch_id)
        if br is None:
            return {"success": False, "error": "batch not found"}
        recalc = hashlib.md5(
            str(sorted([str(r) for r in br.rows])).encode()
        ).hexdigest()
        return {
            "batch_id": batch_id,
            "stored_checksum": br.checksum,
            "recalculated_checksum": recalc,
            "integrity_ok": recalc == br.checksum,
        }

    def promote_to_ingested(self, batch_id: str, snapshot_id: str) -> dict[str, Any]:
        """操作已入湖：绑定快照并标记已入湖。"""
        br = lake_store.update_batch_status(batch_id, "ingested", snapshot_id)
        if br is None:
            return {"success": False, "error": "batch not found"}
        return {
            "batch_id": batch_id,
            "status": "ingested",
            "snapshot_id": snapshot_id,
        }