"""湖表存储引擎：文件注册表、分区清单、快照链、水位存储与批次数据。

本模块提供跨服务共享的不可变数据层：
- 文件注册：每个分区文件有唯一 file_id、所属分区、所属湖表、行数、字节数
- 快照链：每个湖表按时间线记录快照，含 file_ids、manifest、父快照指针
- 水位：每张湖表的已提交/待提交水位记录
- 批次数据：入湖批次的行级数据存储，含校验值
- 隔离区：被质量规则拦截的数据行，按批次隔离
- 写锁：并发提交冲突检测

设计原则：
- 所有写操作在提交前必须拿到全局锁
- 快照只在事务完整落湖后标记为可用
- 归并时先写入新文件清单，再原子地替换旧文件列表
"""
from __future__ import annotations

import copy
import hashlib
import json
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


# ----------------------------- 文件注册 -----------------------------

@dataclass
class PartitionFile:
    """分区文件：每个文件属于一个分区。"""
    file_id: str
    lake_table: str
    partition_key: str
    file_rows: int
    file_bytes: int
    file_manifest: str  # 文件内容摘要（MD5）
    created_at: float


# ----------------------------- 快照 -----------------------------

@dataclass
class LakeSnapshot:
    """湖表快照：记录某个时刻整个湖表的分区文件清单。"""
    snapshot_id: str
    lake_table: str
    parent_snapshot: str | None
    file_ids: list[str]
    added_rows: int
    deleted_rows: int
    manifest_checksum: str
    status: str  # generating | available | expired | rolled_back
    created_at: float


# ----------------------------- 水位 -----------------------------

@dataclass
class WatermarkRecord:
    """入湖水位追踪：记录已提交和待提交水位。"""
    lake_table: str
    field_name: str
    committed_value: str
    pending_value: str | None
    strategy: str  # append | merge | overwrite
    last_advanced_at: float


# ----------------------------- 批次数据 -----------------------------

@dataclass
class BatchRecord:
    """入湖批次：一次入湖操作导出的一组数据行。"""
    batch_id: str
    lake_table: str
    source_system: str
    rows: list[dict[str, Any]]  # 实际数据行
    row_count: int
    window_start: str
    window_end: str
    checksum: str
    snapshot_id: str | None  # 落湖后分配的 snap
    status: str  # pending | ingesting | ingested | failed
    created_at: float


# ----------------------------- 写锁 -----------------------------

class WriteLock:
    """全局写入锁：同一湖表同时只能有一个写入者提交。"""

    def __init__(self) -> None:
        self._locks: dict[str, threading.Lock] = {}
        self._lock = threading.Lock()

    def acquire(self, lake_table: str) -> threading.Lock:
        with self._lock:
            if lake_table not in self._locks:
                self._locks[lake_table] = threading.Lock()
            table_lock = self._locks[lake_table]
        table_lock.acquire()
        return table_lock

    def release(self, lake_table: str) -> None:
        with self._lock:
            lock = self._locks.get(lake_table)
        if lock is not None:
            try:
                lock.release()
            except RuntimeError:
                pass


# ----------------------------- 存储引擎 -----------------------------

class LakeStore:
    """湖表存储引擎主类。

    管理三大维度：
    - files: 分区文件注册表
    - snapshots: 快照链
    - watermarks: 水位
    - batches: 批次数据
    - quarantine: 隔离区数据
    - write_lock: 并发提交的控制锁
    """

    def __init__(self) -> None:
        # 文件注册表：lake_table -> {file_id -> PartitionFile}
        self._files: dict[str, dict[str, PartitionFile]] = {}

        # 快照链：lake_table -> list[LakeSnapshot]（时间序）
        self._snapshots: dict[str, list[LakeSnapshot]] = {}

        # 水位：lake_table -> WatermarkRecord
        self._watermarks: dict[str, WatermarkRecord] = {}

        # 批次数据：逻辑键为 (lake_table, batch_id)，实际存储键带湖表命名空间，
        # 避免不同湖表复用同一个批次编号时互相覆盖。
        self._batches: dict[str, BatchRecord] = {}
        self._batch_scope_index: dict[tuple[str, str], str] = {}
        self._batch_id_aliases: dict[str, set[str]] = {}

        # 隔离区：batch_id -> list[dict]（被拦截的数据行，原始记录保留以便回查）
        self._quarantine: dict[str, list[dict[str, Any]]] = {}

        # 入湖后的可查询数据：lake_table -> lake_row_id -> row
        self._lake_rows: dict[str, dict[str, dict[str, Any]]] = {}

        # 质量检查结果与更正/回执的幂等索引
        self._quality_checks: dict[str, dict[str, Any]] = {}
        self._quality_check_by_batch: dict[tuple[str, str], str] = {}
        self._corrections: dict[str, dict[str, Any]] = {}
        self._correction_by_key: dict[str, str] = {}
        self._receipts: dict[str, dict[str, Any]] = {}

        # 交接状态的进程内锁；接口重试和调度器重放依赖它保证不产生重复行。
        self._handoff_lock = threading.RLock()

        # 写锁
        self._write_lock = WriteLock()

        # 归并中断恢复检查点：merge_id -> {partial_file_ids: [...], ...}
        self._merge_checkpoints: dict[str, dict[str, Any]] = {}

    # ---- 文件操作 ----

    def register_file(
        self, lake_table: str, partition_key: str, rows: int, bytes_: int, manifest: str | None = None
    ) -> PartitionFile:
        file_id = f"{lake_table}-p{partition_key}-{uuid.uuid4().hex[:8]}"
        checksum = manifest or hashlib.md5(f"{lake_table}:{partition_key}:{time.time()}".encode()).hexdigest()
        pf = PartitionFile(
            file_id=file_id,
            lake_table=lake_table,
            partition_key=partition_key,
            file_rows=rows,
            file_bytes=bytes_,
            file_manifest=checksum,
            created_at=time.time(),
        )
        self._files.setdefault(lake_table, {})[file_id] = pf
        return pf

    def list_files(self, lake_table: str, partition_key: str | None = None) -> list[PartitionFile]:
        files = list(self._files.get(lake_table, {}).values())
        if partition_key is not None:
            files = [f for f in files if f.partition_key == partition_key]
        return files

    def get_file(self, lake_table: str, file_id: str) -> PartitionFile | None:
        return self._files.get(lake_table, {}).get(file_id)

    def remove_files(self, lake_table: str, file_ids: list[str]) -> int:
        table_files = self._files.get(lake_table, {})
        removed = 0
        for fid in file_ids:
            if fid in table_files:
                del table_files[fid]
                removed += 1
        return removed

    def replace_file_list(
        self, lake_table: str, old_file_ids: list[str], new_files: list[PartitionFile]
    ) -> None:
        """原子替换：先注册新文件，再删除旧文件。保证归并不留半份清单。"""
        table_files = self._files.setdefault(lake_table, {})
        for nf in new_files:
            table_files[nf.file_id] = nf
        for fid in old_file_ids:
            table_files.pop(fid, None)

    # ---- 快照操作 ----

    def create_snapshot(
        self,
        lake_table: str,
        file_ids: list[str],
        parent_snapshot: str | None = None,
        added_rows: int = 0,
        deleted_rows: int = 0,
    ) -> LakeSnapshot:
        snap_id = f"snap-{lake_table}-{uuid.uuid4().hex[:8]}"
        manifest = hashlib.md5(
            ",".join(sorted(file_ids)).encode()
        ).hexdigest()
        snap = LakeSnapshot(
            snapshot_id=snap_id,
            lake_table=lake_table,
            parent_snapshot=parent_snapshot,
            file_ids=list(file_ids),
            added_rows=added_rows,
            deleted_rows=deleted_rows,
            manifest_checksum=manifest,
            status="generating",
            created_at=time.time(),
        )
        chain = self._snapshots.setdefault(lake_table, [])
        chain.append(snap)
        return snap

    def mark_snapshot_available(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                snap.status = "available"
                return snap
        return None

    def mark_snapshot_expired(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                snap.status = "expired"
                return snap
        return None

    def get_snapshot(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                return snap
        return None

    def list_snapshots(self, lake_table: str) -> list[LakeSnapshot]:
        return list(self._snapshots.get(lake_table, []))

    def get_snapshot_at_time(self, lake_table: str, timestamp: float) -> LakeSnapshot | None:
        """时间旅行：返回 <=timestamp 的最近可用快照。"""
        candidates = [
            s for s in self._snapshots.get(lake_table, [])
            if s.status == "available" and s.created_at <= timestamp
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda s: s.created_at)

    # ---- 水位操作 ----

    def set_watermark(
        self, lake_table: str, field_name: str, committed: str,
        strategy: str = "append"
    ) -> WatermarkRecord:
        wm = WatermarkRecord(
            lake_table=lake_table,
            field_name=field_name,
            committed_value=committed,
            pending_value=None,
            strategy=strategy,
            last_advanced_at=time.time(),
        )
        self._watermarks[lake_table] = wm
        return wm

    def advance_watermark(self, lake_table: str, new_value: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(lake_table)
        if wm is None:
            return None
        wm.pending_value = new_value
        return wm

    def commit_watermark(self, lake_table: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(lake_table)
        if wm is None or wm.pending_value is None:
            return None
        wm.committed_value = wm.pending_value
        wm.pending_value = None
        wm.last_advanced_at = time.time()
        return wm

    def get_watermark(self, lake_table: str) -> WatermarkRecord | None:
        return self._watermarks.get(lake_table)

    def list_watermarks(self) -> list[WatermarkRecord]:
        return list(self._watermarks.values())

    # ---- 批次数据 ----

    @staticmethod
    def _batch_scope_key(lake_table: str, batch_id: str) -> str:
        return f"{lake_table}\x1f{batch_id}"

    def store_batch(
        self, batch_id: str, lake_table: str, source_system: str,
        rows: list[dict[str, Any]], window_start: str, window_end: str,
    ) -> BatchRecord:
        checksum = hashlib.md5(
            str(sorted([str(r) for r in rows])).encode()
        ).hexdigest()
        scope_key = self._batch_scope_key(lake_table, batch_id)
        br = BatchRecord(
            batch_id=batch_id,
            lake_table=lake_table,
            source_system=source_system,
            rows=list(rows),
            row_count=len(rows),
            window_start=window_start,
            window_end=window_end,
            checksum=checksum,
            snapshot_id=None,
            status="pending",
            created_at=time.time(),
        )
        self._batches[scope_key] = br
        self._batch_scope_index[(lake_table, batch_id)] = scope_key
        self._batch_id_aliases.setdefault(batch_id, set()).add(scope_key)
        return br

    def get_batch(
        self, batch_id: str, lake_table: str | None = None
    ) -> BatchRecord | None:
        if lake_table is not None:
            scope_key = self._batch_scope_index.get((lake_table, batch_id))
            if scope_key is not None:
                return self._batches.get(scope_key)
            # 兼容历史调用：批次编号由系统生成且全局唯一时可直接命中。
            return self._batches.get(batch_id)

        direct = self._batches.get(batch_id)
        if direct is not None:
            return direct
        aliases = sorted(self._batch_id_aliases.get(batch_id, set()))
        if not aliases:
            return None
        # 不带湖表的旧接口只能在编号唯一时无歧义；重号时由调用方传 lake_table。
        return self._batches.get(aliases[-1])

    def list_batches(self, lake_table: str) -> list[BatchRecord]:
        return [b for b in self._batches.values() if b.lake_table == lake_table]

    def update_batch_status(
        self, batch_id: str, status: str, snapshot_id: str | None = None,
        lake_table: str | None = None,
    ) -> BatchRecord | None:
        br = self.get_batch(batch_id, lake_table)
        if br is None:
            return None
        br.status = status
        if snapshot_id is not None:
            br.snapshot_id = snapshot_id
        return br

    # ---- 入湖查询行 ----

    def upsert_lake_row(
        self,
        lake_table: str,
        row_id: str,
        row: dict[str, Any],
        *,
        source_batch_id: str | None = None,
        source_row_id: str | None = None,
        receipt_id: str | None = None,
        commit_status: str = "confirmed",
    ) -> dict[str, Any]:
        """按确定性 row_id 幂等写入入湖查询结果。"""
        with self._handoff_lock:
            table_rows = self._lake_rows.setdefault(lake_table, {})
            existing = table_rows.get(row_id)
            now = time.time()
            lake_row = dict(row)
            lake_row["_lake_row_id"] = row_id
            lake_row["_lake_table"] = lake_table
            lake_row["_source_batch_id"] = source_batch_id
            lake_row["_source_row_id"] = source_row_id
            lake_row["_receipt_id"] = receipt_id
            lake_row["_commit_status"] = commit_status
            lake_row["_updated_at"] = now
            if existing is None:
                lake_row["_created_at"] = now
                lake_row["_revision"] = 1
            else:
                lake_row["_created_at"] = existing.get("_created_at", now)
                lake_row["_revision"] = int(existing.get("_revision", 1)) + 1
            table_rows[row_id] = lake_row
            return copy.deepcopy(lake_row)

    def list_lake_rows(
        self, lake_table: str, *, include_internal: bool = False
    ) -> list[dict[str, Any]]:
        rows = list(self._lake_rows.get(lake_table, {}).values())
        if include_internal:
            return copy.deepcopy(rows)
        return [
            {k: copy.deepcopy(v) for k, v in row.items() if not k.startswith("_")}
            for row in copy.deepcopy(rows)
        ]

    def get_lake_row(self, lake_table: str, row_id: str) -> dict[str, Any] | None:
        row = self._lake_rows.get(lake_table, {}).get(row_id)
        return copy.deepcopy(row) if row is not None else None

    def confirm_batch_lake_rows(
        self, lake_table: str, batch_id: str, receipt_id: str | None = None,
        source_row_ids: set[str] | list[str] | tuple[str, ...] | None = None,
    ) -> int:
        """原批迟到回执到达后，把该批原始通过行标记为已对账确认。"""
        allowed = set(source_row_ids) if source_row_ids is not None else None
        count = 0
        with self._handoff_lock:
            for row in self._lake_rows.get(lake_table, {}).values():
                if row.get("_source_batch_id") != batch_id:
                    continue
                if allowed is not None and row.get("_source_row_id") not in allowed:
                    continue
                row["_commit_status"] = "confirmed"
                if receipt_id:
                    row["_receipt_id"] = receipt_id
                row["_reconciled_at"] = time.time()
                count += 1
        return count

    def query_lake_rows(
        self, lake_table: str, *, include_internal: bool = False
    ) -> list[dict[str, Any]]:
        return self.list_lake_rows(lake_table, include_internal=include_internal)

    def list_ingested_rows(self, lake_table: str) -> list[dict[str, Any]]:
        return self.list_lake_rows(lake_table)

    def get_ingested_rows(self, lake_table: str) -> list[dict[str, Any]]:
        return self.list_lake_rows(lake_table)

    get_lake_rows = list_lake_rows

    # ---- 质量检查/更正回执状态 ----

    def save_quality_check(self, key: tuple[str, str], result: dict[str, Any]) -> None:
        check_id = str(result.get("check_id") or f"qc-{uuid.uuid4().hex[:10]}")
        result["check_id"] = check_id
        with self._handoff_lock:
            self._quality_checks[check_id] = result
            self._quality_check_by_batch[key] = check_id

    def get_quality_check(self, lake_table: str, batch_id: str) -> dict[str, Any] | None:
        with self._handoff_lock:
            check_id = self._quality_check_by_batch.get((lake_table, batch_id))
            result = self._quality_checks.get(check_id) if check_id else None
            return copy.deepcopy(result) if result is not None else None

    def save_correction(
        self, natural_key: str, correction: dict[str, Any]
    ) -> tuple[dict[str, Any], bool]:
        """按自然幂等键保存更正；重发同一更正时返回首次记录。"""
        with self._handoff_lock:
            existing_id = self._correction_by_key.get(natural_key)
            if existing_id is not None:
                existing = self._corrections[existing_id]
                return copy.deepcopy(existing), True
            correction_id = str(
                correction.get("correction_id")
                or f"CORR-{hashlib.sha1(natural_key.encode()).hexdigest()[:16]}"
            )
            correction["correction_id"] = correction_id
            correction.setdefault("status", "confirmed")
            self._corrections[correction_id] = correction
            self._correction_by_key[natural_key] = correction_id
            receipt = correction.get("receipt")
            if isinstance(receipt, dict) and receipt.get("receipt_id"):
                self._receipts[str(receipt["receipt_id"])] = receipt
            return copy.deepcopy(correction), False

    def get_correction(self, correction_id: str) -> dict[str, Any] | None:
        correction = self._corrections.get(correction_id)
        return copy.deepcopy(correction) if correction is not None else None

    def get_receipt(self, receipt_id: str) -> dict[str, Any] | None:
        receipt = self._receipts.get(receipt_id)
        return copy.deepcopy(receipt) if receipt is not None else None

    # ---- 隔离区 ----

    def quarantine_rows(
        self, batch_id: str, rule_id: str, rule_name: str,
        rows: list[dict[str, Any]], lake_table: str | None = None,
    ) -> list[dict[str, Any]]:
        """将被质量规则拦截的行放入隔离区。

        同一批次、同一原始行、同一规则的重复隔离请求保持幂等；原始记录在
        更正回炉后也不物理删除，方便后续回查和审计。
        """
        quarantined = []
        with self._handoff_lock:
            bucket = self._quarantine.setdefault(batch_id, [])
            existing_ids = {
                qr.get("_quarantine_record_id")
                for qr in bucket
                if qr.get("_quarantine_rule") == rule_id
            }
            for index, row in enumerate(rows):
                source_row_id = str(
                    row.get("_source_row_id")
                    or row.get("source_row_id")
                    or row.get("row_ref")
                    or (
                        f"row-{index}-"
                        + hashlib.sha1(
                            json.dumps(row, sort_keys=True, ensure_ascii=False, default=str).encode()
                        ).hexdigest()[:10]
                    )
                )
                record_id = str(
                    row.get("_quarantine_record_id")
                    or row.get("quarantine_record_id")
                    or f"QR-{lake_table or 'unknown_table'}-{batch_id}-{rule_id}-{source_row_id}"
                )
                if record_id in existing_ids:
                    prior = next(
                        (qr for qr in bucket
                         if qr.get("_quarantine_record_id") == record_id),
                        None,
                    )
                    if prior is not None:
                        quarantined.append(copy.deepcopy(prior))
                    continue

                qr = {
                    k: copy.deepcopy(v)
                    for k, v in row.items()
                    if not str(k).startswith("_source_")
                }
                qr["_quarantine_batch"] = batch_id
                qr["_quarantine_table"] = lake_table or row.get("所属湖表", "")
                qr["_quarantine_rule"] = rule_id
                qr["_quarantine_rule_name"] = rule_name
                qr["_quarantine_time"] = time.time()
                qr["_quarantine_record_id"] = record_id
                qr["_source_row_id"] = source_row_id
                qr["_quarantine_status"] = "已隔离"
                bucket.append(qr)
                existing_ids.add(record_id)
                quarantined.append(copy.deepcopy(qr))
        return quarantined

    def list_quarantine(
        self, batch_id: str | None = None, lake_table: str | None = None
    ) -> list[dict[str, Any]]:
        with self._handoff_lock:
            if batch_id is not None:
                result = list(self._quarantine.get(batch_id, []))
            else:
                result = []
                for rows in self._quarantine.values():
                    result.extend(rows)
            if lake_table is not None:
                result = [
                    row for row in result
                    if not row.get("_quarantine_table")
                    or row.get("_quarantine_table") == lake_table
                ]
            return copy.deepcopy(result)

    def get_quarantine_record(
        self, record_id: str, lake_table: str | None = None
    ) -> dict[str, Any] | None:
        for row in self.list_quarantine(lake_table=lake_table):
            if str(row.get("_quarantine_record_id")) == str(record_id):
                return row
            if str(row.get("id", "")) == str(record_id):
                return row
            if str(row.get("隔离编号", "")) == str(record_id):
                return row
        return None

    def update_quarantine_record(
        self, record_id: str, updates: dict[str, Any],
        lake_table: str | None = None,
    ) -> dict[str, Any] | None:
        with self._handoff_lock:
            for rows in self._quarantine.values():
                for row in rows:
                    matches = (
                        str(row.get("_quarantine_record_id")) == str(record_id)
                        or str(row.get("id", "")) == str(record_id)
                        or str(row.get("隔离编号", "")) == str(record_id)
                    )
                    table_ok = (
                        lake_table is None
                        or not row.get("_quarantine_table")
                        or row.get("_quarantine_table") == lake_table
                    )
                    if matches and table_ok:
                        row.update(updates)
                        return copy.deepcopy(row)
        return None

    def release_quarantine(
        self, batch_id: str, lake_table: str | None = None
    ) -> list[dict[str, Any]]:
        """回炉重入：复制隔离行作为待写入数据，但保留原始隔离记录。"""
        rows = self.list_quarantine(batch_id, lake_table=lake_table)
        with self._handoff_lock:
            for qr in self._quarantine.get(batch_id, []):
                if lake_table is None or qr.get("_quarantine_table") == lake_table:
                    qr["_quarantine_status"] = "已回炉"
                    qr["_released_at"] = time.time()
        cleaned: list[dict[str, Any]] = []
        for r in rows:
            cleaned_row = {k: v for k, v in r.items() if not k.startswith("_")}
            cleaned.append(cleaned_row)
        return cleaned

    # ---- 归并检查点 ----

    def save_merge_checkpoint(self, merge_id: str, partial_file_ids: list[str]) -> None:
        self._merge_checkpoints[merge_id] = {
            "partial_file_ids": list(partial_file_ids),
            "saved_at": time.time(),
        }

    def get_merge_checkpoint(self, merge_id: str) -> dict[str, Any] | None:
        return self._merge_checkpoints.get(merge_id)

    def clear_merge_checkpoint(self, merge_id: str) -> None:
        self._merge_checkpoints.pop(merge_id, None)

    # ---- 并发写锁 ----

    def acquire_write_lock(self, lake_table: str) -> threading.Lock:
        return self._write_lock.acquire(lake_table)

    def release_write_lock(self, lake_table: str) -> None:
        self._write_lock.release(lake_table)

    # ---- 统计快照 ----

    def stats_snapshot(self, lake_table: str) -> dict[str, Any]:
        """生成湖表统计画像快照。"""
        files = self.list_files(lake_table)
        snapshots = self.list_snapshots(lake_table)
        wm = self.get_watermark(lake_table)
        total_rows = sum(f.file_rows for f in files)
        total_bytes = sum(f.file_bytes for f in files)
        partition_count = len(set(f.partition_key for f in files))
        return {
            "lake_table": lake_table,
            "file_count": len(files),
            "total_rows": total_rows,
            "total_bytes": total_bytes,
            "partition_count": partition_count,
            "available_snapshots": sum(1 for s in snapshots if s.status == "available"),
            "watermark": wm.committed_value if wm else None,
        }


# 全局单例
lake_store = LakeStore()