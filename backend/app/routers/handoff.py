"""质量判定、隔离、回炉更正与入湖查询之间的专用交接接口。

通用 CRUD 路由仍保留；本路由把长流程所需的幂等、湖表作用域和回执结构显式化。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, Query, Request
from fastapi.responses import JSONResponse

from app.lake_store import lake_store
from app.services.quality_rule import QualityRuleService
from app.services.quarantine_zone import QuarantineZoneService
from app.services.ingest_batch import IngestBatchService

router = APIRouter(prefix="/api", tags=["质量-隔离-回炉交接"])
quality_service = QualityRuleService()
quarantine_service = QuarantineZoneService()


def _response(result: dict[str, Any]) -> JSONResponse:
    status_code = int(result.pop("http_status", 200) or 200)
    if status_code >= 400:
        result.setdefault("detail", result.get("error") or result.get("message") or "request failed")
    return JSONResponse(status_code=status_code, content=result)


# ---------------- 质量检查入口 ----------------

_QUALITY_CHECK_PATHS = [
    "/quality/check",
    "/quality/check-batch",
    "/quality/batch-check",
    "/quality/gate",
    "/quality/submit",
    "/quality/inspect-batch",
    "/quality_rule/check",
    "/quality_rule/check-batch",
    "/quality_rule/check_batch",
    "/quality_rule/batch-check",
    "/quality_rule/quality-check",
    "/quality_rule/submit-check",
    "/quality_rules/check",
    "/quality_rules/check-batch",
    "/quality_rules/check_batch",
    "/quality-rules/check",
    "/quality-rules/check-batch",
    "/quality-rules/batch-check",
]


@router.post("/quality/apply")
@router.post("/quality/evaluate")
@router.post("/quality_rule/apply")
@router.post("/quality_rule/evaluate")
@router.post("/quality_rules/apply")
@router.post("/quality_rules/evaluate")
@router.post("/quality-rules/apply")
@router.post("/quality-rules/evaluate")
async def apply_quality(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    data = QualityRuleService.unwrap_payload(payload)
    batch_id = str(data.get("batch_id") or data.get("批次编号") or "")
    lake_table = data.get("lake_table") or data.get("所属湖表")
    rules = data.get("rules") or data.get("rule")
    if batch_id:
        return quality_service.apply_rules_to_batch(batch_id, rules, lake_table)
    rows = data.get("rows") or data.get("records") or []
    return quality_service.evaluate_rows(rows, rules)


def _submit_quality_check(
    payload: dict[str, Any] = Body(default_factory=dict),
) -> JSONResponse:
    return _response(quality_service.submit_quality_check(payload))


for _path in _QUALITY_CHECK_PATHS:
    router.add_api_route(
        _path,
        _submit_quality_check,
        methods=["POST"],
        name=f"quality_check_{_path}",
    )


# ---------------- 原批回执/重放 ----------------

@router.post("/ingest_batch/ingest")
@router.post("/ingest_batch/receive")
@router.post("/ingest-batch/ingest")
async def receive_ingest_batch(payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    data = QualityRuleService.unwrap_payload(payload)
    rows = data.get("rows") or data.get("records") or []
    result = IngestBatchService().ingest_batch(
        str(data.get("lake_table") or data.get("table") or data.get("所属湖表") or ""),
        str(data.get("source_system") or data.get("来源系统") or "API"),
        rows if isinstance(rows, list) else [],
        str(data.get("window_start") or ""),
        str(data.get("window_end") or ""),
        batch_id=data.get("batch_id") or data.get("批次编号"),
        idempotency_key=data.get("idempotency_key"),
    )
    return _response(result)


@router.post("/ingest_batch/receipt")
@router.post("/ingest-batch/receipt")
@router.post("/ingest_batch/ack")
@router.post("/batch/receipt")
@router.post("/batches/receipt")
@router.post("/quality/receipt")
async def original_receipt(payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    return _response(quality_service.receive_original_receipt(payload))


@router.post("/ingest_batch/replay")
@router.post("/ingest-batch/replay")
@router.post("/batch/replay")
@router.post("/scheduler/replay")
async def replay_batch(payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    # submit_quality_check 对同湖表同批次同内容天然幂等；调度器重放直接复用。
    return _response(quality_service.submit_quality_check(payload))


# ---------------- 隔离明细与重试回炉 ----------------

@router.get("/quarantine/records")
@router.get("/quarantine_zone/records")
@router.get("/quarantine-zone/records")
async def quarantine_records(
    lake_table: str | None = Query(default=None),
    batch_id: str | None = Query(default=None),
) -> dict[str, Any]:
    items = lake_store.list_quarantine(batch_id, lake_table=lake_table)
    return {"ok": True, "items": items, "total": len(items)}


@router.get("/quarantine/records/{record_id}")
@router.get("/quarantine/record/{record_id}")
@router.get("/quarantine/detail/{record_id}")
@router.get("/quarantine-records/{record_id}")
@router.get("/quarantine_records/{record_id}")
@router.get("/quarantine_zone/detail/{record_id}")
@router.get("/quarantine_zone/records/{record_id}")
@router.get("/quarantine-zone/detail/{record_id}")
async def quarantine_detail(
    record_id: str,
    lake_table: str | None = Query(default=None),
    batch_id: str | None = Query(default=None),
    source_row_id: str | None = Query(default=None),
) -> JSONResponse:
    result = quarantine_service.get_detail(
        record_id,
        lake_table=lake_table,
        batch_id=batch_id,
        source_row_id=source_row_id,
    )
    return _response(result)


@router.get("/quarantine_zone/{entry_id}/detail")
async def quarantine_entry_detail(
    entry_id: int,
    lake_table: str | None = Query(default=None),
) -> JSONResponse:
    result = quarantine_service.get_detail(entry_id, lake_table=lake_table)
    return _response(result)


@router.get("/quarantine_zone/export")
async def quarantine_export(
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
):
    # 必须在 /quarantine_zone/{record_id} 前保留通用导出路径，避免被字符串详情路由吞掉。
    from app.routers.resource import export_entries
    return export_entries("quarantine_zone", request, keyword=keyword, status=status)


@router.get("/quarantine_zone/{record_id}")
@router.get("/quarantine-zone/{record_id}")
async def quarantine_flat_detail(
    record_id: str,
    lake_table: str | None = Query(default=None),
    batch_id: str | None = Query(default=None),
    source_row_id: str | None = Query(default=None),
) -> JSONResponse:
    # 数字 ID 保持通用资源契约：顶层就是 entry；字符串隔离标识返回关联详情。
    if record_id.isdigit():
        entry = quarantine_service.get_entry(int(record_id))
        if entry is None:
            message = f"{quarantine_service.ENTITY} {record_id} 不存在或已归档"
            return JSONResponse(status_code=404, content={
                "ok": False, "message": message, "detail": message,
            })
        return JSONResponse(content=entry)

    result = quarantine_service.get_detail(
        record_id,
        lake_table=lake_table,
        batch_id=batch_id,
        source_row_id=source_row_id,
    )
    if not result.get("ok"):
        return _response(result)
    entry = result.get("entry") if isinstance(result.get("entry"), dict) else {}
    content = {**entry, **result}
    return JSONResponse(content=content)


_RETRY_PATHS = [
    "/quarantine/retry",
    "/quarantine/retry-reflow",
    "/quarantine/reflow/retry",
    "/quarantine/records/retry",
    "/quarantine-records/retry",
    "/quarantine-records/retry-reflow",
    "/quarantine_records/retry",
    "/quarantine_zone/retry",
    "/quarantine_zone/retry-reflow",
    "/quarantine_zone/retry_reflow",
    "/quarantine-zone/retry",
    "/quarantine-zone/retry-reflow",
]


@router.post("/quarantine/{record_id}/retry")
@router.post("/quarantine/retry/{record_id}")
@router.post("/quarantine/{record_id}/retry-reflow")
@router.post("/quarantine/{record_id}/reflow")
@router.post("/quarantine-records/{record_id}/retry")
@router.post("/quarantine_records/{record_id}/retry")
@router.post("/quarantine_zone/retry/{record_id}")
@router.post("/quarantine_zone/{record_id}/retry")
@router.post("/quarantine_zone/{record_id}/retry-reflow")
@router.post("/quarantine_zone/{record_id}/retry_reflow")
@router.post("/quarantine-zone/{record_id}/retry")
async def retry_quarantine_by_path(
    record_id: str,
    payload: dict[str, Any] = Body(default_factory=dict),
) -> JSONResponse:
    return _response(quarantine_service.retry_reflow(record_id, payload))


for _path in _RETRY_PATHS:
    async def _retry_root(payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
        record_id = (
            payload.get("quarantine_record_id")
            or payload.get("quarantine_id")
            or payload.get("record_id")
            or payload.get("id")
        )
        return _response(quarantine_service.retry_reflow(record_id, payload))

    router.add_api_route(
        _path,
        _retry_root,
        methods=["POST"],
        name=f"quarantine_retry_{_path}",
    )


# ---------------- 回执/回炉作业查询 ----------------

@router.get("/quarantine_zone/{record_id}/receipt")
@router.get("/quarantine/{record_id}/receipt")
async def quarantine_receipt(record_id: str) -> JSONResponse:
    detail = quarantine_service.get_detail(record_id)
    if not detail.get("ok"):
        return _response(detail)
    receipts = detail.get("receipts") or []
    if not receipts:
        return JSONResponse(status_code=404, content={
            "ok": False, "error_code": "receipt_not_found",
            "error": "该隔离记录尚无确认回执",
        })
    return JSONResponse(content={"ok": True, "receipt": receipts[-1], "receipts": receipts})


@router.get("/receipts/{receipt_id}")
@router.get("/receipt/{receipt_id}")
async def get_receipt(receipt_id: str) -> JSONResponse:
    receipt = lake_store.get_receipt(receipt_id)
    if receipt is None:
        return JSONResponse(status_code=404, content={
            "ok": False, "success": False, "error_code": "receipt_not_found",
            "error": "回执不存在；若回执在响应途中丢失，可用原更正请求幂等重发",
        })
    return JSONResponse(content={"ok": True, "receipt": receipt})


@router.get("/reflow/jobs")
@router.get("/reflow-jobs")
@router.get("/reflow_jobs")
async def list_reflow_jobs(
    lake_table: str | None = Query(default=None),
    batch_id: str | None = Query(default=None),
    quarantine_record_id: str | None = Query(default=None),
) -> dict[str, Any]:
    items = list(lake_store._corrections.values())
    if lake_table:
        items = [item for item in items if item.get("lake_table") == lake_table]
    if batch_id:
        items = [item for item in items if item.get("batch_id") == batch_id]
    if quarantine_record_id:
        items = [
            item for item in items
            if item.get("quarantine_record_id") == quarantine_record_id
        ]
    return {"ok": True, "total": len(items), "items": items}


@router.get("/reflow/{correction_id}")
@router.get("/reflow-jobs/{correction_id}")
@router.get("/corrections/{correction_id}")
async def get_correction(correction_id: str) -> JSONResponse:
    correction = lake_store.get_correction(correction_id)
    if correction is None:
        return JSONResponse(status_code=404, content={
            "ok": False, "success": False, "error_code": "correction_not_found",
        })
    return JSONResponse(content={"ok": True, "correction": correction})


# ---------------- 入湖查询 ----------------

@router.get("/lake/rows")
@router.get("/lake_rows")
@router.get("/lake-rows")
@router.get("/lakeRows")
@router.get("/lake/query")
@router.get("/lake_query")
@router.get("/lake-query")
@router.get("/ingest/query")
@router.get("/ingest_query")
@router.get("/ingest-query")
@router.get("/ingest/rows")
@router.get("/ingest_rows")
@router.get("/ingested_rows")
async def lake_query(
    request: Request,
    lake_table: str | None = Query(default=None),
    table: str | None = Query(default=None),
    lakeTable: str | None = Query(default=None),
    amount: str | None = Query(default=None),
    batch_id: str | None = Query(default=None),
) -> JSONResponse:
    resolved_table = lake_table or table or lakeTable or request.query_params.get("所属湖表") or ""
    if not resolved_table:
        return JSONResponse(status_code=422, content={
            "ok": False, "success": False,
            "error_code": "lake_table_required", "error": "lake_table 必填",
        })
    result = quality_service.query_lake(resolved_table, amount, batch_id)
    return JSONResponse(content=result)


@router.post("/lake/query")
@router.post("/lake_query")
@router.post("/lake-query")
@router.post("/ingest/query")
async def lake_query_post(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    lake_table = str(payload.get("lake_table") or payload.get("table") or payload.get("所属湖表") or "")
    amount = payload.get("amount")
    batch_id = payload.get("batch_id")
    return quality_service.query_lake(lake_table, amount, batch_id)
