"""统一资源接口：查询、导出、登记、动作按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services import service_for

router = APIRouter(prefix="/api", tags=["业务资源"])


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, _ = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


# ---------------------------------------------------------------------------
# stream_source：并发度校验 / 连通检测回执 / 定义历史（放在动态 entry 路由之前）
# ---------------------------------------------------------------------------
def _extract_validate_value(request: Request, body: Any = None) -> Any:
    query_value: Any = request.query_params.get("value", request.query_params.get("并发度"))
    if request.method == "GET" or body is None:
        return query_value
    if isinstance(body, str):
        return body
    if not isinstance(body, dict):
        return query_value
    for key in ("value", "input", "concurrency", "并发度"):
        if key in body:
            return body.get(key)
    if isinstance(body.get("values"), dict):
        return body["values"].get("并发度", body["values"].get("value"))
    return query_value


@router.post("/stream_source/validate")
async def validate_stream_source_value(request: Request) -> dict[str, Any]:
    try:
        body = await request.json()
    except Exception:
        body = None
    value = _extract_validate_value(request, body)
    result = require_service("stream_source").validate_concurrency("" if value is None else value)
    result["allowed_range"] = [1, 10]
    result["default"] = 1
    return result


@router.post("/stream_source/validate-concurrency")
async def validate_concurrency_alias_post(request: Request) -> dict[str, Any]:
    try:
        body = await request.json()
    except Exception:
        body = None
    value = _extract_validate_value(request, body)
    result = require_service("stream_source").validate_concurrency("" if value is None else value)
    result["allowed_range"] = [1, 10]
    result["default"] = 1
    return result


@router.get("/stream_source/validate-concurrency")
def validate_concurrency_alias_get(request: Request) -> dict[str, Any]:
    value = _extract_validate_value(request)
    result = require_service("stream_source").validate_concurrency("" if value is None else value)
    result["allowed_range"] = [1, 10]
    result["default"] = 1
    return result


@router.get("/stream_source/validate")
def validate_stream_source_value_get(request: Request) -> dict[str, Any]:
    value = _extract_validate_value(request)
    result = require_service("stream_source").validate_concurrency("" if value is None else value)
    result["allowed_range"] = [1, 10]
    result["default"] = 1
    return result


@router.get("/stream_source/receipts")
def list_all_receipts() -> dict[str, Any]:
    service = require_service("stream_source")
    return {"items": service.list_receipts(None), "total": len(service.list_receipts(None))}


@router.get("/stream_source/eligibility")
def stream_source_eligibility() -> dict[str, Any]:
    service = require_service("stream_source")
    items = service.all_source_states()
    return {"items": items, "total": len(items)}


@router.post("/stream_source/check")
async def check_stream_source_from_body(request: Request) -> dict[str, Any]:
    """连接检测的集合式入口：兼容 POST /api/stream_source/check  body {id|source_id}。"""
    service = require_service("stream_source")
    try:
        body = await request.json()
    except Exception:
        body = {}
    source_id = body.get("source_id", body.get("id")) if isinstance(body, dict) else None
    if source_id is None:
        raise HTTPException(status_code=422, detail="缺少 source_id/id")
    result = service.check_connectivity(int(source_id))
    if not result.get("ok") and result.get("reason") == "源配置不存在":
        raise HTTPException(status_code=404, detail=result["reason"])
    return result


@router.post("/stream_source/{entry_id}/connectivity")
@router.post("/stream_source/{entry_id}/check")
@router.post("/stream_source/{entry_id}/check-connectivity")
@router.get("/stream_source/{entry_id}/connectivity")
def check_stream_source_connectivity(entry_id: int) -> dict[str, Any]:
    service = require_service("stream_source")
    result = service.check_connectivity(entry_id)
    if not result.get("ok") and result.get("reason") == "源配置不存在":
        raise HTTPException(status_code=404, detail=result["reason"])
    return result


@router.post("/stream_source/{entry_id}/update")
def update_stream_source_via_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    service = require_service("stream_source")
    entry, errors = service.update_entry(entry_id, payload.values)
    if entry is None:
        raise HTTPException(status_code=404, detail=str(errors) or "源配置不存在")
    return ActionResult(
        ok=not errors,
        message="配置已更正，定义版本已推进，请重新完成校验" if errors else "配置已更新",
        entry=entry,
        field_errors=errors or None,
    )


@router.get("/stream_source/{entry_id}/receipts")
@router.get("/stream_source/{entry_id}/history")
def stream_source_history(entry_id: int) -> dict[str, Any]:
    service = require_service("stream_source")
    history = service.source_history(entry_id)
    if not history.get("ok", True) and history.get("reason") == "源配置不存在":
        raise HTTPException(status_code=404, detail=history["reason"])
    return history


# ---------------------------------------------------------------------------
# batch_slice：可用源 / 分片选择打开与确认
# ---------------------------------------------------------------------------
@router.get("/batch_slice/sources/available")
def batch_available_sources() -> dict[str, Any]:
    service = require_service("batch_slice")
    return service.list_available_sources()


@router.post("/batch_slice/{batch_id}/selection")
@router.post("/batch_slice/{batch_id}/select-source")
def batch_open_selection(batch_id: int, payload: ActionPayload) -> dict[str, Any]:
    service = require_service("batch_slice")
    result = service.open_selection(
        batch_id,
        payload.source_id,
        expected_version=payload.expected_version,
        remark=payload.remark,
    )
    if not result.get("ok") and "不存在" in str(result.get("reason", "")):
        raise HTTPException(status_code=404, detail=result["reason"])
    return result


@router.post("/batch_slice/{batch_id}/confirm")
def batch_confirm_selection(batch_id: int, payload: ActionPayload) -> dict[str, Any]:
    service = require_service("batch_slice")
    return service.confirm_selection(
        batch_id,
        payload.token,
        execute=bool(payload.execute),
        conflict_policy=payload.conflict_policy or "snapshot",
    )


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int) -> dict[str, Any]:
    service = require_service(module)
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    return entry


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    field_errors = entry.get("field_errors") if isinstance(entry, dict) else None
    if field_errors:
        # 草稿已落库可继续更正，但登记回执明确不通过、字段已定位，且后续启用会被阻止
        detail_text = "；".join(
            f"{field}：{message}" if not str(message).startswith(str(field)) else str(message)
            for field, message in field_errors.items()
        )
        return ActionResult(
            ok=False,
            message=f"配置字段校验未通过：{detail_text}",
            entry=entry,
            field_errors=field_errors,
        )
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.patch("/{module}/{entry_id}", response_model=ActionResult)
@router.put("/{module}/{entry_id}", response_model=ActionResult)
def update_entry(module: str, entry_id: int, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    updater = getattr(service, "update_entry", None)
    if updater is None:
        raise HTTPException(status_code=400, detail=f"{module} 不支持配置更正")
    entry, errors = updater(entry_id, payload.values)
    if entry is None:
        raise HTTPException(status_code=404, detail=str(errors) or f"{service.ENTITY} {entry_id} 不存在")
    ok = not errors
    return ActionResult(
        ok=ok,
        message="配置已更正，定义版本已推进，请重新完成校验" if errors else "配置已更新",
        entry=entry,
        field_errors=errors or None,
    )


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload) -> ActionResult:
    service = require_service(module)
    result = service.execute_action(entry_id, payload.action.strip(), payload)
    return ActionResult(
        ok=bool(result.get("ok")),
        message=str(result.get("message", "")),
        entry=result.get("entry"),
        detail=result.get("detail"),
        field_errors=result.get("field_errors"),
    )
