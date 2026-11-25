"""通用接口契约；模块字段由领域服务持有，不再为每个模块复制一份模型类。"""
from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class PageResult(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int = 1
    size: int = 20


class ActionResult(BaseModel):
    ok: bool
    message: str
    entry: dict[str, Any] | None = None
    detail: dict[str, Any] | None = None
    field_errors: dict[str, Any] | list[Any] | None = None


class EntryPayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)
    remark: str | None = None


class ActionPayload(BaseModel):
    action: str = ""
    remark: str | None = None
    # 富动作载荷：配置更正、分片选源、乐观版本等
    values: dict[str, Any] | None = None
    source_id: int | None = None
    expected_version: int | None = None
    token: str | None = None
    conflict_policy: str | None = None
    execute: bool | None = None
