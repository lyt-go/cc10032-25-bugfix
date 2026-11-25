"""模块服务基类：共享查询、登记、动作和统计骨架，业务差异留在各模块文件。

支持跨模块数据流水线：每个领域服务可声明消费的上游模块、暴露自身产物，
形成输入→转换→版本化产物→下游消费的完整链路。
"""
from __future__ import annotations

from typing import Any

from app.store import store


class BaseModuleService:
    MODULE = ""
    ENTITY = ""
    LIST_FIELDS: list[str] = []
    REQUIRED_FIELDS: list[str] = []
    STATUS_ORDER: list[str] = []
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS: list[str] = []

    # ---- 跨模块流水线元信息 ----
    PIPELINE: str | None = None           # 所属流水线名称
    UPSTREAM_MODULES: list[str] = []       # 消费的上游模块
    DOWNSTREAM_MODULES: list[str] = []     # 产物的下游模块
    PRODUCES_ARTIFACT: str = ""            # 产生的数据产物名称

    # ---- 基础 CRUD（保持向后兼容） ----
    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = list(store.rows(self.MODULE))
        for field, expected in (filters or {}).items():
            if field in self.LIST_FIELDS:
                rows = [
                    row for row in rows
                    if expected.casefold() in str(row.get(field, "")).casefold()
                ]
        if keyword and self.LIST_FIELDS:
            field = self.LIST_FIELDS[0]
            rows = [row for row in rows if keyword.casefold() in str(row.get(field, "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(self.MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [
            field for field in self.REQUIRED_FIELDS
            if not str(values.get(field) or "").strip()
        ]
        if missing:
            return None, missing
        rows = store.rows(self.MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in self.LIST_FIELDS})
        entry["status"] = self.STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        target = self.ACTION_RULES.get(action)
        if target is None:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
        entry["status"] = target
        entry["pending"] = target != self.STATUS_ORDER[-1]
        entry["abnormal"] = action in self.NEGATIVE_ACTIONS
        return entry, f"{self.ENTITY}已{action}"

    def execute_action(self, entry_id: int, action: str, payload: Any | None = None) -> dict[str, Any]:
        """富结果动作入口：默认适配 run_action 的 (entry, message) 二元契约。

        子类可覆盖以返回 issues/detail/field_errors 等完整验收信息。
        """
        entry, message = self.run_action(entry_id, action)
        detail = entry.get("last_action_result") if entry is not None else None
        return {
            "ok": entry is not None,
            "message": message,
            "entry": entry,
            "detail": detail if isinstance(detail, dict) else None,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出模块级数据质量快照，供跨模块消费和题位源码锚点使用。"""
        rows = store.rows(self.MODULE)
        by_status = {
            status: sum(1 for row in rows if row.get("status") == status)
            for status in self.STATUS_ORDER
        }
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": self.PRODUCES_ARTIFACT or f"{self.ENTITY}数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for row in rows if row.get("abnormal")),
        }

    # ---- 跨模块数据流方法 ----
    def consume_upstream(self) -> dict[str, list[dict[str, Any]]]:
        """消费上游模块数据，返回 {module: rows}"""
        upstream_data: dict[str, list[dict[str, Any]]] = {}
        for mod in self.UPSTREAM_MODULES:
            upstream_data[mod] = list(store.rows(mod))
        return upstream_data

    def produce_artifact(self, data: list[dict[str, Any]], batch_id: str) -> dict[str, Any]:
        """产出数据制品，通过流水线写入并返回结果"""
        if not self.PIPELINE:
            return {"ok": False, "reason": "模块未绑定流水线"}
        pl = store.pipeline(self.PIPELINE)
        if pl is None:
            pl = store.create_pipeline(self.PIPELINE, self.UPSTREAM_MODULES, self.MODULE)
        ingested, dirty = pl.ingest(data, batch_id)
        return {"ok": True, "pipeline": self.PIPELINE, "ingested": ingested, "dirty_details": dirty}

    def pipeline_snapshot(self) -> dict[str, Any]:
        """获取当前流水线版本化快照"""
        if not self.PIPELINE:
            return {"error": "no_pipeline"}
        pl = store.pipeline(self.PIPELINE)
        if pl:
            return pl.versioned_snapshot()
        return {"error": "pipeline_not_found"}

    def pipeline_health(self) -> dict[str, Any]:
        """获取流水线健康报告"""
        if not self.PIPELINE:
            return {"error": "no_pipeline"}
        pl = store.pipeline(self.PIPELINE)
        if pl:
            return pl.health_report()
        return {"error": "pipeline_not_found"}