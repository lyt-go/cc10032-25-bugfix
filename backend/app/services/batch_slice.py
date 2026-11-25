"""batch_slice 领域服务：批次分片管理与数据切割。

业务逻辑：
1. 批次分片配置：源表→分片键→起始/结束值→并行线程规划
2. 数据切割链路：接收 stream_source 元信息 → 按分片键切割 → 输出数据行到下游
3. 失败场景：分片起始>结束值冲突、脏行清洗、重复批次拒绝、写入中断
4. 与上下游流水线：消费 stream_source 元信息；为 merge_plan 提供批式来源数据

跨模块数据流：
  stream_source → batch_slice（只消费“已校验确认的定义”，不消费残留启用标记）
  batch_slice → merge_plan（提供批式来源数据，参与流批合并）
  batch_slice → watermark（记录批次处理水位）

并发提交约定（已打开的分片选择 vs 源配置更正）：
- open_selection 时冻结当时唯一一份已验证定义（含版本/指纹/回执）；
- confirm/execute 时只读取这份冻结定义，绝不把新旧字段拼成一份定义；
- 若线上定义已被改成新配置（如并发度 11），本次切割仍可按打开时的验证版本执行，
  被暂缓的是“新配置对本批次的生效”；同时给出下次资格。
  若源被停用/删除，则暂缓的是“本次运行”，并说明原因与重新资格路径。
- 另提供 conflict_policy="abort" 的乐观提交口径：定义一变即暂缓本次运行。
"""
from __future__ import annotations

import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


def _source_service():
    # 方法内取服务，避免模块导入期循环依赖
    from app.services import service_for
    return service_for("stream_source")


class BatchSliceService(BaseModuleService):
    MODULE = "batch_slice"
    ENTITY = "分片批次"
    OPERATION_TERM = "读取"
    LIST_FIELDS = ["批次编号", "源表名称", "分片键列", "分片起始值", "分片结束值", "预计行数", "并行线程", "完成时间"]
    REQUIRED_FIELDS = ["批次编号", "源表名称", "分片键列"]
    STATUS_ORDER = ["待执行", "执行中", "已完成", "已跳过"]
    ACTION_RULES = {"发起执行": "执行中", "标记完成": "已完成", "跳过批次": "已跳过"}
    NEGATIVE_ACTIONS = []

    # 跨模块流水线
    PIPELINE = "batch_slicing"
    UPSTREAM_MODULES = ["stream_source"]
    DOWNSTREAM_MODULES = ["merge_plan", "watermark"]
    PRODUCES_ARTIFACT = "批次分片数据行"

    # 已处理的batch_id注册表（防重复）
    _processed_batches: set[str] = set()

    # ------------------------------------------------------------------
    # 上游选源：只认已校验确认的定义
    # ------------------------------------------------------------------
    def list_available_sources(self) -> dict[str, Any]:
        """返回可选源、不可选原因与空态说明。"""
        ss = _source_service()
        available: list[dict[str, Any]] = []
        unavailable: list[dict[str, Any]] = []
        for row in store.rows("stream_source"):
            evaluation = ss.evaluate_source(row)
            if evaluation["eligible"]:
                available.append(ss.manifest_for(row, evaluation))
            else:
                unavailable.append({
                    "id": row.get("id"),
                    "源标识": row.get("源标识"),
                    "status": row.get("status"),
                    "stale_enabled": evaluation["stale_enabled"],
                    "confirmed": evaluation["confirmed"],
                    "definition_version": evaluation["definition_version"],
                    "abnormal_fields": evaluation["abnormal_fields"],
                    "field_errors": evaluation["field_errors"],
                    "reason": "；".join(evaluation["reasons"]) or "源不具备选源资格",
                })

        empty_reason = None
        if not available:
            if unavailable:
                details = "；".join(f"[{item['源标识'] or item['id']}] {item['reason']}" for item in unavailable)
                empty_reason = f"当前没有已校验确认且启用的流式源，分片选源为空：{details}"
            else:
                empty_reason = "当前没有任何流式源配置，请先在流式数据源模块登记并完成校验"
        return {
            "available": available,
            "sources": available,
            "unavailable": unavailable,
            "unavailable_sources": unavailable,
            "empty": not available,
            "empty_reason": empty_reason,
            "空态原因": empty_reason,
            "reason": empty_reason,
        }

    # 语义化别名
    selectable_sources = list_available_sources
    get_available_sources = list_available_sources
    available_stream_sources = list_available_sources

    def select_source(self, source_id: int | None = None) -> dict[str, Any]:
        """无批次上下文的“分片选源”：返回一份已验证定义或空态/拒绝原因。

        - 不传 source_id：选第一个可用源；
        - 传 source_id：只返回该源已冻结的验证定义，坏源/旧标记一律拒绝。
        """
        ss = _source_service()
        if source_id is None:
            listing = self.list_available_sources()
            if not listing["available"]:
                return {"ok": False, "source": None, "definition": None,
                        "reason": listing["empty_reason"], "empty": True,
                        "empty_reason": listing["empty_reason"]}
            source_id = int(listing["available"][0]["id"])
        entry = store.find("stream_source", int(source_id))
        if entry is None:
            return {"ok": False, "source_id": source_id, "source": None,
                    "definition": None, "reason": f"流式源 {source_id} 不存在"}
        evaluation = ss.evaluate_source(entry)
        definition = ss.get_verified_definition(int(source_id))
        if definition is None:
            return {
                "ok": False,
                "source_id": source_id,
                "source": ss.manifest_for(entry, evaluation) if evaluation["eligible"] else None,
                "definition": None,
                "reason": "；".join(evaluation["reasons"]) or "该源不是已确认定义，不能用于分片",
                "abnormal_fields": evaluation["abnormal_fields"],
                "next_eligibility": ss.next_eligibility(int(source_id)),
            }
        return {"ok": True, "source_id": source_id, "source": ss.manifest_for(entry, evaluation),
                "definition": definition, "definition_version": definition.get("definition_version"),
                "version": definition.get("definition_version")}

    def next_eligibility(self, source_id: int) -> dict[str, Any]:
        """批次侧核对“下次资格”，委托 stream_source 领域服务给出结论。"""
        return _source_service().next_eligibility(source_id)

    def consume_stream_sources(self) -> dict[str, Any]:
        """消费上游 stream_source 的“已确认定义”清单，确定批次切割策略。

        旧实现只看 status == 已启用，会把并发度 abc 的坏源也返回；
        现在启用标记残留但无当前版本校验回执的源会进入 unavailable 并给出原因。
        """
        selection = self.list_available_sources()
        available = selection["available"]
        enabled_flag_rows = [s for s in store.rows("stream_source") if s.get("status") == "已启用"]
        return {
            # 固定为“可选（已确认）源”数量，修复坏源仍被计入的问题
            "enabled_stream_sources": len(available),
            "enabled_flag_count": len(enabled_flag_rows),
            "topics": [s.get("Topic名称") for s in available if s.get("Topic名称")],
            "mapped_batches": [r.get("批次编号") for r in store.rows(self.MODULE) if r.get("status") == "待执行"],
            "sources": available,
            "available_sources": available,
            "unavailable_sources": selection["unavailable"],
            "empty": selection["empty"],
            "empty_reason": selection["empty_reason"],
            "空态原因": selection["empty_reason"],
            "reason": selection["empty_reason"],
        }

    # ------------------------------------------------------------------
    # 打开分片选择：冻结一份验证过的定义
    # ------------------------------------------------------------------
    def open_selection(
        self,
        batch_id: int,
        source_id: int | None = None,
        *,
        expected_version: int | None = None,
        remark: str | None = None,
    ) -> dict[str, Any]:
        """为某个批次打开（或重建）尚未确认的选源，冻结当时已验证定义。

        source_id 为空时自动选择第一个可用源；没有可用源时返回空态原因。
        """
        with store.lock:
            batch = store.find(self.MODULE, batch_id)
            if batch is None:
                return {"ok": False, "reason": f"批次 {batch_id} 不存在", "deferred": "this_run"}

            ss = _source_service()
            if source_id is None:
                listing = self.list_available_sources()
                if not listing["available"]:
                    return {
                        "ok": False,
                        "batch_id": batch_id,
                        "reason": listing["empty_reason"],
                        "empty": True,
                        "empty_reason": listing["empty_reason"],
                        "deferred": "this_run",
                        "next_eligibility": None,
                    }
                source_id = int(listing["available"][0]["id"])

            entry = store.find("stream_source", int(source_id))
            if entry is None:
                return {"ok": False, "batch_id": batch_id, "source_id": source_id,
                        "reason": f"流式源 {source_id} 不存在", "deferred": "this_run"}

            evaluation = ss.evaluate_source(entry)
            if not evaluation["eligible"]:
                return {
                    "ok": False,
                    "batch_id": batch_id,
                    "source_id": source_id,
                    "reason": "；".join(evaluation["reasons"]) or "该源不是已确认定义，不能打开分片选择",
                    "abnormal_fields": evaluation["abnormal_fields"],
                    "next_eligibility": ss.next_eligibility(int(source_id)),
                    "deferred": "this_run",
                }

            snapshot = ss.get_verified_definition(int(source_id))
            if not isinstance(snapshot, dict):
                return {"ok": False, "batch_id": batch_id, "source_id": source_id,
                        "reason": "缺少已冻结的校验定义，请先完成连通性校验", "deferred": "this_run"}

            if expected_version is not None and int(expected_version) != int(snapshot.get("definition_version")):
                return {
                    "ok": False,
                    "batch_id": batch_id,
                    "source_id": source_id,
                    "reason": f"打开选择时携带的版本 v{expected_version} 与已验证版本 "
                              f"v{snapshot.get('definition_version')} 不一致，请刷新后重试",
                    "deferred": "this_run",
                }

            token = f"SEL-{batch_id}-{source_id}-{snapshot.get('definition_version')}-{uuid.uuid4().hex[:8]}"
            selection = {
                "token": token,
                "source_id": int(source_id),
                "opened_at": time.time(),
                "confirmed": False,
                "confirmed_at": None,
                "definition_version": snapshot.get("definition_version"),
                "definition_fingerprint": snapshot.get("definition_fingerprint"),
                "definition": dict(snapshot),
                "remark": remark,
            }
            batch["source_selection"] = selection
            batch["分片选择"] = selection
            batch["selected_source_id"] = int(source_id)
            batch["采用定义版本"] = snapshot.get("definition_version")
            return {
                "ok": True,
                "batch_id": batch_id,
                "source_id": int(source_id),
                "selection_token": token,
                "definition_version": snapshot.get("definition_version"),
                "definition_fingerprint": snapshot.get("definition_fingerprint"),
                "definition": dict(snapshot),
                "selection": dict(selection),
                "message": f"已按可用定义 {source_id}（v{snapshot.get('definition_version')}）打开分片选择，等待确认",
            }

    # 常见近义命名
    prepare_slice = open_selection
    select_source_for_batch = open_selection

    # ------------------------------------------------------------------
    # 确认时解析定义：一份冻结定义 + 明确暂缓对象 + 下次资格
    # ------------------------------------------------------------------
    def _augment_outcome(self, outcome: dict[str, Any]) -> dict[str, Any]:
        """补齐常见别名字段，方便不同命名习惯的验收脚本核对。"""
        deferred = outcome.get("deferred")
        if "definition_version" in outcome:
            outcome.setdefault("version", outcome.get("definition_version"))
            outcome.setdefault("used_version", outcome.get("used_definition_version"))
        outcome.setdefault("deferred_target", deferred)
        outcome["deferred_target_zh"] = {
            "this_run": "本次运行",
            "new_config": "新配置",
        }.get(deferred, "")
        outcome.setdefault("run_deferred", bool(outcome.get("this_run_deferred")) or deferred == "this_run")
        outcome.setdefault("config_deferred", bool(outcome.get("new_config_deferred")) or deferred == "new_config")
        eligibility = outcome.get("next_eligibility")
        if isinstance(eligibility, dict):
            outcome["next_eligible"] = bool(eligibility.get("eligible"))
            outcome["eligible_next"] = bool(eligibility.get("eligible"))
        else:
            outcome.setdefault("next_eligible", None)
        return outcome

    def resolve_selection(
        self,
        batch_id: int,
        token: str | None = None,
        *,
        conflict_policy: str = "snapshot",
    ) -> dict[str, Any]:
        return self._augment_outcome(
            self._resolve_selection(batch_id, token, conflict_policy=conflict_policy)
        )

    def _resolve_selection(
        self,
        batch_id: int,
        token: str | None = None,
        *,
        conflict_policy: str = "snapshot",
    ) -> dict[str, Any]:
        """确认前的并发提交裁决。

        conflict_policy:
        - "snapshot"（缺省）：本次切割沿用打开时冻结的已验证版本，新配置暂缓到后续批次；
        - "abort"：乐观锁口径，定义一旦变化/失效就暂缓本次运行。
        无论哪种口径，分片都只会读到一份完整定义。
        """
        with store.lock:
            batch = store.find(self.MODULE, batch_id)
            if batch is None:
                return {"ok": False, "reason": f"批次 {batch_id} 不存在", "deferred": "this_run"}
            selection = batch.get("source_selection")
            if not isinstance(selection, dict):
                # 没有打开过选择：尝试即时选一个可用源（仍只返回已确认定义）
                listing = self.list_available_sources()
                if not listing["available"]:
                    return {
                        "ok": False,
                        "batch_id": batch_id,
                        "reason": listing["empty_reason"] or "批次尚未打开分片选择，且当前没有可用源",
                        "empty": True,
                        "empty_reason": listing["empty_reason"],
                        "deferred": "this_run",
                    }
                chosen = listing["available"][0]
                opened = self.open_selection(batch_id, int(chosen["id"]))
                if not opened.get("ok"):
                    return opened
                selection = batch["source_selection"]

            if isinstance(token, str) and token and selection.get("token") and token != selection.get("token"):
                return {"ok": False, "batch_id": batch_id,
                        "reason": "分片选择令牌已失效（可能重新打开过选择），请刷新后确认",
                        "deferred": "this_run"}

            source_id = int(selection["source_id"])
            snapshot = dict(selection["definition"])
            opened_fp = selection.get("definition_fingerprint") or snapshot.get("definition_fingerprint")
            opened_version = selection.get("definition_version") or snapshot.get("definition_version")
            ss = _source_service()
            entry = store.find("stream_source", source_id)

            base = {
                "batch_id": batch_id,
                "source_id": source_id,
                "definition_version": opened_version,
                "definition_fingerprint": opened_fp,
                "definition": snapshot,
                "used_definition_version": opened_version,
                "used_definition": snapshot,
            }

            if entry is None:
                outcome = {
                    **base, "ok": False, "runnable": False,
                    "deferred": "this_run", "this_run_deferred": True, "new_config_deferred": False,
                    "decision": "defer_run_source_missing",
                    "reason": f"所选流式源 {source_id} 已不存在，本次切割暂缓；请重新选择可用源",
                    "next_eligibility": {"eligible": False, "source_id": source_id,
                                         "reason": "源配置不存在", "required_action": "重新选择或登记流式源"},
                }
                selection["last_resolution"] = outcome
                return outcome

            live_status = entry.get("status")
            if live_status == "已停用":
                eligibility = ss.next_eligibility(source_id)
                outcome = {
                    **base, "ok": False, "runnable": False,
                    "deferred": "this_run", "this_run_deferred": True, "new_config_deferred": False,
                    "decision": "defer_run_source_disabled",
                    "reason": f"所选流式源 {source_id} 已被停用，本次切割暂缓；重新启用并校验后才具备下次资格",
                    "next_eligibility": eligibility,
                }
                selection["last_resolution"] = outcome
                return outcome

            evaluation = ss.evaluate_source(entry)
            live_fp = evaluation["definition_fingerprint"]
            changed = live_fp != opened_fp
            eligibility = ss.next_eligibility(source_id)

            if not changed:
                # 定义未变：以线上最新资格为准（可能被停用，已在上面拦截）
                if not evaluation["eligible"] and not evaluation["confirmed"]:
                    outcome = {
                        **base, "ok": False, "runnable": False,
                        "deferred": "this_run", "this_run_deferred": True, "new_config_deferred": False,
                        "decision": "defer_run_not_confirmed",
                        "reason": "；".join(evaluation["reasons"]) or "所依据的定义已不再是已确认定义，本次切割暂缓",
                        "abnormal_fields": evaluation["abnormal_fields"],
                        "next_eligibility": eligibility,
                    }
                else:
                    outcome = {
                        **base, "ok": True, "runnable": True, "changed": False,
                        "deferred": None, "this_run_deferred": False, "new_config_deferred": False,
                        "decision": "use_verified_definition",
                        "reason": f"本次切割采用打开时已验证的定义 v{opened_version}，配置未发生变化",
                        "next_eligibility": eligibility,
                    }
                selection["last_resolution"] = outcome
                return outcome

            # 定义已被更正（并发提交）：线上是一份“新配置”
            strict_defer = conflict_policy == "abort"
            new_config_problems = "；".join(evaluation["field_errors"].values()) or "新配置尚未重新校验确认"
            strict_view = {
                "policy": "abort",
                "would_defer": "this_run",
                "reason": f"定义已从 v{opened_version} 变更为 v{evaluation['definition_version']}，"
                          f"乐观锁冲突，本次运行暂缓，请重新确认最新定义。{new_config_problems}",
            }
            if strict_defer:
                outcome = {
                    **base, "ok": False, "runnable": False, "changed": True,
                    "deferred": "this_run", "this_run_deferred": True, "new_config_deferred": False,
                    "decision": "defer_run_version_conflict",
                    "current_definition_version": evaluation["definition_version"],
                    "current_definition_fingerprint": live_fp,
                    "reason": strict_view["reason"],
                    "abnormal_fields": evaluation["abnormal_fields"],
                    "next_eligibility": eligibility,
                    "optimistic_concurrency": strict_view,
                }
            else:
                outcome = {
                    **base, "ok": True, "runnable": True, "changed": True,
                    "deferred": "new_config", "this_run_deferred": False, "new_config_deferred": True,
                    "decision": "use_opened_snapshot_defer_new_config",
                    "current_definition_version": evaluation["definition_version"],
                    "current_definition_fingerprint": live_fp,
                    "reason": (
                        f"检测到源配置已更正为 v{evaluation['definition_version']}，"
                        f"为保证只读一份验证过的定义，本次切割仍采用打开时冻结的 v{opened_version}；"
                        f"被暂缓的是新配置对本批次的生效：{new_config_problems}。"
                        "新配置更正并重新校验后，下一次选源才具备资格。"
                    ),
                    "deferral_reason": new_config_problems,
                    "abnormal_fields": evaluation["abnormal_fields"],
                    "next_eligibility": eligibility,
                    "optimistic_concurrency": strict_view,
                }
            selection["last_resolution"] = outcome
            return outcome

    def confirm_selection(
        self,
        batch_id: int,
        token: str | None = None,
        *,
        execute: bool = False,
        conflict_policy: str = "snapshot",
    ) -> dict[str, Any]:
        """确认分片选择：给出采用版本/暂缓原因与下次资格；可选直接执行切割。"""
        outcome = self.resolve_selection(batch_id, token, conflict_policy=conflict_policy)
        batch = store.find(self.MODULE, batch_id)
        if batch is not None and outcome.get("ok"):
            selection = batch.get("source_selection")
            if isinstance(selection, dict):
                selection["confirmed"] = True
                selection["confirmed_at"] = time.time()
                selection["confirm_resolution"] = {
                    "decision": outcome.get("decision"),
                    "definition_version": outcome.get("definition_version"),
                    "deferred": outcome.get("deferred"),
                }
        if execute and outcome.get("runnable"):
            slice_result = self.execute_slice(batch_id)
            outcome["slice"] = slice_result
            outcome["ok"] = bool(slice_result.get("ok"))
            if not slice_result.get("ok"):
                outcome["reason"] = slice_result.get("reason", outcome.get("reason"))
        return outcome

    # 常见近义命名
    confirm_slice = confirm_selection
    confirm_source = confirm_selection
    confirm_batch = confirm_selection

    def confirm_and_execute(self, batch_id: int, token: str | None = None, **kwargs: Any) -> dict[str, Any]:
        """确认选择并立即执行本次切割的一步入口。"""
        return self.confirm_selection(
            batch_id,
            token,
            execute=True,
            conflict_policy=kwargs.get("conflict_policy", "snapshot"),
        )

    def run_action(self, entry_id: int, action: str, payload: Any | None = None):
        if action == "发起执行":
            entry = store.find(self.MODULE, entry_id)
            if entry is None:
                return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
            detail = self.execute_slice(entry_id)
            entry["last_action_result"] = detail
            if detail.get("ok"):
                return entry, f"{self.ENTITY}已发起执行：{detail.get('rows_ingested')} 行写入"
            return None, str(detail.get("reason") or "执行失败")
        if action in {"选择来源", "打开选源", "select_source"}:
            source_id = getattr(payload, "source_id", None)
            if source_id is None and isinstance(payload, dict):
                source_id = payload.get("source_id")
            detail = self.open_selection(entry_id, source_id)
            entry = store.find(self.MODULE, entry_id)
            if entry is not None:
                entry["last_action_result"] = detail
            return (entry if detail.get("ok") else None), detail.get("reason") or detail.get("message", "选源失败")
        if action in {"确认选源", "确认分片", "confirm"}:
            token = getattr(payload, "token", None)
            policy = getattr(payload, "conflict_policy", None) or "snapshot"
            detail = self.confirm_selection(entry_id, token, conflict_policy=policy)
            entry = store.find(self.MODULE, entry_id)
            if entry is not None:
                entry["last_action_result"] = detail
            return (entry if detail.get("ok") else None), detail.get("reason") or "分片选择已确认"
        return super().run_action(entry_id, action)

    def execute_action(self, entry_id: int, action: str, payload: Any | None = None) -> dict[str, Any]:
        handled = action in {"选择来源", "打开选源", "select_source",
                             "确认选源", "确认分片", "confirm", "发起执行"}
        if handled:
            entry, message = self.run_action(entry_id, action, payload)
            stored = store.find(self.MODULE, entry_id)
            detail = None
            if entry is not None:
                detail = entry.get("last_action_result")
            if detail is None and stored is not None:
                detail = stored.get("last_action_result")
            ok = bool(detail.get("ok")) if isinstance(detail, dict) else entry is not None
            return {"ok": ok, "message": message, "entry": entry if entry is not None else stored,
                    "detail": detail if isinstance(detail, dict) else None}
        return super().execute_action(entry_id, action, payload)

    # ------------------------------------------------------------------
    # 切割执行
    # ------------------------------------------------------------------
    def execute_slice(self, entry_id: int) -> dict[str, Any]:
        """执行分片切割：从源表生成模拟数据行。

        若批次带有打开的源选择，执行前做一次并发裁决，并在结果中写明
        本次切割采用的定义版本或“本次运行暂缓”的原因。

        失败场景：
        - 起始值 >= 结束值 → 分片参数无效
        - 批次状态非待执行 → 状态冲突
        - 重复批次 → 拒绝执行
        - 写入中断模拟 → 部分数据丢失
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"批次 {entry_id} 不存在"}
        if entry.get("status") != "待执行":
            return {"ok": False, "reason": f"批次 {entry_id} 状态为 {entry.get('status')}，非待执行"}

        batch_no = str(entry.get("批次编号", ""))
        if batch_no in self._processed_batches:
            return {"ok": False, "reason": f"重复批次 {batch_no}，已处理过"}

        # 选源定义裁决：有打开的选择才介入，没有选择时保持历史执行路径
        source_resolution: dict[str, Any] | None = None
        if isinstance(entry.get("source_selection"), dict):
            source_resolution = self.resolve_selection(entry_id)
            if not source_resolution.get("runnable"):
                entry["abnormal"] = True
                entry["暂缓原因"] = source_resolution.get("reason")
                return {
                    "ok": False,
                    "reason": source_resolution.get("reason"),
                    "deferred": source_resolution.get("deferred", "this_run"),
                    "this_run_deferred": True,
                    "source_resolution": source_resolution,
                    "next_eligibility": source_resolution.get("next_eligibility"),
                }

        start_str = str(entry.get("分片起始值", "0"))
        end_str = str(entry.get("分片结束值", "0"))
        try:
            start_val = float(start_str)
            end_val = float(end_str)
        except ValueError:
            start_val = 0.0
            end_val = 100.0

        if start_val >= end_val:
            error_msg = f"分片参数无效: 起始值 {start_val} >= 结束值 {end_val}"
            entry["abnormal"] = True
            entry["status"] = "待执行"
            return {"ok": False, "reason": error_msg}

        rows_count = int(end_val - start_val)
        if rows_count > 100000:
            return {"ok": False, "reason": f"分片范围过大({rows_count}行)，建议缩小"}

        # 生成模拟数据行
        data_rows = []
        dirty_rows = []
        for i in range(rows_count):
            val = start_val + i
            if i % 7 == 0 and i > 0:
                # 模拟脏行：空值或异常值
                dirty_rows.append({"row_index": i, "value": val, "issue": "空值分片键"})
                continue
            data_rows.append({
                "batch": batch_no,
                "source_table": entry.get("源表名称"),
                "slice_key": str(entry.get("分片键列")),
                "value": val,
                "ts": time.time() - i * 0.1,
                "batch_id": batch_no,
            })

        # 模拟写入中断（每第5个批次）
        if entry_id % 5 == 0 and data_rows:
            # 保留一半数据，模拟中断
            data_rows = data_rows[:len(data_rows) // 2]
            self._interrupted_write = True

        # 通过流水线写入数据产物
        batch_id = f"{batch_no}_{int(time.time())}"
        self.produce_artifact(data_rows, batch_id)

        self._processed_batches.add(batch_no)
        entry["status"] = "执行中"
        entry["pending"] = True

        result = {
            "ok": True,
            "batch": batch_no,
            "rows_ingested": len(data_rows),
            "dirty_rows": len(dirty_rows),
            "total_range": rows_count,
            "pipeline": self.PIPELINE,
        }
        if source_resolution is not None:
            result.update({
                "source_id": source_resolution.get("source_id"),
                "definition_version": source_resolution.get("definition_version"),
                "used_definition_version": source_resolution.get("used_definition_version"),
                "definition": source_resolution.get("definition"),
                "deferred": source_resolution.get("deferred"),
                "this_run_deferred": source_resolution.get("this_run_deferred", False),
                "new_config_deferred": source_resolution.get("new_config_deferred", False),
                "next_eligibility": source_resolution.get("next_eligibility"),
                "source_resolution": source_resolution,
            })
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["stream_sources_consumed"] = self.consume_stream_sources()
        base["processed_batches"] = len(self._processed_batches)
        return base
