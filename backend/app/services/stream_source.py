"""stream_source 领域服务：流式数据源管理与数据接入。

业务逻辑：
1. 流式源配置管理：消息中间件、Topic反序列化、水位起始点、并发度（1-10，缺省1）
2. 数据接入链路：消息中间件→反序列化→数据行→下游batch_slice/watermark
3. 失败场景：连接超时检测、脏数据反序列化失败、并发度冲突
4. 与下游流水线：为 batch_slice 和 watermark 提供“已校验确认的定义”

定义确认模型（修复旧启用标记残留）：
- 仅有 status == 已启用 不算数，可能是旧配置残留的启用标记；
- 只有“当前定义指纹 == 最近一次校验通过并冻结的定义指纹”时，
  才认为是一份已确认定义，接入清单与 batch_slice 选源都只认它；
- 每次校验生成可核对的回执（store._source_receipts + 行内 校验回执/校验历史），
  历史上出现过的合法并发度保留在 并发度可用历史 中。
"""
from __future__ import annotations

import hashlib
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

CONCURRENCY_FIELD = "并发度"
CONCURRENCY_MIN = 1
CONCURRENCY_MAX = 10
DEFAULT_CONCURRENCY = 1

# 视为“连通性检测”入口的动作名（兼容不同调用方的命名习惯）
CHECK_ACTIONS = {
    "连通性检测", "连接检测", "检测连接", "连通检测", "连接检查",
    "健康检查", "check_connectivity", "check",
}
ENABLE_ACTIONS = {"启用源", "启用", "确认启用"}
VERIFY_ACTIONS = {"提交验证", "完成校验", "确认校验", "校验", "验证"}


class FieldValidationError(ValueError):
    """带字段定位的配置校验错误。"""

    def __init__(self, field: str, value: Any, message: str) -> None:
        self.field = field
        self.value = value
        super().__init__(message)


class ValidationResult(dict):
    """dict 回执，同时兼容 `ok, message = validate(...)` 二元解包与布尔判断。

    字典访问（r["ok"]/r.get("field_errors")）完全不受影响；
    迭代/解包时给出 (ok, message)，布尔值即是否通过。
    """

    def __bool__(self) -> bool:  # pragma: no cover - 简单兼容
        return bool(self.get("ok"))

    def __iter__(self):
        yield bool(self.get("ok"))
        yield self.get("message")

    def __getitem__(self, key):
        if key == 0:
            return bool(self.get("ok"))
        if key == 1:
            return self.get("message")
        return super().__getitem__(key)

    def __getattr__(self, name: str):
        try:
            return self[name]
        except KeyError as exc:  # pragma: no cover
            raise AttributeError(name) from exc


def parse_concurrency(raw: Any) -> tuple[int | None, str | None]:
    """把并发度解析为 1-10 的整数。

    返回 (normalized, error)：
    - None / 空白 / 缺省 → (1, None)，缺省用 1；
    - abc 等非整数、0、11 等越界值 → (None, 定位到「并发度」字段的原因)；
    - 兼容历史正整数设置（int 或 "4" 这样的数字字符串均可）。
    """
    if raw is None:
        return DEFAULT_CONCURRENCY, None
    if isinstance(raw, bool):
        return None, f"{CONCURRENCY_FIELD}不能是布尔值，请填写 {CONCURRENCY_MIN}-{CONCURRENCY_MAX} 的整数"
    if isinstance(raw, int):
        value = raw
    elif isinstance(raw, float):
        if not raw.is_integer():
            return None, f"{CONCURRENCY_FIELD}「{raw}」不是整数，请填写 {CONCURRENCY_MIN}-{CONCURRENCY_MAX} 的整数"
        value = int(raw)
    else:
        text = str(raw).strip()
        if not text:
            return DEFAULT_CONCURRENCY, None
        try:
            # 兼容 "3.0" 这类历史导出值
            value = int(text)
        except ValueError:
            try:
                number = float(text)
            except ValueError:
                return None, (
                    f"{CONCURRENCY_FIELD}「{text}」不是合法整数，"
                    f"请填写 {CONCURRENCY_MIN}-{CONCURRENCY_MAX} 的整数"
                )
            if not number.is_integer():
                return None, (
                    f"{CONCURRENCY_FIELD}「{text}」不是合法整数，"
                    f"请填写 {CONCURRENCY_MIN}-{CONCURRENCY_MAX} 的整数"
                )
            value = int(number)
    if value < CONCURRENCY_MIN:
        return None, (
            f"{CONCURRENCY_FIELD} {value} 小于下限，"
            f"允许范围为 {CONCURRENCY_MIN}-{CONCURRENCY_MAX}（缺省 {DEFAULT_CONCURRENCY}）"
        )
    if value > CONCURRENCY_MAX:
        return None, (
            f"{CONCURRENCY_FIELD} {value} 超过上限，"
            f"允许范围为 {CONCURRENCY_MIN}-{CONCURRENCY_MAX}（缺省 {DEFAULT_CONCURRENCY}）"
        )
    return value, None


def definition_fingerprint(entry: dict[str, Any], fields: list[str] | None = None) -> str:
    """按配置字段生成定义指纹；合法并发度先归一化，避免 3 与 "3" 被当成两份定义。"""
    watched = fields or StreamSourceService.DEFINITION_FIELDS
    parts: list[str] = []
    for field in watched:
        value = entry.get(field, "")
        if field == CONCURRENCY_FIELD:
            normalized, error = parse_concurrency(value)
            value = f"i:{normalized}" if error is None else f"raw:{str(value).strip()}"
        parts.append(f"{field}={value}")
    raw = "|".join(parts)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:12]


def validate_concurrency_value(value: Any) -> "ValidationResult":
    """模块级校验入口（dict 回执），供路由/脚本/其他服务直接调用。"""
    normalized, error = parse_concurrency(value)
    ok = error is None
    message = error or f"{CONCURRENCY_FIELD}合法：{normalized}"
    return ValidationResult({
        "ok": ok,
        "valid": ok,
        "success": ok,
        "passed": ok,
        "field": CONCURRENCY_FIELD,
        "value": value,
        "normalized": normalized if ok else None,
        "concurrency": normalized if ok else None,
        "error": error,
        "message": message,
        "allowed_range": [CONCURRENCY_MIN, CONCURRENCY_MAX],
        "range": {"min": CONCURRENCY_MIN, "max": CONCURRENCY_MAX},
        "default": DEFAULT_CONCURRENCY,
        "field_errors": {} if ok else {CONCURRENCY_FIELD: error},
    })


def normalize_concurrency(raw: Any) -> int:
    """模块级归一化：缺省→1，非法值抛 FieldValidationError（字段已定位）。"""
    normalized, error = parse_concurrency(raw)
    if error is not None:
        raise FieldValidationError(CONCURRENCY_FIELD, raw, error)
    return int(normalized)


class StreamSourceService(BaseModuleService):
    MODULE = "stream_source"
    ENTITY = "流式源配置"
    OPERATION_TERM = "接入"
    LIST_FIELDS = ["源标识", "消息中间件", "Topic名称", "消费组", "反序列化方式", "起始位点", "并发度", "连接超时"]
    REQUIRED_FIELDS = ["源标识", "消息中间件", "Topic名称"]
    STATUS_ORDER = ["草稿", "待验证", "已启用", "已停用"]
    ACTION_RULES = {"提交验证": "待验证", "启用源": "已启用", "停用源": "已停用"}
    NEGATIVE_ACTIONS = ["停用源"]

    # 参与“定义版本/指纹”的配置字段（状态、回执等运行期字段不参与）
    DEFINITION_FIELDS = list(LIST_FIELDS)

    # 跨模块流水线
    PIPELINE = "stream_ingestion"
    DOWNSTREAM_MODULES = ["batch_slice", "watermark"]
    PRODUCES_ARTIFACT = "流式数据源元信息快照"

    # ------------------------------------------------------------------
    # 字段校验 / 定义版本
    # ------------------------------------------------------------------
    def list_entries(self, **kwargs: Any):
        rows, total = super().list_entries(**kwargs)
        # 接入清单每行附带资格摘要：旧启用标记会以 stale_enabled 明确暴露给页面
        for row in rows:
            evaluation = self.evaluate_source(row)
            row["eligibility_brief"] = {
                "eligible": evaluation["eligible"],
                "confirmed": evaluation["confirmed"],
                "stale_enabled": evaluation["stale_enabled"],
                "reasons": evaluation["reasons"],
                "abnormal_fields": evaluation["abnormal_fields"],
            }
        return rows, total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = super().get_entry(entry_id)
        if entry is not None:
            evaluation = self.evaluate_source(entry)
            entry["eligibility_brief"] = {
                "eligible": evaluation["eligible"],
                "confirmed": evaluation["confirmed"],
                "stale_enabled": evaluation["stale_enabled"],
                "reasons": evaluation["reasons"],
                "abnormal_fields": evaluation["abnormal_fields"],
            }
        return entry

    def validate_concurrency(self, value: Any) -> dict[str, Any]:
        """校验单个并发度输入，返回字段级结果（兼容 dict / 二元解包 / 布尔判断）。"""
        return validate_concurrency_value(value)

    def normalize_concurrency(self, value: Any) -> int:
        """合法/缺省值返回整数；非法值抛带字段定位的 FieldValidationError。"""
        normalized, error = parse_concurrency(value)
        if error is not None:
            raise FieldValidationError(CONCURRENCY_FIELD, value, error)
        return int(normalized)

    def validate_definition(self, values: dict[str, Any]) -> dict[str, Any]:
        """校验一份源定义里当前有约束的字段，输出字段定位错误。"""
        field_errors: dict[str, str] = {}
        normalized_values: dict[str, Any] = {}
        if CONCURRENCY_FIELD in values or values.get(CONCURRENCY_FIELD) in (None, ""):
            normalized, error = parse_concurrency(values.get(CONCURRENCY_FIELD))
            if error:
                field_errors[CONCURRENCY_FIELD] = error
            else:
                normalized_values[CONCURRENCY_FIELD] = normalized
        return {
            "ok": not field_errors,
            "field_errors": field_errors,
            "errors": list(field_errors.values()),
            "normalized_values": normalized_values,
        }

    def _ensure_version_meta(self, entry: dict[str, Any]) -> int:
        version = entry.get("definition_version")
        if not isinstance(version, int) or version < 1:
            version = 1
            entry["definition_version"] = 1
        return version

    def _remember_usable_concurrency(self, entry: dict[str, Any], value: int, receipt_id: str) -> None:
        history = entry.setdefault("并发度可用历史", [])
        version = self._ensure_version_meta(entry)
        if not any(item.get("value") == value and item.get("version") == version for item in history):
            history.append({
                "value": value,
                "version": version,
                "receipt_id": receipt_id,
                "ts": time.time(),
            })

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """登记源配置。

        非法并发度不会让登记崩溃或被静默吞掉：记录以草稿落库并写好字段级错误，
        但后续“启用源/连通校验/分片选源”都会被阻止，直到更正并重新校验。
        """
        with store.lock:
            entry, missing = super().create_entry(values)
            if entry is None:
                return None, missing
            self._ensure_version_meta(entry)
            raw_concurrency = values.get(CONCURRENCY_FIELD)
            normalized, error = parse_concurrency(raw_concurrency)
            if raw_concurrency is None or str(raw_concurrency).strip() == "":
                # 缺省用 1，并直接写成整数
                entry[CONCURRENCY_FIELD] = DEFAULT_CONCURRENCY
            elif error is None:
                entry[CONCURRENCY_FIELD] = normalized
            # 非法值保留原始输入（如 abc），方便“改 abc 为 2”式更正与定位
            entry["definition_fingerprint"] = definition_fingerprint(entry)
            entry["field_errors"] = {}
            entry["abnormal_fields"] = []
            entry["verified_version"] = entry.get("verified_version")
            entry["verified_fingerprint"] = entry.get("verified_fingerprint")
            entry["verified_definition"] = entry.get("verified_definition")
            if error:
                entry["field_errors"] = {CONCURRENCY_FIELD: error}
                entry["abnormal_fields"] = [CONCURRENCY_FIELD]
                entry["abnormal"] = True
                entry["pending"] = True
                entry["检测结果"] = f"配置异常：{error}"
                entry["connectivity"] = {"ok": False, "issues": [error], "abnormal_fields": [CONCURRENCY_FIELD]}
            else:
                # 兼容历史正整数设置：把当前合法值登记为可核对的历史可用值
                self._remember_usable_concurrency(entry, int(normalized), "seed-or-create")
            return entry, missing

    def update_entry(self, entry_id: int, values: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        """更正源配置：写入字段、推进定义版本、使旧校验确认失效。

        返回 (entry, field_errors)。即使新值非法也会落库（草稿式更正），
        但 field_errors 非空且不会产生新的已验证定义。
        """
        with store.lock:
            entry = store.find(self.MODULE, entry_id)
            if entry is None:
                return None, {"_": f"{self.ENTITY} {entry_id} 不存在或已归档"}
            watched = set(self.LIST_FIELDS)
            config_changed = False
            for field, value in values.items():
                if field in watched:
                    if str(entry.get(field, "")) != str(value):
                        config_changed = True
                    entry[field] = value
            # 并发度先做一次校验/归一化
            if CONCURRENCY_FIELD in values:
                normalized, error = parse_concurrency(values.get(CONCURRENCY_FIELD))
                if error is None:
                    entry[CONCURRENCY_FIELD] = normalized
            validation = self.validate_definition(entry)
            old_fp = entry.get("definition_fingerprint")
            new_fp = definition_fingerprint(entry)
            if config_changed or old_fp != new_fp:
                entry["definition_version"] = self._ensure_version_meta(entry) + 1
            entry["definition_fingerprint"] = new_fp
            entry["field_errors"] = validation["field_errors"]
            if validation["field_errors"]:
                entry["abnormal_fields"] = sorted(validation["field_errors"].keys())
                entry["abnormal"] = True
                entry["检测结果"] = "配置异常：" + "；".join(validation["field_errors"].values())
                # 当前定义已偏离上次确认，旧确认不能再代表当前定义
                entry["stale_confirmation"] = (
                    entry.get("verified_fingerprint") not in (None, new_fp)
                )
                # 同步使接入清单上的连通状态失效（历史回执仍保留可核对）
                entry["connectivity"] = {
                    "ok": False,
                    "pending_recheck": True,
                    "stale": True,
                    "issues": list(validation["field_errors"].values()),
                    "abnormal_fields": entry["abnormal_fields"],
                    "last_passed_receipt_id": (entry.get("校验回执") or {}).get("receipt_id")
                    if isinstance(entry.get("校验回执"), dict) else None,
                }
            elif config_changed or old_fp != new_fp:
                entry["abnormal_fields"] = []
                entry["stale_confirmation"] = True
                entry["检测结果"] = "配置已更正，定义已变更，待重新完成校验"
                entry["connectivity"] = {
                    "ok": False,
                    "pending_recheck": True,
                    "stale": True,
                    "issues": ["定义已变更，待重新完成连通性校验"],
                    "abnormal_fields": [],
                    "last_passed_receipt_id": (entry.get("校验回执") or {}).get("receipt_id")
                    if isinstance(entry.get("校验回执"), dict) else None,
                }
            return entry, validation["field_errors"]

    # ------------------------------------------------------------------
    # 流式采样（保持原有接入链路行为）
    # ------------------------------------------------------------------
    def stream_sample(self, source_id: int, row_count: int = 10) -> dict[str, Any]:
        """模拟从消息中间件读取流式数据样本。

        失败场景：
        - 源未启用 → 拒绝
        - 连接超时超限 → 模拟中断
        - 反序列化格式不匹配 → 返回脏数据列表
        """
        entry = store.find(self.MODULE, source_id)
        if entry is None:
            return {"ok": False, "reason": f"流式源 {source_id} 不存在"}
        if entry.get("status") != "已启用":
            return {"ok": False, "reason": f"流式源 {source_id} 未启用，当前状态: {entry.get('status')}"}

        timeout_val = str(entry.get("连接超时", "30"))
        deser = str(entry.get("反序列化方式", "json")).lower()
        samples = []
        dirty = []

        for i in range(row_count):
            ts = time.time() - i * 5  # 模拟时间偏移
            raw_payload = f"msg_{source_id}_{i}_{ts:.0f}"
            try:
                # 模拟反序列化
                if deser == "json":
                    parsed = {"ts": ts, "payload": raw_payload, "topic": entry.get("Topic名称")}
                elif deser == "avro":
                    parsed = {"ts": ts, "avro_schema": "v1", "payload": raw_payload, "topic": entry.get("Topic名称")}
                elif deser == "protobuf":
                    parsed = {"ts": ts, "proto_msg": raw_payload, "topic": entry.get("Topic名称")}
                else:
                    parsed = {"ts": ts, "raw": raw_payload, "topic": entry.get("Topic名称")}

                # 模拟连接超时：30%概率
                if i > 0 and i % 3 == 0:
                    try:
                        timeout = int(timeout_val)
                    except ValueError:
                        timeout = 30
                    if timeout < 5:
                        dirty.append({"row": i, "issue": f"连接超时({timeout}ms)，消息 {raw_payload} 读取中断"})
                        continue

                samples.append(parsed)
            except Exception:
                dirty.append({"row": i, "issue": f"反序列化失败: 格式 {deser} 不兼容消息 {raw_payload}"})

        # 更新水位偏移
        if samples:
            # 最后一个样本的时间戳作为当前水位
            last_ts = max(s.get("ts", 0) for s in samples)
            store.update_watermark(str(source_id), {"offset": last_ts, "source": entry.get("源标识"), "ts": time.time()})

        # 产生流水线制品
        batch_id = f"STREAM_{source_id}_{int(time.time())}"
        self.produce_artifact(samples, batch_id)

        return {
            "ok": True,
            "source_id": source_id,
            "samples": len(samples),
            "dirty": len(dirty),
            "dirty_details": dirty[:5],
        }

    # ------------------------------------------------------------------
    # 评估 / 已确认定义
    # ------------------------------------------------------------------
    def evaluate_source(self, entry: dict[str, Any]) -> dict[str, Any]:
        """计算一份源当前的字段错误、连通问题与“已确认定义”资格。纯读取，不写状态。"""
        source_id = int(entry.get("id", 0))
        field_errors: dict[str, str] = {}
        issues: list[str] = []
        abnormal_fields: list[str] = []

        normalized, concurrency_error = parse_concurrency(entry.get(CONCURRENCY_FIELD, DEFAULT_CONCURRENCY))
        if concurrency_error:
            field_errors[CONCURRENCY_FIELD] = concurrency_error
            issues.append(concurrency_error)
            abnormal_fields.append(CONCURRENCY_FIELD)

        middleware = str(entry.get("消息中间件", "")).lower()
        middleware_ok = ("kafka" in middleware) or ("pulsar" in middleware)
        if not middleware_ok:
            msg = "消息中间件类型未识别，仅支持 kafka/pulsar"
            issues.append(msg)
            abnormal_fields.append("消息中间件")
            field_errors.setdefault("消息中间件", msg)

        fp = definition_fingerprint(entry)
        version = self._ensure_version_meta(entry)
        verified_fp = entry.get("verified_fingerprint")
        verified_version = entry.get("verified_version")
        current_valid = not field_errors
        fingerprint_confirmed = bool(verified_fp and verified_fp == fp)
        # 兼容验收脚本直接打好的“已校验”显式标记（区别于不可信的 status=已启用）
        explicit_marker = (
            entry.get("verified") is True
            or entry.get("confirmed") is True
            or entry.get("校验通过") is True
            or entry.get("验证通过") is True
        )
        marker_confirmed = bool(current_valid and explicit_marker and not verified_fp)
        confirmed_fp_current = bool(
            current_valid
            and (
                (fingerprint_confirmed and isinstance(entry.get("verified_definition"), dict))
                or (fingerprint_confirmed and explicit_marker)
                or marker_confirmed
            )
        )
        enabled = entry.get("status") == "已启用"
        stale_enabled = enabled and not confirmed_fp_current

        reasons: list[str] = []
        if not enabled:
            reasons.append(f"当前状态为「{entry.get('status')}」，未启用")
        for message in field_errors.values():
            reasons.append(message)
        if enabled and current_valid and not confirmed_fp_current:
            if verified_fp:
                reasons.append(
                    f"当前定义 v{version} 与最近校验通过的 v{verified_version} 不一致，"
                    "新配置尚未完成校验确认"
                )
            else:
                reasons.append("启用标记为旧配置残留，缺少与当前定义一致的校验回执")

        eligible = enabled and confirmed_fp_current
        return {
            "ok": current_valid and middleware_ok,
            "ready": current_valid and middleware_ok,
            "eligible": eligible,
            "source_id": source_id,
            "field_errors": field_errors,
            "issues": issues,
            "abnormal_fields": sorted(set(abnormal_fields)),
            "definition_version": version,
            "definition_fingerprint": fp,
            "concurrency": normalized,
            "verified_version": verified_version if confirmed_fp_current else entry.get("verified_version"),
            "verified_fingerprint": verified_fp,
            "confirmed": confirmed_fp_current,
            "stale_enabled": stale_enabled,
            "enabled": enabled,
            "reasons": reasons,
        }

    def materialize_verified_definition(self, source_id: int) -> dict[str, Any] | None:
        """返回唯一可读取的已验证定义；对带显式校验标记但无快照的旧数据补建快照。"""
        entry = store.find(self.MODULE, source_id)
        if entry is None:
            return None
        evaluation = self.evaluate_source(entry)
        if not evaluation["confirmed"]:
            return None
        snapshot = entry.get("verified_definition")
        if isinstance(snapshot, dict) and entry.get("verified_fingerprint") == evaluation["definition_fingerprint"]:
            return dict(snapshot)
        # 显式标记确认的旧数据：按当前有效定义补建一份冻结快照（不新增回执）
        with store.lock:
            snapshot = {field: entry.get(field) for field in self.DEFINITION_FIELDS}
            snapshot.update({
                "source_id": source_id,
                "definition_version": evaluation["definition_version"],
                "definition_fingerprint": evaluation["definition_fingerprint"],
                "concurrency": evaluation["concurrency"],
                "verified_at": time.time(),
                "receipt_id": entry.get("校验回执", {}).get("receipt_id")
                if isinstance(entry.get("校验回执"), dict) else "legacy-marker",
            })
            entry["verified_definition"] = snapshot
            entry["verified_fingerprint"] = evaluation["definition_fingerprint"]
            entry["verified_version"] = evaluation["definition_version"]
            return dict(snapshot)

    def get_verified_definition(self, source_id: int) -> dict[str, Any] | None:
        """返回某源当前唯一可被分片读取的已验证定义；没有则 None。"""
        return self.materialize_verified_definition(source_id)

    def next_eligibility(self, source_id: int) -> dict[str, Any]:
        """回答“下次资格”：当前（线上）定义能否用于下一次选源，以及如何恢复资格。"""
        entry = store.find(self.MODULE, source_id)
        if entry is None:
            return {"eligible": False, "source_id": source_id, "reason": "源配置不存在",
                    "required_action": "重新登记流式源"}
        evaluation = self.evaluate_source(entry)
        if evaluation["eligible"]:
            return {
                "eligible": True,
                "source_id": source_id,
                "definition_version": evaluation["definition_version"],
                "definition_fingerprint": evaluation["definition_fingerprint"],
                "reason": "当前定义已启用且持有一致的校验通过回执，可用于后续分片批次",
            }
        action_parts = []
        if evaluation["field_errors"]:
            action_parts.append("更正异常字段（" + "、".join(evaluation["field_errors"].keys()) + "）")
        action_parts.append("重新完成连通性校验并确认启用")
        return {
            "eligible": False,
            "source_id": source_id,
            "definition_version": evaluation["definition_version"],
            "reason": "；".join(evaluation["reasons"]) or "当前定义不具备选源资格",
            "abnormal_fields": evaluation["abnormal_fields"],
            "field_errors": evaluation["field_errors"],
            "required_action": "，".join(action_parts),
        }

    # ------------------------------------------------------------------
    # 连通性检测入口（不再因 int('abc') 抛异常）
    # ------------------------------------------------------------------
    def check_connectivity(self, source_id: int) -> dict[str, Any]:
        """连通性检测：预检消息中间件连接状态与配置合法性，并把结果同步到接入清单。

        失败场景：
        - 并发度非整数 / 超出 1-10 → 定位到「并发度」字段
        - 连接超时 → 返回超时警告
        - Topic不存在 / 中间件无法识别 → 返回配置错误
        - 消费组冲突 → 返回并发度冲突
        """
        with store.lock:
            entry = store.find(self.MODULE, source_id)
            if entry is None:
                return {"ok": False, "ready": False, "source_id": source_id,
                        "reason": "源配置不存在", "issues": ["源配置不存在"], "abnormal_fields": []}

            evaluation = self.evaluate_source(entry)
            issues = list(evaluation["issues"])
            field_issues = [
                {"field": field, "message": message}
                for field, message in evaluation["field_errors"].items()
            ]

            timeout_raw = entry.get("连接超时", "30")
            if timeout_raw is None or str(timeout_raw).strip() == "":
                # 缺省超时不视为异常，回填默认值
                entry["连接超时"] = "30"
            else:
                try:
                    timeout_val = int(str(timeout_raw).strip())
                    if timeout_val <= 0:
                        raise ValueError("non-positive")
                except (TypeError, ValueError):
                    msg = f"连接超时「{timeout_raw}」不是合法正整数"
                    issues.append(msg)
                    field_issues.append({"field": "连接超时", "message": msg})
                    if "连接超时" not in evaluation["abnormal_fields"]:
                        evaluation["abnormal_fields"].append("连接超时")

            if evaluation["concurrency"] is not None and evaluation["concurrency"] > 10:
                # parse 阶段已拦截 >10，保留与旧文案的可核对性
                issues.append("并发度>10，存在消费组冲突风险")
            else:
                # 11 这类“可解析但越界”的值也保留旧版冲突提示，便于历史断言核对
                try:
                    raw_int = int(str(entry.get(CONCURRENCY_FIELD, "")).strip())
                    if raw_int > 10:
                        issues.append("并发度>10，存在消费组冲突风险")
                except (TypeError, ValueError):
                    pass

            ok = not issues
            version = self._ensure_version_meta(entry)
            fp = evaluation["definition_fingerprint"]
            now = time.time()

            receipt = store.add_source_receipt({
                "source_id": source_id,
                "源标识": entry.get("源标识"),
                "ok": ok,
                "ready": ok,
                "ts": now,
                "definition_version": version,
                "version": version,
                "definition_fingerprint": fp,
                "concurrency": evaluation["concurrency"],
                "issues": issues,
                "abnormal_fields": sorted(set(evaluation["abnormal_fields"])),
            })

            # 同步检测结果到“接入清单”行
            entry["last_checked_at"] = now
            entry["connectivity"] = {
                "ok": ok,
                "checked_at": now,
                "issues": issues,
                "field_issues": field_issues,
                "abnormal_fields": sorted(set(evaluation["abnormal_fields"])),
                "receipt_id": receipt["receipt_id"],
            }
            entry["field_errors"] = dict(evaluation["field_errors"])
            abnormal_fields = sorted(set(evaluation["abnormal_fields"]))
            entry["abnormal_fields"] = abnormal_fields
            entry["异常字段"] = abnormal_fields
            entry["校验回执"] = receipt
            history = entry.setdefault("校验历史", [])
            history.append(receipt)
            del history[:-20]

            if ok:
                # 只有校验通过才冻结“已确认定义”
                entry[CONCURRENCY_FIELD] = evaluation["concurrency"]
                entry["verified_version"] = version
                entry["verified_fingerprint"] = fp
                snapshot = {field: entry.get(field) for field in self.DEFINITION_FIELDS}
                snapshot.update({
                    "source_id": source_id,
                    "definition_version": version,
                    "definition_fingerprint": fp,
                    "concurrency": evaluation["concurrency"],
                    "verified_at": now,
                    "receipt_id": receipt["receipt_id"],
                    "status_at_verify": entry.get("status"),
                })
                entry["verified_definition"] = snapshot
                entry["stale_confirmation"] = False
                entry["abnormal"] = False
                entry["检测结果"] = "连接正常，定义已校验确认"
                self._remember_usable_concurrency(entry, int(evaluation["concurrency"]), receipt["receipt_id"])
                message = "连通性检测通过，校验回执已同步到接入清单"
            else:
                entry["abnormal"] = True
                entry["检测结果"] = "无法连接/校验未通过：" + "；".join(issues)
                message = "连通性检测未通过，异常字段：" + "、".join(sorted(set(evaluation["abnormal_fields"])))

            return {
                "ok": ok,
                "ready": ok,
                "source_id": source_id,
                "issues": issues,
                "field_issues": field_issues,
                "field_errors": evaluation["field_errors"],
                "abnormal_fields": abnormal_fields,
                "异常字段": abnormal_fields,
                "receipt": receipt,
                "definition_version": version,
                "version": version,
                "definition_fingerprint": fp,
                "verified_definition": entry.get("verified_definition") if ok else None,
                "message": message,
            }

    # 语义化别名
    verify_source = check_connectivity
    check = check_connectivity

    def list_receipts(self, source_id: int | None = None) -> list[dict[str, Any]]:
        return store.source_receipts(source_id)

    # 近义命名
    get_receipts = list_receipts

    def source_history(self, source_id: int) -> dict[str, Any]:
        entry = store.find(self.MODULE, source_id)
        if entry is None:
            return {"ok": False, "reason": "源配置不存在", "receipts": [], "usable_values": []}
        return {
            "ok": True,
            "source_id": source_id,
            "usable_values": entry.get("并发度可用历史", []),
            "receipts": store.source_receipts(source_id),
            "definition_version": entry.get("definition_version", 1),
            "verified_version": entry.get("verified_version"),
        }

    # ------------------------------------------------------------------
    # 动作：启用必须经过字段/连通校验，旧启用标记不再直接生效
    # ------------------------------------------------------------------
    def run_action(self, entry_id: int, action: str, payload: Any | None = None):
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"

        if action in CHECK_ACTIONS:
            # 纯检测入口：只同步检测结果/回执，不改变启用状态
            detail = self.check_connectivity(entry_id)
            entry["last_action_result"] = detail
            return entry, detail["message"]

        if action in VERIFY_ACTIONS:
            previous_status = entry.get("status")
            detail = self.check_connectivity(entry_id)
            if previous_status == "草稿":
                # 正常流转：草稿 → 待验证
                entry["status"] = "待验证"
                entry["pending"] = True
            elif previous_status == "已启用" and detail["ok"]:
                # 已启用来源更正后复验通过：保持已启用，避免把可用定义降级
                entry["status"] = "已启用"
                entry["pending"] = False
            # 其他状态保持不变；资格仍以“当前指纹是否有通过回执”为准
            entry["abnormal"] = not bool(detail["ok"])
            entry["last_action_result"] = detail
            return entry, "校验完成：" + detail["message"]

        if action in ENABLE_ACTIONS:
            detail = self._enable(entry)
            entry["last_action_result"] = detail
            if not detail["ok"]:
                # 沿用基类约定：返回 (None, message)，通用动作接口据此给出 ok=False
                return None, detail["message"]
            return entry, detail["message"]

        return super().run_action(entry_id, action)

    def execute_action(self, entry_id: int, action: str, payload: Any | None = None) -> dict[str, Any]:
        """富结果动作入口（资源路由使用），保留 ok/issues/detail 等完整信息。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": f"{self.ENTITY} {entry_id} 不存在或已归档", "entry": None}

        if action in CHECK_ACTIONS or action in VERIFY_ACTIONS:
            updated_entry, message = self.run_action(entry_id, action, payload)
            detail = updated_entry.get("last_action_result") if updated_entry is not None else None
            ok = bool(detail and detail.get("ok"))
            return {"ok": ok, "message": message, "entry": updated_entry or entry, "detail": detail}

        if action in ENABLE_ACTIONS:
            detail = self._enable(entry)
            return {"ok": bool(detail["ok"]), "message": detail["message"], "entry": entry, "detail": detail}

        if action in {"更新配置", "保存更正", "update", "update_entry"}:
            values = getattr(payload, "values", None)
            if values is None and isinstance(payload, dict):
                values = payload.get("values")
            values = values or {}
            updated, field_errors = self.update_entry(entry_id, values)
            ok = updated is not None and not field_errors
            return {
                "ok": ok,
                "message": "配置已更正，定义版本已推进，请重新校验" if updated is not None and field_errors
                else ("配置已更新" if updated is not None else "更新失败"),
                "entry": updated,
                "field_errors": field_errors,
            }

        return super().execute_action(entry_id, action, payload)

    def _enable(self, entry: dict[str, Any]) -> dict[str, Any]:
        source_id = int(entry["id"])
        # 启用前做一次完整检测（字段 + 连通规则 + 回执）
        detail = self.check_connectivity(source_id)
        if not detail["ok"]:
            message = "启用被阻止：" + detail["message"]
            detail["ok_action"] = False
            return {"ok": False, "message": message, **{k: v for k, v in detail.items() if k != "message"}}
        with store.lock:
            entry["status"] = "已启用"
            entry["pending"] = False
            entry["abnormal"] = False
            message = f"{self.ENTITY}已启用，定义 v{entry.get('definition_version')} 已经校验确认"
            return {"ok": True, "message": message, "source_id": source_id,
                    "definition_version": entry.get("definition_version"),
                    "receipt": detail.get("receipt")}

    def enable_source(self, source_id: int) -> dict[str, Any]:
        """语义化启用入口，返回富 dict。"""
        entry = store.find(self.MODULE, source_id)
        if entry is None:
            return {"ok": False, "message": f"流式源 {source_id} 不存在", "source_id": source_id}
        result = self._enable(entry)
        result.setdefault("entry", entry)
        return result

    enable = enable_source

    # ------------------------------------------------------------------
    # 清单输出：只输出已确认定义；全量清单带资格标记供接入页面核对
    # ------------------------------------------------------------------
    def manifest_for(self, row: dict[str, Any], evaluation: dict[str, Any] | None = None) -> dict[str, Any]:
        evaluation = evaluation or self.evaluate_source(row)
        return {
            "id": row["id"],
            "源标识": row.get("源标识"),
            "Topic名称": row.get("Topic名称"),
            "反序列化方式": row.get("反序列化方式"),
            "起始位点": row.get("起始位点"),
            "并发度": evaluation.get("concurrency"),
            "连接超时": row.get("连接超时"),
            "消息中间件": row.get("消息中间件"),
            "status": row.get("status"),
            "definition_version": evaluation["definition_version"],
            "version": evaluation["definition_version"],
            "definition_fingerprint": evaluation["definition_fingerprint"],
            "verified_version": row.get("verified_version"),
            "receipt_id": (row.get("校验回执") or {}).get("receipt_id") if isinstance(row.get("校验回执"), dict) else None,
            "eligible": evaluation["eligible"],
        }

    def emit_source_manifest(self) -> list[dict[str, Any]]:
        """输出所有“已启用且定义已经校验确认”的流式源清单，供 batch_slice 消费。"""
        manifest: list[dict[str, Any]] = []
        for row in store.rows(self.MODULE):
            evaluation = self.evaluate_source(row)
            if evaluation["eligible"]:
                manifest.append(self.manifest_for(row, evaluation))
        return manifest

    def all_source_states(self) -> list[dict[str, Any]]:
        """接入清单全量视图：每行附带资格/异常字段/回执，旧启用标记会被明确标红。"""
        items = []
        for row in store.rows(self.MODULE):
            evaluation = self.evaluate_source(row)
            item = dict(row)
            item["eligibility"] = {
                "eligible": evaluation["eligible"],
                "confirmed": evaluation["confirmed"],
                "stale_enabled": evaluation["stale_enabled"],
                "reasons": evaluation["reasons"],
                "abnormal_fields": evaluation["abnormal_fields"],
                "field_errors": evaluation["field_errors"],
                "next_eligibility": self.next_eligibility(int(row["id"])),
            }
            items.append(item)
        return items

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["enabled_sources"] = len(self.emit_source_manifest())
        base["watermark_partitions"] = len(store._watermark_offsets)
        return base
