"""流处理算子业务规则：状态流转、字段校验与筛选口径都收在这里。"""
from __future__ import annotations

from typing import Any

from app.services.operator_archive import archive_service
from app.store import store

MODULE = "operator"
REQUIRED_FIELDS = ["算子编号", "算子名称", "输入流"]
STATUS_ORDER = ["草稿", "运行中", "已暂停", "已下线"]
# 动作 -> 允许执行的前置状态；目标状态取 STATUS_ORDER 中前置状态的下一个，
# 不再把前置状态回写成结果，避免下线/启动后状态回退。
ACTION_RULES = {"启动算子": "草稿", "暂停算子": "运行中", "下线算子": "已暂停"}
NEGATIVE_ACTIONS = ["下线算子"]
OFFLINE_ACTION = "下线算子"
TERMINAL_STATUS = STATUS_ORDER[-1]


class OperatorService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("算子编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"算子定义 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于流处理算子可执行范围"
        current = str(entry.get("status") or "")
        if action == OFFLINE_ACTION and current == TERMINAL_STATUS:
            # 重复下线只生效一次：回执已存在则不重复写入；存档缺回执时定位失效链路并补写。
            receipt, created = archive_service.record_offline(entry)
            if created:
                return entry, f"算子执行数据存档缺少下线回执，已补写{receipt['回执编号']}，旧输出流仅保留只读历史"
            return entry, f"算子定义已下线，回执{receipt['回执编号']}已存在，本次不重复写入"
        source = ACTION_RULES[action]
        if current == TERMINAL_STATUS:
            return None, f"已下线算子不能重新启动或执行「{action}」，旧输出流仅保留只读历史引用"
        if current != source:
            return None, f"当前状态「{current}」不支持「{action}」，需处于「{source}」"
        target = STATUS_ORDER[STATUS_ORDER.index(source) + 1]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        snapshot = {"status": current, "pending": entry.get("pending"), "abnormal": entry.get("abnormal")}
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        if action != OFFLINE_ACTION:
            return entry, f"算子定义已{action}"
        # 校验下线后的算子状态，再写入算子执行数据存档；回执报错则恢复原执行状态。
        if entry["status"] != TERMINAL_STATUS:
            entry.update(snapshot)
            return None, "算子状态校验失败，已恢复原执行状态"
        try:
            receipt, _ = archive_service.record_offline(entry)
        except Exception as exc:  # noqa: BLE001 - 回执报错必须能恢复原执行状态
            entry.update(snapshot)
            return None, f"下线回执写入算子执行数据存档失败，已恢复原执行状态：{exc}"
        return entry, f"算子定义已{action}，回执{receipt['回执编号']}已写入算子执行数据存档，旧输出流仅保留只读历史"
