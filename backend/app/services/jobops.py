"""流作业运维业务规则：状态流转、字段校验与筛选口径都收在这里。

作业实例动作生效后，同步写入同一份算子执行数据存档并带入统一版本号。
"""
from __future__ import annotations

from typing import Any

from app.services.operator_archive import archive_service
from app.store import store

MODULE = "jobops"
REQUIRED_FIELDS = ["实例编号", "作业名称", "运行节点"]
STATUS_ORDER = ["待启动", "运行中", "已暂停", "已停止"]
ACTION_RULES = {"启动作业": "待启动", "暂停作业": "运行中", "停止作业": "已暂停"}
NEGATIVE_ACTIONS = []


class JobopsService:
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
            rows = [row for row in rows if keyword in str(row.get("实例编号", ""))]
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
            return None, f"作业实例 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于流作业运维可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        # 同步写入同一份算子执行数据存档，写入侧带入统一版本号。
        archive_service.sync_job_snapshot(entry)
        return entry, f"作业实例已{action}"
