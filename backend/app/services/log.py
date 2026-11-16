"""流处理日志中心业务规则：状态流转、字段校验与筛选口径都收在这里。

日志读取只关联统一版本的算子执行数据存档；已归档为只读历史的旧输出流
不能再生成新异常，避免新旧两份存档结果互相冲突。
"""
from __future__ import annotations

from typing import Any

from app.services.operator_archive import archive_service
from app.store import store

MODULE = "log"
REQUIRED_FIELDS = ["记录编号", "流作业名", "日志级别"]
STATUS_ORDER = ["已记录", "已分析", "已处理", "已归档"]
ACTION_RULES = {"分析日志": "已记录", "标记处理": "已分析", "归档日志": "已处理"}
NEGATIVE_ACTIONS = []


class LogService:
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
            rows = [row for row in rows if keyword in str(row.get("记录编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        version = archive_service.unified_version()
        # 读取侧只关联一个统一版本的算子执行数据存档
        return [dict(row, 存档版本=version) for row in rows[start:start + size]], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        row = store.find(MODULE, entry_id)
        if row is None:
            return None
        return dict(row, 存档版本=archive_service.unified_version())

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        stream = str(values.get("输出流") or "").strip()
        if stream and archive_service.is_stream_readonly(stream):
            return None, [f"输出流{stream}已归档为只读历史，不能继续生成新异常"]
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        if stream:
            entry["输出流"] = stream
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return dict(entry, 存档版本=archive_service.unified_version()), []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"日志记录 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于流处理日志中心可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"日志记录已{action}"
