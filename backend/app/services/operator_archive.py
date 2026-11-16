"""算子执行数据存档：算子下线后的执行数据归档、统一版本与只读历史输出流。

写入方有两个，都收敛到这一个服务里：
- 流处理算子（operator）：下线时写入下线回执（OLDP-n），旧输出流标记为只读历史，
  执行快照与异常日志一起停止；
- 流作业运维（jobops）：同步写入同一份存档，写入侧带入统一版本号。

流处理日志中心读取侧只关联统一版本，避免新旧两份存档结果互相冲突。
"""
from __future__ import annotations

import threading
from typing import Any

from app.store import store

MODULE = "operator_archive"
RECEIPT_PREFIX = "OLDP"
RECEIPT_TYPE = "下线回执"
READONLY_HISTORY = "只读历史"
SNAPSHOT_STOPPED = "已停止"
LOG_STOPPED = "已停止"
ARCHIVED_STATUS = "已归档"


def _suffix(code: str) -> str:
    """取编号尾段：OPER-0002 -> 0002，用于沿用 OPER-xxxx 与 JOBO-xxxx 的存量映射。"""
    return code.rsplit("-", 1)[-1] if "-" in code else code


class OperatorArchiveService:
    """算子执行数据存档的写入与读取都走这里，保证并发下线只有一个终态。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()

    # ---- 读取 ----
    def rows(self) -> list[dict[str, Any]]:
        return store.rows(MODULE)

    def unified_version(self) -> int:
        """统一版本号：两个写入方共用一份存档，取存档中最大的版本号。"""
        return max((int(row.get("版本号", 0)) for row in self.rows()), default=0)

    def list_entries(
        self,
        *,
        keyword: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = self.rows()
        if keyword:
            rows = [
                row
                for row in rows
                if keyword in str(row.get("算子编号", "")) or keyword in str(row.get("回执编号", ""))
            ]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def find_by_operator(self, operator_code: str) -> dict[str, Any] | None:
        for row in self.rows():
            if row.get("算子编号") == operator_code:
                return row
        return None

    def is_stream_readonly(self, stream: str) -> bool:
        """旧输出流只保留历史引用：已归档为只读历史的输出流不能再生成新异常。"""
        return any(
            row.get("输出流") == stream and row.get("输出流状态") == READONLY_HISTORY
            for row in self.rows()
        )

    # ---- 写入 ----
    def record_offline(self, entry: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        """写入算子下线回执：同一算子只写一份，重复/并发下线只接受一份。

        返回 (存档记录, 是否本次新建)。已存在回执时直接返回原记录，保证 OLDP-n 只记录一次。
        """
        operator_code = str(entry.get("算子编号") or "")
        with self._lock:
            existing = self.find_by_operator(operator_code)
            if existing is not None:
                return existing, False
            job = self._linked_job(operator_code)
            rows = self.rows()
            record = {
                "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
                "回执编号": f"{RECEIPT_PREFIX}-{len(rows) + 1}",
                "记录类型": RECEIPT_TYPE,
                "算子编号": operator_code,
                "算子名称": entry.get("算子名称"),
                "处理逻辑": entry.get("处理逻辑"),
                "输出流": entry.get("输出流"),
                "输出流状态": READONLY_HISTORY,
                "执行快照": SNAPSHOT_STOPPED,
                "异常日志": LOG_STOPPED,
                "关联作业": job.get("实例编号") if job else None,
                "作业快照": job.get("status") if job else None,
                "版本号": self.unified_version() + 1,
                "写入方": "operator",
                "status": ARCHIVED_STATUS,
                "pending": False,
                "abnormal": False,
            }
            rows.append(record)
            return record, True

    def sync_job_snapshot(self, job_entry: dict[str, Any]) -> dict[str, Any] | None:
        """jobops 同步写入同一份存档：刷新关联作业快照，写入侧带入统一版本号。

        算子尚未下线（无存档）时不落盘；已下线算子的旧输出流保持只读历史，不被作业快照复活。
        """
        job_code = str(job_entry.get("实例编号") or "")
        operator_code = f"OPER-{_suffix(job_code)}"
        with self._lock:
            record = self.find_by_operator(operator_code)
            if record is None:
                return None
            record["关联作业"] = job_code
            record["作业快照"] = job_entry.get("status")
            record["版本号"] = self.unified_version() + 1
            record["写入方"] = "jobops"
            return record

    def _linked_job(self, operator_code: str) -> dict[str, Any] | None:
        """沿用存量编号映射：OPER-0002 关联 JOBO-0002。"""
        job_code = f"JOBO-{_suffix(operator_code)}"
        for row in store.rows("jobops"):
            if row.get("实例编号") == job_code:
                return row
        return None


archive_service = OperatorArchiveService()
