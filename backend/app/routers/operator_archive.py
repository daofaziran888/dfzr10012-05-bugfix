"""算子执行数据存档接口：查看算子下线后的执行数据归档与旧输出流的只读历史。

存档只能由算子下线、流作业运维同步写入，这里只提供只读入口用于核对结果。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import PageResult
from app.services.operator_archive import archive_service

router = APIRouter(prefix="/api/operator-archive", tags=["算子执行数据存档"])


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按算子编号或回执编号检索"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """列出算子执行数据存档；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = archive_service.list_entries(keyword=keyword, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出算子执行数据存档：返回当前过滤条件下的全量数据。"""
    items, total = archive_service.list_entries(page=1, size=10000)
    return {"module": "operator_archive", "total": total, "items": items}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条存档明细；不存在时给出可读的错误说明。"""
    entry = archive_service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"算子执行数据存档 {entry_id} 不存在")
    return entry
