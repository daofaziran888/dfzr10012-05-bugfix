"""算子下线与算子执行数据存档的验收测试。

覆盖验收样例：OPER-0002 下线且 JOBO-0002 运行中时，执行快照将 out-02 标为只读历史，
OLDP-1 只记录一次。

可直接运行：.venv/bin/python tests/test_operator_offline_archive.py
也可用 pytest 运行。
"""
from __future__ import annotations

import os
import sys
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi.testclient import TestClient

from app.main import app
from app.seed import SEED_ROWS
from app.services.operator_archive import archive_service
from app.store import store


def reset_store() -> None:
    """每个用例前重置内存仓库，保证用例互不影响。"""
    store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}


client = TestClient(app)


def offline_oper_0002() -> dict:
    """走完 OPER-0002 的下线链路：运行中 -> 已暂停 -> 已下线。"""
    reset_store()
    r = client.post("/api/operator/2/actions", json={"action": "暂停算子"})
    assert r.json()["ok"], r.json()
    r = client.post("/api/operator/2/actions", json={"action": "下线算子"})
    return r.json()


def test_acceptance_offline_snapshot_marks_stream_readonly_and_single_receipt() -> None:
    """验收样例：OPER-0002 下线且 JOBO-0002 运行中时，执行快照将 out-02 标为只读历史，OLDP-1 只记录一次。"""
    body = offline_oper_0002()
    assert body["ok"], body
    assert body["entry"]["status"] == "已下线"
    assert "OLDP-1" in body["message"]

    # JOBO-0002 仍为运行中
    job = client.get("/api/jobops/2").json()
    assert job["实例编号"] == "JOBO-0002" and job["status"] == "运行中"

    # 算子执行数据存档保留旧输出流为只读历史，执行快照与异常日志一起停止
    archive = client.get("/api/operator-archive").json()
    assert archive["total"] == 1
    record = archive["items"][0]
    assert record["回执编号"] == "OLDP-1"
    assert record["算子编号"] == "OPER-0002"
    assert record["输出流"] == "out-02"
    assert record["输出流状态"] == "只读历史"
    assert record["执行快照"] == "已停止"
    assert record["异常日志"] == "已停止"
    assert record["关联作业"] == "JOBO-0002"
    assert record["作业快照"] == "运行中"
    assert record["版本号"] == 1


def test_repeat_and_concurrent_offline_only_one_receipt() -> None:
    """重复下线只生效一次；并发下线只允许一个终态，OLDP-1 只记录一次。"""
    body = offline_oper_0002()
    assert body["ok"]

    # 重复下线：不重复写入，回执仍是 OLDP-1
    again = client.post("/api/operator/2/actions", json={"action": "下线算子"}).json()
    assert again["ok"] and "OLDP-1" in again["message"] and "不重复写入" in again["message"]
    assert client.get("/api/operator-archive").json()["total"] == 1

    # 并发下线：只接受一份
    reset_store()
    client.post("/api/operator/2/actions", json={"action": "暂停算子"})
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(
            lambda _: client.post("/api/operator/2/actions", json={"action": "下线算子"}).json(),
            range(8),
        ))
    assert all(item["ok"] for item in results)
    archive = client.get("/api/operator-archive").json()
    assert archive["total"] == 1
    assert archive["items"][0]["回执编号"] == "OLDP-1"
    assert client.get("/api/operator/2").json()["status"] == "已下线"


def test_offline_operator_cannot_restart_and_no_state_rollback() -> None:
    """已下线算子不能重新启动；状态机不再回退。"""
    offline_oper_0002()
    r = client.post("/api/operator/2/actions", json={"action": "启动算子"}).json()
    assert not r["ok"] and "已下线算子不能重新启动" in r["message"]
    assert client.get("/api/operator/2").json()["status"] == "已下线"

    # 暂停/启动都落到正确的目标状态，不再回退
    reset_store()
    r = client.post("/api/operator/2/actions", json={"action": "暂停算子"}).json()
    assert r["ok"] and r["entry"]["status"] == "已暂停"
    reset_store()
    r = client.post("/api/operator/1/actions", json={"action": "启动算子"}).json()
    assert r["ok"] and r["entry"]["status"] == "运行中"
    # 非法迁移被拦下并说明原因
    r = client.post("/api/operator/1/actions", json={"action": "下线算子"}).json()
    assert not r["ok"] and "需处于" in r["message"]


def test_detail_and_list_share_fixed_status() -> None:
    """详情页和列表入口由共同原因修复：两处状态一致。"""
    offline_oper_0002()
    detail = client.get("/api/operator/2").json()
    items = client.get("/api/operator", params={"keyword": "OPER-0002"}).json()["items"]
    assert detail["status"] == "已下线" == items[0]["status"]


def test_jobops_sync_write_carries_unified_version() -> None:
    """jobops 同步写入同一份存档并带入版本号；旧输出流保持只读历史。"""
    offline_oper_0002()
    r = client.post("/api/jobops/2/actions", json={"action": "暂停作业"})
    assert r.json()["ok"]
    record = client.get("/api/operator-archive").json()["items"][0]
    assert record["写入方"] == "jobops"
    assert record["版本号"] == 2
    assert record["作业快照"] == "运行中"  # jobops 动作按存量映射落到目标状态
    assert record["输出流状态"] == "只读历史"
    assert record["执行快照"] == "已停止" and record["异常日志"] == "已停止"


def test_log_reads_use_unified_version_and_old_stream_spawns_no_new_exception() -> None:
    """日志读取只关联统一版本；旧输出流不能继续生成新异常。"""
    offline_oper_0002()
    version = archive_service.unified_version()
    items = client.get("/api/log").json()["items"]
    assert items and all(item["存档版本"] == version for item in items)
    assert client.get("/api/log/1").json()["存档版本"] == version

    r = client.post("/api/log", json={"values": {
        "记录编号": "LOG-1001", "流作业名": "流作业运维样例2", "日志级别": "ERROR", "输出流": "out-02",
    }}).json()
    assert not r["ok"] and "只读历史" in r["message"] and "不能继续生成新异常" in r["message"]

    r = client.post("/api/log", json={"values": {
        "记录编号": "LOG-1002", "流作业名": "流作业运维样例1", "日志级别": "INFO", "输出流": "out-01",
    }}).json()
    assert r["ok"]


def test_archive_write_failure_restores_original_state(monkeypatch=None) -> None:
    """下线回执报错时定位存档失效链路并恢复原执行状态。"""
    reset_store()
    client.post("/api/operator/2/actions", json={"action": "暂停算子"})

    original = archive_service.record_offline
    archive_service.record_offline = lambda entry: (_ for _ in ()).throw(RuntimeError("存档不可用"))
    try:
        r = client.post("/api/operator/2/actions", json={"action": "下线算子"}).json()
    finally:
        archive_service.record_offline = original
    assert not r["ok"] and "已恢复原执行状态" in r["message"]
    assert client.get("/api/operator/2").json()["status"] == "已暂停"
    assert client.get("/api/operator-archive").json()["total"] == 0


def test_frontend_action_payload_shape_works() -> None:
    """前端顶层 action 写法和 values.action 写法都能进入调用链。"""
    reset_store()
    r = client.post("/api/operator/2/actions", json={"action": "暂停算子"}).json()
    assert r["ok"] and r["entry"]["status"] == "已暂停"
    reset_store()
    r = client.post("/api/operator/2/actions", json={"values": {"action": "暂停算子"}}).json()
    assert r["ok"] and r["entry"]["status"] == "已暂停"


def main() -> None:
    tests = [value for name, value in sorted(globals().items()) if name.startswith("test_")]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")
    print(f"\n{len(tests)} 项验收全部通过")


if __name__ == "__main__":
    main()
