"""可续算作业自测：并行核查、乱序落盘、重启续算、并发互斥、失败恢复。

运行（仓库根目录）：
    python -m pytest tests/test_jobs.py -q
"""

from __future__ import annotations

import threading
import time

import pytest
from fastapi.testclient import TestClient

import app.main as main
import app.store as store_mod
from app.store import JobStore

# 三批全安全（与 examples/pass_safe.json 相同）
SAFE3 = {
    "target": "T",
    "links": [
        {"a": "T", "b": "A", "old_cost": 1, "new_cost": 1},
        {"a": "T", "b": "B", "old_cost": 2, "new_cost": 2},
        {"a": "A", "b": "B", "old_cost": 1, "new_cost": 1},
    ],
    "batches": [["A"], ["B"], ["T"]],
}

# 第二批才成环（与 tests/test_api.py 场景四相同）
DANGER_AT_2 = {
    "target": "T",
    "links": [
        {"a": "T", "b": "A", "old_cost": 20, "new_cost": 30},
        {"a": "T", "b": "B", "old_cost": 5, "new_cost": 30},
        {"a": "T", "b": "C", "old_cost": 30, "new_cost": 10},
        {"a": "A", "b": "C", "old_cost": 20, "new_cost": 2},
        {"a": "B", "b": "C", "old_cost": 2, "new_cost": 4},
        {"a": "A", "b": "B", "old_cost": 1, "new_cost": 1},
    ],
    "batches": [["A"], ["B", "C"], ["T"]],
}


@pytest.fixture()
def client(tmp_path, monkeypatch):
    """每个用例一套独立作业库。"""
    store = JobStore(str(tmp_path / "jobs.db"))
    monkeypatch.setattr(main, "store", store)
    return TestClient(main.app, raise_server_exceptions=False)


def _create(client: TestClient, payload: dict) -> str:
    r = client.post("/jobs", json=payload)
    assert r.status_code == 201, r.text
    return r.json()["job_id"]


# ---------------------------------------------------------------- 危险作业全流程
def test_danger_job_confirms_first_danger_batch_and_stops(client):
    job_id = _create(client, DANGER_AT_2)

    r = client.post(f"/jobs/{job_id}/run")
    assert r.status_code == 200, r.text
    view = r.json()
    assert view["advanced"] is True
    # 第 2 批确认有环即定为首个危险批次，第 3 批无须再算
    assert view["conclusion"]["status"] == "danger"
    assert view["conclusion"]["first_danger_batch"] == 2
    assert view["batches_total"] == 3
    assert view["batches_checked"] == 2
    saved = {b["batch_index"]: b for b in view["batches"]}
    assert set(saved) == {1, 2}
    assert saved[1]["result"] == "pass"
    assert saved[1]["danger"] is None
    assert saved[2]["result"] == "danger"
    # 环证据随批判定一起保存
    assert saved[2]["danger"]["batch_index"] == 2
    assert saved[2]["danger"]["cycle_routers"] == ["A", "C", "B", "A"]
    assert saved[2]["danger"]["switched_devices"] == ["B"]

    # 查询：已保存批次与已确认结论可见
    r = client.get(f"/jobs/{job_id}")
    assert r.status_code == 200
    got = r.json()
    assert got["conclusion"]["status"] == "danger"
    assert {b["batch_index"] for b in got["batches"]} == {1, 2}

    # 已完结的作业再续算：不推进，返回当前进度
    r = client.post(f"/jobs/{job_id}/run")
    assert r.status_code == 200
    again = r.json()
    assert again["advanced"] is False
    assert again["batches_checked"] == 2

    # 列表接口可见
    listing = client.get("/jobs").json()
    assert [j["job_id"] for j in listing] == [job_id]


# ---------------------------------------------------------------- 全批通过才报通过
def test_pass_job_requires_all_batches_checked(client):
    job_id = _create(client, SAFE3)
    view = client.post(f"/jobs/{job_id}/run").json()
    assert view["advanced"] is True
    assert view["conclusion"]["status"] == "pass"
    assert view["conclusion"]["first_danger_batch"] is None
    assert view["batches_checked"] == 3
    assert [b["result"] for b in view["batches"]] == ["pass", "pass", "pass"]
    assert all(b["danger"] is None for b in view["batches"])


# ------------------------------------------------- 较晚批次先算完 + 失败/重启后续算
def test_later_batch_saved_first_then_restart_resumes(tmp_path, monkeypatch):
    """第一批故意算得慢：同窗口的第二批先落盘；第三批模拟计算失败。

    “重启”（换新 JobStore 打开同一库文件）后续算：只补算未保存的第三批，
    已保存的第一、二批（含先落盘的第二批）被跳过，全批通过才报 pass。
    """
    db_path = str(tmp_path / "jobs.db")
    store1 = JobStore(db_path)
    monkeypatch.setattr(main, "store", store1)
    client = TestClient(main.app, raise_server_exceptions=False)
    job_id = _create(client, SAFE3)

    real_check = store_mod.check_batch
    observed = {"batch2_saved_before_batch1": False}

    def flaky_check(nodes, target, batches, old_nh, new_nh, k):
        if k == 0:
            # 第 1 批等第 2 批落盘后再返回，构造“较晚批次先算完”
            deadline = time.time() + 5
            while time.time() < deadline:
                view = store1.get_view(job_id)
                if view and any(b["batch_index"] == 2 for b in view["batches"]):
                    observed["batch2_saved_before_batch1"] = True
                    break
                time.sleep(0.01)
        if k == 2:
            raise RuntimeError("模拟第 3 批计算失败")
        return real_check(nodes, target, batches, old_nh, new_nh, k)

    monkeypatch.setattr(store_mod, "check_batch", flaky_check)
    r = client.post(f"/jobs/{job_id}/run")
    assert r.status_code == 500  # 计算失败：本轮中断
    assert observed["batch2_saved_before_batch1"], "第二批应先于第一批落盘"

    # 失败现场：第 1、2 批判定已保存（第 2 批先落盘），第 3 批未算，无结论
    view = client.get(f"/jobs/{job_id}").json()
    assert {b["batch_index"] for b in view["batches"]} == {1, 2}
    assert view["conclusion"] is None

    # ---- 模拟服务重启：新 JobStore 打开同一库文件 ----
    monkeypatch.setattr(store_mod, "check_batch", real_check)
    store2 = JobStore(db_path)
    monkeypatch.setattr(main, "store", store2)

    # 重启后凭作业号仍查得到进度
    view = client.get(f"/jobs/{job_id}").json()
    assert {b["batch_index"] for b in view["batches"]} == {1, 2}

    # 续算：只补算第 3 批，已保存批次跳过
    called: list[int] = []

    def spy_check(nodes, target, batches, old_nh, new_nh, k):
        called.append(k + 1)
        return real_check(nodes, target, batches, old_nh, new_nh, k)

    monkeypatch.setattr(store_mod, "check_batch", spy_check)
    r = client.post(f"/jobs/{job_id}/run")
    assert r.status_code == 200, r.text
    view = r.json()
    assert view["advanced"] is True
    assert called == [3], "重启续算必须跳过已保存批次"
    assert view["conclusion"]["status"] == "pass"
    assert view["batches_checked"] == 3


# ---------------------------------------------------------------- 并发续算只推进一轮
def test_concurrent_run_only_one_round_advances(client):
    job_id = _create(client, SAFE3)
    real_check = store_mod.check_batch
    entered = threading.Event()
    release = threading.Event()

    def slow_check(*args):
        entered.set()
        assert release.wait(5)
        return real_check(*args)

    # 直接改模块属性（不走 monkeypatch，便于跨线程还原）
    store_mod.check_batch = slow_check
    try:
        first: list[dict] = []

        def run_first():
            first.append(main.store.run_round(job_id))

        t = threading.Thread(target=run_first)
        t.start()
        assert entered.wait(5), "第一个续算请求应已开始推进"

        # 同一作业的第二个续算请求：不重复推进，直接返回进度
        r = client.post(f"/jobs/{job_id}/run")
        assert r.status_code == 200, r.text
        second = r.json()
        assert second["advanced"] is False

        release.set()
        t.join(timeout=10)
        assert not t.is_alive()
    finally:
        store_mod.check_batch = real_check

    assert first and first[0]["advanced"] is True
    assert first[0]["conclusion"]["status"] == "pass"
    # 最终三批各只算过一次、各保存一份
    final = client.get(f"/jobs/{job_id}").json()
    assert final["batches_checked"] == 3
    assert [b["batch_index"] for b in final["batches"]] == [1, 2, 3]


# ---------------------------------------------------------------- 与 /precheck 结果一致
@pytest.mark.parametrize("payload", [SAFE3, DANGER_AT_2])
def test_job_result_matches_one_shot_precheck(client, payload):
    expected = client.post("/precheck", json=payload).json()
    job_id = _create(client, payload)
    view = client.post(f"/jobs/{job_id}/run").json()

    assert view["conclusion"]["status"] == expected["status"]
    assert view["batches_checked"] == expected["batches_checked"]
    if expected["danger"] is None:
        assert view["conclusion"]["first_danger_batch"] is None
    else:
        assert view["conclusion"]["first_danger_batch"] == expected["danger"]["batch_index"]
        danger_record = next(b for b in view["batches"] if b["result"] == "danger")
        assert danger_record["danger"] == expected["danger"]


# ---------------------------------------------------------------- 异常路径
def test_unknown_job_returns_404(client):
    assert client.get("/jobs/nope").status_code == 404
    assert client.post("/jobs/nope/run").status_code == 404


def test_create_job_validates_payload(client):
    bad = {
        "target": "T",
        "links": [{"a": "T", "b": "A", "old_cost": 1, "new_cost": 1}],
        "batches": [["T"]],  # 未覆盖所有路由器
    }
    assert client.post("/jobs", json=bad).status_code == 422
    # 创建后未续算时：无批次、无结论
    job_id = _create(client, SAFE3)
    view = client.get(f"/jobs/{job_id}").json()
    assert view["batches_checked"] == 0
    assert view["batches"] == []
    assert view["conclusion"] is None
