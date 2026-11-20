"""可续算预检作业：SQLite 持久化、两批并行核查、逐批保存与并发互斥。

设计要点
--------
- 作业（原计划 + 已保存批次判定 + 结论）落盘在 SQLite，服务重启后凭
  作业号按原计划续算，已保存的批次直接跳过（不论保存先后）。
- 一轮续算把未保存的批次按顺序两两并行核查（``ThreadPoolExecutor``
  2 个worker），每批算完立即保存判定，有环连同证据保存；因此同一对
  批次里较晚批次可能先算完先落盘，重启续算时会被跳过。
- 结论确认规则：
    * 某批有环且其前面各批都已保存为通过 ⇒ 确认它是首个危险批次，
      后续批次不再启动（正在并行的同对批次允许算完并保存）；
    * 全部批次都保存为通过 ⇒ 结论 pass。
- 崩溃恢复：结论与批次判定分别落盘，若进程在两者之间退出，下次续算
  先从已保存批次重新推导结论，能定则直接补写，不再重复计算。
- 单实例服务：所有数据库访问都在进程内一把可重入锁下串行（连接对象
  不跨线程并发使用）；每个作业另配一把非阻塞互斥锁，同一作业并发的
  续算请求只由一个推进一轮，其余直接返回当前进度。
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from .core import check_batch, collect_nodes, compute_next_hops
from .models import Link, PrecheckRequest

# 默认放在工作目录下的 data/（容器内 WORKDIR=/srv，即 /srv/data）；
# 可用环境变量 PRECHECK_DB 覆盖，docker-compose 显式指定并挂载命名卷。
DEFAULT_DB_PATH = os.environ.get("PRECHECK_DB", "data/precheck_jobs.db")

PARALLEL_BATCHES = 2  # 同一作业并行核查的批次数

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    target TEXT NOT NULL,
    plan_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS batch_results (
    job_id TEXT NOT NULL REFERENCES jobs(job_id),
    batch_index INTEGER NOT NULL,
    result TEXT NOT NULL,
    danger_json TEXT,
    checked_at TEXT NOT NULL,
    PRIMARY KEY (job_id, batch_index)
);
CREATE TABLE IF NOT EXISTS conclusions (
    job_id TEXT PRIMARY KEY REFERENCES jobs(job_id),
    status TEXT NOT NULL,
    first_danger_batch INTEGER,
    concluded_at TEXT NOT NULL
);
"""


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class JobStore:
    """单实例预检作业存储与推进器。"""

    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.db_path = db_path
        if db_path != ":memory:":
            os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        # 单连接 + 进程内锁串行化所有访问（WAL 主要保证崩溃时的页完整性）
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()
        self._db_lock = threading.RLock()
        # 每个作业一把非阻塞互斥锁：同一作业任意时刻只有一个请求在推进
        self._job_locks: dict[str, threading.Lock] = {}
        self._job_locks_guard = threading.Lock()

    def _job_lock(self, job_id: str) -> threading.Lock:
        with self._job_locks_guard:
            return self._job_locks.setdefault(job_id, threading.Lock())

    # ------------------------------------------------------------ 创建
    def create_job(self, req: PrecheckRequest) -> str:
        """保存原计划，返回作业号。创建时不计算。"""
        job_id = uuid.uuid4().hex[:12]
        plan = {
            "target": req.target,
            "links": [lk.model_dump() for lk in req.links],
            "batches": req.batches,
        }
        with self._db_lock, self._conn:
            self._conn.execute(
                "INSERT INTO jobs (job_id, created_at, target, plan_json) VALUES (?,?,?,?)",
                (job_id, _utcnow(), req.target, json.dumps(plan, ensure_ascii=False)),
            )
        return job_id

    # ------------------------------------------------------------ 读取
    def _load(self, job_id: str) -> dict | None:
        """组装作业视图。调用方须持有 _db_lock。"""
        row = self._conn.execute(
            "SELECT job_id, created_at, target, plan_json FROM jobs WHERE job_id=?",
            (job_id,),
        ).fetchone()
        if row is None:
            return None
        batches = self._conn.execute(
            "SELECT batch_index, result, danger_json, checked_at FROM batch_results"
            " WHERE job_id=? ORDER BY batch_index",
            (job_id,),
        ).fetchall()
        conclusion = self._conn.execute(
            "SELECT status, first_danger_batch, concluded_at FROM conclusions WHERE job_id=?",
            (job_id,),
        ).fetchone()
        plan = json.loads(row["plan_json"])
        return {
            "job_id": row["job_id"],
            "created_at": row["created_at"],
            "target": row["target"],
            "batches_total": len(plan["batches"]),
            "batches_checked": len(batches),
            "batches": [
                {
                    "batch_index": b["batch_index"],
                    "result": b["result"],
                    "danger": json.loads(b["danger_json"]) if b["danger_json"] else None,
                    "checked_at": b["checked_at"],
                }
                for b in batches
            ],
            "conclusion": (
                {
                    "status": conclusion["status"],
                    "first_danger_batch": conclusion["first_danger_batch"],
                    "concluded_at": conclusion["concluded_at"],
                }
                if conclusion
                else None
            ),
        }

    def get_view(self, job_id: str) -> dict | None:
        """作业进度视图：已保存批次 + 已确认结论；不存在返回 None。"""
        with self._db_lock:
            return self._load(job_id)

    def list_views(self) -> list[dict]:
        with self._db_lock:
            ids = [
                r["job_id"]
                for r in self._conn.execute("SELECT job_id FROM jobs ORDER BY created_at, job_id")
            ]
            return [self._load(jid) for jid in ids]

    # ------------------------------------------------------------ 结论推导
    def _derive_conclusion(self, job_id: str) -> tuple[str, int | None] | None:
        """根据已保存批次推导是否可下结论。调用方须持有 _db_lock。

        - 存在危险批次 k 且 1..k-1 全部保存为通过 ⇒ ("danger", k)，k 取最小；
        - 全部批次保存且全为通过 ⇒ ("pass", None)；
        - 否则 None（继续算）。
        """
        rows = self._conn.execute(
            "SELECT batch_index, result FROM batch_results WHERE job_id=?", (job_id,)
        ).fetchall()
        results = {r["batch_index"]: r["result"] for r in rows}
        plan = json.loads(
            self._conn.execute("SELECT plan_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()[
                "plan_json"
            ]
        )
        total = len(plan["batches"])
        for k in sorted(bi for bi, res in results.items() if res == "danger"):
            if all(results.get(j) == "pass" for j in range(1, k)):
                return ("danger", k)
        if len(results) == total and all(res == "pass" for res in results.values()):
            return ("pass", None)
        return None

    def _save_batch(self, job_id: str, batch_index: int, danger: dict | None) -> None:
        """保存一批判定（可乱序；同批重复保存时忽略，保证幂等）。"""
        with self._db_lock, self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO batch_results"
                " (job_id, batch_index, result, danger_json, checked_at) VALUES (?,?,?,?,?)",
                (
                    job_id,
                    batch_index,
                    "danger" if danger else "pass",
                    json.dumps(danger, ensure_ascii=False) if danger else None,
                    _utcnow(),
                ),
            )

    def _conclude(self, job_id: str, status: str, first_danger_batch: int | None) -> None:
        with self._db_lock, self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO conclusions"
                " (job_id, status, first_danger_batch, concluded_at) VALUES (?,?,?,?)",
                (job_id, status, first_danger_batch, _utcnow()),
            )

    # ------------------------------------------------------------ 推进
    def run_round(self, job_id: str) -> dict | None:
        """续算一轮：未保存批次两两并行核查，算到首个危险批次确认或全部通过。

        同一作业已有请求在推进时，本请求不重复计算，直接返回当前进度
        （``advanced=False``）。作业不存在返回 None。
        """
        lock = self._job_lock(job_id)
        if not lock.acquire(blocking=False):
            view = self.get_view(job_id)
            if view is not None:
                view["advanced"] = False
            return view
        try:
            with self._db_lock:
                view = self._load(job_id)
                if view is None:
                    return None
                if view["conclusion"] is not None:
                    view["advanced"] = False
                    return view
                # 崩溃恢复：已保存批次已足以定结论（上次在保存与写结论之间退出），
                # 直接补写结论，不再计算。
                derived = self._derive_conclusion(job_id)
                if derived is not None:
                    self._conclude(job_id, *derived)
                    view = self._load(job_id)
                    view["advanced"] = True
                    return view
                plan = json.loads(
                    self._conn.execute(
                        "SELECT plan_json FROM jobs WHERE job_id=?", (job_id,)
                    ).fetchone()["plan_json"]
                )

            links = [Link(**lk) for lk in plan["links"]]
            batches: list[list[str]] = plan["batches"]
            target = plan["target"]
            nodes = collect_nodes(links)
            old_nh, new_nh = compute_next_hops(nodes, links, target)

            saved = {b["batch_index"] for b in view["batches"]}
            unsaved = [i for i in range(1, len(batches) + 1) if i not in saved]

            confirmed: tuple[str, int | None] | None = None
            # 未保存批次按顺序两批一组并行核查；每批算完立即保存并尝试下结论
            for pos in range(0, len(unsaved), PARALLEL_BATCHES):
                chunk = unsaved[pos : pos + PARALLEL_BATCHES]
                with ThreadPoolExecutor(max_workers=PARALLEL_BATCHES) as pool:
                    futures = {
                        pool.submit(
                            check_batch, nodes, target, batches, old_nh, new_nh, bi - 1
                        ): bi
                        for bi in chunk
                    }
                    for fut in as_completed(futures):
                        bi = futures[fut]
                        danger = fut.result()  # 计算失败：已保存批次保留，可凭作业号续算
                        self._save_batch(job_id, bi, danger)
                        if confirmed is None:
                            with self._db_lock:
                                derived = self._derive_conclusion(job_id)
                            if derived is not None:
                                self._conclude(job_id, *derived)
                                confirmed = derived
                if confirmed is not None:
                    break  # 首个危险批次已确认（或全批通过），后续批次无须再算

            view = self.get_view(job_id)
            assert view is not None
            view["advanced"] = True
            return view
        finally:
            lock.release()
