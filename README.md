# 链路代价分批调整离线预检

机房分批调整链路代价前的**离线环路预检服务**。提交连通路由拓扑、每条双向
链路的新旧两版正整数代价、目标路由器和覆盖全部路由器的分批计划，服务按
两版代价分别计算到目标的最短路径下一跳（等价路径的所有下一跳都可能被选
中），逐批判断错峰切换过程中是否可能出现转发环路。

## 判环模型

对每一批 `k`，切换期间各路由器的可用出边为：

| 路由器所属 | 切换期间可用出边 |
| --- | --- |
| 早于第 `k` 批 | 仅新版下一跳（前一批已全部生效） |
| 晚于第 `k` 批 | 仅旧版下一跳 |
| 第 `k` 批（本批） | **旧版 ∪ 新版**下一跳（设备可按任意顺序切换） |

在该并集转发图中：

- **存在有向环** ⇒ 存在一种错峰切换顺序与下一跳选择使流量兜圈，本批危险；
  环上仅新版可达的出边对应的本批设备，即为构造该环所需的“已切换设备”。
- **不存在有向环** ⇒ 本批切换的任意中间状态（任意已切换子集、任意等价
  下一跳选择）都不可能成环。

该等价关系由随机小拓扑暴力枚举（枚举全部切换子集 × 全部下一跳组合）
回归测试守护，见 `tests/test_api.py::test_union_lemma_against_bruteforce`。

下一跳由 Dijkstra 最短距离推导：无向边 `(u, v, w)` 上，`v` 是 `u` 的下一跳
当且仅当 `dist(u) == w + dist(v)`，等号成立的邻居全部纳入等价下一跳集合。

## 目录结构

```
app/
  main.py      FastAPI 入口（/、/health、/precheck、/jobs 系列、/docs）
  core.py      Dijkstra、下一跳集合、并集图与三色 DFS 找环
  models.py    请求/响应模型与输入校验
  store.py     可续算作业：SQLite 持久化、两批并行核查、逐批保存
tests/         pytest 自测（含同批错峰、ECMP、跨批次、暴力对照、作业续算）
examples/      可直接提交的示例请求
Dockerfile
docker-compose.yml
requirements.txt
```

## 启动（Docker Compose）

```bash
docker compose up --build
```

- 交互式文档：<http://localhost:8000/docs> （在 `/docs` 的 `POST /precheck`
  上 "Try it out" 提交计划；或用 `POST /jobs` 创建可续算作业后
  `POST /jobs/{job_id}/run` 逐轮推进）
- 健康检查：`GET http://localhost:8000/health`

无 Docker 时本地运行：

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 请求格式

```json
{
  "target": "T",
  "links": [
    {"a": "T", "b": "A", "old_cost": 10, "new_cost": 30},
    {"a": "T", "b": "B", "old_cost": 20, "new_cost": 20},
    {"a": "A", "b": "B", "old_cost": 1, "new_cost": 1}
  ],
  "batches": [["A", "B", "T"]]
}
```

约束（违反返回 HTTP 422）：

- 代价为正整数；不允许自环、重复链路；拓扑必须连通。
- 分批计划覆盖所有路由器且不重不漏；单批最多 **50** 台；不允许空批次。
- 批次顺序即切换顺序，前一批全部生效后才进入下一批。

## 响应

安全：

```json
{"status": "pass", "target": "T", "batches_total": 3, "batches_checked": 3,
 "old_next_hops": {...}, "new_next_hops": {...}, "danger": null}
```

危险（首个危险批次）：

```json
{
  "status": "danger",
  "batches_checked": 1,
  "danger": {
    "batch_index": 1,
    "switched_devices": ["A"],
    "cycle_routers": ["A", "B", "A"],
    "forwarding_cycle": [
      {"router": "A", "next_hop": "B", "via": "new"},
      {"router": "B", "next_hop": "A", "via": "old"}
    ]
  }
}
```

`via` 标出该跳取自哪一版下一跳：`old` / `new` / `both`。

## 可续算作业（/jobs）

大型割接要核查多批设备、预检服务可能中途重启时，用作业接口代替一次性
`/precheck`：

| 接口 | 说明 |
| --- | --- |
| `POST /jobs` | 创建作业：校验并**保存原计划**（目标、链路、分批），返回作业号；创建时不计算 |
| `POST /jobs/{job_id}/run` | **续算一轮**：跳过已保存批次，未保存批次两批并行核查，算到首个危险批次确认或全部通过 |
| `GET /jobs/{job_id}` | 查询进度：已保存的批次判定（含环证据）与已确认的结论 |
| `GET /jobs` | 列出全部作业 |

语义约定：

- **逐批保存**：每批算完立即把判定（`pass` / `danger`，有环连同
  `switched_devices`、`cycle_routers`、`forwarding_cycle` 证据）落盘到
  SQLite。同一窗口内较晚批次可能先算完先落盘，属正常现象。
- **结论规则**：某批有环且其前面各批都已保存为通过 ⇒ 确认它是**首个危险
  批次**，后续批次不再启动（正在并行的同对批次允许算完并保存）；**全部
  批次通过才报 `pass`**。
- **续算**：计算失败或服务重启后，凭作业号再次 `POST /jobs/{job_id}/run`
  即按原计划续算，已保存批次（不论保存先后）自动跳过；若崩溃发生在
  “批次已保存、结论未落盘”之间，续算时先从已保存批次推导结论，能定则
  直接补写，不重复计算。
- **并发互斥**：同一作业同时收到多个续算请求时，只有一个推进一轮，其余
  立即返回当前进度（响应中 `advanced=false`；实际推进的为 `true`）。
  已完结的作业再续算同样返回 `advanced=false`。
- **持久化位置**：SQLite 文件默认 `data/precheck_jobs.db`（容器内即
  `/srv/data/precheck_jobs.db`），可用环境变量 `PRECHECK_DB` 覆盖；
  docker-compose 已挂载命名卷 `precheck-data`，容器重建后作业不丢。

示例（`examples/job_danger_second_batch.json` 为第二批才成环的计划）：

```bash
JOB=$(curl -s -X POST localhost:8000/jobs -H 'Content-Type: application/json' \
      -d @examples/job_danger_second_batch.json | python3 -c "import json,sys;print(json.load(sys.stdin)['job_id'])")
curl -s -X POST localhost:8000/jobs/$JOB/run   # 推进一轮：批1 pass、批2 danger 落盘，结论 danger@2，批3 不再算
curl -s localhost:8000/jobs/$JOB               # 查看已保存批次与结论
# 服务重启后直接再 run 同一作业号即可续算，已保存批次自动跳过
```

在 `/docs` 页面演示：`POST /jobs` 创建作业拿到 `job_id` → `POST
/jobs/{job_id}/run` 推进 → `GET /jobs/{job_id}` 查看进度与结论。

## 自测

```bash
pip install -r requirements.txt pytest httpx
python -m pytest -q
```

覆盖：同批错峰成环、等价路径并集成环、安全多批次、第二批才成环、
单批 50 台上限等输入校验、断链拒绝、单路由器拓扑、结果确定性，
以及 40 组随机拓扑的并集判环 vs 全状态暴力枚举对照；
作业部分覆盖：危险批次确认即停、全批通过才报通过、较晚批次先落盘后
失败/重启续算并跳过已保存批次、并发续算只推进一轮、作业结果与
`/precheck` 一致性、未知作业 404。

## 复杂度

每批构造并集图 `O(V+E)` 后做一次 `O(V+E)` 的确定性找环，共 `B` 批；
两次 Dijkstra 为 `O((V+E) log V)`。整体约 `O((V+E) log V + B(V+E))`。
