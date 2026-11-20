"""FastAPI 入口：链路代价分批调整离线预检（一次性 + 可续算作业）。"""

from __future__ import annotations

from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse

from .core import collect_nodes, is_connected, run_precheck
from .models import JobCreateResponse, JobView, PrecheckRequest, PrecheckResponse
from .store import JobStore

app = FastAPI(
    title="链路代价分批调整离线预检",
    version="1.1.0",
    description=(
        "提交连通路由拓扑、每条双向链路的新旧正整数代价、目标路由器与覆盖所有"
        "路由器的分批计划，逐批判断错峰切换过程中是否可能出现转发环路。"
        "等价最短路径的所有下一跳均视为可能被选中。\n\n"
        "- `POST /precheck`：一次性算完并返回。\n"
        "- `POST /jobs` + `POST /jobs/{job_id}/run`：可续算作业。原计划落盘，"
        "每批算完即保存判定（有环连同证据保存）；服务重启后凭作业号按原计划"
        "续算，自动跳过已保存批次。某批确认有环即定为首个危险批次，后续批次"
        "不再计算；全批通过才报通过。同一作业并发的续算请求只推进一轮，"
        "其余请求返回当前进度。\n"
        "- `GET /jobs/{job_id}`：查看已保存的批次与已确认的结论。"
    ),
)

store = JobStore()


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/docs")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/precheck", response_model=PrecheckResponse, summary="提交分批计划并执行环路预检")
def precheck(req: PrecheckRequest) -> PrecheckResponse:
    nodes = collect_nodes(req.links)
    if not is_connected(nodes, req.links, req.target):
        # 连通性在模型层之后二次确认（模型已保证端点一致，这里防御孤立分量）
        raise HTTPException(status_code=422, detail="拓扑必须连通，且目标路由器可达所有设备")

    result = run_precheck(nodes, req.links, req.target, req.batches)
    danger = result["dangerous"]
    return PrecheckResponse(
        status="danger" if danger else "pass",
        target=req.target,
        batches_total=len(req.batches),
        batches_checked=result["batches_checked"],
        old_next_hops=result["old_nh"],
        new_next_hops=result["new_nh"],
        danger=danger,
    )


# ---------------------------------------------------------------- 可续算作业
@app.post(
    "/jobs",
    response_model=JobCreateResponse,
    status_code=201,
    summary="创建可续算预检作业（保存原计划，返回作业号）",
)
def create_job(req: PrecheckRequest) -> JobCreateResponse:
    nodes = collect_nodes(req.links)
    if not is_connected(nodes, req.links, req.target):
        raise HTTPException(status_code=422, detail="拓扑必须连通，且目标路由器可达所有设备")
    job_id = store.create_job(req)
    view = store.get_view(job_id)
    assert view is not None
    return JobCreateResponse(
        job_id=job_id,
        target=req.target,
        batches_total=len(req.batches),
        created_at=view["created_at"],
    )


@app.post(
    "/jobs/{job_id}/run",
    response_model=JobView,
    summary="续算一轮：跳过已保存批次，算到首个危险批次或全部通过",
)
def run_job(job_id: str) -> JobView:
    view = store.run_round(job_id)
    if view is None:
        raise HTTPException(status_code=404, detail=f"作业 {job_id} 不存在")
    return JobView(**view)


@app.get(
    "/jobs/{job_id}",
    response_model=JobView,
    summary="查询作业：已保存的批次判定与已确认的结论",
)
def get_job(job_id: str) -> JobView:
    view = store.get_view(job_id)
    if view is None:
        raise HTTPException(status_code=404, detail=f"作业 {job_id} 不存在")
    return JobView(**view)


@app.get("/jobs", response_model=list[JobView], summary="列出全部作业")
def list_jobs() -> list[JobView]:
    return [JobView(**view) for view in store.list_views()]
