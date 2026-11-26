"""单节点 CPU 准入演练服务.

场景: 批处理 Pod 启动时要解压模型, 除了常驻额度还需要一份"初始化额外额度",
初始化完成后归还额外额度供其他 Pod 申领.

设计要点:
- SQLite 单节点记账, CPU 以毫核(millicore)计.
- 节点余量全部记在 node 单行计数器上, 申领/归还都是对该行的原子条件 UPDATE,
  不遍历其他 Pod(O(1)), 且 `allocated + ? <= total` 条件保证并发申领不会超卖.
- 每个初始化轮次分配独立 round_token, 完成通知必须携带当前轮 token,
  旧通知无法结束新一轮.
- 每次请求的"节点计数器 + Pod 记录"改动放在一个事务边界内: 正常结束才提交,
  业务拒绝或 SQLite 写入/提交失败一律回滚并释放写锁, 失败后一切读写以已提交
  数据为准(见 _txn).
"""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field

DB_PATH = os.environ.get("DB_PATH", "./admission.db")
DEFAULT_NODE_TOTAL = int(os.environ.get("NODE_TOTAL_MILLICORES", "4000"))

log = logging.getLogger("admission")

PHASE_INITIALIZING = "INITIALIZING"  # 持有常驻 + 额外额度, 正在解压/初始化
PHASE_RUNNING = "RUNNING"            # 初始化已完成, 只持有常驻额度

# 单连接 + 锁: 所有写事务串行化; 条件 UPDATE 兜底保证不超卖.
_lock = threading.Lock()


def _connect() -> sqlite3.Connection:
    parent = os.path.dirname(DB_PATH)
    if parent:
        os.makedirs(parent, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


db = _connect()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_db() -> None:
    with _lock:
        try:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS node (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    total_millicores INTEGER NOT NULL,
                    allocated_millicores INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS pods (
                    pod_id TEXT PRIMARY KEY,
                    resident_millicores INTEGER NOT NULL,
                    extra_millicores INTEGER NOT NULL,
                    phase TEXT NOT NULL,
                    extra_held INTEGER NOT NULL,   -- 1=持有额外额度, 0=已归还
                    round_no INTEGER NOT NULL,     -- 当前初始化轮次编号
                    round_token TEXT NOT NULL,     -- 当前轮次的独立令牌
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            if db.execute("SELECT id FROM node WHERE id = 1").fetchone() is None:
                db.execute(
                    "INSERT INTO node (id, total_millicores, allocated_millicores) VALUES (1, ?, 0)",
                    (DEFAULT_NODE_TOTAL,),
                )
            db.commit()
        except sqlite3.Error:
            db.rollback()
            raise


init_db()

app = FastAPI(
    title="单节点 CPU 准入演练",
    version="1.0.0",
    description="批处理 Pod 初始化额外额度申领/归还演练, CPU 按毫核计, SQLite 记账.",
)


# ---------- 请求模型 ----------

class AdmitRequest(BaseModel):
    pod_id: str = Field(min_length=1, max_length=128)
    resident_millicores: int = Field(gt=0, description="常驻额度(毫核)")
    init_extra_millicores: int = Field(gt=0, description="初始化额外额度(毫核)")


class CompleteRequest(BaseModel):
    round_token: str = Field(min_length=1, description="申领时返回的轮次令牌")


class NodeConfig(BaseModel):
    total_millicores: int = Field(gt=0)


# ---------- 内部 helpers(调用方须已持有 _lock) ----------

def _rollback() -> None:
    """丢弃本次未提交改动并释放 SQLite 写锁(锁没开时是 no-op)."""
    try:
        db.rollback()
    except sqlite3.Error:
        log.exception("SQLite rollback failed")


@contextmanager
def _txn():
    """写事务边界: 块内正常结束才提交; 拒绝或失败一律回滚.

    - 业务拒绝(如余量不足)抛 HTTPException 时, 回滚以释放本次写锁, 再原样抛出;
    - SQLite 写入/提交失败时, 回滚整段改动并转 503, Pod 记录与节点占用
      保持已提交的样子(不会出现"节点已归还、Pod 仍持有"的中间态);
    - 任何其它异常同样回滚, 绝不把写锁或半截事务留给下一次请求.
    """
    try:
        if db.in_transaction:
            # 上次请求若遗留了未提交事务(正常路径不应出现), 先丢弃,
            # 保证后续一切读写都以已提交的数据为准.
            log.warning("discarding leftover uncommitted transaction at request start")
            _rollback()
        yield
        if db.in_transaction:
            db.commit()
    except HTTPException:
        _rollback()
        raise
    except sqlite3.Error:
        log.exception("SQLite write/commit failed, rolled back")
        _rollback()
        raise HTTPException(
            503,
            detail={
                "error": "storage_write_failed",
                "message": "数据库写入失败, 本次改动已整体回滚, 可用已提交的数据重试",
            },
        )
    except BaseException:
        _rollback()
        raise


def _node_state() -> dict:
    row = db.execute("SELECT total_millicores, allocated_millicores FROM node WHERE id = 1").fetchone()
    return {
        "total_millicores": row["total_millicores"],
        "allocated_millicores": row["allocated_millicores"],
        "remaining_millicores": row["total_millicores"] - row["allocated_millicores"],
    }


def _get_pod(pod_id: str) -> sqlite3.Row | None:
    return db.execute("SELECT * FROM pods WHERE pod_id = ?", (pod_id,)).fetchone()


def _pod_view(pod: sqlite3.Row) -> dict:
    return {
        "pod_id": pod["pod_id"],
        "phase": pod["phase"],
        "resident_millicores": pod["resident_millicores"],
        "extra_millicores": pod["extra_millicores"],
        "extra_held": bool(pod["extra_held"]),
        "round_no": pod["round_no"],
        "created_at": pod["created_at"],
        "updated_at": pod["updated_at"],
    }


# ---------- 节点 ----------

@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/docs")


@app.get("/healthz", include_in_schema=False)
def healthz():
    return {"ok": True}


@app.get("/node", summary="节点总额/已分配/余量")
def get_node():
    with _lock, _txn():
        return _node_state()


@app.put("/node", summary="配置节点总额(仅在没有占用时允许)")
def set_node(cfg: NodeConfig):
    with _lock, _txn():
        state = _node_state()
        if state["allocated_millicores"] > 0:
            raise HTTPException(
                409,
                detail={
                    "error": "node_in_use",
                    "message": "节点上还有占用, 不能改总额; 先删除 Pod 或调用 /admin/reset",
                    **state,
                },
            )
        db.execute("UPDATE node SET total_millicores = ? WHERE id = 1", (cfg.total_millicores,))
        return _node_state()


@app.post("/admin/reset", summary="清空所有 Pod 并归零占用(演练重跑用)")
def reset():
    with _lock, _txn():
        db.execute("DELETE FROM pods")
        db.execute("UPDATE node SET allocated_millicores = 0 WHERE id = 1")
        return _node_state()


# ---------- Pod 准入 / 申领 / 归还 ----------

@app.post("/pods", status_code=201, summary="首次启动准入: 常驻+额外两份都拿到才接单")
def admit_pod(req: AdmitRequest):
    need = req.resident_millicores + req.init_extra_millicores
    now = _now()
    token = uuid.uuid4().hex
    with _lock, _txn():
        if _get_pod(req.pod_id) is not None:
            raise HTTPException(409, detail={"error": "pod_exists", "pod_id": req.pod_id})
        # 原子条件扣减: 余量不足则一行都不会更新, 并发下也不会超卖
        cur = db.execute(
            "UPDATE node SET allocated_millicores = allocated_millicores + ? "
            "WHERE id = 1 AND allocated_millicores + ? <= total_millicores",
            (need, need),
        )
        if cur.rowcount == 0:
            # 额度不足: 抛 HTTPException 前由 _txn 回滚, 释放本次写锁,
            # 失败的条件 UPDATE 不会把扣减带进下一次准入.
            raise HTTPException(
                409,
                detail={"error": "insufficient_node_cpu", "required_millicores": need, **_node_state()},
            )
        db.execute(
            "INSERT INTO pods (pod_id, resident_millicores, extra_millicores, phase, "
            "extra_held, round_no, round_token, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, 1, 1, ?, ?, ?)",
            (req.pod_id, req.resident_millicores, req.init_extra_millicores,
             PHASE_INITIALIZING, token, now, now),
        )
        pod = _get_pod(req.pod_id)
        return {**_pod_view(pod), "round_token": token, "node": _node_state()}


@app.post("/pods/{pod_id}/init/complete", summary="初始化完成通知: 当前轮首次完成才归还额外额度")
def complete_init(pod_id: str, req: CompleteRequest):
    with _lock, _txn():
        pod = _get_pod(pod_id)
        if pod is None:
            raise HTTPException(404, detail={"error": "pod_not_found", "pod_id": pod_id})
        if pod["round_token"] != req.round_token:
            # 旧通知/伪造令牌: 不能结束当前轮
            raise HTTPException(
                409,
                detail={
                    "error": "stale_round_token",
                    "message": "令牌与当前轮次不匹配(可能是旧通知), 本轮不受影响",
                    "current_round_no": pod["round_no"],
                },
            )
        if not pod["extra_held"]:
            # 本轮已经归还过: 幂等 no-op, 不会重复归还
            return {
                "released": False,
                "already_completed": True,
                **_pod_view(pod),
                "node": _node_state(),
            }
        # 归还额度与 Pod 记账放在同一事务: 提交失败则一起回滚,
        # 不会出现"节点余量已归还、Pod 仍标记持有额外额度"的错账.
        db.execute(
            "UPDATE node SET allocated_millicores = allocated_millicores - ? WHERE id = 1",
            (pod["extra_millicores"],),
        )
        db.execute(
            "UPDATE pods SET extra_held = 0, phase = ?, updated_at = ? WHERE pod_id = ?",
            (PHASE_RUNNING, _now(), pod_id),
        )
        return {"released": True, **_pod_view(_get_pod(pod_id)), "node": _node_state()}


@app.post("/pods/{pod_id}/reinit", summary="运行中重做初始化: 只重新申领额外额度, 领不到就继续运行")
def reinit(pod_id: str):
    with _lock, _txn():
        pod = _get_pod(pod_id)
        if pod is None:
            raise HTTPException(404, detail={"error": "pod_not_found", "pod_id": pod_id})
        if pod["extra_held"]:
            # 已在初始化中: 幂等返回当前轮令牌, 不新开一轮
            return {
                "acquired": True,
                "already_initializing": True,
                **_pod_view(pod),
                "round_token": pod["round_token"],
                "node": _node_state(),
            }
        extra = pod["extra_millicores"]
        cur = db.execute(
            "UPDATE node SET allocated_millicores = allocated_millicores + ? "
            "WHERE id = 1 AND allocated_millicores + ? <= total_millicores",
            (extra, extra),
        )
        if cur.rowcount == 0:
            # 领不到额外额度: 常驻额度与 Pod 状态都不动. 失败的条件 UPDATE
            # 未产生任何生效扣减, 显式回滚以在返回前释放本次写锁, Pod 继续 RUNNING.
            _rollback()
            return {"acquired": False, **_pod_view(pod), "node": _node_state()}
        new_round = pod["round_no"] + 1
        token = uuid.uuid4().hex
        db.execute(
            "UPDATE pods SET extra_held = 1, phase = ?, round_no = ?, round_token = ?, "
            "updated_at = ? WHERE pod_id = ?",
            (PHASE_INITIALIZING, new_round, token, _now(), pod_id),
        )
        return {
            "acquired": True,
            "already_initializing": False,
            **_pod_view(_get_pod(pod_id)),
            "round_token": token,
            "node": _node_state(),
        }


# ---------- 查询 / 清理 ----------

@app.get("/pods/{pod_id}", summary="查询 Pod 阶段和节点余量")
def get_pod(pod_id: str):
    with _lock, _txn():
        pod = _get_pod(pod_id)
        if pod is None:
            raise HTTPException(404, detail={"error": "pod_not_found", "pod_id": pod_id})
        return {**_pod_view(pod), "node": _node_state()}


@app.get("/pods", summary="列出所有 Pod(含节点余量)")
def list_pods():
    with _lock, _txn():
        rows = db.execute("SELECT * FROM pods ORDER BY created_at, pod_id").fetchall()
        return {"pods": [_pod_view(r) for r in rows], "node": _node_state()}


@app.delete("/pods/{pod_id}", summary="删除 Pod 并释放其占用的全部额度")
def delete_pod(pod_id: str):
    with _lock, _txn():
        pod = _get_pod(pod_id)
        if pod is None:
            raise HTTPException(404, detail={"error": "pod_not_found", "pod_id": pod_id})
        free = pod["resident_millicores"] + (pod["extra_millicores"] if pod["extra_held"] else 0)
        # 释放节点额度与删除 Pod 记录在同一事务: 提交失败则两者一起回滚,
        # 不会出现"额度已释放但 Pod 还在"或反之.
        db.execute(
            "UPDATE node SET allocated_millicores = allocated_millicores - ? WHERE id = 1",
            (free,),
        )
        db.execute("DELETE FROM pods WHERE pod_id = ?", (pod_id,))
        return {"deleted": pod_id, "freed_millicores": free, "node": _node_state()}
