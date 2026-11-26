#!/usr/bin/env python3
"""故障注入测试: SQLite 写入/提交失败与额度不足时的 Pod 记账一致性.

不启动网络服务, 在进程内导入 app.main, 并用 FaultyDB 包住模块级 SQLite 连接,
在指定语句上模拟 I/O 错误/提交失败, 然后调用接口函数(状态码语义与 HTTP 一致).
只用标准库; 退出码非 0 表示有失败项.

前置: 依赖已安装(见 requirements.txt), 或通过 PYTHONPATH 指向已安装目录.
"""

import os
import sqlite3
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

_tmp = tempfile.TemporaryDirectory()
os.environ["DB_PATH"] = os.path.join(_tmp.name, "admission.db")
os.environ["NODE_TOTAL_MILLICORES"] = "4000"

import app.main as m  # noqa: E402  必须在设置 DB_PATH 之后导入

_failures = []
_checks = 0


def check(name, cond, detail=""):
    global _checks
    _checks += 1
    mark = "PASS" if cond else "FAIL"
    print(f"[{mark}] {name}" + (f"  | {detail}" if detail else ""))
    if not cond:
        _failures.append(name)


class FaultyDB:
    """代理真实连接; 可在指定 SQL 或 commit 时注入一次 sqlite3.OperationalError."""

    def __init__(self, real):
        self._real = real
        self._pattern = None
        self._when = None  # "execute" | "commit"

    def fail_on(self, pattern, when="execute"):
        self._pattern = pattern
        self._when = when

    def _disarm(self):
        self._pattern = None
        self._when = None

    @property
    def in_transaction(self):
        return self._real.in_transaction

    def execute(self, sql, params=()):
        if self._when == "execute" and self._pattern in sql:
            self._disarm()
            raise sqlite3.OperationalError("simulated disk I/O error")
        return self._real.execute(sql, params)

    def executescript(self, sql):
        return self._real.executescript(sql)

    def commit(self):
        if self._when == "commit":
            self._disarm()
            raise sqlite3.OperationalError("simulated commit failure")
        return self._real.commit()

    def rollback(self):
        return self._real.rollback()

    def __getattr__(self, name):
        return getattr(self._real, name)


real_db = m.db
fdb = FaultyDB(real_db)
m.db = fdb


def committed_state():
    """另开一条连接读取, 只看到已提交的数据."""
    conn = sqlite3.connect(os.environ["DB_PATH"])
    conn.row_factory = sqlite3.Row
    try:
        node = conn.execute(
            "SELECT total_millicores AS t, allocated_millicores AS a FROM node WHERE id = 1"
        ).fetchone()
        pods = conn.execute("SELECT * FROM pods ORDER BY pod_id").fetchall()
        return {
            "remaining": node["t"] - node["a"],
            "allocated": node["a"],
            "pods": {p["pod_id"]: dict(p) for p in pods},
        }
    finally:
        conn.close()


def expect_http_error(fn, status, error):
    try:
        fn()
    except m.HTTPException as e:
        detail = e.detail if isinstance(e.detail, dict) else {}
        return e.status_code == status and detail.get("error") == error, f"{e.status_code} {detail}"
    except Exception as e:  # noqa: BLE001
        return False, f"unexpected {type(e).__name__}: {e}"
    return False, "no exception"


def _token_of(pod_id):
    """从已提交数据里取当前轮令牌(查询走代理连接, 与接口读同一份数据)."""
    return m.db.execute(
        "SELECT round_token FROM pods WHERE pod_id = ?", (pod_id,)
    ).fetchone()["round_token"]


def reset(total=4000):
    m.reset()
    m.set_node(m.NodeConfig(total_millicores=total))


def snap_ok(label, expected_remaining, expected_pod=None, expected_phase=None, expected_extra=None):
    st = committed_state()
    ok = st["remaining"] == expected_remaining
    if expected_pod is not None:
        pod = st["pods"].get(expected_pod)
        ok = ok and pod is not None and pod["phase"] == expected_phase \
            and bool(pod["extra_held"]) == expected_extra
    check(label, ok, st)
    check(f"{label} -> 连接上无遗留未提交事务", not real_db.in_transaction,
          f"in_transaction={real_db.in_transaction}")


print("== 故障注入测试: SQLite 写入/提交失败时 Pod 记账保持一致 ==\n")

# ---------------------------------------------------------------- 场景 A
print("-- 场景A: 完成通知写 Pod 状态失败后, 下一次准入不得用掉未真正归还的 CPU --")
reset()
m.admit_pod(m.AdmitRequest(pod_id="pod-a", resident_millicores=1000, init_extra_millicores=500))
token_a = _token_of("pod-a")
snap_ok("先确认 pod-a 已持有额外额度, 余量 2500m", 2500, "pod-a", "INITIALIZING", True)

fdb.fail_on("UPDATE pods SET extra_held = 0")  # 节点已减 500m, 接着写 Pod 失败
ok, detail = expect_http_error(
    lambda: m.complete_init("pod-a", m.CompleteRequest(round_token=token_a)),
    503, "storage_write_failed",
)
check("完成通知返回 503 storage_write_failed(而不是 500)", ok, detail)
snap_ok("失败后节点余量仍 2500m, Pod 仍 INITIALIZING 持有额外额度",
        2500, "pod-a", "INITIALIZING", True)
check("失败后查询接口读到已提交数据", m.get_node()["remaining_millicores"] == 2500)

# 关键回归: 修复前这里会用上"名义上归还"的 500m, 导致余量只剩 500m.
st = m.admit_pod(m.AdmitRequest(pod_id="pod-x", resident_millicores=1000, init_extra_millicores=500))
check("失败后新 Pod 申领 1500m 成功且余量精确为 1000m",
      st.get("node", {}).get("remaining_millicores") == 1000, st.get("node"))
check("新 Pod 的占用基于已提交数据(allocated=3000m)",
      committed_state()["allocated"] == 3000, committed_state()["allocated"])

# 用同一令牌重试完成通知: 应以已提交数据为准, 真正归还一次.
res = m.complete_init("pod-a", m.CompleteRequest(round_token=token_a))
check("重试完成通知成功归还 500m", res["released"] and res["node"]["remaining_millicores"] == 1500,
      res.get("node"))
check("重试后连接无遗留事务", not real_db.in_transaction)

# ---------------------------------------------------------------- 场景 B
print("\n-- 场景B: 完成通知提交失败(两条写都发出后才失败), 整体回滚 --")
reset()
m.admit_pod(m.AdmitRequest(pod_id="pod-b", resident_millicores=1000, init_extra_millicores=500))
token_b = _token_of("pod-b")
fdb.fail_on("", when="commit")
ok, detail = expect_http_error(
    lambda: m.complete_init("pod-b", m.CompleteRequest(round_token=token_b)),
    503, "storage_write_failed",
)
check("提交失败返回 503", ok, detail)
snap_ok("提交失败后节点仍 2500m, Pod 仍持有额外额度", 2500, "pod-b", "INITIALIZING", True)
res = m.complete_init("pod-b", m.CompleteRequest(round_token=token_b))
check("重试完成通知归还成功, 余量 3000m", res["released"] and res["node"]["remaining_millicores"] == 3000,
      res.get("node"))
snap_ok("重试后 Pod 进入 RUNNING 且不再持有额外额度", 3000, "pod-b", "RUNNING", False)

# ---------------------------------------------------------------- 场景 C
print("\n-- 场景C: 重做初始化提交失败, Pod 与节点占用都保持原状, 可重试 --")
reset()
m.admit_pod(m.AdmitRequest(pod_id="pod-c", resident_millicores=1000, init_extra_millicores=500))
token_c = _token_of("pod-c")
m.complete_init("pod-c", m.CompleteRequest(round_token=token_c))
snap_ok("准备: pod-c RUNNING, 余量 3000m", 3000, "pod-c", "RUNNING", False)

fdb.fail_on("", when="commit")
ok, detail = expect_http_error(lambda: m.reinit("pod-c"), 503, "storage_write_failed")
check("重做初始化提交失败返回 503", ok, detail)
snap_ok("提交失败后 Pod 仍 RUNNING(第 1 轮)且余量 3000m", 3000, "pod-c", "RUNNING", False)
res = m.reinit("pod-c")
check("重试重做初始化成功(第 2 轮), 余量 2500m",
      res["acquired"] and res["round_no"] == 2 and res["node"]["remaining_millicores"] == 2500,
      res)
res = m.complete_init("pod-c", m.CompleteRequest(round_token=res["round_token"]))
check("第 2 轮完成归还, 余量 3000m", res["released"] and res["node"]["remaining_millicores"] == 3000)

print("\n-- 场景C2: 重做初始化写 Pod 状态失败, 刚加上的额度必须回滚 --")
reset()
m.admit_pod(m.AdmitRequest(pod_id="pod-c2", resident_millicores=1000, init_extra_millicores=500))
token_c2 = _token_of("pod-c2")
m.complete_init("pod-c2", m.CompleteRequest(round_token=token_c2))
fdb.fail_on("UPDATE pods SET extra_held = 1")
ok, detail = expect_http_error(lambda: m.reinit("pod-c2"), 503, "storage_write_failed")
check("写 Pod 失败返回 503", ok, detail)
snap_ok("节点已加上的 500m 随事务回滚, 余量仍 3000m, Pod 仍 RUNNING", 3000, "pod-c2", "RUNNING", False)
res = m.reinit("pod-c2")
check("重试成功, 余量 2500m", res["acquired"] and res["node"]["remaining_millicores"] == 2500, res)

# ---------------------------------------------------------------- 场景 D
print("\n-- 场景D: 额度不足的申领在返回前释放写锁 --")
reset()
m.admit_pod(m.AdmitRequest(pod_id="pod-d", resident_millicores=1000, init_extra_millicores=3000))
token_d = _token_of("pod-d")
m.complete_init("pod-d", m.CompleteRequest(round_token=token_d))  # 余量 3000m
res = m.admit_pod(m.AdmitRequest(pod_id="pod-d2", resident_millicores=500, init_extra_millicores=2500))
check("准备: pod-d2 占满节点, 余量 0m", res["node"]["remaining_millicores"] == 0)

ok, detail = expect_http_error(
    lambda: m.admit_pod(m.AdmitRequest(pod_id="pod-d3", resident_millicores=500, init_extra_millicores=100)),
    409, "insufficient_node_cpu",
)
check("余额不足申领返回 409 insufficient_node_cpu", ok, detail)
check("返回前已释放写锁(连接上无未提交事务)", not real_db.in_transaction)
snap_ok("拒绝后余量仍为 0m, 未多出/多扣额度", 0)
check("拒绝后 Pod 列表不含 pod-d3", "pod-d3" not in committed_state()["pods"])

m.delete_pod("pod-d2")  # 余量回到 3000m(常驻 500 + 额外 2500)
res = m.admit_pod(m.AdmitRequest(pod_id="pod-d3", resident_millicores=500, init_extra_millicores=100))
check("额度释放后 pod-d3 以已提交余量申领成功, 余量 2400m",
      res["node"]["remaining_millicores"] == 2400, res.get("node"))

print("\n-- 场景D2: 重做初始化额度不足, 返回前释放写锁且 Pod 继续运行 --")
reset()
m.admit_pod(m.AdmitRequest(pod_id="pod-e", resident_millicores=1000, init_extra_millicores=3000))
token_e = _token_of("pod-e")
m.complete_init("pod-e", m.CompleteRequest(round_token=token_e))  # 余量 3000m
m.admit_pod(m.AdmitRequest(pod_id="pod-f", resident_millicores=500, init_extra_millicores=2500))  # 占满
res = m.reinit("pod-e")
check("额度不足 -> acquired=False 且 Pod 继续 RUNNING",
      res["acquired"] is False and res["phase"] == "RUNNING" and res["round_no"] == 1, res)
check("领不到额度返回前写锁已释放", not real_db.in_transaction)
snap_ok("拒绝后余量仍 0m, 轮次与持有状态不变", 0, "pod-e", "RUNNING", False)
m.delete_pod("pod-f")
res = m.reinit("pod-e")
check("额度释放后重做初始化成功(第 2 轮)", res["acquired"] and res["round_no"] == 2, res)

# ---------------------------------------------------------------- 场景 E
print("\n-- 场景E: 准入提交失败, 节点占用与 Pod 记录一起回滚 --")
reset()
fdb.fail_on("", when="commit")
ok, detail = expect_http_error(
    lambda: m.admit_pod(m.AdmitRequest(pod_id="pod-g", resident_millicores=1000, init_extra_millicores=500)),
    503, "storage_write_failed",
)
check("准入提交失败返回 503", ok, detail)
snap_ok("失败后无 pod-g 记录且余量未变(4000m)", 4000)
res = m.admit_pod(m.AdmitRequest(pod_id="pod-g", resident_millicores=1000, init_extra_millicores=500))
check("重试准入成功且只扣一次, 余量 2500m",
      res["node"]["remaining_millicores"] == 2500, res.get("node"))

print("\n-- 场景F: 删除 Pod 提交失败, 记录与额度都不动 --")
reset()
m.admit_pod(m.AdmitRequest(pod_id="pod-h", resident_millicores=1000, init_extra_millicores=500))
fdb.fail_on("", when="commit")
ok, detail = expect_http_error(lambda: m.delete_pod("pod-h"), 503, "storage_write_failed")
check("删除提交失败返回 503", ok, detail)
snap_ok("失败后 pod-h 仍在且余量仍 2500m", 2500, "pod-h", "INITIALIZING", True)
res = m.delete_pod("pod-h")
check("重试删除释放 1500m, 余量 4000m",
      res["freed_millicores"] == 1500 and res["node"]["remaining_millicores"] == 4000, res)

print("\n-- 场景G: 只读接口不会带出遗留事务 --")
reset()
m.admit_pod(m.AdmitRequest(pod_id="pod-i", resident_millicores=1000, init_extra_millicores=500))
for fn in (lambda: m.get_node(), lambda: m.list_pods(), lambda: m.get_pod("pod-i")):
    fn()
    if real_db.in_transaction:
        check("只读接口结束后无未提交事务", False)
        break
else:
    check("只读接口结束后无未提交事务", True)

print(f"\n== 结果: {_checks - len(_failures)}/{_checks} 通过 ==")
_tmp.cleanup()
if _failures:
    print("失败项:", *_failures, sep="\n  - ")
    sys.exit(1)
print("全部通过")
