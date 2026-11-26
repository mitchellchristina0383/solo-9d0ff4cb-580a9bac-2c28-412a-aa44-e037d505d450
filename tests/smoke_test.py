#!/usr/bin/env python3
"""冒烟测试: 并发申领不超卖 / 迟到通知不结束新一轮 / 归还后他人可申领.

用法: python3 tests/smoke_test.py [base_url]   默认 http://localhost:8000
只用标准库, 退出码非 0 表示有失败项.
"""

import json
import sys
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"

_failures = []
_checks = 0


def req(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(r) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        raw = e.read()
        return e.code, (json.loads(raw) if raw else {})


def check(name, cond, detail=""):
    global _checks
    _checks += 1
    mark = "PASS" if cond else "FAIL"
    print(f"[{mark}] {name}" + (f"  | {detail}" if detail else ""))
    if not cond:
        _failures.append(name)


def remaining():
    st, body = req("GET", "/node")
    assert st == 200, body
    return body["remaining_millicores"]


print(f"== 目标: {BASE} ==\n-- 阶段0: 复位并配置节点总额 4000m --")
st, node = req("POST", "/admin/reset")
check("复位", st == 200, node)
st, node = req("PUT", "/node", {"total_millicores": 4000})
check("配置总额 4000m", st == 200 and node["remaining_millicores"] == 4000, node)

print("\n-- 阶段1: 8 路并发申领(每个 1000m 常驻 + 500m 额外 = 1500m), 总额 4000m 只能成交 2 个 --")


def admit(i):
    return req("POST", "/pods", {
        "pod_id": f"pod-c{i}", "resident_millicores": 1000, "init_extra_millicores": 500,
    })


with ThreadPoolExecutor(max_workers=8) as pool:
    results = list(pool.map(admit, range(8)))

created = [b for s, b in results if s == 201]
rejected = [b for s, b in results if s == 409]
check("恰好 2 个申领成功", len(created) == 2, f"201x{len(created)} 409x{len(rejected)}")
check("其余 6 个被 409 拒绝", len(rejected) == 6)
check("节点余量=1000m 且未超卖", remaining() == 1000, f"remaining={remaining()}")
st, lst = req("GET", "/pods")
check("列表里 2 个 Pod 均为 INITIALIZING 且持有额外额度",
      st == 200 and len(lst["pods"]) == 2
      and all(p["phase"] == "INITIALIZING" and p["extra_held"] for p in lst["pods"]),
      lst.get("pods"))

print("\n-- 阶段2: 迟到通知(旧令牌)不能结束新一轮 --")
req("POST", "/admin/reset")
st, a = req("POST", "/pods", {"pod_id": "pod-a", "resident_millicores": 1000, "init_extra_millicores": 500})
check("pod-a 准入(两份都拿到才接单)", st == 201 and a["phase"] == "INITIALIZING", a)
t1 = a["round_token"]
check("返回第 1 轮独立令牌", a["round_no"] == 1 and bool(t1))

st, b = req("POST", "/pods/pod-a/init/complete", {"round_token": t1})
check("第 1 轮完成 -> 归还 500m, 进入 RUNNING",
      st == 200 and b["released"] and b["phase"] == "RUNNING" and remaining() == 3000,
      f"remaining={remaining()}")

st, b = req("POST", "/pods/pod-a/reinit")
check("重做初始化申领成功 -> 第 2 轮新令牌", st == 200 and b["acquired"] and b["round_no"] == 2, b)
t2 = b["round_token"]
check("新令牌与旧令牌不同", t2 != t1)
check("重新持有额外额度, 余量回到 2500m", remaining() == 2500, f"remaining={remaining()}")

st, b = req("POST", "/pods/pod-a/init/complete", {"round_token": t1})
check("旧令牌完成通知被 409 拒绝", st == 409 and b["detail"]["error"] == "stale_round_token", b)
st, b = req("GET", "/pods/pod-a")
check("pod-a 仍在 INITIALIZING(新一轮未被旧通知结束)",
      st == 200 and b["phase"] == "INITIALIZING" and b["extra_held"], b)
check("节点余量未被旧通知改动", remaining() == 2500, f"remaining={remaining()}")

st, b = req("POST", "/pods/pod-a/init/complete", {"round_token": t2})
check("当前轮令牌完成 -> 归还成功", st == 200 and b["released"] and remaining() == 3000)
st, b = req("POST", "/pods/pod-a/init/complete", {"round_token": t2})
check("同令牌重复完成 -> 幂等不再归还", st == 200 and not b["released"] and b["already_completed"])
check("重复完成后余量不变(未双重归还)", remaining() == 3000, f"remaining={remaining()}")

print("\n-- 阶段3: 领不到就继续运行; 归还后其他 Pod 当即可以申领 --")
st, b = req("POST", "/pods", {"pod_id": "pod-b", "resident_millicores": 1000, "init_extra_millicores": 2000})
check("pod-b 准入占满节点", st == 201 and remaining() == 0, f"remaining={remaining()}")
t_b = b["round_token"]

st, b = req("POST", "/pods/pod-a/reinit")
check("节点已满 -> pod-a 申领额外额度失败但继续 RUNNING",
      st == 200 and not b["acquired"] and b["phase"] == "RUNNING", b)

st, b = req("POST", "/pods/pod-b/init/complete", {"round_token": t_b})
check("pod-b 完成 -> 归还 2000m", st == 200 and b["released"] and remaining() == 2000)
st, b = req("POST", "/pods/pod-a/reinit")
check("归还后 pod-a 当即申领成功(第 3 轮)", st == 200 and b["acquired"] and b["round_no"] == 3, b)
t3 = b["round_token"]
st, b = req("POST", "/pods", {"pod_id": "pod-c", "resident_millicores": 500, "init_extra_millicores": 500})
check("其他新 Pod 也当即申领成功", st == 201 and remaining() == 500, f"remaining={remaining()}")

print("\n-- 阶段4: 初始化中重复 reinit 幂等 --")
st, b = req("POST", "/pods/pod-a/reinit")
check("已在初始化 -> 返回同一轮令牌, 不新开一轮",
      st == 200 and b["already_initializing"] and b["round_no"] == 3 and b["round_token"] == t3, b)

print("\n-- 阶段5: 删除 Pod 释放全部占用 --")
st, b = req("DELETE", "/pods/pod-a")
check("删除 pod-a 释放 1000m 常驻 + 500m 额外", st == 200 and b["freed_millicores"] == 1500, b)
st, b = req("GET", "/pods/pod-a")
check("删除后查询 404", st == 404)

print("\n-- 阶段6: 参数与状态校验 --")
st, b = req("POST", "/pods", {"pod_id": "bad", "resident_millicores": 0, "init_extra_millicores": 100})
check("非正额度被 422 拒绝", st == 422)
st, b = req("PUT", "/node", {"total_millicores": 8000})
check("节点有占用时改总额被 409 拒绝", st == 409, b)
st, b = req("POST", "/admin/reset")
st, b = req("PUT", "/node", {"total_millicores": 8000})
check("清空后可改总额为 8000m", st == 200 and b["remaining_millicores"] == 8000, b)

print(f"\n== 结果: {_checks - len(_failures)}/{_checks} 通过 ==")
if _failures:
    print("失败项:", * _failures, sep="\n  - ")
    sys.exit(1)
print("全部通过")
