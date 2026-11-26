# 单节点 CPU 准入演练(初始化额外额度)

批处理 Pod 启动解压模型时, 除了**常驻额度**还要申领一份**初始化额外额度**;
初始化完成后归还额外额度, 其他 Pod 当即可以申领。FastAPI 单节点演练,
SQLite 记账, CPU 一律按**毫核(millicore)**计。

## 启动

```bash
docker compose up --build
# 打开 http://localhost:8000/docs 操作全部接口
```

节点总额由 `NODE_TOTAL_MILLICORES`(默认 4000)在首次建库时写入;
节点无占用时可用 `PUT /node` 调整, `POST /admin/reset` 清空重跑演练。

本地无 Docker 时: `pip install -r requirements.txt && uvicorn app.main:app`

## 接口

| 方法/路径 | 语义 |
|---|---|
| `POST /pods` | 首次启动准入: 常驻+额外**两份都拿到才接单**(原子), 返回第 1 轮 `round_token` |
| `POST /pods/{id}/init/complete` | 初始化完成通知: 令牌匹配且**当前轮首次完成才归还**额外额度; 重复通知幂等 no-op; 旧令牌 409 |
| `POST /pods/{id}/reinit` | 运行中重做初始化: **只重新申领额外额度**, 领不到就继续 RUNNING; 申领成功则轮次+1、发新令牌 |
| `GET /pods/{id}` / `GET /pods` / `GET /node` | 查询 Pod 阶段(INITIALIZING/RUNNING)和节点余量 |
| `DELETE /pods/{id}` | 删除 Pod, 释放其全部占用 |
| `PUT /node` / `POST /admin/reset` | 配置节点总额(无占用时) / 清空重跑 |

## 关键设计

- **不超卖**: 节点余量只记在 `node` 单行计数器上, 申领/归还都是
  `UPDATE ... SET allocated = allocated + ? WHERE allocated + ? <= total`
  原子条件更新, 不遍历其他 Pod(O(1)); 单连接+锁串行化事务, 并发申领也不会超出节点总额。
- **迟到通知防护**: 每轮初始化发独立 `round_token`(轮次号+随机令牌),
  完成通知必须携带当前轮令牌, 旧通知无法结束新一轮。
- **幂等**: 同令牌重复完成不重复归还; 初始化中重复 reinit 返回当前轮令牌, 不新开一轮。

## 测试

```bash
python3 tests/smoke_test.py            # 默认打 http://localhost:8000
```

29 项断言, 覆盖: 8 路并发申领(4000m 总额只成交 2×1500m, 余量精确 1000m)、
旧令牌 409 且新一轮不受影响、重复完成不双重归还、领不到额外额度继续 RUNNING、
归还后其他 Pod 当即申领成功、删除释放、参数校验。

## 实测结果(本环境, 2026-11-26)

- 本环境**没有 Docker**, compose 未实际执行; 用等价方式
  (Python 3.11 venv + uvicorn 直接跑 `app.main:app`)验证。
- `tests/smoke_test.py` 连续 4 轮全过(每轮 29/29), 服务日志无报错,
  `/docs` 与 `/openapi.json` 均 200。

## 未完成项 / 限制

- **Docker Compose 未在本环境实测**(无容器运行时); Dockerfile/compose 按标准写法交付。
- 单节点单进程演练: 记账用单连接+锁, 未做多副本/多节点扩展。
- 无认证鉴权, `/admin/reset` 等管理接口裸奔, 仅限本地演练。
- 轮次令牌为不透明随机串, 未做过期时间; Pod 崩溃不自动回收(需显式 DELETE)。
