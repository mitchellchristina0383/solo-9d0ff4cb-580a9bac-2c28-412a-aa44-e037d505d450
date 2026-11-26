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
- **写入失败即整段回滚**: 准入、重做初始化、完成通知、删除都在同一事务边界内
  完成"节点计数 + Pod 记录"的改动; 任一条 SQLite 写入或提交失败都会整体回滚
  并返回 `503 storage_write_failed`, Pod 记录与节点占用保持已提交的样子,
  后续查询/申领/重试以已提交数据为准。额度不足这类业务拒绝在抛 409 前同样回滚,
  释放本次写锁, 不会把半截事务留给下一次准入。

## 测试

```bash
python3 tests/smoke_test.py              # 默认打 http://localhost:8000
python3 tests/fault_injection_test.py    # 进程内故障注入, 需能 import app.main
```

`smoke_test.py`: 29 项断言, 覆盖: 8 路并发申领(4000m 总额只成交 2×1500m,
余量精确 1000m)、旧令牌 409 且新一轮不受影响、重复完成不双重归还、
领不到额外额度继续 RUNNING、归还后其他 Pod 当即申领成功、删除释放、参数校验。

`fault_injection_test.py`: 48 项断言, 在模块级连接上注入 I/O / 提交失败,
覆盖准入、完成通知、重做初始化、删除四条写路径的整段回滚; 每次失败后都用
另一条连接核对已提交数据并断言连接上没有遗留未提交事务; 另覆盖额度不足的
申领/重做初始化返回前释放写锁, 以及"完成通知写失败后下一次准入不得超额"
这一具体回归。

## 实测结果(本环境, 2026-11-26)

- 本环境**没有 Docker**(无 `docker` 命令、无容器运行时), compose 未实际执行;
  用等价方式(Python 3.11 + uvicorn 直接跑 `app.main:app`)验证, `/docs` 返回 200。
- `tests/smoke_test.py` 连续 3 轮全过(每轮 29/29), 服务日志无报错。
- `tests/fault_injection_test.py` 48/48 通过; 另用带 `/__fault` 注入端点的测试
  进程做 HTTP 级复跑: 完成通知写 Pod 失败 -> 503, 节点余量仍 2500m, 下一次
  申领只按已提交的 2500m 记账(余量 1000m); 重做初始化提交失败 -> 503, Pod 仍
  RUNNING 第 1 轮、余量 3000m, 重试成功进入第 2 轮; 余量不足申领 409 后余量
  仍为 0m; 8 路并发申领仍只成交 2 个、余量 1000m。
- 修复前版本用同一注入复现: 完成通知写失败后连接遗留写事务, 下一次准入把未
  真正归还的 500m 一起提交, 节点计数器偏离已提交数据。

## 未完成项 / 限制

- **Docker Compose 未在本环境实测**(无容器运行时); Dockerfile/compose 按标准写法交付。
  compose 的 `healthcheck` 在 `/healthz`(仍为 200) 上探活, 未覆盖 503 写失败场景。
- 单节点单进程演练: 记账用单连接+锁, 未做多副本/多节点扩展。
- 无认证鉴权, `/admin/reset` 等管理接口裸奔, 仅限本地演练。
- 轮次令牌为不透明随机串, 未做过期时间; Pod 崩溃不自动回收(需显式 DELETE)。
