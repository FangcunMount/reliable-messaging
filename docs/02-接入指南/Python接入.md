# Python 接入

Python 分发包名为 `fangcun-reliable-messaging`，导入名为 `reliable_messaging`。它不依赖 Go runtime 或宿主仓库。本文解释当前源码的 Python 合同和接线；制品来源、已发布版本及候选范围见[发布索引](../releases/README.md)。

Python 0.1 的核心是绑定原 SQLAlchemy 事务、适配宿主已有 pending/delivered 表，以及核对原事件的持久业务回执。0.2 源码在此基础上增加可选 NSQ、protected wire 和独立回执型 Outbox。它们共享消息身份和部分确认分类，**不共享 Go 租约 Store 的表结构、多进程领取与 fencing 合同**。

## 安装与支持范围

固定所选发布版本，安装方式见 [Python 包入口](../../python/README.md)。NSQ 部分需 `nsq` extra。当前源码要求 Python 3.11～3.13、SQLAlchemy 2.0 async；NSQ extra 的精确依赖由 [`pyproject.toml`](../../python/pyproject.toml) 决定。

| 合同 | 使用位置 | 限制 |
|---|---|---|
| 0.1 核心：`Message`、原事务 appender | `message.py`、`sqlalchemy.py` | MySQL、能验证非 autocommit 的原 async 事务；asyncmy 是已测试驱动 |
| 0.1 核心：原 pending/delivered 适配 | `MySQLPendingOutbox` | 宿主已有表、单进程、接收端重复安全；无租约领取/隔离状态 |
| 0.1 核心：业务回执结算 | `deliver_durable` | callback 必须验证原 event_id 的持久回执，不得替换为 Broker PUB 结果 |
| 0.2 源码：NSQ 与 protected wire | `nsq.py`、`wire.py`、`protected.py` | 显式 nsqd；不实现动态 lookupd 发现；宿主提供密钥与鉴权 |
| 0.2 源码：回执型 Outbox | `MySQLDurableOutbox` | 独立宿主表；原事务内扫描/结算；顺序、回执和 hold 与 Go 标准 Store 分开 |

上述范围说明实现合同，不从版本号或本地构建推断远端包已发布或生产已采用。

## 原 SQLAlchemy 事务

`bind` 固定当前事务和 savepoint；`append` 只接受宿主定义的 INSERT。宿主负责表、列值和重复身份策略，不由这个 appender 编码业务消息或建立 schema。

```python
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.dml import Insert

from reliable_messaging.sqlalchemy import bind


async def write_intent(
    db: AsyncSession, business_insert: Insert, outbox_insert: Insert
) -> None:
    async with db.begin():
        await db.execute(business_insert)
        await bind(db).append(outbox_insert)
```

这段函数由宿主开始和提交事务；已有事务中的调用方只在其原 scope 内 `await bind(db).append(...)`，不要再嵌套一个新的顶层事务。因宿主此前操作而 autobegin 的事务也可绑定，但 bind/append 本身不会建立事务。

SQLAlchemy 的逻辑 begin 不足以证明原子性：adapter 还要求实际 MySQL 驱动提供 `get_autocommit` 且返回 false。AUTOCOMMIT、非 MySQL、缺少可核验方法、结束事务或更换 savepoint 都被拒绝。Append 失败或取消时宿主须回滚；SDK 不 commit/rollback/close，也不 dispose pool。源码见 [`sqlalchemy.py`](../../python/src/reliable_messaging/sqlalchemy.py)。

## 0.1 原表适配与持久回执

`MySQLPendingOutbox(host_table)` 借用宿主已有的 event_id/payload/delivered/attempts/available_at/delivered_at 表。它按数据库 UTC 时钟扫描；`delivered` 与 `retry` 要求显式活动事务，并由宿主提交。原消息 RFC3339 字符串不改写，UTC+8 是业务与运维显示规范。

`deliver_durable` 接受三个参数：原 event_id、提供 `delivered(event_id)`/`retry(event_id)` 的宿主 store，以及无参数的异步 accept callback。`MySQLPendingOutbox` 的方法还需 db 参数，因此应由宿主提供 store 包装，包装在每次结算时进入宿主事务。不要把它直接作为省略事务的 ResultStore。

```python
from collections.abc import Awaitable, Callable

from reliable_messaging import DeliveryResult, deliver_durable
from reliable_messaging.delivery import ResultStore


async def settle_original(
    event_id: str,
    host_store: ResultStore,
    verify_original_receipt: Callable[[], Awaitable[None]],
) -> DeliveryResult:
    return await deliver_durable(event_id, host_store, verify_original_receipt)
```

`verify_original_receipt` 只有在验证业务接收端已持久接单，并核对原 event_id 后才能返回。HTTP/gRPC 请求发送成功或 NSQ PUB OK 不够；Broker `DeliveryResult` 作为 callback 返回值会被拒绝。

回执确认后才调用 store.delivered；异常调用 store.retry，未知结果保留原通知并返回 Unknown；显式 `DeliveryRejected` 返回 Rejected。取消传播且不结算；结算失败传播到宿主监督层，不重新解释为接收端失败。重投对象是已存在的原通知，不能据此重新运行模型、替换冻结配置或重新创建任务。源码与反例见 [`delivery.py`](../../python/src/reliable_messaging/delivery.py)、[结算测试](../../python/tests/test_delivery.py)。

## 循环与停止

若宿主没有扫描循环，可以显式使用 `PeriodicRelay`：

```python
from collections.abc import Awaitable, Callable

from reliable_messaging import PeriodicRelay


async def serve_results(attempt: Callable[[], Awaitable[int]]) -> None:
    relay = PeriodicRelay(attempt, poll_seconds=1, shutdown_seconds=5)
    await relay.start()
    try:
        await relay.wait()
    finally:
        await relay.stop()
```

`attempt` 负责扫描和处理已提交意图；示例时间只是接线参数。构造不创建 task；start 才在宿主共享 asyncio loop 上运行。notify 是可丢失的提交后提示，周期扫描与重启扫描仍必须成立。wait/stop 会暴露扫描失败。

停止先结束新接纳并等待当前 attempt，超过预算后取消并抛 TimeoutError。callback 必须支持合作取消；Stop 超时不代表业务已经完成。宿主在循环与发送方实际排空后再关闭自己的数据库、channel 和其他资源。已有单进程 supervisor/polling loop 的 qs-ai 可继续使用原循环，不必叠加第二个 Relay。

## 0.2 NSQ 与回执型 Outbox

NSQ 是可选传输。Publisher/Subscriber 构造无 I/O；宿主在原事件循环内显式 await start，先启动 Publisher 再启动 Subscriber。Subscriber 借用按 source nsqd 地址配置的 Publisher，要求逐节点 `failure_ready`，并在处理前确认持久失败 channel 就绪。关闭先停止接纳和 Subscriber，再停止 Publisher，最后关闭池；不能引入 `nsq.run`、另一个 loop、进程或线程。详细驱动与故障合同见 [Python NSQ 包入口](../../python/NSQ.md)。

NSQ handler 必须在业务/Inbox/回执 Outbox 提交后返回。failed_handler 先持久保存失败/hold；invalid_handler 用原 wire hash 隔离未经认证的字节，不能相信其宣称的消息 ID。Broker 失败或超时没有业务重试授权。

`MySQLDurableOutbox` 使用 [`delivery/mysql/schema.sql`](../../delivery/mysql/schema.sql) 所描述的另一类表，不是原 pending/delivered 表或 Go `rm_outbox`。宿主通过原事务 INSERT 意图；扫描与 Published/Retry/Confirm/Hold/RearmAck 都借用活动非 autocommit 事务。需业务回执的行 PUB 后等待回执，只能由宿主核验已认证回执后 Confirm。receipt-free final ACK 的 Broker 确认按自身合同结束，没有 ack-of-ack。

`rearm_ack` 仅用于可重发的原 final ACK；原身份、wire 和失败预算保持不变，技术 held 不自动复活。成功的 ACK PUB 不消耗失败预算，未知/失败 PUB 仍计数；旧版计数不能推断或自动归一。持久行、业务顺序、未知模型调用、已接单任务和原冻结配置的恢复均由宿主权威记录决定。见[事务与 Outbox](../01-核心设计/事务与Outbox.md)、[宿主接入边界](宿主接入边界.md)。

## 核验入口

在 `python/` 下先准备锁定依赖，再运行无集成测试：

```sh
uv sync --locked --extra nsq
uv run --locked --extra nsq pytest -m 'not integration'
```

原事务/进程恢复集成需要一次性 MySQL；NSQ 测试需要一次性 NSQ 与部分 Docker 场景，缺少必需依赖必须失败。环境变量和作业矩阵见 [Python workflow](../../.github/workflows/python.yml)、[测试与故障验证](../03-维护与验证/测试与故障验证.md)。包构建、installed-wheel 核验、宿主部署、真实业务验收分别证明各自范围；无真实模型调用是隔离测试的边界，不是生产业务结论。
