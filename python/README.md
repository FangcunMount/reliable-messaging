# Python reliable-messaging SDK

独立分发包 fangcun-reliable-messaging，导入 reliable_messaging。它是嵌入式 async SDK，借用原事务/资源，不依赖 Go runtime 或宿主仓库。当前源码为 [pyproject.toml](pyproject.toml)的 **0.2.0a2**；制品来源与候选状态见[发布索引](../docs/releases/README.md)，不能从源码版本推断发布或生产采用。

## 安装

在核验过的源码 tag 的 python 目录下：

```sh
python -m pip install .
```

需可选 NSQ/JOSE 时：

```sh
python -m pip install '.[nsq]'
```

宿主固定版本与制品摘要。已发布 0.1 核心没有 Broker adapter，0.2 源码增加可选 NSQ/protected wire 与回执型适配，见 [NSQ.md](NSQ.md)。不要用 0.2 接线说明推导 0.1 制品含 NSQ。

## 最小原事务用法

```python
from reliable_messaging.sqlalchemy import bind

async with host_session.begin():
    await host_session.execute(business_insert)
    await bind(host_session).append(outbox_insert)
```

session、两个 INSERT 及重复身份政策由宿主提供，需活动且实际非 autocommit 的 MySQL SQLAlchemy async 事务。SDK 不开始、提交、回滚或关闭资源。当前支持 Python 3.11～3.13、SQLAlchemy 2.0，asyncmy 为已测事务驱动。

详细教程的唯一正文为 [Python接入](../docs/02-接入指南/Python接入.md)：区分原 pending/delivered 表、deliver_durable 业务回执 callback、可选 PeriodicRelay 与回执型 Outbox。[共享向量](../contracts/README.md)验证身份/指纹，payload 不规范化 JSON、发生时间不改写。

SDK 不改变执行任务、冻结配置、模型调用、checkpoint、容量或业务恢复许可。原单进程扫描/监督循环可调用 SDK 原语，不必叠加第二个 Relay。

## 验证与分发

在 python 目录运行：

```sh
uv sync --locked --extra nsq
uv run --locked --extra nsq pytest -m 'not integration'
uv build
```

必需集成使用一次性 RM_M7_MYSQL_URL；NSQ 另需 RM_MQ_NSQ_TCP/RM_MQ_NSQ_HTTP，崩溃丢消息场景需 Docker 且只删除自建容器。缺依赖失败而非跳过。完整作业/installed-wheel/报告检查见 [Python CI](../.github/workflows/python.yml)与[测试与故障验证](../docs/03-维护与验证/测试与故障验证.md)。

构建不授权替换既有制品或升级生产。Go/Python 包、wire、schema、宿主部署为独立边界，见[兼容与升级](../docs/03-维护与验证/兼容与升级.md)。历史 M7 过程与 prerelease 快照留在归档/发布证据层。
