# 共享身份向量与协议参考

`fixtures/identity-vectors.json` 保存 Go/Python 共用的合成样例，不是生产消息导出。通用身份与字节合同由 [Go Message](../message/message.go)、[Python Message](../python/src/reliable_messaging/message.py)及其测试实现；设计解释见[消息身份与协议](../docs/01-核心设计/消息身份与协议.md)。

样例是在 M0 建立的，因此 JSON 的 `status` 和 `tenant` 保留历史命名；测试把 `tenant` 映射为当前 Scope，不强制业务 payload 带 tenant_id，也不把历史 status 当作当前 SDK 发布状态。具体版本见[发布索引](../docs/releases/README.md)。

## 样例解释

- 10 项向量覆盖 IAM 原 payload、相同重放、内容/作用域/格式变化，及 QS envelope 的未知扩展、重放、身份/类型/作用域冲突。
- 指纹保留 payload 原字节；JSON 空白、字段顺序和发生时间表示均不能自动归一。同一意图重用首次持久字节。
- `valid` 是原参考检查器的宿主 wire 判断，不等于通用 Message 构造是否成功。宿主 envelope 不合法的向量仍可有合法通用身份与指纹。
- 旧 source 不自动等于 producer；consumer、领取 token、次数等投递状态不进入发布指纹。业务路由/作用域映射属于宿主。

## 验证

```sh
go test ./tests/compatibility
```

Python 在 `python/` 下运行 `uv run --locked --extra nsq pytest tests/test_contracts.py`，直接读上述向量，并调用 Go 公开 API 验证扩展输入。算法相同不证明宿主已采用、历史行已迁移或 MQ/业务已确认。

不要重新排版 payload 或重算摘要来掩盖兼容变化。变更需核对两端 codec、固定向量、原持久字节与[兼容与升级](../docs/03-维护与验证/兼容与升级.md)。历史形成过程保留在归档层，不作为接入前置。
