# 版本与发布索引

本页索引可消费的语言版本、固定源码与发布资产，不代表宿主已部署或业务已验收。2026-10-06（UTC+8）以GitHub Release / tag API只读核验；本地tag同时核对但不作为远端发布的唯一证明。后续新增版本需更新本页，不能沿用本次“最新”结论。

<!-- source-python-version: 0.2.0a2 -->

## 当前源码、标签、制品与宿主

| 层次 | 本次核对结果 | 结论边界 |
| --- | --- | --- |
| 文档重建前main源码 | [fd06495ca819124c73ffe31b20a5381c06885e87](https://github.com/FangcunMount/reliable-messaging/commit/fd06495ca819124c73ffe31b20a5381c06885e87)；Python源码版本 `0.2.0a2` | main存在实现不证明某个制品或生产宿主已采用；源码包版本见[pyproject](../../python/pyproject.toml) |
| 本地tag | 已见Go M2/M6/MQ预发布及Python三个预发布tag | 本地ref可能过期或与远端不同；本次另读取GitHub ref并解析annotated tag |
| GitHub Go Release | 最新已核验预发布 `v0.3.0-mq-ai.2` | 无Go v1.0 Release；Go module从固定tag消费，不要求另有二进制附件 |
| GitHub Python资产 | 最新已核验预发布 `python/v0.2.0a2`，有wheel与sdist | 仅核验GitHub资产元数据与Release说明；本次未重新下载安装包，也未核验PyPI发布 |
| IAM源码采用 | [c8fc1fe的go.mod](https://github.com/FangcunMount/iam/blob/c8fc1fe61a32056368148e232e96d32763c15ba5/go.mod)固定 `v0.3.0-m6.1` | 源码依赖，不是本次生产二进制采集 |
| QS源码采用 | [2ccc2de的go.mod](https://github.com/FangcunMount/qs-server/blob/2ccc2de44bbd45e26d29e7e130da518cc32426f0/go.mod)固定 `v0.3.0-mq-ai.2` | 源码依赖，不是本次逐实例部署采集 |
| qs-ai与今日生产采用 | 本轮未重新核验 | 由各宿主带采集日期的版本、配置与业务验收记录负责，不从SDK标签推断 |

源于早期阶段的“候选未发布”说明不再适用于已列出的GitHub Release。反之，GitHub存在预发布也不表示原Go M0～M6、宿主MQ切换或完整业务恢复已验收。

## Go公开版本

以下源码来自本次远端tag解析；Release字段 `target_commitish=main` 不用作固定源码。时间均为UTC+8。

| Release | 固定源码 | 发布时刻 | 范围 |
| --- | --- | --- | --- |
| [v0.1.0-m2.1](https://github.com/FangcunMount/reliable-messaging/releases/tag/v0.1.0-m2.1) | [014b2a1](https://github.com/FangcunMount/reliable-messaging/commit/014b2a1bab2a1e155b0d11d318c70bbba3d9adc0) | 2026-09-22 13:39:52 | 预发布基础合同 |
| [v0.1.0](https://github.com/FangcunMount/reliable-messaging/releases/tag/v0.1.0) | [1cab553](https://github.com/FangcunMount/reliable-messaging/commit/1cab5531ee985fff9561b94c9b3d396230f870d4) | 2026-09-23 09:45:29 | 早期已发布Go线 |
| [v0.2.0](https://github.com/FangcunMount/reliable-messaging/releases/tag/v0.2.0) | [5f47f16](https://github.com/FangcunMount/reliable-messaging/commit/5f47f16b7b74374f627095bdbb7a4ea8a87f7af6) | 2026-09-23 20:59:02 | QS Outbox接入线 |
| [v0.2.1](https://github.com/FangcunMount/reliable-messaging/releases/tag/v0.2.1) | [5bbbacb](https://github.com/FangcunMount/reliable-messaging/commit/5bbbacb15f9c044e7ef27d4384110a97b92c745f) | 2026-09-24 16:16:46 | Go修订线 |
| [v0.3.0-m6.1](https://github.com/FangcunMount/reliable-messaging/releases/tag/v0.3.0-m6.1) | [c54dd23](https://github.com/FangcunMount/reliable-messaging/commit/c54dd233dadaf74494a4dc70eb67b2f37752bc95) | 2026-09-28 13:57:46 | 预发布catalog / wire / NSQ生命周期 |
| [v0.3.0-m6.2](https://github.com/FangcunMount/reliable-messaging/releases/tag/v0.3.0-m6.2) | [28b18a9](https://github.com/FangcunMount/reliable-messaging/commit/28b18a9a166b5f8cf37104b2c17d0e3f14069403) | 2026-09-29 09:01:22 | 预发布原确认行审计重排 |
| [v0.3.0-m6.3](https://github.com/FangcunMount/reliable-messaging/releases/tag/v0.3.0-m6.3) | [e4c6984](https://github.com/FangcunMount/reliable-messaging/commit/e4c69847a72dc1068d22badbc093c25beba4380a) | 2026-10-02 13:14:30 | 预发布通用状态构造 |
| [v0.3.0-m6.4](https://github.com/FangcunMount/reliable-messaging/releases/tag/v0.3.0-m6.4) | [a985c08](https://github.com/FangcunMount/reliable-messaging/commit/a985c089d7ef477b7daf0a85d73980e6c7be21a8) | 2026-10-02 19:49:58 | 预发布signaling / Redis adapter |
| [v0.3.0-mq-ai.1](https://github.com/FangcunMount/reliable-messaging/releases/tag/v0.3.0-mq-ai.1) | [9f16fa3](https://github.com/FangcunMount/reliable-messaging/commit/9f16fa30dc43a50a2d4492f3e95c905a8f969331) | 2026-10-03 16:33:26 | 预发布protected wire与独立回执型MySQL |
| [v0.3.0-mq-ai.2](https://github.com/FangcunMount/reliable-messaging/releases/tag/v0.3.0-mq-ai.2) | [8633ba5](https://github.com/FangcunMount/reliable-messaging/commit/8633ba585b5a1c8f95fb16b19469fbd0097611c7) | 2026-10-04 04:46:10 | 预发布持久final ACK失败预算修复 |

0.1.0、0.2.0、0.2.1的GitHub标记不是prerelease；它们仍是0.x范围，不等于已建立v1稳定承诺。`m6.*`与`mq-ai.*`属于已发表的不同范围预发布，不能仅按字串排序判断宿主应升级哪条合同。

## Python公开版本与资产

| Release | 固定源码 | 发布时刻 | 附件与合同 |
| --- | --- | --- | --- |
| [python/v0.1.0a1](https://github.com/FangcunMount/reliable-messaging/releases/tag/python/v0.1.0a1) | [fea9238](https://github.com/FangcunMount/reliable-messaging/commit/fea92389967076e3529289e80b1d96abef76dc73) | 2026-10-03 11:34:15 | wheel、sdist；原事务、原pending/delivered表、持久callback、显式周期Relay；无NSQ adapter |
| [python/v0.2.0a1](https://github.com/FangcunMount/reliable-messaging/releases/tag/python/v0.2.0a1) | [9f16fa3](https://github.com/FangcunMount/reliable-messaging/commit/9f16fa30dc43a50a2d4492f3e95c905a8f969331) | 2026-10-03 16:33:17 | wheel、sdist、provenance、SHA256SUMS；可选NSQ / protected wire / 独立回执型MySQL |
| [python/v0.2.0a2](https://github.com/FangcunMount/reliable-messaging/releases/tag/python/v0.2.0a2) | [8633ba5](https://github.com/FangcunMount/reliable-messaging/commit/8633ba585b5a1c8f95fb16b19469fbd0097611c7) | 2026-10-04 04:46:05 | wheel、sdist、acceptance-summary、SHA256SUMS；final ACK失败预算修复 |

当前a2的GitHub资产元数据摘要如下；旧资产保留在对应Release，不覆盖旧版本。安装前应下载对应资产并核对本地摘要。

| 资产 | GitHub登记SHA-256 |
| --- | --- |
| `fangcun_reliable_messaging-0.2.0a2-py3-none-any.whl` | `a9c424de16b1701c52d04cb51f2898fa868488f110c5772f2fd360e1c543c932` |
| `fangcun_reliable_messaging-0.2.0a2.tar.gz` | `344a04e7679562215af07f4c21615486e515bed3b9c9f317a10e3bb927798dfc` |
| `acceptance-summary.json` | `5dba16b344eba51a7daaefaab5e00d214d27af190fab30aa2ca3511f3c604171` |
| `SHA256SUMS` | `04d30c51b21390d07b2e4ca4ea9b64e26cd9e3a556e6379322ec7981e84e4e44` |

[Python a2说明](https://github.com/FangcunMount/reliable-messaging/releases/tag/python/v0.2.0a2)登记两组矩阵：Python3.11/MySQL8.0.36/Go1.25.9与Python3.13/MySQL8.4/Go1.25.12，每组49源码测试、49已安装wheel测试和2个Go真实存储预算测试，均无失败/错误/跳过，使用NSQ1.3.0。这里引用其发布证据范围，不重新宣称宿主业务或生产已通过。

Go mq-ai.2 Release附同一acceptance-summary与SHA256SUMS；其他已核验Go Release没有额外下载附件。这不影响固定tag作为Go模块版本，也不证明任何宿主镜像已构建。

## 维护规则

新增版本记录语言包版本、远端tag固定commit、Release类型/时间、实际附件及摘要、兼容变化和验证范围。候选CI artifact只记录候选，不登记成公开Release；主线有新实现时也不要移动旧版本的源码链接。

宿主采用条目必须带其自身源码/部署证据和采集日期；源码依赖与运行二进制/镜像分开。若无法核验发布或采用，标明未知而不是沿用早期“未发布”或“已上线”文本。升级前回链[兼容与升级](../03-维护与验证/兼容与升级.md)，机制决策回链[独立语言版本](../04-设计决策/004-语言与包独立版本.md)。
