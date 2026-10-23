# 版本与发布索引

本页记录可消费的固定语言版本、源码与发布资产。2026-10-20（UTC+8）新增Go指标并发修订预发布；Python继续使用原固定版本。宿主依赖、部署和业务验收由各宿主另行记录，历史条目保留其采集日期。

<!-- source-python-version: 0.4.0a3 -->

## Python telemetry 恢复候选（2026-10-23，未发布）

源码版本准备为 `0.4.0a3`，修正仅针对固定 Python 客户端 5.1.2 的存活生产者：收到 OK 或重启丢失 settings 的结构化 40015 心跳响应时，恢复该实例已关闭的 stream，或在活跃但未注册的 stream 上补发 settings。每端点只允许一个真实 settings 写入，重复请求合并；关闭后不再重建，等待真实在途写入及其 callback 结束。401/403 等拒绝不触发重建。复用既有 I/O loop/心跳，不新建 timer/thread，不修改事务 codec、wire、schema 或宿主执行器；Go 本批不重新发布。

[SDK 修正 PR51](https://github.com/FangcunMount/reliable-messaging/pull/51)及隔离原生故障门禁仍在验证，尚未创建此版本 tag 或 Release，也未声明已发布安装、实际宿主或生产验收。固定候选 SHA、完整 CI、wheel/sdist、摘要及重新安装证明必须在发布前齐备；既有 tag/资产不覆盖。进度以 [infra 总表](https://github.com/FangcunMount/infra/blob/main/docs/server/rocketmq-m3-execution.md)为准。

## 显式授权入口修订预发布（2026-10-22）

[Go v0.5.0-rocketmq-tx.3](https://github.com/FangcunMount/reliable-messaging/releases/tag/v0.5.0-rocketmq-tx.3)与[Python 0.4.0a2](https://github.com/FangcunMount/reliable-messaging/releases/tag/python/v0.4.0a2)均绑定源码[`8de893b`](https://github.com/FangcunMount/reliable-messaging/commit/8de893b367cc797580799abd8325f41dd62356db)，源码经PR48/49合并。可选AuthorizedSender的Retry/Replan共享既有准入、数据库预算和真实在途关闭记账；显式业务重试仍由宿主授权。原codec、wire、schema及底层客户端不变，旧版本和资产均保留。

最终源码[Go/race/lint](https://github.com/FangcunMount/reliable-messaging/actions/runs/39671831214)、[Python两档源码与wheel各132](https://github.com/FangcunMount/reliable-messaging/actions/runs/39671831148)、[三档MySQL/Mongo存储各25](https://github.com/FangcunMount/reliable-messaging/actions/runs/39671845590)及[基础设施兼容](https://github.com/FangcunMount/reliable-messaging/actions/runs/39671831227)全部通过，真实依赖矩阵零失败、零跳过。较早候选一次Go依赖代理HTTP/2下载失败已原样重跑成功，原失败记录随Release保留。

两档CI生成的wheel/sdist逐字节一致。下载正式资产并验证摘要后，在隔离Python3.13 wheel、Python3.11 sdist安装环境分别通过104单元测试；各28集成测试的真实依赖验收使用前述最终源码CI，不把本地排除当作执行通过。导入路径均在安装环境，客户端固定5.1.2，无SDK工作区或editable替代。发布Go模块177份选定Go源码/模块文件与固定SHA一致，在模块缓存通过transaction及transport/rocketmq的race，无本地replace。Go module Sum为`h1:25yHjaHAcZYjp96+iNVJyYbdnDPvR6p16J06KeG0sP4=`。

| Python资产 | SHA-256 |
| --- | --- |
| wheel | `570822f9ed51eee25001624858d30b7433468eb7da59e34348289c50a37cabb2` |
| sdist | `50d8baa88296aff035ac1c0978b4660c5c13cca8bfff011431385348c13bc692` |

Release保留初始源码/CI证明与SHA256SUMS，并以不同资产名追加[发布制品安装receipt](https://github.com/FangcunMount/reliable-messaging/releases/download/python/v0.4.0a2/published-installation-receipt.json)、安装测试报告及独立摘要；未替换既有资产或移动tag。宿主须锁定新URL/hash并独立执行接入验收。本次不是M3稳定发布、生产切换或system验收，进度回链[infra执行总表](https://github.com/FangcunMount/infra/blob/main/docs/server/rocketmq-m3-execution.md)。

## 历史Go修订预发布（2026-10-20）

[Go v0.5.0-rocketmq-tx.2](https://github.com/FangcunMount/reliable-messaging/releases/tag/v0.5.0-rocketmq-tx.2)于2026-10-20 05:33:30（UTC+8）发布，固定源码为[`8fb6fc57`](https://github.com/FangcunMount/reliable-messaging/commit/8fb6fc57a415dc11e27fc64d71722f9675838bc3)，已通过[PR46](https://github.com/FangcunMount/reliable-messaging/pull/46)合并。它修复Apache Go5.1.4私有driver中Broker指标回写替换meter指针与消费ACK hook并发读取的race；公开API、事务codec、wire、schema及预算不变。Python源码未变，继续固定`0.4.0a1`及原`bfaae2b`资产，不重新发布Python。

[Go1.25.9/1.25.12源码、race、lint](https://github.com/FangcunMount/reliable-messaging/actions/runs/39326653557)与[NSQ兼容](https://github.com/FangcunMount/reliable-messaging/actions/runs/39326653550)通过；[实际MySQL8.0.36/8.4.11、MongoDB及Go/Python恢复矩阵](https://github.com/FangcunMount/infra/actions/runs/39326382808)每档31项，均0失败/跳过。正式tag下载的Go模块213份选定源码文件逐字节匹配，并在模块缓存中通过meter race/TLS回归和实际SDK探针构建；不使用本地replace。Go module Sum为`h1:kBHB/1aG/fAB6BPlITOwRYIX9XazRisz0PgfwyJ1VFY=`。

Release附六个固定资产：两档实际SDK验收报告、Apache补丁manifest、module-proof、发布receipt及SHA256SUMS；发布后重新下载并逐项核对。`go-meter-release-receipt.json`摘要为`0109f872501e9990cb85551078482d3eb4d81140e05a84d2aea098318750c863`，`SHA256SUMS`摘要为`dfe13bbb09e676a05bc44a114f47a084737a6aecf1df05492e76cfe3a4fb1611`。已有tag/资产未改写。

这是M3接入使用的Go预发布修订；IAM/QS须锁定该版本并独立验收，不表示业务或生产已切换。M3稳定Go/Python同源码SHA发布仍由[执行总表](https://github.com/FangcunMount/infra/blob/main/docs/server/rocketmq-m3-execution.md)的T27负责。

## M2固定预发布（2026-10-19）

Go与Python事务SDK的两个tag均指向同一合并源码[`bfaae2b`](https://github.com/FangcunMount/reliable-messaging/commit/bfaae2b82ee0d1229b8c965fce472cdf5dec486f)，均为GitHub prerelease。事务入口、原业务事务witness、Checker及关闭契约见[事务SDK接入](../02-接入指南/RocketMQ事务SDK.md)。该M2批次Python源码版本为0.4.0a1；不从源码版本或标签推断宿主已采用。

| 版本 | 固定源码 | 发布时间（UTC+8） | 范围 |
| --- | --- | --- | --- |
| [Go v0.5.0-rocketmq-tx.1](https://github.com/FangcunMount/reliable-messaging/releases/tag/v0.5.0-rocketmq-tx.1) | `bfaae2b82ee0d1229b8c965fce472cdf5dec486f` | 2026-10-19 16:49:31 | Go sql/GORM/Mongo事务SDK；普通Publisher/旧Outbox保持独立 |
| [Python python/v0.4.0a1](https://github.com/FangcunMount/reliable-messaging/releases/tag/python/v0.4.0a1) | `bfaae2b82ee0d1229b8c965fce472cdf5dec486f` | 2026-10-19 16:49:37 | SQLAlchemy/MySQL事务SDK；不含Python Mongo |
| [Go v0.4.0-rocketmq.1](https://github.com/FangcunMount/reliable-messaging/releases/tag/v0.4.0-rocketmq.1) | `18c0c38b5869b60eac5ce238c15d5e5c4048eeea` | 2026-10-18 21:52:09 | 前序普通消息预发布 |
| [Python python/v0.3.0a1](https://github.com/FangcunMount/reliable-messaging/releases/tag/python/v0.3.0a1) | `18c0c38b5869b60eac5ce238c15d5e5c4048eeea` | 2026-10-18 21:52:14 | 前序普通消息预发布 |

M2两个Release各附相同的6个固定资产：wheel、sdist、uv.lock、transaction-acceptance.json、transaction-release-receipt.json及SHA256SUMS。服务器摘要与发布前bundle逐项一致；安装前下载对应资产并校验SHA256SUMS，不替换已发布资产或移动tag。本轮不发布PyPI。

| 资产 | SHA-256 |
| --- | --- |
| `fangcun_reliable_messaging-0.4.0a1-py3-none-any.whl` | `47c67e6037f427da4e27597ddaaac4fb4c38f8bc78c5c6aa4758618d53d05606` |
| `fangcun_reliable_messaging-0.4.0a1.tar.gz` | `a6d9e2a1931d9713136a49802eb95078f9f16ddc0f131e3bfe7999ab849f0206` |
| `transaction-release-receipt.json` | `8f7a49641dc39cbc7f0a29f32ca971d818ed0aa7542eb7e252968e25f7da9cfc` |
| `SHA256SUMS` | `03aa42ce9c7ea59e2c9ad25b2561c0be4411f5648489c33f5c216e3a8cc6ecb7` |

发布前精确SHA的[源码/race/lint](https://github.com/FangcunMount/reliable-messaging/actions/runs/39222098471)、[三档真实存储](https://github.com/FangcunMount/reliable-messaging/actions/runs/39222116696)、[双Python源码及wheel](https://github.com/FangcunMount/reliable-messaging/actions/runs/39222109646)、[既有NSQ集成](https://github.com/FangcunMount/reliable-messaging/actions/runs/39222098378)全部通过。Python两档各120源码+120已安装wheel，0失败/跳过；[真实Broker完整48项/档](https://github.com/FangcunMount/infra/actions/runs/39222207821)及[普通消息/raw20/Go19回归](https://github.com/FangcunMount/infra/actions/runs/39222207962)通过。独立wheel/sdist安装各17包文件与固定源码一致，依赖全部符合uv.lock，receipt随Release保存。

正式tag与下载制品的两档[安装及完整48项验收](https://github.com/FangcunMount/infra/actions/runs/39227465314)全部通过，各48项、0失败/跳过，进程组/容器/私有目录全部排空。Go从正式tag下载模块，在模块缓存逐字节核验211个文件并构建实际验收程序；Python wheel/sdist各17包文件匹配固定源码、运行依赖均符合下载的uv.lock。实际执行的Go二进制摘要与安装证明一致，Python从隔离已安装wheel加载，无本地replace/editable或工作区源码导入。[发布后安装证明](https://github.com/FangcunMount/infra/blob/main/docs/server/evidence/rocketmq-m2-published-installation-20261019.json)与[唯一M2进度总表](https://github.com/FangcunMount/infra/blob/main/docs/server/rocketmq-m2-execution.md)分别记录验收及任务合并状态。

固定Broker5.5.1、Go底层5.1.4加可复现私有补丁、Python底层5.1.2。Go启动Telemetry共享RetryPolicy导致的protobuf大小不匹配已在发布前修复，并通过确定性快照、race及实际TLS交换回归。Broker回查有有限窗口；消费者仍需幂等，SDK不重复业务/模型调用、不自动降级NSQ/Outbox。本文不记录M3生产切换或三宿主业务采用。

以下保留2026-10-06的早期版本和宿主采集快照；其中“当时最新”不是2026-10-19的当前发布结论。

## 2026-10-06历史采集：源码、标签、制品与宿主

| 层次 | 本次核对结果 | 结论边界 |
| --- | --- | --- |
| 文档重建前main源码 | [fd06495ca819124c73ffe31b20a5381c06885e87](https://github.com/FangcunMount/reliable-messaging/commit/fd06495ca819124c73ffe31b20a5381c06885e87)；Python源码版本 `0.2.0a2` | main存在实现不证明某个制品或生产宿主已采用；源码包版本见[pyproject](../../python/pyproject.toml) |
| 本地tag | 已见Go M2/M6/MQ预发布及Python三个预发布tag | 本地ref可能过期或与远端不同；本次另读取GitHub ref并解析annotated tag |
| GitHub Go Release | 当时已核验预发布 `v0.3.0-mq-ai.2` | 无Go v1.0 Release；Go module从固定tag消费，不要求另有二进制附件 |
| GitHub Python资产 | 当时已核验预发布 `python/v0.2.0a2`，有wheel与sdist | 仅核验GitHub资产元数据与Release说明；本次未重新下载安装包，也未核验PyPI发布 |
| IAM源码采用 | [c8fc1fe的go.mod](https://github.com/FangcunMount/iam/blob/c8fc1fe61a32056368148e232e96d32763c15ba5/go.mod)固定 `v0.3.0-m6.1` | 源码依赖，不是本次生产二进制采集 |
| QS源码采用 | [2ccc2de的go.mod](https://github.com/FangcunMount/qs-server/blob/2ccc2de44bbd45e26d29e7e130da518cc32426f0/go.mod)固定 `v0.3.0-mq-ai.2` | 源码依赖，不是本次逐实例部署采集 |
| qs-ai与今日生产采用 | 本轮未重新核验 | 由各宿主带采集日期的版本、配置与业务验收记录负责，不从SDK标签推断 |

源于早期阶段的“候选未发布”说明不再适用于已列出的GitHub Release。反之，GitHub存在预发布也不表示原Go M0～M6、宿主MQ切换或完整业务恢复已验收。

## Go早期版本记录（截至2026-10-06）

以下源码来自2026-10-06的远端tag采集；Release字段 `target_commitish=main` 不用作固定源码。时间均为UTC+8。

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

## Python早期版本记录（截至2026-10-06）

| Release | 固定源码 | 发布时刻 | 附件与合同 |
| --- | --- | --- | --- |
| [python/v0.1.0a1](https://github.com/FangcunMount/reliable-messaging/releases/tag/python/v0.1.0a1) | [fea9238](https://github.com/FangcunMount/reliable-messaging/commit/fea92389967076e3529289e80b1d96abef76dc73) | 2026-10-03 11:34:15 | wheel、sdist；原事务、原pending/delivered表、持久callback、显式周期Relay；无NSQ adapter |
| [python/v0.2.0a1](https://github.com/FangcunMount/reliable-messaging/releases/tag/python/v0.2.0a1) | [9f16fa3](https://github.com/FangcunMount/reliable-messaging/commit/9f16fa30dc43a50a2d4492f3e95c905a8f969331) | 2026-10-03 16:33:17 | wheel、sdist、provenance、SHA256SUMS；可选NSQ / protected wire / 独立回执型MySQL |
| [python/v0.2.0a2](https://github.com/FangcunMount/reliable-messaging/releases/tag/python/v0.2.0a2) | [8633ba5](https://github.com/FangcunMount/reliable-messaging/commit/8633ba585b5a1c8f95fb16b19469fbd0097611c7) | 2026-10-04 04:46:05 | wheel、sdist、acceptance-summary、SHA256SUMS；final ACK失败预算修复 |

当时a2的GitHub资产元数据摘要如下；旧资产保留在对应Release，不覆盖旧版本。安装前应下载对应资产并核对本地摘要。

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
