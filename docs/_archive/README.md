# 历史材料

本目录保存阶段评审、验证结果、故障观察和旧入口快照。它们只说明记录时的范围，不是现行能力、线上版本或项目完成度的依据。当前阅读入口是[文档导航](../README.md)。

2026-10-06 从 `fd06495` 基线迁移；仅调整必要的相对链接，保留原陈述、失败结果和限制。[迁移清单](manifest.json)记录原文件和归档文件的 SHA256。归档正文不参加现行结构规则，检查器仍核对清单及索引指向的文件。后续不得在原证据上累计今天的状态。

| 材料 | 使用边界 |
|---|---|
| [M2 评审](milestones/m2-review.md)、[证据](milestones/m2-evidence.md)、[故障矩阵](milestones/m2-fault-matrix.md)、[缺口](milestones/m2-acceptance-gaps.md) | 原隔离验证及故障观察；包括 NSQ abrupt kill 确认后丢失的反证 |
| [旧兼容说明](milestones/compatibility.md) | 原支持组合与阶段升级记录；当前规则见[兼容与升级](../03-维护与验证/兼容与升级.md) |
| [Signaling 退役](milestones/signaling-retirement.md) | 通用机制转移时的源码和验证记录 |
| [AI MQ 候选快照](milestones/ai-mq-candidate.md) | 形成过程；旧“尚未发布”不代表当前 Release 状态 |
| [阶段状态](milestones/status.md) | 历史里程碑快照，不继续更新完成比例 |
| [旧根入口](milestones/root-readme-20261003.md) | 其中的宿主部署信息为历史采集，不重新证明生产采用 |
| [Python 评审](milestones/python-release-review.md)、[M1 示例说明](milestones/examples-m1-readme.md) | 原版本和早期参考；当前接入与示例以现行正文为准 |
| [协议样例说明](milestones/contracts-README.md)、[MySQL](milestones/storage-mysql-README.md)、[Mongo](milestones/storage-mongo-README.md)、[NSQ](milestones/transport-nsq-README.md)、[Python 包](milestones/python-README.md)、[Python NSQ](milestones/python-NSQ.md) | 原包入口快照，保留早期样例形成过程、非 UTC 时区反证及候选范围 |

设计正文可链接原故障证据说明取舍，但必须能独立说明当前机制，不要求读者先理解实施过程。删除归档是单独变更，不能借文档整理批量抹去证据。

本次结构、迁移与验证范围见[文档体系建设记录](documentation-system-20261006.md)。
