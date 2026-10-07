# Silence 生命周期确认状态一致性 - 产品需求文档

## Overview
- **Summary**: 将 Alertmanager 静默规则（silence）的创建、更新、过期、删除（GC）及 matcher index 变更改造为"可恢复候选 → 持久快照确认 → 原子发布"的单状态机事务，消除 GC、快照写入或重启故障期间 API 状态、matcher index 与磁盘快照之间的分歧。
- **Purpose**: 值班人员创建/修改静默后，任何故障点都不得出现"API 显示已过期但仍抑制告警"或"重启后已删除静默复活"；每条静默在查询面、通知管道、HA 广播与磁盘之间以同一个确认版本推进。
- **Target Users**: 值班工程师（通过 UI/amtool/API 管理静默）、接收告警的通知管道、运行 HA 集群的运维人员。

## Goals
- 每条 silence 生命周期变更（创建、原地更新、替换、过期、GC 删除、gossip 合并）都以同一事务边界推进：候选先持久化（可恢复），持久快照 fsync 确认后才对 `Query`/`Silencer.Mutes` 可见并向 HA 广播。
- 任一步失败或进程中止：内存中保留上一已确认集合，磁盘上存在完整有效的上一快照；重启后只继续（replay 完成）未确认候选，已确认数据不重复应用。
- 同一 ID 的并发/跨节点更新按 `UpdatedAt` 时间决胜，时间戳相等时按内容确定性收敛。
- GC 不得删除仍被未确认候选或更新版本引用的规则。
- 保持兼容：`Silencer`（Muter 接口与缓存版本语义）、v2 HTTP API、HA gossip 线格式与全量状态交换、`MaxSilences`/`MaxSilenceSizeBytes` 限制、旧快照文件格式；无快照文件时维持现有纯内存行为。

## Non-Goals
- 不修改 OpenAPI/HTTP 接口形状、不改 `silence.proto` 线格式、不重新生成 proto 代码。
- 不改造 nflog、dispatch、inhibit 等其他子系统。
- 不提供跨集群的分布式事务（不引入 Raft/两阶段提交）；HA 仍为 gossip 最终一致，但本地提交具备持久化原子性。
- 不改变 silence 的业务语义（canUpdate 规则、过期状态机、retention 计算、UUID 分配规则）。
- 不新增第三方依赖。

## Background & Context
现状证据（[silence/silence.go](file:///Users/ding/Documents/swe/0910071/project-11/silence/silence.go)）：

1. [Set](file:///Users/ding/Documents/swe/0910071/project-11/silence/silence.go#L844-L932) 与 [expire](file:///Users/ding/Documents/swe/0910071/project-11/silence/silence.go#L979-L1003) 持锁原地修改 `st`/`mi`/`vi`/`version` 并立即 broadcast；快照仅由 [Maintenance](file:///Users/ding/Documents/swe/0910071/project-11/silence/silence.go#L586-L652) 周期写盘与关闭时补写。写操作成功返回与磁盘持久化之间无任何关联。
2. [Maintenance 默认流程](file:///Users/ding/Documents/swe/0910071/project-11/silence/silence.go#L595-L613) 先 GC（改内存）后写快照；快照失败时内存已删、磁盘仍存，重启经 [loadSnapshot](file:///Users/ding/Documents/swe/0910071/project-11/silence/silence.go#L1235-L1271) 重新加载已删除静默。
3. `st`（state map）、`mi`（matcherIndex）、`vi`（versionIndex）是三套独立结构，靠写路径手工同步；[state.merge](file:///Users/ding/Documents/swe/0910071/project-11/silence/silence.go#L1343-L1362) 对相等 `UpdatedAt` 不做内容决胜，跨节点到达次序不同可能不收敛；gossip 替换同 ID 条目时不重建 matcher index。
4. 快照写盘已采用 temp 文件 + fsync + rename（[openReplace/replaceFile](file:///Users/ding/Documents/swe/0910071/project-11/silence/silence.go#L1437-L1466)），具备原子替换基础；快照体为长度分隔的 `MeshSilence` 记录流（[decodeState](file:///Users/ding/Documents/swe/0910071/project-11/silence/silence.go#L1377-L1397)）。
5. `Silences` 仅在配置 `SnapshotFile` 时运行于持久化模式；大量测试与嵌入式用法以无快照方式构造（纯内存），改造后必须保持该退化模式可用。

## Functional Requirements

- **FR-1 事务提交流程**：每个变更批次（Set、Expire、GC、gossip Merge）在写锁内依次完成：①校验/限制检查/matcher 编译，在由"已确认集合 + 未确认候选"派生的工作副本上计算效果；②候选记录追加至 journal 并 fsync（可恢复）；③将变更后的完整状态以旧快照格式写入临时文件、fsync、原子 rename（持久快照确认）；④一次性发布 `st`/`mi`/`vi`/`version` 变更（可见性点）；⑤写入 CONFIRMED 记录并 fsync；⑥释放锁后进行 HA broadcast。任一步失败：已确认集合不变，API 返回错误，候选留待恢复继续。
- **FR-2 journal（候选日志）**：在快照文件旁新增 journal 文件（路径由 SnapshotFile 派生），记录 BASELINE/PENDING/CONFIRMED 三类带序号、长度与 CRC 校验的帧；PENDING 负载承载 upsert（MeshSilence）或 GC（ID 批次）操作。追加写、每次确认 fsync；尾部截断/CRC 错误时停在最后完整帧；Maintenance 在快照确认后压缩 journal（重写为 BASELINE + rename）。
- **FR-3 启动恢复**：`New` 加载快照作为已确认基线后读取 journal：序号不大于 BASELINE 的记录忽略；未 CONFIRMED 的 PENDING 按序号顺序幂等重放（upsert 走时间/内容收敛，GC 仅删除仍满足过期条件的 ID），完成后重写快照、补 CONFIRMED、压缩 journal。恢复完成前不服务写操作（构造未完成不返回对象）。
- **FR-4 查询/通知只读已确认集合**：`Query`/`QueryOne`/`CountState`/`Version`/`Snapshot`/`MarshalBinary` 与 `Silencer.Mutes` 在任何时刻只观察已发布集合；`mi` 与 `vi`/`st` 对可匹配条目严格一致（加载时编译失败的静默沿用现有容忍策略：不入 state、不入 vi、不产生 mi）。同 ID 内容被替换（含 matcher 变化）时 mi 条目同步重建。
- **FR-5 同 ID 并发收敛**：收敛规则为 `UpdatedAt` 较新者胜；`UpdatedAt` 相等时按确定性编码（deterministic proto marshal）的字节序决胜，内容相等则为空操作；该规则同时用于本地提交、journal 重放与 gossip Merge，保证不同节点、不同到达次序最终一致。本地 Set 的原地更新仍受 canUpdate 约束且总是使用当前时间戳。
- **FR-6 GC 引用保护**：GC 基于"已确认 + 未确认候选"工作集判定；凡被未确认 upsert 候选引用、或工作集中条目的 `ExpiresAt` 已被刷新为未来时间的 ID 一律不删。GC 删除与快照写入同一事务：快照未确认则删除不可见；崩溃恢复时删除候选被继续执行，已删除静默不复活。
- **FR-7 HA 兼容**：gossip 帧仍为长度分隔 `MeshSilence` 流，`Merge([]byte)`/`MarshalBinary()`/`SetBroadcast` 签名与语义保持；入站批次经同一提交流程（一次 journal、一次快照、一次发布），去重后才转发传播（保留 oversized message 不转发规则）；全量状态交换只含已确认集合；broadcast 发生在快照确认与发布之后。
- **FR-8 限制保持前置**：`MaxSilences` 与 `MaxSilenceSizeBytes` 在候选持久化之前按工作集计算并返回与今天相同的错误；被拒操作不留候选、不推进版本、不写快照。恢复重放的是既往已接受候选，不受事后收紧的限制阻断。
- **FR-9 旧格式兼容与模式退化**：无 SnapshotFile/SnapshotReader 时 journal 禁用，行为等同当前纯内存模式；旧版本写出的快照（含 `Matchers` 旧式字段、`Comments` 升级）可直接加载；新写出的快照仍为纯 `MeshSilence` 记录流（含首个 matcher set 回填 `matchers` 的兼容写入），旧版本 Alertmanager 可解码。journal 文件为本地实现细节，不进入任何网络协议。

## Non-Functional Requirements
- **NFR-1 读路径无磁盘 I/O 放大**：查询与 Mutes 仍只持读锁访问内存结构，fsync/快照写只发生在写事务与 Maintenance；读路径现有大 O 复杂度与版本增量缓存语义不变。
- **NFR-2 写延迟**：文件模式下每次成功 Set/Expire 承受一次 journal fsync + 一次快照 fsync（静默数据量通常为 KB~低 MB 级）；失败路径不留半成品文件（临时文件清理或被下次 rename 覆盖）。
- **NFR-3 可观测性**：保留全部现有 silence 指标与错误计数；GC/快照耗时指标仍记录；新增 journal/恢复失败路径必须有结构化错误日志。
- **NFR-4 可测性**：快照写入器、journal、时钟均可在测试中注入/故障化；关键崩溃点可通过故障注入覆盖。
- **NFR-5 代码规范**：遵循 AGENTS.md（testify/require、go-kit log→slog、`%w` 包装、godot 注释、gofumpt/goimports），公开标识符注释完整。

## Constraints
- **Technical**: Go 1.26；仅用标准库实现 journal 帧（建议 `encoding/binary` + `hash/crc32` + `protodelim/proto`）；不得手改生成代码；单进程内写操作由 `mtx` 串行化。
- **Business**: 对外行为（API、gossip 协议、快照格式、指标名）兼容；持久化模式下磁盘故障将导致写 API 显式失败（相对今天"内存成功、静默丢失"是可接受且被要求的语义收紧）。
- **Dependencies**: 无新增模块依赖；app 装配方式（[app.go](file:///Users/ding/Documents/swe/0910071/project-11/app/app.go#L311-L335)）原则上零改动，journal 路径由 SnapshotFile 派生。

## Assumptions
- 本地文件系统 rename 为原子操作、`fsync` 提供崩溃持久性（与现有 `replaceFile` 假设一致）。
- 同一 DataDir 不会被两个 Alertmanager 进程并发以读写方式打开（沿用现有单写者假设，不引入文件锁）。
- "持久快照确认"解释为：变更后完整状态通过 temp+fsync+rename 落到既有快照文件；journal 提供候选持久性与恢复续做能力；Maintenance 快照同时承担 journal 检查点。
- 进程内快照写失败时返回错误且不发布；候选仅在重启恢复时继续（用户明确要求"恢复后只继续未确认操作"）。

## Acceptance Criteria

### AC-1: 创建/更新/过期以快照确认为可见边界
- **Type**: `rule`
- **Given**: 以 SnapshotFile 构造的 Silences，初始为空
- **When**: 成功调用 `Set` 创建静默、再次 `Set` 原地更新 EndsAt、再 `Expire`
- **Then**: 每次成功返回后：磁盘快照文件（重新打开解码）内容与 `Query`/`QueryOne` 可见内容逐一 `proto.Equal`；`mi` 与 `vi` 条目集与 `st` 一致；`Version()` 在每次发布后单调增加；调用返回前快照 rename 已完成（观测文件 mtime/解码内容）
- **Pass Condition**: 三次操作后分别解码快照文件，状态与内存查询结果完全一致，且无任何额外条目
- **Evidence**: 新增 `TestSilenceTransactionalCommit`（文件模式，逐步断言快照内容/mi/vi/version）

### AC-2: 快照写入失败时保留上一有效集合
- **Type**: `rule`
- **Given**: 文件模式 Silences 已有一条已确认静默 S1；注入快照写入故障（如目录权限/磁盘满/fsync 失败注入点）
- **When**: 调用 `Set` 新建 S2 与调用 `GC`
- **Then**: 两次调用均返回错误；`Query` 仍只返回 S1；`QMatches` 与 `Silencer.Mutes` 不感知 S2；`mi`/`vi` 不含 S2；磁盘快照仍为故障前完整内容；journal 中存在 S2 的 PENDING 而无 CONFIRMED
- **Pass Condition**: 故障注入期间与注入撤除后，查询结果恒等于故障前集合；候选记录状态可从 journal 解码验证
- **Evidence**: 新增 `TestCommitFailureKeepsLastConfirmedSet`（故障注入 writer + journal 解码断言）

### AC-3: 崩溃于各阶段后恢复只续做未确认操作
- **Type**: `rule`
- **Given**: 可在提交各阶段（PENDING fsync 后、快照 rename 后、CONFIRMED 前）模拟进程中止并重新 `New` 同一 SnapshotFile
- **When**: 分别在三个崩溃点重启，随后执行 Query 并解码快照与 journal
- **Then**: 重启后状态等价于候选操作全部完成（upsert 生效、GC 删除生效）；快照完整可解码且包含最终集合；journal 中无残留 PENDING（已全部 CONFIRMED/压缩）；重复重启结果幂等；CONFIRMED 后崩溃不产生重复效果/重复版本
- **Pass Condition**: 三个崩溃点 × {upsert, GC} 六组场景恢复后 `st/mi/vi` 与"无崩溃完成"的参照对象一致（按 ID 与 proto 内容比较，version 数值不要求跨重启相等）
- **Evidence**: 新增 `TestJournalRecoveryCrashPoints`（阶段钩子注入 + 重新构造 Silences）

### AC-4: 已删除/已 GC 静默在重启后不复活
- **Type**: `rule`
- **Given**: 文件模式下一条静默超过 retention 且存在同 ID 的未确认 upsert 候选场景
- **When**: 执行 GC（成功）后重启；另在 GC 快照确认失败后重启
- **Then**: 成功 GC 后重启不出现该静默；GC 快照失败重启后，tombstone 候选被续做（静默仍被删除）；被未确认候选或更新版本（ExpiresAt 在未来）引用的 ID 不被 GC 计数、不被删除
- **Pass Condition**: 两种重启路径下 Query 均不含已删 ID；引用保护场景中 `GC()` 返回删除数为 0 且该 ID 仍可查询、matcher 仍可匹配
- **Evidence**: 新增 `TestGCTombstoneRecovery` 与 `TestGCKeepsReferencedSilences`

### AC-5: matcher index 与状态永不分歧
- **Type**: `rule`
- **Given**: 包含创建、同 ID 更新（matcher 不变）、替换（matcher 改变→新 ID+旧 ID 过期）、gossip 同 ID 内容替换（matcher 改变）、过期、GC 的混合操作序列，含故障恢复
- **When**: 每步后枚举 `mi`、`vi`、`st`，并用 `QMatches` 对受影响标签集查询
- **Then**: `mi` 的键集 = `vi` 中对应 state 条目的 ID 集（加载时编译失败者除外，沿用旧容忍规则）；替换同 ID 后 mi 编译结果来自新内容（旧 pattern 不再命中、新 pattern 命中）；`QMatches` 结果与 `Query(QState...)` 内容一致
- **Pass Condition**: 全序列每一步不变量成立；gossip 替换 matcher 的场景新 matcher 生效
- **Evidence**: 新增 `TestMatcherIndexAtomicAcrossLifecycle`；保留并适配既有 GC/缓存泄漏测试

### AC-6: 同 ID 更新按时间与内容收敛
- **Type**: `rule`
- **Given**: 同一 ID 的两个 MeshSilence 版本：UpdatedAt 不同、UpdatedAt 相同但内容不同、UpdatedAt 与内容均相同
- **When**: 以两种到达次序分别执行 state 合并（本地 Merge 与 journal 重放路径均覆盖）
- **Then**: 时间新者胜；时间相同内容不同时两节点选择同一确定性胜出版本（与到达次序无关）；完全相同时无状态变化、无额外版本推进
- **Pass Condition**: 六种组合（3 情形 ×2 次序）结果对称；确定性决胜可由独立的规范化字节比较复算
- **Evidence**: 扩展 `TestStateMerge` / 新增 `TestConvergenceTieBreak`

### AC-7: HA 广播与全量状态交换兼容
- **Type**: `rule`
- **Given**: 两个文件模式 Silences 实例，各自挂接可观测的 broadcast 捕获函数
- **When**: A 提交 Set/Expire（含快照故障）并将广播字节喂给 B 的 Merge；再调用 B 的 MarshalBinary 经 decodeState 交换回 A
- **Then**: 广播字节为旧格式 MeshSilence 长度分隔流（旧解码器可解）；B 合并后经同一事务持久化且再次 Query 一致；故障提交不产生广播；同内容重复 Merge 不重复广播、不重复计传播指标；MarshalBinary 只含已确认集合
- **Pass Condition**: 线格式可被现有 decodeState 解码且 proto 内容一致；去重/不广播/oversized 规则与现状一致
- **Evidence**: 新增 `TestMergeTransactionalBroadcast`；既有 cluster e2e（test/with_api_v2）保持通过

### AC-8: 限制与公开 API 行为兼容
- **Type**: `rule`
- **Given**: 配置 MaxSilences=N、MaxSilenceSizeBytes=M 的文件模式 Silences
- **When**: 超限创建/超大创建；以及通过 v2 HTTP handler 正常创建、查询、删除
- **Then**: 拒绝路径返回与今天相同的错误信息，journal 无候选、快照文件不变、version 不增；接受路径 HTTP 状态码/响应体（含 SilenceID）不变；`Silencer` 仍满足 Muter/mem 回调接口
- **Pass Condition**: 既有 `TestSilenceLimits`/`TestSilenceNoLimits`/`TestSilenceSet*` 与 api v2 silence 测试通过（允许内部实现适配，不断言私有结构无关细节）
- **Evidence**: `go test ./silence/... ./api/v2/...` 及适配后的既有测试

### AC-9: 旧快照格式与无 journal 模式兼容
- **Type**: `rule`
- **Given**: ①旧二进制/旧测试夹具写出的仅含 `Matchers` 与 `Comments` 字段的快照；②无 SnapshotFile 的纯内存 Options
- **When**: 分别 `New` 加载与执行 Set/Expire/GC/Query；再用新代码写出快照并用现有 `decodeState` 与旧字段断言解码
- **Then**: 旧快照正常升级加载（matcher sets/comment 升级规则不变）；纯内存模式不创建任何 journal 文件、行为与今天一致；新快照流不含任何 journal 专属帧，旧 AM 版本可解码
- **Pass Condition**: `TestSilenceBackwardCompatibility` 等既有兼容测试通过；新快照字节流中每条记录均可解为 MeshSilence
- **Evidence**: 既有兼容测试 + 新增纯内存模式"无副作用文件"断言

### AC-10: 实现质量与读路径性能
- **Type**: `rubric`
- **Dimension**: 代码可维护性与读路径效率
- **Scale**: 1-5
- **Anchors**: 1 = 事务逻辑散落、读路径被 fsync 阻塞或公开接口被破坏；3 = 功能正确但提交/恢复逻辑耦合在 silence.go 单文件、锁粒度含糊；5 = 提交内核/journal/恢复边界清晰、读路径零新增 I/O 与锁竞争、公开接口与指标零破坏、注释与错误包装符合规范
- **Pass Threshold**: >= 4
- **Evidence**: 代码评审 + 既有 `TestSilencesQuery`/`Silencer` 基准与 bench 对比（`go test -bench` 无数量级退化）

### AC-11: 测试与工程门禁
- **Type**: `rubric`
- **Dimension**: 验证覆盖与工程纪律
- **Scale**: 1-5
- **Anchors**: 1 = 仅 happy path；3 = 主要故障点有测试但缺少并发/崩溃注入；5 = 单元（journal 帧、截断/CRC、checkpoint）、事务（各生命周期）、崩溃恢复（全阶段注入）、并发收敛、GC 引用、HA/旧格式兼容均有自动化测试，且 `make common-format`、`go test ./silence/... ./api/... ./test/...`（short）与 lint 相关项通过
- **Pass Threshold**: >= 4
- **Evidence**: 测试输出、bench 输出、format/lint 命令结果记录于 tasks 完成证据

## Open Questions
- 无阻断性开放问题。以下为已决策假设（见 Assumptions），评审时可否决：
- [ ] 确认"每次写操作一份完整快照 + fsync"的持久化成本可接受（journal 候选 + 完整快照双 fsync；以正确性优先）。
- [ ] 确认 journal 文件名由 `<SnapshotFile>.journal` 派生，不新增 Options 字段。
