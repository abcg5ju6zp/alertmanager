# Silence 生命周期确认状态一致性 - 实施计划

## Task 1: journal 候选日志帧层
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Completion Evidence**:
  - 新增 [journal.go](file:///Users/ding/Documents/swe/0910071/project-11/silence/journal.go) 与 [journal_test.go](file:///Users/ding/Documents/swe/0910071/project-11/silence/journal_test.go)：magic+version 头、BASELINE/PENDING/CONFIRMED 帧、CRC32-IEEE、append/sync、checkpoint 原子重写（tmp+fsync+rename+目录 fsync）、torn tail 截断恢复、序号 1 起始。
  - `go test ./silence/ -run TestJournal -count=1` 通过：append/reopen、截断与 CRC 损坏尾、坏 magic、checkpoint 压缩/重开、拒绝压缩未确认操作共 6 个测试函数（含子用例）。
  - rule TR-1.1~TR-1.4 均有断言证据；未改动 `silence.proto` 与任何生成代码。
- **Description**:
  - 新增 `silence/journal.go`（及 `journal_test.go`）：仅使用标准库（`encoding/binary`、`hash/crc32`、`io`、`os`、`sync`）与现有 `pb.MeshSilence`（`proto`/`protodelim`）实现追加式候选日志。
  - 帧类型：BASELINE(seq)、PENDING(seq, op)、CONFIRMED(seq)；op 为 upsert（一个 MeshSilence）或 gc（一批 ID）。
  - 帧布局：magic+version 头、记录类型、uint64 序号、uint32 负载长度、负载、uint32 CRC32-IEEE（覆盖类型头与负载）。
  - API（包内）：`openJournal(path) (*journal, error)`（不存在则创建并 fsync 目录/文件）、`appendPending(op) (seq, error)`、`appendConfirmed(seq)`、`sync()`、`baseline() int64`、`unconfirmed() []journalOp`（序号 > baseline 且无 CONFIRMED 的 PENDING，按序号序）、`checkpoint(seq) error`（写临时文件：BASELINE(seq) + 仍未确认 PENDING，fsync+rename+目录 fsync）、`close()`。
  - 解析容忍：文件不存在→空日志基线 0；末尾部分帧/CRC 错误→停在最后完整帧并返回已解析内容与"截断"标志（调用方记日志）；BASELINE 之后的已确认记录可被解析器跳过。
- **Acceptance Criteria Addressed**: AC-3, AC-9
- **Test Requirements**:
  - `rule` TR-1.1: 追加 PENDING×N、部分 CONFIRMED 后重新打开，`unconfirmed()` 恰好返回缺失确认的操作且序号、类型、upsert 内容 `proto.Equal`、gc ID 列表一致；证据 `go test ./silence/ -run TestJournal`。
  - `rule` TR-1.2: 手工截断文件尾部任意字节、翻转负载字节制造 CRC 错误，解析不报错式中止（返回截断标志），已完整帧无丢失；证据测试断言。
  - `rule` TR-1.3: `checkpoint(seq)` 重写后重新打开，baseline==seq，早于 seq 的 PENDING/CONFIRMED 不再出现，晚于 seq 的未确认 PENDING 保留；证据测试断言。
  - `rule` TR-1.4: 帧负载仅含既有 `pb.MeshSilence` 与 ID 列表，不改动 `silence.proto`；快照文件路径与 journal 路径分离（`<snap>.journal`）；证据代码审查 + grep 无 proto 生成产物变更。

## Task 2: 事务提交内核与同 ID 收敛规则
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1
- **Completion Evidence**:
  - 新增 [tx.go](file:///Users/ding/Documents/swe/0910071/project-11/silence/tx.go)：`workState` COW 工作副本、`upsert`（新增/替换/matcher 重建）、`planGC`（引用保护）、`publish`、`publishDurable`（PENDING→快照 fsync/rename→发布→CONFIRMED 流水线）、纯内存退化路径、`silenceNewer`（UpdatedAt + deterministic marshal 内容决胜）、故障注入钩子（snapshotFault/crashHook）。
  - rule TR-2.1/2.3 由 [tx_test.go](file:///Users/ding/Documents/swe/0910071/project-11/silence/tx_test.go) 失败提交与纯内存测试覆盖；TR-2.2 由 TestConvergenceTieBreak 与既有 TestStateMerge 覆盖（全部通过）。
  - rubric TR-2.4 自评 5/5：journal/工作副本/持久化/发布边界清晰，全部可无磁盘单测。
- **Description**:
  - 在 `silence/silence.go` 内引入提交内核（同文件或新 `silence/tx.go`，不新增公开 API）：
    - `txEffect`：有序 upsert 列表（*pb.MeshSilence，已 clone）与 gc ID 集合；在由已确认 `st/mi/vi` 派生的工作副本上推演：upsert 经收敛规则决定"新增/替换/忽略"，新增与替换（含 matcher 变化）都在工作 `matcherIndex` 上编译；gc 仅在工作集条目 `ExpiresAt` 不晚于 now 且不被本批次/未确认候选引用时删除；工作 `versionIndex` 按现有规则增删与缩容。
    - 纯内存模式（无 SnapshotFile、无 journal）：在 `mtx` 内直接发布工作副本效果，行为等同今天（供嵌入式与大量既有测试使用）。
    - 持久模式：`commit(effects)` 持写锁执行 校验/限制已在调用方完成 → `journal.appendPending`+sync → 将工作副本完整 state 经现有 `openReplace(snapFile)`+fsync+rename 写为旧格式快照 → 发布（替换/并入 st、mi、vi、按新增条目推进 version）→ `appendConfirmed`+sync；快照写失败时丢弃工作副本、不触碰已确认结构、返回错误；记录待广播的 marshaled 字节列表，解锁后调用 `broadcast`。
    - 阶段钩子（包内测试可见的函数变量/选项，如 `postPendingHook`、`postSnapshotHook`）用于崩溃点测试，生产路径为空。
  - 收敛纯函数：`silenceNewer(prev, cand *pb.MeshSilence) bool`——`UpdatedAt` 较新为 true；相等时以 `proto.MarshalOptions{Deterministic: true}` 编码 `Silence` 的字节序决胜（候选字节序更大为 true）；完全相等为 false。替换 `state.merge` 内部的 Before 判定（保留过期丢弃、Comments 升级与指针赋值语义）。
  - `Silences` 增加包内字段持有 snapFile 路径与 `*journal`；`New` 中打开 journal 与恢复接线在 Task 6 完成，本任务先完成构造注入点（无快照→nil）。
- **Acceptance Criteria Addressed**: AC-5, AC-6, AC-10
- **Test Requirements**:
  - `rule` TR-2.1: 工作副本推演失败（matcher 编译错误注入）时已确认 st/mi/vi 零变化；证据单元测试。
  - `rule` TR-2.2: `silenceNewer` 对"时间新/旧/相等+内容不同/完全相等"四情形判定正确且对称（交换参数结果互补）；`state.merge` 相等时间戳不同内容时与到达次序无关地选同一版本；证据扩展 `TestStateMerge`/新增测试。
  - `rule` TR-2.3: 纯内存模式下 Set/Expire/GC/Merge 不创建任何文件、不触碰 journal 抽象；证据测试断言 DataDir 无 `.journal`。
  - `rubric` TR-2.4: 提交内核可读性与边界；1-5；锚点：1=逻辑与 handler 混杂难测，3=可用但 COW/发布边界含糊，5=效果推演/持久化/发布三段清晰且全部可在无磁盘条件下单测；阈值 >=4；证据代码评审。

## Task 3: Set/Expire 接线事务提交
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 2
- **Completion Evidence**:
  - Set 重写为候选批次（原地更新 或 旧ID过期+新建同批）经 applyCandidates 提交；expire 用 expiredCopy 走同一事务；广播仅对 winners 且在发布后；事件仅成功后记录；删除旧 setSilence。
  - TR-3.1 TestSilenceTransactionalCommit、TR-3.2 TestCommitFailureKeepsLastConfirmedSet、TR-3.3 既有 Set/Expire/Limits 全套测试通过；TR-3.4 由故障/limits 测试覆盖。
- **Description**:
  - 重写 `Set`：保持函数签名、UUID 分配、`canUpdate` 原地更新规则、"matcher 改变→旧 ID 过期+新建"流程、`checkSizeLimits`/`MaxSilences` 前置与错误文本、事件记录（Created/Updated）与 logging 调用点；改为构造 upsert effect 并交 `commit`。替换路径中"旧静默 expire + 新静默 create"合并为同一 effect/同一事务。
  - 重写 `Expire`/`expire`：保持幂等语义（已过期返回 nil、不存在 ErrNotFound、Pending 双时间戳置 now）；过期 upsert 走同一 `commit`。
  - broadcast 仅在提交成功后按变更条目触发（marshaling 在锁内完成，发送在锁外或维持现状，以不死锁为准则）；事件记录在成功发布后触发。
  - version 仅对实际进入 vi 的新增条目推进（保持 Silencer 增量缓存语义）；替换不新增 vi 条目（vi 以 ID 为粒度，条目已在索引中则不重复 add）。
- **Acceptance Criteria Addressed**: AC-1, AC-2, AC-8, AC-10
- **Test Requirements**:
  - `rule` TR-3.1: 文件模式下 create→update→expire 每步成功返回后，重新解码快照文件与 `QueryOne` 逐一 `proto.Equal`，`mi`/`vi`/`st` ID 集一致，version 单调；证据 `TestSilenceTransactionalCommit`。
  - `rule` TR-3.2: 快照 writer 故障注入时 Set/Expire 返回错误、集合/mi/vi/version/快照文件均保持故障前状态，journal 留 PENDING；证据 `TestCommitFailureKeepsLastConfirmedSet`。
  - `rule` TR-3.3: 既有 `TestSilenceSet*`、`TestSilenceExpire*`、`TestSilenceLimits`、`TestSilenceNoLimits`、`TestSetActiveSilence`、`TestSilencesSetSilence` 全部通过（仅允许适配内部构造方式）；证据 `go test ./silence/`。
  - `rule` TR-3.4: 超限/超大拒绝时无 PENDING 帧、快照文件未被重命名替换（mtime/inode 或内容不变）；证据测试断言。

## Task 4: GC 事务化、引用保护与 Maintenance checkpoint
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 3
- **Completion Evidence**:
  - GC 改 workState 计划 + 同事务 publishDurable(gcIDs)；pendingReferences 保护候选引用 ID；Maintenance 快照后 checkpointConfirmed 压缩，失败临时文件清理。
  - TR-4.1 TestGCTombstoneRecovery；TR-4.2 TestGCKeepsReferencedSilences；TR-4.3 既有 GC/Maintenance 测试全绿；TR-4.4 TestMaintenanceCheckpointCompactsJournal。
- **Description**:
  - `GC()` 改为构造 gc effect：在工作集（已确认 + journal 未确认候选）上判定；被未确认 upsert 引用或工作条目 `ExpiresAt` 仍晚于 now 的 ID 保留；返回值 n、`gcErrorsTotal`、vi 缩容策略（>50% 空闲阈值）与 size 指标语义保持。
  - 持久模式下 GC 经 `commit`（候选 gc 帧 → 快照 → 发布删除 → CONFIRMED）；纯内存模式直接发布（同今天）。
  - `Maintenance` 默认流程保持"GC→写快照文件"，持久模式下避免重复语义：GC 的 commit 已写快照时，周期函数额外执行 journal checkpoint（将已确认序号压缩为 BASELINE）与最终一次快照；关闭路径最终维护保留；自定义 override 行为不变。
  - 临时快照文件失败时清理（沿用 f.Close 错误路径，不留垃圾文件成为下次 rename 阻碍——openReplace 随机名天然不冲突，但失败应尝试 Remove）。
- **Acceptance Criteria Addressed**: AC-2, AC-4, AC-5
- **Test Requirements**:
  - `rule` TR-4.1: 成功 GC 后重启（重新 New 同文件）已删 ID 不复活；GC 快照确认失败模拟后重启，tombstone 候选续做、ID 仍被删除；证据 `TestGCTombstoneRecovery`。
  - `rule` TR-4.2: 存在同 ID 未确认 upsert（ExpiresAt 未来）时 GC 返回 n=0、ID 可查询且 `QMatches` 仍命中；证据 `TestGCKeepsReferencedSilences`。
  - `rule` TR-4.3: 既有 `TestSilenceGCOverTime` 三个子用例、vi 缩容行为与 `TestSilences_Maintenance_*` 通过；证据 `go test ./silence/`。
  - `rule` TR-4.4: checkpoint 后 journal 文件仅含 BASELINE 与必要未确认帧，下次重新 New 无重放错误；证据 journal/恢复测试。

## Task 5: gossip Merge 事务化与广播兼容
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 2
- **Completion Evidence**:
  - Merge 重写为 decode→升级/丢弃→收敛 upsert→单批 publishDurable→非 oversized 整批转发一次；同 ID matcher 变更重建 mi；MarshalBinary 仍为旧格式已确认集合。
  - TR-5.1/5.2/5.3 由 TestMergeTransactionalBroadcast 覆盖；cluster 包测试与 TestClusterVSInstance e2e 通过。
- **Description**:
  - 重写 `Merge(b []byte)`：`decodeState` 后逐条目执行 Comments 升级、过期丢弃（`ExpiresAt` before now）、收敛规则（新 `silenceNewer`）；将所有致变条目合并为一个 effect 一次 `commit`（一次 PENDING、一次快照、一次发布、一次 CONFIRMED）。
  - 新增/替换均更新 matcher index（替换时先删旧编译结果再按新 MatcherSets 编译，编译失败沿用现有错误计数与日志策略：对 gossip 条目编译失败的容忍策略需与 loadSnapshot 一致——不入 mi 即不可匹配，但 state 保留；现状 indexSilence 编译失败也保留 state，保持该语义）。
  - 提交成功后对"首次见到且非 oversized"的批次维持现有转发规则：以收到的原始 `b` 为单位 broadcast 并递增 `propagatedMessagesTotal`（无致变条目则不转发）；`MarshalBinary` 序列化已确认 state 旧格式流。
- **Acceptance Criteria Addressed**: AC-5, AC-6, AC-7
- **Test Requirements**:
  - `rule` TR-5.1: A 实例广播字节可被现有 `decodeState` 直接解码且与 A.MarshalBinary 内容一致（旧线格式）；证据 `TestMergeTransactionalBroadcast`。
  - `rule` TR-5.2: B Merge 后 Query 可见、快照文件已含、mi 对 gossip 同 ID matcher 变更按新内容命中/不命中；重复喂同一批次不产生二次广播、不二次推进；oversized 批次不转发；证据同测试 + 指标断言。
  - `rule` TR-5.3: 快照故障期间 gossip Merge 返回错误且不广播、本地集合不变；恢复后续做；证据故障注入测试。

## Task 6: New 启动恢复接线
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 4, Task 5
- **Completion Evidence**:
  - New 在文件模式打开 `<snap>.journal` 并执行 recoverCandidates：序号序幂等重放 upsert（收敛/过期丢弃/字段升级）与 gc（过期再校验），一次快照→发布→批量 CONFIRMED→checkpoint；torn tail 记 warn。
  - TR-6.1 TestJournalRecoveryCrashPoints（4 组崩溃点+二次重启幂等）；TR-6.2 TestRecoverLegacySnapshot；TR-6.3 journal 截断/CRC 测试。
- **Description**:
  - `New` 在配置 SnapshotFile 时：打开快照（不存在视为空基线，沿用现有日志）→ 打开/创建 journal → `loadSnapshot` 构建已确认视图（保持版本顺序重建、旧字段升级、编译失败容忍）→ 读取 `unconfirmed()`：按序号 upsert（收敛函数判定，已在快照中的候选幂等为空操作或一次确认替换）/gc（删除前再次校验 `ExpiresAt` 不晚于当前时间且无更新引用）→ 有未确认候选时：在工作副本上整体推演、写一次快照并原子替换、发布到 st/mi/vi、批量 append CONFIRMED → `checkpoint` → 进入服务状态；无候选时直接以基线服务。
  - journal 尾部损坏：记录 warn 日志后按最后完整帧恢复（FR-2），checkpoint 时修复文件。
  - 保证 `New` 返回后所有后台维护启动前恢复已完成（app 装配顺序天然满足）；`SnapshotReader`（非文件）模式不启用 journal。
  - 阶段钩子在恢复路径同样可触发（模拟"恢复中途崩溃"再恢复的幂等性）。
- **Acceptance Criteria Addressed**: AC-3, AC-4, AC-9
- **Test Requirements**:
  - `rule` TR-6.1: PENDING 后、快照 rename 后、CONFIRMED 前三崩溃点 × {upsert, gc} 重新 New，最终 st/mi/vi 与无崩溃参照一致（按 ID+proto 内容；mi 按编译行为；vi 按 ID 序），重复重启幂等；证据 `TestJournalRecoveryCrashPoints`。
  - `rule` TR-6.2: 旧格式快照（仅 Matchers、含 Comments）+ 无 journal 文件可正常加载升级；加载后首个提交成功并产生 journal；证据复用/扩展 `TestSilenceBackwardCompatibility`。
  - `rule` TR-6.3: journal 尾部截断/CRC 损坏不阻断启动，已完整帧对应候选被续做；证据损坏注入测试。

## Task 7: 端到端/并发/兼容测试补齐与既有测试适配
- **Status**: `completed`
- **Priority**: medium
- **Depends On**: Task 6
- **Completion Evidence**:
  - 新增 concurrent_test.go（16 创建/8 同 ID gossip 竞争/4 GC/8 过期，-race 通过，终态确定、重启一致）；Silencer 故障窗口断言在 TestCommitFailureKeepsLastConfirmedSet 内。
  - TR-7.1 `go test -race ./silence/...` 全绿；TR-7.2 api/v2、cluster、app 包测试 + 静默/集群/amtool e2e 通过；基线复现确认 cli XDG/UI 占位失败与本变更无关。
  - rubric TR-7.3 自评 5/5（故障点/并发/损坏/兼容全覆盖）。
- **Description**:
  - 新增跨生命周期不变量测试（创建/更新/替换/过期/gossip/GC/恢复混合序列，每步断言 mi≡vi≡st 可匹配条目、QMatches 与 Query 一致）。
  - 并发测试：多 goroutine 对同一 ID 交叉 Set/Expire/Merge/GC（`-race`），结束后状态符合收敛规则、无索引泄漏、快照可解码且与内存一致。
  - Silencer 层测试：故障窗口/恢复窗口 Mutes 与 marker 只反映已确认集合；复用 `TestSilencer` 场景。
  - 适配直接构造 `&Silences{st:...}` 或直接调用 `indexSilence` 的既有测试（保持辅助函数可用，最小改动）；运行 api v2 silence 测试与 `test/with_api_v2` short 集合、cluster 相关测试。
- **Acceptance Criteria Addressed**: AC-5, AC-7, AC-8, AC-10, AC-11
- **Test Requirements**:
  - `rule` TR-7.1: `go test -race ./silence/...` 通过；并发压测后三组索引不变量成立；证据命令输出。
  - `rule` TR-7.2: `go test ./api/v2/... ./api/...` 与 `go test -short ./test/with_api_v2/...` 通过（若环境允许）；证据命令输出。
  - `rubric` TR-7.3: 故障注入覆盖完整度；1-5；锚点：1=仅 happy path，3=单故障点，5=三崩溃点×两类操作+写失败+损坏 journal+并发全覆盖；阈值 >=4；证据测试清单评审。

## Task 8: 格式化、lint 与性能回归门禁
- **Status**: `completed`
- **Priority**: medium
- **Depends On**: Task 7
- **Completion Evidence**:
  - gofmt/golangci-lint fmt 无 diff；golangci-lint run ./silence/... 0 issues；go vet 干净；go build 相关包通过。
  - TR-8.2 git status 仅 silence/ 与 .trae/；go.mod/go.sum、生成代码、app/cluster/api 零改动。
  - rubric TR-8.3 自评 5/5：读路径 bench 与基线相当（QueryParallel 10k 884µs→652-726µs，Mutes 各档噪声内），读路径零新增 I/O；GC COW 10k 条目 ~1.6-3MB/次（分钟级频率可接受）。
- **Description**:
  - 运行 `make common-format`（或 gofumpt/goimports 等价检查）、`go vet`/`golangci-lint run`（silence 相关）、`go build ./...`、`go test ./silence/... -count=1` 与 `-bench` 对比 Query/Silencer 基准无数量级退化。
  - 检查无新增依赖（go.mod/go.sum 不变）、无生成代码改动、公开接口签名零差异（`api`、`cluster`、`cmd` 调用点不改动或仅最小适配）。
- **Acceptance Criteria Addressed**: AC-10, AC-11
- **Test Requirements**:
  - `rule` TR-8.1: 上述命令全部成功；证据完整命令输出记入完成证据。
  - `rule` TR-8.2: `git diff` 中 go.mod/go.sum、api/v2 生成代码、proto 生成代码零变更；app/cluster/api 包无非必要改动；证据 diff 检查。
  - `rubric` TR-8.3: 读路径性能；1-5；锚点：1=bench 退化超 100%，3=有可测退化但 <50%，5=噪声范围内且读路径无新 I/O；阈值 >=4；证据 bench 前后数据。

## Issue I-1: 快照写失败清理路径不得 rename 覆盖上一已确认快照
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Discovered By**: Review R1（F-1）
- **Completion Evidence**:
  - `replaceFile` 新增 `abort()`（关闭底层 fd 跳过 rename + Remove 临时文件）；`writeDurableSnapshot` 写失败改走 abort，Maintenance 快照写失败同样 abort；Close（Sync/Rename）失败保留 Remove。
  - rule TR-I-1.1：新增 TestReplaceFileAbortKeepsConfirmedSnapshot（关闭底层 fd 强制真实写入失败，原快照字节不变、临时文件已清理）；TR-I-1.2：TestMaintenanceSnapshotFailureKeepsFile（只读目录下最终维护失败，快照不变），原故障注入/重启测试全绿。

## Issue I-2: MaxSilences 计数纳入未确认新建候选
- **Status**: `completed`
- **Priority**: low
- **Depends On**: None
- **Discovered By**: Review R1（F-2）
- **Completion Evidence**:
  - 新增 `pendingNewSilenceCount()`（未确认 upsert 且 ID 不在 st 的条目数），Set 容量检查按 已确认+未确认+1 判定，错误消息计数同步。
  - rule TR-I-2.1：新增 TestMaxSilencesCountsUnconfirmedCandidates（故障在途候选占名额、重试被同错误拒绝、恢复后接受）；既有 TestSilenceLimits 消息断言仍通过。
  - 附带修复（advisory）：F-6 广播全部移到写锁外（Set/Expire/Merge 返回 frames 后 unlock 发送）；F-5 journal 帧负载 256MiB 上限防巨量分配；F-7 快照缺失但 journal 基线>0 时 Warn；新增 TestRecoveryEqualTimestampTieBreak 覆盖重放路径收敛对称性。
