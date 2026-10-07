# Silence 生命周期确认状态一致性 - Independent Review

- [ ] CP-R1: 快照确认是唯一可见边界（创建/更新/过期）
  - **Type**: `rule`
  - **Covers**: AC-1, TR-3.1
  - **Evidence**: Pending

- [ ] CP-R2: 提交失败保留上一已确认集合（API、matcher index、Silencer、磁盘）
  - **Type**: `rule`
  - **Covers**: AC-2, TR-3.2, TR-5.3
  - **Evidence**: Pending

- [ ] CP-R3: 各崩溃点恢复只续做未确认操作且幂等
  - **Type**: `rule`
  - **Covers**: AC-3, TR-6.1, TR-6.3
  - **Evidence**: Pending

- [ ] CP-R4: 删除不复活，GC 不删除被引用规则
  - **Type**: `rule`
  - **Covers**: AC-4, TR-4.1, TR-4.2
  - **Evidence**: Pending

- [ ] CP-R5: matcher index 与 state/version index 全生命周期一致
  - **Type**: `rule`
  - **Covers**: AC-5, TR-2.1
  - **Evidence**: Pending

- [ ] CP-R6: 同 ID 更新按 UpdatedAt 时间 + 确定性内容收敛
  - **Type**: `rule`
  - **Covers**: AC-6, TR-2.2
  - **Evidence**: Pending

- [ ] CP-R7: HA gossip 线格式、去重传播、全量交换兼容
  - **Type**: `rule`
  - **Covers**: AC-7, TR-5.1, TR-5.2
  - **Evidence**: Pending

- [ ] CP-R8: 限制前置与公开 API 行为兼容
  - **Type**: `rule`
  - **Covers**: AC-8, TR-3.3, TR-3.4
  - **Evidence**: Pending

- [ ] CP-R9: 旧快照格式与无 journal 纯内存模式兼容
  - **Type**: `rule`
  - **Covers**: AC-9, TR-6.2
  - **Evidence**: Pending

- [ ] CP-U1: 实现质量与读路径性能
  - **Type**: `rubric`
  - **Covers**: AC-10, TR-2.4, TR-8.3
  - **Scale**: 1-5
  - **Anchors**: 1 = 读路径被阻塞/公开接口破坏/事务边界混乱；3 = 功能正确但锁与恢复逻辑含糊；5 = 提交内核/持久化/恢复边界清晰、读路径零新增 I/O、接口指标零破坏
  - **Pass Threshold**: >= 4
  - **Evidence**: Pending

- [ ] CP-U2: 测试覆盖与工程门禁
  - **Type**: `rubric`
  - **Covers**: AC-11, TR-7.1, TR-7.2, TR-7.3, TR-8.1, TR-8.2
  - **Scale**: 1-5
  - **Anchors**: 1 = 仅 happy path；3 = 主故障点有测试但缺并发/崩溃注入；5 = journal 帧、全崩溃点、并发、损坏恢复、HA/旧格式覆盖且 lint/test/bench 门禁通过
  - **Pass Threshold**: >= 4
  - **Evidence**: Pending

## Review History

### Review R1
- **Result**: `fail`
- **Evidence**:
  - CP-R1 pass；CP-R2 **fail**（F-1）；CP-R3 pass（F-4/F-7 advisory 缺口）；CP-R4 pass（F-3 advisory）；CP-R5 pass；CP-R6 pass（重放 tie-break 独立断言缺口 advisory）；CP-R7 pass；CP-R8 pass（F-2 low actionable）；CP-R9 pass；CP-U1 4/5；CP-U2 4/5。
  - 门禁：`go test ./silence/ -count=1`、`-race`、`go test ./api/v2/... ./cluster/...`、`golangci-lint run ./silence/...`（0 issues）均通过；评审者另用 7 组 /tmp 对抗探针实证 F-1（写失败但 Close sync 成功时空文件 rename 覆盖有效快照）与 F-3/F-4/F-5 边界行为。
- **Actionable Findings**:
  - F-1（high）：`writeDurableSnapshot` 与 Maintenance 错误分支调用 `replaceFile.Close()`，其 Sync+Rename 语义会在写失败但 Close 成功时用空/半成品临时文件覆盖上一已确认快照，违反 FR-1/AC-2 磁盘不变量；`os.Remove(f.Name())` 在 rename 后删除的是不存在的临时名。修复：错误路径关闭底层 fd（不 rename）+ Remove 临时文件；新增真实 writer 失败测试。
  - F-2（low）：MaxSilences 仅按 `len(s.st)+1` 计算，未计入未确认新建候选；故障+重试可在恢复后超上限 1 条。
- **Advisory Findings（不阻断，择优修复）**:
  - F-3 CONFIRMED fsync 失败后本进程 pending 视图导致 GC 引用保护到重启才自愈（用户可观察影响为零）。
  - F-4 替换批次两条 PENDING 帧之间崩溃会只重放前缀（逐帧语义字面合规，建议未来单帧批次）。
  - F-5 journal 中部损坏丢弃后续完整帧（FR-2 仅承诺尾部容忍）；损坏长度头无上限可能巨量分配。
  - F-6 广播在写锁内执行（回调当前非阻塞、无死锁，但与 FR-1⑥"释放锁后广播"字面不符）。
  - F-7 CONFIRMED fsync 失败钩子缺失、重放 tie-break 无独立断言、快照文件被删但 journal 仍在时无 warn。
- **Resumption**: 修复 F-1、F-2（并顺手处理 F-5 长度上限、F-6 锁外广播、F-7 warn 三项低风险项），重跑四项门禁后发起 R2 新评审；F-3/F-4 记录为已知边界。

### Review R2
- **Result**: `fail`
- **Evidence**:
  - CP-R2/3/4/5/6/7/8/9 pass（F-1/F-2 修复经独立复核为真，F-5/F-6/F-7 已处理）；CP-R1 与 CP-R4 的重启一致性 **fail**（F-R2-1）；CP-U1 4/5、CP-U2 4/5。
  - 四项门禁全绿；评审者用公开 API 按 Maintenance 系统调用顺序手工交错，确定性实证数据丢失（CONFIRMED 静默被陈旧 rename 回滚）。
- **Actionable Findings**:
  - F-R2-1（high）：Maintenance 快照 rename/checkpoint 未与提交路径 rename 串行化（[silence.go doMaintenance]），交错可使已 CONFIRMED 静默被旧快照覆盖且 journal 不重放，重启永久丢失。要求"序列化→fsync→rename→checkpoint"纳入与提交互斥的临界区并补并发回归测试。→ Issue I-3。
- **Advisory Findings**: A-1 journal GC count 按剩余字节校验；A-2 pendingNewSilenceCount 去重；A-3 Sync 失败 fd 泄漏；A-4 jrnl 无 Close 生命周期（无正确性问题）；A-5 TestMaintenanceSnapshotFailureKeepsFile 未覆盖 abort 分支（abort 已被单测覆盖）；A-6 GC CONFIRMED 失败返回 (0,err) 与指标轻微过计数；A-7 F-3/F-4 已知边界确认；A-8 GC 全量 COW 成本（分钟级可接受）。
- **Resumption**: I-3 已修复（维护临界区整段 RLock；A-1/A-2/A-3 一并处理），新增确定性互斥测试（移除锁即失败）与并发快照写者回归；发起 R3。

### Review R3
- **Result**: `pass`
- **Evidence**:
  - 全部 checkpoint 通过：CP-R1~R9 pass；CP-U1 4/5；CP-U2 5/5。无 actionable findings。
  - I-3（F-R2-1）修复经锁序审查、形式化互斥论证与两组独立对抗探针确认真实闭合且无死锁/新交错窗口；R1 修复（F-1/F-2/A-1/A-3）复核仍成立。
  - 门禁：`go test ./silence/ -count=1` ok 3.512s；`go test -race ./silence/ -count=1` ok；`go test ./api/v2/... ./cluster/... ./app/...` 三包 ok；`golangci-lint run ./silence/...` 0 issues；`go vet`/gofmt 干净；`go test -short ./test/with_api_v2/...` ok；git status 仅 silence/ 与 .trae/，go.mod/生成代码/app/cluster/api 零改动。
  - 残留 advisory（不阻断，均已记录）：钩子协程断言卫生（实现者随后已改为缓冲通道回传）、maintenance abort 两条分支无直接测试、GC publish 失败返回 (0,err)、CONFIRMED fsync 失败为已知边界、写路径 COW 成本、loadSnapshot 编译失败容忍为存量行为、Merge 传播计数语义修正为有意变更。
- **Checkpoints**: 见上方 CP-R1..CP-U2 清单，最终均由 R3 独立证据支持。
