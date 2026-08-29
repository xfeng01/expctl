# expctl 中文使用指南

expctl 用 Git 仓库中的请求、回执和结果文件交接异步实验：实验设计者不必与集群操作者同时在线，也能确认运行了哪一版代码、使用了哪些参数，并追溯结果来源。

expctl 不替代 SLURM、工作流引擎或实验跟踪平台。它只负责让实验交接可验证、可审计：

```text
发布请求 → 提交作业并生成回执 → 收集日志和指标 → 人工审阅并记录结论
```

本文约定：

- **发布请求**：将请求文件提交并推送到 Git 仓库。
- **提交作业**：运行 `expctl submit`，通过 `sbatch` 提交到 SLURM。
- **回执**：`<root>/results/<id>/receipt.json`。回执生成后，请求文件不得再修改；重跑或修改参数必须使用新 ID。

`<root>` 默认为 `expctl/`，可通过 `expctl.toml` 中的 `paths.root` 修改。

## 一、安装

所有机器都需要 Git 和 Python 3.11 或更高版本。负责运行作业的集群环境还需要可用的 `sbatch`、`squeue`、`sacct`、`scancel`；启用节点预算时还会使用 `scontrol`。

推荐使用 `uv` 安装：

```bash
uv tool install git+https://github.com/xfeng01/expctl
```

更新，以及查看当前安装的版本：

```bash
uv tool upgrade expctl
expctl --version
```

若目标机器不能安装包，可只复制 `src/expctl/core.py`，然后运行：

```bash
python core.py <command>
```

`core.py` 仅依赖 Python 标准库；Git 和相应的 SLURM 命令仍须存在。所有命令都应在 Git 工作树内运行。

## 二、初始化项目

在项目仓库中运行：

```bash
expctl init
```

该命令只补齐缺失项，不覆盖已有文件：

```text
expctl.toml                  # 仓库级策略
expctl/
├── requests/               # 实验请求
├── results/                # 回执、日志、指标和报告
└── templates/request.toml  # 请求模板
```

随后编辑 `expctl.toml`，至少确认允许的 SLURM 参数、节点上限、共享运行时目录和 worktree 位置。若修改 `paths.root`，再次运行 `expctl init` 会在新位置补齐目录，但不会移动或删除旧目录。

可随时检查仓库配置、目录权限和集群依赖：

```bash
expctl doctor
```

`doctor` 分别报告仓库是否可用、当前机器是否具备 POSIX 锁和 SLURM 命令。任一必需检查失败时退出码为 `1`；使用 `--json` 可供脚本读取。

将 `expctl.toml`、请求、模板和结果元数据纳入版本控制。不要提交数据集、模型权重、缓存或密钥。expctl 只读写文件，不会自动执行 `git add`、`git commit` 或 `git push`。

## 三、创建并发布实验请求

1. 提交并推送本次实验所需的脚本、配置和评测代码。
2. 从模板创建请求；命令会自动填写 ID、当前 commit、branch 和 worktree 名称：

   ```bash
   expctl new 20260901-lr-sweep
   ```

   若工作树存在未提交改动，`new` 默认拒绝创建，因为这些内容不属于固定的 `HEAD`。应先提交或暂存；只有明确接受该风险时才使用 `--allow-dirty`。

3. 编辑生成的请求，填写实验问题、判定规则、SLURM 脚本、输出和指标，然后验证：

   ```bash
   expctl validate 20260901-lr-sweep
   ```

4. 将请求文件提交并推送到 Git 仓库。

`validate` 会检查请求格式、固定提交是否存在，以及该提交中的作业脚本是否包含所有 `scheduler.required_script_lines`。回执生成前可以修正并重新发布请求；回执生成后必须保持文件字节不变。

## 四、提交、查看和收集

在集群上的项目仓库中运行：

```bash
git pull --ff-only
expctl list
expctl show <id>
expctl submit <id> --dry-run
expctl submit <id>
```

`expctl list` 在交互终端中自动显示按终端宽度对齐的表格，中文按实际显示宽度计算，过长标题以 `…` 截断；状态在支持颜色的终端中会着色。对于带有效作业号的 `submitted` 回执，它会把所有作业号合并为一次 `squeue` 查询，再把已离开队列的作业合并为一次 `sacct` 查询；数组任务同时存在多种状态时显示 `MIXED`。查询是只读的，不修改回执；若 SLURM 命令缺失、查询失败或部分作业无状态，受影响的行安全回退为 `submitted`，并且只在标准错误输出一条警告。输出到管道或文件时仍使用纯净、稳定的 TSV；JSON 同样只写入标准输出。可用 `--table`、`--tsv`、`--json` 强制格式，或用 `--no-color`（以及标准 `NO_COLOR` 环境变量）关闭颜色。

`list` 默认按 ID 从新到旧排列；可用 `--sort oldest` 反转。`--status running,failed` 按实时或回执状态筛选且不区分大小写，`--limit 20` 在筛选和排序后限制数量。相同 commit 和脚本的 Git 校验会在单次列表中复用。

面向生命周期的命令在交互终端中显示简洁摘要和建议的下一步；输出到管道或文件时保持 JSON，原始日志除外。需要在终端中获取 JSON 时使用命令支持的 `--json`。

`--dry-run` 会验证请求并输出固定提交、worktree 路径、`sbatch` 命令、声明的节点上限和从作业脚本推导出的节点上限。它不会创建 worktree、链接共享目录、检查运行时依赖或当前节点预算，也不会调用 `sbatch`。

正式提交时，expctl 依次执行：

1. 以独占方式创建 `preparing` 回执，阻止同一请求被并发提交；
2. 创建或验证属于当前仓库、位于固定提交且内容干净的 detached worktree；
3. 从固定脚本的 `#SBATCH --nodes`、`--array` 和 `%N` 节流值核验最坏并发节点数；
4. 链接 `runtime.shared_dirs`，并检查 `notes.requirements`；
5. 若启用节点预算，在本 Git checkout 内锁住“查询—提交”窗口并校验上限；
6. 清除宿主环境中的 `SBATCH_*` 选项，调用 `sbatch --parsable`，再原子写入作业号和请求哈希。

已有回执的请求不能再次提交。若 `sbatch` 没有返回可确认的作业号，回执会保留为 `submission_unknown`；此时必须先由操作者在调度器中对账，不能删除回执或自动重试。已确认作业需要再跑一次时使用 `expctl rerun <id>`（见第八节）。若需要让请求方立即看到作业号，应在提交成功后单独提交并推送 `receipt.json`。

查看状态：

```bash
expctl status <id>
expctl status <id> --watch
```

`in_queue: true` 表示 `squeue` 仍能查到该作业或其数组任务。作业离开队列，或 `squeue` 暂时不可用时，expctl 改用 `sacct` 返回状态和退出码；两者都不可用时返回 `UNKNOWN` 并保留错误详情。`--watch [SECONDS]` 默认每 5 秒刷新，直到终态；管道或 `--json` 模式每次输出一行 JSON。

运行中查看日志，或在收集后查看已归档日志：

```bash
expctl logs <id> --tail 100
expctl logs <id> --follow
```

运行时从回执核验的 worktree 读取 `outputs.log_glob`，收集后则从结果目录读取。若提交时指定了 `--worktree-root`，运行中查看日志也须传入同一目录；`--follow` 以 Ctrl+C 结束。

不再需要作业时先预览，再发出取消请求：

```bash
expctl cancel <id> --reason "superseded" --dry-run
expctl cancel <id> --reason "superseded"
```

`cancel` 只接受带确认作业号且尚未收集的回执。`scancel` 成功后，回执记录操作者、时间和原因，并进入 `cancel_requested`；实时状态随后会显示 `CANCELLED`。取消不会替代 `collect`，失败日志仍应收集并审阅。

确认作业结束且日志写入完成后收集结果：

```bash
expctl collect <id> [--worktree-root <dir>]
```

`collect` 会：

- 校验请求文件的 SHA-256 是否与回执一致；
- 根据请求重新计算 worktree 并与回执核对；若 `submit` 使用了 `--worktree-root`，`collect` 必须传入相同目录；
- 先确认作业已经离开 `squeue`，再将匹配 `outputs.log_glob` 且文件名不冲突的日志暂存并发布到 `<root>/results/<id>/logs/`；
- 从复制后的日志提取指标并写入 `metrics.json`，在回执中记录 `missing_metrics`；
- 将 `sacct` 结果写入回执，并把回执状态改为 `collected`。

`collect` 不会等待作业，也不会覆盖已经收集或残留的日志和指标。同一请求的收集过程使用互斥锁串行化；作业仍在队列、没有匹配日志、worktree 不一致、结果已存在或不同来源日志会使用同一目标文件名时都会报错。指标缺失不会阻止失败作业的证据收集，但会明确记录。完成后应审阅原始日志，按 `decision_rule` 写入 `<root>/results/<id>/report.md`，再提交并推送结果目录。

结果收集后可清理 detached worktree：

```bash
expctl clean <id> --dry-run
expctl clean <id>
```

`clean` 重新核对回执、固定提交、仓库归属和工作树内容，只允许 `collected` 回执，并拒绝仍被未收集 rerun 使用的 worktree。它只删除 worktree，不删除结果证据；共享目录的符号链接目标不会被删除。若提交时使用了自定义 worktree 根目录，这里也必须传入相同的 `--worktree-root`。

## 五、请求文件参考

| 字段 | 约束与含义 |
|---|---|
| `id` | 形如 `20260901-lr-sweep`；必须与文件名一致，并匹配 `^[0-9]{8}-[a-z0-9][a-z0-9-]*$` |
| `title` | 非空的实验标题 |
| `question` | 本实验要消除的不确定性 |
| `decision_rule` | 哪种结果会改变下一步决策 |
| `code.commit` | 完整的 40 位 Git 提交哈希；这是实际运行的代码版本 |
| `code.branch` | 仅供阅读的分支标签，不参与检出或校验 |
| `code.worktree` | 实验 worktree 的普通目录名（不能是 `.` 或 `..`）；不得与主仓库重叠 |
| `slurm.script` | 固定提交中的仓库相对路径，不得越出仓库 |
| `slurm.max_concurrent_nodes` | 本请求允许的最坏并发节点数，至少为 1，且不能超过非零的 `scheduler.max_total_nodes`；固定脚本必须显式写出数字形式的 `#SBATCH --nodes`，数组范围和 `%N` 节流也必须可静态解析，推导值不得超过这里的声明 |
| `slurm.env` | 传给 `sbatch --export` 的环境变量；名称须匹配 `[A-Za-z_][A-Za-z0-9_]*` 且不能是 `GROUPS`，值必须是字符串且不能包含逗号或换行 |
| `outputs.log_glob` | 相对于实验 worktree 的 glob，不得越出 worktree；唯一支持的占位符是必须出现的 `{job_id}` |
| `outputs.metrics` | 非空指标名数组；可提取的名称须匹配 `[A-Za-z_][A-Za-z0-9_.-]*`，对应值必须是独占一行的数字 |
| `notes.requirements` | 可选的 worktree 相对路径数组；正式提交前逐项检查是否存在 |
| `notes.instructions` | 可选的操作说明；expctl 不解释其内容 |
| `rerun_of` | 可选，顶层字段；由 `expctl rerun` 写入，指向被重跑的请求 ID |
| `rerun_reason` | 可选，顶层字段；`expctl rerun --reason` 记录的重跑原因 |

支持 `name: 1.2`、`name = 1.2` 和 `name  1.2` 三种完整行。指标名必须完全匹配；单个日志中同名指标出现多次时保留最后一个值，未找到的指标不写入结果。`metrics.json` 按日志文件分组。

复用 worktree 时，expctl 会验证它属于当前 Git 仓库、`HEAD` 等于固定提交，而且除 `runtime.shared_dirs` 外没有 tracked、untracked 或 ignored 变更。同一 worktree 若仍有已记录的排队/运行作业或结果不确定的提交，也不能再次使用。

## 六、配置参考

```toml
version = 1

[paths]
root = "expctl"

[scheduler]
required_script_lines = ["#SBATCH -p my-partition"]
max_total_nodes = 4

[runtime]
shared_dirs = [".venv", "data", "runs", "logs"]
create_missing = ["runs", "logs"]

[worktree]
root = ".."
```

- `paths.root`：请求、结果和模板的仓库相对目录。
- `scheduler.required_script_lines`：必须出现在固定脚本可生效的头部区域；其中的 `#SBATCH` 选项还会作为 `sbatch` 命令行参数再次传入，防止后续脚本选项覆盖分区、账号或 QOS 策略。
- `scheduler.max_total_nodes`：当前用户的跨作业节点上限；`0` 表示禁用检查。
- `runtime.shared_dirs`：从主工作树链接到实验 worktree 的顶层目录名。源目录不存在时跳过。
- `runtime.create_missing`：提交前可在主工作树中自动创建的共享目录，必须是 `shared_dirs` 的子集。
- `worktree.root`：实验 worktree 的父目录；相对路径以仓库根目录为基准。

节点预算按以下方式计算：`RUNNING`、`COMPLETING`、`CONFIGURING`、`SUSPENDED` 和 `RESIZING` 作业按实际节点名去重，`PENDING` 作业按 `squeue` 报告的节点数累加，再加上请求中的 `slurm.max_concurrent_nodes`。数组任务使用 `squeue -r` 展开，因此该检查有意偏保守。expctl 会在同一 Git checkout 内串行化预算查询和 `sbatch`，但不同 clone 或人工提交之间的硬上限仍应由 SLURM QOS/account 策略保证。`--skip-node-check` 会跳过此检查，只应在操作者明确授权后使用。

## 七、命令与状态

| 命令 | 作用 |
|---|---|
| `expctl init` | 补齐配置和 `paths.root` 指定的目录结构 |
| `expctl doctor [--json]` | 检查仓库结构、worktree 位置、POSIX 锁和 SLURM 命令 |
| `expctl new <id> [--allow-dirty] [--json]` | 从模板创建请求并填写 Git 元数据；默认拒绝未提交改动 |
| `expctl list [--table\|--tsv\|--json] [--status <states>] [--sort newest\|oldest] [--limit <n>] [--no-color]` | 列出请求及实时状态；默认从新到旧，终端表格，管道 TSV |
| `expctl validate <id>` | 验证请求、固定提交和作业脚本策略 |
| `expctl show <id>` | 将验证后的请求输出为 JSON |
| `expctl submit <id> [--json]` | 准备 worktree、检查依赖和预算，并提交 SLURM 作业 |
| `expctl submit <id> --dry-run` | 验证并预览提交，不创建资源或调用 SLURM |
| `expctl submit <id> --worktree-root <dir>` | 本次提交使用指定的 worktree 父目录 |
| `expctl status <id> [--watch [<seconds>]] [--json]` | 查询一次状态，或定时刷新直到终态；`squeue` 不可用时回退到 `sacct` |
| `expctl logs <id> [--tail <n>] [--follow] [--worktree-root <dir>]` | 查看运行中或已收集的日志；默认显示末尾 100 行 |
| `expctl cancel <id> [--reason <text>] [--dry-run] [--json]` | 预览或调用 `scancel`，并在回执中记录取消审计信息 |
| `expctl collect <id> [--worktree-root <dir>] [--json]` | 复制日志、提取指标并更新回执；自定义 worktree 根目录须与提交时一致 |
| `expctl clean <id> [--dry-run] [--worktree-root <dir>] [--json]` | 结果收集后验证并删除 detached worktree，不删除结果证据 |
| `expctl rerun <id> [--as <new-id>] [--reason <text>] [--json]` | 把已提交的请求复制为新 ID（默认 `<id>-r2`、`-r3`……），写入 `rerun_of`，供再次提交；commit 和 worktree 不变 |

`expctl list` 显示的标准状态：

```text
requested  只有有效的请求文件
preparing  已独占请求，正在执行提交前检查
submitting 已开始调用 sbatch，尚未持久化确认的作业号
submission_unknown  sbatch 结果不确定，必须人工对账且不能自动重试
submitted  已生成回执
cancel_requested  已调用 scancel，等待 SLURM 确认终态
collected  已执行 collect
invalid    请求验证失败，或回执不是有效的 JSON
```

若 `submitted` 回执含有有效作业号且 SLURM 可查询，`list` 会临时用大写的调度状态替换显示值，例如 `PENDING`、`RUNNING`、`COMPLETING`、`COMPLETED`、`FAILED`、`CANCELLED`、`TIMEOUT` 或 `OUT_OF_MEMORY`。数组任务有多种状态时显示 `MIXED`；需要逐任务详情时使用 `expctl status <id>`。

“已审阅”不是工具状态；结论由操作者写入 `report.md` 或项目自己的实验记录。

## 八、异常处理与自动化约定

- **请求在提交作业后被修改**：恢复与回执中 `request_sha256` 完全一致的文件；否则 `collect` 会拒绝执行。
- **回执损坏、不可读或缺少有效 worktree**：从 Git 或可靠备份恢复回执并先与 SLURM 对账。在回执恢复前，expctl 会保守阻止 worktree 复用和清理，不会假设该回执与目标 worktree 无关。
- **提交结果不确定**：保留 `submission_unknown` 回执，根据其中的时间、命令和操作者信息查询 SLURM；确认是否存在作业前不得删除回执、执行 `rerun` 或再次提交。
- **节点预算不足**：等待已有作业释放节点后重试。不要通过修改旧请求或静默重提绕过限制。
- **作业不再需要**：先执行 `expctl cancel <id> --dry-run` 核对作业号，再正式取消；作业离开队列后仍应运行 `collect` 保存失败或取消证据。
- **作业失败、超时或被抢占**：保留原回执和日志，在报告中记录失败原因。代码不需要改动时（抢占、节点故障、共享环境未就绪），运行方直接执行 `expctl rerun <id> --reason "preempted"`，提交生成的 `<id>-r2.toml`，再 `expctl submit <id>-r2`；commit 和 worktree 不变，位于固定提交上的已有 worktree 会被复用。需要改代码时由请求方修正、提交，并发布固定新提交的新请求。任何情况下都不要删除回执重提。
- **日志未找到**：核对作业是否结束、`outputs.log_glob` 是否包含正确的 `{job_id}` 位置，以及日志是否写在固定 worktree 中。
- **自动化或 AI 操作**：先执行并审阅 `--dry-run`，再明确授权正式提交。expctl 不会自动重试、重新提交、提交 Git 变更或判定实验结论。
