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

所有机器都需要 Git 和 Python 3.11 或更高版本。负责提交作业的集群环境还需要可用的 `sbatch`、`squeue`、`sacct`；启用节点预算时还会使用 `scontrol`。

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

随后编辑 `expctl.toml`，至少确认允许的 SLURM 参数、节点上限、共享运行时目录和 worktree 位置。若修改 `paths.root`，需将 `init` 生成的目录移动到新位置。

将 `expctl.toml`、请求、模板和结果元数据纳入版本控制。不要提交数据集、模型权重、缓存或密钥。expctl 只读写文件，不会自动执行 `git add`、`git commit` 或 `git push`。

## 三、创建并发布实验请求

1. 提交并推送本次实验所需的脚本、配置和评测代码。
2. 将 `<root>/templates/request.toml` 复制为 `<root>/requests/<id>.toml`，填写请求字段。
3. 使用完整的 40 位提交哈希固定代码版本：

   ```bash
   git rev-parse HEAD
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

`--dry-run` 会验证请求并输出固定提交、worktree 路径、`sbatch` 命令和声明的节点数。它不会创建 worktree、链接共享目录、检查运行时依赖或节点预算，也不会调用 `sbatch`。

正式提交时，expctl 依次执行：

1. 创建或验证位于固定提交上的 detached worktree；
2. 链接 `runtime.shared_dirs`，并检查 `notes.requirements`；
3. 若启用节点预算，查询当前用户的 SLURM 作业并校验上限；
4. 调用 `sbatch --parsable`，将作业号和请求哈希写入回执。

已有回执的请求不能再次提交：`submit` 会报出那次提交的作业号、提交人和 `sacct` 终态。需要再跑一次时用 `expctl rerun <id>`（见第八节），不要删除回执。若需要让请求方立即看到作业号，应在提交成功后单独提交并推送 `receipt.json`。

查看状态：

```bash
expctl status <id>
```

`in_queue: true` 表示 `squeue` 仍能查到该作业或其数组任务。作业离开队列后，expctl 改用 `sacct` 返回状态和退出码；查询失败或无记录时返回 `UNKNOWN`。

确认作业结束且日志写入完成后收集结果：

```bash
expctl collect <id>
```

`collect` 会：

- 校验请求文件的 SHA-256 是否与回执一致；
- 将匹配 `outputs.log_glob` 的日志复制到 `<root>/results/<id>/logs/`；
- 按日志文件提取指标并写入 `metrics.json`；
- 将 `sacct` 结果写入回执，并把回执状态改为 `collected`。

`collect` 不会等待作业，也不会判断日志是否完整。没有匹配日志时会报错。完成后应审阅原始日志，按 `decision_rule` 写入 `<root>/results/<id>/report.md`，再提交并推送结果目录。

## 五、请求文件参考

| 字段 | 约束与含义 |
|---|---|
| `id` | 形如 `20260901-lr-sweep`；必须与文件名一致，并匹配 `^[0-9]{8}-[a-z0-9][a-z0-9-]*$` |
| `title` | 非空的实验标题 |
| `question` | 本实验要消除的不确定性 |
| `decision_rule` | 哪种结果会改变下一步决策 |
| `code.commit` | 完整的 40 位 Git 提交哈希；这是实际运行的代码版本 |
| `code.branch` | 仅供阅读的分支标签，不参与检出或校验 |
| `code.worktree` | 实验 worktree 的目录名；一个请求应使用一个唯一名称 |
| `slurm.script` | 固定提交中的仓库相对路径，不得越出仓库 |
| `slurm.max_concurrent_nodes` | 本请求最坏情况下的并发节点数，至少为 1，且不能超过非零的 `scheduler.max_total_nodes`；该值由请求者声明，工具不会从作业脚本推导 |
| `slurm.env` | 传给 `sbatch --export` 的环境变量；名称须匹配 `[A-Za-z_][A-Za-z0-9_]*` 且不能是 `GROUPS`，值必须是字符串且不能包含逗号或换行 |
| `outputs.log_glob` | 相对于实验 worktree 的 glob，不得越出 worktree，且必须包含 `{job_id}` |
| `outputs.metrics` | 非空指标名数组；可提取的名称须匹配 `[A-Za-z_][A-Za-z0-9_.-]*`，对应值必须是独占一行的数字 |
| `notes.requirements` | 可选的 worktree 相对路径数组；正式提交前逐项检查是否存在 |
| `notes.instructions` | 可选的操作说明；expctl 不解释其内容 |
| `rerun_of` | 可选，顶层字段；由 `expctl rerun` 写入，指向被重跑的请求 ID |
| `rerun_reason` | 可选，顶层字段；`expctl rerun --reason` 记录的重跑原因 |

支持 `name: 1.2`、`name = 1.2` 和 `name  1.2` 三种完整行。指标名必须完全匹配；单个日志中同名指标出现多次时保留最后一个值，未找到的指标不写入结果。`metrics.json` 按日志文件分组。

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
- `scheduler.required_script_lines`：必须在固定提交的作业脚本中完整、逐行匹配的文本。
- `scheduler.max_total_nodes`：当前用户的跨作业节点上限；`0` 表示禁用检查。
- `runtime.shared_dirs`：从主工作树链接到实验 worktree 的顶层目录名。源目录不存在时跳过。
- `runtime.create_missing`：提交前可在主工作树中自动创建的共享目录，必须是 `shared_dirs` 的子集。
- `worktree.root`：实验 worktree 的父目录；相对路径以仓库根目录为基准。

节点预算按以下方式计算：`RUNNING` 和 `COMPLETING` 作业按实际节点名去重，`PENDING` 作业按 `squeue` 报告的节点数累加，再加上请求中的 `slurm.max_concurrent_nodes`。数组任务使用 `squeue -r` 展开，因此该检查有意偏保守。`--skip-node-check` 会跳过此检查，只应在操作者明确授权后使用。

## 七、命令与状态

| 命令 | 作用 |
|---|---|
| `expctl init` | 补齐默认配置和目录结构 |
| `expctl list` | 列出请求及其仓库状态 |
| `expctl validate <id>` | 验证请求、固定提交和作业脚本策略 |
| `expctl show <id>` | 将验证后的请求输出为 JSON |
| `expctl submit <id>` | 准备 worktree、检查依赖和预算，并提交 SLURM 作业 |
| `expctl submit <id> --dry-run` | 验证并预览提交，不创建资源或调用 SLURM |
| `expctl submit <id> --worktree-root <dir>` | 本次提交使用指定的 worktree 父目录 |
| `expctl status <id>` | 查询队列；不在队列时查询记账记录 |
| `expctl collect <id>` | 复制日志、提取指标并更新回执 |
| `expctl rerun <id> [--as <new-id>] [--reason <text>]` | 把已提交的请求复制为新 ID（默认 `<id>-r2`、`-r3`……），写入 `rerun_of`，供再次提交；commit 和 worktree 不变 |

`expctl list` 显示的标准状态：

```text
requested  只有有效的请求文件
submitted  已生成回执
collected  已执行 collect
invalid    请求验证失败，或回执不是有效的 JSON
```

“已审阅”不是工具状态；结论由操作者写入 `report.md` 或项目自己的实验记录。

## 八、异常处理与自动化约定

- **请求在提交作业后被修改**：恢复与回执中 `request_sha256` 完全一致的文件；否则 `collect` 会拒绝执行。
- **节点预算不足**：等待已有作业释放节点后重试。不要通过修改旧请求或静默重提绕过限制。
- **作业失败、超时或被抢占**：保留原回执和日志，在报告中记录失败原因。代码不需要改动时（抢占、节点故障、共享环境未就绪），运行方直接执行 `expctl rerun <id> --reason "preempted"`，提交生成的 `<id>-r2.toml`，再 `expctl submit <id>-r2`；commit 和 worktree 不变，位于固定提交上的已有 worktree 会被复用。需要改代码时由请求方修正、提交，并发布固定新提交的新请求。任何情况下都不要删除回执重提。
- **日志未找到**：核对作业是否结束、`outputs.log_glob` 是否包含正确的 `{job_id}` 位置，以及日志是否写在固定 worktree 中。
- **自动化或 AI 操作**：先执行并审阅 `--dry-run`，再明确授权正式提交。expctl 不会自动重试、重新提交、提交 Git 变更或判定实验结论。
