# expctl

`expctl` is a small Git-backed CLI for handing off asynchronous experiments.
An author publishes an immutable request; a runner reviews, submits, monitors,
and collects it. Git preserves the handoff and its audit trail.

It supports two execution backends: SLURM and direct execution on a Linux
compute node. A request pinned to SLURM can also be run on a single host with
`expctl submit --backend local`, which leaves the request, the receipt, and the
collected evidence identical. It is not a scheduler, workflow engine, or
experiment tracker.

[Changelog](CHANGELOG.md)

## Install

Every machine needs Git and Python 3.11 or newer. Direct execution requires a
Linux host with `/proc`, plus `bash` to run a SLURM script locally. The SLURM
backend also needs `sbatch`, `squeue`, `sacct`, and `scancel`; node-budget
checks use `scontrol`.

```bash
uv tool install git+https://github.com/xfeng01/expctl
```

Upgrade with `uv tool upgrade expctl`. For zero-install use, copy
`src/expctl/core.py` and run `python core.py <command>`; it uses only the
Python standard library.

Run all commands inside the target Git working tree.

## Commands

Day to day, two commands are enough: `expctl new <name>` starts a request,
and `expctl` with no command lists every experiment that is not reviewed yet,
each with the exact command it needs next:

```text
OPEN EXPERIMENTS
EXPERIMENT ID      STATUS     NEXT
-----------------  ---------  -----------------------------
20261014-lr-sweep  requested  expctl submit lr-sweep
20261012-decoder   RUNNING    expctl status decoder --watch
20261011-stagger   COMPLETED  expctl collect stagger
20261009-metric    collected  expctl report metric

3 reviewed experiments not shown: expctl list --status reviewed
Every command, by workflow stage: expctl --help
```

`expctl --help` groups the commands by workflow stage: the everyday
`new`, `submit`, `status`, `collect`, and `report`, then commands to inspect,
to cancel, repeat, or clean up, and to set up a repository.

Complete command reference:

```text
# Show every experiment that is not reviewed yet with the command it needs
# next; several requests to submit or jobs to collect also get the --all form.
expctl

# Initialize expctl in the current repository.
expctl init

# Check repository and backend readiness. --cluster is an alias for
# --backend slurm.
expctl doctor [--backend slurm|local] [--cluster] [--json]

# Create a request from the repository template and fill in Git metadata.
# A bare short name such as lr-sweep is prefixed with today's date. --from
# copies every setting of an existing request but pins the current HEAD.
expctl new <id> [--from ID] [--title TEXT] [--allow-dirty] [--json]

# List, filter, sort, and limit experiment requests.
expctl list [--table | --tsv | --json] [-l | --long] [--status STATUS[,STATUS...]] [--sort newest|oldest] [--limit N | --all] [--no-color]

# Describe one validated request. A terminal gets a summary of its question,
# decision rule, code, backend, outputs, and recorded state with next steps;
# redirected output or --json prints the request as JSON.
expctl show <id> [--json]

# Validate the request, pinned commit, script, and resource policy; a terminal
# also gets a one-screen summary of what was pinned.
expctl validate <id> [--json]

# Validate every request, oldest ID first, and exit non-zero if any is
# invalid; suitable for CI or a pre-commit hook.
expctl validate --all [--json]

# Preview or submit through the backend declared by the request.
# --backend local runs a [slurm] request's pinned script on this host.
expctl submit <id> [--backend slurm|local] [--dry-run] [--worktree-root DIR] [--skip-node-check] [--allow-login-node] [--json]

# Submit every request that has no receipt yet, oldest ID first. Each request
# is validated and submitted on its own; failures are reported and the rest
# continue, and the exit status is non-zero if any failed.
expctl submit --all [--backend slurm|local] [--dry-run] [--worktree-root DIR] [--skip-node-check] [--allow-login-node] [--json]

# Show detailed state once, or refresh until the job finishes.
expctl status <id> [--watch [SECONDS]] [--json]

# Show the last 100 log lines, or keep following new output.
expctl logs <id> [--tail N] [--follow] [--worktree-root DIR]

# Preview or request an audited cancellation.
expctl cancel <id> [--reason TEXT] [--dry-run] [--json]

# Copy logs, extract metrics, and record the execution verdict.
expctl collect <id> [--worktree-root DIR] [--json]

# Collect every submitted request that is not collected yet, oldest ID
# first. Jobs still queued or running are reported and skipped; any other
# failure is reported, the rest continue, and the exit status is non-zero.
expctl collect --all [--worktree-root DIR] [--json]

# Preview or remove the verified worktree after collection.
expctl clean <id> [--dry-run] [--worktree-root DIR] [--json]

# Scaffold results/<id>/report.md from the request, receipt, and metrics.
expctl report <id> [--json]

# Create a new request for the same pinned code. --as also accepts a bare
# short name, which is prefixed with today's date.
expctl rerun <id> [--as NEW_ID] [--reason TEXT] [--json]

# Show version or help.
expctl --version
expctl --help
expctl <command> --help
```

Every `<id>` that names an existing request can be shortened. The short name
after the date (`cross-probe` for `20260829-cross-probe`) or any part of the ID
that matches exactly one request is enough, and `@` names the newest request in
`list` order. An ambiguous reference is refused with the matching IDs, and the
expansion is echoed on stderr, for example `expctl: cross-probe ->
20260829-cross-probe`. PowerShell needs the quoted form `'@'`.

`--skip-node-check` applies only to SLURM submissions. It bypasses the
cross-job node-budget check and requires explicit operator authorization.
`--allow-login-node` lets a local job start on a host that looks like a SLURM
login node; see [GPUs and login nodes](#gpus-and-login-nodes).

Set a repository-wide default row count in `expctl.toml`:

```toml
[display]
list_limit = 20
```

`expctl list --limit N` overrides it once, while `expctl list --all` shows all
matching requests. Existing configurations without `display.list_limit`
continue to show every request.

`expctl.toml` is shared, but where worktrees fit often depends on the machine.
Set `EXPCTL_WORKTREE_ROOT` (for example, to a scratch filesystem on the
cluster) to override `worktree.root` on one machine; `~` is expanded and a
relative path is resolved against the repository root. `submit
--worktree-root DIR` still overrides both for one submission, and `doctor`
reports which root is in effect.

## Execution backends

A request must contain exactly one backend table.

```toml
[slurm]
script = "scripts/train.slurm"
max_concurrent_nodes = 1

[slurm.env]
CONFIG = "configs/run.toml"
```

SLURM submissions use `sbatch`; status, cancellation, and final accounting use
`squeue`, `scancel`, and `sacct`.

`slurm.max_concurrent_nodes` is optional. When set, the pinned script must
declare a numeric `#SBATCH --nodes` (array ranges and `%N` throttles included)
whose worst case stays within it. Omit it to leave the request's node envelope
unlimited; a non-zero `scheduler.max_total_nodes` still requires a derivable
count, which is then reserved against the budget.

```toml
[local]
script = "scripts/train.py"
args = ["--config", "configs/run.toml"]
gpus = 1

[local.env]
MODE = "evaluation"
```

The local backend runs the pinned script asynchronously in its detached
worktree, captures stdout and stderr in the requested log, and records the
supervisor PID, Linux `/proc` start identity, and exit status. The script must
be executable in Git (for example,
`git update-index --chmod=+x scripts/train.py`), and
`outputs.log_glob` must be one exact path containing `{job_id}`, not a wildcard.
The receipt also pins the execution hostname: local status, cancellation, and
collection must run on that same host. Cancellation sends `SIGTERM` to the
recorded process group.

Local mode is intended for a machine where compute resources are already
allocated. Apart from the GPU claims below, it provides no queue, resource
isolation, or restart after the host reboots.

### GPUs and login nodes

Every local job, from a `[local]` request or from `--backend local`, claims
GPUs on its host and sees only those, through `CUDA_VISIBLE_DEVICES`:

- A `[local]` request claims `local.gpus` GPUs, or every GPU when `gpus` is
  left out. A `[slurm]` request run with `--backend local` claims what its
  `#SBATCH --gpus-per-node`, `--gres=gpu`, `--gpus`, or `--gpus-per-task`
  lines ask for, and none when it asks for none, as under SLURM; it also gets
  `SLURM_GPUS_ON_NODE` and `SLURM_JOB_GPUS`.
- The host's GPUs are the `CUDA_VISIBLE_DEVICES` that `expctl submit` itself
  runs with, or else every GPU `nvidia-smi` lists. Inside a SLURM allocation
  that is the allocation's GPUs; on a workstation,
  `CUDA_VISIBLE_DEVICES=2,3 expctl submit <id>` keeps GPUs 0 and 1 out of
  expctl's hands. GPUs are numbered the way `nvidia-smi` numbers them
  (`CUDA_DEVICE_ORDER=PCI_BUS_ID` unless it is already set).
- A claim is an `flock` in `/tmp/expctl-gpu-locks` that the job's supervisor
  holds, so it covers every repository and user on the host and ends when the
  job ends, however it ends. When too few GPUs are free, `submit` refuses and
  names the jobs holding them; it does not queue. `submit --all` submits the
  requests that fit and reports the rest, which keep no receipt and are picked
  up by the next `submit --all`.
- `local.env` cannot set `CUDA_VISIBLE_DEVICES`. When `nvidia-smi` is missing
  and `CUDA_VISIBLE_DEVICES` is unset, a job that needs a number of GPUs is
  refused, and a `[local]` request without `gpus` keeps the environment it was
  started with.

The receipt, `status`, and the report record the claimed GPUs, and `doctor`
lists the host's GPUs and which of them are in use.

A host with `sbatch` but no `slurmd` is a SLURM login node, and local
execution there is refused: submit the request to the cluster, or run expctl
inside an allocation (`srun --pty bash`), where `SLURMD_NODENAME` is set.
`--allow-login-node` overrides the check on a host that really is meant to run
jobs. `doctor --backend local` reports it as `Execution host`.

### Running a SLURM request locally

```bash
expctl submit <id> --backend local
```

`--backend local` runs a `[slurm]` request's pinned script on the current host
instead of calling `sbatch`. Nothing else about the request changes: the same
file, worktree, receipt, and collected evidence work whether a colleague sends
it to the cluster or you run it on one machine.

The `#SBATCH` preamble stays a block of comments in the script. expctl reads it
to reproduce what the scheduler would have done with it:

- the script runs under `bash` in its detached worktree, supervised by a
  detached process group, with `SLURM_SUBMIT_DIR`, `SLURM_SUBMIT_HOST`,
  `SLURM_JOB_ID`, `SLURM_JOB_NAME`, `SLURM_JOB_USER`, the single-node
  variables, and `[slurm.env]` exported to every task;
- `#SBATCH --partition`, `--account`, `--cpus-per-task`, `--ntasks`, and
  `--gpus-per-node` are re-exported as `SLURM_JOB_PARTITION`,
  `SLURM_JOB_ACCOUNT`, `SLURM_CPUS_PER_TASK`, `SLURM_NTASKS`/`SLURM_NPROCS`,
  and `SLURM_GPUS_PER_NODE`;
- `#SBATCH --array` runs its tasks one at a time, in index order, each with
  `SLURM_ARRAY_JOB_ID`, `SLURM_ARRAY_TASK_ID`, `SLURM_ARRAY_TASK_COUNT`,
  `SLURM_ARRAY_TASK_MIN`, and `SLURM_ARRAY_TASK_MAX`. A `%N` throttle is
  recorded but not honoured: one host has no queue to spread tasks over;
- `#SBATCH --output` and `--error` are expanded the way SLURM expands them
  (`%j`, `%A`, `%a`, `%x`, `%N`, `%u`, `%%`, and widths such as `%4a`), and
  default to `slurm-%j.out`, or `slurm-%A_%a.out` for an array. Every log is
  created before anything runs, so a submission fails outright instead of
  part-way through an array.

The job ID is an eight-digit number drawn on this host; an array task's
`SLURM_JOB_ID` is `<job id>_<task id>`. The receipt records `backend = "local"`
with `emulated_backend = "slurm"`, and `collect` writes the same per-task
`job_id`, `state`, and `exit_code` rows `sacct` would have produced, so reports
and metrics come out in the cluster's format. `status` reads the recorded
process and exit-status file instead of `squeue` and `sacct`; `cancel` sends
`SIGTERM` to the process group instead of calling `scancel`, which stops the
running task and every array task after it.

`submit` warns when a single host cannot reproduce the submission: a script
that could occupy several nodes under SLURM, or `#SBATCH --output` names that
`outputs.log_glob` does not match. Apart from GPUs, nothing enforces the
`#SBATCH` resource request — there is no partition, node isolation, or time
limit.

`--backend local` on a request that already declares `[local]` changes nothing,
so `submit --all --backend local` stays usable in a mixed repository.
`--backend slurm` refuses a `[local]` request, and `--skip-node-check` is
refused with `--backend local`.

`new` refuses a dirty working tree because uncommitted changes are not part of
the pinned `HEAD`; `--allow-dirty` is an explicit override. `validate` rejects
a request that still carries starter-template placeholder values (title,
question, decision rule, script, log glob, metrics, requirements).
`validate --all` checks every request file in one pass, with the same batched
Git queries as `list`, and lists each invalid one with its reason. `doctor`
exits nonzero when a repository check fails; on the execution host add
`--backend local` or `--backend slurm` to require that backend's checks. The
legacy `--cluster` flag is an alias for `--backend slurm`.

In a terminal, `list` renders an aligned table. Redirected output defaults to
TSV. For example:

```text
EXPERIMENT ID                 STATUS     TITLE
----------------------------  ---------  ---------------------------
20260829-stagger-control      FAILED     Iso-cost stagger control
20260829-decoder-metric       PENDING    Decoder-induced metric
20260829-decoder-cross-probe  RUNNING    Decoder block-RoPE probe
```

SLURM array tasks with mixed states show `MIXED`. If SLURM is unavailable or
returns no state, `list` warns once on stderr and keeps affected SLURM rows as
`submitted`. Local rows are refreshed from the recorded process and status
file. Receipts are never modified by a refresh.

`list --long` (or `-l`) adds the backend, job ID, submission time, and
execution host recorded in each receipt; a `[slurm]` request run with
`--backend local` shows as `local (slurm)`. The table shows submission times
in local time and leaves out detail columns that are empty in every row;
TSV and JSON always carry all of them, with the recorded ISO timestamps.

`list` sorts newest requests first by default: rows are ordered by the ID's
date, and same-day requests by the commit time that first added their file
(an uncommitted request file counts as the newest of its day). `--sort
oldest` reverses the order. `--status` is case-insensitive, may
contain comma-separated values, and may be repeated. Filtering uses refreshed
execution states. Each listing verifies pinned commits and scripts with a
couple of batched `git cat-file` queries rather than one Git process per
request. The configured or command-line limit is applied after filtering and
sorting. Without a status filter, a limited listing validates and refreshes
only those N request paths. Status-filtered listings scan in bounded batches
and stop once N matches are found; `--all` performs a complete scan.

For SLURM, `status` falls back to `sacct` when `squeue` is unavailable. For
local runs, it checks the recorded process identity and exit-status file. With
`--watch`, it stops at a terminal state; redirected or `--json` watch output
is newline-delimited JSON.

`logs` reads the submitted worktree while a job is active and the collected
result directory afterward. `clean` only removes a verified worktree after
collection and refuses one still claimed by an uncollected rerun.

Human-oriented lifecycle commands show aligned detail and next-step tables in
a terminal. Redirected output stays JSON unless the command emits raw logs;
use `--json` to force JSON where supported.

The bare `expctl` overview refreshes states exactly as `list` does and orders
open experiments by stage: requests to submit, jobs queued or running,
finished jobs to collect, collected results awaiting a report, and anything
that needs attention (an invalid request, an unreadable receipt, or a
submission without a confirmed job ID). Commands name each request by its
short name when no other request shares it. Redirected output is a JSON object
whose `experiments` rows carry `stage` and `next`. Outside an initialized
repository, `expctl` prints the grouped help and the reason.

`submit`, `status`, `logs --follow`, `cancel`, `collect`, and `clean` run on the
Linux execution host. The other commands also work on Windows; non-following
`logs` works wherever its log path is available.

## Repository model

Initialization creates:

```text
expctl.toml
expctl/
|-- requests/<id>.toml          immutable experiment request
|-- results/<id>/receipt.json   submission identity and lifecycle state
|-- results/<id>/logs/          collected execution output
|-- results/<id>/metrics.json   extracted metrics
|-- results/<id>/report.md      human conclusion
|-- templates/request.toml      request template
```

The lifecycle is:

```text
requested -> preparing/submitting -> submitted -> [cancel_requested] -> collected -> reviewed
```

`reviewed` means `results/<id>/report.md` exists: `expctl report` scaffolds it
from the request, receipt, and metrics, and `list`/`status` show the state,
but no receipt field is written for it. For SLURM, `submission_unknown` means
`sbatch` may have accepted the job but no job ID was safely recorded; reconcile
it with SLURM manually.

## Safety rules

- A request pins a full commit, backend script and settings, expected logs,
  and decision rule. Do not edit it after a receipt exists.
- `submit` uses a clean detached worktree at the pinned commit. SLURM requests
  verify resource policy and record a scheduler job ID; local runs record a
  process identity and exit-status file. A `[slurm]` request run with
  `--backend local` still verifies its node envelope, but no node budget is
  reserved: this host is not the cluster.
- Each request gets at most one submission attempt. Never delete a receipt to
  resubmit; use `rerun` to create a new ID.
- An unreadable or structurally invalid receipt blocks worktree reuse and
  cleanup until its ownership evidence is restored; expctl never assumes a
  corrupt receipt is unrelated.
- `collect` verifies the submitted worktree and publishes results once.
  `collect --all` does this for every uncollected receipt and skips jobs
  that have not finished.
- `logs`, `collect`, and `clean` use the worktree recorded in the receipt,
  provided it still ends in the request's `code.worktree` and lies outside
  the repository, so a `--worktree-root` given to `submit` need not be
  repeated. Passing `--worktree-root` to them asserts that location instead.
- `cancel` records the operator, time, and optional reason after the backend
  accepts the request. `clean` never removes an uncollected worktree.
- `expctl.toml` defines repository policy and should be committed.
- `expctl` never runs `git add`, `git commit`, or `git push`.

Use `expctl new <id>` to start from `expctl/templates/request.toml` with the
current commit, branch, ID, and worktree name filled in; `expctl new lr-sweep
--title "Learning-rate sweep"` creates `<today>-lr-sweep` with its title. To
repeat an earlier experiment on newer code, `expctl new lr-sweep-v2 --from
lr-sweep` copies that request's question, backend, environment, and outputs,
replaces only the ID, branch, commit, and worktree name, and drops any
`rerun_of`. (`rerun` is for the same code; `new --from` pins the current
`HEAD`.) The
[detailed guide (Chinese)](docs/usage.zh.md) covers every request field,
configuration option, and recovery procedure. For an AI runner, adapt
[`examples/skill/SKILL.md`](examples/skill/SKILL.md) with project-specific
stopping conditions.

## Development

```bash
uv run pytest -q
```

## License

MIT — see [LICENSE](LICENSE).
