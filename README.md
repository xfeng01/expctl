# expctl

`expctl` is a small Git-backed CLI for handing off asynchronous experiments.
An author publishes an immutable request; a runner reviews, submits, monitors,
and collects it. Git preserves the handoff and its audit trail.

It supports two execution backends: SLURM and direct execution on a Linux
compute node. It is not a scheduler, workflow engine, or experiment tracker.

[Changelog](CHANGELOG.md)

## Install

Every machine needs Git and Python 3.11 or newer. Direct execution requires a
Linux host with `/proc`. The SLURM backend also needs `sbatch`, `squeue`,
`sacct`, and `scancel`; node-budget checks use `scontrol`.

```bash
uv tool install git+https://github.com/xfeng01/expctl
```

Upgrade with `uv tool upgrade expctl`. For zero-install use, copy
`src/expctl/core.py` and run `python core.py <command>`; it uses only the
Python standard library.

Run all commands inside the target Git working tree.

## Commands

Complete command reference:

```text
# Initialize expctl in the current repository.
expctl init

# Check repository and backend readiness. --cluster is an alias for
# --backend slurm.
expctl doctor [--backend slurm|local] [--cluster] [--json]

# Create a request from the repository template and fill in Git metadata.
expctl new <id> [--allow-dirty] [--json]

# List, filter, sort, and limit experiment requests.
expctl list [--table | --tsv | --json] [--status STATUS[,STATUS...]] [--sort newest|oldest] [--limit N | --all] [--no-color]

# Print one validated request as JSON.
expctl show <id>

# Validate the request, pinned commit, script, and resource policy; a terminal
# also gets a one-screen summary of what was pinned.
expctl validate <id>

# Preview or submit through the backend declared by the request.
expctl submit <id> [--dry-run] [--worktree-root DIR] [--skip-node-check] [--json]

# Show detailed state once, or refresh until the job finishes.
expctl status <id> [--watch [SECONDS]] [--json]

# Show the last 100 log lines, or keep following new output.
expctl logs <id> [--tail N] [--follow] [--worktree-root DIR]

# Preview or request an audited cancellation.
expctl cancel <id> [--reason TEXT] [--dry-run] [--json]

# Copy logs, extract metrics, and record the execution verdict.
expctl collect <id> [--worktree-root DIR] [--json]

# Preview or remove the verified worktree after collection.
expctl clean <id> [--dry-run] [--worktree-root DIR] [--json]

# Scaffold results/<id>/report.md from the request, receipt, and metrics.
expctl report <id> [--json]

# Create a new request for the same pinned code.
expctl rerun <id> [--as NEW_ID] [--reason TEXT] [--json]

# Show version or help.
expctl --version
expctl --help
expctl <command> --help
```

`--skip-node-check` applies only to SLURM. It bypasses the cross-job node-budget
check and requires explicit operator authorization.

Set a repository-wide default row count in `expctl.toml`:

```toml
[display]
list_limit = 20
```

`expctl list --limit N` overrides it once, while `expctl list --all` shows all
matching requests. Existing configurations without `display.list_limit`
continue to show every request.

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

```toml
[local]
script = "scripts/train.py"
args = ["--config", "configs/run.toml"]

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
allocated. It provides no queue, resource isolation, or restart after the host
reboots.

`new` refuses a dirty working tree because uncommitted changes are not part of
the pinned `HEAD`; `--allow-dirty` is an explicit override. `validate` rejects
a request that still carries starter-template placeholder values (title,
question, decision rule, script, log glob, metrics, requirements). `doctor`
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

`list` sorts newest IDs first by default. `--status` is case-insensitive, may
contain comma-separated values, and may be repeated. Filtering uses refreshed
execution states; repeated commit/script validation is cached within each
listing. The configured or command-line limit is applied after filtering and
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
  verify resource policy and record a scheduler job ID; local requests record
  a process identity and exit-status file.
- Each request gets at most one submission attempt. Never delete a receipt to
  resubmit; use `rerun` to create a new ID.
- An unreadable or structurally invalid receipt blocks worktree reuse and
  cleanup until its ownership evidence is restored; expctl never assumes a
  corrupt receipt is unrelated.
- `collect` verifies the submitted worktree and publishes results once. If
  `submit` used `--worktree-root`, pass the same directory to `collect`.
- `cancel` records the operator, time, and optional reason after the backend
  accepts the request. `clean` never removes an uncollected worktree.
- `expctl.toml` defines repository policy and should be committed.
- `expctl` never runs `git add`, `git commit`, or `git push`.

Use `expctl new <id>` to start from `expctl/templates/request.toml` with the
current commit, branch, ID, and worktree name filled in. The
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
