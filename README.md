# expctl

`expctl` is a small Git-backed CLI for handing asynchronous experiments to a
SLURM cluster. An author publishes an immutable request; a runner reviews,
submits, monitors, and collects it. Git preserves the handoff and its audit
trail.

> `expctl` currently supports SLURM only. It is not a scheduler, workflow
> engine, or experiment tracker.

[Changelog](CHANGELOG.md)

## Install

Every machine needs Git and Python 3.11 or newer. The POSIX cluster host also
needs `sbatch`, `squeue`, `sacct`, and `scancel`; node-budget checks use
`scontrol`.

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

# Check repository configuration and cluster dependencies.
expctl doctor [--json]

# Create a request from the repository template and fill in Git metadata.
expctl new <id> [--allow-dirty] [--json]

# List, filter, sort, and limit experiment requests.
expctl list [--table | --tsv | --json] [--status STATUS[,STATUS...]] [--sort newest|oldest] [--limit N] [--no-color]

# Print one validated request as JSON.
expctl show <id>

# Validate the request, pinned commit, script, and resource policy.
expctl validate <id>

# Preview or submit a request. Omit --dry-run to call sbatch.
expctl submit <id> [--dry-run] [--worktree-root DIR] [--skip-node-check] [--json]

# Show detailed state once, or refresh until the job finishes.
expctl status <id> [--watch [SECONDS]] [--json]

# Show the last 100 log lines, or keep following new output.
expctl logs <id> [--tail N] [--follow] [--worktree-root DIR]

# Preview or request an audited SLURM cancellation.
expctl cancel <id> [--reason TEXT] [--dry-run] [--json]

# Copy logs, extract metrics, and record the scheduler verdict.
expctl collect <id> [--worktree-root DIR] [--json]

# Preview or remove the verified worktree after collection.
expctl clean <id> [--dry-run] [--worktree-root DIR] [--json]

# Create a new request for the same pinned code.
expctl rerun <id> [--as NEW_ID] [--reason TEXT] [--json]

# Show version or help.
expctl --version
expctl --help
expctl <command> --help
```

`--skip-node-check` bypasses the cross-job node-budget check and requires
explicit operator authorization.

`new` refuses a dirty working tree because uncommitted changes are not part of
the pinned `HEAD`; `--allow-dirty` is an explicit override. `doctor` reports
repository and cluster readiness and exits nonzero unless both are ready.

In a terminal, `list` renders an aligned table. Redirected output defaults to
TSV. For example:

```text
EXPERIMENT ID                 STATUS     TITLE
----------------------------  ---------  ---------------------------
20260829-stagger-control      FAILED     Iso-cost stagger control
20260829-decoder-metric       PENDING    Decoder-induced metric
20260829-decoder-cross-probe  RUNNING    Decoder block-RoPE probe
```

Mixed array-task states show `MIXED`. If SLURM is unavailable or returns no
state, `list` warns once on stderr and keeps the affected rows as `submitted`.
Receipts are never modified by a refresh.

`list` sorts newest IDs first by default. `--status` is case-insensitive, may
contain comma-separated values, and may be repeated. Filtering uses refreshed
SLURM states; repeated commit/script validation is cached within each listing.

`status` falls back to `sacct` when `squeue` is unavailable. With `--watch`, it
stops at a terminal scheduler state; redirected or `--json` watch output is
newline-delimited JSON.

`logs` reads the submitted worktree while a job is active and the collected
result directory afterward. `clean` only removes a verified worktree after
collection and refuses one still claimed by an uncollected rerun.

Human-oriented lifecycle commands show concise summaries and a suggested next
step in a terminal. Redirected output stays JSON unless the command emits raw
logs; use `--json` to force JSON where supported.

`submit`, `status`, `logs --follow`, `cancel`, `collect`, and `clean` run on the
cluster host. The other commands also work on Windows; non-following `logs`
works wherever its log path is available.

## Repository model

Initialization creates:

```text
expctl.toml
expctl/
|-- requests/<id>.toml          immutable experiment request
|-- results/<id>/receipt.json   submission identity and lifecycle state
|-- results/<id>/logs/          collected scheduler output
|-- results/<id>/metrics.json   extracted metrics
|-- results/<id>/report.md      human conclusion
|-- templates/request.toml      request template
```

The lifecycle is:

```text
requested -> preparing/submitting -> submitted -> [cancel_requested] -> collected -> reviewed
```

`reviewed` is a human conclusion, not a receipt state.
`submission_unknown` means `sbatch` may have accepted the job but no job ID
was safely recorded; reconcile it with SLURM manually.

## Safety rules

- A request pins a full commit, SLURM script, resource envelope, expected
  logs, and decision rule. Do not edit it after a receipt exists.
- `submit` uses a clean detached worktree at the pinned commit, verifies node
  and array limits, removes ambient `SBATCH_*` overrides, and records the job
  ID atomically.
- Each request gets at most one submission attempt. Never delete a receipt to
  resubmit; use `rerun` to create a new ID.
- `collect` verifies the submitted worktree and publishes results once. If
  `submit` used `--worktree-root`, pass the same directory to `collect`.
- `cancel` records the operator, time, and optional reason after `scancel`
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
