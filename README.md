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
needs `sbatch`, `squeue`, and `sacct`; node-budget checks use `scontrol`.

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

# List experiment requests. Choose at most one output format.
expctl list [--table | --tsv | --json] [--no-color]

# Print one validated request as JSON.
expctl show <id>

# Validate the request, pinned commit, script, and resource policy.
expctl validate <id>

# Preview or submit a request. Omit --dry-run to call sbatch.
expctl submit <id> [--dry-run] [--worktree-root DIR] [--skip-node-check]

# Show detailed squeue or sacct state for one submitted request.
expctl status <id>

# Copy logs, extract metrics, and record the scheduler verdict.
expctl collect <id> [--worktree-root DIR]

# Create a new request for the same pinned code.
expctl rerun <id> [--as NEW_ID] [--reason TEXT]

# Show version or help.
expctl --version
expctl --help
expctl <command> --help
```

`--skip-node-check` bypasses the cross-job node-budget check and requires
explicit operator authorization.

In a terminal, `list` renders an aligned table. Redirected output defaults to
TSV. For example:

```text
EXPERIMENT ID                 STATUS     TITLE
----------------------------  ---------  ---------------------------
20260829-decoder-cross-probe  RUNNING    Decoder block-RoPE probe
20260829-decoder-metric       PENDING    Decoder-induced metric
20260829-stagger-control      FAILED     Iso-cost stagger control
```

Mixed array-task states show `MIXED`. If SLURM is unavailable or returns no
state, `list` warns once on stderr and keeps the affected rows as `submitted`.
Receipts are never modified by a refresh.

`submit`, `status`, and `collect` run on the cluster host. The other commands
also work on Windows.

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
requested -> preparing/submitting -> submitted -> collected -> reviewed
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
- `expctl.toml` defines repository policy and should be committed.
- `expctl` never runs `git add`, `git commit`, or `git push`.

Start from `expctl/templates/request.toml`. The
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
