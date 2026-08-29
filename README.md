# expctl

Repository-backed asynchronous experiment handoff. One person (or AI agent)
designs an experiment; another executes it on a SLURM cluster, hours later,
without a conversation in between. The only channel is Git.

expctl is deliberately **not** a scheduler, pipeline engine, or tracking
server (use Snakemake, submitit, DVC, or W&B for those). It does one thing:
make the handoff itself safe and auditable.

中文使用指南：[docs/usage.zh.md](docs/usage.zh.md)

## The protocol

- `expctl/requests/<id>.toml` — an **immutable request**. It pins the
  exact Git commit, the SLURM entrypoint, the resource envelope, the expected
  logs, and the decision rule the result feeds.
- `expctl/results/<id>/receipt.json` — claimed before submission and finalized
  with the job ID, who, when, and worktree. An interrupted handoff remains
  visibly `preparing`, `submitting`, or `submission_unknown` and cannot be
  submitted again by accident.
- `expctl/results/<id>/` — collected logs and scraped metrics, pushed
  back for the author to read.
- `expctl.toml` — per-repository policy: required scheduler flags, node
  ceiling, shared runtime directories, and the name of the directory above
  (`expctl/` by default; change `[paths] root` if that name is taken).

State is defined by which files exist:

```text
request only -> preparing/submitting receipt -> submitted -> collected -> reviewed
```

Requests are never edited after submission; a rerun or changed configuration
is a new request ID. Every result stays traceable to the exact request and
commit that produced it.

## Install

```bash
uv tool install git+https://github.com/xfeng01/expctl   # or: uv tool install /path/to/expctl
```

Zero-install fallback: the whole tool is one stdlib-only file. Copy
`src/expctl/core.py` to the target machine and run `python core.py <command>`
(Python 3.11+).

`submit`, `status`, and `collect` run on the cluster host (POSIX, SLURM tools
on `PATH`). `init`, `list`, `validate`, and `show` work anywhere, including
Windows. On a cluster, `list` enriches `submitted` receipts with live SLURM
state; elsewhere it safely keeps the stored state. `expctl --version` prints
the installed version.

## Quickstart

In the project repository:

```bash
expctl init          # creates expctl.toml + expctl/ skeleton
# edit expctl.toml: partitions, node ceiling, shared dirs
```

Author side:

```bash
# copy expctl/templates/request.toml to expctl/requests/<id>.toml,
# pin a full 40-character commit, then:
expctl validate <id>
git add expctl/requests/<id>.toml && git commit && git push
```

Runner side (on the cluster):

```bash
git pull --ff-only
expctl list                     # table + live SLURM state; TSV when piped
expctl show <id>                # read it before spending resources
expctl submit <id> --dry-run    # preview commit, worktree, sbatch command
expctl submit <id>              # detached worktree + budget check + sbatch
expctl status <id>              # squeue while running, sacct afterwards
expctl collect <id>             # copy logs, scrape metrics, update receipt
# write expctl/results/<id>/report.md, then commit and push results/
expctl rerun <id> --reason "preempted"   # same code again? new request <id>-r2
```

## Commands

| command | what it does |
|---|---|
| `init` | create `expctl.toml` and the `expctl/` skeleton |
| `list` | all requests with their repository state. Confirmed submissions are refreshed via `squeue`, then `sacct`; unavailable SLURM tools safely leave them as `submitted`, and mixed array states show as `MIXED`. Interactive output is an aligned, terminal-width-aware table; pipes receive stable TSV. Use `--table`, `--tsv`, `--json`, or `--no-color` to override formatting |
| `validate <id>` | schema check, plus: pinned commit exists and its job script contains every `required_script_lines` entry |
| `show <id>` | print the validated request as JSON |
| `submit <id>` | exclusively claim the request, create/verify a clean detached worktree from this repository at the pinned commit, verify the script's node envelope and `notes.requirements`, check the cross-job node budget via `squeue`, run `sbatch --parsable`, and atomically finalize the receipt. `--dry-run` previews; `--skip-node-check` needs explicit operator authorization |
| `status <id>` | queue state per task (`squeue`), falling back to accounting (`sacct`) once the job left the queue |
| `collect <id>` | after the job leaves `squeue`, atomically copy non-colliding logs matching `outputs.log_glob`, scrape `outputs.metrics` into `metrics.json`, record missing metrics and the scheduler verdict |
| `rerun <id>` | copy a submitted request to `<id>-r2` (or `--as NEW_ID`) with `rerun_of` pointing back and `--reason` recorded; same commit, same worktree. Commit the copy, then `submit` it |

## When a job fails

A request gets at most one automatic submission attempt. `submit` claims the
receipt exclusively before invoking `sbatch`; concurrent callers lose that
claim. If `sbatch` does not return a confirmed job ID, the receipt becomes
`submission_unknown` and remains locked for manual scheduler reconciliation.
Never delete or retry such a receipt until an operator has established whether
a job exists. A confirmed run of the same code is a new request:

```bash
expctl rerun <id> --reason "preempted"          # writes requests/<id>-r2.toml
git add expctl/requests/<id>-r2.toml && git commit -m "rerun <id>: preempted"
expctl submit <id>-r2
```

The copy differs from the original only in its `id` line plus `rerun_of` and
`rerun_reason`. Commit and worktree are unchanged, so `submit` reuses the
existing worktree; the failed run's receipt and logs stay in `results/<id>/`.
If the fix needs a code change, that is the author's job: commit it and write
a request pinning the new commit. Never delete a receipt to resubmit.

## Request format

```toml
version = 1
id = "20260101-short-name"        # must match the filename
title = "..."
question = "What uncertainty does this run resolve?"
decision_rule = "Which outcome changes the next decision?"

[code]
branch = "main"                   # label only; nothing is checked out from it
commit = "<full 40-char hash>"    # the exact code that runs — never a branch
worktree = "myproject-short-name" # sibling directory the job runs in

[slurm]
script = "scripts/job.slurm"      # repo-relative
max_concurrent_nodes = 1          # audited upper bound, not a free-form estimate

[slurm.env]                       # passed via sbatch --export
NUM_SAMPLES = "256"               # quoted strings, no commas

[outputs]
log_glob = "logs/job-{job_id}_*.out"
metrics = ["gen_ppl"]             # scraped from `name: value` log lines
                                  # (column-aligned `name  value` also works)

[notes]
requirements = ["runs/ckpt.pt"]   # must exist on the cluster before submit
instructions = "free text for the runner"
```

The pinned job script must declare an explicit numeric node allocation, for
example `#SBATCH --nodes=1`. If it declares an array, expctl also parses its
numeric range and `%N` throttle. The derived maximum (`nodes × concurrently
runnable array tasks`) must not exceed `max_concurrent_nodes`. Ambient
`SBATCH_*` environment options are removed before submission so they cannot
override the audited script.

Copies made by `expctl rerun` carry two extra top-level keys, `rerun_of`
(the predecessor's ID) and optionally `rerun_reason`.

## Configuration (`expctl.toml`)

```toml
version = 1

[paths]
# Directory (relative to the repo root) holding requests/, results/, templates/.
root = "expctl"

[scheduler]
# Verbatim lines that must appear in the job script at the pinned commit.
# Approved #SBATCH options are also passed on sbatch's command line so later
# script directives cannot override partitions, accounts, or QOS policy.
required_script_lines = ["#SBATCH -p my-partition"]
# Cross-job node ceiling counted via squeue. 0 disables the check.
max_total_nodes = 4

[runtime]
# Repo-root entries symlinked into each experiment worktree.
shared_dirs = [".venv", "data", "runs", "logs"]
# Subset of shared_dirs created in the main repo if missing.
create_missing = ["runs", "logs"]

[worktree]
# Where detached worktrees are created, relative to the repo root.
root = ".."
```

Notes on the shared directories: each entry is symlinked into the experiment
worktree. If the checkout itself already contains one of the `create_missing`
(output) directories — tracked log files under `logs/`, say — it is kept and
the job writes there; `collect` reads from the worktree either way. An *input*
directory in that state is an error. The receipt lists the outcome per
directory under `runtime_dirs`. Reused worktrees must belong to the same Git
repository, have the requested `HEAD`, and contain no tracked changes or
untracked/ignored files outside these shared directories. A worktree cannot be
reused while another recorded job is still queued or its submission outcome is
unknown.

Notes on the node budget: every pending array task counts as a reserved node,
so the check is deliberately conservative — it can refuse a submission that a
`%N` throttle would in fact keep within budget. Waiting for the queue to
drain is the intended response. expctl serializes its own check-and-submit
window inside one Git checkout; scheduler QOS/account limits remain the only
hard ceiling across other clones and manually submitted jobs.

## Working with AI agents

The request/receipt protocol is designed so an agent can act as the runner:
everything it needs is in the repository, every resource-consuming step has a
preview (`--dry-run`) for an approval gate, and nothing is retried or
resubmitted implicitly. Pair it with an agent skill that encodes your
project's stopping conditions: copy
[`examples/skill/SKILL.md`](examples/skill/SKILL.md) into your project and
add its own rules.

## Development

```bash
uv run pytest -q
```

Changes are listed in [CHANGELOG.md](CHANGELOG.md).

## License

MIT — see [LICENSE](LICENSE).
