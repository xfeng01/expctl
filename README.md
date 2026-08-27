# expctl

Repository-backed asynchronous experiment handoff. One person (or AI agent)
designs an experiment; another executes it on a SLURM cluster, hours later,
without a conversation in between. The only channel is Git.

expctl is deliberately **not** a scheduler, pipeline engine, or tracking
server (use Snakemake, submitit, DVC, or W&B for those). It does one thing:
make the handoff itself safe and auditable.

## The protocol

- `experiments/requests/<id>.toml` — an **immutable request**. It pins the
  exact Git commit, the SLURM entrypoint, the resource envelope, the expected
  logs, and the decision rule the result feeds.
- `experiments/results/<id>/receipt.json` — written when the request is
  submitted: job ID, who, when, from which worktree.
- `experiments/results/<id>/` — collected logs and scraped metrics, pushed
  back for the author to read.
- `expctl.toml` — per-repository policy: required scheduler flags, node
  ceiling, shared runtime directories.

State is defined by which files exist:

```text
request only -> submitted receipt -> collected result -> reviewed conclusion
```

Requests are never edited after submission; a rerun or changed configuration
is a new request ID. Every result stays traceable to the exact request and
commit that produced it.

## Install

```bash
uv tool install git+ssh://git@github.com/xfeng01/expctl   # or: uv tool install /path/to/expctl
```

Zero-install fallback: the whole tool is one stdlib-only file. Copy
`src/expctl/core.py` to the target machine and run `python core.py <command>`
(Python 3.11+).

## Quickstart

In the project repository:

```bash
expctl init          # creates expctl.toml + experiments/ skeleton
# edit expctl.toml: partitions, node ceiling, shared dirs
```

Author side:

```bash
# copy experiments/templates/request.toml to experiments/requests/<id>.toml,
# pin a full 40-character commit, then:
expctl validate <id>
git add experiments/requests/<id>.toml && git commit && git push
```

Runner side (on the cluster):

```bash
git pull --ff-only
expctl list                     # what is requested
expctl show <id>                # read it before spending resources
expctl submit <id> --dry-run    # preview commit, worktree, sbatch command
expctl submit <id>              # detached worktree + budget check + sbatch
expctl status <id>              # squeue while running, sacct afterwards
expctl collect <id>             # copy logs, scrape metrics, update receipt
# write experiments/results/<id>/report.md, then commit and push results/
```

## Commands

| command | what it does |
|---|---|
| `init` | create `expctl.toml` and the `experiments/` skeleton |
| `list` | all requests with their state (`requested` / `submitted` / `collected` / `invalid`) |
| `validate <id>` | schema check, plus: pinned commit exists and its job script contains every `required_script_lines` entry |
| `show <id>` | print the validated request as JSON |
| `submit <id>` | create/verify a detached worktree at the pinned commit, symlink shared runtime dirs, verify `notes.requirements`, check the cross-job node budget via `squeue`, run `sbatch --parsable`, write the receipt. `--dry-run` previews; `--skip-node-check` needs explicit operator authorization |
| `status <id>` | queue state per task (`squeue`), falling back to accounting (`sacct`) once the job left the queue |
| `collect <id>` | copy logs matching `outputs.log_glob` into `results/<id>/logs/`, scrape `outputs.metrics` into `metrics.json`, record the scheduler verdict |

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
max_concurrent_nodes = 1          # worst-case simultaneous nodes

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

## Configuration (`expctl.toml`)

```toml
version = 1

[scheduler]
# Verbatim lines that must appear in the job script at the pinned commit.
# Enforce approved partitions, accounts, or QOS flags here.
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

Notes on the node budget: every pending array task counts as a reserved node,
so the check is deliberately conservative — it can refuse a submission that a
`%N` throttle would in fact keep within budget. Waiting for the queue to
drain is the intended response.

## Working with AI agents

The request/receipt protocol is designed so an agent can act as the runner:
everything it needs is in the repository, every resource-consuming step has a
preview (`--dry-run`) for an approval gate, and nothing is retried or
resubmitted implicitly. Pair it with an agent skill that encodes your
project's stopping conditions; see the `cdlm-experiment-runner` skill in the
first consumer project for a template.

## Development

```bash
uv run pytest -q
```
