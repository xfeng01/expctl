# Changelog

## 0.11.0 — 2026-08-30

- `list` now orders same-day requests by the commit time that first added
  each request file instead of alphabetically, so listings follow creation
  order; an uncommitted request file counts as the newest of its day. The
  extra `git log` query only runs when a listing contains same-day IDs.
- `slurm.max_concurrent_nodes` is now optional. Omitting it leaves a request's
  node envelope unlimited, and the pinned script no longer needs a parseable
  `#SBATCH --nodes` unless `scheduler.max_total_nodes` is enabled, in which
  case the derived count must fit the budget and is reserved against it.
  Receipts, `--dry-run`, and `validate` report an omitted cap as `unlimited`.
- Added `expctl submit --all`, which submits every request without a receipt,
  oldest ID first. Each request is validated and submitted independently, so
  one failure is reported without blocking the others; the command exits
  non-zero when any request failed. `--dry-run` previews the whole batch.

## 0.10.2 — 2026-08-30

- `list` now verifies pinned commits and scripts with two batched
  `git cat-file` queries per scan instead of two Git processes per request,
  so listings stay fast when every request pins its own commit.

## 0.10.1 — 2026-08-30

- Limited `list` calls now avoid validating and refreshing requests that cannot
  appear in the output. Status-filtered calls scan in bounded batches and stop
  after enough matches; `--all` retains a complete scan.

## 0.10.0 — 2026-08-30

- Added repository-level `display.list_limit` for the default `expctl list`
  row count. `--limit N` overrides it for one command and `--all` bypasses it.

## 0.9.0 — 2026-08-29

- Added a `local` backend for asynchronous execution on an already allocated
  Linux compute node, including PID-safe status checks, process-group
  cancellation, log capture, collection, and reporting. Requests must select
  exactly one of `slurm` or `local`.
- `doctor --backend local|slurm` can now enforce readiness for the selected
  execution backend; `--cluster` remains an alias for SLURM.

## 0.8.0 — 2026-08-29

- Added `expctl report <id>`: scaffolds `results/<id>/report.md` from the
  request, receipt, and `metrics.json` (facts, question, decision rule, a
  per-log metrics table, missing metrics, and empty Observations/Conclusion
  sections). `list` and `status` show a collected request whose report exists
  as `reviewed`; `collect` and `status` now point at `report` as the next step.
- `validate` rejects requests that still carry starter-template placeholder
  values, and on a terminal prints a summary of what was pinned (commit,
  script, verified nodes, env, log glob, metrics, requirements).
- Friendlier Git failures: a commit missing from the clone says which ref to
  fetch, a script missing at the pinned commit says to commit and push it, and
  running outside a repository says so instead of echoing `git rev-parse`.
- `doctor` exits 0 when the repository checks pass; the SLURM checks are
  informational unless `--cluster` is given, so authors' machines and CI no
  longer fail for lacking `sbatch`.

## 0.7.1 — 2026-08-29

- Worktree reuse and cleanup now fail safely when any receipt cannot be read
  or lacks a resolvable absolute worktree, instead of ignoring potentially
  active ownership evidence.
- `logs --follow` now tracks a stable file identity and byte cursor from one
  open-file snapshot, preventing concurrent appends from being skipped or
  emitted twice and safely restarting when truncation is observed.
- Request validation now enforces the documented `outputs.metrics` name
  grammar used by metric extraction.

## 0.7.0 — 2026-08-29

- Added audited `cancel`, live or collected `logs`, and guarded `clean`
  commands. Cleanup requires collected results, verifies the detached
  worktree, and blocks worktrees still claimed by an uncollected rerun.
- `status --watch [SECONDS]` now refreshes until a terminal state. One-shot
  status and watch both fall back from unavailable `squeue` queries to
  `sacct`; redirected watch output is newline-delimited JSON.
- Cancellation, collection, and cleanup serialize receipt lifecycle writes;
  `doctor` now checks `scancel` with the other required SLURM commands.

## 0.6.0 — 2026-08-29

- `expctl new` now refuses uncommitted changes that are absent from the pinned
  `HEAD`; `--allow-dirty` provides an explicit, visible override.
- `expctl list` now supports repeatable `--status` filters, newest/oldest
  sorting, and `--limit`. It defaults to newest first and caches Git validation
  for requests sharing a commit and script.
- Added `expctl doctor [--json]` to report repository readiness, worktree-root
  safety and writability, POSIX locking, and required SLURM commands.

## 0.5.0 — 2026-08-29

- Added `expctl new <id>` to create a request from the repository template and
  fill in its ID, current commit, branch label, and generated worktree name.
- `new`, `submit`, `status`, `collect`, and `rerun` now show concise summaries
  and next-step hints on terminals. Redirected output remains JSON, and
  `--json` forces machine-readable output interactively.
- `init` now honors an existing `paths.root`, and unsupported
  `outputs.log_glob` placeholders produce a validation error instead of a
  Python traceback.

## 0.4.4 — 2026-08-29

- `collect` now serializes result publication, stages logs and metrics, and
  refuses to overwrite a completed or partial collection.
- `collect` recomputes the expected worktree instead of trusting the receipt;
  `--worktree-root` must match a submit-time override.
- `paths.root` now resolves symlinks and rejects targets outside the repository.

## 0.4.3 — 2026-08-29

- `expctl list` now batches all submitted job IDs into one `squeue` query and
  one `sacct` query for jobs no longer queued, avoiding per-experiment
  scheduler round trips.
- Failed, unavailable, or incomplete live refreshes emit one warning on
  stderr while affected rows retain `submitted`; TSV and JSON stdout remain
  machine-readable and receipts remain unchanged.
- Reorganized the README into a concise English command reference and added a
  compact `list` output example.

## 0.4.2 — 2026-08-29

- `expctl list` now refreshes confirmed `submitted` receipts from SLURM:
  active jobs show their `squeue` state and finished jobs show their `sacct`
  verdict. Mixed array-task states are reported as `MIXED`.
- Listing remains portable and read-only. If `squeue` or `sacct` is absent or
  unavailable, the stored `submitted` state is shown instead, and receipts are
  never modified by a status refresh.

## 0.4.1 — 2026-08-28

- `expctl list` now renders an aligned, width-aware table on interactive
  terminals, including correct CJK column widths, safe truncation, and optional
  status colors. Redirected output remains stable TSV.
- Added `expctl list --table`, `--tsv`, `--json`, and `--no-color`.
- List cells are restricted to one safe terminal line, and output degrades
  cleanly when the terminal encoding cannot represent Unicode table rules.

## 0.4.0 — 2026-08-28

- Submission now claims a `preparing` receipt exclusively before `sbatch`,
  uses atomic JSON updates, and preserves `submission_unknown` when no job ID
  can be confirmed. Concurrent callers can no longer submit the same request.
- Existing worktrees must belong to the same repository, match the pinned
  commit, and be clean outside configured runtime directories. Active or
  uncertain runs block reuse of the same worktree.
- Validation derives the worst-case node envelope from explicit numeric
  `#SBATCH --nodes` and `--array` directives. Ambient `SBATCH_*` options are
  removed, required scheduler options are reapplied on the `sbatch` command
  line, and expctl serializes its local node-budget check/submit window.
- `collect` refuses queued jobs and colliding log basenames, copies files and
  updates state atomically, and records missing expected metrics.
- Receipt parsing now reports user-facing errors instead of JSON tracebacks;
  plain-name validation rejects `.` and `..`.

## 0.3.2 — 2026-08-28

- `expctl rerun <id> [--as NEW_ID] [--reason TEXT]`: copy a submitted request
  to `<id>-r2` (then `-r3`, ...) with top-level `rerun_of` and `rerun_reason`
  keys, so the runner can rerun unchanged code without waiting for the author
  and without touching the failed run's receipt. Commit and worktree stay the
  same; `submit` reuses a worktree that sits at the pinned commit.
- `submit` on a request that already has a receipt now says how that
  submission ended (job ID, submitter, `sacct` verdict) and points at `rerun`,
  instead of inviting a manual receipt deletion.
- Requests accept optional top-level `rerun_of` (an experiment ID) and
  `rerun_reason` (a string).

## 0.3.1 — 2026-08-27

- `submit` no longer fails when the checkout itself materialises an output
  directory listed in `runtime.create_missing` (e.g. tracked files under
  `logs/`): the directory is kept and the job writes into the worktree copy,
  which is where `collect` reads. Input directories in that state are still an
  error. The receipt records the decision per directory under `runtime_dirs`.

## 0.3.0 — 2026-08-26

- `expctl --version`.
- MIT license.
- Generic agent skill template in `examples/skill/SKILL.md`.
- CI runs the test suite on every push (Python 3.11 and 3.13).
- Docs: `submit`, `status`, and `collect` require a POSIX host with SLURM;
  the other commands run anywhere.

## 0.2.0 — 2026-08-26

- **Breaking:** the data directory defaults to `expctl/` (was `experiments/`).
  Existing projects either rename the directory or set
  `[paths] root = "experiments"` in `expctl.toml`.
- New `[paths] root` config key; the directory must stay inside the repository.

## 0.1.0 — 2026-08-26

- Extracted from the cDLM project as a standalone, stdlib-only tool.
- Repository policy (required scheduler lines, node ceiling, shared runtime
  directories, worktree root) moved from code into `expctl.toml`.
- `expctl init` creates the config and directory skeleton.
