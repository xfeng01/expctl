# Changelog

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
