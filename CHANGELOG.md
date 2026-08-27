# Changelog

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
