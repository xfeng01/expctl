---
name: experiment-runner
description: Prepare, validate, submit, monitor, and collect expctl experiment requests for this repository. Use for work under the expctl/ directory; do not use for ordinary local tests or ad hoc development commands.
---

# Experiment runner (expctl)

Use `expctl/` as the handoff record and the `expctl` command as the execution
layer. Read `expctl.toml`, the repository's contributor guide (`AGENTS.md` or
equivalent), and the selected request before acting. Do not duplicate or
weaken the repository's own cluster and branch rules here.

## Choose the operation

- To prepare a handoff, create a new request from
  `expctl/templates/request.toml`, pin a full Git commit, and run
  `expctl validate <id>`. State the scientific question and decision rule
  before the run. Never reuse an ID for changed settings.
- To inspect the queue, run `expctl list`, then `show` or `validate` the
  selected request. Report invalid or ambiguous requests instead of guessing.
- To submit, first show the operator the pinned commit, job script, runtime
  requirements, maximum node count, and `--dry-run` output. An explicit request
  to consume cluster resources is required before the real
  `expctl submit <id>`; never substitute `git pull && sbatch`.
- To check a submitted run, use `expctl status <id>`. Report the queue counts
  factually; do not collect while `in_queue` is true.
- To collect, run `expctl collect <id>`, read every copied log rather than
  only `metrics.json`, and draft `expctl/results/<id>/report.md` with factual
  run status, failures, metrics, qualitative observations, and the request's
  decision rule. Keep inference clearly separate from recorded output.

## Safety and stopping conditions

- If the `expctl` command is unavailable, stop and ask the operator to install
  it. Do not substitute ad hoc `git`/`sbatch` commands and do not vendor a copy
  into the repository.
- Never select a newer branch head in place of `code.commit`.
- Never edit a request after its receipt exists. Create a new request for a
  rerun, corrected checkpoint, or changed environment.
- Never add checkpoints, datasets, credentials, caches, or unreviewed
  sensitive output to Git.
- Do not use `--skip-node-check` autonomously. If scheduler status is
  unavailable, stop before submission unless the operator supplies independent
  node-budget evidence and explicitly authorizes the override.
- Do not retry a failed or preempted job automatically. Collect the evidence
  and request a new ID or an explicit rerun decision.
- Preparing result files does not authorize committing, pushing, merging, or
  updating the scientific journal. Perform those actions only when requested.

## Adapting this template

Copy this file into your project as
`.claude/skills/experiment-runner/SKILL.md` (Claude Code) or
`.agents/skills/experiment-runner/SKILL.md` (other agents), then add the
project's own rules: where results are interpreted, which files are
main-branch-owned, and anything the runner must never touch.
