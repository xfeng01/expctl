"""expctl: repository-backed asynchronous experiment handoff.

The whole tool lives in this one file on purpose: on a machine where nothing
can be installed, copy `core.py` anywhere and run `python core.py <command>`
(Python 3.11+, stdlib only).

Protocol: an immutable request file (`<root>/requests/<id>.toml`) pins the
exact commit, entrypoint, and resource envelope of a run. Submitting writes a
receipt (`<root>/results/<id>/receipt.json`); collecting copies logs and
scrapes metrics next to it. State is defined by which files exist — requests
are never edited after submission. `<root>` is `expctl/` unless `expctl.toml`
says otherwise; that file also holds the per-repository policy (required
scheduler flags, node ceiling, shared runtime directories).
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import getpass
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path, PurePosixPath
from typing import Any


# Kept in sync with pyproject.toml by tests; duplicated here so the single-file
# copy still knows its version.
__version__ = "0.3.1"

CONFIG_NAME = "expctl.toml"
DEFAULT_ROOT = "expctl"
DEFAULT_SHARED_DIRS = (".venv", "data", "runs", "logs")
DEFAULT_CREATE_MISSING = ("runs", "logs")

ID_RE = re.compile(r"^\d{8}-[a-z0-9][a-z0-9-]*$")
COMMIT_RE = re.compile(r"^[0-9a-fA-F]{40}$")
ENV_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9.][A-Za-z0-9._-]*$")
# Matches "name: value", "name = value", and column-aligned "name   value"
# lines, so both dedicated machine-readable blocks and plain metric dumps work.
METRIC_LINE_RE = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_.-]*)(?:\s*[:=]\s*|\s+)"
    r"(-?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*$",
    re.MULTILINE,
)

STARTER_CONFIG = """\
# expctl repository policy. Commit this next to the code it governs.
version = 1

[paths]
# Directory (relative to the repo root) holding requests/, results/, templates/.
root = "expctl"

[scheduler]
# Verbatim lines that must appear in the job script at the pinned commit.
# Use this to enforce approved partitions, accounts, or QOS flags.
required_script_lines = []
# Cross-job node ceiling, counted via squeue across everything you have
# queued or running. 0 disables the check.
max_total_nodes = 0

[runtime]
# Repository-root entries symlinked into each experiment worktree.
shared_dirs = [".venv", "data", "runs", "logs"]
# Subset of shared_dirs created in the main repository if missing.
create_missing = ["runs", "logs"]

[worktree]
# Where detached experiment worktrees are created, relative to the repo root.
root = ".."
"""

STARTER_TEMPLATE = """\
version = 1
id = "YYYYMMDD-short-name"
title = "Short experiment title"
question = "What uncertainty does this experiment resolve?"
decision_rule = "What result changes the next decision?"

[code]
branch = "ablation/short-name"
commit = "FULL_40_CHARACTER_GIT_COMMIT"
worktree = "myproject-short-name"

[slurm]
script = "scripts/example.slurm"
max_concurrent_nodes = 1

[slurm.env]
EXAMPLE_OPTION = "value"

[outputs]
log_glob = "logs/example-{job_id}.out"
metrics = ["metric_name"]

[notes]
requirements = ["runs/example/ckpt.pt"]
instructions = "Any non-obvious recovery or rerun instructions."
"""


class ExpctlError(RuntimeError):
    """A user-correctable experiment-control error."""


@dataclasses.dataclass(frozen=True)
class Config:
    root: str
    required_script_lines: tuple[str, ...]
    max_total_nodes: int
    shared_dirs: tuple[str, ...]
    create_missing: tuple[str, ...]
    worktree_root: str


def _run(
    args: list[str],
    *,
    cwd: Path,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            args,
            cwd=cwd,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except FileNotFoundError as exc:
        raise ExpctlError(f"required command not found: {args[0]}") from exc
    if check and result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise ExpctlError(f"command failed ({' '.join(args)}): {detail}")
    return result


def find_repo_root(start: Path | None = None) -> Path:
    here = (start or Path.cwd()).resolve()
    result = _run(["git", "rev-parse", "--show-toplevel"], cwd=here)
    return Path(result.stdout.strip()).resolve()


def _config_list(
    table: dict[str, Any],
    key: str,
    context: str,
    default: tuple[str, ...],
) -> tuple[str, ...]:
    value = table.get(key)
    if value is None:
        return default
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item for item in value
    ):
        raise ExpctlError(f"{context}.{key} must be an array of non-empty strings")
    return tuple(value)


def load_config(repo: Path) -> Config:
    path = repo / CONFIG_NAME
    if not path.is_file():
        raise ExpctlError(
            f"{CONFIG_NAME} not found in {repo}; run `expctl init` to create a starter"
        )
    try:
        with path.open("rb") as handle:
            data = tomllib.load(handle)
    except tomllib.TOMLDecodeError as exc:
        raise ExpctlError(f"invalid TOML in {path}: {exc}") from exc
    if data.get("version") != 1:
        raise ExpctlError(f"{CONFIG_NAME} version must be 1")

    tables: dict[str, dict[str, Any]] = {}
    for name in ("paths", "scheduler", "runtime", "worktree"):
        table = data.get(name, {})
        if not isinstance(table, dict):
            raise ExpctlError(f"[{name}] in {CONFIG_NAME} must be a table")
        tables[name] = table

    data_root = tables["paths"].get("root", DEFAULT_ROOT)
    if not isinstance(data_root, str) or not data_root.strip():
        raise ExpctlError("paths.root must be a non-empty string")
    _relative_path(data_root, "paths.root")

    max_total = tables["scheduler"].get("max_total_nodes", 0)
    if not isinstance(max_total, int) or isinstance(max_total, bool) or max_total < 0:
        raise ExpctlError(
            "scheduler.max_total_nodes must be an integer >= 0 (0 disables the check)"
        )

    shared = _config_list(
        tables["runtime"], "shared_dirs", "runtime", DEFAULT_SHARED_DIRS
    )
    create_missing = _config_list(
        tables["runtime"], "create_missing", "runtime", DEFAULT_CREATE_MISSING
    )
    for name in shared:
        if not SAFE_NAME_RE.fullmatch(name):
            raise ExpctlError(f"runtime.shared_dirs entry is not a plain name: {name}")
    for name in create_missing:
        if name not in shared:
            raise ExpctlError(
                f"runtime.create_missing entry must also be in shared_dirs: {name}"
            )

    root = tables["worktree"].get("root", "..")
    if not isinstance(root, str) or not root.strip():
        raise ExpctlError("worktree.root must be a non-empty string")

    return Config(
        root=data_root,
        required_script_lines=_config_list(
            tables["scheduler"], "required_script_lines", "scheduler", ()
        ),
        max_total_nodes=max_total,
        shared_dirs=shared,
        create_missing=create_missing,
        worktree_root=root,
    )


def init_repo(repo: Path) -> list[str]:
    root = repo / DEFAULT_ROOT
    entries = (
        (repo / CONFIG_NAME, STARTER_CONFIG),
        (root / "templates" / "request.toml", STARTER_TEMPLATE),
        (root / "requests" / ".gitkeep", ""),
        (root / "results" / ".gitkeep", ""),
    )
    created: list[str] = []
    for path, content in entries:
        if path.exists():
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        created.append(str(path.relative_to(repo)).replace("\\", "/"))
    return created


def request_path(repo: Path, config: Config, experiment_id: str) -> Path:
    if not ID_RE.fullmatch(experiment_id):
        raise ExpctlError(
            "experiment ID must look like YYYYMMDD-lowercase-short-name"
        )
    return repo / config.root / "requests" / f"{experiment_id}.toml"


def result_dir(repo: Path, config: Config, experiment_id: str) -> Path:
    return repo / config.root / "results" / experiment_id


def _table(data: dict[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key)
    if not isinstance(value, dict):
        raise ExpctlError(f"missing or invalid [{key}] table")
    return value


def _text(data: dict[str, Any], key: str, context: str = "request") -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ExpctlError(f"{context}.{key} must be a non-empty string")
    return value


def _relative_path(value: str, field: str) -> PurePosixPath:
    normalized = value.replace("\\", "/")
    path = PurePosixPath(normalized)
    if path.is_absolute() or ".." in path.parts or ":" in normalized:
        raise ExpctlError(f"{field} must stay inside the repository: {value}")
    return path


def _git_text(repo: Path, *args: str) -> str:
    return _run(["git", *args], cwd=repo).stdout


def load_request(
    repo: Path,
    config: Config,
    experiment_id: str,
    *,
    check_git: bool = True,
) -> tuple[dict[str, Any], Path]:
    path = request_path(repo, config, experiment_id)
    if not path.is_file():
        raise ExpctlError(f"request not found: {path}")
    try:
        with path.open("rb") as handle:
            data = tomllib.load(handle)
    except tomllib.TOMLDecodeError as exc:
        raise ExpctlError(f"invalid TOML in {path}: {exc}") from exc
    validate_request(data, path=path, repo=repo, config=config, check_git=check_git)
    return data, path


def validate_request(
    data: dict[str, Any],
    *,
    path: Path,
    repo: Path,
    config: Config,
    check_git: bool = True,
) -> None:
    if data.get("version") != 1:
        raise ExpctlError("request.version must be 1")

    experiment_id = _text(data, "id")
    if not ID_RE.fullmatch(experiment_id):
        raise ExpctlError("request.id must look like YYYYMMDD-lowercase-short-name")
    if path.stem != experiment_id:
        raise ExpctlError(f"request.id must match filename: {path.name}")
    for key in ("title", "question", "decision_rule"):
        _text(data, key)

    code = _table(data, "code")
    commit = _text(code, "commit", "code")
    _text(code, "branch", "code")
    worktree = _text(code, "worktree", "code")
    if not COMMIT_RE.fullmatch(commit):
        raise ExpctlError("code.commit must be a full 40-character Git commit")
    if not SAFE_NAME_RE.fullmatch(worktree):
        raise ExpctlError("code.worktree must be a safe directory basename")

    slurm = _table(data, "slurm")
    script = _text(slurm, "script", "slurm")
    _relative_path(script, "slurm.script")
    max_nodes = slurm.get("max_concurrent_nodes")
    if not isinstance(max_nodes, int) or isinstance(max_nodes, bool):
        raise ExpctlError("slurm.max_concurrent_nodes must be an integer")
    if max_nodes < 1:
        raise ExpctlError("slurm.max_concurrent_nodes must be at least 1")
    if config.max_total_nodes and max_nodes > config.max_total_nodes:
        raise ExpctlError(
            "slurm.max_concurrent_nodes must be between 1 and "
            f"{config.max_total_nodes} (scheduler.max_total_nodes)"
        )

    env = slurm.get("env", {})
    if not isinstance(env, dict):
        raise ExpctlError("slurm.env must be a table")
    for key, value in env.items():
        if not ENV_RE.fullmatch(key) or key == "GROUPS":
            raise ExpctlError(f"unsafe or reserved environment name: {key}")
        if not isinstance(value, str):
            raise ExpctlError(f"slurm.env.{key} must be a quoted string")
        if any(character in value for character in (",", "\n", "\r")):
            raise ExpctlError(f"slurm.env.{key} cannot contain commas or newlines")

    outputs = _table(data, "outputs")
    log_glob = _text(outputs, "log_glob", "outputs")
    if "{job_id}" not in log_glob:
        raise ExpctlError("outputs.log_glob must contain {job_id}")
    _relative_path(log_glob.format(job_id="123"), "outputs.log_glob")
    metrics = outputs.get("metrics")
    if not isinstance(metrics, list) or not metrics or not all(
        isinstance(metric, str) and metric for metric in metrics
    ):
        raise ExpctlError("outputs.metrics must be a non-empty string array")

    notes = data.get("notes", {})
    if not isinstance(notes, dict):
        raise ExpctlError("notes must be a table")
    requirements = notes.get("requirements", [])
    if not isinstance(requirements, list) or not all(
        isinstance(item, str) for item in requirements
    ):
        raise ExpctlError("notes.requirements must be a string array")
    for item in requirements:
        _relative_path(item, "notes.requirements")

    if check_git:
        _run(["git", "cat-file", "-e", f"{commit}^{{commit}}"], cwd=repo)
        script_lines = _git_text(repo, "show", f"{commit}:{script}").splitlines()
        for required in config.required_script_lines:
            if required not in script_lines:
                raise ExpctlError(
                    f"{script} at {commit[:12]} is missing the required line: "
                    f"{required}"
                )


def _request_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _default_worktree(repo: Path, config: Config, request: dict[str, Any]) -> Path:
    root = (repo / config.worktree_root).resolve()
    return root / _table(request, "code")["worktree"]


def _ensure_worktree(repo: Path, worktree: Path, commit: str) -> None:
    if worktree.exists():
        if not (worktree / ".git").exists():
            raise ExpctlError(f"existing path is not a Git worktree: {worktree}")
        actual = _git_text(worktree, "rev-parse", "HEAD").strip()
        if actual != commit:
            raise ExpctlError(
                f"worktree {worktree} is at {actual}, expected {commit}; "
                "choose a new request/worktree name"
            )
        return
    _run(
        ["git", "worktree", "add", "--detach", str(worktree), commit],
        cwd=repo,
    )


def _ensure_runtime_links(repo: Path, config: Config, worktree: Path) -> dict[str, str]:
    """Symlink the shared runtime directories into the worktree.

    Returns {name: "linked" | "already-linked" | "kept-checkout-dir" | "absent"}.
    An output directory (one listed in `create_missing`, e.g. `logs/`) that the
    checkout itself materialised -- tracked log files, say -- is left in place:
    the job then writes into the worktree copy, which is where `collect` reads.
    An input directory in that state is an error, because the job would read
    the checkout's copy instead of the shared data.
    """
    status: dict[str, str] = {}
    for name in config.shared_dirs:
        source = repo / name
        destination = worktree / name
        if name in config.create_missing:
            source.mkdir(exist_ok=True)
        if not source.exists():
            status[name] = "absent"
            continue
        if destination.is_symlink() or destination.exists():
            try:
                same = destination.resolve() == source.resolve()
            except OSError:
                same = False
            if same:
                status[name] = "already-linked"
                continue
            if (
                name in config.create_missing
                and destination.is_dir()
                and not destination.is_symlink()
            ):
                status[name] = "kept-checkout-dir"
                continue
            raise ExpctlError(f"runtime link destination already exists: {destination}")
        destination.symlink_to(source.resolve(), target_is_directory=True)
        status[name] = "linked"
    return status


def _check_requirements(worktree: Path, request: dict[str, Any]) -> None:
    requirements = request.get("notes", {}).get("requirements", [])
    missing = [item for item in requirements if not (worktree / item).exists()]
    if missing:
        rendered = "\n  - ".join(missing)
        raise ExpctlError(f"required runtime paths are missing:\n  - {rendered}")


def _slurm_reservations(repo: Path) -> tuple[set[str], int]:
    user = os.environ.get("USER") or getpass.getuser()
    result = _run(
        [
            "squeue",
            "-r",
            "-u",
            user,
            "-h",
            "-t",
            "PENDING,RUNNING,COMPLETING",
            "-o",
            "%T|%N|%D",
        ],
        cwd=repo,
    )
    active_nodes: set[str] = set()
    pending_nodes = 0
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        state, node_expression, node_count_text = line.split("|", 2)
        node_count = int(node_count_text.strip())
        if state.strip() == "PENDING":
            pending_nodes += node_count
            continue
        expression = node_expression.strip()
        if not expression or expression in {"(null)", "N/A"}:
            raise ExpctlError("could not determine nodes for an active SLURM job")
        expanded = _run(
            ["scontrol", "show", "hostnames", expression], cwd=repo
        ).stdout.splitlines()
        active_nodes.update(node.strip() for node in expanded if node.strip())
    return active_nodes, pending_nodes


def _check_node_budget(repo: Path, config: Config, requested: int) -> dict[str, int]:
    active_nodes, pending_nodes = _slurm_reservations(repo)
    reserved = len(active_nodes) + pending_nodes
    if reserved + requested > config.max_total_nodes:
        raise ExpctlError(
            f"SLURM node budget would exceed {config.max_total_nodes}: "
            f"active={len(active_nodes)}, pending_reservations={pending_nodes}, "
            f"request={requested}. Wait or submit manually with an approved hold."
        )
    return {
        "active_nodes": len(active_nodes),
        "pending_reservations": pending_nodes,
        "requested_nodes": requested,
    }


def build_sbatch_command(request: dict[str, Any]) -> list[str]:
    slurm = _table(request, "slurm")
    env = slurm.get("env", {})
    command = ["sbatch", "--parsable"]
    if env:
        exports = ",".join(f"{key}={value}" for key, value in sorted(env.items()))
        command.append(f"--export=ALL,{exports}")
    command.append(slurm["script"])
    return command


def submit_request(
    repo: Path,
    config: Config,
    experiment_id: str,
    *,
    dry_run: bool,
    skip_node_check: bool,
    worktree_root: Path | None,
) -> dict[str, Any]:
    request, path = load_request(repo, config, experiment_id)
    receipt_path = result_dir(repo, config, experiment_id) / "receipt.json"
    if receipt_path.exists():
        raise ExpctlError(
            f"receipt already exists: {receipt_path}; use a new request ID for reruns"
        )

    code = _table(request, "code")
    worktree = (
        worktree_root.resolve() / code["worktree"]
        if worktree_root is not None
        else _default_worktree(repo, config, request)
    )
    command = build_sbatch_command(request)
    preview = {
        "experiment_id": experiment_id,
        "commit": code["commit"],
        "worktree": str(worktree),
        "command": command,
        "max_concurrent_nodes": request["slurm"]["max_concurrent_nodes"],
    }
    if dry_run:
        return preview

    _ensure_worktree(repo, worktree, code["commit"])
    runtime_dirs = _ensure_runtime_links(repo, config, worktree)
    _check_requirements(worktree, request)
    budget = None
    if config.max_total_nodes and not skip_node_check:
        budget = _check_node_budget(
            repo, config, request["slurm"]["max_concurrent_nodes"]
        )
    result = _run(command, cwd=worktree)
    job_id = result.stdout.strip().split(";", 1)[0]
    if not job_id or not re.fullmatch(r"\d+(?:_\d+)?", job_id):
        raise ExpctlError(f"could not parse sbatch job ID from: {result.stdout!r}")

    receipt = {
        "schema_version": 1,
        "experiment_id": experiment_id,
        "request_sha256": _request_hash(path),
        "branch_label": code["branch"],
        "commit": code["commit"],
        "worktree": str(worktree),
        "job_id": job_id,
        "submitted_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "submitted_by": getpass.getuser(),
        "status": "submitted",
        "node_budget": budget,
        "runtime_dirs": runtime_dirs,
        "sbatch_command": command,
    }
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return receipt


def _queue_status(repo: Path, job_id: str) -> list[dict[str, str]]:
    result = _run(
        ["squeue", "-r", "-j", job_id, "-h", "-o", "%i|%T|%r"],
        cwd=repo,
        check=False,
    )
    if result.returncode != 0:
        return []
    rows: list[dict[str, str]] = []
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        job, state, reason = (line.split("|", 2) + ["", ""])[:3]
        rows.append(
            {"job": job.strip(), "state": state.strip(), "reason": reason.strip()}
        )
    return rows


def status_request(repo: Path, config: Config, experiment_id: str) -> dict[str, Any]:
    load_request(repo, config, experiment_id, check_git=False)
    receipt_path = result_dir(repo, config, experiment_id) / "receipt.json"
    if not receipt_path.is_file():
        raise ExpctlError(
            f"not submitted yet: no receipt at {receipt_path}"
        )
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    queue = _queue_status(repo, receipt["job_id"])
    counts: dict[str, int] = {}
    for row in queue:
        counts[row["state"]] = counts.get(row["state"], 0) + 1
    payload: dict[str, Any] = {
        "experiment_id": experiment_id,
        "job_id": receipt["job_id"],
        "receipt_status": receipt.get("status"),
        "in_queue": bool(queue),
        "queue_counts": counts,
        "queue": queue,
    }
    if not queue:
        payload["accounting"] = _scheduler_status(repo, receipt["job_id"])
    return payload


def extract_metrics(text: str, names: list[str]) -> dict[str, float]:
    wanted = set(names)
    extracted: dict[str, float] = {}
    for match in METRIC_LINE_RE.finditer(text):
        key, value = match.groups()
        if key in wanted:
            extracted[key] = float(value)
    return extracted


def _scheduler_status(repo: Path, job_id: str) -> dict[str, Any]:
    result = _run(
        [
            "sacct",
            "-j",
            job_id,
            "--parsable2",
            "--noheader",
            "--format=JobIDRaw,State,ExitCode",
        ],
        cwd=repo,
        check=False,
    )
    if result.returncode != 0:
        return {"state": "UNKNOWN", "detail": result.stderr.strip()}
    rows = []
    for line in result.stdout.splitlines():
        fields = line.split("|")
        if len(fields) >= 3 and "." not in fields[0]:
            rows.append(
                {"job_id": fields[0], "state": fields[1], "exit_code": fields[2]}
            )
    states = sorted({row["state"] for row in rows})
    return {"state": ",".join(states) if states else "UNKNOWN", "jobs": rows}


def collect_request(repo: Path, config: Config, experiment_id: str) -> dict[str, Any]:
    request, path = load_request(repo, config, experiment_id)
    destination = result_dir(repo, config, experiment_id)
    receipt_path = destination / "receipt.json"
    if not receipt_path.is_file():
        raise ExpctlError(f"submission receipt not found: {receipt_path}")
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    if receipt.get("request_sha256") != _request_hash(path):
        raise ExpctlError(
            "request changed after submission; restore it before collecting results"
        )

    worktree = Path(receipt["worktree"])
    if not worktree.is_dir():
        raise ExpctlError(f"submission worktree is missing: {worktree}")
    pattern = request["outputs"]["log_glob"].format(job_id=receipt["job_id"])
    sources = sorted(worktree.glob(pattern))
    if not sources:
        raise ExpctlError(f"no logs match {worktree / pattern}")

    logs_dir = destination / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    metric_names = request["outputs"]["metrics"]
    metrics: dict[str, dict[str, float]] = {}
    copied: list[str] = []
    for source in sources:
        if not source.is_file():
            continue
        target = logs_dir / source.name
        shutil.copy2(source, target)
        copied.append(str(target.relative_to(repo)).replace("\\", "/"))
        metrics[source.name] = extract_metrics(
            source.read_text(encoding="utf-8", errors="replace"), metric_names
        )

    metrics_path = destination / "metrics.json"
    metrics_path.write_text(
        json.dumps(metrics, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    collection = {
        "collected_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "logs": copied,
        "metrics_file": str(metrics_path.relative_to(repo)).replace("\\", "/"),
        "scheduler": _scheduler_status(repo, receipt["job_id"]),
    }
    receipt["status"] = "collected"
    receipt["collection"] = collection
    receipt_path.write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return collection


def list_requests(repo: Path, config: Config) -> list[dict[str, str]]:
    requests_dir = repo / config.root / "requests"
    rows: list[dict[str, str]] = []
    for path in sorted(requests_dir.glob("*.toml")):
        experiment_id = path.stem
        try:
            data, _ = load_request(repo, config, experiment_id)
            receipt = result_dir(repo, config, experiment_id) / "receipt.json"
            status = "requested"
            if receipt.is_file():
                status = json.loads(receipt.read_text(encoding="utf-8")).get(
                    "status", "submitted"
                )
            rows.append(
                {"id": experiment_id, "status": status, "title": data["title"]}
            )
        except (ExpctlError, json.JSONDecodeError) as exc:
            rows.append({"id": experiment_id, "status": "invalid", "title": str(exc)})
    return rows


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="expctl",
        description="Manage repository-backed asynchronous experiment handoffs.",
    )
    parser.add_argument(
        "--version", action="version", version=f"expctl {__version__}"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser(
        "init", help=f"create {CONFIG_NAME} and the {DEFAULT_ROOT}/ skeleton"
    )

    subparsers.add_parser("list", help="list requests and their repository state")

    validate = subparsers.add_parser("validate", help="validate one request")
    validate.add_argument("experiment_id")

    show = subparsers.add_parser("show", help="print one validated request as JSON")
    show.add_argument("experiment_id")

    submit = subparsers.add_parser("submit", help="submit one request through SLURM")
    submit.add_argument("experiment_id")
    submit.add_argument("--dry-run", action="store_true")
    submit.add_argument(
        "--skip-node-check",
        action="store_true",
        help="submit without squeue evidence; requires explicit operator authorization",
    )
    submit.add_argument(
        "--worktree-root",
        type=Path,
        help="parent directory for detached experiment worktrees",
    )

    status = subparsers.add_parser(
        "status", help="report queue and accounting state for a submitted request"
    )
    status.add_argument("experiment_id")

    collect = subparsers.add_parser("collect", help="copy logs and extract metrics")
    collect.add_argument("experiment_id")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        repo = find_repo_root()
        if args.command == "init":
            created = init_repo(repo)
            for entry in created:
                print(f"created {entry}")
            if created:
                print(f"Edit {CONFIG_NAME} to set your cluster policy.")
            else:
                print("Nothing to do; expctl files already exist.")
            return 0
        config = load_config(repo)
        if args.command == "list":
            rows = list_requests(repo, config)
            if not rows:
                print("No experiment requests.")
            for row in rows:
                print(f"{row['id']}\t{row['status']}\t{row['title']}")
        elif args.command == "validate":
            load_request(repo, config, args.experiment_id)
            print(f"valid: {args.experiment_id}")
        elif args.command == "show":
            request, _ = load_request(repo, config, args.experiment_id)
            print(json.dumps(request, indent=2, sort_keys=True))
        elif args.command == "submit":
            result = submit_request(
                repo,
                config,
                args.experiment_id,
                dry_run=args.dry_run,
                skip_node_check=args.skip_node_check,
                worktree_root=args.worktree_root,
            )
            print(json.dumps(result, indent=2, sort_keys=True))
        elif args.command == "status":
            result = status_request(repo, config, args.experiment_id)
            print(json.dumps(result, indent=2, sort_keys=True))
        elif args.command == "collect":
            result = collect_request(repo, config, args.experiment_id)
            print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except ExpctlError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
