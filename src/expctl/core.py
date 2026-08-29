"""expctl: repository-backed asynchronous experiment handoff.

The whole tool lives in this one file on purpose: on a machine where nothing
can be installed, copy `core.py` anywhere and run `python core.py <command>`
(Python 3.11+, stdlib only).

Protocol: an immutable request file (`<root>/requests/<id>.toml`) pins the
exact commit, entrypoint, and resource envelope of a run. Submitting writes a
receipt (`<root>/results/<id>/receipt.json`); collecting copies logs and
scrapes metrics next to it. State is defined by which files exist — requests
are never edited after submission; running one again is a new request
(`expctl rerun` copies it under a fresh ID). `<root>` is `expctl/` unless
`expctl.toml` says otherwise; that file also holds the per-repository policy
(required scheduler flags, node ceiling, shared runtime directories).
"""

from __future__ import annotations

import argparse
import contextlib
import dataclasses
import datetime as dt
import getpass
import hashlib
import json
import os
import re
import shlex
import shutil
import string
import subprocess
import sys
import tempfile
import time
import tomllib
import unicodedata
import uuid
from collections.abc import Iterator
from pathlib import Path, PurePosixPath
from typing import Any

try:  # submit only runs on POSIX, but the read-only commands support Windows.
    import fcntl
except ImportError:  # pragma: no cover - exercised by Windows users, not POSIX CI
    fcntl = None  # type: ignore[assignment]


# Kept in sync with pyproject.toml by tests; duplicated here so the single-file
# copy still knows its version.
__version__ = "0.8.0"

CONFIG_NAME = "expctl.toml"
DEFAULT_ROOT = "expctl"
DEFAULT_SHARED_DIRS = (".venv", "data", "runs", "logs")
DEFAULT_CREATE_MISSING = ("runs", "logs")

ID_RE = re.compile(r"^\d{8}-[a-z0-9][a-z0-9-]*$")
COMMIT_RE = re.compile(r"^[0-9a-fA-F]{40}$")
ENV_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
METRIC_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]*$")
SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9.][A-Za-z0-9._-]*$")
RERUN_SUFFIX_RE = re.compile(r"-r\d+$")
# The top-level `id = "..."` line of a request file, as `rerun` rewrites it.
ID_LINE_RE = re.compile(r"""^(\s*)id\s*=\s*(['"])[^'"]*\2\s*(#.*)?$""")
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
# #SBATCH options here are also passed on the command line so the script cannot
# override approved partitions, accounts, or QOS flags later.
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
# Audited upper bound. The script must declare numeric #SBATCH --nodes;
# numeric array ranges and their %N throttle are included in this bound.
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
_BUILTIN_TEMPLATE = tomllib.loads(STARTER_TEMPLATE)
# Fields whose starter-template defaults must be replaced before a request is
# usable; `validate` rejects a request that still carries any of them.
PLACEHOLDER_FIELDS: tuple[tuple[str, ...], ...] = (
    ("title",),
    ("question",),
    ("decision_rule",),
    ("slurm", "script"),
    ("outputs", "log_glob"),
    ("outputs", "metrics"),
    ("notes", "requirements"),
)


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
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            args,
            cwd=cwd,
            check=False,
            text=True,
            capture_output=True,
            env=env,
        )
    except FileNotFoundError as exc:
        raise ExpctlError(f"required command not found: {args[0]}") from exc
    if check and result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise ExpctlError(f"command failed ({' '.join(args)}): {detail}")
    return result


def _plain_name(value: str, field: str) -> str:
    if value in {".", ".."} or not SAFE_NAME_RE.fullmatch(value):
        raise ExpctlError(f"{field} must be a plain directory name: {value}")
    return value


def _atomic_write_text(path: Path, content: str) -> None:
    """Replace a text file atomically after flushing its complete contents."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except OSError as exc:
        raise ExpctlError(f"could not atomically write {path}: {exc}") from exc
    finally:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    _atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _atomic_copy2(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=target.parent,
            prefix=f".{target.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
        shutil.copy2(source, temporary)
        os.replace(temporary, target)
    except OSError as exc:
        raise ExpctlError(f"could not copy {source} to {target}: {exc}") from exc
    finally:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass


def _exclusive_copy2(source: Path, target: Path) -> None:
    """Copy a file only if the destination does not already exist."""
    created = False
    try:
        with source.open("rb") as source_handle, target.open("xb") as target_handle:
            created = True
            shutil.copyfileobj(source_handle, target_handle)
            target_handle.flush()
            os.fsync(target_handle.fileno())
        shutil.copystat(source, target)
    except FileExistsError as exc:
        raise ExpctlError(
            f"file already exists and will not be overwritten: {target}"
        ) from exc
    except OSError as exc:
        if created:
            try:
                target.unlink(missing_ok=True)
            except OSError:
                pass
        raise ExpctlError(f"could not copy {source} to {target}: {exc}") from exc


def _exclusive_write_text(path: Path, content: str) -> None:
    """Create a file once; concurrent callers cannot overwrite the winner."""
    path.parent.mkdir(parents=True, exist_ok=True)
    created = False
    try:
        handle = path.open("x", encoding="utf-8", newline="")
        created = True
        with handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as exc:
        raise ExpctlError(f"file already exists: {path}") from exc
    except OSError as exc:
        if created:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
        raise ExpctlError(f"could not create {path}: {exc}") from exc


def _load_json_object(path: Path, context: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ExpctlError(f"unreadable {context} at {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ExpctlError(f"invalid {context} at {path}: expected a JSON object")
    return payload


def _load_receipt(path: Path) -> dict[str, Any]:
    return _load_json_object(path, "submission receipt")


def find_repo_root(start: Path | None = None) -> Path:
    here = (start or Path.cwd()).resolve()
    result = _run(["git", "rev-parse", "--show-toplevel"], cwd=here, check=False)
    if result.returncode != 0:
        raise ExpctlError(f"not inside a Git repository: {here}")
    return Path(result.stdout.strip()).resolve()


def _lookup(data: dict[str, Any], keys: tuple[str, ...]) -> Any:
    value: Any = data
    for key in keys:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def _template_placeholders(data: dict[str, Any]) -> list[str]:
    """Names of fields still equal to their starter-template placeholder."""
    return [
        ".".join(keys)
        for keys in PLACEHOLDER_FIELDS
        if _lookup(data, keys) is not None
        and _lookup(data, keys) == _lookup(_BUILTIN_TEMPLATE, keys)
    ]


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
    resolved_data_root = _resolved_repo_path(repo, data_root, "paths.root")
    data_root = resolved_data_root.relative_to(repo.resolve()).as_posix()

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
        _plain_name(name, "runtime.shared_dirs entry")
    for name in create_missing:
        if name not in shared:
            raise ExpctlError(
                f"runtime.create_missing entry must also be in shared_dirs: {name}"
            )

    root = tables["worktree"].get("root", "..")
    if not isinstance(root, str) or not root.strip():
        raise ExpctlError("worktree.root must be a non-empty string")

    required_script_lines = _config_list(
        tables["scheduler"], "required_script_lines", "scheduler", ()
    )
    _required_sbatch_options(required_script_lines)

    return Config(
        root=data_root,
        required_script_lines=required_script_lines,
        max_total_nodes=max_total,
        shared_dirs=shared,
        create_missing=create_missing,
        worktree_root=root,
    )


def init_repo(repo: Path) -> list[str]:
    config_path = repo / CONFIG_NAME
    root_name = load_config(repo).root if config_path.is_file() else DEFAULT_ROOT
    root = _resolved_repo_path(repo, root_name, "paths.root")
    entries = (
        (config_path, STARTER_CONFIG),
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
        raise ExpctlError("experiment ID must look like YYYYMMDD-lowercase-short-name")
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


def _resolved_repo_path(repo: Path, value: str, field: str) -> Path:
    """Resolve a repository-relative path without allowing symlink escapes."""
    relative = _relative_path(value, field)
    repository = repo.resolve()
    resolved = repository.joinpath(*relative.parts).resolve()
    try:
        resolved.relative_to(repository)
    except ValueError as exc:
        raise ExpctlError(
            f"{field} must resolve inside the repository: {value}"
        ) from exc
    return resolved


def _git_text(repo: Path, *args: str) -> str:
    return _run(["git", *args], cwd=repo).stdout


def _slurm_preamble(script_lines: list[str]) -> list[str]:
    preamble: list[str] = []
    for line in script_lines:
        stripped = line.lstrip()
        if stripped and not stripped.startswith("#"):
            break
        preamble.append(line)
    return preamble


def _sbatch_option(
    script_lines: list[str], long_name: str, short_name: str
) -> str | None:
    """Return the last value for one option in pinned #SBATCH directives."""
    value: str | None = None
    for line in _slurm_preamble(script_lines):
        stripped = line.lstrip()
        if not stripped:
            continue
        if not stripped.startswith("#SBATCH"):
            continue
        try:
            tokens = shlex.split(
                stripped[len("#SBATCH") :].strip(), comments=True, posix=True
            )
        except ValueError as exc:
            raise ExpctlError(f"invalid #SBATCH directive {line!r}: {exc}") from exc
        index = 0
        while index < len(tokens):
            token = tokens[index]
            inline: str | None = None
            if token.startswith(f"{long_name}="):
                inline = token.split("=", 1)[1]
            elif token == long_name or token == short_name:
                index += 1
                if index >= len(tokens):
                    raise ExpctlError(f"{token} in #SBATCH requires a value")
                inline = tokens[index]
            elif (
                token.startswith(short_name)
                and token != short_name
                and not token.startswith("--")
            ):
                inline = token[len(short_name) :].removeprefix("=")
            if inline is not None:
                if not inline:
                    raise ExpctlError(
                        f"{long_name} in #SBATCH requires a non-empty value"
                    )
                value = inline
            index += 1
    return value


def _array_task_count(specification: str) -> tuple[int, int]:
    """Return (number of tasks, maximum simultaneously runnable tasks)."""
    expression, separator, throttle_text = specification.rpartition("%")
    if not separator:
        expression, throttle_text = specification, ""
    elif not throttle_text.isdigit() or int(throttle_text) < 1:
        raise ExpctlError(f"invalid #SBATCH --array throttle: {specification}")

    count = 0
    for item in expression.split(","):
        match = re.fullmatch(r"(\d+)(?:-(\d+)(?::(\d+))?)?", item)
        if not match:
            raise ExpctlError(
                "#SBATCH --array must use numeric indexes/ranges so its node "
                f"envelope can be verified: {specification}"
            )
        start = int(match.group(1))
        if match.group(2) is None:
            count += 1
            continue
        stop = int(match.group(2))
        step = int(match.group(3) or "1")
        if stop < start or step < 1:
            raise ExpctlError(f"invalid #SBATCH --array range: {item}")
        count += (stop - start) // step + 1
    if count < 1:
        raise ExpctlError("#SBATCH --array must contain at least one task")
    throttle = int(throttle_text) if throttle_text else count
    return count, min(count, throttle)


def _script_max_concurrent_nodes(script_lines: list[str], script: str) -> int:
    for line in _slurm_preamble(script_lines):
        if re.match(r"^\s*#SBATCH\s+(?:hetjob|packjob)\b", line):
            raise ExpctlError(
                f"heterogeneous #SBATCH jobs are not supported in {script}; "
                "use separate expctl requests so the node envelope is auditable"
            )
    nodes_text = _sbatch_option(script_lines, "--nodes", "-N")
    if nodes_text is None:
        raise ExpctlError(
            f"{script} must declare an explicit integer #SBATCH --nodes value "
            "so the node envelope can be verified"
        )
    match = re.fullmatch(r"(\d+)(?:-(\d+))?", nodes_text)
    if not match:
        raise ExpctlError(
            f"unsupported #SBATCH --nodes value in {script}: {nodes_text}"
        )
    minimum = int(match.group(1))
    maximum = int(match.group(2) or match.group(1))
    if minimum < 1 or maximum < minimum:
        raise ExpctlError(f"invalid #SBATCH --nodes value in {script}: {nodes_text}")

    array_text = _sbatch_option(script_lines, "--array", "-a")
    concurrent_tasks = 1
    if array_text is not None:
        _, concurrent_tasks = _array_task_count(array_text)
    return maximum * concurrent_tasks


def _required_sbatch_options(required_lines: tuple[str, ...]) -> list[str]:
    """Turn required #SBATCH policy lines into command-line overrides."""
    options: list[str] = []
    for line in required_lines:
        stripped = line.lstrip()
        if not stripped.startswith("#SBATCH"):
            continue
        try:
            tokens = shlex.split(
                stripped[len("#SBATCH") :].strip(), comments=True, posix=True
            )
        except ValueError as exc:
            raise ExpctlError(
                f"invalid scheduler.required_script_lines entry {line!r}: {exc}"
            ) from exc
        if not tokens or not tokens[0].startswith("-"):
            raise ExpctlError(
                "scheduler.required_script_lines #SBATCH entries must contain "
                f"scheduler options: {line}"
            )
        options.extend(tokens)
    return options


def load_request(
    repo: Path,
    config: Config,
    experiment_id: str,
    *,
    check_git: bool = True,
    check_placeholders: bool = True,
) -> tuple[dict[str, Any], Path]:
    path = request_path(repo, config, experiment_id)
    if not path.is_file():
        raise ExpctlError(f"request not found: {path}")
    try:
        with path.open("rb") as handle:
            data = tomllib.load(handle)
    except tomllib.TOMLDecodeError as exc:
        raise ExpctlError(f"invalid TOML in {path}: {exc}") from exc
    validate_request(
        data,
        path=path,
        repo=repo,
        config=config,
        check_git=check_git,
        check_placeholders=check_placeholders,
    )
    return data, path


def validate_request(
    data: dict[str, Any],
    *,
    path: Path,
    repo: Path,
    config: Config,
    check_git: bool = True,
    check_placeholders: bool = True,
) -> None:
    if data.get("version") != 1:
        raise ExpctlError("request.version must be 1")

    experiment_id = _text(data, "id")
    if not ID_RE.fullmatch(experiment_id):
        raise ExpctlError("request.id must look like YYYYMMDD-lowercase-short-name")
    if path.stem != experiment_id:
        raise ExpctlError(f"request.id must match filename: {path.name}")
    rerun_of = data.get("rerun_of")
    if rerun_of is not None and (
        not isinstance(rerun_of, str) or not ID_RE.fullmatch(rerun_of)
    ):
        raise ExpctlError("request.rerun_of must be an experiment ID")
    if "rerun_reason" in data and not isinstance(data["rerun_reason"], str):
        raise ExpctlError("request.rerun_reason must be a string")
    for key in ("title", "question", "decision_rule"):
        _text(data, key)

    code = _table(data, "code")
    commit = _text(code, "commit", "code")
    _text(code, "branch", "code")
    worktree = _text(code, "worktree", "code")
    if not COMMIT_RE.fullmatch(commit):
        raise ExpctlError("code.commit must be a full 40-character Git commit")
    _plain_name(worktree, "code.worktree")

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
    try:
        placeholders = [
            (field, format_spec, conversion)
            for _, field, format_spec, conversion in string.Formatter().parse(log_glob)
            if field is not None
        ]
    except ValueError as exc:
        raise ExpctlError(
            f"invalid outputs.log_glob placeholder syntax: {exc}"
        ) from exc
    if not any(field == "job_id" for field, _, _ in placeholders):
        raise ExpctlError("outputs.log_glob must contain {job_id}")
    unsupported = [
        field
        for field, format_spec, conversion in placeholders
        if field != "job_id" or format_spec or conversion
    ]
    if unsupported:
        rendered = ", ".join(f"{{{field}}}" for field in unsupported)
        raise ExpctlError(
            "outputs.log_glob supports only the plain {job_id} placeholder; "
            f"unsupported: {rendered}"
        )
    _relative_path(log_glob.format(job_id="123"), "outputs.log_glob")
    metrics = outputs.get("metrics")
    if (
        not isinstance(metrics, list)
        or not metrics
        or not all(isinstance(metric, str) and metric for metric in metrics)
    ):
        raise ExpctlError("outputs.metrics must be a non-empty string array")
    invalid_metrics = [
        metric for metric in metrics if METRIC_NAME_RE.fullmatch(metric) is None
    ]
    if invalid_metrics:
        raise ExpctlError(
            "outputs.metrics entries must match [A-Za-z_][A-Za-z0-9_.-]*: "
            + ", ".join(invalid_metrics)
        )

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

    if check_placeholders:
        placeholders = _template_placeholders(data)
        if placeholders:
            raise ExpctlError(
                "request still has template placeholder values in "
                + ", ".join(placeholders)
                + f"; edit {path.name} before validating"
            )

    if check_git:
        _validate_request_git(repo, config, data)


def _validate_request_git(
    repo: Path,
    config: Config,
    request: dict[str, Any],
    *,
    commit_cache: dict[str, str | None] | None = None,
    script_cache: dict[tuple[str, str], tuple[int | None, str | None]] | None = None,
) -> int:
    """Validate pinned Git content, optionally reusing results across requests.

    Returns the worst-case concurrent node count derived from the script.
    """
    commit = request["code"]["commit"]
    branch = request["code"]["branch"]
    script = request["slurm"]["script"]
    max_nodes = request["slurm"]["max_concurrent_nodes"]
    commit_cache = {} if commit_cache is None else commit_cache
    script_cache = {} if script_cache is None else script_cache

    if commit not in commit_cache:
        probe = _run(
            ["git", "cat-file", "-e", f"{commit}^{{commit}}"], cwd=repo, check=False
        )
        commit_cache[commit] = (
            None
            if probe.returncode == 0
            else (
                f"commit {commit[:12]} is not in this clone; fetch the ref that "
                f"contains it (git fetch origin {branch}) and retry"
            )
        )
    if error := commit_cache[commit]:
        raise ExpctlError(error)

    cache_key = (commit, script)
    if cache_key not in script_cache:
        try:
            shown = _run(["git", "show", f"{commit}:{script}"], cwd=repo, check=False)
            if shown.returncode != 0:
                detail = shown.stderr.strip() or shown.stdout.strip()
                if "does not exist" in detail or "exists on disk" in detail:
                    raise ExpctlError(
                        f"slurm.script {script} does not exist at commit "
                        f"{commit[:12]}; commit and push the script, then pin "
                        "that commit"
                    )
                raise ExpctlError(
                    f"command failed (git show {commit}:{script}): {detail}"
                )
            script_lines = shown.stdout.splitlines()
            active_script_lines = _slurm_preamble(script_lines)
            for required in config.required_script_lines:
                if required not in active_script_lines:
                    raise ExpctlError(
                        f"{script} at {commit[:12]} is missing the required line: "
                        f"{required}"
                    )
            verified_nodes = _script_max_concurrent_nodes(script_lines, script)
        except ExpctlError as exc:
            script_cache[cache_key] = (None, str(exc))
        else:
            script_cache[cache_key] = (verified_nodes, None)
    verified_nodes, error = script_cache[cache_key]
    if error:
        raise ExpctlError(error)
    assert verified_nodes is not None
    if verified_nodes > max_nodes:
        raise ExpctlError(
            f"{script} can use {verified_nodes} concurrent nodes, exceeding "
            f"slurm.max_concurrent_nodes={max_nodes}"
        )
    return verified_nodes


def _request_hash(path: Path) -> str:
    try:
        content = path.read_bytes()
    except OSError as exc:
        raise ExpctlError(
            f"could not read request for hashing at {path}: {exc}"
        ) from exc
    return hashlib.sha256(content).hexdigest()


def _new_worktree_name(repo: Path, experiment_id: str) -> str:
    repository_name = re.sub(r"[^A-Za-z0-9._-]+", "-", repo.name).strip("-._")
    repository_name = repository_name.lower() or "experiment"
    short_name = experiment_id.split("-", 1)[1]
    return _plain_name(f"{repository_name}-{short_name}", "generated code.worktree")


def _uncommitted_changes(repo: Path) -> list[str]:
    result = _run(
        [
            "git",
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
            "--ignore-submodules=none",
        ],
        cwd=repo,
    )
    return [line for line in result.stdout.splitlines() if line]


def _render_new_request(
    template: str,
    *,
    experiment_id: str,
    branch: str,
    commit: str,
    worktree: str,
) -> str:
    replacements = {
        ("", "id"): experiment_id,
        ("code", "branch"): branch,
        ("code", "commit"): commit,
        ("code", "worktree"): worktree,
    }
    found: set[tuple[str, str]] = set()
    output: list[str] = []
    section = ""
    for line in template.splitlines(keepends=True):
        body = line.rstrip("\r\n")
        newline = line[len(body) :]
        section_match = re.match(r"^\s*\[([^]]+)]\s*(?:#.*)?$", body)
        if section_match:
            section = section_match.group(1).strip()
            output.append(line)
            continue
        assignment = re.match(r"^(\s*)([A-Za-z0-9_.-]+)\s*=", body)
        key = (section, assignment.group(2)) if assignment else None
        if assignment and key in replacements:
            value = json.dumps(replacements[key], ensure_ascii=False)
            output.append(f"{assignment.group(1)}{key[1]} = {value}{newline}")
            found.add(key)
        else:
            output.append(line)

    missing = [
        f"{section_name + '.' if section_name else ''}{key}"
        for section_name, key in replacements
        if (section_name, key) not in found
    ]
    if missing:
        raise ExpctlError(
            "request template is missing fields required by `expctl new`: "
            + ", ".join(missing)
        )
    return "".join(output)


def new_request(
    repo: Path,
    config: Config,
    experiment_id: str,
    *,
    allow_dirty: bool = False,
) -> dict[str, Any]:
    destination = request_path(repo, config, experiment_id)
    if destination.exists():
        raise ExpctlError(f"request already exists: {destination}")
    uncommitted = _uncommitted_changes(repo)
    if uncommitted and not allow_dirty:
        preview = "\n  - ".join(uncommitted[:10])
        suffix = "\n  - ..." if len(uncommitted) > 10 else ""
        raise ExpctlError(
            "uncommitted changes are not included in the pinned HEAD commit:\n"
            f"  - {preview}{suffix}\n"
            "commit or stash them first, or rerun with --allow-dirty"
        )
    template_path = repo / config.root / "templates" / "request.toml"
    try:
        template = template_path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise ExpctlError(
            f"could not read request template at {template_path}; run `expctl init`: {exc}"
        ) from exc

    commit = _git_text(repo, "rev-parse", "HEAD").strip()
    if not COMMIT_RE.fullmatch(commit):
        raise ExpctlError(f"Git returned an invalid HEAD commit: {commit!r}")
    branch_result = _run(
        ["git", "symbolic-ref", "--quiet", "--short", "HEAD"],
        cwd=repo,
        check=False,
    )
    branch = branch_result.stdout.strip() or "detached-head"
    worktree = _new_worktree_name(repo, experiment_id)
    content = _render_new_request(
        template,
        experiment_id=experiment_id,
        branch=branch,
        commit=commit,
        worktree=worktree,
    )
    _exclusive_write_text(destination, content)
    try:
        load_request(
            repo, config, experiment_id, check_git=False, check_placeholders=False
        )
    except ExpctlError:
        destination.unlink(missing_ok=True)
        raise
    return {
        "experiment_id": experiment_id,
        "path": str(destination.relative_to(repo)).replace("\\", "/"),
        "branch": branch,
        "commit": commit,
        "worktree": worktree,
        "uncommitted_changes": uncommitted,
    }


def _doctor_entry(
    name: str,
    ok: bool,
    detail: str,
    *,
    scope: str,
    required: bool = True,
) -> dict[str, Any]:
    return {
        "name": name,
        "ok": ok,
        "detail": detail,
        "scope": scope,
        "required": required,
    }


def doctor_repo(repo: Path) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    git_path = shutil.which("git")
    checks.append(
        _doctor_entry(
            "Git",
            git_path is not None,
            git_path or "not found on PATH",
            scope="repository",
        )
    )

    config: Config | None = None
    try:
        config = load_config(repo)
    except ExpctlError as exc:
        checks.append(
            _doctor_entry("Configuration", False, str(exc), scope="repository")
        )
    else:
        checks.append(
            _doctor_entry(
                "Configuration",
                True,
                str(repo / CONFIG_NAME),
                scope="repository",
            )
        )
        data_root = repo / config.root
        for name, path, kind in (
            ("Request template", data_root / "templates" / "request.toml", "file"),
            ("Requests directory", data_root / "requests", "directory"),
            ("Results directory", data_root / "results", "directory"),
        ):
            ok = path.is_file() if kind == "file" else path.is_dir()
            checks.append(
                _doctor_entry(
                    name,
                    ok,
                    str(path) if ok else f"missing {kind}: {path}",
                    scope="repository",
                )
            )

        worktree_root = (repo / config.worktree_root).resolve()
        location_safe = not (
            worktree_root == repo.resolve() or repo.resolve() in worktree_root.parents
        )
        writable_path = worktree_root
        while not writable_path.exists() and writable_path != writable_path.parent:
            writable_path = writable_path.parent
        writable = writable_path.is_dir() and os.access(writable_path, os.W_OK)
        root_ok = (
            location_safe
            and writable
            and not (worktree_root.exists() and not worktree_root.is_dir())
        )
        reasons: list[str] = []
        if not location_safe:
            reasons.append("must not be inside the repository")
        if not writable:
            reasons.append(f"nearest existing parent is not writable: {writable_path}")
        if worktree_root.exists() and not worktree_root.is_dir():
            reasons.append("path exists but is not a directory")
        checks.append(
            _doctor_entry(
                "Worktree root",
                root_ok,
                str(worktree_root) if root_ok else "; ".join(reasons),
                scope="repository",
            )
        )

    checks.append(
        _doctor_entry(
            "POSIX locking",
            fcntl is not None,
            "available" if fcntl is not None else "fcntl unavailable",
            scope="cluster",
        )
    )
    for command in ("sbatch", "squeue", "sacct", "scancel"):
        command_path = shutil.which(command)
        checks.append(
            _doctor_entry(
                command,
                command_path is not None,
                command_path or "not found on PATH",
                scope="cluster",
            )
        )
    scontrol_path = shutil.which("scontrol")
    scontrol_required = bool(config and config.max_total_nodes)
    checks.append(
        _doctor_entry(
            "scontrol",
            scontrol_path is not None,
            scontrol_path
            or (
                "not found on PATH"
                if scontrol_required
                else "not found; only required when node budgeting is enabled"
            ),
            scope="cluster",
            required=scontrol_required,
        )
    )

    repository_ready = all(
        check["ok"]
        for check in checks
        if check["scope"] == "repository" and check["required"]
    )
    cluster_ready = repository_ready and all(
        check["ok"]
        for check in checks
        if check["scope"] == "cluster" and check["required"]
    )
    return {
        "repository": str(repo),
        "repository_ready": repository_ready,
        "cluster_ready": cluster_ready,
        "checks": checks,
    }


def _worktree_path(
    repo: Path,
    config: Config,
    request: dict[str, Any],
    override_root: Path | None = None,
) -> Path:
    root = (
        override_root.resolve()
        if override_root is not None
        else (repo / config.worktree_root).resolve()
    )
    name = _plain_name(_table(request, "code")["worktree"], "code.worktree")
    worktree = (root / name).resolve()
    repo = repo.resolve()
    if worktree == repo or worktree in repo.parents or repo in worktree.parents:
        raise ExpctlError(
            f"experiment worktree must not overlap the main repository: {worktree}"
        )
    if worktree.parent != root:
        raise ExpctlError(
            f"experiment worktree escapes its configured root: {worktree}"
        )
    return worktree


def _git_common_dir(worktree: Path) -> Path:
    value = _git_text(worktree, "rev-parse", "--git-common-dir").strip()
    path = Path(value)
    return (worktree / path).resolve() if not path.is_absolute() else path.resolve()


def _check_worktree_clean(worktree: Path, allowed_untracked: tuple[str, ...]) -> None:
    result = _run(
        [
            "git",
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
            "--ignored=matching",
            "--ignore-submodules=none",
        ],
        cwd=worktree,
    )
    unsafe: list[str] = []
    for line in result.stdout.splitlines():
        if not line:
            continue
        status, path = line[:2], line[3:]
        if status in {"??", "!!"} and any(
            path == name or path.startswith(f"{name}/") for name in allowed_untracked
        ):
            continue
        unsafe.append(line)
    if unsafe:
        preview = "\n  - ".join(unsafe[:10])
        suffix = "\n  - ..." if len(unsafe) > 10 else ""
        raise ExpctlError(
            f"experiment worktree is not clean: {worktree}\n  - {preview}{suffix}"
        )


def _ensure_worktree(
    repo: Path,
    worktree: Path,
    commit: str,
    *,
    allowed_untracked: tuple[str, ...] = (),
) -> None:
    if worktree.exists():
        if not (worktree / ".git").exists():
            raise ExpctlError(f"existing path is not a Git worktree: {worktree}")
        top_level = Path(_git_text(worktree, "rev-parse", "--show-toplevel").strip())
        if top_level.resolve() != worktree.resolve():
            raise ExpctlError(f"existing path is not a worktree root: {worktree}")
        if _git_common_dir(worktree) != _git_common_dir(repo):
            raise ExpctlError(
                f"existing worktree belongs to a different repository: {worktree}"
            )
        actual = _git_text(worktree, "rev-parse", "HEAD").strip()
        if actual != commit:
            raise ExpctlError(
                f"worktree {worktree} is at {actual}, expected {commit}; "
                "choose a new request/worktree name"
            )
        _check_worktree_clean(worktree, allowed_untracked)
        return
    _run(
        ["git", "worktree", "add", "--detach", str(worktree), commit],
        cwd=repo,
    )
    _check_worktree_clean(worktree, allowed_untracked)


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
            "PENDING,RUNNING,COMPLETING,CONFIGURING,SUSPENDED,RESIZING",
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


@contextlib.contextmanager
def _git_metadata_lock(repo: Path, name: str, purpose: str) -> Iterator[None]:
    if fcntl is None:
        raise ExpctlError(
            f"safe {purpose} locking requires POSIX fcntl; submit on the cluster host"
        )
    lock_text = _git_text(repo, "rev-parse", "--git-path", name).strip()
    lock_path = Path(lock_text)
    if not lock_path.is_absolute():
        lock_path = repo / lock_path
    lock_path = lock_path.resolve()
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with lock_path.open("a+b") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    except OSError as exc:
        raise ExpctlError(f"could not acquire {purpose} lock: {exc}") from exc


@contextlib.contextmanager
def _node_budget_guard(repo: Path) -> Iterator[None]:
    """Serialize expctl's check-and-submit window within one Git checkout."""
    with _git_metadata_lock(repo, "expctl-submit.lock", "node-budget submission"):
        yield


@contextlib.contextmanager
def _worktree_submission_guard(repo: Path, worktree: Path) -> Iterator[None]:
    digest = hashlib.sha256(str(worktree).encode("utf-8")).hexdigest()[:20]
    with _git_metadata_lock(
        repo, f"expctl-worktree-{digest}.lock", "worktree submission"
    ):
        yield


@contextlib.contextmanager
def _result_collection_guard(repo: Path, experiment_id: str) -> Iterator[None]:
    """Serialize receipt and result lifecycle writes for one experiment."""
    with _git_metadata_lock(
        repo, f"expctl-collect-{experiment_id}.lock", "result lifecycle update"
    ):
        yield


def _receipt_claim_worktree(
    receipt: dict[str, Any], receipt_path: Path, *, operation: str
) -> Path:
    value = receipt.get("worktree")
    if not isinstance(value, str) or not value or not Path(value).is_absolute():
        raise ExpctlError(
            f"cannot safely {operation} while receipt has no valid absolute "
            f"worktree: {receipt_path}"
        )
    try:
        return Path(value).resolve()
    except OSError as exc:
        raise ExpctlError(
            f"cannot safely {operation} while receipt worktree cannot be "
            f"resolved: {receipt_path}: {exc}"
        ) from exc


def _check_worktree_available(
    repo: Path,
    config: Config,
    worktree: Path,
    experiment_id: str,
) -> None:
    """Refuse to share one worktree with another queued or uncertain run."""
    receipts_root = repo / config.root / "results"
    for other_path in receipts_root.glob("*/receipt.json"):
        if other_path.parent.name == experiment_id:
            continue
        try:
            other = _load_receipt(other_path)
        except ExpctlError as exc:
            raise ExpctlError(
                f"cannot safely determine worktree availability while receipt "
                f"is unreadable: {other_path}: {exc}"
            ) from exc
        other_worktree = _receipt_claim_worktree(
            other,
            other_path,
            operation="determine worktree availability",
        )
        same_worktree = other_worktree == worktree.resolve()
        if not same_worktree:
            continue
        other_id = other.get("experiment_id", other_path.parent.name)
        job_id = other.get("job_id")
        if not isinstance(job_id, str) or not job_id:
            raise ExpctlError(
                f"worktree {worktree} is claimed by {other_id} with an "
                f"unconfirmed submission ({other.get('status', 'invalid')})"
            )
        queue = _queue_status(repo, job_id)
        if queue:
            raise ExpctlError(
                f"worktree {worktree} is still used by {other_id} (job {job_id})"
            )


def build_sbatch_command(
    request: dict[str, Any], policy_options: tuple[str, ...] = ()
) -> list[str]:
    slurm = _table(request, "slurm")
    env = slurm.get("env", {})
    command = ["sbatch", "--parsable"]
    if env:
        exports = ",".join(f"{key}={value}" for key, value in sorted(env.items()))
        command.append(f"--export=ALL,{exports}")
    command.extend(policy_options)
    command.append(slurm["script"])
    return command


def _sbatch_environment() -> tuple[dict[str, str], list[str]]:
    """Prevent ambient SBATCH_* options from overriding the pinned script."""
    removed = sorted(key for key in os.environ if key.startswith("SBATCH_"))
    environment = {
        key: value for key, value in os.environ.items() if key not in removed
    }
    return environment, removed


def submit_request(
    repo: Path,
    config: Config,
    experiment_id: str,
    *,
    dry_run: bool,
    skip_node_check: bool,
    worktree_root: Path | None,
) -> dict[str, Any]:
    path = request_path(repo, config, experiment_id)
    if not path.is_file():
        raise ExpctlError(f"request not found: {path}")
    request_hash = _request_hash(path)
    request, path = load_request(repo, config, experiment_id)
    if _request_hash(path) != request_hash:
        raise ExpctlError("request changed while it was being validated")
    receipt_path = result_dir(repo, config, experiment_id) / "receipt.json"
    if receipt_path.exists():
        raise ExpctlError(
            f"receipt already exists: {receipt_path} "
            f"({_receipt_summary(repo, receipt_path)}). A request is submitted "
            f"once; to run it again: expctl rerun {experiment_id}"
        )

    code = _table(request, "code")
    worktree = _worktree_path(repo, config, request, worktree_root)
    policy_options = tuple(_required_sbatch_options(config.required_script_lines))
    command = build_sbatch_command(request, policy_options)
    sbatch_environment, cleared_sbatch_environment = _sbatch_environment()
    script_lines = _git_text(
        repo, "show", f"{code['commit']}:{request['slurm']['script']}"
    ).splitlines()
    verified_nodes = _script_max_concurrent_nodes(
        script_lines, request["slurm"]["script"]
    )
    preview = {
        "experiment_id": experiment_id,
        "commit": code["commit"],
        "worktree": str(worktree),
        "command": command,
        "declared_max_concurrent_nodes": request["slurm"]["max_concurrent_nodes"],
        "verified_max_concurrent_nodes": verified_nodes,
        "cleared_sbatch_environment": cleared_sbatch_environment,
    }
    if dry_run:
        return preview

    attempt_id = uuid.uuid4().hex
    pending: dict[str, Any] = {
        "schema_version": 1,
        "experiment_id": experiment_id,
        "submission_attempt_id": attempt_id,
        "request_sha256": request_hash,
        "branch_label": code["branch"],
        "commit": code["commit"],
        "worktree": str(worktree),
        "started_at": dt.datetime.now(dt.UTC).isoformat(),
        "submitted_by": getpass.getuser(),
        "status": "preparing",
        "declared_max_concurrent_nodes": request["slurm"]["max_concurrent_nodes"],
        "verified_max_concurrent_nodes": verified_nodes,
        "cleared_sbatch_environment": cleared_sbatch_environment,
        "sbatch_command": command,
    }
    try:
        _exclusive_write_text(
            receipt_path, json.dumps(pending, indent=2, sort_keys=True) + "\n"
        )
    except ExpctlError:
        if receipt_path.exists():
            raise ExpctlError(
                f"receipt already exists: {receipt_path} "
                f"({_receipt_summary(repo, receipt_path)}). A request is submitted "
                f"once; to run it again: expctl rerun {experiment_id}"
            ) from None
        raise

    submission_started = False
    try:
        with _worktree_submission_guard(repo, worktree):
            _check_worktree_available(repo, config, worktree, experiment_id)
            _ensure_worktree(
                repo,
                worktree,
                code["commit"],
                allowed_untracked=config.shared_dirs,
            )
            runtime_dirs = _ensure_runtime_links(repo, config, worktree)
            _check_requirements(worktree, request)

            guard = (
                _node_budget_guard(repo)
                if config.max_total_nodes and not skip_node_check
                else contextlib.nullcontext()
            )
            with guard:
                budget = None
                if config.max_total_nodes and not skip_node_check:
                    budget = _check_node_budget(
                        repo, config, request["slurm"]["max_concurrent_nodes"]
                    )
                if _request_hash(path) != request_hash:
                    raise ExpctlError(
                        "request changed while submission was being prepared"
                    )
                _check_worktree_clean(worktree, config.shared_dirs)

                pending["status"] = "submitting"
                pending["runtime_dirs"] = runtime_dirs
                pending["node_budget"] = budget
                pending["submitting_at"] = dt.datetime.now(dt.UTC).isoformat()
                _atomic_write_json(receipt_path, pending)
                submission_started = True
                try:
                    result = _run(command, cwd=worktree, env=sbatch_environment)
                except ExpctlError as exc:
                    pending["status"] = "submission_unknown"
                    pending["submission_error"] = str(exc)
                    _atomic_write_json(receipt_path, pending)
                    raise ExpctlError(
                        "sbatch did not return a confirmed job ID; submission outcome "
                        f"is unknown and this request is locked. Inspect {receipt_path} "
                        "and the scheduler before taking further action"
                    ) from exc

                job_id = result.stdout.strip().split(";", 1)[0]
                if not job_id or not re.fullmatch(r"\d+(?:_\d+)?", job_id):
                    pending["status"] = "submission_unknown"
                    pending["submission_error"] = (
                        f"could not parse sbatch job ID from: {result.stdout!r}"
                    )
                    _atomic_write_json(receipt_path, pending)
                    raise ExpctlError(
                        "sbatch returned an unrecognized response; submission outcome "
                        f"is unknown and this request is locked. Inspect {receipt_path} "
                        "and the scheduler before taking further action"
                    )

                receipt = dict(pending)
                receipt["job_id"] = job_id
                receipt["submitted_at"] = dt.datetime.now(dt.UTC).isoformat()
                receipt["status"] = "submitted"
                receipt.pop("submitting_at", None)
                try:
                    _atomic_write_json(receipt_path, receipt)
                except ExpctlError as exc:
                    raise ExpctlError(
                        f"SLURM job {job_id} was submitted, but its receipt could not "
                        f"be finalized at {receipt_path}; do not submit again: {exc}"
                    ) from exc
                return receipt
    except BaseException:
        if not submission_started:
            try:
                current = _load_receipt(receipt_path)
                if current.get("submission_attempt_id") == attempt_id:
                    receipt_path.unlink(missing_ok=True)
            except (ExpctlError, OSError):
                pass
        raise


def _queue_statuses(repo: Path, job_ids: list[str]) -> list[dict[str, str]]:
    if not job_ids:
        return []
    result = _run(
        ["squeue", "-r", "-j", ",".join(job_ids), "-h", "-o", "%i|%T|%r"],
        cwd=repo,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        if "invalid job id" not in detail.lower():
            raise ExpctlError(
                f"squeue query failed for jobs {','.join(job_ids)}: {detail}"
            )
        # Completed or purged jobs produce this on some SLURM versions. Keep
        # any stdout rows in case a mixed batch also contained active jobs.
    rows: list[dict[str, str]] = []
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        job, state, reason = (line.split("|", 2) + ["", ""])[:3]
        rows.append(
            {"job": job.strip(), "state": state.strip(), "reason": reason.strip()}
        )
    return rows


def _queue_status(repo: Path, job_id: str) -> list[dict[str, str]]:
    return _queue_statuses(repo, [job_id])


def status_request(repo: Path, config: Config, experiment_id: str) -> dict[str, Any]:
    request_path(repo, config, experiment_id)  # Validate before using the ID as a path.
    receipt_path = result_dir(repo, config, experiment_id) / "receipt.json"
    if not receipt_path.is_file():
        raise ExpctlError(f"not submitted yet: no receipt at {receipt_path}")
    receipt = _load_receipt(receipt_path)
    job_id = receipt.get("job_id")
    checked_at = dt.datetime.now(dt.UTC).isoformat()
    if not isinstance(job_id, str) or not job_id:
        payload = {
            "experiment_id": experiment_id,
            "job_id": None,
            "receipt_status": receipt.get("status", "invalid"),
            "in_queue": False,
            "checked_at": checked_at,
            "source": "receipt",
            "detail": receipt.get(
                "submission_error",
                "submission has no confirmed job ID; inspect the receipt before retrying",
            ),
        }
        if isinstance(receipt.get("cancellation"), dict):
            payload["cancellation"] = receipt["cancellation"]
        return payload
    queue_error: str | None = None
    try:
        queue = _queue_status(repo, job_id)
    except ExpctlError as exc:
        queue = []
        queue_error = str(exc)
    counts: dict[str, int] = {}
    for row in queue:
        counts[row["state"]] = counts.get(row["state"], 0) + 1
    payload: dict[str, Any] = {
        "experiment_id": experiment_id,
        "job_id": job_id,
        "receipt_status": receipt.get("status"),
        "in_queue": bool(queue),
        "queue_counts": counts,
        "queue": queue,
        "checked_at": checked_at,
        "source": "squeue" if queue else "sacct",
    }
    if isinstance(receipt.get("cancellation"), dict):
        payload["cancellation"] = receipt["cancellation"]
    report = report_path(repo, config, experiment_id)
    if report.is_file():
        payload["report"] = str(report.relative_to(repo)).replace("\\", "/")
    if not queue:
        try:
            accounting = _scheduler_status(repo, job_id)
        except ExpctlError as exc:
            accounting = {"state": "UNKNOWN", "detail": str(exc)}
        payload["accounting"] = accounting
        details: list[str] = []
        if queue_error:
            details.append(f"squeue unavailable ({queue_error}); used sacct")
        if accounting.get("state") == "UNKNOWN" and accounting.get("detail"):
            details.append(f"sacct: {accounting['detail']}")
        if details:
            payload["detail"] = "; ".join(details)
    return payload


TERMINAL_SLURM_STATES = {
    "BOOT_FAIL",
    "CANCELLED",
    "COMPLETED",
    "DEADLINE",
    "FAILED",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "PREEMPTED",
    "REVOKED",
    "SPECIAL_EXIT",
    "TIMEOUT",
}


def _status_state(result: dict[str, Any]) -> str:
    receipt_status = str(result.get("receipt_status", "invalid"))
    counts = result.get("queue_counts", {})
    accounting = result.get("accounting", {})
    if receipt_status == "collected":
        return "REVIEWED" if result.get("report") else "COLLECTED"
    if result.get("in_queue"):
        return next(iter(counts)) if len(counts) == 1 else "MIXED"
    if isinstance(accounting, dict) and accounting.get("state"):
        return str(accounting["state"])
    return receipt_status.upper()


def _status_is_terminal(result: dict[str, Any]) -> bool:
    if not result.get("job_id") or result.get("receipt_status") == "collected":
        return True
    if result.get("in_queue"):
        return False
    state = _status_state(result)
    normalized = {
        normalized
        for part in state.split(",")
        if (normalized := _slurm_state_name(part)) is not None
    }
    return bool(normalized) and normalized.issubset(TERMINAL_SLURM_STATES)


def cancel_request(
    repo: Path,
    config: Config,
    experiment_id: str,
    *,
    reason: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    request_path(repo, config, experiment_id)
    if dry_run:
        return _cancel_request_locked(
            repo,
            config,
            experiment_id,
            reason=reason,
            dry_run=True,
        )
    with _result_collection_guard(repo, experiment_id):
        return _cancel_request_locked(
            repo,
            config,
            experiment_id,
            reason=reason,
            dry_run=dry_run,
        )


def _cancel_request_locked(
    repo: Path,
    config: Config,
    experiment_id: str,
    *,
    reason: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Cancel one confirmed SLURM job and record who requested it."""
    receipt_path = result_dir(repo, config, experiment_id) / "receipt.json"
    if not receipt_path.is_file():
        raise ExpctlError(f"not submitted yet: no receipt at {receipt_path}")
    receipt = _load_receipt(receipt_path)
    job_id = receipt.get("job_id")
    if not isinstance(job_id, str) or not job_id:
        raise ExpctlError(
            "cannot cancel a submission without a confirmed job ID; "
            "reconcile it with SLURM manually"
        )
    if isinstance(receipt.get("cancellation"), dict):
        cancellation = receipt["cancellation"]
        raise ExpctlError(
            f"cancellation was already requested for job {job_id} at "
            f"{cancellation.get('requested_at', 'an unknown time')}"
        )
    if receipt.get("status") == "collected":
        raise ExpctlError(f"job {job_id} has already been collected")
    if reason is not None:
        reason = reason.strip()
        if not reason:
            raise ExpctlError("cancellation reason must not be empty")

    command = ["scancel", job_id]
    result: dict[str, Any] = {
        "experiment_id": experiment_id,
        "job_id": job_id,
        "command": command,
        "dry_run": dry_run,
        "reason": reason,
    }
    if dry_run:
        return result

    _run(command, cwd=repo)
    cancellation = {
        "requested_at": dt.datetime.now(dt.UTC).isoformat(),
        "requested_by": getpass.getuser(),
        "reason": reason,
    }
    receipt["status"] = "cancel_requested"
    receipt["cancellation"] = cancellation
    try:
        _atomic_write_json(receipt_path, receipt)
    except ExpctlError as exc:
        raise ExpctlError(
            f"SLURM accepted the cancellation for job {job_id}, but the receipt "
            f"could not be updated at {receipt_path}: {exc}"
        ) from exc
    result.update(cancellation)
    result["receipt_status"] = "cancel_requested"
    return result


def _receipt_worktree_context(
    repo: Path,
    config: Config,
    experiment_id: str,
    *,
    worktree_root: Path | None,
) -> tuple[dict[str, Any], dict[str, Any], Path, Path]:
    request, request_file = load_request(repo, config, experiment_id, check_git=False)
    receipt_path = result_dir(repo, config, experiment_id) / "receipt.json"
    if not receipt_path.is_file():
        raise ExpctlError(f"submission receipt not found: {receipt_path}")
    receipt = _load_receipt(receipt_path)
    request_sha256 = receipt.get("request_sha256")
    if isinstance(request_sha256, str) and request_sha256 != _request_hash(
        request_file
    ):
        raise ExpctlError(
            "request changed after submission; restore it before accessing the worktree"
        )
    worktree = _verified_receipt_worktree(
        repo,
        config,
        request,
        receipt,
        worktree_root=worktree_root,
    )
    return request, receipt, receipt_path, worktree


def _verified_receipt_worktree(
    repo: Path,
    config: Config,
    request: dict[str, Any],
    receipt: dict[str, Any],
    *,
    worktree_root: Path | None,
) -> Path:
    expected_worktree = _worktree_path(
        repo, config, request, override_root=worktree_root
    )
    worktree_text = receipt.get("worktree")
    if (
        not isinstance(worktree_text, str)
        or not worktree_text
        or not Path(worktree_text).is_absolute()
    ):
        raise ExpctlError("submission receipt has no valid absolute worktree path")
    recorded_worktree = Path(worktree_text).resolve()
    if recorded_worktree != expected_worktree:
        raise ExpctlError(
            f"receipt worktree {recorded_worktree} does not match expected worktree "
            f"{expected_worktree}; pass the same --worktree-root used for submit"
        )
    return expected_worktree


def _log_sources(
    repo: Path,
    config: Config,
    experiment_id: str,
    *,
    worktree_root: Path | None,
) -> tuple[Path, str, list[Path], bool]:
    request, request_file = load_request(repo, config, experiment_id, check_git=False)
    receipt_path = result_dir(repo, config, experiment_id) / "receipt.json"
    if not receipt_path.is_file():
        raise ExpctlError(f"submission receipt not found: {receipt_path}")
    receipt = _load_receipt(receipt_path)
    request_sha256 = receipt.get("request_sha256")
    if isinstance(request_sha256, str) and request_sha256 != _request_hash(
        request_file
    ):
        raise ExpctlError(
            "request changed after submission; restore it before reading logs"
        )

    collected_logs = receipt_path.parent / "logs"
    if receipt.get("status") == "collected" and collected_logs.is_dir():
        sources = sorted(
            source for source in collected_logs.iterdir() if source.is_file()
        )
        return collected_logs, "*", sources, True

    worktree = _verified_receipt_worktree(
        repo,
        config,
        request,
        receipt,
        worktree_root=worktree_root,
    )
    if not worktree.is_dir():
        raise ExpctlError(f"submission worktree is missing: {worktree}")
    job_id = receipt.get("job_id")
    if not isinstance(job_id, str) or not job_id:
        raise ExpctlError(
            f"submission has no confirmed job ID (receipt status: "
            f"{receipt.get('status', 'invalid')})"
        )
    pattern = request["outputs"]["log_glob"].format(job_id=job_id)
    sources = sorted(source for source in worktree.glob(pattern) if source.is_file())
    return worktree, pattern, sources, False


LogCursor = tuple[int, int, int]


def _decode_log_bytes(data: bytes) -> str:
    return (
        data.decode("utf-8", errors="replace").replace("\r\n", "\n").replace("\r", "\n")
    )


def _read_log_update(
    path: Path,
    cursor: LogCursor | None,
    *,
    tail: int,
) -> tuple[str, LogCursor, bool]:
    """Read one stable log snapshot and return text, cursor, and reset status."""
    try:
        with path.open("rb") as handle:
            stat = os.fstat(handle.fileno())
            identity = (stat.st_dev, stat.st_ino)
            end = stat.st_size
            reset = cursor is None or cursor[:2] != identity
            if reset:
                if tail == 0:
                    data = b""
                else:
                    position = end
                    data = b""
                    while position > 0 and data.count(b"\n") <= tail:
                        block_size = min(64 * 1024, position)
                        position -= block_size
                        handle.seek(position)
                        data = handle.read(block_size) + data
                    data = b"".join(data.splitlines(keepends=True)[-tail:])
                return _decode_log_bytes(data), (*identity, end), True

            offset = cursor[2]
            truncated = end < offset
            if truncated:
                offset = 0
            handle.seek(offset)
            # Read only the bytes covered by fstat. Data appended concurrently
            # remains beyond the returned cursor for the next poll.
            data = handle.read(end - offset)
            return (
                _decode_log_bytes(data),
                (*identity, offset + len(data)),
                truncated,
            )
    except OSError as exc:
        raise ExpctlError(f"could not read log {path}: {exc}") from exc


def _tail_file(path: Path, lines: int) -> str:
    content, _, _ = _read_log_update(path, None, tail=lines)
    return content


def logs_request(
    repo: Path,
    config: Config,
    experiment_id: str,
    *,
    tail: int = 100,
    worktree_root: Path | None = None,
) -> dict[str, Any]:
    log_root, pattern, sources, collected = _log_sources(
        repo,
        config,
        experiment_id,
        worktree_root=worktree_root,
    )
    if not sources:
        raise ExpctlError(f"no log files match {log_root / pattern}")
    return {
        "experiment_id": experiment_id,
        "location": str(log_root),
        "pattern": pattern,
        "tail": tail,
        "collected": collected,
        "logs": [
            {
                "path": source.relative_to(log_root).as_posix(),
                "content": _tail_file(source, tail),
            }
            for source in sources
        ],
    }


def _render_logs(result: dict[str, Any]) -> str:
    logs = result["logs"]
    multiple = len(logs) > 1
    chunks: list[str] = []
    for log in logs:
        content = str(log["content"])
        if multiple:
            chunks.append(f"==> {log['path']} <==\n{content}")
        else:
            chunks.append(content)
    return "\n".join(chunk.rstrip("\n") for chunk in chunks) + "\n"


def _follow_logs(
    repo: Path,
    config: Config,
    experiment_id: str,
    *,
    tail: int,
    worktree_root: Path | None,
    poll_interval: float = 1.0,
) -> None:
    """Print existing log tails and follow appended bytes until interrupted."""
    cursors: dict[Path, LogCursor] = {}
    announced_wait = False
    last_source: Path | None = None
    next_status_check = 0.0
    while True:
        log_root, pattern, sources, collected = _log_sources(
            repo,
            config,
            experiment_id,
            worktree_root=worktree_root,
        )
        if not sources and not announced_wait:
            if collected:
                raise ExpctlError(f"collected log directory is empty: {log_root}")
            print(f"Waiting for logs matching {log_root / pattern}", file=sys.stderr)
            announced_wait = True
        if not sources and time.monotonic() >= next_status_check:
            status = status_request(repo, config, experiment_id)
            if _status_is_terminal(status):
                raise ExpctlError(
                    f"job reached {_status_state(status)} but no log files match "
                    f"{log_root / pattern}"
                )
            next_status_check = time.monotonic() + max(5.0, poll_interval)
        multiple = len(sources) > 1
        for source in sources:
            previous = cursors.get(source)
            content, cursor, reset = _read_log_update(source, previous, tail=tail)
            cursors[source] = cursor
            if not content:
                continue
            first_read = previous is None
            show_header = (
                (first_read and (multiple or len(cursors) > 1))
                or (not first_read and reset)
                or (source != last_source and (multiple or len(cursors) > 1))
            )
            if show_header:
                print(f"==> {source.relative_to(log_root).as_posix()} <==")
            sys.stdout.write(_stdout_safe(content))
            if first_read and not content.endswith("\n"):
                sys.stdout.write("\n")
            sys.stdout.flush()
            last_source = source
        if collected:
            return
        time.sleep(poll_interval)


def _other_uncollected_worktree_claims(
    repo: Path,
    config: Config,
    worktree: Path,
    experiment_id: str,
) -> list[str]:
    claims: list[str] = []
    receipts_root = repo / config.root / "results"
    for other_path in receipts_root.glob("*/receipt.json"):
        other_id = other_path.parent.name
        if other_id == experiment_id:
            continue
        try:
            other = _load_receipt(other_path)
        except ExpctlError as exc:
            raise ExpctlError(
                f"cannot safely clean worktree while receipt is unreadable: "
                f"{other_path}: {exc}"
            ) from exc
        other_worktree = _receipt_claim_worktree(
            other,
            other_path,
            operation="clean worktree",
        )
        same_worktree = other_worktree == worktree.resolve()
        if same_worktree and other.get("status") != "collected":
            claims.append(other_id)
    return sorted(claims)


def _validate_clean_worktree(
    repo: Path,
    config: Config,
    request: dict[str, Any],
    worktree: Path,
) -> None:
    if not (worktree / ".git").exists():
        raise ExpctlError(f"existing path is not a Git worktree: {worktree}")
    if _git_common_dir(worktree) != _git_common_dir(repo):
        raise ExpctlError(
            f"existing worktree belongs to a different repository: {worktree}"
        )
    actual = _git_text(worktree, "rev-parse", "HEAD").strip()
    expected = request["code"]["commit"]
    if actual != expected:
        raise ExpctlError(f"worktree {worktree} is at {actual}, expected {expected}")
    _check_worktree_clean(worktree, config.shared_dirs)


def clean_request(
    repo: Path,
    config: Config,
    experiment_id: str,
    *,
    dry_run: bool = False,
    worktree_root: Path | None = None,
) -> dict[str, Any]:
    request_path(repo, config, experiment_id)
    if dry_run:
        return _clean_request_locked(
            repo,
            config,
            experiment_id,
            dry_run=True,
            worktree_root=worktree_root,
        )
    with _result_collection_guard(repo, experiment_id):
        return _clean_request_locked(
            repo,
            config,
            experiment_id,
            dry_run=dry_run,
            worktree_root=worktree_root,
        )


def _clean_request_locked(
    repo: Path,
    config: Config,
    experiment_id: str,
    *,
    dry_run: bool = False,
    worktree_root: Path | None = None,
) -> dict[str, Any]:
    """Remove a collected experiment's verified detached worktree."""
    request, receipt, receipt_path, worktree = _receipt_worktree_context(
        repo,
        config,
        experiment_id,
        worktree_root=worktree_root,
    )
    if receipt.get("status") != "collected":
        raise ExpctlError(
            f"refusing to remove worktree before results are collected "
            f"(receipt status: {receipt.get('status', 'invalid')})"
        )
    if isinstance(receipt.get("cleanup"), dict):
        cleanup = receipt["cleanup"]
        return {
            "experiment_id": experiment_id,
            "worktree": str(worktree),
            "removed": False,
            "worktree_exists": worktree.exists(),
            "already_removed_at": cleanup.get("removed_at"),
            "dry_run": dry_run,
        }
    claims = _other_uncollected_worktree_claims(repo, config, worktree, experiment_id)
    if claims:
        raise ExpctlError(
            f"worktree {worktree} is still claimed by uncollected experiment(s): "
            + ", ".join(claims)
        )
    worktree_exists = worktree.exists()
    result: dict[str, Any] = {
        "experiment_id": experiment_id,
        "worktree": str(worktree),
        "worktree_exists": worktree_exists,
        "removed": False,
        "dry_run": dry_run,
    }
    if not worktree_exists:
        return result

    _validate_clean_worktree(repo, config, request, worktree)
    if dry_run:
        return result

    with _worktree_submission_guard(repo, worktree):
        claims = _other_uncollected_worktree_claims(
            repo, config, worktree, experiment_id
        )
        if claims:
            raise ExpctlError(
                f"worktree {worktree} is still claimed by uncollected "
                f"experiment(s): {', '.join(claims)}"
            )
        if not worktree.exists():
            result["worktree_exists"] = False
            return result
        _validate_clean_worktree(repo, config, request, worktree)
        _run(["git", "worktree", "remove", "--force", str(worktree)], cwd=repo)

    cleanup = {
        "removed_at": dt.datetime.now(dt.UTC).isoformat(),
        "removed_by": getpass.getuser(),
        "worktree": str(worktree),
    }
    receipt["cleanup"] = cleanup
    try:
        _atomic_write_json(receipt_path, receipt)
    except ExpctlError as exc:
        raise ExpctlError(
            f"worktree {worktree} was removed, but cleanup could not be recorded "
            f"in {receipt_path}: {exc}"
        ) from exc
    result.update(cleanup)
    result["removed"] = True
    result["worktree_exists"] = False
    return result


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
        detail = result.stderr.strip() or result.stdout.strip()
        return {"state": "UNKNOWN", "detail": detail}
    rows = []
    for line in result.stdout.splitlines():
        fields = line.split("|")
        if len(fields) >= 3 and "." not in fields[0]:
            rows.append(
                {"job_id": fields[0], "state": fields[1], "exit_code": fields[2]}
            )
    states = sorted({row["state"] for row in rows})
    return {"state": ",".join(states) if states else "UNKNOWN", "jobs": rows}


def _scheduler_status_rows(repo: Path, job_ids: list[str]) -> list[dict[str, str]]:
    if not job_ids:
        return []
    result = _run(
        [
            "sacct",
            "-j",
            ",".join(job_ids),
            "--parsable2",
            "--noheader",
            "--format=JobIDRaw,State,ExitCode",
        ],
        cwd=repo,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise ExpctlError(f"sacct query failed for jobs {','.join(job_ids)}: {detail}")
    rows: list[dict[str, str]] = []
    for line in result.stdout.splitlines():
        fields = line.split("|")
        if len(fields) >= 3 and "." not in fields[0]:
            rows.append(
                {"job_id": fields[0], "state": fields[1], "exit_code": fields[2]}
            )
    return rows


def _slurm_state_name(value: object) -> str | None:
    """Return the stable part of a state such as ``CANCELLED by 123``."""
    state = str(value).strip().upper()
    if not state:
        return None
    state = state.split(maxsplit=1)[0].rstrip("+")
    return None if state == "UNKNOWN" else state


def _summarize_slurm_states(states: Iterator[object]) -> str | None:
    normalized = {
        state for value in states if (state := _slurm_state_name(value)) is not None
    }
    if not normalized:
        return None
    if len(normalized) == 1:
        return normalized.pop()
    return "MIXED"


def _requested_job_id(raw_job_id: object, requested: set[str]) -> str | None:
    raw = str(raw_job_id)
    if raw in requested:
        return raw
    candidate = raw
    while "_" in candidate:
        candidate = candidate.rsplit("_", 1)[0]
        if candidate in requested:
            return candidate
    return None


def _live_scheduler_statuses(
    repo: Path, job_ids: list[str]
) -> tuple[dict[str, str], str | None]:
    """Refresh many receipts with at most one squeue and one sacct query."""
    requested = list(dict.fromkeys(job_ids))
    requested_set = set(requested)
    if not requested:
        return {}, None
    if shutil.which("squeue") is None:
        return {}, "squeue not found; showing stored receipt states"
    try:
        queue = _queue_statuses(repo, requested)
    except ExpctlError as exc:
        return {}, f"{exc}; showing stored receipt states"

    queue_by_job: dict[str, list[dict[str, str]]] = {}
    for row in queue:
        if owner := _requested_job_id(row.get("job"), requested_set):
            queue_by_job.setdefault(owner, []).append(row)
    statuses = {
        job_id: status
        for job_id, rows in queue_by_job.items()
        if (status := _summarize_slurm_states(row.get("state") for row in rows))
        is not None
    }
    finished = [job_id for job_id in requested if job_id not in statuses]
    if not finished:
        return statuses, None

    if shutil.which("sacct") is None:
        return statuses, (
            f"sacct not found; showing stored receipt state for {len(finished)} job(s)"
        )
    try:
        accounting = _scheduler_status_rows(repo, finished)
    except ExpctlError as exc:
        return statuses, (
            f"{exc}; showing stored receipt state for {len(finished)} job(s)"
        )

    accounting_by_job: dict[str, list[dict[str, str]]] = {}
    finished_set = set(finished)
    for row in accounting:
        if owner := _requested_job_id(row.get("job_id"), finished_set):
            accounting_by_job.setdefault(owner, []).append(row)
    for job_id, rows in accounting_by_job.items():
        parent_status = _summarize_slurm_states(
            row.get("state") for row in rows if row.get("job_id") == job_id
        )
        task_status = _summarize_slurm_states(row.get("state") for row in rows)
        if status := parent_status or task_status:
            statuses[job_id] = status

    unresolved = [job_id for job_id in finished if job_id not in statuses]
    warning = None
    if unresolved:
        warning = (
            f"SLURM returned no state for {len(unresolved)} submitted job(s); "
            "showing stored receipt states"
        )
    return statuses, warning


def _live_scheduler_status(repo: Path, job_id: str) -> str | None:
    """Backward-compatible single-job wrapper around the batched refresh."""
    statuses, _ = _live_scheduler_statuses(repo, [job_id])
    return statuses.get(job_id)


def collect_request(
    repo: Path,
    config: Config,
    experiment_id: str,
    *,
    worktree_root: Path | None = None,
) -> dict[str, Any]:
    # Validate the ID before using it in the Git lock name.
    request_path(repo, config, experiment_id)
    with _result_collection_guard(repo, experiment_id):
        request, path = load_request(repo, config, experiment_id, check_git=False)
        destination = result_dir(repo, config, experiment_id)
        receipt_path = destination / "receipt.json"
        if not receipt_path.is_file():
            raise ExpctlError(f"submission receipt not found: {receipt_path}")
        receipt = _load_receipt(receipt_path)
        if receipt.get("status") == "collected":
            raise ExpctlError(
                f"results for {experiment_id} have already been collected; "
                "existing evidence will not be overwritten"
            )

        request_sha256 = receipt.get("request_sha256")
        if request_sha256 != _request_hash(path):
            raise ExpctlError(
                "request changed after submission; restore it before collecting results"
            )

        logs_dir = destination / "logs"
        metrics_path = destination / "metrics.json"
        existing = [
            artifact
            for artifact in (logs_dir, metrics_path)
            if artifact.exists() or artifact.is_symlink()
        ]
        if existing:
            rendered = ", ".join(str(artifact) for artifact in existing)
            raise ExpctlError(
                "collection artifacts already exist and will not be overwritten: "
                f"{rendered}"
            )

        job_id = receipt.get("job_id")
        if not isinstance(job_id, str) or not job_id:
            raise ExpctlError(
                f"submission has no confirmed job ID (receipt status: "
                f"{receipt.get('status', 'invalid')})"
            )
        queue = _queue_status(repo, job_id)
        if queue:
            states = ", ".join(
                f"{state}={count}"
                for state, count in sorted(
                    {
                        state: sum(row["state"] == state for row in queue)
                        for state in {row["state"] for row in queue}
                    }.items()
                )
            )
            raise ExpctlError(
                f"job {job_id} is still in the queue ({states}); "
                "collect after it leaves"
            )

        expected_worktree = _worktree_path(
            repo, config, request, override_root=worktree_root
        )
        worktree_text = receipt.get("worktree")
        if (
            not isinstance(worktree_text, str)
            or not worktree_text
            or not Path(worktree_text).is_absolute()
        ):
            raise ExpctlError("submission receipt has no valid absolute worktree path")
        recorded_worktree = Path(worktree_text).resolve()
        if recorded_worktree != expected_worktree:
            raise ExpctlError(
                f"receipt worktree {recorded_worktree} does not match expected "
                f"worktree {expected_worktree}; pass the same --worktree-root used "
                "for submit"
            )
        if not expected_worktree.is_dir():
            raise ExpctlError(f"submission worktree is missing: {expected_worktree}")
        pattern = request["outputs"]["log_glob"].format(job_id=job_id)
        sources = sorted(
            source for source in expected_worktree.glob(pattern) if source.is_file()
        )
        if not sources:
            raise ExpctlError(f"no log files match {expected_worktree / pattern}")

        names: dict[str, list[Path]] = {}
        for source in sources:
            names.setdefault(source.name.casefold(), []).append(source)
        collisions = [matches for matches in names.values() if len(matches) > 1]
        if collisions:
            rendered = "; ".join(
                ", ".join(str(collision) for collision in matches)
                for matches in collisions
            )
            raise ExpctlError(
                "log files have colliding basenames and cannot be collected safely: "
                f"{rendered}"
            )

        try:
            staging = Path(tempfile.mkdtemp(prefix=".collect-", dir=destination))
        except OSError as exc:
            raise ExpctlError(
                f"could not create result staging directory in {destination}: {exc}"
            ) from exc
        published_logs = False
        published_metrics = False
        try:
            staged_logs = staging / "logs"
            staged_logs.mkdir()
            metric_names = request["outputs"]["metrics"]
            metrics: dict[str, dict[str, float]] = {}
            copied: list[str] = []
            for source in sources:
                staged_log = staged_logs / source.name
                _atomic_copy2(source, staged_log)
                final_log = logs_dir / source.name
                copied.append(str(final_log.relative_to(repo)).replace("\\", "/"))
                metrics[source.name] = extract_metrics(
                    staged_log.read_text(encoding="utf-8", errors="replace"),
                    metric_names,
                )

            staged_metrics = staging / "metrics.json"
            _atomic_write_json(staged_metrics, metrics)
            found_metrics = {metric for values in metrics.values() for metric in values}
            collection = {
                "collected_at": dt.datetime.now(dt.UTC).isoformat(),
                "logs": copied,
                "metrics_file": str(metrics_path.relative_to(repo)).replace("\\", "/"),
                "missing_metrics": sorted(set(metric_names) - found_metrics),
                "scheduler": _scheduler_status(repo, job_id),
            }

            # Do not publish evidence if the request changed during collection.
            if request_sha256 != _request_hash(path):
                raise ExpctlError(
                    "request changed during collection; restore it and try again"
                )

            try:
                logs_dir.mkdir()
            except FileExistsError as exc:
                raise ExpctlError(
                    f"collection artifacts already exist and will not be overwritten: "
                    f"{logs_dir}"
                ) from exc
            except OSError as exc:
                raise ExpctlError(
                    f"could not create result log directory {logs_dir}: {exc}"
                ) from exc
            published_logs = True
            for staged_log in staged_logs.iterdir():
                _exclusive_copy2(staged_log, logs_dir / staged_log.name)

            _exclusive_write_text(
                metrics_path, staged_metrics.read_text(encoding="utf-8")
            )
            published_metrics = True
            receipt["status"] = "collected"
            receipt["collection"] = collection
            _atomic_write_json(receipt_path, receipt)
            return collection
        except BaseException:
            if published_metrics:
                with contextlib.suppress(OSError):
                    metrics_path.unlink(missing_ok=True)
            if published_logs:
                shutil.rmtree(logs_dir, ignore_errors=True)
            raise
        finally:
            shutil.rmtree(staging, ignore_errors=True)


def report_path(repo: Path, config: Config, experiment_id: str) -> Path:
    return result_dir(repo, config, experiment_id) / "report.md"


def _format_metric(value: object) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return "n/a"
    return format(value, "g") if isinstance(value, float) else str(value)


def _render_report(
    request: dict[str, Any],
    receipt: dict[str, Any],
    metrics: dict[str, dict[str, Any]],
    *,
    request_file: str,
) -> str:
    code = request["code"]
    collection = receipt.get("collection")
    collection = collection if isinstance(collection, dict) else {}
    scheduler = collection.get("scheduler")
    scheduler = scheduler if isinstance(scheduler, dict) else {}
    exit_codes = sorted(
        {
            str(row["exit_code"])
            for row in scheduler.get("jobs", [])
            if isinstance(row, dict) and row.get("exit_code")
        }
    )
    verdict = str(scheduler.get("state") or "UNKNOWN")
    if exit_codes:
        verdict += f" (exit {', '.join(exit_codes)})"
    request_line = f"`{request_file}`"
    if request.get("rerun_of"):
        request_line += f" (rerun of `{request['rerun_of']}`"
        if request.get("rerun_reason"):
            request_line += f": {request['rerun_reason']}"
        request_line += ")"
    submitted = " ".join(
        filter(
            None,
            [
                str(receipt.get("job_id") or "unknown"),
                (
                    f"submitted {receipt['submitted_at']}"
                    if receipt.get("submitted_at")
                    else ""
                ),
                f"by {receipt['submitted_by']}" if receipt.get("submitted_by") else "",
            ],
        )
    )
    logs = [str(log) for log in collection.get("logs", []) if log]
    facts = [
        ("Request", request_line),
        ("Commit", f"`{code['commit']}` ({code['branch']})"),
        ("Job", submitted),
        ("Scheduler", verdict),
        ("Collected", collection.get("collected_at")),
        ("Logs", ", ".join(f"`{log}`" for log in logs) or "none"),
    ]
    lines = [f"# {request['title']}", ""]
    lines.extend(f"- {label}: {value}" for label, value in facts if value)
    lines.extend(
        [
            "",
            "## Question",
            "",
            str(request["question"]),
            "",
            "## Decision rule",
            "",
            str(request["decision_rule"]),
            "",
            "## Metrics",
            "",
        ]
    )
    log_names = sorted(metrics)
    if log_names:
        columns = " | ".join(f"`{name}`" for name in log_names)
        lines.append(f"| metric | {columns} |")
        lines.append("|---|" + "---|" * len(log_names))
        for metric in request["outputs"]["metrics"]:
            cells = [
                _format_metric((metrics[name] or {}).get(metric)) for name in log_names
            ]
            lines.append(f"| {metric} | " + " | ".join(cells) + " |")
    else:
        lines.append("No metrics were extracted.")
    missing = [str(name) for name in collection.get("missing_metrics", []) or []]
    if missing:
        rendered = ", ".join(f"`{name}`" for name in missing)
        lines.extend(["", f"Missing from every log: {rendered}"])
    lines.extend(
        [
            "",
            "## Observations",
            "",
            (
                "<!-- What the logs show beyond the numbers: failures, warnings, "
                "qualitative samples. -->"
            ),
            "",
            "## Conclusion",
            "",
            "<!-- Apply the decision rule. What is the next decision? -->",
            "",
        ]
    )
    return "\n".join(lines)


def report_request(repo: Path, config: Config, experiment_id: str) -> dict[str, Any]:
    """Scaffold results/<id>/report.md from the request, receipt, and metrics."""
    request, path = load_request(repo, config, experiment_id, check_git=False)
    destination = result_dir(repo, config, experiment_id)
    receipt_path = destination / "receipt.json"
    if not receipt_path.is_file():
        raise ExpctlError(f"not submitted yet: no receipt at {receipt_path}")
    receipt = _load_receipt(receipt_path)
    if receipt.get("status") != "collected":
        raise ExpctlError(
            f"results for {experiment_id} are not collected yet (receipt status: "
            f"{receipt.get('status', 'invalid')}); run expctl collect "
            f"{experiment_id} first"
        )
    target = report_path(repo, config, experiment_id)
    if target.exists():
        raise ExpctlError(f"report already exists: {target}; edit it in place")
    metrics_path = destination / "metrics.json"
    metrics: dict[str, dict[str, Any]] = {}
    if metrics_path.is_file():
        loaded = _load_json_object(metrics_path, "metrics file")
        metrics = {
            str(name): values
            for name, values in loaded.items()
            if isinstance(values, dict)
        }
    content = _render_report(
        request,
        receipt,
        metrics,
        request_file=str(path.relative_to(repo)).replace("\\", "/"),
    )
    _exclusive_write_text(target, content)
    collection = receipt.get("collection")
    collection = collection if isinstance(collection, dict) else {}
    scheduler = collection.get("scheduler")
    found = sorted({metric for values in metrics.values() for metric in values})
    return {
        "experiment_id": experiment_id,
        "path": str(target.relative_to(repo)).replace("\\", "/"),
        "result_path": str(destination.relative_to(repo)).replace("\\", "/"),
        "metrics": found,
        "missing_metrics": list(collection.get("missing_metrics", []) or []),
        "scheduler": scheduler.get("state") if isinstance(scheduler, dict) else None,
    }


def _receipt_summary(repo: Path, receipt_path: Path) -> str:
    """One line about an existing receipt: who submitted what, and how it ended."""
    try:
        receipt = _load_receipt(receipt_path)
    except ExpctlError:
        return "unreadable receipt"
    parts: list[str] = []
    job_id = receipt.get("job_id")
    if job_id:
        parts.append(f"job {job_id}")
    when, who = receipt.get("submitted_at"), receipt.get("submitted_by")
    if when or who:
        parts.append(
            " ".join(filter(None, ["submitted", when, f"by {who}" if who else ""]))
        )
    if receipt.get("status"):
        parts.append(f"receipt status {receipt['status']}")
    if job_id:
        try:
            state = _scheduler_status(repo, str(job_id))["state"]
        except ExpctlError:  # no sacct on this host
            state = "UNKNOWN"
        if state != "UNKNOWN":
            parts.append(f"sacct {state}")
    return ", ".join(parts) or "no details"


def _next_rerun_id(repo: Path, config: Config, experiment_id: str) -> str:
    base = RERUN_SUFFIX_RE.sub("", experiment_id)
    number = 2
    while request_path(repo, config, f"{base}-r{number}").exists():
        number += 1
    return f"{base}-r{number}"


def rerun_request(
    repo: Path,
    config: Config,
    experiment_id: str,
    *,
    new_id: str | None = None,
    reason: str | None = None,
    check_git: bool = True,
) -> dict[str, Any]:
    """Copy a submitted request under a new ID so it can be submitted again.

    The copy is byte-identical apart from the top-level `id` line, which is
    followed by `rerun_of` (and `rerun_reason` when given); any earlier
    `rerun_of`/`rerun_reason` lines are dropped so the chain always points at
    the immediate predecessor. Commit and worktree stay the same: `submit`
    reuses an existing worktree that sits at the pinned commit. The original
    request, its receipt, and its logs are untouched.
    """
    _, path = load_request(repo, config, experiment_id, check_git=False)
    receipt_path = result_dir(repo, config, experiment_id) / "receipt.json"
    if not receipt_path.is_file():
        raise ExpctlError(
            f"nothing to rerun: {experiment_id} has no receipt; submit it instead"
        )
    receipt = _load_receipt(receipt_path)
    if not isinstance(receipt.get("job_id"), str) or not receipt["job_id"]:
        raise ExpctlError(
            f"nothing to rerun safely: {experiment_id} has no confirmed job ID "
            f"(receipt status: {receipt.get('status', 'invalid')})"
        )
    if new_id is None:
        new_id = _next_rerun_id(repo, config, experiment_id)
    new_path = request_path(repo, config, new_id)
    if new_path.exists():
        raise ExpctlError(f"request already exists: {new_path}")
    if reason is not None and any(c in reason for c in '"\\\n\r'):
        raise ExpctlError(
            "rerun reason cannot contain quotes, backslashes, or newlines"
        )

    output: list[str] = []
    replaced = in_table = False
    with path.open(encoding="utf-8", newline="") as handle:  # keep CRLF as is
        source = handle.read()
    for line in source.splitlines(keepends=True):
        body = line.rstrip("\r\n")
        newline = line[len(body) :]
        if body.lstrip().startswith("["):
            in_table = True
        id_match = ID_LINE_RE.match(body)
        if not in_table and not replaced and id_match:
            indent = id_match.group(1)
            output.append(f'{indent}id = "{new_id}"{newline}')
            output.append(f'{indent}rerun_of = "{experiment_id}"{newline}')
            if reason is not None:
                output.append(f'{indent}rerun_reason = "{reason}"{newline}')
            replaced = True
            continue
        if not in_table and re.match(r"^\s*rerun_(of|reason)\s*=", body):
            continue
        output.append(line)
    if not replaced:
        raise ExpctlError(f"could not find the top-level id line in {path}")

    _exclusive_write_text(new_path, "".join(output))
    try:
        request, _ = load_request(repo, config, new_id, check_git=check_git)
    except ExpctlError:
        new_path.unlink()
        raise
    return {
        "experiment_id": new_id,
        "rerun_of": experiment_id,
        "path": str(new_path.relative_to(repo)).replace("\\", "/"),
        "commit": request["code"]["commit"],
        "worktree": request["code"]["worktree"],
    }


def _render_summary(
    heading: str,
    fields: list[tuple[str, object]],
    *,
    next_steps: list[str] | None = None,
) -> str:
    visible = [(label, value) for label, value in fields if value not in (None, "")]
    width = max((len(label) for label, _ in visible), default=0)
    lines = [heading]
    lines.extend(
        f"{label + ':':<{width + 1}}  {_safe_list_cell(value)}"
        for label, value in visible
    )
    if next_steps:
        lines.extend(["", "Next:"])
        lines.extend(f"  {_safe_list_cell(step)}" for step in next_steps)
    return "\n".join(lines)


def _render_new_result(result: dict[str, Any]) -> str:
    experiment_id = str(result["experiment_id"])
    path = str(result["path"])
    return _render_summary(
        "REQUEST CREATED",
        [
            ("Experiment", experiment_id),
            ("Path", path),
            ("Commit", result["commit"]),
            ("Branch", result["branch"]),
            ("Worktree", result["worktree"]),
            (
                "Uncommitted",
                len(result.get("uncommitted_changes", [])) or None,
            ),
        ],
        next_steps=[
            f"Edit {path}",
            shlex.join(["expctl", "validate", experiment_id]),
        ],
    )


def _render_validate_summary(
    experiment_id: str, request: dict[str, Any], *, verified_nodes: int
) -> str:
    code, slurm, outputs = request["code"], request["slurm"], request["outputs"]
    metrics = list(outputs["metrics"])
    requirements = list(request.get("notes", {}).get("requirements", []))
    env = slurm.get("env", {})
    shown_metrics = ", ".join(metrics[:6]) + (", ..." if len(metrics) > 6 else "")
    return _render_summary(
        f"valid: {experiment_id}",
        [
            ("Commit", f"{code['commit'][:12]} ({code['branch']})"),
            ("Script", slurm["script"]),
            (
                "Nodes",
                f"{verified_nodes} verified / {slurm['max_concurrent_nodes']} declared",
            ),
            ("Env", ", ".join(f"{k}={v}" for k, v in sorted(env.items())) or None),
            ("Logs", outputs["log_glob"]),
            ("Metrics", f"{len(metrics)}: {shown_metrics}"),
            ("Requires", ", ".join(requirements) or None),
            ("Rerun of", request.get("rerun_of")),
        ],
    )


def _render_doctor_result(
    result: dict[str, Any], *, require_cluster: bool = False
) -> str:
    checks = result["checks"]
    name_width = max(len(str(check["name"])) for check in checks)
    cluster_note = ""
    if not result["cluster_ready"] and not require_cluster:
        cluster_note = " (informational here; --cluster makes it required)"
    lines = [
        "EXPCTL DOCTOR",
        f"Repository ready: {'yes' if result['repository_ready'] else 'no'}",
        f"Cluster ready:    {'yes' if result['cluster_ready'] else 'no'}{cluster_note}",
        "",
    ]
    for check in checks:
        if check["ok"]:
            status = "OK"
        elif check["scope"] == "cluster" and not require_cluster:
            status = "INFO"
        elif check["required"]:
            status = "FAIL"
        else:
            status = "OPTIONAL"
        lines.append(
            f"{check['name']!s:<{name_width}}  {status:<8}  "
            f"{_safe_list_cell(check['detail'])}"
        )
    if not result["repository_ready"]:
        lines.extend(["", "Next:", "  Fix the failed repository checks above."])
    elif not result["cluster_ready"] and require_cluster:
        lines.extend(
            [
                "",
                "Next:",
                "  Run expctl on a POSIX cluster host with the missing SLURM tools.",
            ]
        )
    elif not result["cluster_ready"]:
        lines.extend(
            [
                "",
                "Next:",
                (
                    "  Repository is ready for authoring. On the cluster host, run: "
                    "expctl doctor --cluster"
                ),
            ]
        )
    return "\n".join(lines)


def _render_submit_result(
    result: dict[str, Any],
    *,
    dry_run: bool,
    worktree_root: Path | None,
    skip_node_check: bool,
) -> str:
    experiment_id = str(result["experiment_id"])
    nodes = (
        f"{result.get('verified_max_concurrent_nodes')} verified / "
        f"{result.get('declared_max_concurrent_nodes')} declared"
    )
    fields: list[tuple[str, object]] = [
        ("Experiment", experiment_id),
        ("Job", result.get("job_id")),
        ("Commit", result.get("commit")),
        ("Worktree", result.get("worktree")),
        ("Nodes", nodes),
    ]
    if dry_run:
        fields.append(("Command", shlex.join(result["command"])))
        submit_command = ["expctl", "submit", experiment_id]
        if worktree_root is not None:
            submit_command.extend(["--worktree-root", str(worktree_root)])
        if skip_node_check:
            submit_command.append("--skip-node-check")
        return _render_summary(
            "SUBMISSION PREVIEW",
            fields,
            next_steps=[shlex.join(submit_command)],
        )
    return _render_summary(
        "SUBMITTED",
        fields,
        next_steps=[shlex.join(["expctl", "status", experiment_id])],
    )


def _render_status_result(result: dict[str, Any], *, result_path: str) -> str:
    experiment_id = str(result["experiment_id"])
    receipt_status = str(result.get("receipt_status", "invalid"))
    counts = result.get("queue_counts", {})
    accounting = result.get("accounting", {})
    queue_rows = result.get("queue", [])
    cancellation = result.get("cancellation", {})
    state = _status_state(result)

    queue_summary = None
    if isinstance(counts, dict) and counts:
        queue_summary = ", ".join(
            f"{name}={count}" for name, count in sorted(counts.items())
        )
    reasons = sorted(
        {
            str(row.get("reason"))
            for row in queue_rows
            if isinstance(row, dict) and row.get("reason") not in (None, "", "None")
        }
    )
    exit_codes = (
        sorted(
            {
                str(row.get("exit_code"))
                for row in accounting.get("jobs", [])
                if isinstance(row, dict) and row.get("exit_code")
            }
        )
        if isinstance(accounting, dict)
        else []
    )
    detail = result.get("detail")
    if not detail and isinstance(accounting, dict):
        detail = accounting.get("detail")
    fields = [
        ("Experiment", experiment_id),
        ("Status", state),
        ("Job", result.get("job_id")),
        ("Receipt", receipt_status),
        ("Queue", queue_summary),
        ("Source", result.get("source")),
        ("Reason", ", ".join(reasons) if reasons else None),
        (
            "Scheduler",
            accounting.get("state")
            if receipt_status == "collected" and isinstance(accounting, dict)
            else None,
        ),
        ("Exit", ", ".join(exit_codes) if exit_codes else None),
        (
            "Cancelled",
            " ".join(
                filter(
                    None,
                    [
                        str(cancellation.get("requested_at", "")),
                        (
                            f"by {cancellation['requested_by']}"
                            if cancellation.get("requested_by")
                            else ""
                        ),
                    ],
                )
            )
            if isinstance(cancellation, dict)
            else None,
        ),
        (
            "Cancel reason",
            cancellation.get("reason") if isinstance(cancellation, dict) else None,
        ),
        ("Checked", result.get("checked_at")),
        ("Detail", detail),
    ]
    if not result.get("job_id"):
        next_steps = [f"Inspect {result_path}/receipt.json before retrying."]
    elif receipt_status == "collected" and result.get("report"):
        next_steps = [f"Review {result['report']}"]
    elif receipt_status == "collected":
        next_steps = [shlex.join(["expctl", "report", experiment_id])]
    elif not _status_is_terminal(result):
        command = shlex.join(["expctl", "status", experiment_id])
        next_steps = [f"Wait for SLURM, then run: {command}"]
    else:
        next_steps = [shlex.join(["expctl", "collect", experiment_id])]
    return _render_summary("EXPERIMENT STATUS", fields, next_steps=next_steps)


def _render_cancel_result(result: dict[str, Any]) -> str:
    experiment_id = str(result["experiment_id"])
    if result.get("dry_run"):
        cancel_command = ["expctl", "cancel", experiment_id]
        if result.get("reason"):
            cancel_command.extend(["--reason", str(result["reason"])])
        return _render_summary(
            "CANCELLATION PREVIEW",
            [
                ("Experiment", experiment_id),
                ("Job", result["job_id"]),
                ("Reason", result.get("reason")),
                ("Command", shlex.join(result["command"])),
            ],
            next_steps=[shlex.join(cancel_command)],
        )
    return _render_summary(
        "CANCELLATION REQUESTED",
        [
            ("Experiment", experiment_id),
            ("Job", result["job_id"]),
            ("Reason", result.get("reason")),
            ("Requested", result.get("requested_at")),
            ("By", result.get("requested_by")),
        ],
        next_steps=[shlex.join(["expctl", "status", experiment_id, "--watch"])],
    )


def _render_collect_result(
    result: dict[str, Any], *, experiment_id: str, result_path: str
) -> str:
    scheduler = result.get("scheduler", {})
    scheduler_state = (
        scheduler.get("state") if isinstance(scheduler, dict) else scheduler
    )
    missing = result.get("missing_metrics", [])
    return _render_summary(
        "RESULTS COLLECTED",
        [
            ("Experiment", experiment_id),
            ("Scheduler", scheduler_state),
            ("Logs", len(result.get("logs", []))),
            ("Metrics", result.get("metrics_file")),
            ("Missing", ", ".join(missing) if missing else "none"),
        ],
        next_steps=[
            shlex.join(["expctl", "report", experiment_id])
            + f"  # scaffold {result_path}/report.md"
        ],
    )


def _render_report_result(result: dict[str, Any]) -> str:
    experiment_id = str(result["experiment_id"])
    path = str(result["path"])
    found = result.get("metrics", [])
    missing = result.get("missing_metrics", [])
    return _render_summary(
        "REPORT CREATED",
        [
            ("Experiment", experiment_id),
            ("Path", path),
            ("Scheduler", result.get("scheduler")),
            ("Metrics", ", ".join(found) if found else "none extracted"),
            ("Missing", ", ".join(missing) if missing else None),
        ],
        next_steps=[
            f"Fill in Observations and Conclusion in {path}",
            shlex.join(["git", "add", str(result["result_path"])]) + " && git commit",
        ],
    )


def _render_rerun_result(result: dict[str, Any]) -> str:
    experiment_id = str(result["experiment_id"])
    path = str(result["path"])
    return _render_summary(
        "RERUN REQUEST CREATED",
        [
            ("Experiment", experiment_id),
            ("Rerun of", result["rerun_of"]),
            ("Path", path),
            ("Commit", result["commit"]),
            ("Worktree", result["worktree"]),
        ],
        next_steps=[
            shlex.join(["git", "add", path]),
            shlex.join(["git", "commit", "-m", f"Add rerun request {experiment_id}"]),
            shlex.join(["expctl", "submit", experiment_id]),
        ],
    )


def _render_clean_result(result: dict[str, Any]) -> str:
    experiment_id = str(result["experiment_id"])
    if result.get("already_removed_at"):
        heading = "WORKTREE ALREADY CLEAN"
        action = "already removed"
        next_steps = None
    elif result.get("dry_run"):
        heading = "CLEANUP PREVIEW"
        action = "would remove" if result.get("worktree_exists") else "already absent"
        next_steps = (
            [
                shlex.join(
                    [
                        "expctl",
                        "clean",
                        experiment_id,
                        "--worktree-root",
                        str(Path(result["worktree"]).parent),
                    ]
                )
            ]
            if result.get("worktree_exists")
            else None
        )
    elif result.get("removed"):
        heading = "WORKTREE REMOVED"
        action = "removed"
        next_steps = ["Commit the updated receipt when you are ready."]
    else:
        heading = "WORKTREE ALREADY CLEAN"
        action = "already removed"
        next_steps = None
    return _render_summary(
        heading,
        [
            ("Experiment", experiment_id),
            ("Worktree", result["worktree"]),
            ("Action", action),
            ("Removed", result.get("removed_at") or result.get("already_removed_at")),
        ],
        next_steps=next_steps,
    )


def _print_command_result(
    result: dict[str, Any], human_output: str, *, force_json: bool
) -> bool:
    """Print one command result; return whether human output was selected."""
    human = sys.stdout.isatty() and not force_json
    if human:
        print(_stdout_safe(human_output))
    else:
        print(json.dumps(result, indent=2, sort_keys=True))
    return human


LIST_HEADERS = ("EXPERIMENT ID", "STATUS", "TITLE")
STATUS_COLORS = {
    "requested": "\x1b[36m",
    "preparing": "\x1b[33m",
    "submitting": "\x1b[33m",
    "submission_unknown": "\x1b[31m",
    "submitted": "\x1b[34m",
    "cancel_requested": "\x1b[35m",
    "collected": "\x1b[32m",
    "reviewed": "\x1b[32m",
    "invalid": "\x1b[31m",
    "PENDING": "\x1b[33m",
    "CONFIGURING": "\x1b[33m",
    "REQUEUED": "\x1b[33m",
    "RUNNING": "\x1b[34m",
    "COMPLETING": "\x1b[34m",
    "SUSPENDED": "\x1b[35m",
    "MIXED": "\x1b[35m",
    "COMPLETED": "\x1b[32m",
    "FAILED": "\x1b[31m",
    "CANCELLED": "\x1b[31m",
    "TIMEOUT": "\x1b[31m",
    "OUT_OF_MEMORY": "\x1b[31m",
    "NODE_FAIL": "\x1b[31m",
    "BOOT_FAIL": "\x1b[31m",
    "PREEMPTED": "\x1b[31m",
}
ANSI_RESET = "\x1b[0m"


def _safe_list_cell(value: object) -> str:
    """Keep one request field on one terminal line without control sequences."""
    output: list[str] = []
    for character in str(value):
        if character in "\t\r\n":
            output.append(" ")
        elif unicodedata.category(character).startswith("C"):
            output.append("?")
        else:
            output.append(character)
    return "".join(output)


def _display_width(value: str) -> int:
    width = 0
    for character in value:
        if unicodedata.combining(character) or unicodedata.category(character) in {
            "Mn",
            "Me",
        }:
            continue
        width += 2 if unicodedata.east_asian_width(character) in {"W", "F"} else 1
    return width


def _stdout_safe(value: str) -> str:
    encoding = sys.stdout.encoding or "utf-8"
    try:
        value.encode(encoding)
        return value
    except LookupError:
        return value
    except UnicodeEncodeError:
        return value.encode(encoding, errors="replace").decode(encoding)


def _list_rule_character() -> str:
    return "─" if _stdout_safe("─") == "─" else "-"


def _fit_list_cell(value: str, width: int) -> str:
    if width < 1:
        return ""
    if _display_width(value) <= width:
        return value + " " * (width - _display_width(value))
    if width == 1:
        return "…"
    output: list[str] = []
    used = 0
    for character in value:
        character_width = _display_width(character)
        if used + character_width > width - 1:
            break
        output.append(character)
        used += character_width
    return "".join(output) + "…" + " " * (width - used - 1)


def _list_table_widths(
    rows: list[dict[str, str]], terminal_width: int
) -> tuple[int, int, int]:
    terminal_width = max(12, terminal_width)
    id_natural = max(
        _display_width(LIST_HEADERS[0]),
        *(_display_width(row["id"]) for row in rows),
    )
    status_natural = max(
        _display_width(LIST_HEADERS[1]),
        *(_display_width(row["status"]) for row in rows),
    )
    title_natural = max(
        _display_width(LIST_HEADERS[2]),
        *(_display_width(row["title"]) for row in rows),
    )
    gap_width = 4
    minimum_title = max(1, min(8, terminal_width // 4))
    fixed_space = max(2, terminal_width - gap_width - minimum_title)
    status_width = min(
        status_natural, max(1, min(terminal_width // 4, fixed_space - 1))
    )
    id_width = min(id_natural, max(1, fixed_space - status_width))
    title_width = max(
        1, min(title_natural, terminal_width - gap_width - id_width - status_width)
    )
    return id_width, status_width, title_width


def _render_list_table(
    rows: list[dict[str, str]], *, terminal_width: int, color: bool
) -> str:
    if not rows:
        return "No experiment requests."
    safe_rows = [
        {key: _safe_list_cell(row[key]) for key in ("id", "status", "title")}
        for row in rows
    ]
    widths = _list_table_widths(safe_rows, terminal_width)
    header = "  ".join(
        _fit_list_cell(value, width)
        for value, width in zip(LIST_HEADERS, widths, strict=True)
    ).rstrip()
    rule = _list_rule_character()
    separator = "  ".join(rule * width for width in widths).rstrip()
    lines = [header, separator]
    for row in safe_rows:
        identifier = _fit_list_cell(row["id"], widths[0])
        status = _fit_list_cell(row["status"], widths[1])
        title = _fit_list_cell(row["title"], widths[2]).rstrip()
        if color and row["status"] in STATUS_COLORS:
            status = f"{STATUS_COLORS[row['status']]}{status}{ANSI_RESET}"
        lines.append(f"{identifier}  {status}  {title}".rstrip())
    return "\n".join(lines)


def _render_list_tsv(rows: list[dict[str, str]]) -> str:
    return "\n".join(
        "\t".join(_safe_list_cell(row[key]) for key in ("id", "status", "title"))
        for row in rows
    )


def _list_color_enabled() -> bool:
    return (
        sys.stdout.isatty()
        and "NO_COLOR" not in os.environ
        and os.environ.get("TERM") != "dumb"
    )


def list_requests(repo: Path, config: Config) -> list[dict[str, str]]:
    requests_dir = repo / config.root / "requests"
    rows: list[dict[str, str]] = []
    submitted_rows: dict[str, list[dict[str, str]]] = {}
    commit_cache: dict[str, str | None] = {}
    script_cache: dict[tuple[str, str], tuple[int | None, str | None]] = {}
    for path in sorted(requests_dir.glob("*.toml"), key=lambda p: p.stem):
        experiment_id = path.stem
        try:
            data, _ = load_request(repo, config, experiment_id, check_git=False)
            _validate_request_git(
                repo,
                config,
                data,
                commit_cache=commit_cache,
                script_cache=script_cache,
            )
            receipt = result_dir(repo, config, experiment_id) / "receipt.json"
            status = "requested"
            if receipt.is_file():
                receipt_data = _load_receipt(receipt)
                stored_status = receipt_data.get("status")
                status = (
                    stored_status
                    if isinstance(stored_status, str) and stored_status
                    else "invalid"
                )
                job_id = receipt_data.get("job_id")
                if (
                    status in {"submitted", "cancel_requested"}
                    and isinstance(job_id, str)
                    and job_id
                ):
                    row = {
                        "id": experiment_id,
                        "status": status,
                        "title": data["title"],
                    }
                    submitted_rows.setdefault(job_id, []).append(row)
                    rows.append(row)
                    continue
                if (
                    status == "collected"
                    and report_path(repo, config, experiment_id).is_file()
                ):
                    status = "reviewed"
            rows.append({"id": experiment_id, "status": status, "title": data["title"]})
        except ExpctlError as exc:
            rows.append({"id": experiment_id, "status": "invalid", "title": str(exc)})

    if submitted_rows:
        live_statuses, warning = _live_scheduler_statuses(repo, list(submitted_rows))
        for job_id, live_status in live_statuses.items():
            for row in submitted_rows.get(job_id, []):
                row["status"] = live_status
        if warning:
            print(f"warning: {warning}", file=sys.stderr)
    return rows


def _status_filter_argument(value: str) -> tuple[str, ...]:
    statuses = tuple(status.strip().casefold() for status in value.split(","))
    if not statuses or any(not status for status in statuses):
        raise argparse.ArgumentTypeError(
            "statuses must be a comma-separated list, for example running,failed"
        )
    return statuses


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _nonnegative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a non-negative integer") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be a non-negative integer")
    return parsed


def _positive_seconds(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive number") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive number")
    return parsed


def _select_list_rows(
    rows: list[dict[str, str]],
    *,
    statuses: set[str],
    sort_order: str,
    limit: int | None,
) -> list[dict[str, str]]:
    selected = [
        row for row in rows if not statuses or row["status"].casefold() in statuses
    ]
    selected.sort(key=lambda row: row["id"], reverse=sort_order == "newest")
    return selected[:limit] if limit is not None else selected


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="expctl",
        description="Manage repository-backed asynchronous experiment handoffs.",
    )
    parser.add_argument("--version", action="version", version=f"expctl {__version__}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser(
        "init", help=f"create {CONFIG_NAME} and the configured data skeleton"
    )

    doctor = subparsers.add_parser(
        "doctor", help="check repository configuration and cluster dependencies"
    )
    doctor.add_argument(
        "--cluster",
        action="store_true",
        help="also require the SLURM checks (exit 1 unless this is a cluster host)",
    )
    doctor.add_argument(
        "--json", action="store_true", help="emit JSON even in an interactive terminal"
    )

    new = subparsers.add_parser(
        "new", help="create a request from the repository template"
    )
    new.add_argument("experiment_id", metavar="ID")
    new.add_argument(
        "--allow-dirty",
        action="store_true",
        help="create the request even when changes are not included in HEAD",
    )
    new.add_argument(
        "--json", action="store_true", help="emit JSON even in an interactive terminal"
    )

    listing = subparsers.add_parser(
        "list", help="list requests and their repository state"
    )
    list_formats = listing.add_mutually_exclusive_group()
    list_formats.add_argument(
        "--table",
        dest="list_format",
        action="store_const",
        const="table",
        help="force an aligned table even when output is redirected",
    )
    list_formats.add_argument(
        "--tsv",
        dest="list_format",
        action="store_const",
        const="tsv",
        help="force stable tab-separated output",
    )
    list_formats.add_argument(
        "--json",
        dest="list_format",
        action="store_const",
        const="json",
        help="emit a JSON array",
    )
    listing.set_defaults(list_format="auto")
    listing.add_argument(
        "--no-color", action="store_true", help="disable status colors in table output"
    )
    listing.add_argument(
        "--status",
        action="append",
        default=[],
        type=_status_filter_argument,
        metavar="STATUS[,STATUS...]",
        help="include only these stored or live statuses; may be repeated",
    )
    listing.add_argument(
        "--sort",
        choices=("newest", "oldest"),
        default="newest",
        help="sort by experiment ID (default: newest)",
    )
    listing.add_argument(
        "--limit",
        type=_positive_int,
        metavar="N",
        help="show at most N experiments after filtering and sorting",
    )

    validate = subparsers.add_parser("validate", help="validate one request")
    validate.add_argument("experiment_id", metavar="ID")

    show = subparsers.add_parser("show", help="print one validated request as JSON")
    show.add_argument("experiment_id", metavar="ID")

    submit = subparsers.add_parser("submit", help="submit one request through SLURM")
    submit.add_argument("experiment_id", metavar="ID")
    submit.add_argument(
        "--dry-run",
        action="store_true",
        help="validate and preview without creating a worktree or calling sbatch",
    )
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
    submit.add_argument(
        "--json", action="store_true", help="emit JSON even in an interactive terminal"
    )

    status = subparsers.add_parser(
        "status", help="report queue and accounting state for a submitted request"
    )
    status.add_argument("experiment_id", metavar="ID")
    status.add_argument(
        "--watch",
        nargs="?",
        const=5.0,
        type=_positive_seconds,
        metavar="SECONDS",
        help="refresh until the job reaches a terminal state (default: every 5s)",
    )
    status.add_argument(
        "--json", action="store_true", help="emit JSON even in an interactive terminal"
    )

    cancel = subparsers.add_parser(
        "cancel", help="cancel a submitted SLURM job and record the request"
    )
    cancel.add_argument("experiment_id", metavar="ID")
    cancel.add_argument("--reason", metavar="TEXT", help="record why it was cancelled")
    cancel.add_argument(
        "--dry-run",
        action="store_true",
        help="show the scancel command without cancelling the job",
    )
    cancel.add_argument(
        "--json", action="store_true", help="emit JSON even in an interactive terminal"
    )

    logs = subparsers.add_parser("logs", help="show or follow scheduler log files")
    logs.add_argument("experiment_id", metavar="ID")
    logs.add_argument(
        "--tail",
        type=_nonnegative_int,
        default=100,
        metavar="N",
        help="show the last N lines from each log (default: 100)",
    )
    logs.add_argument(
        "--follow",
        action="store_true",
        help="continue printing data appended to matching logs",
    )
    logs.add_argument(
        "--worktree-root",
        type=Path,
        help="parent directory used for the submitted experiment worktree",
    )

    collect = subparsers.add_parser("collect", help="copy logs and extract metrics")
    collect.add_argument("experiment_id", metavar="ID")
    collect.add_argument(
        "--worktree-root",
        type=Path,
        help="parent directory used for the submitted experiment worktree",
    )
    collect.add_argument(
        "--json", action="store_true", help="emit JSON even in an interactive terminal"
    )

    clean = subparsers.add_parser(
        "clean", help="remove a verified worktree after results are collected"
    )
    clean.add_argument("experiment_id", metavar="ID")
    clean.add_argument(
        "--dry-run",
        action="store_true",
        help="verify and preview without removing the worktree",
    )
    clean.add_argument(
        "--worktree-root",
        type=Path,
        help="parent directory used for the submitted experiment worktree",
    )
    clean.add_argument(
        "--json", action="store_true", help="emit JSON even in an interactive terminal"
    )

    report = subparsers.add_parser(
        "report",
        help="scaffold results/<id>/report.md from the request, receipt, and metrics",
    )
    report.add_argument("experiment_id", metavar="ID")
    report.add_argument(
        "--json", action="store_true", help="emit JSON even in an interactive terminal"
    )

    rerun = subparsers.add_parser(
        "rerun",
        help="copy a submitted request to a new ID so it can be submitted again",
    )
    rerun.add_argument("experiment_id", metavar="ID")
    rerun.add_argument(
        "--as",
        dest="new_id",
        metavar="NEW_ID",
        help="ID for the copy (default: <id>-r2, then -r3, ...)",
    )
    rerun.add_argument(
        "--reason",
        metavar="TEXT",
        help='recorded in the copy as rerun_reason, e.g. "preempted"',
    )
    rerun.add_argument(
        "--json", action="store_true", help="emit JSON even in an interactive terminal"
    )
    return parser


def _watch_status_command(
    repo: Path,
    config: Config,
    experiment_id: str,
    *,
    interval: float,
    force_json: bool,
) -> int:
    human = sys.stdout.isatty() and not force_json
    first = True
    while True:
        result = status_request(repo, config, experiment_id)
        if human:
            if not first and os.environ.get("TERM") != "dumb":
                sys.stdout.write("\x1b[2J\x1b[H")
            result_path = str(
                result_dir(repo, config, experiment_id).relative_to(repo)
            ).replace("\\", "/")
            print(_stdout_safe(_render_status_result(result, result_path=result_path)))
        else:
            print(json.dumps(result, ensure_ascii=True, sort_keys=True), flush=True)
        if _status_is_terminal(result):
            return 0
        first = False
        time.sleep(interval)


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
        if args.command == "doctor":
            result = doctor_repo(repo)
            _print_command_result(
                result,
                _render_doctor_result(result, require_cluster=args.cluster),
                force_json=args.json,
            )
            ready = result["repository_ready"] and (
                result["cluster_ready"] or not args.cluster
            )
            return 0 if ready else 1
        config = load_config(repo)
        if args.command == "new":
            result = new_request(
                repo,
                config,
                args.experiment_id,
                allow_dirty=args.allow_dirty,
            )
            _print_command_result(
                result, _render_new_result(result), force_json=args.json
            )
        elif args.command == "list":
            rows = list_requests(repo, config)
            statuses = {status for group in args.status for status in group}
            rows = _select_list_rows(
                rows,
                statuses=statuses,
                sort_order=args.sort,
                limit=args.limit,
            )
            output_format = args.list_format
            if output_format == "auto":
                output_format = "table" if sys.stdout.isatty() else "tsv"
            if output_format == "json":
                output = json.dumps(rows, indent=2, ensure_ascii=True, sort_keys=True)
            elif output_format == "table":
                output = _render_list_table(
                    rows,
                    terminal_width=shutil.get_terminal_size(fallback=(120, 24)).columns,
                    color=_list_color_enabled() and not args.no_color,
                )
            else:
                output = _render_list_tsv(rows)
            if output:
                print(_stdout_safe(output))
        elif args.command == "validate":
            request, _ = load_request(repo, config, args.experiment_id, check_git=False)
            verified_nodes = _validate_request_git(repo, config, request)
            if sys.stdout.isatty():
                print(
                    _stdout_safe(
                        _render_validate_summary(
                            args.experiment_id, request, verified_nodes=verified_nodes
                        )
                    )
                )
            else:
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
            _print_command_result(
                result,
                _render_submit_result(
                    result,
                    dry_run=args.dry_run,
                    worktree_root=args.worktree_root,
                    skip_node_check=args.skip_node_check,
                ),
                force_json=args.json,
            )
        elif args.command == "status":
            if args.watch is not None:
                try:
                    return _watch_status_command(
                        repo,
                        config,
                        args.experiment_id,
                        interval=args.watch,
                        force_json=args.json,
                    )
                except KeyboardInterrupt:
                    print("\nStatus watch stopped.", file=sys.stderr)
                    return 130
            result = status_request(repo, config, args.experiment_id)
            result_path = str(
                result_dir(repo, config, args.experiment_id).relative_to(repo)
            ).replace("\\", "/")
            _print_command_result(
                result,
                _render_status_result(result, result_path=result_path),
                force_json=args.json,
            )
        elif args.command == "cancel":
            result = cancel_request(
                repo,
                config,
                args.experiment_id,
                reason=args.reason,
                dry_run=args.dry_run,
            )
            _print_command_result(
                result,
                _render_cancel_result(result),
                force_json=args.json,
            )
        elif args.command == "logs":
            if args.follow:
                try:
                    _follow_logs(
                        repo,
                        config,
                        args.experiment_id,
                        tail=args.tail,
                        worktree_root=args.worktree_root,
                    )
                except KeyboardInterrupt:
                    print("\nLog follow stopped.", file=sys.stderr)
                    return 130
            else:
                result = logs_request(
                    repo,
                    config,
                    args.experiment_id,
                    tail=args.tail,
                    worktree_root=args.worktree_root,
                )
                sys.stdout.write(_stdout_safe(_render_logs(result)))
        elif args.command == "collect":
            result = collect_request(
                repo,
                config,
                args.experiment_id,
                worktree_root=args.worktree_root,
            )
            result_path = str(
                result_dir(repo, config, args.experiment_id).relative_to(repo)
            ).replace("\\", "/")
            _print_command_result(
                result,
                _render_collect_result(
                    result,
                    experiment_id=args.experiment_id,
                    result_path=result_path,
                ),
                force_json=args.json,
            )
        elif args.command == "clean":
            result = clean_request(
                repo,
                config,
                args.experiment_id,
                dry_run=args.dry_run,
                worktree_root=args.worktree_root,
            )
            _print_command_result(
                result,
                _render_clean_result(result),
                force_json=args.json,
            )
        elif args.command == "report":
            result = report_request(repo, config, args.experiment_id)
            _print_command_result(
                result, _render_report_result(result), force_json=args.json
            )
        elif args.command == "rerun":
            result = rerun_request(
                repo,
                config,
                args.experiment_id,
                new_id=args.new_id,
                reason=args.reason,
            )
            human = _print_command_result(
                result, _render_rerun_result(result), force_json=args.json
            )
            if not human:
                print(
                    f"next: git add {result['path']} && git commit, "
                    f"then expctl submit {result['experiment_id']}",
                    file=sys.stderr,
                )
        return 0
    except ExpctlError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
