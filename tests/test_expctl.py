import json
import os
import subprocess
import time
import tomllib
from pathlib import Path

import pytest

from expctl import core
from expctl.core import (
    Config,
    ExpctlError,
    __version__,
    _ensure_runtime_links,
    _ensure_worktree,
    _receipt_summary,
    _script_max_concurrent_nodes,
    build_sbatch_command,
    extract_metrics,
    init_repo,
    load_config,
    load_request,
    rerun_request,
    status_request,
    submit_request,
)

EXAMPLE_ID = "20260101-example"
ZERO_COMMIT = "0" * 40
EXAMPLE_CONFIG = """\
version = 1

[scheduler]
required_script_lines = ["#SBATCH -p example-partition"]
max_total_nodes = 4

[runtime]
shared_dirs = [".venv", "data", "runs", "logs"]
create_missing = ["runs", "logs"]

[worktree]
root = ".."
"""
EXAMPLE_REQUEST = f"""\
version = 1
id = "{EXAMPLE_ID}"
title = "Example sweep"
question = "Does the framework parse a well-formed request?"
decision_rule = "Validation passes."

[code]
branch = "main"
commit = "{ZERO_COMMIT}"
worktree = "myproject-example"

[slurm]
script = "scripts/sweep.slurm"
max_concurrent_nodes = 4

[slurm.env]
NUM_SAMPLES = "256"

[outputs]
log_glob = "logs/example-{{job_id}}_*.out"
metrics = ["gen_ppl"]
"""
LOCAL_REQUEST = f"""\
version = 1
id = "{EXAMPLE_ID}"
title = "Local example"
question = "Does direct execution work?"
decision_rule = "The local process exits successfully."

[code]
branch = "main"
commit = "{ZERO_COMMIT}"
worktree = "myproject-local-example"

[local]
script = "scripts/run-local.py"
args = ["--one", "two words"]

[local.env]
LOCAL_VALUE = "3.5"

[outputs]
log_glob = "logs/local-{{job_id}}.out"
metrics = ["score"]
"""


def _example_repo(tmp_path: Path, request_text: str = EXAMPLE_REQUEST) -> Path:
    (tmp_path / "expctl.toml").write_text(EXAMPLE_CONFIG, encoding="utf-8")
    requests = tmp_path / "expctl" / "requests"
    requests.mkdir(parents=True)
    (requests / f"{EXAMPLE_ID}.toml").write_text(request_text, encoding="utf-8")
    return tmp_path


def test_missing_config_points_to_init(tmp_path: Path) -> None:
    with pytest.raises(ExpctlError, match="expctl init"):
        load_config(tmp_path)


def test_init_creates_skeleton_and_is_idempotent(tmp_path: Path) -> None:
    created = init_repo(tmp_path)

    assert "expctl.toml" in created
    assert (tmp_path / "expctl" / "templates" / "request.toml").is_file()
    assert (tmp_path / "expctl" / "requests" / ".gitkeep").is_file()
    assert load_config(tmp_path).max_total_nodes == 0
    assert load_config(tmp_path).list_limit == 20
    assert init_repo(tmp_path) == []


def test_init_honors_an_existing_custom_root(tmp_path: Path) -> None:
    config_text = EXAMPLE_CONFIG.replace(
        "[scheduler]", '[paths]\nroot = "handoffs/cluster"\n\n[scheduler]'
    )
    (tmp_path / "expctl.toml").write_text(config_text, encoding="utf-8")

    created = init_repo(tmp_path)

    assert "handoffs/cluster/templates/request.toml" in created
    assert (tmp_path / "handoffs" / "cluster" / "requests" / ".gitkeep").is_file()
    assert not (tmp_path / "expctl").exists()


def test_root_directory_is_configurable(tmp_path: Path) -> None:
    config_text = EXAMPLE_CONFIG.replace(
        "[scheduler]", '[paths]\nroot = "handoffs/cluster"\n\n[scheduler]'
    )
    (tmp_path / "expctl.toml").write_text(config_text, encoding="utf-8")
    requests = tmp_path / "handoffs" / "cluster" / "requests"
    requests.mkdir(parents=True)
    (requests / f"{EXAMPLE_ID}.toml").write_text(EXAMPLE_REQUEST, encoding="utf-8")

    config = load_config(tmp_path)
    _, path = load_request(tmp_path, config, EXAMPLE_ID, check_git=False)

    assert config.root == "handoffs/cluster"
    assert path.parent == requests


def test_list_limit_is_optional_and_must_be_positive(tmp_path: Path) -> None:
    (tmp_path / "expctl.toml").write_text(EXAMPLE_CONFIG, encoding="utf-8")
    assert load_config(tmp_path).list_limit is None

    configured = EXAMPLE_CONFIG.replace(
        "[scheduler]", "[display]\nlist_limit = 12\n\n[scheduler]"
    )
    (tmp_path / "expctl.toml").write_text(configured, encoding="utf-8")
    assert load_config(tmp_path).list_limit == 12

    for invalid in ("0", "-1", "true", '"12"'):
        invalid_config = EXAMPLE_CONFIG.replace(
            "[scheduler]", f"[display]\nlist_limit = {invalid}\n\n[scheduler]"
        )
        (tmp_path / "expctl.toml").write_text(invalid_config, encoding="utf-8")
        with pytest.raises(ExpctlError, match="display.list_limit"):
            load_config(tmp_path)


def test_root_directory_must_stay_inside_the_repo(tmp_path: Path) -> None:
    config_text = EXAMPLE_CONFIG.replace(
        "[scheduler]", '[paths]\nroot = "../elsewhere"\n\n[scheduler]'
    )
    (tmp_path / "expctl.toml").write_text(config_text, encoding="utf-8")

    with pytest.raises(ExpctlError, match="paths.root"):
        load_config(tmp_path)


def test_root_directory_symlink_must_stay_inside_the_repo(tmp_path: Path) -> None:
    if not _symlinks_allowed(tmp_path):
        pytest.skip("symlinks not permitted here")
    repo = tmp_path / "repo"
    outside = tmp_path / "outside"
    repo.mkdir()
    outside.mkdir()
    (repo / "outside-link").symlink_to(outside, target_is_directory=True)
    config_text = EXAMPLE_CONFIG.replace(
        "[scheduler]", '[paths]\nroot = "outside-link"\n\n[scheduler]'
    )
    (repo / "expctl.toml").write_text(config_text, encoding="utf-8")

    with pytest.raises(ExpctlError, match="resolve inside the repository"):
        load_config(repo)


def test_wellformed_request_validates(tmp_path: Path) -> None:
    repo = _example_repo(tmp_path)
    config = load_config(repo)

    request, path = load_request(repo, config, EXAMPLE_ID, check_git=False)

    assert path.name == f"{EXAMPLE_ID}.toml"
    assert len(request["code"]["commit"]) == 40


def test_local_request_validates_an_executable_pinned_script(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _example_repo(tmp_path, LOCAL_REQUEST)
    config = load_config(repo)

    def fake_run(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        if args[1] == "cat-file":
            return subprocess.CompletedProcess(args, 0, "", "")
        if args[1] == "show":
            return subprocess.CompletedProcess(args, 0, "#!/usr/bin/env python3\n", "")
        assert args[1] == "ls-tree"
        return subprocess.CompletedProcess(
            args,
            0,
            f"100755 blob {'a' * 40}\tscripts/run-local.py\n",
            "",
        )

    monkeypatch.setattr(core, "_run", fake_run)

    request, _ = load_request(repo, config, EXAMPLE_ID)

    assert core._execution_table(request)[0] == "local"
    assert core.build_local_command(request) == [
        "./scripts/run-local.py",
        "--one",
        "two words",
    ]
    assert core._validate_request_git(repo, config, request) is None


def test_request_requires_one_backend_and_local_logs_are_exact(tmp_path: Path) -> None:
    both = LOCAL_REQUEST.replace(
        "[local]",
        '[slurm]\nscript = "scripts/sweep.slurm"\nmax_concurrent_nodes = 1\n\n[local]',
    )
    repo = _example_repo(tmp_path, both)
    config = load_config(repo)
    with pytest.raises(ExpctlError, match="exactly one"):
        load_request(repo, config, EXAMPLE_ID, check_git=False)

    wildcard = LOCAL_REQUEST.replace(
        'log_glob = "logs/local-{job_id}.out"',
        'log_glob = "logs/local-{job_id}-*.out"',
    )
    request_file = repo / "expctl" / "requests" / f"{EXAMPLE_ID}.toml"
    request_file.write_text(wildcard, encoding="utf-8")
    with pytest.raises(ExpctlError, match="exact log file"):
        load_request(repo, config, EXAMPLE_ID, check_git=False)


def test_new_request_fills_git_identity_and_never_overwrites(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _example_repo(tmp_path)
    config = load_config(repo)
    template = repo / "expctl" / "templates" / "request.toml"
    template.parent.mkdir()
    template.write_text(core.STARTER_TEMPLATE, encoding="utf-8")
    monkeypatch.setattr(core, "_git_text", lambda *args: "a" * 40 + "\n")

    def fake_run(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        output = "" if args[:2] == ["git", "status"] else "feature/better-ux\n"
        return subprocess.CompletedProcess(args, 0, output, "")

    monkeypatch.setattr(
        core,
        "_run",
        fake_run,
    )
    experiment_id = "20260102-new-sweep"

    result = core.new_request(repo, config, experiment_id)
    request, path = load_request(
        repo, config, experiment_id, check_git=False, check_placeholders=False
    )

    assert result["path"] == f"expctl/requests/{experiment_id}.toml"
    assert result["branch"] == "feature/better-ux"
    assert request["id"] == experiment_id
    assert request["code"]["commit"] == "a" * 40
    assert request["code"]["branch"] == "feature/better-ux"
    assert request["code"]["worktree"].endswith("-new-sweep")
    rendered = core._render_new_result(result)
    assert "FIELD" in rendered and "VALUE" in rendered
    assert "NEXT STEPS" in rendered and "ACTION" in rendered
    original = path.read_text(encoding="utf-8")
    with pytest.raises(ExpctlError, match="already exists"):
        core.new_request(repo, config, experiment_id)
    assert path.read_text(encoding="utf-8") == original


def test_new_request_uses_real_git_metadata(tmp_path: Path) -> None:
    repo = tmp_path / "My Project"
    repo.mkdir()
    _git(repo, "init", "-q")
    init_repo(repo)
    (repo / "train.py").write_text("version = 1\n", encoding="utf-8")
    _git(repo, "add", ".")
    _git(
        repo,
        "-c",
        "user.name=expctl tests",
        "-c",
        "user.email=expctl@example.invalid",
        "commit",
        "-q",
        "-m",
        "initial",
    )
    config = load_config(repo)
    experiment_id = "20260103-real-git"
    (repo / "train.py").write_text("version = 2\n", encoding="utf-8")

    with pytest.raises(ExpctlError, match="not included in the pinned HEAD"):
        core.new_request(repo, config, experiment_id)
    result = core.new_request(repo, config, experiment_id, allow_dirty=True)

    assert result["commit"] == _git(repo, "rev-parse", "HEAD")
    assert result["branch"] == _git(repo, "branch", "--show-current")
    assert result["worktree"] == "my-project-real-git"
    assert any("train.py" in change for change in result["uncommitted_changes"])
    request, _ = load_request(
        repo, config, experiment_id, check_git=False, check_placeholders=False
    )
    assert request["code"]["commit"] == result["commit"]


def test_request_cannot_exceed_the_node_ceiling(tmp_path: Path) -> None:
    text = EXAMPLE_REQUEST.replace(
        "max_concurrent_nodes = 4", "max_concurrent_nodes = 5"
    )
    repo = _example_repo(tmp_path, text)
    config = load_config(repo)

    with pytest.raises(ExpctlError, match="max_total_nodes"):
        load_request(repo, config, EXAMPLE_ID, check_git=False)


def test_plain_names_reject_dot_segments(tmp_path: Path) -> None:
    text = EXAMPLE_REQUEST.replace('worktree = "myproject-example"', 'worktree = ".."')
    repo = _example_repo(tmp_path, text)
    config = load_config(repo)

    with pytest.raises(ExpctlError, match="plain directory name"):
        load_request(repo, config, EXAMPLE_ID, check_git=False)

    (repo / "expctl.toml").write_text(
        EXAMPLE_CONFIG.replace(
            'shared_dirs = [".venv", "data", "runs", "logs"]',
            'shared_dirs = [".."]',
        ).replace('create_missing = ["runs", "logs"]', "create_missing = []"),
        encoding="utf-8",
    )
    with pytest.raises(ExpctlError, match="plain directory name"):
        load_config(repo)


def test_log_glob_rejects_unknown_placeholders_cleanly(tmp_path: Path) -> None:
    request_text = EXAMPLE_REQUEST.replace(
        'log_glob = "logs/example-{job_id}_*.out"',
        'log_glob = "logs/{job_id}-{task}.out"',
    )
    repo = _example_repo(tmp_path, request_text)
    config = load_config(repo)

    with pytest.raises(ExpctlError, match="supports only the plain .*job_id"):
        load_request(repo, config, EXAMPLE_ID, check_git=False)


def test_metric_names_must_match_the_extractor_grammar(tmp_path: Path) -> None:
    request = EXAMPLE_REQUEST.replace(
        'metrics = ["gen_ppl"]', 'metrics = ["loss total"]'
    )
    repo = _example_repo(tmp_path, request)
    config = load_config(repo)

    with pytest.raises(ExpctlError, match="outputs.metrics entries must match"):
        load_request(repo, config, EXAMPLE_ID, check_git=False)


def test_script_node_envelope_includes_array_throttle() -> None:
    lines = ["#!/bin/bash", "#SBATCH --nodes=2", "#SBATCH --array=0-9%3"]

    assert _script_max_concurrent_nodes(lines, "job.slurm") == 6
    assert _script_max_concurrent_nodes(["#SBATCH -N 1-4"], "job.slurm") == 4
    with pytest.raises(ExpctlError, match="explicit integer"):
        _script_max_concurrent_nodes(["#!/bin/bash"], "job.slurm")
    with pytest.raises(ExpctlError, match="numeric indexes"):
        _script_max_concurrent_nodes(
            ["#SBATCH --nodes=1", "#SBATCH --array=$TASKS"], "job.slurm"
        )
    with pytest.raises(ExpctlError, match="heterogeneous"):
        _script_max_concurrent_nodes(
            ["#SBATCH --nodes=1", "#SBATCH hetjob", "#SBATCH --nodes=2"],
            "job.slurm",
        )
    assert (
        _script_max_concurrent_nodes(
            ["#SBATCH --nodes=2", "echo start", "#SBATCH --nodes=1"], "job.slurm"
        )
        == 2
    )


def test_required_scheduler_line_after_code_is_not_accepted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _example_repo(tmp_path)
    config = load_config(repo)
    monkeypatch.setattr(
        core,
        "_run",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )
    monkeypatch.setattr(
        core,
        "_git_text",
        lambda *args: (
            "#!/bin/bash\n#SBATCH --nodes=1\necho start\n#SBATCH -p example-partition\n"
        ),
    )

    with pytest.raises(ExpctlError, match="missing the required line"):
        load_request(repo, config, EXAMPLE_ID)


def test_ambient_sbatch_options_are_removed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SBATCH_NODES", "99")
    monkeypatch.setenv("EXPCTL_TEST_VALUE", "kept")

    environment, removed = core._sbatch_environment()

    assert removed == ["SBATCH_NODES"]
    assert "SBATCH_NODES" not in environment
    assert environment["EXPCTL_TEST_VALUE"] == "kept"


def test_sbatch_command_is_argument_safe(tmp_path: Path) -> None:
    repo = _example_repo(tmp_path)
    config = load_config(repo)
    request, _ = load_request(repo, config, EXAMPLE_ID, check_git=False)

    assert build_sbatch_command(request) == [
        "sbatch",
        "--parsable",
        "--export=ALL,NUM_SAMPLES=256",
        "scripts/sweep.slurm",
    ]
    assert build_sbatch_command(request, ("-p", "example-partition")) == [
        "sbatch",
        "--parsable",
        "--export=ALL,NUM_SAMPLES=256",
        "-p",
        "example-partition",
        "scripts/sweep.slurm",
    ]


def test_extract_metrics_keeps_last_value() -> None:
    text = "gen_ppl: 40\nignored: 1\ngen_ppl = 24.65\ngen_ppl_full: 30.5\n"

    assert extract_metrics(text, ["gen_ppl", "gen_ppl_full"]) == {
        "gen_ppl": 24.65,
        "gen_ppl_full": 30.5,
    }


def test_extract_metrics_reads_column_aligned_lines() -> None:
    # Plain metric dumps with no colon, just column alignment, still parse.
    text = (
        "  gen_ppl                        24.6500\n"
        "  gap   3 blocks          0.0400\n"
        "  different sample       0.0100   <- unrelated-document floor\n"
        "  gen_blockovlp_ratio_d32: 1.02\n"
    )

    assert extract_metrics(text, ["gen_ppl", "gen_blockovlp_ratio_d32"]) == {
        "gen_ppl": 24.65,
        "gen_blockovlp_ratio_d32": 1.02,
    }


def test_list_table_aligns_cjk_and_truncates_to_terminal_width() -> None:
    rows = [
        {
            "id": "20260828-decoder-cross-repo",
            "status": "requested",
            "title": "跨仓库解码器实验：这个标题需要被安全截断",
        },
        {
            "id": "20260828-decoder-metric",
            "status": "submission_unknown",
            "title": "line one\nline two\x1b[31m",
        },
    ]

    table = core._render_list_table(rows, terminal_width=80, color=False)
    lines = table.splitlines()

    assert lines[0].startswith("EXPERIMENT ID")
    assert len(lines) == 4
    assert all(core._display_width(line) <= 80 for line in lines)
    assert "…" in table
    assert "line one line two?[31m" in table
    assert core._display_width("实验A") == 5
    narrow = core._render_list_table(rows, terminal_width=30, color=False)
    assert all(core._display_width(line) <= 30 for line in narrow.splitlines())


def test_list_table_colors_status_only_when_requested() -> None:
    rows = [{"id": EXAMPLE_ID, "status": "collected", "title": "done"}]

    plain = core._render_list_table(rows, terminal_width=80, color=False)
    colored = core._render_list_table(rows, terminal_width=80, color=True)

    assert "\x1b[" not in plain
    assert "\x1b[32m" in colored
    assert colored.endswith("done")


def test_summary_table_aligns_cjk_and_sanitizes_control_characters() -> None:
    rendered = core._render_summary(
        "SUMMARY",
        [("实验", "中文 value"), ("Detail", "line one\nline two\x1b[31m")],
        next_steps=["expctl status 20260829-example --watch"],
    )

    lines = rendered.splitlines()
    assert lines[0] == "SUMMARY"
    assert lines[1].startswith("FIELD   VALUE")
    assert lines[2][6:8] == "  "
    assert "实验    中文 value" in rendered
    assert "line one line two?[31m" in rendered
    assert "NEXT STEPS\nSTEP  ACTION" in rendered


def test_list_tsv_is_single_line_per_request_and_parser_exposes_formats() -> None:
    rows = [{"id": EXAMPLE_ID, "status": "requested", "title": "a\tb\nc"}]

    assert core._render_list_tsv(rows) == f"{EXAMPLE_ID}\trequested\ta b c"
    assert core._parser().parse_args(["list", "--table"]).list_format == "table"
    assert core._parser().parse_args(["list", "--tsv"]).list_format == "tsv"
    assert core._parser().parse_args(["list", "--json"]).list_format == "json"
    collect_args = core._parser().parse_args(
        ["collect", EXAMPLE_ID, "--worktree-root", "worktrees"]
    )
    assert collect_args.worktree_root == Path("worktrees")
    assert core._parser().parse_args(["new", EXAMPLE_ID]).json is False
    assert (
        core._parser().parse_args(["new", EXAMPLE_ID, "--allow-dirty"]).allow_dirty
        is True
    )
    assert core._parser().parse_args(["status", EXAMPLE_ID, "--json"]).json is True
    list_args = core._parser().parse_args(
        [
            "list",
            "--status",
            "running,failed",
            "--status",
            "requested",
            "--sort",
            "oldest",
            "--limit",
            "2",
        ]
    )
    assert list_args.status == [("running", "failed"), ("requested",)]
    assert list_args.sort == "oldest" and list_args.limit == 2
    assert list_args.all is False
    assert core._parser().parse_args(["list", "--all"]).all is True


def test_list_filters_sorts_and_limits_after_live_status_resolution() -> None:
    rows = [
        {"id": "20260101-old", "status": "requested", "title": "old"},
        {"id": "20260103-new", "status": "RUNNING", "title": "new"},
        {"id": "20260102-middle", "status": "FAILED", "title": "middle"},
    ]

    selected = core._select_list_rows(
        rows,
        statuses={"running", "failed"},
        sort_order="newest",
        limit=1,
    )

    assert selected == [rows[1]]


def test_list_caches_git_validation_for_shared_commits_and_scripts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _example_repo(tmp_path)
    second_id = "20260102-second"
    second = EXAMPLE_REQUEST.replace(EXAMPLE_ID, second_id)
    (repo / "expctl" / "requests" / f"{second_id}.toml").write_text(
        second, encoding="utf-8"
    )
    config = load_config(repo)
    calls = {"commit": 0, "script": 0}

    def fake_run(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        if args[1] == "cat-file":
            calls["commit"] += 1
            return subprocess.CompletedProcess(args, 0, "", "")
        assert args[1] == "show"
        calls["script"] += 1
        return subprocess.CompletedProcess(
            args, 0, "#SBATCH -p example-partition\n#SBATCH --nodes=1\n", ""
        )

    monkeypatch.setattr(core, "_run", fake_run)

    rows = core.list_requests(repo, config)

    assert [row["id"] for row in rows] == [EXAMPLE_ID, second_id]
    assert calls == {"commit": 1, "script": 1}


def test_doctor_reports_repository_and_cluster_readiness(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _example_repo(tmp_path)
    template = repo / "expctl" / "templates" / "request.toml"
    template.parent.mkdir()
    template.write_text(core.STARTER_TEMPLATE, encoding="utf-8")
    (repo / "expctl" / "results").mkdir()
    monkeypatch.setattr(core.shutil, "which", lambda command: f"/bin/{command}")
    monkeypatch.setattr(core, "fcntl", object())
    monkeypatch.setattr(core, "_process_start_token", lambda pid: "42")

    result = core.doctor_repo(repo)

    assert result["repository_ready"] is True
    assert result["cluster_ready"] is True
    assert all(check["ok"] for check in result["checks"])
    rendered = core._render_doctor_result(result)
    assert "EXPCTL DOCTOR" in rendered
    assert "AREA" in rendered and "STATUS" in rendered and "SLURM" in rendered
    assert "Local" in rendered
    assert "READY" in rendered
    assert core._parser().parse_args(["doctor", "--json"]).json is True
    assert core._parser().parse_args(["doctor", "--backend", "local"]).backend == (
        "local"
    )


def test_doctor_json_returns_nonzero_when_cluster_is_not_ready(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = {
        "repository": str(tmp_path),
        "repository_ready": True,
        "cluster_ready": False,
        "checks": [
            {
                "name": "sbatch",
                "ok": False,
                "detail": "not found on PATH",
                "scope": "cluster",
                "required": True,
            }
        ],
    }
    monkeypatch.setattr(core, "find_repo_root", lambda: tmp_path)
    monkeypatch.setattr(core, "doctor_repo", lambda repo: result)

    # An author's machine has no SLURM: repository readiness alone is success.
    assert core.main(["doctor", "--json"]) == 0
    assert json.loads(capsys.readouterr().out) == result
    assert core.main(["doctor", "--cluster", "--json"]) == 1
    assert json.loads(capsys.readouterr().out) == result
    assert core.main(["doctor", "--backend", "local", "--json"]) == 1
    assert json.loads(capsys.readouterr().out) == result
    rendered = core._render_doctor_result(result)
    assert "informational" in rendered and "--backend local|slurm" in rendered
    assert "INFO" in rendered and "FAIL" not in rendered
    strict = core._render_doctor_result(result, require_cluster=True)
    assert "informational" not in strict and "cluster host" in strict
    assert "FAIL" in strict and "INFO" not in strict


def test_list_cli_keeps_tsv_for_pipes_and_supports_json(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    rows = [{"id": EXAMPLE_ID, "status": "requested", "title": "中文标题"}]
    config = Config(
        root="expctl",
        required_script_lines=(),
        max_total_nodes=0,
        shared_dirs=(),
        create_missing=(),
        worktree_root="..",
        list_limit=1,
    )
    monkeypatch.setattr(core, "find_repo_root", lambda: tmp_path)
    monkeypatch.setattr(core, "load_config", lambda repo: config)
    monkeypatch.setattr(core, "list_requests", lambda repo, loaded: rows)

    assert core.main(["list"]) == 0
    assert capsys.readouterr().out == f"{EXAMPLE_ID}\trequested\t中文标题\n"

    assert core.main(["list", "--json"]) == 0
    assert json.loads(capsys.readouterr().out) == rows

    filtered_rows = [
        *rows,
        {"id": "20260102-running", "status": "RUNNING", "title": "active"},
        {"id": "20260103-failed", "status": "FAILED", "title": "failed"},
    ]
    monkeypatch.setattr(core, "list_requests", lambda repo, loaded: filtered_rows)
    assert core.main(["list", "--json"]) == 0
    assert json.loads(capsys.readouterr().out) == [filtered_rows[2]]

    assert core.main(["list", "--json", "--all"]) == 0
    assert json.loads(capsys.readouterr().out) == list(reversed(filtered_rows))

    assert (
        core.main(["list", "--json", "--status", "running,failed", "--limit", "1"]) == 0
    )
    assert json.loads(capsys.readouterr().out) == [filtered_rows[2]]


def test_status_cli_uses_a_human_summary_on_tty_and_honors_json(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config = Config(
        root="expctl",
        required_script_lines=(),
        max_total_nodes=0,
        shared_dirs=(),
        create_missing=(),
        worktree_root="..",
    )
    payload = {
        "experiment_id": EXAMPLE_ID,
        "job_id": "123",
        "receipt_status": "submitted",
        "in_queue": False,
        "queue_counts": {},
        "queue": [],
        "accounting": {"state": "COMPLETED", "jobs": []},
    }
    monkeypatch.setattr(core, "find_repo_root", lambda: tmp_path)
    monkeypatch.setattr(core, "load_config", lambda repo: config)
    monkeypatch.setattr(core, "status_request", lambda *args: payload)
    monkeypatch.setattr(core.sys.stdout, "isatty", lambda: True)

    assert core.main(["status", EXAMPLE_ID]) == 0
    human = capsys.readouterr().out
    assert "EXPERIMENT STATUS" in human
    assert "FIELD" in human and "VALUE" in human
    assert "Status" in human and "COMPLETED" in human
    assert f"expctl collect {EXAMPLE_ID}" in human

    assert core.main(["status", EXAMPLE_ID, "--json"]) == 0
    assert json.loads(capsys.readouterr().out) == payload

    monkeypatch.setattr(core.sys.stdout, "isatty", lambda: False)
    assert core.main(["status", EXAMPLE_ID]) == 0
    assert json.loads(capsys.readouterr().out) == payload


def test_human_result_renderers_include_lifecycle_next_steps() -> None:
    preview = {
        "experiment_id": EXAMPLE_ID,
        "commit": ZERO_COMMIT,
        "worktree": "/worktrees/example",
        "command": ["sbatch", "scripts/sweep.slurm"],
        "declared_max_concurrent_nodes": 4,
        "verified_max_concurrent_nodes": 2,
    }
    submit_text = core._render_submit_result(
        preview,
        dry_run=True,
        worktree_root=Path("/worktrees"),
        skip_node_check=False,
    )
    collect_text = core._render_collect_result(
        {
            "logs": ["one.out"],
            "metrics_file": "expctl/results/example/metrics.json",
            "missing_metrics": [],
            "scheduler": {"state": "COMPLETED"},
        },
        experiment_id=EXAMPLE_ID,
    )
    rerun_text = core._render_rerun_result(
        {
            "experiment_id": f"{EXAMPLE_ID}-r2",
            "rerun_of": EXAMPLE_ID,
            "path": f"expctl/requests/{EXAMPLE_ID}-r2.toml",
            "commit": ZERO_COMMIT,
            "worktree": "myproject-example",
        }
    )

    assert "SUBMISSION PREVIEW" in submit_text
    assert f"expctl submit {EXAMPLE_ID}" in submit_text
    assert "RESULTS COLLECTED" in collect_text
    assert f"expctl report {EXAMPLE_ID}" in collect_text
    assert "RERUN REQUEST CREATED" in rerun_text
    assert f"expctl submit {EXAMPLE_ID}-r2" in rerun_text


def test_list_refreshes_submitted_receipts_without_mutating_them(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _example_repo(tmp_path)
    config = load_config(repo)
    receipt_dir = repo / "expctl" / "results" / EXAMPLE_ID
    receipt_dir.mkdir(parents=True)
    receipt = receipt_dir / "receipt.json"
    original = {"job_id": "123", "status": "submitted"}
    receipt.write_text(json.dumps(original), encoding="utf-8")
    request = tomllib.loads(EXAMPLE_REQUEST)
    request_path = repo / "expctl" / "requests" / f"{EXAMPLE_ID}.toml"
    monkeypatch.setattr(
        core, "load_request", lambda *args, **kwargs: (request, request_path)
    )
    monkeypatch.setattr(core, "_validate_request_git", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        core,
        "_live_scheduler_statuses",
        lambda *args: ({"123": "RUNNING"}, None),
    )

    assert core.list_requests(repo, config) == [
        {"id": EXAMPLE_ID, "status": "RUNNING", "title": "Example sweep"}
    ]
    assert json.loads(receipt.read_text(encoding="utf-8")) == original


def test_live_list_status_summarizes_queue_and_accounting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(core.shutil, "which", lambda command: f"/bin/{command}")
    calls: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        if args[0] == "squeue":
            assert args[args.index("-j") + 1] == "123,456"
            return subprocess.CompletedProcess(
                args,
                0,
                "123_1|RUNNING|None\n123_2|PENDING|Resources\n",
                "",
            )
        assert args[0] == "sacct"
        assert args[args.index("-j") + 1] == "456"
        return subprocess.CompletedProcess(
            args,
            0,
            "456|FAILED|1:0\n456.batch|FAILED|1:0\n",
            "",
        )

    monkeypatch.setattr(core, "_run", fake_run)

    statuses, warning = core._live_scheduler_statuses(tmp_path, ["123", "456"])

    assert statuses == {"123": "MIXED", "456": "FAILED"}
    assert warning is None
    assert [call[0] for call in calls] == ["squeue", "sacct"]


def test_live_list_status_falls_back_when_slurm_is_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(core.shutil, "which", lambda command: None)
    monkeypatch.setattr(
        core,
        "_queue_statuses",
        lambda *args: pytest.fail("squeue should not be called when it is unavailable"),
    )

    statuses, warning = core._live_scheduler_statuses(tmp_path, ["123", "456"])
    assert statuses == {}
    assert warning == "squeue not found; showing stored receipt states"
    assert core._slurm_state_name("CANCELLED by 456") == "CANCELLED"

    monkeypatch.setattr(core.shutil, "which", lambda command: f"/bin/{command}")
    monkeypatch.setattr(
        core,
        "_queue_statuses",
        lambda *args: (_ for _ in ()).throw(ExpctlError("controller unavailable")),
    )
    statuses, warning = core._live_scheduler_statuses(tmp_path, ["123", "456"])
    assert statuses == {}
    assert warning == "controller unavailable; showing stored receipt states"

    monkeypatch.setattr(core, "_queue_statuses", lambda *args: [])
    monkeypatch.setattr(core, "_scheduler_status_rows", lambda *args: [])
    statuses, warning = core._live_scheduler_statuses(tmp_path, ["123", "456"])
    assert statuses == {}
    assert warning == (
        "SLURM returned no state for 2 submitted job(s); showing stored receipt states"
    )


def test_list_warns_once_when_live_status_is_unavailable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo = _example_repo(tmp_path)
    config = load_config(repo)
    receipt_dir = repo / "expctl" / "results" / EXAMPLE_ID
    receipt_dir.mkdir(parents=True)
    (receipt_dir / "receipt.json").write_text(
        json.dumps({"job_id": "123", "status": "submitted"}), encoding="utf-8"
    )
    request = tomllib.loads(EXAMPLE_REQUEST)
    request_path = repo / "expctl" / "requests" / f"{EXAMPLE_ID}.toml"
    monkeypatch.setattr(
        core, "load_request", lambda *args, **kwargs: (request, request_path)
    )
    monkeypatch.setattr(core, "_validate_request_git", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        core,
        "_live_scheduler_statuses",
        lambda *args: ({}, "squeue not found; showing stored receipt states"),
    )
    monkeypatch.setattr(core, "find_repo_root", lambda: repo)
    monkeypatch.setattr(core, "load_config", lambda loaded: config)

    assert core.main(["list", "--json"]) == 0
    captured = capsys.readouterr()

    assert json.loads(captured.out)[0]["status"] == "submitted"
    assert captured.err == (
        "warning: squeue not found; showing stored receipt states\n"
    )


def test_invalid_id_is_rejected(tmp_path: Path) -> None:
    repo = _example_repo(tmp_path)
    config = load_config(repo)

    with pytest.raises(ExpctlError, match="experiment ID"):
        load_request(repo, config, "../unsafe")


def test_status_before_submission_is_a_clear_error(tmp_path: Path) -> None:
    repo = _example_repo(tmp_path)
    config = load_config(repo)

    with pytest.raises(ExpctlError, match="not submitted yet"):
        status_request(repo, config, EXAMPLE_ID)


def test_status_falls_back_to_sacct_and_watch_stops_at_terminal_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo = _example_repo(tmp_path)
    config = load_config(repo)
    _fake_receipt(repo, EXAMPLE_ID)
    monkeypatch.setattr(
        core,
        "_queue_status",
        lambda *args: (_ for _ in ()).throw(ExpctlError("controller unavailable")),
    )
    monkeypatch.setattr(
        core,
        "_scheduler_status",
        lambda *args: {
            "state": "COMPLETED",
            "jobs": [{"job_id": "123", "state": "COMPLETED", "exit_code": "0:0"}],
        },
    )

    result = status_request(repo, config, EXAMPLE_ID)

    assert result["source"] == "sacct"
    assert result["accounting"]["state"] == "COMPLETED"
    assert "used sacct" in result["detail"]
    assert core._status_is_terminal(result) is True

    running = {
        **result,
        "accounting": {"state": "RUNNING", "jobs": []},
        "detail": None,
    }
    responses = iter([running, result])
    monkeypatch.setattr(core, "find_repo_root", lambda: repo)
    monkeypatch.setattr(core, "load_config", lambda found: config)
    monkeypatch.setattr(core, "status_request", lambda *args: next(responses))
    sleeps: list[float] = []
    monkeypatch.setattr(core.time, "sleep", sleeps.append)
    monkeypatch.setattr(core.sys.stdout, "isatty", lambda: False)

    assert core.main(["status", EXAMPLE_ID, "--watch", "0.25"]) == 0
    snapshots = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [snapshot["accounting"]["state"] for snapshot in snapshots] == [
        "RUNNING",
        "COMPLETED",
    ]
    assert sleeps == [0.25]


def test_cancel_records_an_audited_request_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _example_repo(tmp_path)
    config = load_config(repo)
    receipt_path = _fake_receipt(repo, EXAMPLE_ID)
    calls: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(core, "_run", fake_run)
    monkeypatch.setattr(core.getpass, "getuser", lambda: "runner")
    monkeypatch.setattr(
        core,
        "_result_collection_guard",
        lambda *args: core.contextlib.nullcontext(),
    )

    preview = core.cancel_request(
        repo, config, EXAMPLE_ID, reason="no longer needed", dry_run=True
    )
    assert preview["command"] == ["scancel", "123"]
    assert calls == []

    result = core.cancel_request(repo, config, EXAMPLE_ID, reason="no longer needed")
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))

    assert calls == [["scancel", "123"]]
    assert result["receipt_status"] == "cancel_requested"
    assert receipt["status"] == "cancel_requested"
    assert receipt["cancellation"]["requested_by"] == "runner"
    assert receipt["cancellation"]["reason"] == "no longer needed"
    with pytest.raises(ExpctlError, match="already requested"):
        core.cancel_request(repo, config, EXAMPLE_ID)


def test_local_status_and_cancel_use_the_recorded_process_group(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _example_repo(tmp_path, LOCAL_REQUEST)
    config = load_config(repo)
    receipt_dir = repo / "expctl" / "results" / EXAMPLE_ID
    receipt_dir.mkdir(parents=True)
    receipt_path = receipt_dir / "receipt.json"
    receipt_path.write_text(
        json.dumps(
            {
                "backend": "local",
                "experiment_id": EXAMPLE_ID,
                "job_id": "local-abc123",
                "pid": 4321,
                "process_start_token": "99",
                "hostname": core.socket.gethostname(),
                "status": "submitted",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(core, "_local_process_running", lambda *args: True)

    running = status_request(repo, config, EXAMPLE_ID)

    assert running["backend"] == "local"
    assert running["pid"] == 4321
    assert core._status_state(running) == "RUNNING"
    assert core._status_is_terminal(running) is False

    monkeypatch.setattr(
        core,
        "_local_accounting",
        lambda *args: ({"state": "RUNNING", "jobs": []}, True),
    )
    monkeypatch.setattr(core, "_process_start_token", lambda pid: "99")
    signals: list[tuple[int, int]] = []
    monkeypatch.setattr(
        core.os,
        "killpg",
        lambda pid, sig: signals.append((pid, sig)),
        raising=False,
    )
    monkeypatch.setattr(
        core, "_result_collection_guard", lambda *args: core.contextlib.nullcontext()
    )

    preview = core.cancel_request(repo, config, EXAMPLE_ID, dry_run=True)
    assert preview["command"] == ["kill", "-TERM", "--", "-4321"]
    result = core.cancel_request(repo, config, EXAMPLE_ID, reason="superseded")

    assert signals == [(4321, core.signal.SIGTERM)]
    assert result["backend"] == "local"
    assert result["pid"] == 4321
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["status"] == "cancel_requested"


def test_local_operations_do_not_interpret_a_pid_from_another_host(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _example_repo(tmp_path, LOCAL_REQUEST)
    config = load_config(repo)
    receipt_dir = repo / "expctl" / "results" / EXAMPLE_ID
    receipt_dir.mkdir(parents=True)
    (receipt_dir / "receipt.json").write_text(
        json.dumps(
            {
                "backend": "local",
                "experiment_id": EXAMPLE_ID,
                "job_id": "local-remote",
                "pid": 4321,
                "process_start_token": "99",
                "hostname": "another-compute-node",
                "status": "submitted",
            }
        ),
        encoding="utf-8",
    )

    status = status_request(repo, config, EXAMPLE_ID)

    assert core._status_state(status) == "UNKNOWN"
    assert "another-compute-node" in status["detail"]
    # `status --watch` must not poll forever for a job this host cannot see.
    assert core._status_is_terminal(status) is True
    monkeypatch.setattr(
        core, "_result_collection_guard", lambda *args: core.contextlib.nullcontext()
    )
    with pytest.raises(ExpctlError, match="another-compute-node"):
        core.cancel_request(repo, config, EXAMPLE_ID)


def test_local_completion_status_maps_exit_codes_to_terminal_states(
    tmp_path: Path,
) -> None:
    status_path = tmp_path / "local-status.json"
    receipt = {"job_id": "local-abc", "pid": 123}
    status_path.write_text(
        json.dumps(
            {
                "returncode": 7,
                "started_at": "2026-08-29T00:00:00+00:00",
                "finished_at": "2026-08-29T00:00:01+00:00",
            }
        ),
        encoding="utf-8",
    )

    accounting, running = core._local_accounting(receipt, status_path)

    assert running is False
    assert accounting["state"] == "FAILED"
    assert accounting["jobs"][0]["exit_code"] == "7:0"


def _stub_submit_preflight(
    monkeypatch: pytest.MonkeyPatch, repo: Path, request: dict[str, object]
) -> None:
    request_file = repo / "expctl" / "requests" / f"{EXAMPLE_ID}.toml"
    monkeypatch.setattr(
        core, "load_request", lambda *args, **kwargs: (request, request_file)
    )
    monkeypatch.setattr(
        core,
        "_git_text",
        lambda *args: "#!/bin/bash\n#SBATCH --nodes=1\n",
    )
    monkeypatch.setattr(core, "_ensure_worktree", lambda *args, **kwargs: None)
    monkeypatch.setattr(core, "_check_worktree_clean", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        core, "_worktree_submission_guard", lambda *args: core.contextlib.nullcontext()
    )
    monkeypatch.setattr(
        core, "_ensure_runtime_links", lambda *args, **kwargs: {"logs": "linked"}
    )
    monkeypatch.setattr(core, "_check_requirements", lambda *args, **kwargs: None)


def test_submit_writes_confirmed_receipt_and_refuses_a_second_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _example_repo(tmp_path)
    config = load_config(repo)
    request = tomllib.loads(EXAMPLE_REQUEST)
    _stub_submit_preflight(monkeypatch, repo, request)
    monkeypatch.setattr(
        core,
        "_run",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "123;cluster\n", ""),
    )
    worktree_root = tmp_path.parent / f"{tmp_path.name}-worktrees"

    receipt = submit_request(
        repo,
        config,
        EXAMPLE_ID,
        dry_run=False,
        skip_node_check=True,
        worktree_root=worktree_root,
    )

    assert receipt["status"] == "submitted"
    assert receipt["job_id"] == "123"
    assert receipt["verified_max_concurrent_nodes"] == 1
    with pytest.raises(ExpctlError, match="receipt already exists"):
        submit_request(
            repo,
            config,
            EXAMPLE_ID,
            dry_run=False,
            skip_node_check=True,
            worktree_root=worktree_root,
        )


def test_local_submit_records_process_identity_without_calling_slurm(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _example_repo(tmp_path, LOCAL_REQUEST)
    config = load_config(repo)
    request = tomllib.loads(LOCAL_REQUEST)
    _stub_submit_preflight(monkeypatch, repo, request)
    started: dict[str, object] = {}

    def fake_start(
        loaded_repo: Path,
        loaded_config: Config,
        experiment_id: str,
        loaded_request: dict[str, object],
        worktree: Path,
        *,
        job_id: str,
    ) -> dict[str, object]:
        started.update(
            {
                "repo": loaded_repo,
                "config": loaded_config,
                "experiment_id": experiment_id,
                "request": loaded_request,
                "worktree": worktree,
                "job_id": job_id,
            }
        )
        return {
            "job_id": job_id,
            "pid": 4321,
            "process_start_token": "99",
            "local_status_file": "expctl/results/example/local-status.json",
            "log": f"logs/local-{job_id}.out",
        }

    monkeypatch.setattr(core, "_start_local_process", fake_start)
    monkeypatch.setattr(
        core,
        "_run",
        lambda *args, **kwargs: pytest.fail("local submission called SLURM"),
    )
    worktree_root = tmp_path.parent / f"{tmp_path.name}-local-worktrees"

    receipt = submit_request(
        repo,
        config,
        EXAMPLE_ID,
        dry_run=False,
        skip_node_check=False,
        worktree_root=worktree_root,
    )

    assert receipt["backend"] == "local"
    assert receipt["status"] == "submitted"
    assert receipt["job_id"].startswith("local-")
    assert receipt["pid"] == 4321
    assert receipt["command"] == [
        "./scripts/run-local.py",
        *request["local"]["args"],
    ]
    assert started["job_id"] == receipt["job_id"]


def test_preflight_failure_removes_pending_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _example_repo(tmp_path)
    config = load_config(repo)
    request = tomllib.loads(EXAMPLE_REQUEST)
    _stub_submit_preflight(monkeypatch, repo, request)
    monkeypatch.setattr(
        core,
        "_ensure_worktree",
        lambda *args, **kwargs: (_ for _ in ()).throw(ExpctlError("dirty")),
    )

    with pytest.raises(ExpctlError, match="dirty"):
        submit_request(
            repo,
            config,
            EXAMPLE_ID,
            dry_run=False,
            skip_node_check=True,
            worktree_root=tmp_path.parent / f"{tmp_path.name}-worktrees",
        )

    assert not (repo / "expctl" / "results" / EXAMPLE_ID / "receipt.json").exists()


def test_sbatch_failure_preserves_an_unknown_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _example_repo(tmp_path)
    config = load_config(repo)
    request = tomllib.loads(EXAMPLE_REQUEST)
    _stub_submit_preflight(monkeypatch, repo, request)
    monkeypatch.setattr(
        core,
        "_run",
        lambda *args, **kwargs: (_ for _ in ()).throw(ExpctlError("connection lost")),
    )

    with pytest.raises(ExpctlError, match="outcome is unknown"):
        submit_request(
            repo,
            config,
            EXAMPLE_ID,
            dry_run=False,
            skip_node_check=True,
            worktree_root=tmp_path.parent / f"{tmp_path.name}-worktrees",
        )

    receipt = json.loads(
        (repo / "expctl" / "results" / EXAMPLE_ID / "receipt.json").read_text(
            encoding="utf-8"
        )
    )
    assert receipt["status"] == "submission_unknown"
    assert "connection lost" in receipt["submission_error"]
    status = status_request(repo, config, EXAMPLE_ID)
    assert status["job_id"] is None
    assert status["receipt_status"] == "submission_unknown"
    with pytest.raises(ExpctlError, match="no confirmed job ID"):
        rerun_request(repo, config, EXAMPLE_ID, check_git=False)


def _fake_receipt(repo: Path, experiment_id: str) -> Path:
    directory = repo / "expctl" / "results" / experiment_id
    directory.mkdir(parents=True)
    receipt = directory / "receipt.json"
    receipt.write_text(
        json.dumps(
            {
                "job_id": "123",
                "status": "submitted",
                "submitted_at": "2026-08-28T00:26:08+00:00",
                "submitted_by": "peng",
            }
        ),
        encoding="utf-8",
    )
    return receipt


def test_status_wraps_a_corrupt_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _example_repo(tmp_path)
    config = load_config(repo)
    receipt = _fake_receipt(repo, EXAMPLE_ID)
    receipt.write_text("{not json", encoding="utf-8")

    with pytest.raises(ExpctlError, match="unreadable submission receipt"):
        status_request(repo, config, EXAMPLE_ID)


def _collectable_receipt(repo: Path, worktree: Path) -> Path:
    request = repo / "expctl" / "requests" / f"{EXAMPLE_ID}.toml"
    directory = repo / "expctl" / "results" / EXAMPLE_ID
    directory.mkdir(parents=True)
    receipt = directory / "receipt.json"
    receipt.write_text(
        json.dumps(
            {
                "job_id": "123",
                "status": "submitted",
                "request_sha256": core._request_hash(request),
                "worktree": str(worktree),
            }
        ),
        encoding="utf-8",
    )
    return receipt


def _collect_worktree(tmp_path: Path) -> tuple[Path, Path]:
    root = tmp_path.parent / f"{tmp_path.name}-worktrees"
    return root, root / "myproject-example"


def _disable_collection_lock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        core,
        "_result_collection_guard",
        lambda *args: core.contextlib.nullcontext(),
    )


def test_logs_tail_reads_the_verified_submission_worktree(tmp_path: Path) -> None:
    repo = _example_repo(tmp_path)
    config = load_config(repo)
    worktree_root, worktree = _collect_worktree(tmp_path)
    log_dir = worktree / "logs"
    log_dir.mkdir(parents=True)
    (log_dir / "example-123_0.out").write_text(
        "first\nsecond\nthird\n", encoding="utf-8"
    )
    _collectable_receipt(repo, worktree)

    result = core.logs_request(
        repo,
        config,
        EXAMPLE_ID,
        tail=2,
        worktree_root=worktree_root,
    )

    assert result["pattern"] == "logs/example-123_*.out"
    assert result["logs"] == [
        {
            "path": "logs/example-123_0.out",
            "content": "second\nthird\n",
        }
    ]
    assert core._render_logs(result) == "second\nthird\n"

    collected_dir = repo / "expctl" / "results" / EXAMPLE_ID / "logs"
    collected_dir.mkdir()
    (collected_dir / "saved.out").write_text("saved\n", encoding="utf-8")
    receipt_path = repo / "expctl" / "results" / EXAMPLE_ID / "receipt.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["status"] = "collected"
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")

    collected = core.logs_request(repo, config, EXAMPLE_ID, tail=10)
    assert collected["collected"] is True
    assert collected["logs"] == [{"path": "saved.out", "content": "saved\n"}]


def test_parser_exposes_cancel_logs_clean_and_status_watch() -> None:
    parser = core._parser()

    cancel = parser.parse_args(
        ["cancel", EXAMPLE_ID, "--reason", "obsolete", "--dry-run"]
    )
    logs = parser.parse_args(["logs", EXAMPLE_ID, "--tail", "25", "--follow"])
    clean = parser.parse_args(["clean", EXAMPLE_ID, "--dry-run"])
    status = parser.parse_args(["status", EXAMPLE_ID, "--watch"])

    assert cancel.reason == "obsolete" and cancel.dry_run is True
    assert logs.tail == 25 and logs.follow is True
    assert clean.dry_run is True
    assert status.watch == 5.0


def test_log_cursor_never_skips_appends_and_detects_truncation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "job.out"
    initial = b"first\nsecond\n"
    path.write_bytes(initial)

    content, cursor, reset = core._read_log_update(path, None, tail=1)
    assert content == "second\n"
    assert reset is True
    assert cursor[2] == len(initial)

    real_fstat = core.os.fstat
    appended = False

    def fstat_then_append(file_descriptor: int) -> object:
        nonlocal appended
        stat = real_fstat(file_descriptor)
        if not appended:
            with path.open("ab") as handle:
                handle.write(b"third\n")
            appended = True
        return stat

    monkeypatch.setattr(core.os, "fstat", fstat_then_append)
    content, unchanged_cursor, reset = core._read_log_update(path, cursor, tail=1)
    assert content == ""
    assert unchanged_cursor == cursor
    assert reset is False
    assert cursor[2] < path.stat().st_size

    monkeypatch.setattr(core.os, "fstat", real_fstat)
    content, cursor, reset = core._read_log_update(path, cursor, tail=1)
    assert content == "third\n"
    assert reset is False
    assert cursor[2] == path.stat().st_size

    path.write_bytes(b"new\n")
    content, cursor, reset = core._read_log_update(path, cursor, tail=1)
    assert content == "new\n"
    assert reset is True
    assert cursor[2] == path.stat().st_size


def test_follow_logs_stops_when_a_terminal_job_produced_no_logs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _example_repo(tmp_path)
    config = load_config(repo)
    worktree_root, worktree = _collect_worktree(tmp_path)
    worktree.mkdir(parents=True)
    _collectable_receipt(repo, worktree)
    monkeypatch.setattr(
        core,
        "status_request",
        lambda *args: {
            "experiment_id": EXAMPLE_ID,
            "job_id": "123",
            "receipt_status": "submitted",
            "in_queue": False,
            "accounting": {"state": "FAILED", "jobs": []},
        },
    )

    with pytest.raises(ExpctlError, match="reached FAILED"):
        core._follow_logs(
            repo,
            config,
            EXAMPLE_ID,
            tail=100,
            worktree_root=worktree_root,
            poll_interval=0.01,
        )


def test_clean_only_removes_a_collected_verified_worktree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    (repo / "tracked.py").write_text("value = 1\n", encoding="utf-8")
    _git(repo, "add", "tracked.py")
    _git(
        repo,
        "-c",
        "user.name=expctl tests",
        "-c",
        "user.email=expctl@example.invalid",
        "commit",
        "-q",
        "-m",
        "initial",
    )
    commit = _git(repo, "rev-parse", "HEAD")
    request_text = EXAMPLE_REQUEST.replace(ZERO_COMMIT, commit)
    _example_repo(repo, request_text)
    config = load_config(repo)
    worktree_root = tmp_path / "worktrees"
    worktree = worktree_root / "myproject-example"
    _ensure_worktree(repo, worktree, commit)
    receipt_path = _collectable_receipt(repo, worktree)
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    monkeypatch.setattr(
        core,
        "_result_collection_guard",
        lambda *args: core.contextlib.nullcontext(),
    )

    with pytest.raises(ExpctlError, match="before results are collected"):
        core.clean_request(repo, config, EXAMPLE_ID, worktree_root=worktree_root)

    receipt["status"] = "collected"
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    monkeypatch.setattr(
        core,
        "_worktree_submission_guard",
        lambda *args: core.contextlib.nullcontext(),
    )
    other_receipt = repo / "expctl" / "results" / "20260102-rerun" / "receipt.json"
    other_receipt.parent.mkdir()
    other_receipt.write_text(
        json.dumps({"status": "submitted", "worktree": str(worktree)}),
        encoding="utf-8",
    )
    with pytest.raises(ExpctlError, match="still claimed"):
        core.clean_request(
            repo,
            config,
            EXAMPLE_ID,
            dry_run=True,
            worktree_root=worktree_root,
        )
    other_receipt.unlink()

    preview = core.clean_request(
        repo,
        config,
        EXAMPLE_ID,
        dry_run=True,
        worktree_root=worktree_root,
    )
    assert preview["worktree_exists"] is True
    assert preview["removed"] is False
    assert worktree.is_dir()

    result = core.clean_request(repo, config, EXAMPLE_ID, worktree_root=worktree_root)
    saved = json.loads(receipt_path.read_text(encoding="utf-8"))

    assert result["removed"] is True
    assert not worktree.exists()
    assert saved["cleanup"]["worktree"] == str(worktree.resolve())
    assert (
        core.clean_request(repo, config, EXAMPLE_ID, worktree_root=worktree_root)[
            "removed"
        ]
        is False
    )


def test_an_active_job_blocks_reuse_of_its_worktree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _example_repo(tmp_path)
    config = load_config(repo)
    worktree = tmp_path.parent / f"{tmp_path.name}-worktree"
    receipt = _collectable_receipt(repo, worktree)
    payload = json.loads(receipt.read_text(encoding="utf-8"))
    payload["experiment_id"] = EXAMPLE_ID
    receipt.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(
        core,
        "_queue_status",
        lambda *args: [{"job": "123", "state": "RUNNING", "reason": "None"}],
    )

    with pytest.raises(ExpctlError, match="still used"):
        core._check_worktree_available(repo, config, worktree, "20260102-another")


def test_unreadable_receipts_conservatively_block_worktree_operations(
    tmp_path: Path,
) -> None:
    repo = _example_repo(tmp_path)
    config = load_config(repo)
    worktree = tmp_path.parent / f"{tmp_path.name}-worktree"
    corrupt = repo / "expctl" / "results" / "20260102-corrupt" / "receipt.json"
    corrupt.parent.mkdir(parents=True)
    corrupt.write_text("{not json", encoding="utf-8")

    with pytest.raises(ExpctlError, match="receipt is unreadable"):
        core._check_worktree_available(repo, config, worktree, "20260103-another")
    with pytest.raises(ExpctlError, match="receipt is unreadable"):
        core._other_uncollected_worktree_claims(repo, config, worktree, EXAMPLE_ID)

    corrupt.write_text(
        json.dumps({"status": "submitted", "job_id": "999"}), encoding="utf-8"
    )
    with pytest.raises(ExpctlError, match="no valid absolute worktree"):
        core._check_worktree_available(repo, config, worktree, "20260103-another")
    with pytest.raises(ExpctlError, match="no valid absolute worktree"):
        core._other_uncollected_worktree_claims(repo, config, worktree, EXAMPLE_ID)


def test_collect_refuses_a_job_that_is_still_queued(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _example_repo(tmp_path)
    config = load_config(repo)
    worktree_root, worktree = _collect_worktree(tmp_path)
    worktree.mkdir(parents=True)
    _collectable_receipt(repo, worktree)
    _disable_collection_lock(monkeypatch)
    monkeypatch.setattr(
        core,
        "_queue_status",
        lambda *args: [{"job": "123", "state": "RUNNING", "reason": "None"}],
    )

    with pytest.raises(ExpctlError, match="still in the queue"):
        core.collect_request(repo, config, EXAMPLE_ID, worktree_root=worktree_root)


def test_collect_rejects_colliding_log_basenames(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request_text = EXAMPLE_REQUEST.replace(
        'log_glob = "logs/example-{job_id}_*.out"',
        'log_glob = "logs/**/job-{job_id}.out"',
    )
    repo = _example_repo(tmp_path, request_text)
    config = load_config(repo)
    worktree_root, worktree = _collect_worktree(tmp_path)
    for subdir in ("first", "second"):
        directory = worktree / "logs" / subdir
        directory.mkdir(parents=True)
        (directory / "job-123.out").write_text("gen_ppl: 2\n", encoding="utf-8")
    _collectable_receipt(repo, worktree)
    _disable_collection_lock(monkeypatch)
    monkeypatch.setattr(core, "_queue_status", lambda *args: [])

    with pytest.raises(ExpctlError, match="colliding basenames"):
        core.collect_request(repo, config, EXAMPLE_ID, worktree_root=worktree_root)


def test_collect_records_missing_metrics_from_the_copied_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _example_repo(tmp_path)
    config = load_config(repo)
    worktree_root, worktree = _collect_worktree(tmp_path)
    logs = worktree / "logs"
    logs.mkdir(parents=True)
    (logs / "example-123_0.out").write_text("loss: 1.5\n", encoding="utf-8")
    receipt_path = _collectable_receipt(repo, worktree)
    _disable_collection_lock(monkeypatch)
    monkeypatch.setattr(core, "_queue_status", lambda *args: [])
    monkeypatch.setattr(
        core, "_scheduler_status", lambda *args: {"state": "FAILED", "jobs": []}
    )

    collection = core.collect_request(
        repo, config, EXAMPLE_ID, worktree_root=worktree_root
    )

    assert collection["missing_metrics"] == ["gen_ppl"]
    assert collection["scheduler"]["state"] == "FAILED"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["status"] == "collected"


def test_collect_never_overwrites_collected_results(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _example_repo(tmp_path)
    config = load_config(repo)
    worktree_root, worktree = _collect_worktree(tmp_path)
    source = worktree / "logs" / "example-123_0.out"
    source.parent.mkdir(parents=True)
    source.write_text("gen_ppl: 1\n", encoding="utf-8")
    _collectable_receipt(repo, worktree)
    _disable_collection_lock(monkeypatch)
    monkeypatch.setattr(core, "_queue_status", lambda *args: [])
    monkeypatch.setattr(
        core, "_scheduler_status", lambda *args: {"state": "COMPLETED", "jobs": []}
    )

    core.collect_request(repo, config, EXAMPLE_ID, worktree_root=worktree_root)
    collected_log = (
        repo / "expctl" / "results" / EXAMPLE_ID / "logs" / "example-123_0.out"
    )
    source.write_text("gen_ppl: 2\n", encoding="utf-8")

    with pytest.raises(ExpctlError, match="already been collected"):
        core.collect_request(repo, config, EXAMPLE_ID, worktree_root=worktree_root)
    assert collected_log.read_text(encoding="utf-8") == "gen_ppl: 1\n"


def test_collect_refuses_partial_existing_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _example_repo(tmp_path)
    config = load_config(repo)
    _, worktree = _collect_worktree(tmp_path)
    _collectable_receipt(repo, worktree)
    existing_log = repo / "expctl" / "results" / EXAMPLE_ID / "logs" / "existing.out"
    existing_log.parent.mkdir()
    existing_log.write_text("keep me\n", encoding="utf-8")
    _disable_collection_lock(monkeypatch)

    with pytest.raises(ExpctlError, match="artifacts already exist"):
        core.collect_request(repo, config, EXAMPLE_ID)
    assert existing_log.read_text(encoding="utf-8") == "keep me\n"


def test_collect_rolls_back_published_artifacts_if_receipt_update_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _example_repo(tmp_path)
    config = load_config(repo)
    worktree_root, worktree = _collect_worktree(tmp_path)
    source = worktree / "logs" / "example-123_0.out"
    source.parent.mkdir(parents=True)
    source.write_text("gen_ppl: 1\n", encoding="utf-8")
    receipt_path = _collectable_receipt(repo, worktree)
    _disable_collection_lock(monkeypatch)
    monkeypatch.setattr(core, "_queue_status", lambda *args: [])
    monkeypatch.setattr(
        core, "_scheduler_status", lambda *args: {"state": "COMPLETED", "jobs": []}
    )
    atomic_write_json = core._atomic_write_json

    def fail_receipt_update(path: Path, payload: dict[str, object]) -> None:
        if path == receipt_path:
            raise ExpctlError("forced receipt failure")
        atomic_write_json(path, payload)

    monkeypatch.setattr(core, "_atomic_write_json", fail_receipt_update)

    with pytest.raises(ExpctlError, match="forced receipt failure"):
        core.collect_request(repo, config, EXAMPLE_ID, worktree_root=worktree_root)

    destination = repo / "expctl" / "results" / EXAMPLE_ID
    assert not (destination / "logs").exists()
    assert not (destination / "metrics.json").exists()
    assert not list(destination.glob(".collect-*"))
    assert json.loads(receipt_path.read_text(encoding="utf-8"))["status"] == "submitted"


def test_collect_rejects_a_receipt_worktree_outside_the_expected_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _example_repo(tmp_path)
    config = load_config(repo)
    worktree_root, _ = _collect_worktree(tmp_path)
    recorded_worktree = tmp_path.parent / f"{tmp_path.name}-untrusted-worktree"
    recorded_worktree.mkdir()
    _collectable_receipt(repo, recorded_worktree)
    _disable_collection_lock(monkeypatch)
    monkeypatch.setattr(core, "_queue_status", lambda *args: [])

    with pytest.raises(ExpctlError, match="does not match expected worktree"):
        core.collect_request(repo, config, EXAMPLE_ID, worktree_root=worktree_root)


def test_rerun_needs_a_receipt(tmp_path: Path) -> None:
    repo = _example_repo(tmp_path)
    config = load_config(repo)

    with pytest.raises(ExpctlError, match="no receipt"):
        rerun_request(repo, config, EXAMPLE_ID, check_git=False)


def test_rerun_copies_the_request_under_the_next_id(tmp_path: Path) -> None:
    repo = _example_repo(tmp_path)
    config = load_config(repo)
    _fake_receipt(repo, EXAMPLE_ID)
    original, original_path = load_request(repo, config, EXAMPLE_ID, check_git=False)

    result = rerun_request(
        repo, config, EXAMPLE_ID, reason="preempted", check_git=False
    )

    assert result == {
        "experiment_id": f"{EXAMPLE_ID}-r2",
        "rerun_of": EXAMPLE_ID,
        "backend": "slurm",
        "path": f"expctl/requests/{EXAMPLE_ID}-r2.toml",
        "commit": "0" * 40,
        "worktree": "myproject-example",
    }
    copy, copy_path = load_request(repo, config, f"{EXAMPLE_ID}-r2", check_git=False)
    assert copy["rerun_of"] == EXAMPLE_ID
    assert copy["rerun_reason"] == "preempted"
    assert copy["code"] == original["code"]
    assert copy["slurm"] == original["slurm"]
    # Byte-for-byte the same file apart from the header lines.
    copy_lines = copy_path.read_text(encoding="utf-8").splitlines()
    original_lines = original_path.read_text(encoding="utf-8").splitlines()
    assert copy_lines[:4] == [
        "version = 1",
        f'id = "{EXAMPLE_ID}-r2"',
        f'rerun_of = "{EXAMPLE_ID}"',
        'rerun_reason = "preempted"',
    ]
    assert copy_lines[4:] == original_lines[2:]
    # The original request and its receipt are untouched.
    assert original_path.read_text(encoding="utf-8") == EXAMPLE_REQUEST
    assert (repo / "expctl" / "results" / EXAMPLE_ID / "receipt.json").is_file()

    # Rerunning the rerun points at its immediate predecessor and drops the
    # old reason.
    _fake_receipt(repo, f"{EXAMPLE_ID}-r2")
    again = rerun_request(repo, config, f"{EXAMPLE_ID}-r2", check_git=False)

    assert again["experiment_id"] == f"{EXAMPLE_ID}-r3"
    third, _ = load_request(repo, config, f"{EXAMPLE_ID}-r3", check_git=False)
    assert third["rerun_of"] == f"{EXAMPLE_ID}-r2"
    assert "rerun_reason" not in third


def test_rerun_keeps_crlf_and_honours_an_explicit_id(tmp_path: Path) -> None:
    repo = _example_repo(tmp_path)
    config = load_config(repo)
    request = repo / "expctl" / "requests" / f"{EXAMPLE_ID}.toml"
    request.write_bytes(EXAMPLE_REQUEST.replace("\n", "\r\n").encode("utf-8"))
    _fake_receipt(repo, EXAMPLE_ID)

    result = rerun_request(
        repo, config, EXAMPLE_ID, new_id="20260102-example", check_git=False
    )

    copy = repo / "expctl" / "requests" / "20260102-example.toml"
    assert result["experiment_id"] == "20260102-example"
    assert b"\n" not in copy.read_bytes().replace(b"\r\n", b"")
    with pytest.raises(ExpctlError, match="already exists"):
        rerun_request(repo, config, EXAMPLE_ID, new_id=EXAMPLE_ID, check_git=False)


def test_rerun_of_must_be_an_experiment_id(tmp_path: Path) -> None:
    text = EXAMPLE_REQUEST.replace("title = ", 'rerun_of = "nope"\ntitle = ')
    repo = _example_repo(tmp_path, text)
    config = load_config(repo)

    with pytest.raises(ExpctlError, match="rerun_of"):
        load_request(repo, config, EXAMPLE_ID, check_git=False)


def test_receipt_summary_names_job_submitter_and_verdict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    receipt = _fake_receipt(tmp_path, EXAMPLE_ID)
    monkeypatch.setattr(
        core, "_scheduler_status", lambda repo, job_id: {"state": "FAILED"}
    )

    assert _receipt_summary(tmp_path, receipt) == (
        "slurm job 123, submitted 2026-08-28T00:26:08+00:00 by peng, "
        "receipt status submitted, sacct FAILED"
    )

    receipt.write_text("{not json", encoding="utf-8")
    assert _receipt_summary(tmp_path, receipt) == "unreadable receipt"


def test_validate_rejects_template_placeholders(tmp_path: Path) -> None:
    filled = core._render_new_request(
        core.STARTER_TEMPLATE,
        experiment_id=EXAMPLE_ID,
        branch="main",
        commit=ZERO_COMMIT,
        worktree="myproject-example",
    )
    repo = _example_repo(tmp_path, filled)
    config = load_config(repo)

    with pytest.raises(ExpctlError, match="placeholder") as excinfo:
        load_request(repo, config, EXAMPLE_ID, check_git=False)
    message = str(excinfo.value)
    for field in ("title", "question", "decision_rule", "outputs.metrics"):
        assert field in message
    load_request(repo, config, EXAMPLE_ID, check_git=False, check_placeholders=False)

    # A request that merely shares one plausible value with the template but
    # has real content elsewhere is fine.
    real = filled.replace("Short experiment title", "LR sweep")
    real = real.replace(
        "What uncertainty does this experiment resolve?", "Is 1e-4 too high?"
    )
    real = real.replace("What result changes the next decision?", "Loss below 2.0.")
    real = real.replace('"logs/example-{job_id}.out"', '"logs/lr-{job_id}.out"')
    real = real.replace('["metric_name"]', '["loss"]')
    real = real.replace('["runs/example/ckpt.pt"]', "[]")
    real = real.replace('"scripts/example.slurm"', '"scripts/lr.slurm"')
    request_file = repo / "expctl" / "requests" / f"{EXAMPLE_ID}.toml"
    request_file.write_text(real, encoding="utf-8")
    load_request(repo, config, EXAMPLE_ID, check_git=False)


def test_git_validation_explains_missing_commits_and_scripts(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    (repo / "expctl.toml").write_text(EXAMPLE_CONFIG, encoding="utf-8")
    (repo / "scripts").mkdir()
    (repo / "scripts" / "sweep.slurm").write_text(
        "#!/bin/bash\n#SBATCH -p example-partition\n#SBATCH --nodes=1\n",
        encoding="utf-8",
    )
    _git(repo, "add", ".")
    _git(
        repo,
        "-c",
        "user.name=expctl tests",
        "-c",
        "user.email=expctl@example.invalid",
        "commit",
        "-q",
        "-m",
        "initial",
    )
    commit = _git(repo, "rev-parse", "HEAD")
    requests = repo / "expctl" / "requests"
    requests.mkdir(parents=True)
    config = load_config(repo)

    (requests / f"{EXAMPLE_ID}.toml").write_text(EXAMPLE_REQUEST, encoding="utf-8")
    with pytest.raises(ExpctlError, match="not in this clone.*git fetch origin main"):
        load_request(repo, config, EXAMPLE_ID)

    pinned = EXAMPLE_REQUEST.replace(ZERO_COMMIT, commit)
    (requests / f"{EXAMPLE_ID}.toml").write_text(pinned, encoding="utf-8")
    request, _ = load_request(repo, config, EXAMPLE_ID)
    assert core._validate_request_git(repo, config, request) == 1

    missing = pinned.replace("scripts/sweep.slurm", "scripts/missing.slurm")
    (requests / f"{EXAMPLE_ID}.toml").write_text(missing, encoding="utf-8")
    with pytest.raises(ExpctlError, match="does not exist at commit .*commit and push"):
        load_request(repo, config, EXAMPLE_ID)

    with pytest.raises(ExpctlError, match="not inside a Git repository"):
        core.find_repo_root(tmp_path)


def test_validate_cli_prints_a_summary_on_a_terminal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo = _example_repo(tmp_path)
    monkeypatch.setattr(core, "find_repo_root", lambda: repo)
    monkeypatch.setattr(core, "_validate_request_git", lambda *args, **kwargs: 2)

    monkeypatch.setattr(core.sys.stdout, "isatty", lambda: False)
    assert core.main(["validate", EXAMPLE_ID]) == 0
    assert capsys.readouterr().out == f"valid: {EXAMPLE_ID}\n"

    monkeypatch.setattr(core.sys.stdout, "isatty", lambda: True)
    assert core.main(["validate", EXAMPLE_ID]) == 0
    human = capsys.readouterr().out
    assert human.startswith("REQUEST VALID\n")
    assert EXAMPLE_ID in human
    assert "2 verified / 4 declared" in human
    assert "scripts/sweep.slurm" in human and "NUM_SAMPLES=256" in human
    assert "Metrics" in human and "gen_ppl" in human


def _collected_receipt(repo: Path, experiment_id: str) -> Path:
    directory = repo / "expctl" / "results" / experiment_id
    directory.mkdir(parents=True)
    receipt = directory / "receipt.json"
    receipt.write_text(
        json.dumps(
            {
                "job_id": "123",
                "status": "collected",
                "submitted_at": "2026-08-28T00:26:08+00:00",
                "submitted_by": "peng",
                "collection": {
                    "collected_at": "2026-08-28T09:00:00+00:00",
                    "logs": [f"expctl/results/{experiment_id}/logs/job-123.out"],
                    "missing_metrics": ["gen_ppl_full"],
                    "scheduler": {
                        "state": "COMPLETED",
                        "jobs": [
                            {"job_id": "123", "state": "COMPLETED", "exit_code": "0:0"}
                        ],
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    (directory / "metrics.json").write_text(
        json.dumps({"job-123.out": {"gen_ppl": 24.65}}), encoding="utf-8"
    )
    return receipt


def test_report_scaffolds_from_collected_results_and_marks_reviewed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request_text = EXAMPLE_REQUEST.replace(
        'metrics = ["gen_ppl"]', 'metrics = ["gen_ppl", "gen_ppl_full"]'
    )
    repo = _example_repo(tmp_path, request_text)
    config = load_config(repo)
    monkeypatch.setattr(core, "_validate_request_git", lambda *args, **kwargs: 1)

    with pytest.raises(ExpctlError, match="not submitted yet"):
        core.report_request(repo, config, EXAMPLE_ID)
    _fake_receipt(repo, EXAMPLE_ID)
    with pytest.raises(ExpctlError, match="not collected yet.*expctl collect"):
        core.report_request(repo, config, EXAMPLE_ID)
    (repo / "expctl" / "results" / EXAMPLE_ID / "receipt.json").unlink()
    (repo / "expctl" / "results" / EXAMPLE_ID).rmdir()
    _collected_receipt(repo, EXAMPLE_ID)
    assert core.list_requests(repo, config)[0]["status"] == "collected"

    result = core.report_request(repo, config, EXAMPLE_ID)

    report = repo / "expctl" / "results" / EXAMPLE_ID / "report.md"
    assert result["path"] == f"expctl/results/{EXAMPLE_ID}/report.md"
    assert result["metrics"] == ["gen_ppl"]
    assert result["missing_metrics"] == ["gen_ppl_full"]
    text = report.read_text(encoding="utf-8")
    assert text.startswith("# Example sweep\n")
    assert f"- Request: `expctl/requests/{EXAMPLE_ID}.toml`" in text
    assert "- Job: 123 submitted 2026-08-28T00:26:08+00:00 by peng" in text
    assert "- Backend: slurm" in text
    assert "- Execution: COMPLETED (exit 0:0)" in text
    assert "## Question\n\nDoes the framework parse a well-formed request?" in text
    assert "## Decision rule\n\nValidation passes." in text
    assert "| metric | `job-123.out` |" in text
    assert "| gen_ppl | 24.65 |" in text
    assert "| gen_ppl_full | n/a |" in text
    assert "Missing from every log: `gen_ppl_full`" in text
    assert "## Observations" in text and "## Conclusion" in text
    rendered = core._render_report_result(result)
    assert "REPORT CREATED" in rendered and "git add" in rendered

    with pytest.raises(ExpctlError, match="already exists"):
        core.report_request(repo, config, EXAMPLE_ID)
    assert report.read_text(encoding="utf-8") == text
    assert core.list_requests(repo, config)[0]["status"] == "reviewed"
    status = {
        "experiment_id": EXAMPLE_ID,
        "job_id": "123",
        "receipt_status": "collected",
        "report": result["path"],
    }
    assert core._status_state(status) == "REVIEWED"
    assert f"Review {result['path']}" in core._render_status_result(
        status, result_path=f"expctl/results/{EXAMPLE_ID}"
    )
    del status["report"]
    assert f"expctl report {EXAMPLE_ID}" in core._render_status_result(
        status, result_path=f"expctl/results/{EXAMPLE_ID}"
    )


def _local_e2e_repo(tmp_path: Path, script_body: str) -> tuple[Path, Config]:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    config_text = EXAMPLE_CONFIG.replace(
        'required_script_lines = ["#SBATCH -p example-partition"]',
        "required_script_lines = []",
    ).replace('root = ".."', 'root = "../worktrees"')
    (repo / "expctl.toml").write_text(config_text, encoding="utf-8")
    script = repo / "scripts" / "run-local.py"
    script.parent.mkdir()
    script.write_text(script_body, encoding="utf-8")
    script.chmod(0o755)
    _git(repo, "add", "expctl.toml", "scripts/run-local.py")
    _git(
        repo,
        "-c",
        "user.name=expctl tests",
        "-c",
        "user.email=expctl@example.invalid",
        "commit",
        "-q",
        "-m",
        "local fixture",
    )
    commit = _git(repo, "rev-parse", "HEAD")
    request_text = LOCAL_REQUEST.replace(ZERO_COMMIT, commit).replace(
        "myproject-local-example", "local-e2e-worktree"
    )
    requests = repo / "expctl" / "requests"
    requests.mkdir(parents=True)
    (requests / f"{EXAMPLE_ID}.toml").write_text(request_text, encoding="utf-8")
    return repo, load_config(repo)


@pytest.mark.skipif(os.name != "posix", reason="local execution is Linux-only")
def test_local_backend_runs_collects_reports_and_cleans_end_to_end(
    tmp_path: Path,
) -> None:
    repo, config = _local_e2e_repo(
        tmp_path,
        "#!/usr/bin/env python3\n"
        "import os\n"
        "print(f\"score: {os.environ['LOCAL_VALUE']}\", flush=True)\n",
    )

    preview = submit_request(
        repo,
        config,
        EXAMPLE_ID,
        dry_run=True,
        skip_node_check=False,
        worktree_root=None,
    )
    assert preview["backend"] == "local"
    assert preview["command"][0] == "./scripts/run-local.py"

    receipt = submit_request(
        repo,
        config,
        EXAMPLE_ID,
        dry_run=False,
        skip_node_check=False,
        worktree_root=None,
    )
    assert receipt["backend"] == "local"
    assert receipt["job_id"].startswith("local-")
    assert receipt["pid"] > 0

    deadline = time.monotonic() + 10
    while True:
        status = status_request(repo, config, EXAMPLE_ID)
        if core._status_is_terminal(status):
            break
        if time.monotonic() >= deadline:
            pytest.fail("local experiment did not finish")
        time.sleep(0.05)
    assert core._status_state(status) == "COMPLETED"
    assert (
        "score: 3.5"
        in core.logs_request(repo, config, EXAMPLE_ID)["logs"][0]["content"]
    )

    collection = core.collect_request(repo, config, EXAMPLE_ID)
    assert collection["backend"] == "local"
    assert collection["scheduler"]["state"] == "COMPLETED"
    assert collection["missing_metrics"] == []
    report = core.report_request(repo, config, EXAMPLE_ID)
    assert report["backend"] == "local"
    assert core.list_requests(repo, config)[0]["status"] == "reviewed"
    cleaned = core.clean_request(repo, config, EXAMPLE_ID)
    assert cleaned["removed"] is True


@pytest.mark.skipif(os.name != "posix", reason="local execution is Linux-only")
def test_local_backend_cancels_and_collects_an_actual_process_group(
    tmp_path: Path,
) -> None:
    repo, config = _local_e2e_repo(
        tmp_path,
        "#!/usr/bin/env python3\n"
        "import time\n"
        "print('started', flush=True)\n"
        "time.sleep(30)\n",
    )
    receipt = submit_request(
        repo,
        config,
        EXAMPLE_ID,
        dry_run=False,
        skip_node_check=False,
        worktree_root=None,
    )

    deadline = time.monotonic() + 5
    while True:
        try:
            log = core.logs_request(repo, config, EXAMPLE_ID)["logs"][0]["content"]
        except ExpctlError:
            log = ""
        if log:
            break
        if time.monotonic() >= deadline:
            pytest.fail("local experiment did not start")
        time.sleep(0.05)
    cancelled = core.cancel_request(repo, config, EXAMPLE_ID, reason="test")
    assert cancelled["pid"] == receipt["pid"]

    deadline = time.monotonic() + 5
    while True:
        status = status_request(repo, config, EXAMPLE_ID)
        if core._status_is_terminal(status):
            break
        if time.monotonic() >= deadline:
            pytest.fail("cancelled local experiment did not stop")
        time.sleep(0.05)
    assert core._status_state(status) == "CANCELLED"
    collection = core.collect_request(repo, config, EXAMPLE_ID)
    assert collection["scheduler"]["state"] == "CANCELLED"
    assert collection["missing_metrics"] == ["score"]


def test_version_matches_pyproject() -> None:
    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    with pyproject.open("rb") as handle:
        declared = tomllib.load(handle)["project"]["version"]

    assert __version__ == declared


def _symlinks_allowed(tmp_path: Path) -> bool:
    probe = tmp_path / "symlink-probe"
    try:
        probe.symlink_to(tmp_path, target_is_directory=True)
    except OSError:
        return False
    probe.unlink()
    return True


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        text=True,
        capture_output=True,
    ).stdout.strip()


def test_reused_worktree_must_be_clean_and_from_the_same_repository(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    (repo / "tracked.py").write_text("value = 1\n", encoding="utf-8")
    _git(repo, "add", "tracked.py")
    _git(
        repo,
        "-c",
        "user.name=expctl tests",
        "-c",
        "user.email=expctl@example.invalid",
        "commit",
        "-q",
        "-m",
        "initial",
    )
    commit = _git(repo, "rev-parse", "HEAD")
    worktree = tmp_path / "worktree"

    _ensure_worktree(repo, worktree, commit, allowed_untracked=("logs",))
    (worktree / "logs").mkdir()
    (worktree / "logs" / "job.out").write_text("ok", encoding="utf-8")
    _ensure_worktree(repo, worktree, commit, allowed_untracked=("logs",))

    (worktree / "tracked.py").write_text("value = 2\n", encoding="utf-8")
    with pytest.raises(ExpctlError, match="not clean"):
        _ensure_worktree(repo, worktree, commit, allowed_untracked=("logs",))


def test_output_dir_materialised_by_the_checkout_is_kept(tmp_path: Path) -> None:
    if not _symlinks_allowed(tmp_path):
        pytest.skip("symlinks not permitted here")
    repo, worktree = tmp_path / "repo", tmp_path / "wt"
    (repo / "data").mkdir(parents=True)
    (repo / "logs").mkdir()
    # Tracked log files put a real logs/ directory into every checkout.
    (worktree / "logs").mkdir(parents=True)
    (worktree / "logs" / "old-job.out").write_text("tracked", encoding="utf-8")
    config = Config(
        root="expctl",
        required_script_lines=(),
        max_total_nodes=0,
        shared_dirs=("data", "logs", "runs"),
        create_missing=("runs", "logs"),
        worktree_root="..",
    )

    status = _ensure_runtime_links(repo, config, worktree)

    assert status == {"data": "linked", "logs": "kept-checkout-dir", "runs": "linked"}
    assert (worktree / "data").is_symlink() and (worktree / "runs").is_symlink()
    assert (worktree / "logs" / "old-job.out").is_file()
    # Re-running is idempotent.
    assert _ensure_runtime_links(repo, config, worktree)["data"] == "already-linked"

    # An INPUT directory the checkout materialised is still an error: the job
    # would read the checkout's copy instead of the shared data.
    (worktree / "data").unlink()
    (worktree / "data").mkdir()
    with pytest.raises(ExpctlError, match="already exists"):
        _ensure_runtime_links(repo, config, worktree)
