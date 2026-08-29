import json
import subprocess
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
script = "scripts/example.slurm"
max_concurrent_nodes = 4

[slurm.env]
NUM_SAMPLES = "256"

[outputs]
log_glob = "logs/example-{{job_id}}_*.out"
metrics = ["gen_ppl"]
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
    assert init_repo(tmp_path) == []


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
        "scripts/example.slurm",
    ]
    assert build_sbatch_command(request, ("-p", "example-partition")) == [
        "sbatch",
        "--parsable",
        "--export=ALL,NUM_SAMPLES=256",
        "-p",
        "example-partition",
        "scripts/example.slurm",
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
    )
    monkeypatch.setattr(core, "find_repo_root", lambda: tmp_path)
    monkeypatch.setattr(core, "load_config", lambda repo: config)
    monkeypatch.setattr(core, "list_requests", lambda repo, loaded: rows)

    assert core.main(["list"]) == 0
    assert capsys.readouterr().out == f"{EXAMPLE_ID}\trequested\t中文标题\n"

    assert core.main(["list", "--json"]) == 0
    assert json.loads(capsys.readouterr().out) == rows


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
        "job 123, submitted 2026-08-28T00:26:08+00:00 by peng, "
        "receipt status submitted, sacct FAILED"
    )

    receipt.write_text("{not json", encoding="utf-8")
    assert _receipt_summary(tmp_path, receipt) == "unreadable receipt"


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
