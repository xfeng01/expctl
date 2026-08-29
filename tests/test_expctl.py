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
    worktree = tmp_path.parent / f"{tmp_path.name}-worktree"
    worktree.mkdir()
    _collectable_receipt(repo, worktree)
    monkeypatch.setattr(
        core,
        "_queue_status",
        lambda *args: [{"job": "123", "state": "RUNNING", "reason": "None"}],
    )

    with pytest.raises(ExpctlError, match="still in the queue"):
        core.collect_request(repo, config, EXAMPLE_ID)


def test_collect_rejects_colliding_log_basenames(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request_text = EXAMPLE_REQUEST.replace(
        'log_glob = "logs/example-{job_id}_*.out"',
        'log_glob = "logs/**/job-{job_id}.out"',
    )
    repo = _example_repo(tmp_path, request_text)
    config = load_config(repo)
    worktree = tmp_path.parent / f"{tmp_path.name}-worktree"
    for subdir in ("first", "second"):
        directory = worktree / "logs" / subdir
        directory.mkdir(parents=True)
        (directory / "job-123.out").write_text("gen_ppl: 2\n", encoding="utf-8")
    _collectable_receipt(repo, worktree)
    monkeypatch.setattr(core, "_queue_status", lambda *args: [])

    with pytest.raises(ExpctlError, match="colliding basenames"):
        core.collect_request(repo, config, EXAMPLE_ID)


def test_collect_records_missing_metrics_from_the_copied_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _example_repo(tmp_path)
    config = load_config(repo)
    worktree = tmp_path.parent / f"{tmp_path.name}-worktree"
    logs = worktree / "logs"
    logs.mkdir(parents=True)
    (logs / "example-123_0.out").write_text("loss: 1.5\n", encoding="utf-8")
    receipt_path = _collectable_receipt(repo, worktree)
    monkeypatch.setattr(core, "_queue_status", lambda *args: [])
    monkeypatch.setattr(
        core, "_scheduler_status", lambda *args: {"state": "FAILED", "jobs": []}
    )

    collection = core.collect_request(repo, config, EXAMPLE_ID)

    assert collection["missing_metrics"] == ["gen_ppl"]
    assert collection["scheduler"]["state"] == "FAILED"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["status"] == "collected"


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
