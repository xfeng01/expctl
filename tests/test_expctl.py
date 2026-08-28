import json
import tomllib
from pathlib import Path

import pytest

import expctl.core as core
from expctl.core import (
    Config,
    _ensure_runtime_links,
    _receipt_summary,
    ExpctlError,
    __version__,
    build_sbatch_command,
    extract_metrics,
    init_repo,
    load_config,
    load_request,
    rerun_request,
    status_request,
)


EXAMPLE_ID = "20260101-example"
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
commit = "{'0' * 40}"
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
    text = EXAMPLE_REQUEST.replace('title = ', 'rerun_of = "nope"\ntitle = ')
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
        root="expctl", required_script_lines=(), max_total_nodes=0,
        shared_dirs=("data", "logs", "runs"), create_missing=("runs", "logs"),
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
