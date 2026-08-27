import tomllib
from pathlib import Path

import pytest

from expctl.core import (
    ExpctlError,
    __version__,
    build_sbatch_command,
    extract_metrics,
    init_repo,
    load_config,
    load_request,
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


def test_version_matches_pyproject() -> None:
    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    with pyproject.open("rb") as handle:
        declared = tomllib.load(handle)["project"]["version"]

    assert __version__ == declared
