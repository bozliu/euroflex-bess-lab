"""deploy/ templates: they parse, reference files that exist, stay paper-only and agree with run_live.sh."""

from __future__ import annotations

import configparser
import json
import os
import plistlib
import re
import shlex
import shutil
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
DEPLOY = REPO_ROOT / "deploy"
OPS = REPO_ROOT / "scripts" / "ops"
RUN_LIVE = OPS / "run_live.sh"
#: Environment variables the templates may set besides the EUROFLEX_* settings documented in run_live.sh.
GENERIC_ENV = {"TZ", "PATH", "MPLCONFIGDIR", "PYTHONUNBUFFERED", "PYTHONDONTWRITEBYTECODE", "OMP_NUM_THREADS"}
CONTAINER_SCRIPTS = {
    "/app/scripts/ops/healthcheck.py": OPS / "healthcheck.py",
    "/app/scripts/ops/run_live.sh": RUN_LIVE,
    "/app/scripts/ops/build_models.py": OPS / "build_models.py",
}


def documented_env() -> set[str]:
    names = set(re.findall(r"^#\s+(EUROFLEX_[A-Z_]+)\s", RUN_LIVE.read_text(encoding="utf-8"), flags=re.M))
    return names | {"EUROFLEX_HEALTH_MAX_AGE_S"}  # read by healthcheck.py


def allowed_env() -> set[str]:
    return documented_env() | GENERIC_ENV


def walk(node: Any) -> Iterator[Any]:
    yield node
    if isinstance(node, dict):
        for value in node.values():
            yield from walk(value)
    elif isinstance(node, list):
        for value in node:
            yield from walk(value)


def container_paths(node: Any) -> set[str]:
    return {v for v in walk(node) if isinstance(v, str) and v.startswith("/app/scripts/")}


# --------------------------------------------------------------------------------------------------------- generic


def test_every_template_is_present() -> None:
    expected = [
        "Dockerfile",
        "Dockerfile.dockerignore",
        "docker-compose.live.yml",
        "k8s/namespace.yaml",
        "k8s/configmap.yaml",
        "k8s/pvc.yaml",
        "k8s/deployment.yaml",
        "k8s/networkpolicy.yaml",
        "k8s/cronjob-reconcile.yaml",
        "k8s/kustomization.yaml",
        "aws/ecs-task-definition.json",
        "aws/cloudformation.yaml",
        "systemd/euroflex-live.service",
        "systemd/live.env.example",
        "systemd/euroflex-live-healthcheck.service",
        "systemd/euroflex-live-healthcheck.timer",
        "launchd/io.github.euroflex-bess-lab.live.plist",
    ]
    missing = [name for name in expected if not (DEPLOY / name).is_file()]
    assert not missing, missing
    for script in ("healthcheck.py", "build_models.py", "track_record.py"):
        assert (OPS / script).is_file()
    assert os.access(RUN_LIVE, os.X_OK), "run_live.sh is the container entrypoint and must be executable"


def test_no_account_ids_credentials_or_local_paths() -> None:
    patterns = {
        "aws account id": re.compile(r"(?<![\d.])\d{12}(?![\d.])"),
        "aws access key": re.compile(r"AKIA[0-9A-Z]{16}"),
        "private key": re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
        "assigned secret": re.compile(r"(?i)\b(password|passwd|secret|token|api[_-]?key)\s*[:=]\s*\S+"),
        "local user path": re.compile("/" + "Users/"),
    }
    offenders = []
    for path in [*DEPLOY.rglob("*"), *OPS.glob("*")]:
        if not path.is_file() or path.suffix == ".pyc":
            continue
        text = path.read_text(encoding="utf-8")
        offenders += [f"{path.relative_to(REPO_ROOT)}: {name}" for name, rx in patterns.items() if rx.search(text)]
    assert not offenders, offenders


def test_templates_state_the_paper_only_guarantee() -> None:
    for path in [
        DEPLOY / "Dockerfile",
        DEPLOY / "docker-compose.live.yml",
        DEPLOY / "k8s" / "deployment.yaml",
        DEPLOY / "aws" / "cloudformation.yaml",
        DEPLOY / "systemd" / "euroflex-live.service",
        DEPLOY / "launchd" / "io.github.euroflex-bess-lab.live.plist",
        RUN_LIVE,
    ]:
        text = path.read_text(encoding="utf-8").lower()
        assert "paper" in text and ("never submits" in text or "no order" in text), path.name


# ----------------------------------------------------------------------------------------------------------- docker


def _dockerfile() -> list[str]:
    lines, buffer = [], ""
    for raw in (DEPLOY / "Dockerfile").read_text(encoding="utf-8").splitlines():
        if raw.strip().startswith("#") and not buffer:
            continue
        buffer += raw.rstrip("\\").strip() + " "
        if not raw.rstrip().endswith("\\"):
            if buffer.strip():
                lines.append(buffer.strip())
            buffer = ""
    return lines


def test_dockerfile_is_multistage_non_root_with_healthcheck() -> None:
    lines = _dockerfile()
    stages = [line.split()[-1] for line in lines if line.startswith("FROM ")]
    assert stages == ["build", "app", "with-models", "runtime"] and stages[-1] == "runtime"
    assert all("-slim" in line for line in lines if line.startswith("FROM python"))
    text = "\n".join(lines)
    assert 'pip install ".[ml]"' in text
    users = [line.split()[1] for line in lines if line.startswith("USER ")]
    assert users and users[-1] not in {"root", "0"}
    assert re.search(r"HEALTHCHECK .*/app/scripts/ops/healthcheck\.py", text)
    assert 'ENTRYPOINT ["/app/scripts/ops/run_live.sh"]' in text
    assert "EUROFLEX_EXECUTION=paper" in text


def test_dockerfile_copies_only_existing_allowlisted_paths() -> None:
    ignore = (DEPLOY / "Dockerfile.dockerignore").read_text(encoding="utf-8").splitlines()
    allow = [line[1:].rstrip("/") for line in ignore if line.startswith("!")]
    assert ignore[[i for i, line in enumerate(ignore) if not line.startswith("#")][0]] == "*"
    for line in _dockerfile():
        if not line.startswith("COPY ") or "--from=" in line:
            continue
        parts = [p for p in shlex.split(line)[1:] if not p.startswith("--")]
        for source in parts[:-1]:
            source = source.rstrip("/")
            assert (REPO_ROOT / source).exists(), f"COPY source missing: {source}"
            assert any(source == a or source.startswith(a + "/") for a in allow), f"{source} not in the allowlist"
    for excluded in ("data", "artifacts", ".venv", "reports/track_record", "tests"):
        assert not any(excluded == a or excluded.startswith(a + "/") for a in allow), excluded


def test_compose_live_service() -> None:
    compose = yaml.safe_load((DEPLOY / "docker-compose.live.yml").read_text(encoding="utf-8"))
    live = compose["services"]["live"]
    assert (DEPLOY / live["build"]["context"] / live["build"]["dockerfile"]).resolve() == (DEPLOY / "Dockerfile")
    assert live["restart"] == "unless-stopped"
    assert any(str(v).endswith(":/data") for v in live["volumes"])
    assert live["environment"]["EUROFLEX_EXECUTION"] == "paper"
    assert "/app/scripts/ops/healthcheck.py" in live["healthcheck"]["test"]
    assert live["read_only"] is True and "ALL" in live["cap_drop"]
    assert set(live["environment"]) <= allowed_env()
    for name in ("build-models", "reconcile"):
        assert compose["services"][name]["profiles"] == ["tools"]
    for path in container_paths(compose):
        assert CONTAINER_SCRIPTS[path].is_file()
    assert (DEPLOY / ".." / "data" / "public").resolve() == REPO_ROOT / "data" / "public"


@pytest.mark.skipif(shutil.which("docker") is None, reason="docker CLI not installed")
def test_compose_file_validates_with_docker_compose() -> None:
    result = subprocess.run(
        ["docker", "compose", "-f", str(DEPLOY / "docker-compose.live.yml"), "config", "--quiet"],
        capture_output=True,
        text=True,
        timeout=60,
    )
    if result.returncode != 0 and "compose" in result.stderr and "unknown" in result.stderr:
        pytest.skip("docker compose plugin not available")
    assert result.returncode == 0, result.stderr


# ------------------------------------------------------------------------------------------------------- kubernetes


def _k8s() -> dict[str, dict[str, Any]]:
    docs: dict[str, dict[str, Any]] = {}
    for path in sorted((DEPLOY / "k8s").glob("*.yaml")):
        for doc in yaml.safe_load_all(path.read_text(encoding="utf-8")):
            if doc:
                docs[f"{doc['kind']}/{doc.get('metadata', {}).get('name', path.stem)}"] = doc
    return docs


def test_kubernetes_manifests() -> None:
    docs = _k8s()
    assert {
        "Namespace/euroflex",
        "ConfigMap/euroflex-live-config",
        "PersistentVolumeClaim/euroflex-live-run",
        "PersistentVolumeClaim/euroflex-live-models",
        "Deployment/euroflex-live",
        "NetworkPolicy/euroflex-live",
        "CronJob/euroflex-live-reconcile",
        "Kustomization/kustomization",
    } <= set(docs)
    kustomization = docs["Kustomization/kustomization"]
    for resource in kustomization["resources"]:
        assert (DEPLOY / "k8s" / resource).is_file()
    deployment = docs["Deployment/euroflex-live"]["spec"]
    assert deployment["replicas"] == 1 and deployment["strategy"]["type"] == "Recreate"
    pod = deployment["template"]["spec"]
    assert pod["securityContext"]["runAsNonRoot"] is True and pod["automountServiceAccountToken"] is False
    (container,) = pod["containers"]
    assert container["securityContext"]["readOnlyRootFilesystem"] is True
    assert container["securityContext"]["capabilities"]["drop"] == ["ALL"]
    for probe in ("startupProbe", "livenessProbe", "readinessProbe"):
        command = container[probe]["exec"]["command"]
        assert "/app/scripts/ops/healthcheck.py" in command
    assert "--min-decisions" in container["readinessProbe"]["exec"]["command"]
    assert {"cpu", "memory"} <= set(container["resources"]["limits"])
    claims = {v["persistentVolumeClaim"]["claimName"] for v in pod["volumes"] if "persistentVolumeClaim" in v}
    assert claims == {"euroflex-live-run", "euroflex-live-models"}
    config = docs["ConfigMap/euroflex-live-config"]["data"]
    assert config["EUROFLEX_EXECUTION"] == "paper" and set(config) <= allowed_env()
    assert docs["NetworkPolicy/euroflex-live"]["spec"]["ingress"] == []
    assert "cronjob-reconcile.yaml" not in kustomization["resources"]  # optional, applied explicitly
    for doc in docs.values():
        for path in container_paths(doc):
            assert CONTAINER_SCRIPTS[path].is_file()


# -------------------------------------------------------------------------------------------------------------- aws


def test_ecs_task_definition() -> None:
    task = json.loads((DEPLOY / "aws" / "ecs-task-definition.json").read_text(encoding="utf-8"))
    assert task["requiresCompatibilities"] == ["FARGATE"] and task["networkMode"] == "awsvpc"
    (container,) = task["containerDefinitions"]
    env = {item["name"]: item["value"] for item in container["environment"]}
    assert env["EUROFLEX_EXECUTION"] == "paper" and set(env) <= allowed_env()
    assert container["readonlyRootFilesystem"] is True and container["user"].startswith("10001")
    assert "/app/scripts/ops/healthcheck.py" in container["healthCheck"]["command"]
    efs = next(v for v in task["volumes"] if "efsVolumeConfiguration" in v)
    assert efs["efsVolumeConfiguration"]["transitEncryption"] == "ENABLED"
    assert {m["containerPath"] for m in container["mountPoints"]} == {"/data", "/tmp"}
    placeholders = {v for v in walk(task) if isinstance(v, str) and v.startswith("REPLACE_WITH_")}
    assert "REPLACE_WITH_IMAGE_URI" in placeholders and "REPLACE_WITH_EFS_FILE_SYSTEM_ID" in placeholders


def test_cloudformation_references_resolve() -> None:
    template = yaml.safe_load((DEPLOY / "aws" / "cloudformation.yaml").read_text(encoding="utf-8"))
    params, resources = set(template["Parameters"]), set(template["Resources"])
    known = params | resources
    for node in walk(template):
        if not isinstance(node, dict):
            continue
        if "Ref" in node and len(node) == 1:
            target = node["Ref"]
            assert target in known or target.startswith("AWS::"), target
        if "Fn::GetAtt" in node:
            assert node["Fn::GetAtt"][0] in resources, node
        if "Fn::Sub" in node:
            for name in re.findall(r"\$\{([^}]+)\}", node["Fn::Sub"]):
                assert name in known or name.startswith("AWS::"), name
        for value in node.values():
            if isinstance(value, str):
                assert not value.startswith("!"), "use long-form intrinsics (plain YAML)"
    res = template["Resources"]
    assert res["FileSystem"]["Properties"]["Encrypted"] is True
    service = res["Service"]["Properties"]
    assert service["DesiredCount"] == 1 and service["DeploymentConfiguration"]["MaximumPercent"] == 100
    assert res["EfsSecurityGroup"]["Properties"]["SecurityGroupIngress"][0]["FromPort"] == 2049
    assert "SecurityGroupIngress" not in res["TaskSecurityGroup"]["Properties"]
    (container,) = res["TaskDefinition"]["Properties"]["ContainerDefinitions"]
    env = {item["Name"] for item in container["Environment"]}
    assert env <= allowed_env()
    paper = next(item for item in container["Environment"] if item["Name"] == "EUROFLEX_EXECUTION")
    assert paper["Value"] == "paper"
    volume = res["TaskDefinition"]["Properties"]["Volumes"][0]["EFSVolumeConfiguration"]
    assert volume["TransitEncryption"] == "ENABLED" and volume["AuthorizationConfig"]["IAM"] == "ENABLED"
    assert set(template["Parameters"]["Strategy"]["AllowedValues"]) >= {"threshold", "prism", "forecast-mpc"}


# --------------------------------------------------------------------------------------------- systemd and launchd


def _unit(path: Path) -> configparser.ConfigParser:
    parser = configparser.ConfigParser(strict=False, interpolation=None)
    parser.optionxform = str  # type: ignore[assignment,method-assign]
    parser.read_string(path.read_text(encoding="utf-8"))
    return parser


def test_systemd_units() -> None:
    service = _unit(DEPLOY / "systemd" / "euroflex-live.service")
    assert service["Service"]["ExecStart"].endswith("/scripts/ops/run_live.sh")
    assert service["Service"]["Restart"] == "always" and service["Service"]["User"] == "euroflex"
    assert service["Service"]["NoNewPrivileges"] == "yes" and service["Install"]["WantedBy"] == "multi-user.target"
    check = _unit(DEPLOY / "systemd" / "euroflex-live-healthcheck.service")
    command = check["Service"]["ExecStart"]
    assert "scripts/ops/healthcheck.py" in command and "$$rc" in command and "$rc" not in command.replace("$$", "")
    assert "OnUnitActiveSec" in _unit(DEPLOY / "systemd" / "euroflex-live-healthcheck.timer")["Timer"]
    env = {}
    for line in (DEPLOY / "systemd" / "live.env.example").read_text(encoding="utf-8").splitlines():
        if line and not line.startswith("#"):
            key, value = line.split("=", 1)
            env[key] = value
    assert env["EUROFLEX_EXECUTION"] == "paper" and set(env) <= allowed_env()


def test_launchd_agent() -> None:
    plist = plistlib.loads((DEPLOY / "launchd" / "io.github.euroflex-bess-lab.live.plist").read_bytes())
    assert plist["Label"] == "io.github.euroflex-bess-lab.live"
    assert plist["ProgramArguments"][-1] == "__REPO_DIR__/scripts/ops/run_live.sh"
    assert plist["KeepAlive"] is True and plist["RunAtLoad"] is True
    env = plist["EnvironmentVariables"]
    assert env["EUROFLEX_EXECUTION"] == "paper" and set(env) <= allowed_env()
    assert env["EUROFLEX_RUN_DIR"].startswith("__STATE_DIR__/")  # state outside the checkout


# ------------------------------------------------------------------------------------------------------ run_live.sh


def _run_live(env: dict[str, str], *args: str) -> subprocess.CompletedProcess[str]:
    clean = {k: v for k, v in os.environ.items() if not k.startswith("EUROFLEX_")}
    clean.update({"EUROFLEX_DRY_RUN": "1", "EUROFLEX_PYTHON": str(Path(sys.executable)), **env})
    return subprocess.run(["bash", str(RUN_LIVE), *args], env=clean, capture_output=True, text=True, timeout=60)


def test_run_live_syntax_and_documented_variables() -> None:
    assert subprocess.run(["bash", "-n", str(RUN_LIVE)], capture_output=True, timeout=30).returncode == 0
    body = RUN_LIVE.read_text(encoding="utf-8")
    used = set(re.findall(r"\$\{(EUROFLEX_[A-Z_]+)", body))
    assert used <= documented_env() and documented_env() - {"EUROFLEX_HEALTH_MAX_AGE_S"} <= used


def test_run_live_maps_environment_to_a_paper_run(tmp_path: Path) -> None:
    result = _run_live(
        {"EUROFLEX_RUN_DIR": str(tmp_path / "run"), "EUROFLEX_POWER_MW": "2", "EUROFLEX_STRATEGY_PARAMS": "a=1 b=x"},
        "--max-minutes",
        "5",
    )
    assert result.returncode == 0, result.stderr
    args = shlex.split(result.stdout)
    assert args[args.index("run") - 1] == "live"
    flags = dict(zip(args, args[1:], strict=False))  # each token -> the next one
    assert flags["--strategy"] == "threshold" and flags["--execution"] == "paper" and flags["--power-mw"] == "2"
    assert flags["--run-dir"] == str(tmp_path / "run") and flags["--mechanism"] == "auto"
    assert args.count("--param") == 2 and args[-2:] == ["--max-minutes", "5"]


def test_run_live_refuses_webhook_and_missing_models(tmp_path: Path) -> None:
    assert _run_live({"EUROFLEX_EXECUTION": "webhook"}).returncode == 2
    missing = _run_live({"EUROFLEX_STRATEGY": "prism"})
    assert missing.returncode == 2 and "EUROFLEX_MODELS_DIR" in missing.stderr
    empty = _run_live({"EUROFLEX_STRATEGY": "forecast-mpc", "EUROFLEX_MODELS_DIR": str(tmp_path)})
    assert empty.returncode == 2 and "deploy_price_current.pkl" in empty.stderr


def test_run_live_verifies_the_model_manifest(tmp_path: Path) -> None:
    models = tmp_path / "models"
    models.mkdir()
    for name in ("deploy_price_current.pkl", "deploy_prism_current.pkl", "deploy_prism_zero.pkl"):
        (models / name).write_bytes(name.encode())
    (models / "deploy_fusion_variance.json").write_text('{"variance": [1.0]}', encoding="utf-8")
    build = subprocess.run(
        [sys.executable, str(OPS / "build_models.py"), "manifest", str(models)], capture_output=True, timeout=60
    )
    assert build.returncode == 0
    ok = _run_live({"EUROFLEX_STRATEGY": "prism", "EUROFLEX_MODELS_DIR": str(models)})
    assert ok.returncode == 0 and "--strategy prism" in ok.stdout
    (models / "deploy_prism_zero.pkl").write_bytes(b"tampered")
    bad = _run_live({"EUROFLEX_STRATEGY": "prism", "EUROFLEX_MODELS_DIR": str(models)})
    assert bad.returncode == 2 and "MANIFEST.json" in bad.stderr
