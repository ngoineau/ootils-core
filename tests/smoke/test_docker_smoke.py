"""
tests/smoke/test_docker_smoke.py — Lot D: Docker non-regression smoke tests (tests 27–30).

Tests 27-28: Static file/Dockerfile analysis — run without Docker.
Tests 29-30: Full Docker Compose smoke — require Docker daemon.
             Skip if Docker is not available.

Usage (with Docker):
    docker compose -f docker-compose.yml up -d postgres
    pytest tests/smoke/ -v

Or just run 27-28 without Docker:
    pytest tests/smoke/ -v -k "test_27 or test_28"
"""
from __future__ import annotations

import os
import shutil
import subprocess
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parents[2]
DOCKERFILE = REPO_ROOT / "Dockerfile"
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
SEED_SCRIPT = REPO_ROOT / "scripts" / "seed_demo_data.py"


def _docker_available() -> bool:
    """Return True if Docker CLI is available and daemon is running."""
    if shutil.which("docker") is None:
        return False
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True, timeout=10,
        )
        return result.returncode == 0
    except Exception:
        return False


DOCKER_AVAILABLE = _docker_available()

requires_docker = pytest.mark.skipif(
    not DOCKER_AVAILABLE,
    reason="Docker daemon not available",
)

# Test API token for smoke tests
SMOKE_API_TOKEN = "smoke-test-token"


# ---------------------------------------------------------------------------
# Test 27 — Docker image includes scripts/seed_demo_data.py
# ---------------------------------------------------------------------------

def test_27_dockerfile_copies_scripts():
    """
    Dockerfile must COPY scripts/ into the image.
    Checks the Dockerfile statically — no build required.
    """
    assert DOCKERFILE.exists(), f"Dockerfile not found at {DOCKERFILE}"
    content = DOCKERFILE.read_text(encoding="utf-8")

    # Must have a COPY instruction that includes scripts/
    lines = content.splitlines()
    copy_lines = [l.strip() for l in lines if l.strip().upper().startswith("COPY")]

    scripts_copied = any("scripts" in line for line in copy_lines)
    assert scripts_copied, (
        f"Dockerfile does not COPY scripts/ directory.\n"
        f"COPY lines found: {copy_lines}\n"
        f"scripts/seed_demo_data.py must be included in the image."
    )

    # Also verify the seed script actually exists
    assert SEED_SCRIPT.exists(), f"Seed script not found at {SEED_SCRIPT}"


# ---------------------------------------------------------------------------
# Test 28 — Dockerfile build order: COPY src/ before pip install
# ---------------------------------------------------------------------------

def test_28_dockerfile_copy_before_install():
    """
    Dockerfile must COPY source files before running pip install.
    This prevents 'editable install fails because src/ not found' regressions.
    """
    assert DOCKERFILE.exists(), f"Dockerfile not found at {DOCKERFILE}"
    content = DOCKERFILE.read_text(encoding="utf-8")
    lines = [l.strip() for l in content.splitlines() if l.strip()]

    # Find line numbers for COPY src/ and RUN pip install
    copy_src_line = None
    pip_install_line = None

    for i, line in enumerate(lines):
        if line.upper().startswith("COPY") and "src" in line.lower():
            copy_src_line = i
        if "pip install" in line.lower() and copy_src_line is not None:
            if pip_install_line is None:
                pip_install_line = i

    assert copy_src_line is not None, (
        "Dockerfile must have a COPY instruction for src/\n"
        f"Lines: {lines}"
    )
    assert pip_install_line is not None, (
        "Dockerfile must have a RUN pip install after COPY src/"
    )
    assert copy_src_line < pip_install_line, (
        f"COPY src/ (line {copy_src_line}) must come BEFORE pip install (line {pip_install_line}).\n"
        f"Dockerfile content:\n{content}"
    )


# ---------------------------------------------------------------------------
# Test 29 — docker compose up --build -d succeeds on clean environment
# ---------------------------------------------------------------------------

@requires_docker
def test_29_docker_compose_up_build():
    """
    docker compose up --build -d succeeds.
    Cleans up containers after the test.
    """
    if not COMPOSE_FILE.exists():
        pytest.skip(f"docker-compose.yml not found at {COMPOSE_FILE}")

    env_file = REPO_ROOT / ".env"
    # Bug 5 fix: always use ephemeral .env, never reuse prod/dev .env
    existing_env_backup = env_file.read_text(encoding="utf-8") if env_file.exists() else None
    smoke_env_content = (
        "POSTGRES_USER=ootils\n"
        "POSTGRES_PASSWORD=ootils\n"
        "POSTGRES_DB=ootils_smoke\n"
        f"OOTILS_API_TOKEN={SMOKE_API_TOKEN}\n"
    )
    env_file.write_text(smoke_env_content)

    try:
        # Build and start
        result = subprocess.run(
            ["docker", "compose", "-f", str(COMPOSE_FILE), "up", "--build", "-d"],
            capture_output=True, text=True, timeout=300,
            cwd=str(REPO_ROOT),
        )

        if result.returncode != 0:
            # Try older syntax
            result = subprocess.run(
                ["docker-compose", "-f", str(COMPOSE_FILE), "up", "--build", "-d"],
                capture_output=True, text=True, timeout=300,
                cwd=str(REPO_ROOT),
            )

        assert result.returncode == 0, (
            f"docker compose up failed (rc={result.returncode}):\n"
            f"stdout: {result.stdout[-2000:]}\n"
            f"stderr: {result.stderr[-2000:]}"
        )

        # Wait for services to be healthy
        time.sleep(10)

        # Verify containers are running
        ps_result = subprocess.run(
            ["docker", "compose", "-f", str(COMPOSE_FILE), "ps"],
            capture_output=True, text=True, timeout=30,
            cwd=str(REPO_ROOT),
        )
        assert ps_result.returncode == 0

    finally:
        # Tear down
        subprocess.run(
            ["docker", "compose", "-f", str(COMPOSE_FILE), "down", "-v"],
            capture_output=True, timeout=60, cwd=str(REPO_ROOT),
        )
        # Restore original .env or remove ephemeral one
        if existing_env_backup is not None:
            env_file.write_text(existing_env_backup)
        elif env_file.exists():
            env_file.unlink()


# ---------------------------------------------------------------------------
# Test 30 — Full smoke: migrate → seed → health → issues(auth)
# ---------------------------------------------------------------------------

@requires_docker
def test_30_full_smoke_migrate_seed_health_issues():
    """
    Full end-to-end smoke: docker compose up → migrate → seed → health → issues.
    """
    import requests
    import sys

    if not COMPOSE_FILE.exists():
        pytest.skip(f"docker-compose.yml not found at {COMPOSE_FILE}")

    env_file = REPO_ROOT / ".env"
    # Bug 5 fix: always use ephemeral .env, never reuse prod/dev .env
    existing_env_backup = env_file.read_text(encoding="utf-8") if env_file.exists() else None
    env_content = (
        "POSTGRES_USER=ootils\n"
        "POSTGRES_PASSWORD=ootils\n"
        "POSTGRES_DB=ootils_smoke30\n"
        f"OOTILS_API_TOKEN={SMOKE_API_TOKEN}\n"
    )
    env_file.write_text(env_content)

    db_url = "postgresql://ootils:ootils@localhost:5432/ootils_smoke30"

    try:
        # 1. Start services
        up_result = subprocess.run(
            ["docker", "compose", "-f", str(COMPOSE_FILE), "up", "--build", "-d"],
            capture_output=True, text=True, timeout=300,
            cwd=str(REPO_ROOT),
        )
        # Bug 2 fix: FAIL (not skip) when Docker is available but compose fails
        if up_result.returncode != 0:
            pytest.fail(f"docker compose up failed: {up_result.stderr[-1000:]}")

        # 2. Wait for postgres healthcheck
        max_wait = 60
        for i in range(max_wait):
            time.sleep(1)
            ps = subprocess.run(
                ["docker", "compose", "-f", str(COMPOSE_FILE), "ps", "--format", "json"],
                capture_output=True, text=True, timeout=10,
                cwd=str(REPO_ROOT),
            )
            if "healthy" in ps.stdout or i > 15:
                break

        # 3. Run seed via docker exec (the seed script is in the image)
        # Get the API container name
        ps_result = subprocess.run(
            ["docker", "compose", "-f", str(COMPOSE_FILE), "ps", "-q", "api"],
            capture_output=True, text=True, timeout=10,
            cwd=str(REPO_ROOT),
        )
        api_container = ps_result.stdout.strip()

        if api_container:
            seed_result = subprocess.run(
                [
                    "docker", "exec",
                    "-e", f"DATABASE_URL={db_url}",
                    api_container,
                    "python", "/app/scripts/seed_demo_data.py",
                ],
                capture_output=True, text=True, timeout=60,
            )
            assert seed_result.returncode == 0, (
                f"Seed failed in container:\n{seed_result.stdout}\n{seed_result.stderr}"
            )

        # 4. Wait for API to be ready (expose port 8000)
        api_ready = False
        for _ in range(30):
            time.sleep(2)
            try:
                r = requests.get("http://localhost:8000/health", timeout=3)
                if r.status_code == 200:
                    api_ready = True
                    break
            except Exception:
                pass

        # Bug 2 fix: FAIL (not skip) when Docker is running but API is unreachable
        if not api_ready:
            pytest.fail("API not reachable on localhost:8000 — port may not be exposed in docker-compose.yml")

        # 5. Health check
        resp = requests.get("http://localhost:8000/health", timeout=10)
        assert resp.status_code == 200, f"Health check failed: {resp.text}"
        assert resp.json()["status"] == "ok"

        # 6. Issues with auth
        resp = requests.get(
            "http://localhost:8000/v1/issues",
            headers={"Authorization": f"Bearer {SMOKE_API_TOKEN}"},
            timeout=10,
        )
        assert resp.status_code == 200, f"Issues endpoint failed: {resp.text}"
        data = resp.json()
        assert "issues" in data
        assert "total" in data

    finally:
        subprocess.run(
            ["docker", "compose", "-f", str(COMPOSE_FILE), "down", "-v"],
            capture_output=True, timeout=60, cwd=str(REPO_ROOT),
        )
        # Restore original .env or remove ephemeral one
        if existing_env_backup is not None:
            env_file.write_text(existing_env_backup)
        elif env_file.exists():
            env_file.unlink()
