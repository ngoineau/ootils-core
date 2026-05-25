"""
harness.py — boot + tear-down the ootils-engine binary from Python.

Used by integration tests and the parity script. Manages:
- spawning the binary as a subprocess
- waiting for the gRPC port to open (boot is async, ~3s on profile L)
- forwarding stdout/stderr to caller-supplied files (for log capture)
- clean shutdown via SIGTERM, with SIGKILL fallback after timeout
- WAL file management — option to clean before start, option to kill -9
  the process to simulate crashes for recovery testing
"""
from __future__ import annotations

import logging
import os
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from typing import IO, Optional

logger = logging.getLogger(__name__)


class EngineHarness:
    """Lifecycle wrapper around the ootils-engine binary.

    Designed for integration tests. Production runs should use a proper
    process supervisor (systemd, k8s, etc.) — this is for local dev,
    CI, and the parity / kill-9 scripts.
    """

    def __init__(
        self,
        binary_path: Path,
        dsn: str,
        listen_addr: str = "127.0.0.1:50051",
        wal_path: Optional[Path] = None,
        flush_interval_ms: int = 100,
        log_level: str = "info,ootils_engine=debug",
    ) -> None:
        self.binary_path = Path(binary_path)
        if not self.binary_path.exists():
            raise FileNotFoundError(
                f"engine binary not found at {self.binary_path}. "
                "Did you run `cargo build --release -p ootils_engine`?"
            )
        self.dsn = dsn
        self.listen_addr = listen_addr
        self.wal_path = wal_path or Path(tempfile.gettempdir()) / "ootils-engine-test.wal"
        self.flush_interval_ms = flush_interval_ms
        self.log_level = log_level
        self.process: Optional[subprocess.Popen] = None
        self.stdout_log: Optional[IO] = None
        self.stderr_log: Optional[IO] = None
        self._stdout_path: Optional[Path] = None
        self._stderr_path: Optional[Path] = None

    def __enter__(self) -> "EngineHarness":
        self.start()
        return self

    def __exit__(self, *args) -> None:
        self.stop()

    def clean_wal(self) -> None:
        """Delete the WAL file. Use before starting the engine to
        guarantee an empty state."""
        if self.wal_path.exists():
            self.wal_path.unlink()
            logger.info("removed WAL at %s", self.wal_path)

    def start(self, wait_for_ready: bool = True, ready_timeout_s: float = 15.0) -> None:
        if self.process is not None and self.process.poll() is None:
            raise RuntimeError("engine already running")

        env = os.environ.copy()
        env["DATABASE_URL"] = self.dsn
        env["OOTILS_ENGINE_LISTEN"] = self.listen_addr
        env["OOTILS_WAL_PATH"] = str(self.wal_path)
        env["OOTILS_FLUSH_INTERVAL_MS"] = str(self.flush_interval_ms)
        env["RUST_LOG"] = self.log_level

        td = Path(tempfile.gettempdir())
        self._stdout_path = td / f"ootils-engine-stdout-{os.getpid()}.log"
        self._stderr_path = td / f"ootils-engine-stderr-{os.getpid()}.log"
        self.stdout_log = open(self._stdout_path, "wb")
        self.stderr_log = open(self._stderr_path, "wb")

        logger.info(
            "starting engine: %s (DSN=%s LISTEN=%s WAL=%s)",
            self.binary_path,
            self.dsn,
            self.listen_addr,
            self.wal_path,
        )
        self.process = subprocess.Popen(
            [str(self.binary_path)],
            env=env,
            stdout=self.stdout_log,
            stderr=self.stderr_log,
        )

        if wait_for_ready:
            self.wait_until_ready(timeout_s=ready_timeout_s)

    def wait_until_ready(self, timeout_s: float = 15.0) -> None:
        """Block until the gRPC port accepts a TCP connection, or
        timeout/crash. Polls every 100 ms."""
        host, port_s = self.listen_addr.rsplit(":", 1)
        port = int(port_s)
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if self.process is None or self.process.poll() is not None:
                raise RuntimeError(
                    f"engine exited during boot (code={self.process.returncode if self.process else 'no proc'}). "
                    f"See logs at {self._stdout_path}, {self._stderr_path}"
                )
            try:
                with socket.create_connection((host, port), timeout=0.2):
                    logger.info("engine ready (gRPC port reachable)")
                    return
            except (ConnectionRefusedError, OSError, socket.timeout):
                time.sleep(0.1)
        raise TimeoutError(
            f"engine did not become ready within {timeout_s}s. "
            f"Process state: {'running' if self.process and self.process.poll() is None else 'exited'}. "
            f"Logs: {self._stdout_path}, {self._stderr_path}"
        )

    def stop(self, timeout_s: float = 5.0) -> None:
        if self.process is None:
            return
        if self.process.poll() is not None:
            logger.info("engine already exited (code=%d)", self.process.returncode)
            self._close_logs()
            self.process = None
            return
        logger.info("stopping engine (SIGTERM)")
        self.process.terminate()
        try:
            self.process.wait(timeout=timeout_s)
            logger.info("engine stopped cleanly (code=%d)", self.process.returncode)
        except subprocess.TimeoutExpired:
            logger.warning("engine did not stop within %ss, SIGKILL'ing", timeout_s)
            self.process.kill()
            self.process.wait()
        self._close_logs()
        self.process = None

    def kill9(self) -> None:
        """Simulate a hard crash — SIGKILL. Use this to validate WAL
        recovery on restart."""
        if self.process is None or self.process.poll() is not None:
            logger.warning("kill9: engine not running")
            return
        logger.info("KILLING engine (SIGKILL/9)")
        self.process.kill()
        self.process.wait()
        self._close_logs()
        self.process = None

    def is_alive(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def read_stdout(self) -> str:
        if not self._stdout_path or not self._stdout_path.exists():
            return ""
        return self._stdout_path.read_text(errors="replace")

    def read_stderr(self) -> str:
        if not self._stderr_path or not self._stderr_path.exists():
            return ""
        return self._stderr_path.read_text(errors="replace")

    def _close_logs(self) -> None:
        if self.stdout_log:
            self.stdout_log.close()
            self.stdout_log = None
        if self.stderr_log:
            self.stderr_log.close()
            self.stderr_log = None
