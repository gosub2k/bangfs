"""
Shared test infrastructure for BangFS test suites.

Provides colors, config defaults, shell helpers, and the BangFSSetup class
for creating/mounting/tearing down BangFS filesystems.
"""

import os
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Optional

# Colors
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"

# Configuration defaults
DEFAULT_RIAK_HOST = "172.17.0.2"
DEFAULT_RIAK_PORT = "8087"
DEFAULT_NAMESPACE = "foobar"
DEFAULT_MOUNTPOINT = os.path.join(tempfile.gettempdir(), "bangfs")
TMPDIR = tempfile.gettempdir()
PROJECT_ROOT = Path(__file__).parent.parent


def run_command(cmd: str, timeout: int = 30) -> tuple[bool, str, str]:
    """Run a shell command, return (success, stdout, stderr)."""
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=timeout
        )
        return (result.returncode == 0, result.stdout.strip(), result.stderr.strip())
    except subprocess.TimeoutExpired:
        return (False, "", "TIMEOUT")
    except Exception as e:
        return (False, "", str(e))


def go_run(cmd: str, args: list[str]) -> subprocess.CompletedProcess:
    """Run a Go command using 'go run'."""
    return subprocess.run(
        ["go", "run", f"./cmd/{cmd}", *args],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True
    )


def log_info(msg: str):
    print(f"{GREEN}[INFO]{RESET} {msg}")


def log_warn(msg: str):
    print(f"{YELLOW}[WARN]{RESET} {msg}")


def log_error(msg: str):
    print(f"{RED}[ERROR]{RESET} {msg}")


class BangFSSetup:
    """Handles setup and teardown of a BangFS mount."""

    def __init__(self, host: str, port: str, namespace: str, mountpoint: str,
                 dummy: bool = False, trace_log: Optional[str] = None):
        self.host = host
        self.port = port
        self.namespace = namespace
        self.mountpoint = mountpoint
        self.dummy = dummy
        self.trace_log = trace_log

    def backend_args(self) -> list[str]:
        """Return backend flags: either -dummy or -host/-port."""
        if self.dummy:
            return ["-dummy"]
        return ["-host", self.host, "-port", self.port]

    def is_mounted(self) -> bool:
        """Check if mountpoint is currently mounted."""
        try:
            result = subprocess.run(
                ["mountpoint", "-q", self.mountpoint],
                capture_output=True
            )
            return result.returncode == 0
        except FileNotFoundError:
            try:
                with open("/proc/mounts") as f:
                    return any(self.mountpoint in line for line in f)
            except:
                return False

    def unmount(self):
        """Unmount the filesystem if mounted."""
        if self.is_mounted():
            log_info(f"Unmounting {self.mountpoint}...")
            result = subprocess.run(
                ["fusermount", "-u", self.mountpoint],
                capture_output=True
            )
            if result.returncode != 0:
                subprocess.run(["umount", self.mountpoint], capture_output=True)
            time.sleep(1)

    def cleanup_mountpoint(self):
        """Remove the mountpoint directory."""
        if os.path.isdir(self.mountpoint):
            try:
                os.rmdir(self.mountpoint)
            except OSError:
                pass

    def wipe_filesystem(self):
        """Wipe existing filesystem from backend."""
        log_info(f"Wiping existing filesystem (namespace={self.namespace})...")
        result = go_run("reformat-bangfs", [
            *self.backend_args(),
            "-namespace", self.namespace,
            "-force"
        ])
        if result.returncode != 0:
            log_warn("No existing filesystem to wipe (or wipe failed)")
        for line in result.stderr.split('\n'):
            if line != "":
                log_info(f"{DIM}l{line}{RESET}")

    def create_filesystem(self):
        """Create a new filesystem in the backend."""
        log_info(f"Creating filesystem (namespace={self.namespace})...")
        result = go_run("mkfs-bangfs", [
            *self.backend_args(),
            "-namespace", self.namespace
        ])
        if result.returncode != 0:
            log_error(f"Failed to create filesystem: {result.stderr}")
            raise RuntimeError("Failed to create filesystem")
        log_info("Filesystem created")

    def mount_filesystem(self):
        """Mount the filesystem in daemon mode."""
        log_info(f"Creating mountpoint {self.mountpoint}...")
        os.makedirs(self.mountpoint, exist_ok=True)

        log_info("Mounting filesystem in daemon mode...")
        mount_args = [
            *self.backend_args(),
            "-namespace", self.namespace,
            "-mount", self.mountpoint,
            "-daemon",
        ]
        if self.trace_log:
            mount_args.extend(["-trace", "-tracelog", self.trace_log])

        result = go_run("mount-fuse-bangfs", mount_args)
        if result.returncode != 0:
            log_error(f"Mount failed: {result.stderr}")
            raise RuntimeError("Mount failed")

        time.sleep(2)

        if not self.is_mounted():
            log_error("Mount failed - filesystem not mounted")
            raise RuntimeError("Mount verification failed")

        log_info(f"Filesystem mounted at {self.mountpoint}")

    def setup(self):
        """Full setup: create, mount."""
        self.create_filesystem()
        self.mount_filesystem()

    def teardown(self):
        """Full teardown: unmount, wipe."""
        log_info("Tearing down...")
        self.unmount()
        self.wipe_filesystem()
        log_info("Teardown complete")
