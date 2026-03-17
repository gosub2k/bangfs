#!/usr/bin/env python3
"""
BangFS Multi-Client Test Suite

Mounts the same BangFS namespace at two mountpoints and tests that
writes from one client are visible to the other within a reasonable
window (eventual consistency).

Usage:
    python3 test_multiclient.py                # Against Riak
    python3 test_multiclient.py --dummy        # Against file-backed store

Configuration (via environment or defaults):
    RIAK_HOST=172.17.0.2
    RIAK_PORT=8087
    BANGFS_NAMESPACE=multitest
"""

import argparse
import os
import signal
import sys
import time

from bangfs_test_common import (
    RED, GREEN, YELLOW, BLUE, RESET, BOLD,
    DEFAULT_RIAK_HOST, DEFAULT_RIAK_PORT, TMPDIR,
    run_command, log_info, log_error,
    BangFSSetup,
)

DEFAULT_NAMESPACE = "multitest"

# How long to wait for eventual consistency (seconds)
CONSISTENCY_TIMEOUT = 15
POLL_INTERVAL = 0.5


def wait_for_visibility(path, check_fn, timeout=CONSISTENCY_TIMEOUT):
    """Poll until check_fn(path) returns True, or timeout.

    Returns (passed, elapsed_seconds).
    """
    start = time.time()
    while time.time() - start < timeout:
        if check_fn(path):
            return True, time.time() - start
        time.sleep(POLL_INTERVAL)
    return False, timeout


class MultiClientTest:
    def __init__(self, host, port, namespace, dummy=False):
        self.dummy = dummy
        self.namespace = namespace
        self.mount_a_path = os.path.join(TMPDIR, "bangfs_multi_a")
        self.mount_b_path = os.path.join(TMPDIR, "bangfs_multi_b")
        # One setup handles mkfs/wipe, both handle mount/unmount
        self.setup_a = BangFSSetup(host, port, namespace, self.mount_a_path, dummy=dummy)
        self.setup_b = BangFSSetup(host, port, namespace, self.mount_b_path, dummy=dummy)
        self.passed = 0
        self.failed = 0

    def setup(self):
        """Create filesystem and mount at both points."""
        self.setup_a.wipe_filesystem()
        self.setup_a.create_filesystem()
        self.setup_a.mount_filesystem()
        self.setup_b.mount_filesystem()
        log_info("Both clients mounted.")

    def teardown(self):
        log_info("Tearing down...")
        self.setup_b.unmount()
        self.setup_a.unmount()
        self.setup_a.wipe_filesystem()
        log_info("Teardown complete.")

    def _result(self, passed, desc, detail="", elapsed=None):
        elapsed_str = f" ({elapsed:.1f}s)" if elapsed is not None else ""
        if passed:
            print(f"  {GREEN}PASS{RESET} {desc}{elapsed_str}")
            self.passed += 1
        else:
            print(f"  {RED}FAIL{RESET} {desc}{elapsed_str}")
            if detail:
                print(f"       {RED}{detail}{RESET}")
            self.failed += 1
        return passed

    @property
    def mount_a(self):
        return self.mount_a_path

    @property
    def mount_b(self):
        return self.mount_b_path

    def test_file_visibility(self):
        """Write a file on A, read it on B."""
        print(f"\n{BLUE}{BOLD}--- File Visibility (A writes, B reads) ---{RESET}")

        ok, _, err = run_command(f"echo -n 'hello from A' > '{self.mount_a}/crossfile.txt'")
        self._result(ok, "write file on client A", err)
        if not ok:
            return

        passed, elapsed = wait_for_visibility(
            f"{self.mount_b}/crossfile.txt",
            lambda p: os.path.exists(p),
        )
        self._result(passed, "file appears on client B", f"not visible after {CONSISTENCY_TIMEOUT}s", elapsed)
        if not passed:
            run_command(f"rm -f '{self.mount_a}/crossfile.txt'")
            return

        passed, elapsed = wait_for_visibility(
            f"{self.mount_b}/crossfile.txt",
            lambda p: run_command(f"cat '{p}'")[1] == "hello from A",
        )
        self._result(passed, "content matches on client B", f"got: {run_command(f'cat {self.mount_b}/crossfile.txt')[1]!r}", elapsed)

        run_command(f"rm '{self.mount_a}/crossfile.txt'")

    def test_dir_visibility(self):
        """Create a dir on A, see it on B."""
        print(f"\n{BLUE}{BOLD}--- Directory Visibility (A creates, B sees) ---{RESET}")

        ok, _, err = run_command(f"mkdir '{self.mount_a}/crossdir'")
        self._result(ok, "mkdir on client A", err)
        if not ok:
            return

        passed, elapsed = wait_for_visibility(
            f"{self.mount_b}/crossdir",
            lambda p: os.path.isdir(p),
        )
        self._result(passed, "directory appears on client B", f"not visible after {CONSISTENCY_TIMEOUT}s", elapsed)

        run_command(f"rmdir '{self.mount_a}/crossdir'")

    def test_write_on_b_read_on_a(self):
        """Write on B, read on A (reverse direction)."""
        print(f"\n{BLUE}{BOLD}--- Reverse Direction (B writes, A reads) ---{RESET}")

        ok, _, err = run_command(f"echo -n 'hello from B' > '{self.mount_b}/reverse.txt'")
        self._result(ok, "write file on client B", err)
        if not ok:
            return

        passed, elapsed = wait_for_visibility(
            f"{self.mount_a}/reverse.txt",
            lambda p: run_command(f"cat '{p}'")[1] == "hello from B",
        )
        self._result(passed, "content visible on client A", "", elapsed)

        run_command(f"rm '{self.mount_b}/reverse.txt'")

    def test_large_file_visibility(self):
        """Write a multi-chunk file on A, verify on B."""
        print(f"\n{BLUE}{BOLD}--- Large File Visibility (multi-chunk) ---{RESET}")

        ok, _, err = run_command(
            f"dd if=/dev/zero bs=1 count=30720 2>/dev/null | tr '\\0' 'X' > '{self.mount_a}/bigfile.bin'"
        )
        self._result(ok, "write 30KB file on client A", err)
        if not ok:
            return

        def size_matches(p):
            ok, out, _ = run_command(f"stat -c '%s' '{p}'")
            return ok and out == "30720"

        passed, elapsed = wait_for_visibility(
            f"{self.mount_b}/bigfile.bin",
            size_matches,
        )
        self._result(passed, "30KB file has correct size on client B", "", elapsed)
        if not passed:
            run_command(f"rm -f '{self.mount_a}/bigfile.bin'")
            return

        ok, out_a, _ = run_command(f"md5sum '{self.mount_a}/bigfile.bin'")
        ok2, out_b, _ = run_command(f"md5sum '{self.mount_b}/bigfile.bin'")
        hash_a = out_a.split()[0] if ok else ""
        hash_b = out_b.split()[0] if ok2 else ""
        self._result(
            ok and ok2 and hash_a == hash_b,
            "content hash matches across clients",
            f"A={hash_a} B={hash_b}"
        )

        run_command(f"rm '{self.mount_a}/bigfile.bin'")

    def test_delete_visibility(self):
        """Delete on A, confirm gone on B."""
        print(f"\n{BLUE}{BOLD}--- Delete Visibility (A deletes, B sees removal) ---{RESET}")

        run_command(f"echo -n 'soon gone' > '{self.mount_a}/todelete.txt'")
        passed, _ = wait_for_visibility(
            f"{self.mount_b}/todelete.txt",
            lambda p: os.path.exists(p),
        )
        if not passed:
            self._result(False, "setup: file visible on B before delete")
            run_command(f"rm -f '{self.mount_a}/todelete.txt'")
            return

        ok, _, err = run_command(f"rm '{self.mount_a}/todelete.txt'")
        self._result(ok, "delete file on client A", err)
        if not ok:
            return

        passed, elapsed = wait_for_visibility(
            f"{self.mount_b}/todelete.txt",
            lambda p: not os.path.exists(p),
        )
        self._result(passed, "file disappears on client B", f"still exists after {CONSISTENCY_TIMEOUT}s", elapsed)

    def test_concurrent_writes_different_files(self):
        """Both clients write different files simultaneously."""
        print(f"\n{BLUE}{BOLD}--- Concurrent Writes (different files) ---{RESET}")

        ok_a, _, err_a = run_command(f"echo -n 'data-A' > '{self.mount_a}/from_a.txt'")
        ok_b, _, err_b = run_command(f"echo -n 'data-B' > '{self.mount_b}/from_b.txt'")
        self._result(ok_a, "client A writes from_a.txt", err_a)
        self._result(ok_b, "client B writes from_b.txt", err_b)
        if not (ok_a and ok_b):
            run_command(f"rm -f '{self.mount_a}/from_a.txt' '{self.mount_b}/from_b.txt'")
            return

        passed, elapsed = wait_for_visibility(
            f"{self.mount_a}/from_b.txt",
            lambda p: run_command(f"cat '{p}'")[1] == "data-B",
        )
        self._result(passed, "client A sees from_b.txt with correct content", "", elapsed)

        passed, elapsed = wait_for_visibility(
            f"{self.mount_b}/from_a.txt",
            lambda p: run_command(f"cat '{p}'")[1] == "data-A",
        )
        self._result(passed, "client B sees from_a.txt with correct content", "", elapsed)

        run_command(f"rm -f '{self.mount_a}/from_a.txt' '{self.mount_a}/from_b.txt'")

    def test_concurrent_writes_same_dir(self):
        """Both clients create files in the same directory."""
        print(f"\n{BLUE}{BOLD}--- Concurrent Writes (same directory) ---{RESET}")

        ok, _, err = run_command(f"mkdir '{self.mount_a}/shared'")
        self._result(ok, "create shared directory", err)
        if not ok:
            return

        wait_for_visibility(f"{self.mount_b}/shared", lambda p: os.path.isdir(p))

        ok_a, _, _ = run_command(f"echo -n 'A-content' > '{self.mount_a}/shared/a.txt'")
        ok_b, _, _ = run_command(f"echo -n 'B-content' > '{self.mount_b}/shared/b.txt'")
        self._result(ok_a, "client A writes shared/a.txt")
        self._result(ok_b, "client B writes shared/b.txt")

        passed_a, elapsed_a = wait_for_visibility(
            f"{self.mount_b}/shared/a.txt",
            lambda p: run_command(f"cat '{p}'")[1] == "A-content",
        )
        self._result(passed_a, "client B sees shared/a.txt", "", elapsed_a)

        passed_b, elapsed_b = wait_for_visibility(
            f"{self.mount_a}/shared/b.txt",
            lambda p: run_command(f"cat '{p}'")[1] == "B-content",
        )
        self._result(passed_b, "client A sees shared/b.txt", "", elapsed_b)

        def both_visible(mount):
            ok, out, _ = run_command(f"ls '{mount}/shared'")
            return ok and "a.txt" in out and "b.txt" in out

        passed, elapsed = wait_for_visibility(self.mount_a, lambda m: both_visible(m))
        self._result(passed, "ls on A shows both files", "", elapsed)

        passed, elapsed = wait_for_visibility(self.mount_b, lambda m: both_visible(m))
        self._result(passed, "ls on B shows both files", "", elapsed)

        run_command(f"rm -rf '{self.mount_a}/shared'")

    def run_all(self):
        print(f"{BOLD}BangFS Multi-Client Test Suite{RESET}")
        print(f"{'='*60}")
        if self.dummy:
            print(f"Backend:    file ({TMPDIR}/bangfs_{self.namespace}/)")
        else:
            print(f"Backend:    Riak ({self.setup_a.host}:{self.setup_a.port})")
        print(f"Namespace:  {self.namespace}")
        print(f"Client A:   {self.mount_a}")
        print(f"Client B:   {self.mount_b}")
        print(f"Consistency timeout: {CONSISTENCY_TIMEOUT}s")
        print(f"{'='*60}")

        self.setup()

        try:
            self.test_file_visibility()
            self.test_dir_visibility()
            self.test_write_on_b_read_on_a()
            self.test_large_file_visibility()
            self.test_delete_visibility()
            self.test_concurrent_writes_different_files()
            self.test_concurrent_writes_same_dir()
        finally:
            self.teardown()

        total = self.passed + self.failed
        print(f"\n{BOLD}{'='*60}{RESET}")
        print(f"{BOLD}RESULTS:{RESET} {GREEN}{self.passed} passed{RESET}, {RED}{self.failed} failed{RESET} / {total} total")
        if self.failed == 0:
            print(f"{GREEN}{BOLD}ALL TESTS PASSED!{RESET}")
        print(f"{BOLD}{'='*60}{RESET}")

        return self.failed == 0


def main():
    parser = argparse.ArgumentParser(description="BangFS Multi-Client Test Suite")
    parser.add_argument("--host", default=os.environ.get("RIAK_HOST", DEFAULT_RIAK_HOST))
    parser.add_argument("--port", default=os.environ.get("RIAK_PORT", DEFAULT_RIAK_PORT))
    parser.add_argument("--namespace", default=os.environ.get("BANGFS_NAMESPACE", DEFAULT_NAMESPACE))
    parser.add_argument("--dummy", action="store_true", help="Use file-backed store instead of Riak")
    args = parser.parse_args()

    test = MultiClientTest(args.host, args.port, args.namespace, dummy=args.dummy)

    def signal_handler(sig, frame):
        print(f"\n{YELLOW}Interrupted, cleaning up...{RESET}")
        test.teardown()
        sys.exit(1)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    success = test.run_all()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
