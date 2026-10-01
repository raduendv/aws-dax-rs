"""Offline harness tests: every AWS, compiler, and firewall command is replaced."""

import asyncio
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import types
import unittest
from unittest.mock import AsyncMock, patch


ROOT = Path(__file__).resolve().parents[2]
MARKERS = {
    "go": "Go", "dotnet": ".NET", "java": "Java", "kotlin": "Kotlin",
    "python": "Python", "javascript": "JavaScript",
}

# Executed only inside a private temporary fake checkout, never against AWS.
STUB = r'''
import json
import os
from pathlib import Path
import sys
import time

root = Path(os.environ["FAKE_ROOT"])
mode = os.environ.get("FAKE_MODE", "")
tool = Path(sys.argv[0]).name
def event(name, **data):
    with (root / "events").open("a") as stream:
        stream.write(json.dumps(dict(event=name, **data)) + "\n")
def rules():
    return json.loads((root / "rules").read_text())
def consumer(language):
    event("launch", language=language, pid=os.getpid())
    if mode == "early_exit":
        sys.exit(7)
    if mode == "ready_timeout":
        time.sleep(30)
    ready = Path(os.environ["DAX_FAULT_READY_FILE"])
    release = Path(os.environ["DAX_FAULT_RELEASE_FILE"])
    assert not rules(), "firewall inserted before client prewarm"
    ready.touch(exist_ok=False)
    event("ready", directory=str(ready.parent))
    while not release.exists():
        time.sleep(0.01)
    assert len(rules()) == int(os.environ.get("DAX_BLOCK_COUNT", "1"))
    event("released")
    if mode in ("workload_timeout", "signal"):
        time.sleep(30)
    if mode == "lost_rule":
        (root / "rules").write_text("[]")
    if mode != "missing_marker":
        if language == "transaction":
            print("DAX_FAULT_RESULT workload=transaction operation=TransactGetItems success=1")
        else:
            label = dict(go="Go", dotnet=".NET", java="Java", kotlin="Kotlin",
                         python="Python", javascript="JavaScript")[language]
            print(label + " FFI direct, batch, transaction, paginator, route, and lifecycle operations passed")
    event("completed")
    sys.exit(9 if mode == "nonzero" else 0)

if tool == "setsid":
    os.setsid()
    os.execvp(sys.argv[1], sys.argv[1:])
elif tool == "sudo":
    assert sys.argv[1:3] == ["-n", "iptables"]
    args = sys.argv[3:]
    action = args[0]
    rule = args[1:]
    current = rules()
    event(action, rule=rule)
    if action == "-I":
        if mode == "insert_failure" and len(current) == 1:
            sys.exit(1)
        current.append(rule)
    elif action == "-D":
        if rule not in current:
            sys.exit(1)
        current.remove(rule)
    elif action == "-C":
        sys.exit(0 if rule in current else 1)
    else:
        raise AssertionError(args)
    (root / "rules").write_text(json.dumps(current))
elif tool == "cargo":
    if "exercises_live_endpoint_discovery_and_route_health" in sys.argv:
        event("discovery")
        if mode == "discovery_failure":
            sys.exit(1)
        if mode != "no_routes":
            for ip, port in (("10.0.0.1", 8111), ("10.0.0.1", 9111), ("10.0.0.2", 8111)):
                print(f"DAX_ROUTE node=test address={ip}:{port} healthy=true")
    elif os.environ.get("DAX_FAULT_PREWARM") == "1":
        consumer("transaction")
    else:
        workload = os.environ["DAX_FAULT_WORKLOAD"]
        assert rules()
        event("rust", workload=workload)
        print(f"DAX_FAULT_RESULT workload={workload} operation=GetItem success=1"
              if workload != "transaction" else
              "DAX_FAULT_RESULT workload=transaction operation=TransactGetItems success=0")
elif tool == "consumer":
    if sys.argv[1] == "build":
        event("build", language=sys.argv[2])
    else:
        consumer(sys.argv[2])
else:
    raise AssertionError(tool)
'''


class FaultRunnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="dax-fault-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "scripts").mkdir()
        (self.root / "bin").mkdir()
        (self.root / "tmp").mkdir()
        (self.root / "rules").write_text("[]")
        self.script = self.root / "scripts/live_fault_injection.sh"
        shutil.copyfile(ROOT / "scripts/live_fault_injection.sh", self.script)
        for tool in ("sudo", "cargo", "setsid", "consumer"):
            path = self.root / "bin" / tool
            path.write_text(f"#!{sys.executable}\n" + STUB)
            path.chmod(0o700)
        (self.root / "scripts/live_ffi_consumer.sh").write_text(
            '#!/usr/bin/env bash\nexec consumer "$@"\n'
        )
        self.env = {
            **{key: value for key, value in os.environ.items() if not key.startswith("DAX_")},
            "PATH": f"{self.root / 'bin'}:{os.environ['PATH']}",
            "TMPDIR": str(self.root / "tmp"),
            "FAKE_ROOT": str(self.root),
            "DAX_FAULT_INJECTION": "1",
            "DAX_ENDPOINT": "dax://offline.invalid:8111",
            "DAX_FAULT_READY_TIMEOUT_SECONDS": "3",
            "DAX_FAULT_WORKLOAD_TIMEOUT_SECONDS": "3",
        }

    def events(self):
        path = self.root / "events"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def run_harness(self, args=("--consumer", "python"), **env):
        result = subprocess.run(
            ["bash", str(self.script), *args], env={**self.env, **env},
            text=True, capture_output=True, timeout=20,
        )
        self.assertEqual(json.loads((self.root / "rules").read_text()), [], result.stderr)
        self.assertEqual(list((self.root / "tmp").iterdir()), [], result.stderr)
        for event in self.events():
            if event["event"] == "launch":
                with self.assertRaises(ProcessLookupError):
                    os.kill(event["pid"], 0)
        return result

    def test_syntax(self):
        subprocess.run(["bash", "-n", str(ROOT / "scripts/live_fault_injection.sh"),
                        str(ROOT / "scripts/live_ffi_consumer.sh")], check=True)

    def test_guards_have_no_side_effects(self):
        cases = [
            (("--consumer", "python"), {"DAX_FAULT_INJECTION": ""}),
            (("--consumer", "python; touch unexpected"), {}),
            (("--consumer", "python", "extra"), {}),
            (("--consumer", "python"), {"DAX_BLOCK_COUNT": "0"}),
            (("--consumer", "python"), {"DAX_BLOCK_COUNT": "3"}),
            (("--consumer", "python"), {"DAX_FAULT_READY_TIMEOUT_SECONDS": "0"}),
            (("--consumer", "python"), {"DAX_FAULT_WORKLOAD_TIMEOUT_SECONDS": "x"}),
            (("--consumer", "python"), {"DAX_ENDPOINT": ""}),
        ]
        for args, env in cases:
            with self.subTest(args=args, env=env):
                self.assertNotEqual(self.run_harness(args, **env).returncode, 0)
                self.assertEqual(self.events(), [])
        for args in (["run", "python; touch unexpected"], ["build", "ruby"], ["bogus", "go"]):
            result = subprocess.run(["bash", str(ROOT / "scripts/live_ffi_consumer.sh"), *args],
                                    text=True, capture_output=True)
            self.assertEqual(result.returncode, 2)

    def test_all_language_dispatch_and_ordering(self):
        for language in MARKERS:
            with self.subTest(language=language):
                (self.root / "events").write_text("")
                result = self.run_harness(("--consumer", language))
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                events = [event["event"] for event in self.events()]
                self.assertEqual(events, [
                    "build", "discovery", "launch", "ready", "-I", "-C",
                    "released", "completed", "-C", "-D", "-C",
                ])
                self.assertIn("DAX_FAULT_CLEANUP status=ok count=1", result.stdout)

    def test_two_distinct_routes(self):
        result = self.run_harness(DAX_BLOCK_COUNT="2")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        inserted = [event["rule"][2] for event in self.events() if event["event"] == "-I"]
        self.assertEqual(inserted, ["10.0.0.1", "10.0.0.2"])

    def test_failures_cleanup_and_do_not_claim_success(self):
        for mode in ("early_exit", "missing_marker", "nonzero", "ready_timeout",
                     "workload_timeout", "discovery_failure", "no_routes", "lost_rule",
                     "insert_failure"):
            with self.subTest(mode=mode):
                (self.root / "events").write_text("")
                result = self.run_harness(
                    FAKE_MODE=mode, DAX_BLOCK_COUNT="2" if mode == "insert_failure" else "1",
                    DAX_FAULT_READY_TIMEOUT_SECONDS="1",
                    DAX_FAULT_WORKLOAD_TIMEOUT_SECONDS="1",
                )
                self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertNotIn("DAX_FAULT_SUMMARY status=ok", result.stdout)
                if mode in ("early_exit", "ready_timeout", "discovery_failure", "no_routes"):
                    self.assertNotIn("-I", [event["event"] for event in self.events()])
                if mode == "insert_failure":
                    self.assertNotIn("released", [event["event"] for event in self.events()])

    def test_term_removes_rules_and_stops_child(self):
        process = subprocess.Popen(["bash", str(self.script), "--consumer", "python"],
                                   env={**self.env, "FAKE_MODE": "signal"},
                                   text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            deadline = time.monotonic() + 10
            while "released" not in [event["event"] for event in self.events()]:
                self.assertLess(time.monotonic(), deadline)
                time.sleep(0.02)
            process.send_signal(signal.SIGTERM)
            stdout, stderr = process.communicate(timeout=10)
            self.assertEqual(process.returncode, 143, stdout + stderr)
            self.assertEqual(json.loads((self.root / "rules").read_text()), [])
            self.assertEqual(list((self.root / "tmp").iterdir()), [])
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate()

    def test_rust_modes_preserved(self):
        for prewarm in ("", "1"):
            with self.subTest(prewarm=prewarm):
                (self.root / "events").write_text("")
                result = self.run_harness((), DAX_FAULT_PREWARM=prewarm)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                if not prewarm:
                    self.assertEqual([event["workload"] for event in self.events()
                                      if event["event"] == "rust"], ["read", "write", "transaction"])


class PythonBarrierTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        path = ROOT / "src/aws-dax-ffi/tests/consumers/python/live_direct_operations.py"
        spec = importlib.util.spec_from_file_location("live_consumer", path)
        self.module = importlib.util.module_from_spec(spec)
        modules = {
            "aws_dax_ffi": types.ModuleType("aws_dax_ffi"),
            "boto3": types.ModuleType("boto3"),
        }
        with patch.dict(sys.modules, modules):
            spec.loader.exec_module(self.module)
        self.temp = tempfile.TemporaryDirectory(prefix="dax-barrier-test-")
        self.addCleanup(self.temp.cleanup)
        self.ready = Path(self.temp.name) / "ready"
        self.release = Path(self.temp.name) / "release"
        self.client = types.SimpleNamespace(
            discover_endpoints=AsyncMock(return_value=[object()]),
            refresh_endpoints=AsyncMock(),
            route_snapshot=lambda: [object()],
        )
        env = patch.dict(os.environ, {"DAX_FAULT_READY_FILE": str(self.ready),
                                     "DAX_FAULT_RELEASE_FILE": str(self.release)})
        env.start()
        self.addCleanup(env.stop)

    async def test_disabled_unless_both_paths_set(self):
        for variable in ("DAX_FAULT_READY_FILE", "DAX_FAULT_RELEASE_FILE"):
            with patch.dict(os.environ, {variable: ""}):
                await self.module.fault_barrier(self.client)
        self.client.discover_endpoints.assert_not_called()
        self.assertFalse(self.ready.exists())

    async def test_prewarm_ready_then_release(self):
        async def refresh():
            self.assertFalse(self.ready.exists())
        self.client.refresh_endpoints.side_effect = refresh
        task = asyncio.create_task(self.module.fault_barrier(self.client))
        try:
            for _ in range(100):
                if self.ready.exists():
                    break
                await asyncio.sleep(0.01)
            self.assertTrue(self.ready.exists())
            self.assertFalse(task.done())
            self.client.refresh_endpoints.assert_awaited_once()
            self.release.touch()
            await asyncio.wait_for(task, 1)
        finally:
            if not task.done():
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task

    async def test_timeout_is_explicit(self):
        with patch.object(self.module.time, "monotonic", side_effect=[0, 121]):
            with self.assertRaisesRegex(TimeoutError, "DAX_FAULT_RELEASE_FILE"):
                await self.module.fault_barrier(self.client)
        self.assertTrue(self.ready.is_file())

    async def test_empty_routes_do_not_signal_ready(self):
        self.client.route_snapshot = lambda: []
        with self.assertRaisesRegex(RuntimeError, "no routes"):
            await self.module.fault_barrier(self.client)
        self.assertFalse(self.ready.exists())

    async def test_ready_is_exclusive_and_io_errors_propagate(self):
        self.ready.write_text("existing")
        with self.assertRaises(FileExistsError):
            await self.module.fault_barrier(self.client)
        self.assertEqual(self.ready.read_text(), "existing")
        self.ready.unlink()
        with patch.object(self.module.os, "stat", side_effect=PermissionError("denied")):
            with self.assertRaises(PermissionError):
                await self.module.fault_barrier(self.client)


if __name__ == "__main__":
    unittest.main()
