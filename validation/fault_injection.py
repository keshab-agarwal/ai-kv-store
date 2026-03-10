"""Fault injection framework for correctness testing."""

import logging
import random
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger(__name__)


@dataclass
class FaultEvent:
    timestamp: float
    fault_type: str
    target_node: int
    description: str


class FaultInjector:
    def __init__(self, output_dir: Path, base_port: int = 9000, num_nodes: int = 3):
        self._output_dir = output_dir
        self._base_port = base_port
        self._num_nodes = num_nodes
        self._events: list[FaultEvent] = []
        self._node_pids: dict[int, int] = {}

    def register_node_pid(self, node_id: int, pid: int):
        self._node_pids[node_id] = pid

    def crash_node(self, node_id: int) -> bool:
        pid = self._node_pids.get(node_id)
        if pid is None:
            log.error("No PID registered for node %d", node_id)
            return False

        try:
            subprocess.run(["kill", "-9", str(pid)], check=True, timeout=5)
            event = FaultEvent(
                timestamp=time.time(),
                fault_type="crash",
                target_node=node_id,
                description=f"Hard-killed node {node_id} (pid={pid})",
            )
            self._events.append(event)
            log.info("Crashed node %d (pid=%d)", node_id, pid)
            return True
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            log.error("Failed to crash node %d: %s", node_id, e)
            return False

    def recover_node(self, node_id: int, start_cmd: list[str]) -> bool:
        try:
            proc = subprocess.Popen(
                start_cmd,
                cwd=str(self._output_dir),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            self._node_pids[node_id] = proc.pid
            event = FaultEvent(
                timestamp=time.time(),
                fault_type="recover",
                target_node=node_id,
                description=f"Recovered node {node_id} (new pid={proc.pid})",
            )
            self._events.append(event)
            log.info("Recovered node %d (pid=%d)", node_id, proc.pid)
            return True
        except Exception as e:
            log.error("Failed to recover node %d: %s", node_id, e)
            return False

    def schedule_crash_recover(
        self,
        node_id: int,
        start_cmd: list[str],
        crash_after_s: float = 5.0,
        recover_after_s: float = 10.0,
    ) -> list[FaultEvent]:
        time.sleep(crash_after_s)
        self.crash_node(node_id)
        time.sleep(recover_after_s)
        self.recover_node(node_id, start_cmd)
        return list(self._events)

    def pick_random_node(self, seed: int | None = None) -> int:
        rng = random.Random(seed)
        return rng.randint(0, self._num_nodes - 1)

    @property
    def events(self) -> list[FaultEvent]:
        return list(self._events)

    def summary(self) -> str:
        lines = [f"Fault injection: {len(self._events)} events"]
        for e in self._events:
            lines.append(f"  [{e.fault_type}] node={e.target_node} t={e.timestamp:.3f} {e.description}")
        return "\n".join(lines)
