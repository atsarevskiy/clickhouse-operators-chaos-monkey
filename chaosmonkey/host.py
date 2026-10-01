"""Health of the machine the clusters run on.

Every k3d node shares one host. When the host runs out of CPU or memory, API calls time out,
probes fail and the kernel kills containers, and a scenario then measures the host instead of
the operator. One sampler per process records load, available memory and the kernel's OOM kill
counter; the runner checks a scenario's window against it and refuses to score an overloaded one.
The OOM counter also counts a container killed at its own memory limit, which is a property of
the scenario rather than of the host, so it is recorded but only load and free memory decide.
"""

from __future__ import annotations

import os
import threading
import time

#: 1-minute load average per CPU above which a window is overloaded
MAX_LOAD_PER_CPU = 2.0
#: available memory below which a window is overloaded
MIN_AVAILABLE_GB = 3.0
#: a scenario starts only below these, so it doesn't begin on a host that is already struggling
START_LOAD_PER_CPU = 1.2
START_AVAILABLE_GB = 6.0


def _read() -> tuple[float, float, int]:
    load1 = float(open("/proc/loadavg").read().split()[0]) / (os.cpu_count() or 1)
    avail_kb = next(int(line.split()[1]) for line in open("/proc/meminfo") if line.startswith("MemAvailable:"))
    oom = next((int(line.split()[1]) for line in open("/proc/vmstat") if line.startswith("oom_kill ")), 0)
    return load1, avail_kb / 1024 / 1024, oom


class HostMonitor:
    def __init__(self, interval: float = 2.0):
        self.interval = interval
        self.samples: list[tuple[float, float, float, int]] = []
        self.lock = threading.Lock()
        threading.Thread(target=self._loop, name="host-monitor", daemon=True).start()

    def _loop(self) -> None:
        while True:
            try:
                load, avail, oom = _read()
                with self.lock:
                    self.samples.append((time.time(), load, avail, oom))
                    del self.samples[:-20000]
            except OSError:
                pass
            time.sleep(self.interval)

    def window(self, start: float, end: float) -> dict:
        with self.lock:
            rows = [s for s in self.samples if start <= s[0] <= end]
        if not rows:
            return {}
        return {"max_load_per_cpu": max(r[1] for r in rows), "min_available_gb": min(r[2] for r in rows),
                "oom_kills": rows[-1][3] - rows[0][3]}

    def overload(self, start: float, end: float) -> str | None:
        """Why the window can't be scored, or None if the host kept up."""
        w = self.window(start, end)
        reasons = []
        if w.get("max_load_per_cpu", 0) > MAX_LOAD_PER_CPU:
            reasons.append(f"load reached {w['max_load_per_cpu']:.1f} per CPU")
        if w.get("min_available_gb", 99) < MIN_AVAILABLE_GB:
            reasons.append(f"available memory fell to {w['min_available_gb']:.1f} GB")
        return "; ".join(reasons) or None

    def wait_for_headroom(self, timeout: float = 900) -> float:
        """Block until the host has room to start a scenario; returns seconds waited."""
        start = time.time()
        while time.time() - start < timeout:
            load, avail, _ = _read()
            if load < START_LOAD_PER_CPU and avail > START_AVAILABLE_GB:
                break
            time.sleep(5)
        return time.time() - start


_monitor: HostMonitor | None = None
_monitor_lock = threading.Lock()


def monitor() -> HostMonitor:
    global _monitor
    with _monitor_lock:
        if _monitor is None:
            _monitor = HostMonitor()
        return _monitor
