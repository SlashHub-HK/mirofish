"""Resource guard — keep one simulation from taking down a shared host.

MiroFish runs on a **shared 24 GB host**. A simulation subprocess holds a full
OASIS agent stack (plus, for Twitter, a lazily loaded TWHIN-BERT model), and the
in-process prepare/report tasks build their own agent stacks too. Nothing
previously bounded how much of that could run at once, so concurrent work — or a
single run that overran the box — ended in the kernel OOM-killer taking out the
whole container rather than one request failing.

Two cheap, honest controls:

* `assert_memory_available()` — refuse to start heavy work when the host is
  already short of memory, with a message that says so, instead of dying later.
* `heavy_stage()` — a process-wide gate so only `MAX_CONCURRENT_HEAVY_STAGES`
  heavy stages run at once, and a cap on concurrently *running* simulations
  (`MAX_CONCURRENT_SIMULATIONS`, default 1 — the host cannot hold two).

Both fail OPEN when the value cannot be determined: refusing work because
`/proc/meminfo` was unreadable would be worse than trying.
"""
from __future__ import annotations

import os
import threading
import time
from contextlib import contextmanager
from typing import Iterator, Optional

from ..config import Config
from ..utils.logger import get_logger

logger = get_logger('mirofish.resource_guard')

# `MemAvailable` is the kernel's own estimate of what a new workload can claim
# without swapping. `MemFree` is misleadingly small because of page cache, so we
# deliberately do not use it.
_MEMINFO_PATH = '/proc/meminfo'
# Containers cap memory with a cgroup, and the host's MemAvailable does NOT
# reflect that limit — the same oversight that makes Kuzu's own default unsafe.
# cgroup v2 first (what modern Docker / Railway use), then v1.
_CGROUP_V2_MAX = '/sys/fs/cgroup/memory.max'
_CGROUP_V2_CURRENT = '/sys/fs/cgroup/memory.current'
_CGROUP_V1_MAX = '/sys/fs/cgroup/memory/memory.limit_in_bytes'
_CGROUP_V1_CURRENT = '/sys/fs/cgroup/memory/memory.usage_in_bytes'
# cgroup v1 reports "unlimited" as a sentinel value near 2**63.
_CGROUP_UNLIMITED = 1 << 62


def _read_int(path: str) -> Optional[int]:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            raw = f.read().strip()
    except OSError:
        return None
    if not raw or raw == 'max':
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def cgroup_headroom_mb() -> Optional[int]:
    """Memory headroom inside the container's own limit, or None if unlimited.

    On a shared host this is the figure that actually matters: the kernel kills
    us at the cgroup limit no matter how much the host has free.
    """
    for max_path, current_path in (
        (_CGROUP_V2_MAX, _CGROUP_V2_CURRENT),
        (_CGROUP_V1_MAX, _CGROUP_V1_CURRENT),
    ):
        limit = _read_int(max_path)
        if limit is None or limit <= 0 or limit > _CGROUP_UNLIMITED:
            continue
        used = _read_int(current_path) or 0
        return max(0, (limit - used) // (1024 * 1024))
    return None


def available_memory_mb() -> Optional[int]:
    """Best-effort memory available to THIS process in MB, or None if unknown.

    The smaller of the host's `MemAvailable` and the container's cgroup
    headroom. Reading only the host figure — which is what this did first —
    makes the preflight useless on a shared Railway host: it would happily start
    a run sized against the whole machine.
    """
    host = _host_available_mb()
    cgroup = cgroup_headroom_mb()
    if host is None:
        return cgroup
    if cgroup is None:
        return host
    return min(host, cgroup)


def _host_available_mb() -> Optional[int]:
    """Host-wide `MemAvailable` in MB, or None when it cannot be read."""
    try:
        with open(_MEMINFO_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                if line.startswith('MemAvailable:'):
                    return int(line.split()[1]) // 1024
    except (OSError, ValueError, IndexError):
        pass
    # Non-Linux (local dev on macOS): no MemAvailable. Report unknown rather
    # than guess — the guard then fails open, which is right for a laptop but
    # means it is inert, so say so once instead of silently doing nothing.
    _warn_unknown_once()
    return None


_unknown_warned = False


def _warn_unknown_once() -> None:
    global _unknown_warned
    if _unknown_warned:
        return
    _unknown_warned = True
    logger.warning(
        '%s not readable — the memory preflight is INACTIVE on this host '
        '(expected on macOS; production runs Linux).',
        _MEMINFO_PATH,
    )


class ResourceError(RuntimeError):
    """Raised when a stage is refused because the host has no headroom."""


def assert_memory_available(stage: str) -> None:
    """Refuse `stage` when free memory is below `MIROFISH_MIN_FREE_MB`.

    Returns silently when the figure is unavailable (fail open) so an unusual
    platform cannot wedge the service.
    """
    needed = int(getattr(Config, 'MIROFISH_MIN_FREE_MB', 0) or 0)
    if needed <= 0:
        return
    available = available_memory_mb()
    if available is None:
        return
    if available < needed:
        raise ResourceError(
            f'Not enough memory to start {stage}: {available} MB available, '
            f'{needed} MB required. Another simulation is likely still holding '
            f'memory — wait for it to finish or raise the host size.'
        )


# ── Process-wide gate for the heavy stages ──────────────────────────
_HEAVY_LOCK = threading.Lock()
_heavy_active = 0
_heavy_waiting = 0
_HEAVY_COND = threading.Condition(_HEAVY_LOCK)


def _heavy_limit() -> int:
    return max(1, int(getattr(Config, 'MAX_CONCURRENT_HEAVY_STAGES', 1) or 1))


def _heavy_wait_seconds() -> float:
    try:
        return max(0.0, float(getattr(Config, 'MAX_HEAVY_STAGE_WAIT_SECONDS', 180) or 0))
    except (TypeError, ValueError):
        return 180.0


@contextmanager
def heavy_stage(stage: str) -> Iterator[None]:
    """Serialise the memory-heavy stages across this process.

    Waits a BOUNDED time for a slot, then refuses. An unbounded wait looks
    friendlier but is worse under load: on a multi-user host the waiters pile up
    holding their request threads and their partially-built state, so one slow
    run converts into an outage. Failing fast with a retryable 503 lets the
    caller back off instead — and SlashMarketer already treats 503 as transient,
    so it retries on its own.
    """
    global _heavy_active, _heavy_waiting
    limit = _heavy_limit()
    wait_for = _heavy_wait_seconds()
    deadline = time.monotonic() + wait_for
    with _HEAVY_COND:
        if _heavy_active >= limit:
            _heavy_waiting += 1
            try:
                while _heavy_active >= limit:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise ResourceError(
                            f'The simulation engine is busy: another {stage} stage '
                            f'is still running (limit {limit}). Please retry in a '
                            f'moment.'
                        )
                    _HEAVY_COND.wait(timeout=min(1.0, remaining))
            finally:
                _heavy_waiting -= 1
        _heavy_active += 1
    try:
        yield
    finally:
        with _HEAVY_COND:
            _heavy_active -= 1
            _HEAVY_COND.notify_all()


def heavy_stages_active() -> int:
    """Current in-flight heavy stages (for /health and diagnostics)."""
    with _HEAVY_LOCK:
        return _heavy_active


def heavy_stages_waiting() -> int:
    """Callers queued for a heavy-stage slot — the backlog signal."""
    with _HEAVY_LOCK:
        return _heavy_waiting


def assert_simulation_slot(running: int) -> None:
    """Refuse a new simulation when one is already running.

    Counted by the caller from its own process registry (truth), rather than by
    trusting a gauge, so the check cannot drift.
    """
    cap = max(1, int(getattr(Config, 'MAX_CONCURRENT_SIMULATIONS', 1) or 1))
    if running >= cap:
        raise ResourceError(
            f'The simulation engine is already running {running} simulation(s) '
            f'(limit {cap}). This host cannot hold another — try again once the '
            f'current run finishes.'
        )
