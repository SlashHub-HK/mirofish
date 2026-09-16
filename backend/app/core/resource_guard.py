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
from contextlib import contextmanager
from typing import Iterator, Optional

from ..config import Config
from ..utils.logger import get_logger

logger = get_logger('mirofish.resource_guard')

# `MemAvailable` is the kernel's own estimate of what a new workload can claim
# without swapping. `MemFree` is misleadingly small because of page cache, so we
# deliberately do not use it.
_MEMINFO_PATH = '/proc/meminfo'


def available_memory_mb() -> Optional[int]:
    """Best-effort available physical memory in MB, or None if unknown."""
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
_HEAVY_COND = threading.Condition(_HEAVY_LOCK)


def _heavy_limit() -> int:
    return max(1, int(getattr(Config, 'MAX_CONCURRENT_HEAVY_STAGES', 1) or 1))


@contextmanager
def heavy_stage(stage: str) -> Iterator[None]:
    """Serialise the memory-heavy stages across this process.

    Deliberately a *blocking* gate rather than a refusal: a second Verity run
    should wait its turn, not fail. The acquire has no timeout because the
    engine's own request timeouts bound the caller side.
    """
    global _heavy_active
    limit = _heavy_limit()
    with _HEAVY_COND:
        waited = 0
        while _heavy_active >= limit:
            _HEAVY_COND.wait(timeout=1.0)
            waited += 1
            if waited == 30:  # ~30s — say something rather than look hung
                logger.info(
                    f'{stage}: waiting for a heavy-stage slot '
                    f'({_heavy_active}/{limit} in use)'
                )
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
