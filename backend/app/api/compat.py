"""SlashMarketer compatibility layer.

SlashMarketer's Verify (System 2) proxy speaks a **simplified** ``/api/projects/*``
lifecycle:

    POST /api/projects                        → {project_id}
    POST /api/projects/<pid>/graph            → kick off graph build
    GET  /api/projects/<pid>/graph/status     → {status}
    POST /api/projects/<pid>/simulation/prepare
    GET  /api/projects/<pid>/simulation/prepare/status
    POST /api/projects/<pid>/simulation/run
    GET  /api/projects/<pid>/simulation/run/status
    POST /api/projects/<pid>/report/generate
    GET  /api/projects/<pid>/report/generate/status

MiroFish itself exposes the real pipeline under ``/api/graph``,
``/api/simulation`` and ``/api/report``. This blueprint maps one to the other
server-side (same process, loopback HTTP) so SlashMarketer stays unchanged.

The adapter owns a small state file (``backend/data/compat_projects.json``) that
remembers, per project, the upstream graph-build task, the simulation id, the
prepare task and the report task — the real API is task/id oriented, while
SlashMarketer is project oriented.

Statuses are normalised to the three values SlashMarketer understands:
``running`` | ``done`` | ``failed``.
"""
from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime
from typing import Any
from urllib.parse import urlencode

import httpx
from flask import jsonify, request

# Reuse the package-level blueprint (same pattern as graph/report/simulation);
# defining a *new* Blueprint here would attach routes to an object that is never
# registered by the app factory.
from . import compat_bp  # noqa: E402

# NOTE: auth is enforced globally in app/__init__.py (all /api/* require the
# MIROFISH_API_KEY bearer when configured), so the compat layer itself is open.

# ── Loopback base (same server, real pipeline endpoints) ──────────────────
# Must mirror run.py's precedence (PORT wins on PaaS).
_PORT = int(os.environ.get('PORT') or os.environ.get('FLASK_PORT') or 5001)
_BASE = f'http://127.0.0.1:{_PORT}'
_TIMEOUT = httpx.Timeout(connect=10.0, read=900.0, write=120.0, pool=10.0)

# Upper bound for an incoming seed document (a full campaign brief + product
# context is well under this; guards memory + LLM token usage).
MAX_SEED_CHARS = int(os.environ.get('COMPAT_MAX_SEED_CHARS', str(200_000)))

_client_lock = threading.Lock()
_client: httpx.Client | None = None


def _client_get() -> httpx.Client:
    """Process-wide pooled client for loopback calls.

    The compat layer fans out several loopback requests per SlashMarketer poll
    (2 arms × status + kick). Reusing connections removes a TCP handshake from
    every hop instead of opening a fresh socket per ``httpx.request``.
    """
    global _client
    if _client is None:
        with _client_lock:
            if _client is None:
                _client = httpx.Client(
                    timeout=_TIMEOUT,
                    limits=httpx.Limits(max_connections=64, max_keepalive_connections=32),
                )
    return _client


# ── State (persisted on the data volume, cached in memory) ────────────────
_DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data'))
_STATE_FILE = os.path.join(_DATA_DIR, 'compat_projects.json')
_lock = threading.Lock()
_STATE: dict[str, dict[str, Any]] | None = None


def _load_state() -> dict[str, dict[str, Any]]:
    """Return the in-memory state, loading it from disk at most once.

    Polling hammers the status routes; without this cache every GET would read
    (and JSON-parse) the volume file. Only mutations persist.
    """
    global _STATE
    if _STATE is not None:
        return _STATE
    try:
        with open(_STATE_FILE, encoding='utf-8') as fh:
            data = json.load(fh)
    except (FileNotFoundError, ValueError):
        data = {}
    _STATE = data if isinstance(data, dict) else {}
    return _STATE


def _save_state(state: dict[str, dict[str, Any]]) -> None:
    os.makedirs(_DATA_DIR, exist_ok=True)
    tmp = f'{_STATE_FILE}.tmp'
    with open(tmp, 'w', encoding='utf-8') as fh:
        json.dump(state, fh, ensure_ascii=False, indent=2)
    os.replace(tmp, _STATE_FILE)


def _update(pid: str, **fields: Any) -> None:
    with _lock:
        state = _load_state()
        entry = state.setdefault(pid, {})
        entry.update(fields)
        _save_state(state)


def _get(pid: str) -> dict[str, Any]:
    with _lock:
        return dict(_load_state().get(pid) or {})


# ── Upstream helpers ──────────────────────────────────────────────────────
def _normalize(status: Any) -> str:
    s = str(status or '').strip().lower()
    if s in ('completed', 'complete', 'ready', 'done', 'finished', 'success', 'succeeded'):
        return 'done'
    if s in (
        'failed',
        'error',
        'cancelled',
        'canceled',
        'stopped',
        'stop',
        'terminated',
        'killed',
    ):
        return 'failed'
    return 'running'


def _error(status: int, detail: str):
    return jsonify({'detail': f'MiroFish upstream HTTP {status}: {detail[:280]}'}), status


def _call(method: str, path: str, *, json_body: Any = None, files=None, data=None):
    """Call the loopback pipeline. Returns (ok, payload)."""
    # The app-level gate protects ALL /api/* — the loopback must authenticate too.
    headers = {}
    key = os.environ.get('MIROFISH_API_KEY', '')
    if key:
        headers['Authorization'] = f'Bearer {key}'
    try:
        resp = _client_get().request(
            method,
            f'{_BASE}{path}',
            json=json_body,
            files=files,
            data=data,
            headers=headers,
        )
    except httpx.RequestError as exc:
        raise RuntimeError(f'MiroFish loopback unreachable: {exc}') from exc
    try:
        payload = resp.json()
    except ValueError:
        payload = {'detail': resp.text[:280]}
    if resp.status_code >= 400:
        detail = payload.get('error') if isinstance(payload, dict) else None
        raise _UpstreamError(resp.status_code, str(detail or resp.text[:280]))
    return payload if isinstance(payload, dict) else {'data': payload}


class _UpstreamError(RuntimeError):
    def __init__(self, status: int, detail: str):
        super().__init__(detail)
        self.status = status


def _data(payload: dict) -> dict:
    d = payload.get('data')
    return d if isinstance(d, dict) else {}


# ── Routes ────────────────────────────────────────────────────────────────
@compat_bp.route('', methods=['POST'])
@compat_bp.route('/', methods=['POST'])
def create_project():
    body = request.get_json(silent=True) or {}
    title = str(body.get('title') or 'MiroFish project').strip()[:200] or 'MiroFish project'
    description = str(body.get('description') or '').strip()[:2000]
    seed_text = str(body.get('seed_text') or '').strip()
    if not seed_text:
        seed_text = description or title
    # Bound the payload so a huge seed can't exhaust memory or the LLM budget.
    if len(seed_text) > MAX_SEED_CHARS:
        seed_text = seed_text[:MAX_SEED_CHARS]

    files = {'files': ('seed.txt', seed_text.encode('utf-8'), 'text/plain')}
    form = {
        'simulation_requirement': description or title,
        'project_name': title,
    }
    try:
        payload = _call('POST', '/api/graph/ontology/generate', files=files, data=form)
    except _UpstreamError as exc:
        return _error(exc.status, str(exc))
    except RuntimeError as exc:
        return jsonify({'detail': str(exc)}), 502

    d = _data(payload)
    project_id = str(d.get('project_id') or d.get('id') or '').strip()
    if not project_id:
        return jsonify({'detail': 'MiroFish did not return a project id'}), 502
    _update(project_id, created_at=time.time(), title=title)
    return jsonify({'project_id': project_id, 'stage': 'init', 'status': 'running'})


@compat_bp.route('/<pid>/graph', methods=['POST'])
def graph(pid: str):
    try:
        payload = _call('POST', '/api/graph/build', json_body={'project_id': pid})
    except _UpstreamError as exc:
        return _error(exc.status, str(exc))
    except RuntimeError as exc:
        return jsonify({'detail': str(exc)}), 502
    d = _data(payload)
    _update(pid, graph_task=d.get('task_id'))
    return jsonify(payload)


@compat_bp.route('/<pid>/graph/status', methods=['GET'])
def graph_status(pid: str):
    task_id = _get(pid).get('graph_task')
    if not task_id:
        return jsonify({'status': 'running'})
    try:
        payload = _call('GET', f'/api/graph/task/{task_id}')
    except _UpstreamError as exc:
        return _error(exc.status, str(exc))
    except RuntimeError as exc:
        return jsonify({'detail': str(exc)}), 502
    d = _data(payload)
    out = {'status': _normalize(d.get('status'))}
    err = d.get('error') or d.get('message')
    if out['status'] == 'failed' and err:
        out['error'] = str(err)
    return jsonify(out)


@compat_bp.route('/<pid>/simulation/prepare', methods=['POST'])
def simulation_prepare(pid: str):
    sid = _get(pid).get('simulation_id')
    if sid:
        # The stored simulation may have been lost (e.g. created before the
        # data dir moved onto the volume). Recreate it from the graph instead
        # of failing forever.
        try:
            _call('GET', f'/api/simulation/{sid}')
        except _UpstreamError as exc:
            if exc.status == 404:
                sid = None
            else:
                return _error(exc.status, str(exc))
        except RuntimeError as exc:
            return jsonify({'detail': str(exc)}), 502

    if not sid:
        # Mirror MiroFish's own UI: create the simulation bound to the built
        # graph (the server also falls back to project.graph_id).
        create_body: dict[str, Any] = {'project_id': pid}
        try:
            gid = _resolve_graph_id(pid)
            if gid:
                create_body['graph_id'] = gid
        except (_UpstreamError, RuntimeError):
            pass
        try:
            payload = _call('POST', '/api/simulation/create', json_body=create_body)
        except _UpstreamError as exc:
            return _error(exc.status, str(exc))
        except RuntimeError as exc:
            return jsonify({'detail': str(exc)}), 502
        d = _data(payload)
        sid = str(d.get('simulation_id') or d.get('id') or '').strip()
        if not sid:
            return jsonify({'detail': 'MiroFish did not return a simulation id'}), 502
        _update(pid, simulation_id=sid)

    try:
        # use_llm_for_profiles / parallel_profile_count match the upstream UI.
        payload = _call(
            'POST',
            '/api/simulation/prepare',
            json_body={
                'simulation_id': sid,
                'use_llm_for_profiles': True,
                'parallel_profile_count': 5,
            },
        )
    except _UpstreamError as exc:
        return _error(exc.status, str(exc))
    except RuntimeError as exc:
        return jsonify({'detail': str(exc)}), 502
    _update(pid, prepare_task=_data(payload).get('task_id'))
    return jsonify(payload)


@compat_bp.route('/<pid>/simulation/prepare/status', methods=['GET'])
def simulation_prepare_status(pid: str):
    entry = _get(pid)
    sid = entry.get('simulation_id')
    if not sid:
        return jsonify({'status': 'running'})
    try:
        payload = _call(
            'POST',
            '/api/simulation/prepare/status',
            json_body={'simulation_id': sid, 'task_id': entry.get('prepare_task')},
        )
    except _UpstreamError as exc:
        return _error(exc.status, str(exc))
    except RuntimeError as exc:
        return jsonify({'detail': str(exc)}), 502
    d = _data(payload)
    out = {'status': _normalize(d.get('status'))}
    err = d.get('error') or d.get('message')
    if out['status'] == 'failed' and err:
        out['error'] = str(err)
    return jsonify(out)


@compat_bp.route('/<pid>/simulation/run', methods=['POST'])
def simulation_run(pid: str):
    sid = _get(pid).get('simulation_id')
    if not sid:
        return jsonify({'detail': 'simulation not prepared'}), 409
    body = request.get_json(silent=True) or {}
    rounds = body.get('rounds')
    try:
        rounds = int(rounds) if rounds else None
    except (TypeError, ValueError):
        rounds = None
    # ``force`` restarts an already-completed/stopped run so the same prepared
    # environment can be re-simulated (SlashMarketer's "re-simulate").
    force = bool(body.get('force'))
    # MiroFish's own UI updates the knowledge graph with agent activity during
    # the run; keep that on (env-overridable for safety).
    graph_memory = os.environ.get('COMPAT_GRAPH_MEMORY_UPDATE', 'true').strip().lower() in ('1', 'true', 'yes')
    start_body: dict[str, Any] = {
        'simulation_id': sid,
        'platform': 'parallel',
        'enable_graph_memory_update': graph_memory,
    }
    # Only cap the horizon when a limit is explicitly requested; otherwise use
    # MiroFish's auto-configured time config (total_simulation_hours).
    if rounds:
        start_body['max_rounds'] = rounds
    if force:
        start_body['force'] = True
    try:
        payload = _call('POST', '/api/simulation/start', json_body=start_body)
    except _UpstreamError as exc:
        return _error(exc.status, str(exc))
    except RuntimeError as exc:
        return jsonify({'detail': str(exc)}), 502
    if force:
        # Drop the previous run's report so the re-run regenerates fresh.
        _update(pid, report_task=None, report_id=None)
    _update(pid, run_started_at=time.time())
    return jsonify(payload)


@compat_bp.route('/<pid>/simulation/run/status', methods=['GET'])
def simulation_run_status(pid: str):
    entry = _get(pid)
    sid = entry.get('simulation_id')
    if not sid:
        return jsonify({'status': 'running'})
    try:
        payload = _call('GET', f'/api/simulation/{sid}/run-status')
    except _UpstreamError as exc:
        # A missing simulation (e.g. state lost) is a terminal failure, not a
        # reason to poll forever.
        if exc.status == 404:
            return jsonify({'status': 'failed', 'error': 'simulation run state not found (re-simulate required)'})
        return _error(exc.status, str(exc))
    except RuntimeError as exc:
        return jsonify({'detail': str(exc)}), 502
    d = _data(payload)
    runner = str(d.get('runner_status') or '').strip().lower()
    # ``idle`` means upstream has no run state. Allow a short grace window for
    # the just-started process to register before declaring failure.
    started = float(entry.get('run_started_at') or 0)
    # If the returned state predates our run (a leftover from a previous run),
    # don't trust it — wait for the fresh run to register.
    if started and d.get('started_at'):
        try:
            upstream_started = datetime.fromisoformat(str(d['started_at'])).timestamp()
            if upstream_started + 5 < started:
                return jsonify({'status': 'running', 'progress': 0.0})
        except (TypeError, ValueError):
            pass
    if runner in ('', 'idle') and (not started or (time.time() - started) > 45.0):
        return jsonify({'status': 'failed', 'error': 'simulation run state not found (re-simulate required)'})
    out = {'status': 'running' if runner in ('', 'idle') else _normalize(runner)}
    for src, dst in (
        ('progress_percent', 'progress'),
        ('current_round', 'round'),
        ('total_rounds', 'total_rounds'),
        ('total_actions_count', 'actions'),
    ):
        if d.get(src) is not None:
            out[dst] = d.get(src)
    err = d.get('error') or d.get('message')
    if out['status'] == 'failed' and err:
        out['error'] = str(err)
    return jsonify(out)


@compat_bp.route('/<pid>/report/generate', methods=['POST'])
def report_generate(pid: str):
    sid = _get(pid).get('simulation_id')
    if not sid:
        return jsonify({'detail': 'simulation not prepared'}), 409
    body = request.get_json(silent=True) or {}
    # Always start a fresh report: on a re-simulate the previous run's report
    # must not be returned as "already generated".
    force_regenerate = bool(body.get('force_regenerate', True))
    try:
        payload = _call(
            'POST',
            '/api/report/generate',
            json_body={'simulation_id': sid, 'force_regenerate': force_regenerate},
        )
    except _UpstreamError as exc:
        return _error(exc.status, str(exc))
    except RuntimeError as exc:
        return jsonify({'detail': str(exc)}), 502
    d = _data(payload)
    _update(pid, report_task=d.get('task_id'), report_id=d.get('report_id'))
    return jsonify(payload)


@compat_bp.route('/<pid>/report/generate/status', methods=['GET'])
def report_generate_status(pid: str):
    entry = _get(pid)
    sid = entry.get('simulation_id')
    if not sid:
        return jsonify({'status': 'running'})
    try:
        payload = _call(
            'POST',
            '/api/report/generate/status',
            json_body={
                'simulation_id': sid,
                'task_id': entry.get('report_task'),
                'report_id': entry.get('report_id'),
            },
        )
    except _UpstreamError as exc:
        return _error(exc.status, str(exc))
    except RuntimeError as exc:
        return jsonify({'detail': str(exc)}), 502
    d = _data(payload)
    out = {'status': _normalize(d.get('status'))}
    err = d.get('error') or d.get('message')
    if out['status'] == 'failed' and err:
        out['error'] = str(err)
    if d.get('report_id'):
        out['report_id'] = d.get('report_id')
    return jsonify(out)


# ── Visualizer (read-only) ────────────────────────────────────────────────
# SlashMarketer renders the full MiroFish experience (graph, agent process,
# social feed, report) from these normalized, project-scoped reads. Ids are
# resolved from state and cached so the UI never has to know graph/sim/report
# identifiers.

def _forward_query(allowed: tuple[str, ...]) -> str:
    params = {k: str(request.args.get(k))[:128] for k in allowed if request.args.get(k)}
    return f'?{urlencode(params)}' if params else ''


def _resolve_graph_id(pid: str) -> str:
    gid = str(_get(pid).get('graph_id') or '').strip()
    if gid:
        return gid
    d = _data(_call('GET', f'/api/graph/project/{pid}'))
    gid = str(d.get('graph_id') or '').strip()
    if gid:
        _update(pid, graph_id=gid)
    return gid


def _resolve_simulation_id(pid: str) -> str:
    sid = str(_get(pid).get('simulation_id') or '').strip()
    if sid:
        return sid
    payload = _call('GET', f'/api/simulation/list?project_id={pid}')
    raw = payload.get('data')
    sims = raw if isinstance(raw, list) else []
    if sims:
        sid = str(sims[0].get('simulation_id') or '').strip()
        if sid:
            _update(pid, simulation_id=sid, graph_id=sims[0].get('graph_id'))
    return sid


def _resolve_report_id(pid: str) -> str:
    rid = str(_get(pid).get('report_id') or '').strip()
    if rid:
        return rid
    sid = _resolve_simulation_id(pid)
    if not sid:
        return ''
    d = _data(_call('GET', f'/api/report/by-simulation/{sid}'))
    rid = str(d.get('report_id') or '').strip()
    if rid:
        _update(pid, report_id=rid)
    return rid


def _read(pid: str, path: str) -> dict:
    """Read a loopback endpoint, mapping errors to a JSON-safe envelope."""
    payload = _call('GET', path)
    d = _data(payload)
    return d if d else payload


@compat_bp.route('/<pid>/overview', methods=['GET'])
def overview(pid: str):
    entry = _get(pid)
    out: dict[str, Any] = {
        'project_id': pid,
        'title': entry.get('title'),
        'created_at': entry.get('created_at'),
        'graph_id': entry.get('graph_id'),
        'simulation_id': entry.get('simulation_id'),
        'report_id': entry.get('report_id'),
        'node_count': 0,
        'edge_count': 0,
        'rounds': 0,
        'actions': 0,
        'agents': 0,
        'started_at': None,
        'completed_at': None,
        'runner_status': None,
        'has_report': bool(entry.get('report_id')),
    }
    try:
        gid = _resolve_graph_id(pid)
        if gid:
            g = _data(_call('GET', f'/api/graph/data/{gid}'))
            out['graph_id'] = gid
            out['node_count'] = g.get('node_count', 0)
            out['edge_count'] = g.get('edge_count', 0)
    except (_UpstreamError, RuntimeError):
        pass
    try:
        sid = _resolve_simulation_id(pid)
        if sid:
            st = _data(_call('GET', f'/api/simulation/{sid}/run-status'))
            out['simulation_id'] = sid
            out['rounds'] = st.get('current_round', 0)
            out['total_rounds'] = st.get('total_rounds', 0)
            out['actions'] = st.get('total_actions_count', 0)
            out['started_at'] = st.get('started_at')
            out['completed_at'] = st.get('completed_at')
            out['runner_status'] = st.get('runner_status')
    except (_UpstreamError, RuntimeError):
        pass
    # Report presence is read from cached state only — resolving it upstream on
    # every overview poll would add a call while no report exists yet.
    rid = str(entry.get('report_id') or '').strip()
    if rid:
        out['report_id'] = rid
        out['has_report'] = True
    return jsonify(out)


@compat_bp.route('/<pid>/graph', methods=['GET'])
def graph_view(pid: str):
    try:
        gid = _resolve_graph_id(pid)
        if not gid:
            return jsonify({'graph_id': None, 'node_count': 0, 'edge_count': 0, 'nodes': [], 'edges': []})
        d = _read(pid, f'/api/graph/data/{gid}')
        d.setdefault('graph_id', gid)
        d.setdefault('nodes', [])
        d.setdefault('edges', [])
        return jsonify(d)
    except _UpstreamError as exc:
        return _error(exc.status, str(exc))
    except RuntimeError as exc:
        return jsonify({'detail': str(exc)}), 502


@compat_bp.route('/<pid>/entities', methods=['GET'])
def entities_view(pid: str):
    try:
        gid = _resolve_graph_id(pid)
        if not gid:
            return jsonify({'entities': []})
        d = _read(pid, f'/api/simulation/entities/{gid}{_forward_query(("enrich",))}')
        d.setdefault('entities', [])
        return jsonify(d)
    except _UpstreamError as exc:
        return _error(exc.status, str(exc))
    except RuntimeError as exc:
        return jsonify({'detail': str(exc)}), 502


@compat_bp.route('/<pid>/actions', methods=['GET'])
def actions_view(pid: str):
    try:
        sid = _resolve_simulation_id(pid)
        if not sid:
            return jsonify({'count': 0, 'actions': []})
        d = _read(pid, f'/api/simulation/{sid}/actions{_forward_query(("limit", "offset", "platform", "round_num"))}')
        d.setdefault('actions', [])
        return jsonify(d)
    except _UpstreamError as exc:
        return _error(exc.status, str(exc))
    except RuntimeError as exc:
        return jsonify({'detail': str(exc)}), 502


@compat_bp.route('/<pid>/timeline', methods=['GET'])
def timeline_view(pid: str):
    try:
        sid = _resolve_simulation_id(pid)
        if not sid:
            return jsonify({'rounds_count': 0, 'timeline': []})
        d = _read(pid, f'/api/simulation/{sid}/timeline{_forward_query(("start_round", "end_round"))}')
        d.setdefault('timeline', [])
        return jsonify(d)
    except _UpstreamError as exc:
        return _error(exc.status, str(exc))
    except RuntimeError as exc:
        return jsonify({'detail': str(exc)}), 502


@compat_bp.route('/<pid>/agents', methods=['GET'])
def agents_view(pid: str):
    try:
        sid = _resolve_simulation_id(pid)
        if not sid:
            return jsonify({'agents_count': 0, 'stats': []})
        d = _read(pid, f'/api/simulation/{sid}/agent-stats')
        d.setdefault('stats', [])
        return jsonify(d)
    except _UpstreamError as exc:
        return _error(exc.status, str(exc))
    except RuntimeError as exc:
        return jsonify({'detail': str(exc)}), 502


@compat_bp.route('/<pid>/posts', methods=['GET'])
def posts_view(pid: str):
    try:
        sid = _resolve_simulation_id(pid)
        if not sid:
            return jsonify({'platform': 'reddit', 'count': 0, 'total': 0, 'posts': []})
        d = _read(pid, f'/api/simulation/{sid}/posts{_forward_query(("platform", "limit", "offset"))}')
        d.setdefault('posts', [])
        return jsonify(d)
    except _UpstreamError as exc:
        return _error(exc.status, str(exc))
    except RuntimeError as exc:
        return jsonify({'detail': str(exc)}), 502


@compat_bp.route('/<pid>/comments', methods=['GET'])
def comments_view(pid: str):
    try:
        sid = _resolve_simulation_id(pid)
        if not sid:
            return jsonify({'count': 0, 'comments': []})
        d = _read(pid, f'/api/simulation/{sid}/comments{_forward_query(("platform", "limit", "offset", "post_id"))}')
        d.setdefault('comments', [])
        return jsonify(d)
    except _UpstreamError as exc:
        return _error(exc.status, str(exc))
    except RuntimeError as exc:
        return jsonify({'detail': str(exc)}), 502


@compat_bp.route('/<pid>/profiles', methods=['GET'])
def profiles_view(pid: str):
    try:
        sid = _resolve_simulation_id(pid)
        if not sid:
            return jsonify({'platform': 'reddit', 'count': 0, 'profiles': []})
        d = _read(pid, f'/api/simulation/{sid}/profiles{_forward_query(("platform",))}')
        d.setdefault('profiles', [])
        return jsonify(d)
    except _UpstreamError as exc:
        return _error(exc.status, str(exc))
    except RuntimeError as exc:
        return jsonify({'detail': str(exc)}), 502


@compat_bp.route('/<pid>/report', methods=['GET'])
def report_view(pid: str):
    try:
        rid = _resolve_report_id(pid)
        if not rid:
            return jsonify({'report_id': None, 'markdown_content': '', 'sections': []})
        d = _read(pid, f'/api/report/{rid}')
        d.setdefault('report_id', rid)
        try:
            sec = _data(_call('GET', f'/api/report/{rid}/sections'))
            d['sections'] = sec.get('sections') or []
            d['is_complete'] = sec.get('is_complete', True)
        except (_UpstreamError, RuntimeError):
            d.setdefault('sections', [])
        return jsonify(d)
    except _UpstreamError as exc:
        return _error(exc.status, str(exc))
    except RuntimeError as exc:
        return jsonify({'detail': str(exc)}), 502


# ── Interactive capabilities (interviews / report chat / stop) ────────────
# MiroFish keeps the OASIS environment alive in command-waiting mode after a
# run, so agents can be interviewed and the report agent can be chatted with.

@compat_bp.route('/<pid>/interview', methods=['POST'])
def interview(pid: str):
    """Interview one agent (``agent_id``) or ask every agent (omit it)."""
    sid = _resolve_simulation_id(pid)
    if not sid:
        return jsonify({'detail': 'simulation not prepared'}), 409
    body = request.get_json(silent=True) or {}
    prompt = str(body.get('prompt') or '').strip()
    if not prompt:
        return jsonify({'detail': 'prompt is required'}), 400
    payload: dict[str, Any] = {'simulation_id': sid, 'prompt': prompt}
    for key in ('platform', 'timeout'):
        if body.get(key) not in (None, ''):
            payload[key] = body[key]
    agent_id = body.get('agent_id')
    if agent_id is None:
        endpoint = '/api/simulation/interview/all'
    else:
        endpoint = '/api/simulation/interview'
        payload['agent_id'] = agent_id
    try:
        return jsonify(_call('POST', endpoint, json_body=payload))
    except _UpstreamError as exc:
        return _error(exc.status, str(exc))
    except RuntimeError as exc:
        return jsonify({'detail': str(exc)}), 502


@compat_bp.route('/<pid>/interview/history', methods=['GET'])
def interview_history(pid: str):
    sid = _resolve_simulation_id(pid)
    if not sid:
        return jsonify({'interviews': []})
    payload: dict[str, Any] = {'simulation_id': sid}
    for key in ('platform', 'agent_id', 'limit'):
        value = request.args.get(key)
        if value:
            payload[key] = value
    try:
        d = _data(_call('POST', '/api/simulation/interview/history', json_body=payload))
        return jsonify(d)
    except _UpstreamError as exc:
        return _error(exc.status, str(exc))
    except RuntimeError as exc:
        return jsonify({'detail': str(exc)}), 502


@compat_bp.route('/<pid>/report/chat', methods=['POST'])
def report_chat(pid: str):
    sid = _resolve_simulation_id(pid)
    if not sid:
        return jsonify({'detail': 'simulation not prepared'}), 409
    body = request.get_json(silent=True) or {}
    message = str(body.get('message') or '').strip()
    if not message:
        return jsonify({'detail': 'message is required'}), 400
    history = body.get('chat_history') or []
    try:
        return jsonify(
            _call(
                'POST',
                '/api/report/chat',
                json_body={'simulation_id': sid, 'message': message, 'chat_history': history},
            )
        )
    except _UpstreamError as exc:
        return _error(exc.status, str(exc))
    except RuntimeError as exc:
        return jsonify({'detail': str(exc)}), 502


@compat_bp.route('/<pid>/simulation/stop', methods=['POST'])
def simulation_stop(pid: str):
    sid = _get(pid).get('simulation_id')
    if not sid:
        return jsonify({'detail': 'simulation not prepared'}), 409
    try:
        return jsonify(_call('POST', '/api/simulation/stop', json_body={'simulation_id': sid}))
    except _UpstreamError as exc:
        return _error(exc.status, str(exc))
    except RuntimeError as exc:
        return jsonify({'detail': str(exc)}), 502


@compat_bp.route('/<pid>/env-status', methods=['GET'])
def env_status(pid: str):
    sid = _get(pid).get('simulation_id')
    if not sid:
        return jsonify({'alive': False})
    try:
        payload = _call('POST', '/api/simulation/env-status', json_body={'simulation_id': sid})
        d = _data(payload)
        return jsonify({'alive': bool(d.get('alive') or d.get('is_alive')), 'detail': d})
    except (_UpstreamError, RuntimeError):
        return jsonify({'alive': False})
