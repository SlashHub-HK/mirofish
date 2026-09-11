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
from typing import Any

import httpx
from flask import Blueprint, jsonify, request

compat_bp = Blueprint('compat', __name__)

# ── Loopback base (same server, real pipeline endpoints) ──────────────────
_PORT = int(os.environ.get('FLASK_PORT') or os.environ.get('PORT') or 5001)
_BASE = f'http://127.0.0.1:{_PORT}'
_TIMEOUT = httpx.Timeout(connect=10.0, read=900.0, write=120.0, pool=10.0)

# ── State (persisted on the data volume) ──────────────────────────────────
_DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data'))
_STATE_FILE = os.path.join(_DATA_DIR, 'compat_projects.json')
_lock = threading.Lock()


def _load_state() -> dict[str, dict[str, Any]]:
    try:
        with open(_STATE_FILE, encoding='utf-8') as fh:
            data = json.load(fh)
            return data if isinstance(data, dict) else {}
    except (FileNotFoundError, ValueError):
        return {}


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
        state[pid] = entry
        _save_state(state)


def _get(pid: str) -> dict[str, Any]:
    with _lock:
        return _load_state().get(pid, {})


# ── Upstream helpers ──────────────────────────────────────────────────────
def _normalize(status: Any) -> str:
    s = str(status or '').strip().lower()
    if s in ('completed', 'complete', 'ready', 'done', 'finished', 'success', 'succeeded'):
        return 'done'
    if s in ('failed', 'error', 'cancelled', 'canceled'):
        return 'failed'
    return 'running'


def _error(status: int, detail: str):
    return jsonify({'detail': f'MiroFish upstream HTTP {status}: {detail[:280]}'}), status


def _call(method: str, path: str, *, json_body: Any = None, files=None, data=None):
    """Call the loopback pipeline. Returns (ok, payload)."""
    try:
        resp = httpx.request(
            method,
            f'{_BASE}{path}',
            json=json_body,
            files=files,
            data=data,
            timeout=_TIMEOUT,
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
    description = str(body.get('description') or '').strip()
    seed_text = str(body.get('seed_text') or '').strip()
    if not seed_text:
        seed_text = description or title

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
    if not sid:
        try:
            payload = _call('POST', '/api/simulation/create', json_body={'project_id': pid})
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
        payload = _call('POST', '/api/simulation/prepare', json_body={'simulation_id': sid})
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
    try:
        payload = _call(
            'POST',
            '/api/simulation/start',
            json_body={
                'simulation_id': sid,
                'max_rounds': rounds,
                'platform': 'parallel',
                'enable_graph_memory_update': False,
            },
        )
    except _UpstreamError as exc:
        return _error(exc.status, str(exc))
    except RuntimeError as exc:
        return jsonify({'detail': str(exc)}), 502
    return jsonify(payload)


@compat_bp.route('/<pid>/simulation/run/status', methods=['GET'])
def simulation_run_status(pid: str):
    sid = _get(pid).get('simulation_id')
    if not sid:
        return jsonify({'status': 'running'})
    try:
        payload = _call('GET', f'/api/simulation/{sid}/run-status')
    except _UpstreamError as exc:
        return _error(exc.status, str(exc))
    except RuntimeError as exc:
        return jsonify({'detail': str(exc)}), 502
    d = _data(payload)
    out = {'status': _normalize(d.get('runner_status'))}
    if 'progress_percent' in d:
        out['progress'] = d.get('progress_percent')
    err = d.get('error') or d.get('message')
    if out['status'] == 'failed' and err:
        out['error'] = str(err)
    return jsonify(out)


@compat_bp.route('/<pid>/report/generate', methods=['POST'])
def report_generate(pid: str):
    sid = _get(pid).get('simulation_id')
    if not sid:
        return jsonify({'detail': 'simulation not prepared'}), 409
    try:
        payload = _call('POST', '/api/report/generate', json_body={'simulation_id': sid})
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
