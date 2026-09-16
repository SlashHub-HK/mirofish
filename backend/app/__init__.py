"""
MiroFish Backend - Flask Application Factory
"""

import hmac
import os
import warnings

# Suppress multiprocessing resource_tracker warnings (from third-party libs like transformers)
# Must be set before all other imports
warnings.filterwarnings("ignore", message=".*resource_tracker.*")

from flask import Flask, jsonify, request
from flask_cors import CORS

from .config import Config
from .utils.logger import setup_logger, get_logger


def _capacity_snapshot() -> dict:
    """Best-effort memory/concurrency state. Never raises."""
    try:
        from .core.resource_guard import (
            available_memory_mb,
            heavy_stages_active,
            heavy_stages_waiting,
        )
        from .services.graph_db import open_database_count
        from .services.simulation_runner import SimulationRunner

        return {
            'available_mb': available_memory_mb(),
            'min_free_mb': Config.MIROFISH_MIN_FREE_MB,
            'heavy_stages_active': heavy_stages_active(),
            'heavy_stages_waiting': heavy_stages_waiting(),
            'max_heavy_stages': Config.MAX_CONCURRENT_HEAVY_STAGES,
            'running_simulations': SimulationRunner.running_simulation_count(),
            'max_simulations': Config.MAX_CONCURRENT_SIMULATIONS,
            'kuzu_pools_open': open_database_count(),
            'kuzu_buffer_pool_mb': Config.KUZU_BUFFER_POOL_MB,
        }
    except Exception as exc:  # noqa: BLE001 - diagnostics must never break /health
        return {'error': str(exc)[:160]}


def create_app(config_class=Config):
    """Flask application factory function (headless API — no frontend)."""
    # Backend-only service: SlashMarketer owns the UI and calls this API.
    app = Flask(__name__)
    app.config.from_object(config_class)
    
    # Set JSON encoding: ensure non-ASCII characters are displayed directly (instead of \uXXXX format)
    # Flask >= 2.3 uses app.json.ensure_ascii, older versions use JSON_AS_ASCII config
    if hasattr(app, 'json') and hasattr(app.json, 'ensure_ascii'):
        app.json.ensure_ascii = False
    
    # Set up logging
    logger = setup_logger('mirofish')
    
    # Only print startup info in the reloader subprocess (avoid printing twice in debug mode)
    is_reloader_process = os.environ.get('WERKZEUG_RUN_MAIN') == 'true'
    debug_mode = app.config.get('DEBUG', False)
    should_log_startup = not debug_mode or is_reloader_process
    
    if should_log_startup:
        logger.info("=" * 50)
        logger.info("MiroFish Backend starting...")
        logger.info("=" * 50)
    
    # Enable CORS
    CORS(app, resources={r"/api/*": {"origins": app.config.get('CORS_ORIGINS', [])}})
    
    # Register simulation process cleanup (ensure all simulation processes are terminated on server shutdown)
    from .services.simulation_runner import SimulationRunner
    SimulationRunner.register_cleanup()
    if should_log_startup:
        logger.info("Simulation process cleanup registered")
    
    # Auth gate: this is an internal integration surface. When MIROFISH_API_KEY
    # is set, every /api/* request must present it as a Bearer token. /health is
    # exempt so the platform health-check and load balancers work unauthenticated.
    @app.before_request
    def require_api_key():
        path = request.path
        if not path.startswith('/api/'):
            return None
        required = os.environ.get('MIROFISH_API_KEY', '')
        if not required:
            return None
        # Constant-time compare so a timing side-channel can't probe the key.
        if not hmac.compare_digest(request.headers.get('Authorization', ''), f'Bearer {required}'):
            return jsonify({'error': 'Unauthorized'}), 401
        return None

    if should_log_startup and not os.environ.get('MIROFISH_API_KEY', '').strip():
        logger.warning(
            'MIROFISH_API_KEY is not set — all /api/* endpoints are UNAUTHENTICATED. '
            'Set it in production.'
        )

    # Request logging middleware
    @app.before_request
    def log_request():
        logger = get_logger('mirofish.request')
        logger.debug(f"Request: {request.method} {request.path}")
        if app.config.get('DEBUG') and request.content_type and 'json' in request.content_type:
            logger.debug(f"Request body: {request.get_json(silent=True)}")
    
    @app.after_request
    def log_response(response):
        logger = get_logger('mirofish.request')
        logger.debug(f"Response: {response.status_code}")
        # Status endpoints are polled constantly; without an explicit no-store
        # an edge/proxy (Railway) caches GETs and serves stale run/graph state.
        if request.path.startswith('/api/') or request.path == '/health':
            response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
            response.headers['Pragma'] = 'no-cache'
        return response
    
    # Register blueprints
    from .api import compat_bp, graph_bp, report_bp, simulation_bp
    app.register_blueprint(graph_bp, url_prefix='/api/graph')
    app.register_blueprint(simulation_bp, url_prefix='/api/simulation')
    app.register_blueprint(report_bp, url_prefix='/api/report')
    # SlashMarketer Verify (System 2) compatibility contract.
    app.register_blueprint(compat_bp, url_prefix='/api/projects')
    
    # Health check (unauthenticated — used by the platform + SlashMarketer).
    @app.route('/health')
    def health():
        # Reports the commit so a deploy is verifiable from outside, plus a
        # capacity snapshot: when a run is refused for lack of headroom, this is
        # where an operator can see *why*. Wrapped so /health can never fail or
        # slow down — the platform polls it, and Railway health-checks it.
        return {
            'status': 'ok',
            'service': 'MiroFish Backend',
            'mode': 'headless',
            'commit': (os.environ.get('RAILWAY_GIT_COMMIT_SHA') or '')[:12] or None,
            'capacity': _capacity_snapshot(),
        }

    # Service metadata.
    @app.route('/')
    def root():
        return {
            'service': 'MiroFish Backend',
            'mode': 'headless',
            'integration': 'SlashMarketer (Verify · System 2)',
            'endpoints': ['/health', '/api/projects/*', '/api/graph/*', '/api/simulation/*', '/api/report/*'],
            'routes': sorted(str(r) for r in app.url_map.iter_rules() if str(r).startswith('/api/')),
        }

    # Release the cached Kuzu database handles on exit. Each one owns a buffer
    # pool (see services/graph_db.py), so leaving them to the OS means a
    # restart/rolling deploy can briefly hold two full sets of pools.
    import atexit

    from .services.graph_db import close_all_databases

    atexit.register(close_all_databases)

    if should_log_startup:
        logger.info("MiroFish Backend started successfully (headless API)")

    return app

