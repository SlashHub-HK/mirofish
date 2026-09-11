"""
MiroFish Backend - Flask Application Factory
"""

import os
import warnings

# Suppress multiprocessing resource_tracker warnings (from third-party libs like transformers)
# Must be set before all other imports
warnings.filterwarnings("ignore", message=".*resource_tracker.*")

from flask import Flask, jsonify, request
from flask_cors import CORS

from .config import Config
from .utils.logger import setup_logger, get_logger


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
        if request.headers.get('Authorization', '') != f'Bearer {required}':
            return jsonify({'error': 'Unauthorized'}), 401
        return None

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
        return {'status': 'ok', 'service': 'MiroFish Backend', 'mode': 'headless'}

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

    if should_log_startup:
        logger.info("MiroFish Backend started successfully (headless API)")

    return app

