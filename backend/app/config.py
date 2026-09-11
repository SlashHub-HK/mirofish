"""
Configuration Management
Loads configuration from the project root .env file
"""

import os
import secrets
from dotenv import load_dotenv


def _get_bool_env(key, default=False):
    """Get a boolean value from environment variable."""
    val = os.environ.get(key, '')
    if not val:
        return default
    return val.lower() in ('true', '1', 'yes')


def _get_cors_origins():
    """Get CORS origins from environment or return defaults."""
    origins = os.environ.get('CORS_ORIGINS', '')
    if origins:
        return [o.strip() for o in origins.split(',')]
    return ['http://localhost:3000', 'http://localhost:5173']

# Load the .env file from project root
# Path: MiroFish/.env (relative to backend/app/config.py)
project_root_env = os.path.join(os.path.dirname(__file__), '../../.env')

if os.path.exists(project_root_env):
    load_dotenv(project_root_env, override=True)
else:
    # If no .env in root, try loading environment variables (for production)
    load_dotenv(override=True)


class Config:
    """Flask configuration class"""

    # Flask config
    DEBUG = _get_bool_env('FLASK_DEBUG', False)
    SECRET_KEY = os.environ.get('SECRET_KEY') or secrets.token_hex(32)
    CORS_ORIGINS = _get_cors_origins()

    # JSON config - disable ASCII escaping for proper Unicode display
    JSON_AS_ASCII = False

    # LLM config
    LLM_API_KEY = os.environ.get('LLM_API_KEY', '')
    LLM_BASE_URL = os.environ.get('LLM_BASE_URL', 'https://api.openai.com/v1')
    LLM_MODEL_NAME = os.environ.get('LLM_MODEL_NAME', 'gpt-4o-mini')
    LLM_PROVIDER = os.environ.get('LLM_PROVIDER', '')  # 'openai', 'anthropic', 'claude-cli', 'codex-cli'

    # Graph database config (KuzuDB - local embedded graph database)
    GRAPH_DB_PATH = os.environ.get('GRAPH_DB_PATH', os.path.join(os.path.dirname(__file__), '../data/graphdb'))

    # File upload config (env-overridable so PaaS can point it at a volume)
    MAX_CONTENT_LENGTH = 50 * 1024 * 1024  # 50MB
    UPLOAD_FOLDER = os.environ.get(
        'UPLOAD_FOLDER',
        os.path.join(os.path.dirname(__file__), '../uploads'),
    )
    ALLOWED_EXTENSIONS = {'pdf', 'md', 'txt', 'markdown'}

    # Text processing config
    DEFAULT_CHUNK_SIZE = 500
    DEFAULT_CHUNK_OVERLAP = 50

    # OASIS simulation config
    OASIS_DEFAULT_MAX_ROUNDS = int(os.environ.get('OASIS_DEFAULT_MAX_ROUNDS', '10'))
    OASIS_SIMULATION_DATA_DIR = os.environ.get(
        'OASIS_SIMULATION_DATA_DIR',
        os.path.join(os.path.dirname(__file__), '../uploads/simulations'),
    )

    # OASIS platform available actions
    OASIS_TWITTER_ACTIONS = [
        'CREATE_POST', 'LIKE_POST', 'REPOST', 'FOLLOW', 'DO_NOTHING', 'QUOTE_POST'
    ]
    OASIS_REDDIT_ACTIONS = [
        'LIKE_POST', 'DISLIKE_POST', 'CREATE_POST', 'CREATE_COMMENT',
        'LIKE_COMMENT', 'DISLIKE_COMMENT', 'SEARCH_POSTS', 'SEARCH_USER',
        'TREND', 'REFRESH', 'DO_NOTHING', 'FOLLOW', 'MUTE'
    ]

    # Report Agent config
    REPORT_AGENT_MAX_TOOL_CALLS = int(os.environ.get('REPORT_AGENT_MAX_TOOL_CALLS', '5'))
    REPORT_AGENT_MAX_REFLECTION_ROUNDS = int(os.environ.get('REPORT_AGENT_MAX_REFLECTION_ROUNDS', '2'))
    REPORT_AGENT_TEMPERATURE = float(os.environ.get('REPORT_AGENT_TEMPERATURE', '0.5'))

    @classmethod
    def validate(cls):
        """Validate required configuration"""
        errors = []
        # CLI providers don't need an API key
        if cls.LLM_PROVIDER not in ('claude-cli', 'codex-cli') and not cls.LLM_API_KEY:
            errors.append("LLM_API_KEY not configured (set LLM_PROVIDER=claude-cli to use CLI instead)")
        return errors
