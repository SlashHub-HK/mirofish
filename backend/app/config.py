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

    # ── Kuzu (embedded graph DB) memory limits ──────────────────────────
    # Kuzu defaults its buffer pool to ~80% of TOTAL PHYSICAL RAM and — per its
    # own docs — "in a container, an explicit value is safer because that
    # calculation does not explicitly inspect the cgroup limit". On a shared
    # 24 GB host that is ~19 GB PER kuzu.Database instance, and one used to be
    # constructed per query. These caps are per database object; the pool is a
    # ceiling, not a preallocation, so small graphs still cost little.
    KUZU_BUFFER_POOL_MB = int(os.environ.get('KUZU_BUFFER_POOL_MB', '256'))
    # Kuzu otherwise uses every core; a graph scan that fans out across all of
    # them multiplies transient working memory inside the pool.
    KUZU_MAX_NUM_THREADS = int(os.environ.get('KUZU_MAX_NUM_THREADS', '4'))

    # ── Host-resource guards (see core/resource_guard.py) ───────────────
    # This service shares a 24 GB host. Refuse heavy work below this much free
    # memory instead of letting the OOM-killer take out the container.
    # 0 disables the check.
    MIROFISH_MIN_FREE_MB = int(os.environ.get('MIROFISH_MIN_FREE_MB', '800'))
    # In-process heavy stages (prepare/report) allowed at once.
    MAX_CONCURRENT_HEAVY_STAGES = int(os.environ.get('MAX_CONCURRENT_HEAVY_STAGES', '1'))
    # How long a queued heavy stage waits for a slot before refusing with a
    # retryable 503. Bounded on purpose: unbounded waiting piles up request
    # threads and half-built state, turning one slow run into an outage.
    MAX_HEAVY_STAGE_WAIT_SECONDS = int(os.environ.get('MAX_HEAVY_STAGE_WAIT_SECONDS', '180'))
    # Simulation subprocesses allowed at once. One: a run holds a full OASIS
    # agent stack (plus TWHIN-BERT for Twitter), which the host cannot duplicate.
    MAX_CONCURRENT_SIMULATIONS = int(os.environ.get('MAX_CONCURRENT_SIMULATIONS', '1'))
    # Fallback agent/entity cap for callers that do not state a world size —
    # including projects created before the request was persisted. One profile
    # (and one agent per platform) is built per entity, so this is the ceiling
    # that keeps a run inside the host. 0 disables the cap.
    MIROFISH_DEFAULT_MAX_ENTITIES = int(os.environ.get('MIROFISH_DEFAULT_MAX_ENTITIES', '40'))

    # File upload config (env-overridable so PaaS can point it at a volume)
    MAX_CONTENT_LENGTH = 50 * 1024 * 1024  # 50MB
    UPLOAD_FOLDER = os.environ.get(
        'UPLOAD_FOLDER',
        os.path.join(os.path.dirname(__file__), '../uploads'),
    )
    # Derived data dirs live under UPLOAD_FOLDER so a single volume keeps the
    # knowledge graph, simulations, reports and async task files together.
    REPORTS_DIR = os.environ.get('REPORTS_DIR', os.path.join(UPLOAD_FOLDER, 'reports'))
    TASKS_DIR = os.environ.get('TASKS_DIR', os.path.join(UPLOAD_FOLDER, 'tasks'))
    ALLOWED_EXTENSIONS = {'pdf', 'md', 'txt', 'markdown'}

    # Text processing config
    DEFAULT_CHUNK_SIZE = 500
    DEFAULT_CHUNK_OVERLAP = 50

    # OASIS simulation config
    OASIS_DEFAULT_MAX_ROUNDS = int(os.environ.get('OASIS_DEFAULT_MAX_ROUNDS', '10'))
    OASIS_SIMULATION_DATA_DIR = os.environ.get(
        'OASIS_SIMULATION_DATA_DIR',
        os.path.join(UPLOAD_FOLDER, 'simulations'),
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
