"""
MiroFish Backend Entry Point
"""

import os
import sys

# Fix Windows console encoding: set UTF-8 before all other imports
if sys.platform == 'win32':
    os.environ.setdefault('PYTHONIOENCODING', 'utf-8')
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app import create_app
from app.config import Config


def main():
    """Main function"""
    # Validate configuration
    errors = Config.validate()
    if errors:
        print("Configuration errors:")
        for err in errors:
            print(f"  - {err}")
        print("\nPlease check your .env file configuration")
        sys.exit(1)

    # Create application
    app = create_app()

    # Get run configuration
    host = os.environ.get('FLASK_HOST', '0.0.0.0')
    # PaaS (Railway/Heroku) inject PORT and route to it — it must WIN over the
    # image's default FLASK_PORT, otherwise the app binds 5001 and the platform
    # returns 502. FLASK_PORT stays as the local-dev fallback.
    port = int(os.environ.get('PORT') or os.environ.get('FLASK_PORT') or 5001)
    debug = Config.DEBUG

    # Start server
    if debug:
        app.run(host=host, port=port, debug=True, threaded=True)
        return

    try:
        from waitress import serve
        threads = int(os.environ.get('WAITRESS_THREADS', '8'))
        serve(app, host=host, port=port, threads=threads)
    except ImportError:
        app.run(host=host, port=port, debug=False, threaded=True)


if __name__ == '__main__':
    main()
