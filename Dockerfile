# MiroFish backend (headless API).
#
# SlashMarketer owns the frontend (its own /verify UI) and calls this service
# server-side through its whitelisted /api/v1/verify/mirofish/* proxy. This image
# therefore builds ONLY the Python backend — no Node, no Vue, no static build —
# which keeps it small, fast to start and easy to scale.

FROM python:3.11-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates curl \
  && rm -rf /var/lib/apt/lists/*

# uv for fast, reproducible dependency installation.
COPY --from=ghcr.io/astral-sh/uv:0.9.26 /uv /uvx /bin/

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    FLASK_DEBUG=false \
    FLASK_HOST=0.0.0.0

# Dependencies first (layer cache).
COPY backend/requirements.txt ./backend/requirements.txt
RUN uv pip install --system -r backend/requirements.txt

# Backend source.
COPY backend/ ./backend/

EXPOSE 5001

# run.py binds PORT (PaaS) → FLASK_PORT → 5001 and serves via waitress.
CMD ["python", "backend/run.py"]
