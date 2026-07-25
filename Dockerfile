# syntax=docker/dockerfile:1

# --- builder: resolve and install dependencies into a self-contained prefix ---
FROM python:3.12-slim AS builder

# build-essential covers the rare case a dependency has no prebuilt wheel for
# this platform and needs to compile from source (e.g. rapidfuzz on an
# uncommon arch) — discarded along with the rest of this stage either way.
RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir --prefix=/install .

# --- runtime: just the installed package, no build tooling ---
FROM python:3.12-slim AS runtime

RUN useradd --create-home --uid 1000 wolt
COPY --from=builder /install /usr/local

# The cache DB, FTS index, and crawl job logs all live here — mount this as a
# volume so `docker compose down`/image rebuilds don't lose indexed data.
ENV WOLT_IL_DB=/data/cache.db \
    PYTHONUNBUFFERED=1
RUN mkdir -p /data && chown -R wolt:wolt /data
VOLUME ["/data"]

USER wolt
WORKDIR /home/wolt

EXPOSE 8787
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8787/api/status', timeout=4)" || exit 1

CMD ["wolt-il-webui", "--host", "0.0.0.0", "--port", "8787"]
