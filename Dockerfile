# One image for the decision API, the feature consumer, the ledger stub and migrate (D40j).

# Build stage: uv from PyPI, hash-checked (D42), then the locked runtime dependencies (D41).
FROM python:3.12.16-slim-trixie AS build
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_ROOT_USER_ACTION=ignore \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_NO_CACHE=1 \
    UV_PYTHON_DOWNLOADS=never
COPY docker/uv-requirements.txt /tmp/uv-requirements.txt
RUN python -m pip install --require-hashes --only-binary :all: --no-deps -r /tmp/uv-requirements.txt
WORKDIR /app
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --locked --no-dev

# Runtime stage: the virtual environment and the code, no uv, run as an unprivileged user.
FROM python:3.12.16-slim-trixie
RUN useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin app
WORKDIR /app
COPY --from=build /app/.venv /app/.venv
COPY risk ./risk
COPY ledger_stub ./ledger_stub
ENV PATH=/app/.venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
USER app
