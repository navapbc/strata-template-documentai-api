# Dockerfile following platform best practices
# References:
# - https://github.com/navapbc/platform-cli/blob/main/Dockerfile
# - https://docs.astral.sh/uv/guides/integration/docker/

FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim

# install runtime dependencies
RUN apt-get update -qq \
    && apt-get install --no-install-recommends --yes \
        build-essential \
        curl \
        libmagic1 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_SYSTEM_PYTHON=true

WORKDIR /app

RUN touch README.md
COPY pyproject.toml uv.lock* /app/
# install project dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

# copy application code
COPY src/ /app/src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# Add project's virtual env to the PATH so we can directly run scripts defined
# in pyproject.toml
ENV PATH="/app/.venv/bin:$PATH"

# Set the host to 0.0.0.0 to make the server available external to the container
# that it's running in.
ENV HOST=0.0.0.0

CMD ["documentai_api"]
