# Start with slim Python 3.13 image
FROM python:3.13.10-slim

# Copy uv binary from official uv image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# Set working directory
WORKDIR /app

# Enable the venv automatically by adding it to the PATH
ENV PATH="/app/.venv/bin:$PATH"

# Copy dependency files
COPY "pyproject.toml" "uv.lock" ".python-version" ./

# Install dependencies
RUN uv sync --locked

# Copy application code
COPY pipeline.py pipeline.py

# Set entry point
ENTRYPOINT ["python", "pipeline.py"]