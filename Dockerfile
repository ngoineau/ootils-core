FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for psycopg (binary)
RUN apt-get update && apt-get install -y \
    libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy project
COPY pyproject.toml .
COPY src/ src/

# Install Python dependencies
RUN pip install --no-cache-dir -e ".[dev]"

EXPOSE 8000

CMD ["uvicorn", "ootils_core.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
