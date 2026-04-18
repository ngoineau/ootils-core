FROM python:3.12-slim

WORKDIR /app

# Copy source before install (editable install requires src/ to exist)
COPY pyproject.toml .
COPY src/ /app/src/
COPY scripts/ /app/scripts/

RUN pip install --no-cache-dir .

RUN addgroup --system ootils \
    && adduser --system --ingroup ootils --home /app ootils \
    && chown -R ootils:ootils /app

USER ootils

CMD ["uvicorn", "ootils_core.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
