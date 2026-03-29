FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first for layer caching
COPY requirements.txt pyproject.toml ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy source and config
COPY src/ src/
COPY config/ config/
COPY scripts/ scripts/

# Install the package
RUN pip install --no-cache-dir -e .

# Healthcheck: verify the module can be imported
HEALTHCHECK --interval=60s --timeout=5s --retries=3 \
  CMD python -c "import systrade" || exit 1

CMD ["python", "src/systrade/trading_app.py"]