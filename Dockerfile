FROM python:3.11-slim

# git: pip installs the model package from GitHub; libgomp1: LightGBM runtime.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN pip install --no-cache-dir -e .

CMD ["python", "-m", "streaming_fraud.pipeline"]
