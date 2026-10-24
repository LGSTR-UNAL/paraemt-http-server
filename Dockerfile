FROM python:3.14-trixie

WORKDIR /opt/paraemt-http-server
RUN apt-get update && apt-get install -y --no-install-recommends librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ ./src/
RUN useradd --system --no-create-home app && chown -R app:app /opt/paraemt-http-server
USER app
EXPOSE 8000
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
