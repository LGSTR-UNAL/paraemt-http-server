"""HTTP API for submitting parameterized ParaEMT simulation campaigns."""

import logging
import os
from typing import Any

from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException

from .campaign import SpecError, submit

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))


def required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


producer = Producer({"bootstrap.servers": required("KAFKA_BOOTSTRAP_SERVERS")})
request_topic = required("PARAEMT_REQUEST_TOPIC")
all_topic = required("ALL_REQUEST_TOPIC")

app = FastAPI(title="ParaEMT campaign service", version="0.1.0")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/campaigns/paraemt", status_code=202)
def create_campaign(spec: dict[str, Any]) -> dict[str, Any]:
    try:
        return submit(spec, producer, request_topic, all_topic)
    except SpecError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except (RuntimeError, ValueError) as exc:
        logging.exception("Campaign submission failed")
        raise HTTPException(status_code=503, detail="Campaign publication failed") from exc
