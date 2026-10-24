"""Campaign expansion for ParaEMT: a parameter specification to Kafka requests.

This module is used by the ParaEMT HTTP service and can also be run as a command-line tool:

    python paraemt_campaign.py spec.json --kafka localhost:9093 \
        --request-topic hydraflowsim.streaming.paraemt-simulations \
        --all-topic hydraflowsim.streaming.all-simulations [--dry-run]

Same spec and same seed give the same samples. Each request is published to the inventory topic
and to the ParaEMT work topic.

Spec:

    {
      "system": "ieee39", "n": 1000, "seed": 1, "method": "monte_carlo",   # or "lhs", "grid"
      "fixed": {"t_end": 3.0},
      "params": {
        "gen_index": {"dist": "int_uniform", "low": 0, "high": 9},
        "gen_step":  {"dist": "int_uniform_excluding", "low": 0, "high": 9, "exclude": "gen_index"},
        "t_trip":    {"dist": "uniform", "low": 0.3, "high": 1.0, "round": 3},
        "t_step":    {"dist": "uniform", "low": 0.3, "high": 1.5, "round": 3},
        "dsp":       {"dist": "uniform", "low": -0.15, "high": 0.15, "round": 4}
      }
    }

For "grid", every param carries "values": [...] instead of a distribution and "n" is ignored.
"""

import argparse
import itertools
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Any

import numpy as np

METHODS = ("monte_carlo", "lhs", "grid")

# Kafka Connect schema for a ParaEMT campaign request.
ALL_REQUEST_SCHEMA: dict[str, Any] = {
    "type": "struct",
    "name": "paraemt-case.Value",
    "fields": [
        {"type": "string", "optional": False, "field": "id"},
        {"type": "int64", "name": "org.apache.kafka.connect.data.Timestamp", "version": 1, "optional": False, "field": "fecha"},
        {"type": "string", "optional": True, "field": "run_id"},
        {"type": "string", "optional": True, "field": "plugin"},
        {"type": "string", "optional": True, "field": "case"},
        {"type": "string", "optional": True, "field": "system"},
        {"type": "int32", "optional": True, "field": "gen_index"},
        {"type": "int32", "optional": True, "field": "gen_step"},
        {"type": "double", "optional": True, "field": "t_trip"},
        {"type": "double", "optional": True, "field": "t_step"},
        {"type": "double", "optional": True, "field": "dsp"},
        {"type": "double", "optional": True, "field": "t_end"},
        {"type": "int32", "optional": True, "field": "ds_rate"},
        {"type": "int64", "optional": True, "field": "seed"},
        {"type": "int32", "optional": True, "field": "sample_index"},
        {"type": "string", "optional": True, "field": "status"},
        {"type": "string", "optional": True, "field": "case_started_at"},
        {"type": "string", "optional": True, "field": "started_at"},
        {"type": "string", "optional": True, "field": "finished_at"},
        {"type": "float", "optional": True, "field": "duration_s"},
        {"type": "string", "optional": True, "field": "error"},
    ],
}


class SpecError(ValueError):
    pass


def _check_spec(spec: dict[str, Any]) -> None:
    if spec.get("method", "monte_carlo") not in METHODS:
        raise SpecError(f"method must be one of {METHODS}")
    if not spec.get("system"):
        raise SpecError("spec needs a system (for example 'ieee39')")
    if not isinstance(spec.get("params"), dict) or not spec["params"]:
        raise SpecError("spec needs a non-empty params object")
    if spec.get("method") != "grid":
        n = spec.get("n")
        if not isinstance(n, int) or n < 1:
            raise SpecError("n must be a positive integer")
    for name, d in spec["params"].items():
        if spec.get("method") == "grid":
            if not isinstance(d.get("values"), list) or not d["values"]:
                raise SpecError(f"grid param {name!r} needs a non-empty values list")
        elif d.get("dist") not in ("uniform", "int_uniform", "int_uniform_excluding", "choice"):
            raise SpecError(f"param {name!r}: unknown dist {d.get('dist')!r}")
        elif d["dist"] == "int_uniform_excluding" and d.get("exclude") not in spec["params"]:
            raise SpecError(f"param {name!r}: exclude must name another param")


def _unit_samples(method: str, n: int, rng: np.random.Generator) -> np.ndarray:
    """n samples in [0, 1): plain random, or one per stratum in random order (Latin hypercube)."""
    if method == "lhs":
        return (rng.permutation(n) + rng.random(n)) / n
    return rng.random(n)


def _draw(d: dict[str, Any], u: np.ndarray) -> list[Any]:
    kind = d["dist"]
    if kind == "uniform":
        values = d["low"] + u * (d["high"] - d["low"])
        return [round(float(v), d["round"]) if "round" in d else float(v) for v in values]
    if kind == "choice":
        options = d["values"]
        return [options[min(int(x * len(options)), len(options) - 1)] for x in u]
    span = d["high"] - d["low"] + 1
    if kind == "int_uniform_excluding":
        span -= 1  # one value is excluded; resolved after the referenced param is known
    return [d["low"] + min(int(x * span), span - 1) for x in u]


def expand(spec: dict[str, Any]) -> list[dict[str, Any]]:
    """Turn a spec into the list of case parameters (no ids yet). Deterministic for a given spec."""
    _check_spec(spec)
    method = spec.get("method", "monte_carlo")
    params = spec["params"]
    fixed = dict(spec.get("fixed", {}))
    if method == "grid":
        names = list(params)
        rows = [dict(zip(names, combo)) for combo in itertools.product(*(params[k]["values"] for k in names))]
    else:
        n = spec["n"]
        rng = np.random.default_rng(spec.get("seed", 0))
        columns: dict[str, list[Any]] = {}
        for name, d in params.items():  # insertion order keeps the draws reproducible
            columns[name] = _draw(d, _unit_samples(method, n, rng))
        for name, d in params.items():
            if d["dist"] == "int_uniform_excluding":
                base = columns[d["exclude"]]
                total = d["high"] - d["low"] + 1
                columns[name] = [
                    d["low"] + ((base[i] - d["low"]) + 1 + (columns[name][i] - d["low"])) % total
                    for i in range(n)
                ]
        rows = [{k: columns[k][i] for k in params} for i in range(n)]
    return [{"system": spec["system"], **fixed, **row} for row in rows]


def build_requests(spec: dict[str, Any], run_id: str | None = None) -> tuple[str, list[dict[str, Any]]]:
    """Full request payloads (ids, plugin, seed, status) for a spec."""
    run_id = run_id or str(uuid.uuid4())
    namespace = uuid.uuid5(uuid.NAMESPACE_URL, f"hydraflowsim-paraemt-run:{run_id}")
    requests = []
    for i, case in enumerate(expand(spec)):
        requests.append(
            {
                "id": str(uuid.uuid5(namespace, str(i))),
                "fecha": int(datetime.now(timezone.utc).timestamp() * 1000),
                "run_id": run_id,
                "plugin": "paraemt",
                "case": f"paraemt_{spec['system']}",
                "seed": int(spec.get("seed", 0)),
                "sample_index": i,
                **case,
                "status": "queued",
                "case_started_at": None,
                "started_at": None,
                "finished_at": None,
                "duration_s": None,
                "error": None,
            }
        )
    return run_id, requests


def submit(
    spec: dict[str, Any],
    producer: Any,
    request_topic: str,
    all_topic: str,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Publish a campaign. `producer` is a confluent_kafka.Producer."""
    run_id, requests = build_requests(spec, run_id)
    delivery_errors: list[str] = []
    def delivery_report(error: Any, message: Any) -> None:
        if error is not None:
            delivery_errors.append(f"{message.topic()}: {error}")

    for payload in requests:
        value = json.dumps({"schema": ALL_REQUEST_SCHEMA, "payload": payload})
        for topic in (all_topic, request_topic):
            while True:
                try:
                    producer.produce(topic=topic, value=value, on_delivery=delivery_report)
                    break
                except BufferError:
                    producer.poll(0.1)
        producer.poll(0)
    pending = producer.flush(60)
    if pending or delivery_errors:
        raise RuntimeError(
            f"Kafka publication incomplete: {pending} messages pending; "
            + "; ".join(delivery_errors[:5])
        )
    return {"run_id": run_id, "n": len(requests), "request_topic": request_topic}


def make_router(producer: Any, request_topic: str, all_topic: str):
    """FastAPI router: POST /campaigns/paraemt with the specification as JSON."""
    from fastapi import APIRouter, Body, HTTPException

    router = APIRouter()

    @router.post("/campaigns/paraemt")
    async def start_paraemt_campaign(spec: dict = Body(...)):
        try:
            return submit(spec, producer, request_topic, all_topic)
        except SpecError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

    return router


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("spec", help="path to the JSON spec")
    ap.add_argument("--kafka", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
    ap.add_argument("--request-topic", default=os.getenv("PARAEMT_REQUEST_TOPIC"))
    ap.add_argument("--all-topic", default=os.getenv("ALL_REQUEST_TOPIC"))
    ap.add_argument("--dry-run", action="store_true", help="print the first cases and exit")
    a = ap.parse_args(argv)
    with open(a.spec, encoding="utf8") as f:
        spec = json.load(f)
    if a.dry_run:
        run_id, requests = build_requests(spec)
        print(f"{len(requests)} cases, run_id={run_id}")
        for r in requests[:5]:
            print(json.dumps({k: r[k] for k in ("sample_index", "gen_index", "t_trip", "dsp") if k in r}))
        return 0
    if not (a.kafka and a.request_topic and a.all_topic):
        ap.error("--kafka, --request-topic and --all-topic are required (or their environment variables)")
    from confluent_kafka import Producer

    print(json.dumps(submit(spec, Producer({"bootstrap.servers": a.kafka}), a.request_topic, a.all_topic)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
