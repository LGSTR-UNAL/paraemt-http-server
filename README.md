# paraemt-http-server

HTTP service for submitting parameterized ParaEMT campaigns to the HydraFlowSim message broker.
The service expands one JSON specification into reproducible case requests and publishes each
request to the ParaEMT work topic and the all simulations inventory topic.

## API

`POST /campaigns/paraemt` accepts JSON with a `system`, a sampling `method` (`monte_carlo`,
`lhs`, or `grid`), a `seed`, and parameter definitions. `GET /health` is a liveness endpoint.
Campaign requests return a `run_id` and the number of generated requests after Kafka confirms
publication. Each message includes its UUID, `run_id`, parameters, and `fecha`, the creation time
encoded as epoch milliseconds for Kafka Connect.

Example:

```json
{
  "system": "ieee9",
  "n": 2,
  "seed": 42,
  "method": "monte_carlo",
  "fixed": {"t_end": 1.0},
  "params": {
    "gen_index": {"dist": "int_uniform", "low": 0, "high": 2},
    "t_trip": {"dist": "uniform", "low": 0.2, "high": 0.8, "round": 3}
  }
}
```

See [`examples/`](examples/) for complete campaign specifications and the accepted distributions.
The generated seed and sample index are included with each event so a campaign can be reproduced.

## Configuration

Copy `.env.example` and set `KAFKA_BOOTSTRAP_SERVERS`, `PARAEMT_REQUEST_TOPIC`, and
`ALL_REQUEST_TOPIC` for the deployment. The work topic and inventory topic must already exist.
The service publishes to the inventory topic before the work topic; Kafka publication errors
cause the request to fail rather than return a successful campaign response.

## Run

```sh
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
set -a && . ./.env && set +a
uvicorn src.main:app --host 0.0.0.0 --port 8000
```

Build the container with `docker build -t <registry>/paraemt-http-server:<tag> .`, then push it
to a container image repository reachable by the cluster. Deployment manifests belong to the
separate HydraFlowSim Kubernetes repository.
