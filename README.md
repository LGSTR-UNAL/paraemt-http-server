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

The included campaign is for the IEEE 39 bus system and generates 1,000 paired event cases with
Monte Carlo sampling. It selects the tripped generator uniformly from the ten machines and the
governor step generator uniformly from the other nine. The trip time is sampled uniformly from
0.3 to 1.0 s, the step time from 0.3 to 1.5 s, and the step magnitude from −0.15 to +0.15 pu.
The simulation horizon is 10 s, so both event orderings are represented. The fixed seed and
complete specification are in [`examples/campaign_ieee39.json`](examples/campaign_ieee39.json).

Example request body:

```json
{
  "system": "ieee39",
  "n": 1000,
  "seed": 1,
  "method": "monte_carlo",
  "fixed": {"t_end": 10.0},
  "params": {
    "gen_index": {"dist": "int_uniform", "low": 0, "high": 9},
    "gen_step": {"dist": "int_uniform_excluding", "low": 0, "high": 9, "exclude": "gen_index"},
    "t_trip": {"dist": "uniform", "low": 0.3, "high": 1.0, "round": 3},
    "t_step": {"dist": "uniform", "low": 0.3, "high": 1.5, "round": 3},
    "dsp": {"dist": "uniform", "low": -0.15, "high": 0.15, "round": 4}
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
