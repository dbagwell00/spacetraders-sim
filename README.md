# spacetraders-sim — Sim Lab

Interactive web app to **launch, watch, and review** SpaceTraders sim runs in
real time. Runs as its own deployment (separate pod/svc) so heavy sims never
steal CPU from the live trading agent.

## How it works

```
browser ──SSE──> lab_server (Quart) ──subprocess──> python -m sim stream ──JSONL──┐
   ▲                   │                                                          │
   └───── live ticks ──┴── persists each run to Postgres (sim_runs) ◀────────────┘
```

- The image builds **FROM `registry.dlb.im/spacetraders:latest`**, inheriting
  the full `sim/` + `planner/` + `worker/` code. The lab is just `lab_server.py`
  + `sim_lab.html` on top.
- Each run spawns `python -m sim stream` as a **subprocess** — mandatory,
  because the sim harness patches `time.time`/`asyncio.sleep` process-globally
  and could never share a process with anything else.
- `sim stream` emits one JSON frame per tick on stdout; the server relays them
  to the browser over Server-Sent Events and downsamples a credits-over-time
  series into the `sim_runs` row for later review.

## Endpoints

| Route | Purpose |
|---|---|
| `GET /` | the Sim Lab UI |
| `POST /api/sim/launch` | create a run (params), returns `{run_id}` |
| `GET /api/sim/stream/<id>` | SSE — spawns the subprocess, streams frames, persists |
| `POST /api/sim/stop/<id>` | kill a running subprocess |
| `GET /api/sim/runs` | list past runs |
| `GET /api/sim/runs/<id>` | one run: params + final metrics + chart series |
| `GET /healthz` | liveness + running run-ids |

## Config (env)

| Var | Default | Notes |
|---|---|---|
| `DATABASE_URL` | — (required) | Postgres for the `sim_runs` table + the `live` seeder |
| `PORT` | `8080` | |
| `SIM_CMD` | `python -m sim stream` | the streaming entrypoint |
| `SIM_CWD` | `/app` | where the sim code lives in the image |
| `SIM_LAB_MAX_CONCURRENT` | `2` | concurrent sims (each is CPU-heavy) |
| `SIM_LAB_SERIES_POINTS` | `240` | max stored chart points per run |

## Deploy

Manifests live in `talos-k8s-argocd` under
`k8s/apps/spacetraders-sim/overlays/talos-cilium/`. ArgoCD's ApplicationSet
auto-discovers them. Deployed into the `spacetraders` namespace so it reaches
`spacetraders-pg-rw` and reuses the `spacetraders-pg-app` secret.

## Local dev

```bash
DATABASE_URL=postgresql://… SIM_CWD=/path/to/spacetraders \
  hypercorn lab_server:app --bind 0.0.0.0:8080
```
Then open http://localhost:8080.
