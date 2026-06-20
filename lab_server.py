"""Sim Lab — interactive web app to launch, watch, and review sim runs.

Runs as its OWN deployment (separate pod/svc) so heavy sims never steal CPU
from the live trading agent. Built FROM the spacetraders image, so it has the
full `sim/` + `planner/` + `worker/` codebase available; it spawns
`python -m sim stream` as a SUBPROCESS (mandatory — the harness patches
time.time/asyncio.sleep process-globally) and relays the per-tick JSONL frames
to the browser over Server-Sent Events, persisting each run to Postgres.

Routes:
  GET  /                       -> the Sim Lab UI
  POST /api/sim/launch         -> create a run row (params), return {run_id}
  GET  /api/sim/stream/<id>    -> SSE: spawn the subprocess, relay frames, persist
  POST /api/sim/stop/<id>      -> kill a running subprocess
  GET  /api/sim/runs           -> list past runs (summary)
  GET  /api/sim/runs/<id>      -> one run: params + metrics + chart series
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any

import asyncpg
from quart import Quart, Response, jsonify, request, send_file

app = Quart(__name__, static_folder=None)

DATABASE_URL = os.environ["DATABASE_URL"]
SIM_CMD = os.environ.get("SIM_CMD", "python -m sim stream").split()
SIM_CWD = os.environ.get("SIM_CWD", "/app")
MAX_CONCURRENT = int(os.environ.get("SIM_LAB_MAX_CONCURRENT", "2"))
# Keep the persisted chart series small: at most this many points per run.
SERIES_MAX_POINTS = int(os.environ.get("SIM_LAB_SERIES_POINTS", "240"))

_pool: asyncpg.Pool | None = None
# run_id -> live subprocess (so /stop can signal it).
_procs: dict[int, asyncio.subprocess.Process] = {}
_run_sem = asyncio.Semaphore(MAX_CONCURRENT)

_DDL = """
CREATE TABLE IF NOT EXISTS sim_runs (
    id          bigserial PRIMARY KEY,
    label       text,
    created_at  double precision NOT NULL,
    status      text NOT NULL DEFAULT 'pending',   -- pending|running|done|failed|stopped
    params      jsonb NOT NULL,
    final       jsonb,
    series      jsonb,
    wall_secs   double precision
);
"""


async def _db() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4)
        async with _pool.acquire() as con:
            await con.execute(_DDL)
    return _pool


# ── allowed launch params (whitelist -> CLI args) ─────────────────────────────

_SCENARIOS = {"live", "greenfield", "gate-rush"}


def _build_cli_args(params: dict[str, Any]) -> list[str]:
    """Translate a validated params dict into `sim stream` CLI args."""
    scenario = params.get("scenario", "live")
    if scenario not in _SCENARIOS:
        raise ValueError(f"bad scenario {scenario!r}")
    args = ["--scenario", scenario,
            "--hours", str(float(params.get("hours", 2.0))),
            "--seed", str(int(params.get("seed", 0))),
            "--tick-every-secs", str(float(params.get("tick_every_secs", 5.0))),
            "--api-rate-limit", str(float(params.get("api_rate_limit", 0.0))),
            "--n-systems", str(int(params.get("n_systems", 8)))]
    if not params.get("evolving_market_scores", True):
        args.append("--no-evolving-scores")
    for kv in params.get("cfg", []) or []:
        if isinstance(kv, str) and "=" in kv:
            args += ["--cfg", kv]
    return args


# ── routes ────────────────────────────────────────────────────────────────────

@app.route("/")
async def index() -> Any:
    return await send_file(Path(__file__).parent / "sim_lab.html")


@app.route("/api/sim/launch", methods=["POST"])
async def launch() -> Any:
    params = await request.get_json() or {}
    try:
        _build_cli_args(params)  # validate now; raises on bad input
    except (ValueError, TypeError) as e:
        return jsonify({"error": str(e)}), 400
    pool = await _db()
    label = (params.get("label") or "").strip() or None
    row = await pool.fetchrow(
        "INSERT INTO sim_runs(label, created_at, status, params) "
        "VALUES($1,$2,'pending',$3) RETURNING id",
        label, time.time(), json.dumps(params))
    return jsonify({"run_id": row["id"]})


@app.route("/api/sim/stream/<int:run_id>")
async def stream(run_id: int) -> Any:
    pool = await _db()
    rec = await pool.fetchrow("SELECT params, status FROM sim_runs WHERE id=$1", run_id)
    if rec is None:
        return jsonify({"error": "no such run"}), 404
    if rec["status"] != "pending":
        return jsonify({"error": f"run already {rec['status']}"}), 409
    params = json.loads(rec["params"])
    cli = _build_cli_args(params)

    async def gen():
        await _run_sem.acquire()
        proc = None
        series: list[dict[str, Any]] = []
        final: dict[str, Any] | None = None
        every = 1
        wall0 = time.time()
        try:
            proc = await asyncio.create_subprocess_exec(
                *SIM_CMD, *cli, cwd=SIM_CWD,
                # Raise the StreamReader buffer past the 64KB default: the live
                # `start` frame carries every mapped system's coords (a single
                # JSON line that can run to hundreds of KB), which overran the
                # default and killed the stream with "chunk exceed the limit".
                limit=2 ** 23,  # 8 MB
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL)
            _procs[run_id] = proc
            await pool.execute("UPDATE sim_runs SET status='running' WHERE id=$1", run_id)
            yield f"event: open\ndata: {json.dumps({'run_id': run_id})}\n\n"
            # The `live` seeder loads the whole prod snapshot (~1700 ships, every
            # market/waypoint) before the sim emits its first frame — 1-3 min of
            # silence. Tell the UI we're seeding so it doesn't look hung.
            yield ("data: " + json.dumps({
                "type": "seeding", "scenario": params.get("scenario")}) + "\n\n")

            tick_n = 0
            assert proc.stdout is not None
            while True:
                # Read with a timeout so we can heartbeat during the long silent
                # `live` seed (~1-3 min before the first frame). Without this the
                # idle SSE connection drops, EventSource reconnects, hits the now
                # non-pending run (409), and the UI sticks on "stream closed".
                try:
                    raw = await asyncio.wait_for(proc.stdout.readline(), timeout=15)
                except asyncio.TimeoutError:
                    yield ": ping\n\n"  # SSE comment — keeps the connection warm
                    continue
                if not raw:
                    break  # EOF: subprocess closed stdout
                line = raw.decode(errors="replace").strip()
                if not line:
                    continue
                try:
                    frame = json.loads(line)
                except json.JSONDecodeError:
                    continue  # stray non-JSON (shouldn't happen on stdout)
                yield f"data: {line}\n\n"
                ft = frame.get("type")
                if ft == "tick":
                    tick_n += 1
                    # Downsample into <= SERIES_MAX_POINTS for the stored chart.
                    if tick_n % every == 0:
                        series.append({"h": frame.get("hours"),
                                       "c": frame.get("credits"),
                                       "tr": frame.get("trades"),
                                       "api": frame.get("api")})
                        if len(series) > SERIES_MAX_POINTS:
                            series = series[::2]  # halve resolution, keep span
                            every *= 2
                elif ft == "done":
                    final = frame
                elif ft == "error":
                    final = frame
            await proc.wait()
            # A non-zero exit with no final frame = the subprocess crashed before
            # reporting. Surface the exit code so the UI shows an error rather
            # than hanging on "waiting for ticks". (stderr stays DEVNULL — piping
            # it undrained would deadlock, since the sim logs every tick.)
            if final is None and proc.returncode not in (0, None):
                final = {"type": "error", "phase": "exit",
                         "error": f"sim subprocess exited {proc.returncode}"}
                yield "data: " + json.dumps(final) + "\n\n"
            status = "done" if (final and final.get("type") == "done") else \
                ("failed" if (final and final.get("type") == "error") else "stopped")
            await pool.execute(
                "UPDATE sim_runs SET status=$2, final=$3, series=$4, wall_secs=$5 WHERE id=$1",
                run_id, status, json.dumps(final) if final else None,
                json.dumps(series), time.time() - wall0)
            yield f"event: end\ndata: {json.dumps({'status': status})}\n\n"
        except asyncio.CancelledError:
            # Browser disconnected — kill the subprocess so it doesn't orphan.
            if proc and proc.returncode is None:
                proc.kill()
            await pool.execute(
                "UPDATE sim_runs SET status='stopped', series=$2, wall_secs=$3 WHERE id=$1",
                run_id, json.dumps(series), time.time() - wall0)
            raise
        finally:
            _procs.pop(run_id, None)
            _run_sem.release()

    return Response(gen(), content_type="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/sim/stop/<int:run_id>", methods=["POST"])
async def stop(run_id: int) -> Any:
    proc = _procs.get(run_id)
    if proc is None:
        return jsonify({"error": "not running"}), 404
    if proc.returncode is None:
        proc.kill()
    return jsonify({"stopped": run_id})


@app.route("/api/sim/runs")
async def runs() -> Any:
    pool = await _db()
    rows = await pool.fetch(
        "SELECT id, label, created_at, status, params, final, wall_secs "
        "FROM sim_runs ORDER BY id DESC LIMIT 100")
    out = []
    for r in rows:
        out.append({
            "id": r["id"], "label": r["label"], "created_at": r["created_at"],
            "status": r["status"], "params": json.loads(r["params"]),
            "final": json.loads(r["final"]) if r["final"] else None,
            "wall_secs": r["wall_secs"],
        })
    return jsonify(out)


@app.route("/api/sim/runs/<int:run_id>")
async def run_detail(run_id: int) -> Any:
    pool = await _db()
    r = await pool.fetchrow("SELECT * FROM sim_runs WHERE id=$1", run_id)
    if r is None:
        return jsonify({"error": "no such run"}), 404
    return jsonify({
        "id": r["id"], "label": r["label"], "created_at": r["created_at"],
        "status": r["status"], "params": json.loads(r["params"]),
        "final": json.loads(r["final"]) if r["final"] else None,
        "series": json.loads(r["series"]) if r["series"] else [],
        "wall_secs": r["wall_secs"],
    })


_universe_coords: dict[str, list[int]] | None = None


@app.route("/api/universe")
async def universe() -> Any:
    """The live universe backdrop: every system's galaxy coords (cached — static
    per reset) plus the CURRENT real probe/hauler distribution from prod. The UI
    draws this on page load (like whater's map); a live sim then animates its
    own spread on top, picking up from this same state."""
    global _universe_coords
    pool = await _db()
    if _universe_coords is None:
        rows = await pool.fetch("SELECT symbol, x, y FROM systems")
        _universe_coords = {r["symbol"]: [r["x"], r["y"]] for r in rows}
    pos: dict[str, list[int]] = {}
    for s in await pool.fetch("SELECT role, position FROM ships"):
        sys = "-".join(s["position"].split("-")[:2])
        role = s["role"]
        idx = 0 if role in ("SATELLITE", "EXPLORER") else (
            1 if "HAULER" in role else None)
        if idx is None:
            continue
        pos.setdefault(sys, [0, 0])[idx] += 1
    return jsonify({"systems": _universe_coords, "pos": pos})


@app.route("/healthz")
async def healthz() -> Any:
    return jsonify({"ok": True, "running": list(_procs.keys())})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
