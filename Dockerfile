# Sim Lab — thin web layer ON TOP of the spacetraders image, so it inherits the
# full sim/ + planner/ + worker/ codebase and can spawn `python -m sim stream`.
# Rebuild this whenever the base image changes to pick up planner/sim updates.
FROM registry.dlb.im/spacetraders:latest

WORKDIR /app

# quart + asyncpg + hypercorn already ship in the base image (the hub web uses
# them); install is a near no-op but keeps the dependency intent explicit.
COPY requirements.txt /tmp/sim-lab-requirements.txt
RUN pip install --no-cache-dir -r /tmp/sim-lab-requirements.txt

# The lab server + UI live alongside the inherited code at /app so the
# subprocess (`python -m sim stream`, cwd=/app) resolves the packages.
COPY lab_server.py sim_lab.html /app/

EXPOSE 8080

CMD ["hypercorn", "lab_server:app", "--bind", "0.0.0.0:8080", "--workers", "1"]
