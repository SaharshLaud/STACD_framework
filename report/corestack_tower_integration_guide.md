# CoRE Stack Tower — Service Integration Guide

> **Audience:** Developer teams (bioacoustics, drone, DIY LULC, pest forecasting, etc.) onboarding a
> new service onto the CoRE Stack Tower platform hosted at `act4dws5`.
>
> **What this covers:** How to structure your Docker container, wire it into the platform's
> Airflow-based compute orchestration (STACD), handle auth, avoid CORS, and hand the deployment
> to Kapil sir.
>
> **Reference implementation:** The CoRE Stack LULC service (`salil2003/corestack-lulc`) is the
> worked example throughout this guide — it went through exactly this process and all code snippets
> are taken from or modelled on it. Use it as a template.

---

## 0. The platform in one picture

```
User's browser
     │
     │  (same-origin requests only — no CORS issues)
     ▼
Your frontend  (served by your backend or nginx inside the container)
     │
     │  (server-side calls — CORS does not apply here)
     ▼
Your backend API  ──────────────────────────────────► External compute
     │   (FastAPI / Express / Flask)                  (GEE, ML model, etc.)
     │
     │  POST /api/dag/run      ← trigger
     │  GET  /api/dag/status   ← poll
     ▼
STACD / Airflow  ──► DAG executes ──► calls back your /api/export-asset
                          │
                          ▼
                   registers STAC item ──► browsable catalog
```

Every service on the platform follows this shape. The browser calls **your** backend only
(same origin). Your backend is the only thing that calls Airflow. This is not optional — see §5.

---

## 1. What you hand to Kapil sir for deployment

Kapil sir is the deployer. He needs exactly these five things — nothing more, nothing less.

### 1a. A Docker image on Docker Hub

Your entire service must be a Docker image pushed to Docker Hub:

```
your-dockerhub-username/your-service-name:latest
```

Tell Kapil sir:
- The image name and tag
- Which packaging strategy you used (§2) — baked image or mounted code
- The update path: `docker pull` + restart, OR `git pull` + restart

### 1b. A `.env.example` file (committed to your repo)

Every environment variable your container needs, with a placeholder value and one-line comment.
Kapil sir copies this to `.env`, fills in real values, and passes it to Docker. Real values travel
out-of-band (Signal, in person) — **never in the repo, never in email**.

```bash
# .env.example

# === Paths (inside the container) ===
DATA_DIR=/app/data
LOG_DIR=/app/logs
GEE_SERVICE_ACCOUNT_KEY_PATH=/app/secrets/key.json

# === External compute ===
EE_PROJECT=<gee-project-id>
EE_ASSET_ROOT=projects/<gee-project>/assets/<folder>

# === STACD / Airflow ===
AIRFLOW_API_BASE=http://corestac-stacd-airflow:8002/api/v1
AIRFLOW_DAG_ID=your_dag_id
AIRFLOW_USERNAME=admin
AIRFLOW_PASSWORD=admin
# AIRFLOW_TOKEN=                     # use this instead of user/pass for bearer auth

# Airflow worker calls back into YOUR backend at this address:
CORESTACK_API_BASE=http://your-service:8000

# === Google Auth ===
GOOGLE_CLIENT_ID=your-google-oauth-client-id
GOOGLE_CLIENT_SECRET=your-google-oauth-client-secret
GOOGLE_REDIRECT_URI=https://www.cse.iitd.ernet.in/act4dws5/your-service/auth/callback

# === Networking ===
PUBLIC_BASE_URL=https://www.cse.iitd.ernet.in/act4dws5/your-service/
```

> **`AIRFLOW_API_BASE` with an empty value turns Airflow integration off cleanly** — the app
> still boots without it, which makes local development easier. Make your code handle this
> gracefully (return a clear error if someone calls `/api/dag/run` when Airflow is not configured).

### 1c. A `docker run` command or `docker-compose.yml`

The exact command Kapil sir copy-pastes. It follows the standard act4dws5 directory layout (§2b):

```bash
docker run -dit \
  --name your-service \
  --network corestack-network \
  --restart unless-stopped \
  -p 8XXX:8000 \
  -v /home/corestk/deployment/your-service/code:/app \
  -v /home/corestk/deployment/your-service/data:/app/data \
  -v /home/corestk/deployment/your-service/logs:/app/logs \
  -v /home/corestk/deployment/your-service/secrets/key.json:/app/secrets/key.json:ro \
  --env-file /home/corestk/deployment/your-service/.env \
  your-dockerhub-username/your-service-name:latest
```

Or equivalently as a `docker-compose.yml` (Salil's pattern — cleaner for Option B):

```yaml
services:
  your-service:
    image: your-dockerhub-username/your-service-name:latest
    pull_policy: always
    ports: ["8XXX:8000"]
    env_file: [.env]
    volumes:
      - ./code:/app                                    # Option B only; drop for Option A
      - ./data:/app/data
      - ./logs:/app/logs
      - ./secrets/key.json:/app/secrets/key.json:ro
    networks: [corestack-network]
    restart: unless-stopped

networks:
  corestack-network:
    external: true
```

Key rules:
- **Always `--network corestack-network`** — without this your container cannot reach Airflow,
  GeoServer, or any other service by name.
- **Never hardcode a port already in use.** Ask Kapil sir for a free port before picking one.
- **Drop mounts that don't apply** — no credential file? Drop that `-v` line. Baked image? Drop
  the `code` mount.
- **`--host 0.0.0.0` in your server startup command** — not `127.0.0.1`. Without this the
  container listens only on loopback and nothing can reach it, even on the same machine.

### 1d. An nginx location block

Kapil sir manages nginx on act4dws5. Give him the path prefix you want and the internal port:

```nginx
location /your-service/ {
    proxy_pass http://your-service:8000/;   # trailing slash strips the prefix
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_read_timeout 3600s;               # increase for long-running compute
}
```

The trailing slash on `proxy_pass` is not optional — it strips `/your-service/` before
forwarding, so your app receives `/api/...` not `/your-service/api/...`.

### 1e. Secrets, out-of-band

Credential files (GEE service account JSON, API tokens) go to Kapil sir via Signal or in person.
Never in email, never in the repo. Tell Kapil sir the exact filename and where to place it
(`secrets/key.json` by default).

---

## 2. Packaging strategy — baked image vs mounted code

Choose one. Document it in your README. The deployer must know which one you used.

### Option A — Code baked into the image (simpler, good for stable services)

The Dockerfile copies everything in. Image rebuild required for every code change.

```dockerfile
FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends git libgomp1 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

Update path: `docker pull` → `docker stop` → `docker run` (Kapil sir does this).

### Option B — Dependencies image + code mounted at runtime (faster iteration)

The image contains only the Python/Node dependencies. Code is a git checkout bind-mounted in.
This is what the CoRE Stack LULC service uses:

```dockerfile
FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends git libgomp1 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY deploy/requirements-docker.txt /tmp/requirements.txt   # deps only — no code
RUN pip install --no-cache-dir -r /tmp/requirements.txt
EXPOSE 8000
CMD ["uvicorn", "backend:app", "--app-dir", "src", "--host", "0.0.0.0", "--port", "8000"]
```

Update path: `git pull` in `code/` → `docker restart your-service` (no image rebuild).

**Rebuild the image only when dependencies change** (`requirements.txt` or `package.json`).

> State your choice in the README with the exact update command. The most common deployer
> confusion is not knowing whether `git pull` is enough or whether a new image is needed.

---

## 2b. Standard directory layout on act4dws5

Every service lives under `/home/corestk/deployment/your-service/`. This is not negotiable —
Kapil sir expects this layout and all mount paths in §1c are based on it.

```
/home/corestk/deployment/
└── your-service/
    ├── code/           ← git checkout of your repo (Option B only)
    │                      Kapil sir runs: git clone <your-repo> code/
    ├── data/           ← persistent output files (survive container restarts)
    │                      GEE exports, processed rasters, DB files, user uploads
    ├── logs/           ← API execution logs (readable from host without exec-ing in)
    ├── secrets/        ← credential files; never in the repo; sent to Kapil sir out-of-band
    │   └── key.json
    └── .env            ← real env values; gitignored; Kapil sir fills this from .env.example
```

### What goes where

**`code/`** — Only for Option B. Kapil sir clones your repo here. `git pull` inside this folder
updates the running app on next restart. For Option A this folder is not used.

**`data/`** — Everything your service writes that must survive container restarts: GEE asset
paths, processed files, SQLite databases, user uploads. Your app writes to `/app/data/` inside
the container (which maps here). Kapil sir can inspect outputs without going inside the container.
Tell Kapil sir whether this starts empty or needs seed files.

**`logs/`** — All API execution logs (see §8). Your app writes to `/app/logs/api.log` inside
the container (maps here on host). This is how the team audits who ran what without exec-ing in.

**`secrets/`** — GEE service account JSON, API tokens, certificates. Kapil sir places files here
after you send them out-of-band. Mounted read-only into the container.

**`.env`** — Kapil sir copies your `.env.example` here and fills in real values. Gitignored.

### Your app must read all paths from env vars

```python
# config.py — Salil's pattern, works identically inside Docker and locally
import os

DATA_DIR    = os.getenv("DATA_DIR",    "/app/data")
LOG_DIR     = os.getenv("LOG_DIR",     "/app/logs")
GEE_KEY     = os.getenv("GEE_SERVICE_ACCOUNT_KEY_PATH", "/app/secrets/key.json")
```

Add `DATA_DIR`, `LOG_DIR`, and credential paths to your `.env.example` with their in-container
defaults. The same image then works locally (with different mounts) and on act4dws5.

### Ship a one-command credential check

```bash
python config.py
# success: "EE auth OK — project: your-project-id"
# failure: "ERROR: Could not authenticate. Check GEE_SERVICE_ACCOUNT_KEY_PATH and file mount."
```

This lets Kapil sir verify credentials independently of starting the whole app. Document it in your
README. It is the fastest way to catch the most common deployment failure (env var set but file
not mounted).

---

## 3. Configuration — everything from environment variables

Nothing host-specific is hardcoded. Every URL, port, credential path, and secret is read from
the environment. This is what makes the same Docker image work on your laptop, on act4dws5, and
on any future server.

```python
# config.py
import os

# Paths
DATA_DIR             = os.getenv("DATA_DIR",    "/app/data")
LOG_DIR              = os.getenv("LOG_DIR",     "/app/logs")
GEE_KEY_PATH         = os.getenv("GEE_SERVICE_ACCOUNT_KEY_PATH")

# Airflow — empty string = feature disabled
AIRFLOW_API_BASE     = os.getenv("AIRFLOW_API_BASE", "")
AIRFLOW_USERNAME     = os.getenv("AIRFLOW_USERNAME", "")
AIRFLOW_PASSWORD     = os.getenv("AIRFLOW_PASSWORD", "")
AIRFLOW_TOKEN        = os.getenv("AIRFLOW_TOKEN",    "")
AIRFLOW_DAG_ID       = os.getenv("AIRFLOW_DAG_ID",  "")

# Where the Airflow worker reaches THIS backend
CORESTACK_API_BASE   = os.getenv("CORESTACK_API_BASE", "")

# Google auth
GOOGLE_CLIENT_ID     = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_REDIRECT_URI  = os.getenv("GOOGLE_REDIRECT_URI")
```

**Fail loudly for required credentials** — if `GEE_KEY_PATH` is missing and the app needs it,
raise an error at startup with a clear message. Silent failures that surface later are harder to
debug than an immediate crash with a helpful message.

---

## 4. Credential files — two things must both be true

If your service uses a key file (GEE service account JSON, a certificate, etc.):

1. An env var points to the path inside the container:
   `GEE_SERVICE_ACCOUNT_KEY_PATH=/app/secrets/key.json`
2. The file is actually mounted at that path:
   `-v /home/corestk/deployment/your-service/secrets/key.json:/app/secrets/key.json:ro`

Setting the env var without mounting the file is the most common credential failure. The process
looks, finds nothing, and reports "not authorized" — with no indication that the file is simply
absent.

---

## 5. CORS — why the browser must never call Airflow directly

**Never have your frontend call the Airflow API directly.**

When a browser makes a request to a different origin (different host or port), it first sends a
CORS preflight. Airflow does not return the `Access-Control-Allow-Origin` header the browser
requires, so the browser blocks the call — even though `curl` from a terminal works fine. This
produces the confusing error: "works in Postman, fails in the browser."

The correct architecture:

```
Browser  ──► POST /your-service/api/dag/run     ──► Your backend  ──► Airflow API (trigger)
Browser  ──► GET  /your-service/api/dag/status  ──► Your backend  ──► Airflow API (poll)
```

Your backend acts as a proxy to Airflow. The browser calls your own backend (same origin after
nginx) — no CORS issue. Airflow credentials stay server-side, never exposed to the browser.

### The Airflow client module (`src/airflow_client.py`)

This is Salil's pattern — copy it into your project and change the config imports:

```python
# src/airflow_client.py
import requests
import config

def _auth():
    """Choose basic or bearer auth based on which env vars are set."""
    if config.AIRFLOW_TOKEN:
        return None, {"Authorization": f"Bearer {config.AIRFLOW_TOKEN}"}
    if config.AIRFLOW_USERNAME:
        return (config.AIRFLOW_USERNAME, config.AIRFLOW_PASSWORD), {}
    return None, {}

def trigger_conf(conf: dict) -> dict:
    """Trigger a DAG run. Returns the full Airflow dagRun response."""
    auth, headers = _auth()
    url = f"{config.AIRFLOW_API_BASE}/dags/{config.AIRFLOW_DAG_ID}/dagRuns"
    r = requests.post(url, json={"conf": conf}, auth=auth, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()   # contains dag_run_id, state, etc.

def run_state(run_id: str) -> str | None:
    """Poll a DAG run. Returns 'queued' | 'running' | 'success' | 'failed'."""
    auth, headers = _auth()
    url = f"{config.AIRFLOW_API_BASE}/dags/{config.AIRFLOW_DAG_ID}/dagRuns/{run_id}"
    r = requests.get(url, auth=auth, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json().get("state")
```

### The same-origin proxy endpoints (in your backend)

```python
# src/backend.py (FastAPI example)
from fastapi import FastAPI, Request
import airflow_client

app = FastAPI()

@app.post("/api/dag/run")
async def dag_run(request: Request):
    """Frontend calls this. Backend forwards to Airflow. No CORS."""
    body = await request.json()
    conf = body.get("conf", body)   # accept conf wrapped or unwrapped
    resp = airflow_client.trigger_conf(conf)
    return {"dag_run_id": resp.get("dag_run_id"), "state": resp.get("state")}

@app.get("/api/dag/status")
def dag_status(run_id: str):
    """Frontend polls this. Backend polls Airflow. No CORS."""
    state = airflow_client.run_state(run_id)
    return {
        "dag_run_id": run_id,
        "state": state,
        "done": state in ("success", "failed"),
        "success": state == "success",
    }
```

### Frontend polling (JavaScript)

```javascript
async function triggerAndPoll(params) {
    // Step 1 — trigger via YOUR backend (same origin, no CORS)
    const triggerResp = await fetch('/your-service/api/dag/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ conf: params }),
    });
    const { dag_run_id } = await triggerResp.json();

    // Step 2 — poll via YOUR backend until terminal state
    while (true) {
        await new Promise(r => setTimeout(r, 3000));   // wait 3 s between polls
        const status = await fetch(
            `/your-service/api/dag/status?run_id=${encodeURIComponent(dag_run_id)}`
        ).then(r => r.json());

        if (status.success) return status;
        if (status.state === 'failed') throw new Error('DAG run failed');
        // else 'running' or 'queued' — keep polling
    }
}
```

### The raw Airflow REST calls (for reference / curl testing)

```bash
# Trigger a DAG run
curl -u admin:admin \
  -H "Content-Type: application/json" \
  -X POST "http://corestac-stacd-airflow:8002/api/v1/dags/your_dag_id/dagRuns" \
  -d '{"conf": {"param1": "value1", "execution_type": "fullexec"}}'
# → { "dag_run_id": "manual__2026-...", "state": "queued", ... }

# Poll run state
curl -u admin:admin \
  "http://corestac-stacd-airflow:8002/api/v1/dags/your_dag_id/dagRuns/<dag_run_id>"
# → { "state": "running" | "success" | "failed", ... }
```

> **If you get 401:** Airflow's REST API needs `basic_auth` enabled. Inside the Airflow container:
> ```bash
> sed -i 's/auth_backends = .*/auth_backends = airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session/' /opt/airflow/airflow.cfg
> pkill -f "airflow webserver" && airflow webserver -p 8080 &
> ```

---

## 6. The "do the work" endpoint — what the DAG calls back into

When the DAG runs, it calls **your backend's compute endpoint** (e.g. `POST /api/export-asset`).
This endpoint does the actual work (calls GEE, runs the model, etc.) and returns a STAC item.

### What your endpoint receives

The DAG forwards the `conf` dict it was triggered with, possibly wrapped in an extra envelope.
Write your endpoint to handle both forms, and ignore keys you don't need:

```python
# The keys your algorithm actually needs
_COMPUTE_KEYS = {"region", "year", "base_scheme"}

@app.post("/api/export-asset")
async def export_asset(request: Request):
    body = await request.json()
    # unwrap conf envelope if present
    params = body.get("conf", body)
    # keep only the keys this algorithm uses; coerce types
    region = params.get("region")
    year   = int(params.get("year", 2024))
    scheme = params.get("base_scheme", "indiasat")

    # ... run your compute ...
    stac_item = build_stac_item(result)
    return {
        "status": "success",
        "asset_id": [result.asset_path],
        "stac_items": [stac_item],   # always a list, even if one item
    }
```

### What your endpoint must return

STACD reads `stac_items` from the response. The format is strict:

```json
{
  "status": "success",
  "asset_id": ["path/to/output/file"],
  "stac_items": [
    {
      "type": "Feature",
      "stac_version": "1.1.0",
      "id": "unique-item-id-per-run",
      "geometry": {
        "type": "Polygon",
        "coordinates": [[[lon1, lat1], [lon2, lat2], "..."]]
      },
      "bbox": [min_lon, min_lat, max_lon, max_lat],
      "properties": {
        "datetime": "2024-01-01T00:00:00Z",
        "start_datetime": "2024-01-01T00:00:00Z",
        "end_datetime": "2024-12-31T23:59:59Z",
        "title": "Human-readable title",
        "description": "What this output is"
      },
      "assets": {
        "data": {
          "href": "/api/your-service/results/output.geojson",
          "type": "application/geo+json",
          "title": "Main output file",
          "roles": ["data"]
        }
      },
      "links": []
    }
  ]
}
```

**Critical rules:**
- `stac_items` must be a **list**, even for a single item. Not a bare object.
- Each item must have `"type": "Feature"` — STACD skips anything else.
- `asset_id` at the top level must be a **list of strings**.
- `id` must be **unique per run** — use a combination of project ID, region, year, timestamp.
- Do **not** include a `stacd` block — STACD generates its own provenance from the database.

### Async variant (for long-running compute)

If your compute takes more than ~30 seconds, return immediately with a task ID and let the
caller poll:

```python
@app.post("/api/export-asset")
async def export_asset(request: Request, wait: bool = True):
    body = await request.json()
    task = start_background_task(body)
    if not wait:
        return {"task_id": task.id, "status": "running"}
    result = await task.wait()
    return {"status": "success", "asset_id": [result.path], "stac_items": [result.stac]}

@app.get("/api/export-status")
def export_status(task_id: str):
    task = get_task(task_id)
    return {"task_id": task_id, "done": task.done, "status": task.status}
```

---

## 7. STACD integration — the YAML files you give Saharsh

To register your pipeline in STACD, give Saharsh three YAML files. He initialises the DAG via
the Airflow UI and it becomes triggerable via the REST API.

### `your_service_dag.yaml`

```yaml
--- !DAG
id: your_service_pipeline
name: Your Service Pipeline
version: "1"
group: your_group          # one of: corestack, drone, bioacoustic
description: "What this pipeline does."
params:
  - param_one
  - param_two
documentation:
  description: "Longer description for the STACD browser."
  outputs: "What files/layers are produced."
alg_type_nodes:
  - Your_Algo
dataset_type_nodes:
  - Your_Output

--- !Algorithm_Type
id: Your_Algo
name: Your Algorithm Name
description: "What the algorithm does."
params:
  - param_one
  - param_two
input_datasets: []
outputs:
  - Your_Output

--- !Dataset_Type
id: Your_Output
name: Your Output Dataset
description: "What the output dataset contains."
format: GeoJSON            # or GEEAsset, GeoServerLayer, CSV, etc.
```

### `your_service_algorithm_repo.yaml`

```yaml
--- !Algorithm_Instance
type: Your_Algo
version: "1"
date: 2026-01-01
assets:
  code: "https://github.com/your-org/your-repo"
execution_modes:
  api:
    enabled: true
    priority: 1
    url: "http://your-service:8000/api/export-asset"  # container name, internal port
  docker:
    enabled: false
```

> The URL must use the **Docker container name** as hostname (not `localhost`, not a LAN IP).
> The port must be the container's **internal** port (e.g. `8000`), not the host-mapped port.
> Both containers are on `corestack-network` so Docker resolves the name automatically.

### `your_service_dataset_repo.yaml`

```yaml
# No root datasets required for this pipeline.
```

(Only needed if your pipeline has pre-existing input datasets to register, like the Pan-India
MWS dataset for the CoRE Stack LULC pipeline.)

### What Saharsh does with these

Saharsh uploads the YAMLs into the running Airflow container and initialises the DAG via the
"Initialize Workflow" UI at `http://act4dws5/airflow/stacd_initialize_workflow/`. After that:
- The DAG appears in the Airflow dashboard
- It is triggerable via the REST API (§5)
- STAC items it produces appear in the catalog browser

---

## 8. What changed in an existing app to make it work (Salil's summary)

Starting from a plain FastAPI app, these were the concrete additions. The whole integration is
small and self-contained:

| What | Where | Why |
|---|---|---|
| `src/airflow_client.py` | new file | the two REST calls to Airflow (trigger + poll) |
| Two proxy endpoints | `src/backend.py` | `POST /api/dag/run`, `GET /api/dag/status` — so browser never calls Airflow |
| Airflow env vars | `config.py` | `AIRFLOW_API_BASE`, `AIRFLOW_USERNAME/PASSWORD/TOKEN`, `AIRFLOW_DAG_ID`, `CORESTACK_API_BASE` |
| STAC response shape | `POST /api/export-asset` | returns `{status, asset_id, stac_items}` — STACD reads this |
| Poll loop | `src/static/app.js` | UI triggers run, stores `dag_run_id`, polls until `success`/`failed` |
| Three YAML files | `deploy/stacd/` | registers the pipeline in STACD |
| Dockerfile | `Dockerfile` | deps-only image, `--host 0.0.0.0`, code+data bind-mounted |

None of these required changes to the core algorithm. The algorithm code stayed untouched.

---

## 9. Google authentication

Every service must integrate Google OAuth. Do not build your own user database.

### Flow

```
User clicks "Sign in with Google"
     │
     ▼
Backend redirects to Google OAuth consent screen
     │
     ▼
Google redirects back to: /your-service/auth/callback?code=...
     │
     ▼
Backend exchanges code for token, reads user email + name from Google
     │
     ▼
Backend creates a session, logs the login event (§10)
     │
     ▼
User is authenticated — email/name in session for all subsequent requests
```

### FastAPI implementation

```python
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
import httpx, os, config

app = FastAPI()

@app.get("/auth/login")
def login():
    params = (
        f"client_id={config.GOOGLE_CLIENT_ID}"
        f"&redirect_uri={config.GOOGLE_REDIRECT_URI}"
        f"&response_type=code"
        f"&scope=openid%20email%20profile"
    )
    return RedirectResponse(
        f"https://accounts.google.com/o/oauth2/auth?{params}"
    )

@app.get("/auth/callback")
async def callback(request: Request, code: str):
    async with httpx.AsyncClient() as client:
        token = await client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "code": code,
                "client_id": config.GOOGLE_CLIENT_ID,
                "client_secret": config.GOOGLE_CLIENT_SECRET,
                "redirect_uri": config.GOOGLE_REDIRECT_URI,
                "grant_type": "authorization_code",
            },
        )
        user_info = await client.get(
            "https://www.googleapis.com/oauth2/v3/userinfo",
            headers={"Authorization": f"Bearer {token.json()['access_token']}"},
        )
    user = user_info.json()
    # store user["email"], user["name"] in your session
    # log_api_call(user["email"], user["name"], "/auth/callback", {}, "login")
    return RedirectResponse("/your-service/")
```

The `GOOGLE_REDIRECT_URI` must be registered in Google Cloud Console under your OAuth client's
"Authorised redirect URIs." Ask Kapil sir for the client credentials for the act4dws5 deployment.

---

## 10. Logging

Every API call must be logged with the authenticated user's details. This is a hard requirement
from Adi sir so the team can audit who ran what queries.

### Minimum fields per log entry

```python
import logging, json
from datetime import datetime, timezone

logger = logging.getLogger("your_service")

def log_api_call(
    user_email: str,
    user_name:  str,
    endpoint:   str,
    params:     dict,
    status:     str,   # "triggered" | "success" | "failed"
):
    logger.info(json.dumps({
        "timestamp":  datetime.now(timezone.utc).isoformat(),
        "user_email": user_email,
        "user_name":  user_name,
        "endpoint":   endpoint,
        "params":     params,   # input params — not secrets or tokens
        "status":     status,
    }))
```

Call this at the start of every API handler (status `"triggered"`) and again on completion
(status `"success"` or `"failed"`).

### Writing to disk

```python
# at app startup
import logging, os

logging.basicConfig(
    filename=os.path.join(os.getenv("LOG_DIR", "/app/logs"), "api.log"),
    level=logging.INFO,
    format="%(asctime)s %(message)s",
)
```

The `LOG_DIR` env var maps to `/home/corestk/deployment/your-service/logs/` on the host.
Kapil sir reads logs there without exec-ing into the container.

---

## 11. The three addresses — use the right one from the right place

| Caller | Address to use | Example |
|---|---|---|
| Browser (user's machine) | Public nginx URL | `https://www.cse.iitd.ernet.in/act4dws5/your-service/` |
| Frontend → backend | Nginx-proxied relative path (same origin) | `/your-service/api/dag/run` |
| Backend → another container | Container name + **internal** port | `http://corestac-stacd-airflow:8002` |
| Backend → host machine | `host.docker.internal` | `http://host.docker.internal:8123` |
| Kapil sir on the server | `localhost` + published port | `http://localhost:8XXX` |

**Never use a LAN IP (`192.168.x.x`, `10.x.x.x`) in code or YAML files.** LAN IPs are
environment-specific, change when containers restart, and break container-to-container
communication where Docker DNS (container names) works perfectly.

### Check which network a container is on

```bash
docker inspect your-container \
  --format '{{json .NetworkSettings.Networks}}' | python3 -m json.tool
```

If it shows `corestack-network` your container can reach all other services on that network
by name.

---

## 12. End-to-end verification sequence

Verify inside-out — each step must pass before the next one makes sense:

```bash
# 1. App boots
curl http://localhost:8000/api/health
# → {"status": "ok"}

# 2. Credentials work
python config.py
# → "EE auth OK — project: your-project-id"

# 3. Compute endpoint works independently of Airflow
curl -X POST http://localhost:8000/api/export-asset \
  -H "Content-Type: application/json" \
  -d '{"region": [...], "year": "2024"}'
# → {"status": "success", "asset_id": [...], "stac_items": [...]}

# 4. Airflow trigger works (from backend's perspective)
curl -u admin:admin \
  -X POST "http://corestac-stacd-airflow:8002/api/v1/dags/your_dag_id/dagRuns" \
  -H "Content-Type: application/json" \
  -d '{"conf": {"param1": "value1", "execution_type": "fullexec"}}'
# → {"dag_run_id": "manual__...", "state": "queued"}

# 5. Full flow — trigger via your proxy, poll until success
curl -X POST http://localhost:8000/api/dag/run \
  -H "Content-Type: application/json" \
  -d '{"conf": {"param1": "value1"}}'
# → {"dag_run_id": "manual__...", "state": "queued"}
# poll /api/dag/status?run_id=... until state == "success"
```

---

## 13. Pre-handoff checklist

Before handing anything to Kapil sir, verify every item:

**Code & packaging**
- [ ] Image builds and runs locally without errors
- [ ] `git status --short` shows no untracked files the app imports
- [ ] `.env.example` lists every variable with description and placeholder
- [ ] `.env` is in `.gitignore` — verify with `git check-ignore .env`
- [ ] README states the packaging strategy and exact update command
- [ ] Server starts with `--host 0.0.0.0`

**Standard directory layout**
- [ ] Told Kapil sir: git repo URL + branch for `code/` (or N/A if baked image)
- [ ] Told Kapil sir: what goes in `data/` initially (empty, or seed files)
- [ ] Told Kapil sir: credential filenames for `secrets/`
- [ ] App reads `DATA_DIR`, `LOG_DIR`, credential paths from env vars
- [ ] App writes outputs to `/app/data/`, logs to `/app/logs/` — not just stdout
- [ ] One-command credential check exists and is in the README

**Configuration**
- [ ] Every URL, port, and secret is from env — nothing hardcoded
- [ ] Credential files are both env-var-referenced AND bind-mounted
- [ ] Empty `AIRFLOW_API_BASE` disables Airflow gracefully (app still boots)

**Networking**
- [ ] Container is on `corestack-network`
- [ ] URLs in YAML and config use container names, not LAN IPs
- [ ] No LAN IP anywhere in code or YAML

**CORS / architecture**
- [ ] Frontend never calls Airflow directly — all Airflow calls go through your backend
- [ ] Frontend never calls another container directly

**STACD integration**
- [ ] `/api/export-asset` returns `stac_items` as a list, `asset_id` as a list
- [ ] No `stacd` block in the response
- [ ] STAC item `id` is unique per run
- [ ] YAML files given to Saharsh; DAG initialised in Airflow
- [ ] Algorithm repo YAML URL uses container name + internal port

**Auth & logging**
- [ ] Google OAuth integrated — email/name in session after login
- [ ] Every API call logged: timestamp, email, name, endpoint, params, status
- [ ] Logs written to bind-mounted `/app/logs/` path

**For Kapil sir**
- [ ] Docker image name and tag
- [ ] Exact `docker run` command or `docker-compose.yml`
- [ ] `.env.example`
- [ ] Nginx location block with path prefix and internal port
- [ ] Secrets sent out-of-band

---

## 14. Getting help

| Who | What they handle |
|---|---|
| **Saharsh** | STACD/Airflow integration, DAG YAML files, STAC item response format, catalog browser, triggering and polling patterns |
| **Kapil sir** | Docker deployment on act4dws5, nginx config, port assignments, Google OAuth client credentials for act4dws5 |
| **Salil** | Reference implementation questions — how his CoRE Stack LULC service is structured |
| **Adi sir** | Architecture decisions, data archiving policy (test runs vs production runs) |

For STACD questions, bring: your API response JSON, your proposed YAML files, and the Docker
container name your backend will run as on act4dws5.

---

*Last updated: September 2026 — CoRE Stack Tower platform, act4dws5 workstation.*
