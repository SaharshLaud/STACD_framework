# CoRE Stack Tower — Service Integration Guide

> **Audience:** Developer teams (bioacoustics, drone, DIY LULC, pest forecasting, etc.) onboarding a new service onto the CoRE Stack Tower platform hosted at `act4dws5`.
>
> **What this covers:** How to structure your Docker container, wire it into the platform's Airflow-based compute orchestration (STACD), handle auth, avoid CORS, and hand the deployment to Kapil sir.

---

## 0. The platform in one picture

```
User's browser
     │
     │  (same-origin requests only)
     ▼
Your frontend (nginx, port 80 inside container)
     │
     │  (server-side calls — no CORS restriction)
     ▼
Your backend API  ──────────────────────────────────► External compute
     │                                                 (GEE, ML model, etc.)
     │  (trigger)
     ▼
STACD / Airflow  ──► DAG runs compute ──► registers STAC item ──► catalog
     │
     ▼
Your backend polls Airflow for run state, returns to frontend
```

Every service on the platform follows this shape. The key constraint is **the browser never talks to Airflow directly** — your backend does. This is not optional; see §5 for why.

---

## 1. What you hand to Kapil sir for deployment

Kapil sir is the deployer. He needs exactly these things from you — nothing more, nothing less.

### 1a. A single Docker image

Your entire service (frontend + backend, or just a backend) must be packaged as a Docker image and pushed to Docker Hub (or another registry Kapil sir can pull from).

```
your-dockerhub-username/your-service-name:latest
```

Tell Kapil sir:
- The image name and tag
- Which strategy you used (see §2) — baked image vs mounted code
- The update path: "to update, pull new image and restart" OR "to update, git pull on server and restart"

### 1b. A `.env.example` file

Every environment variable your container needs, listed with a one-line description and a placeholder value. Kapil sir copies this, fills in the real values, and passes it to Docker at run time. Real values travel to Kapil sir out-of-band (Signal, in person) — **never in the repo**.

```bash
# .env.example

# === Networking ===
BACKEND_BASE_URL=http://your-service-backend:8000   # internal container URL, used by frontend
PUBLIC_BASE_URL=https://www.cse.iitd.ernet.in/act4dws5/your-service/  # public URL after nginx

# === External compute auth ===
GEE_SERVICE_ACCOUNT_KEY_PATH=/app/secrets/key.json  # path inside container; mount the file in
GEE_PROJECT_ID=your-gee-project-id

# === STACD / Airflow (if using compute orchestration) ===
AIRFLOW_BASE_URL=http://corestac-stacd-airflow:8002  # internal Docker network address
AIRFLOW_DAG_ID=your_dag_id
AIRFLOW_USERNAME=admin
AIRFLOW_PASSWORD=admin

# === Google Auth ===
GOOGLE_CLIENT_ID=your-google-oauth-client-id
GOOGLE_CLIENT_SECRET=your-google-oauth-client-secret
GOOGLE_REDIRECT_URI=https://www.cse.iitd.ernet.in/act4dws5/your-service/auth/callback
```

### 1c. A `docker run` command (or `docker-compose.yml`)

The exact command to start your container. Kapil sir should be able to copy-paste this. It follows the **standard act4dws5 directory layout** (see §2b):

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

Key points:
- **Always use `--network corestack-network`** — this is the shared Docker network on act4dws5. Without it, your container cannot reach Airflow, GeoServer, or other services by name.
- **Never hardcode a port that's already taken.** Ask Kapil sir for an available port before you pick one.
- **Credential files** must be bind-mounted in (`-v`) — setting an env var pointing to a path is not enough if the file isn't actually inside the container.
- **Only include mounts that apply to your service** — if you have no credential file, drop that `-v` line. If your image is fully baked (Option A below), drop the `code` mount.

### 1d. A one-line nginx location block

Kapil sir manages nginx. All services are reverse-proxied through it at `act4dws5`. Tell him the path prefix you want:

```nginx
location /your-service/ {
    proxy_pass http://your-service:8000/;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_read_timeout 3600s;   # increase if your compute takes a long time
}
```

The trailing slash on `proxy_pass` is important — it strips the `/your-service/` prefix before forwarding.

---

## 2. Packaging strategy: baked image vs mounted code

Choose one and document it.

### Option A — Code baked into the image (recommended for production)

The Dockerfile copies your code in. Every code change = rebuild + push + pull.

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

Update path: `docker pull` + `docker stop` + `docker run`.

### Option B — Dependencies image + code mounted at runtime

The image only has the runtime/dependencies. Code is bind-mounted from a git checkout on the server.

```bash
docker run -dit \
  -v /home/corestk/deployment/your-service/code:/app \
  your-image:latest
```

Update path: `git pull` inside `code/` + `docker restart your-service`.

> Whichever you choose, **state it explicitly in your README**. The single most confusing thing for a deployer is not knowing whether `git pull` updates the app or whether they must rebuild an image.

---

## 2b. Standard directory layout on act4dws5

Every service on the workstation follows the same folder structure under `/home/corestk/deployment/`. This is not optional — Kapil sir expects this layout and all the mount paths in §1c are based on it.

```
/home/corestk/deployment/
└── your-service/
    ├── code/           ← your git checkout (bind-mounted into container as /app)
    │                      only needed for Option B (mounted code strategy)
    ├── data/           ← persistent output data (bind-mounted as /app/data)
    │                      files your service writes that must survive container restarts
    ├── logs/           ← log files (bind-mounted as /app/logs)
    │                      never write logs only to stdout — they vanish on restart
    ├── secrets/        ← credential files (key.json, tokens etc.)
    │                      never committed to git; sent to Kapil sir out-of-band
    │   └── key.json
    └── .env            ← real environment values; gitignored; sent to Kapil sir out-of-band
```

### What goes where

**`code/`** — Your application source code. Kapil sir does `git clone your-repo code/` here. For Option A (baked image) this folder is not needed since code is inside the image. For Option B this is the live checkout the container runs from — `git pull` here updates the app.

**`data/`** — Anything your service writes that needs to persist across container restarts or be accessible from the host. Examples: GEE export files, processed rasters, database files, uploaded user files. Your app should write to `/app/data/` inside the container, which maps to this folder on the host. Kapil sir can inspect outputs here without going inside the container.

**`logs/`** — All API execution logs (see §8). Your app writes to `/app/logs/api.log` inside the container, which lands here on the host. This is how Kapil sir and Adi sir can audit who ran what queries without exec-ing into the container.

**`secrets/`** — Credential files that cannot go in the repo. GEE service account JSON, API tokens, certificates. Kapil sir creates this folder and places the files here after you send them out-of-band. The folder is mounted read-only (`-v .../secrets/key.json:/app/secrets/key.json:ro`).

**`.env`** — Real environment variable values. Kapil sir copies your `.env.example`, fills in the real values, and saves it here. Never in the repo.

### Tell Kapil sir explicitly

When you hand over your service, tell Kapil sir:
- What to put in `data/` initially (empty, or seed files he needs to place)
- What credential files go in `secrets/` and what to name them
- What the container writes to `data/` so he knows what to expect

### Your app must read paths from env vars

The paths inside the container (`/app/data`, `/app/logs`, `/app/secrets/key.json`) must come from environment variables, not be hardcoded. This way the same image works locally (with different mounts) and on act4dws5:

```python
import os

DATA_DIR    = os.getenv("DATA_DIR",    "/app/data")
LOG_DIR     = os.getenv("LOG_DIR",     "/app/logs")
GEE_KEY     = os.getenv("GEE_SERVICE_ACCOUNT_KEY_PATH", "/app/secrets/key.json")
```

Add these to your `.env.example`:
```bash
DATA_DIR=/app/data
LOG_DIR=/app/logs
GEE_SERVICE_ACCOUNT_KEY_PATH=/app/secrets/key.json
```

---

## 3. All configuration via environment variables

**Rule: nothing host-specific is hardcoded.** Every URL, port, key path, and secret is read from the environment.

```python
# Python example
import os

AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL", "")          # empty = feature disabled
GEE_KEY_PATH     = os.getenv("GEE_SERVICE_ACCOUNT_KEY_PATH")  # required; crash early if missing
PUBLIC_BASE_URL  = os.getenv("PUBLIC_BASE_URL", "/")
```

A good default for an optional integration is empty/off — the app still boots without it, which makes it easier to run locally and test. A required credential should fail loudly at startup with a clear message, not silently produce wrong results later.

### The `.env` pattern

```
your-service/
├── .env.example    ← committed to repo, placeholders only
├── .env            ← gitignored, real values, never in repo
└── ...
```

Verify `.env` is gitignored:
```bash
git check-ignore .env   # prints ".env" if correctly ignored
```

Real secrets travel to Kapil sir out-of-band. The repo only ever shows `.env.example`.

---

## 4. Credential files (GEE service accounts, API keys)

If your external service uses a key file (e.g. a GEE service account JSON), two things must both be true:

1. An env var points to the path: `GEE_SERVICE_ACCOUNT_KEY_PATH=/app/secrets/key.json`
2. The file is mounted into the container at that exact path:
   ```bash
   -v /home/corestk/deployment/your-service/key.json:/app/secrets/key.json:ro
   ```

Setting the env var without mounting the file is the most common credential failure — the process looks, finds nothing, and falls back to "not authorized."

### Add a one-command credential check

Ship a small command in your repo that only tests auth and prints OK or the exact error:

```bash
# example
python -m your_service.check_auth
# success: "GEE auth OK — project: your-project-id"
# failure: "ERROR: Could not authenticate. Check GEE_SERVICE_ACCOUNT_KEY_PATH and file mount."
```

This lets Kapil sir verify credentials independently of the whole app. Document it in the README.

---

## 5. CORS — why the browser must never call Airflow directly

This is the most common architecture mistake. **Never have your frontend call the Airflow API directly.**

When a browser makes a request to a different origin (different host or port), it first sends a CORS preflight. Airflow does not return the headers the browser requires (`Access-Control-Allow-Origin`), so the browser blocks the response — even though `curl` from a terminal works fine. This produces the confusing error: "works in Postman, fails in the browser."

The correct architecture:

```
Browser  ──► /your-service/api/trigger-run  ──► Your backend  ──► Airflow API
                                                      │
Browser  ──► /your-service/api/run-status   ──► Your backend  ──► Airflow API (poll)
```

Your backend acts as a proxy to Airflow. Since the browser calls your own backend (same origin after nginx), there is no CORS issue. Your backend holds the Airflow credentials server-side — they are never exposed to the browser.

### Triggering a DAG run (backend → Airflow)

```python
import requests
import os

AIRFLOW_BASE = os.getenv("AIRFLOW_BASE_URL", "http://corestac-stacd-airflow:8002")
AIRFLOW_USER = os.getenv("AIRFLOW_USERNAME", "admin")
AIRFLOW_PASS = os.getenv("AIRFLOW_PASSWORD", "admin")
DAG_ID       = os.getenv("AIRFLOW_DAG_ID")

def trigger_dag(conf: dict) -> str:
    """Trigger a DAG run. Returns the dag_run_id."""
    resp = requests.post(
        f"{AIRFLOW_BASE}/api/v1/dags/{DAG_ID}/dagRuns",
        json={"conf": conf},
        auth=(AIRFLOW_USER, AIRFLOW_PASS),
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["dag_run_id"]
```

### Polling run status (backend → Airflow)

```python
def get_run_status(dag_run_id: str) -> str:
    """Returns 'success', 'failed', 'running', or 'queued'."""
    resp = requests.get(
        f"{AIRFLOW_BASE}/api/v1/dags/{DAG_ID}/dagRuns/{dag_run_id}",
        auth=(AIRFLOW_USER, AIRFLOW_PASS),
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["state"]
```

### Frontend polling pattern

```javascript
async function triggerAndPoll(params) {
    // 1. Trigger via YOUR backend — same origin, no CORS
    const { dag_run_id } = await fetch('/your-service/api/trigger-run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(params),
    }).then(r => r.json());

    // 2. Poll via YOUR backend until terminal state
    while (true) {
        await new Promise(r => setTimeout(r, 3000));  // wait 3s between polls
        const { state, result } = await fetch(
            `/your-service/api/run-status/${dag_run_id}`
        ).then(r => r.json());

        if (state === 'success') return result;
        if (state === 'failed')  throw new Error('DAG run failed');
        // else 'running' or 'queued' — keep polling
    }
}
```

---

## 6. Integrating with STACD — how your API response must look

STACD (the STAC + DAG lineage system) registers your compute outputs as STAC items and builds a browsable catalog. For this to work, your backend API must return a response in a specific shape.

### Required response format

```json
{
  "asset_id": ["path/to/your/output/file.geojson"],
  "stac_items": [
    {
      "type": "Feature",
      "stac_version": "1.1.0",
      "id": "unique-item-id",
      "geometry": {
        "type": "Polygon",
        "coordinates": [[[lon1, lat1], [lon2, lat2], ...]]
      },
      "bbox": [min_lon, min_lat, max_lon, max_lat],
      "properties": {
        "datetime": "2024-01-01T00:00:00Z",
        "start_datetime": "2024-01-01T00:00:00Z",
        "end_datetime": "2024-12-31T23:59:59Z",
        "title": "Human-readable title",
        "description": "What this output is",
        "project_id": "optional-project-identifier"
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

- `stac_items` must be an **array**, even if there is only one item. Not a single object.
- Each item must have `"type": "Feature"` — STACD skips anything that isn't a Feature.
- `asset_id` at the top level must be a **list of strings** — the paths to your actual output files.
- Do **not** include a `stacd` block — STACD builds its own provenance from the database. Any `stacd` key in your response is ignored.
- `id` in each STAC item must be unique across all your runs. Use a combination of project ID, run number, or timestamp to ensure this.

### What happens after your API returns

STACD's Airflow task automatically:
1. Reads `stac_items` from your response
2. Injects provenance fields (`stacd:dag_id`, `stacd:run_id`, `stacd:algo_version`, etc.)
3. Writes each item as a JSON file to the catalog directory
4. Registers a dataset instance in the STACD database
5. Makes the item browsable in the STAC browser at `http://act4dws5/stac/`

You do not need to write any catalog files yourself.

### YAML files you must provide to Saharsh

To register your pipeline in STACD, Saharsh needs three YAML files from you:

**`your_service_dag.yaml`** — describes the pipeline:
```yaml
--- !DAG
id: your_service_pipeline
name: Your Service Pipeline
version: "1"
group: your_group          # one of: corestack, drone, bioacoustic
description: "What this pipeline does and where outputs are browsable."
params:
  - param_one              # list every parameter your API accepts
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
input_datasets: []         # list input dataset type IDs if any
outputs:
  - Your_Output

--- !Dataset_Type
id: Your_Output
name: Your Output Dataset
description: "What the output dataset contains."
format: GeoJSON            # or GEEAsset, GeoServerLayer, CSV, etc.
```

**`your_service_algorithm_repo.yaml`** — the actual API endpoint:
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
    url: "http://your-container-name:8000/api/v1/your-endpoint"
  docker:
    enabled: false
```

**`your_service_dataset_repo.yaml`** — root datasets if any (leave as comment if none):
```yaml
# No root datasets required for this pipeline.
```

> The URL in `algorithm_repo.yaml` must use the **Docker container name** as the hostname, not `localhost` or a LAN IP. Both containers are on `corestack-network`, so Docker resolves the container name automatically. The port must be the container's **internal** port (e.g. `8000`), not the host-mapped port (e.g. `8123`).

### Triggering your DAG via Airflow API

```
POST http://corestac-stacd-airflow:8002/api/v1/dags/your_service_pipeline/dagRuns
Authorization: Basic admin:admin
Content-Type: application/json

{
  "conf": {
    "param_one": "value1",
    "param_two": "value2",
    "execution_type": "fullexec"
  }
}
```

Response:
```json
{
  "dag_run_id": "manual__2026-08-20T07:00:00+00:00",
  "state": "queued"
}
```

Poll for completion:
```
GET http://corestac-stacd-airflow:8002/api/v1/dags/your_service_pipeline/dagRuns/{dag_run_id}
```

---

## 7. Google authentication

Every service must integrate Google OAuth. Do not build your own user database.

### Flow

```
User clicks "Sign in with Google"
     │
     ▼
Your backend redirects to Google OAuth
     │
     ▼
Google redirects back to: /your-service/auth/callback?code=...
     │
     ▼
Your backend exchanges code for token, reads user email + name
     │
     ▼
Your backend creates a session, logs the event (see §8)
     │
     ▼
User is authenticated — email/name available in session
```

### Required env vars

```bash
GOOGLE_CLIENT_ID=...       # from Google Cloud Console, OAuth 2.0 credentials
GOOGLE_CLIENT_SECRET=...   # same
GOOGLE_REDIRECT_URI=https://www.cse.iitd.ernet.in/act4dws5/your-service/auth/callback
```

The redirect URI must be registered in the Google Cloud Console under your OAuth client's "Authorized redirect URIs." Ask Kapil sir for the client ID/secret for the act4dws5 deployment, or create your own in the shared GCP project.

### Minimal Python (FastAPI) example

```python
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
import httpx, os

app = FastAPI()

GOOGLE_CLIENT_ID     = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_REDIRECT_URI  = os.getenv("GOOGLE_REDIRECT_URI")

@app.get("/auth/login")
def login():
    params = (
        f"client_id={GOOGLE_CLIENT_ID}"
        f"&redirect_uri={GOOGLE_REDIRECT_URI}"
        f"&response_type=code"
        f"&scope=openid%20email%20profile"
    )
    return RedirectResponse(f"https://accounts.google.com/o/oauth2/auth?{params}")

@app.get("/auth/callback")
async def callback(request: Request, code: str):
    async with httpx.AsyncClient() as client:
        token_resp = await client.post("https://oauth2.googleapis.com/token", data={
            "code": code,
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "redirect_uri": GOOGLE_REDIRECT_URI,
            "grant_type": "authorization_code",
        })
        token_data = token_resp.json()
        user_info = await client.get(
            "https://www.googleapis.com/oauth2/v3/userinfo",
            headers={"Authorization": f"Bearer {token_data['access_token']}"}
        )
    user = user_info.json()
    # store user["email"], user["name"] in session
    # log the login event (see §8)
    return RedirectResponse("/your-service/")
```

---

## 8. Logging

Every API execution must be logged with the authenticated user's details. Adi sir specifically requires this so the team knows who ran what queries.

### Minimum log entry per API call

```python
import logging
from datetime import datetime, timezone

logger = logging.getLogger("your_service")

def log_api_call(user_email: str, user_name: str, endpoint: str, params: dict, status: str):
    logger.info({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_email": user_email,
        "user_name": user_name,
        "endpoint": endpoint,
        "params": params,        # log the input params, not secrets
        "status": status,        # "triggered", "success", "failed"
    })
```

### Writing logs to disk (so Kapil sir can read them)

Logs must be written to a volume-mounted path so they survive container restarts and are readable on the host:

```bash
# in your docker run command:
-v /home/corestk/deployment/your-service/logs:/app/logs
```

```python
# in your app:
import logging
logging.basicConfig(
    filename="/app/logs/api.log",
    level=logging.INFO,
    format="%(asctime)s %(message)s",
)
```

Tell Kapil sir the log path so he knows where to look. The host path will be `/home/corestk/deployment/your-service/logs/api.log`.

---

## 9. The three addresses — know which one to use from where

This is the most common networking confusion.

| Caller | Address to use | Example |
|---|---|---|
| Browser on user's machine | Public nginx URL | `https://www.cse.iitd.ernet.in/act4dws5/your-service/` |
| Your frontend → your backend | Nginx-proxied path (same origin) | `/your-service/api/...` |
| Your backend → another container | Container name + internal port | `http://corestac-stacd-airflow:8002` |
| Your backend → host machine | `host.docker.internal` | `http://host.docker.internal:8123` |
| Kapil sir on the server → any container | `localhost` + published port | `http://localhost:8XXX` |

**Never use a LAN IP (like `192.168.x.x`) in your code or YAML files.** These change, are environment-specific, and break when containers are on the same Docker network where container names work. Use container names inside Docker networks.

### Checking which network a container is on

```bash
docker inspect your-container --format '{{json .NetworkSettings.Networks}}' | python3 -m json.tool
```

If it shows `corestack-network`, your container can reach all other containers on that network by name.

---

## 10. Pre-handoff checklist for your team

Before you give anything to Kapil sir, verify these:

**Code & packaging**
- [ ] Docker image builds and runs locally
- [ ] All new files are committed — `git status --short` shows no `??` entries that the app imports
- [ ] `.env.example` lists every variable with description and placeholder
- [ ] `.env` is in `.gitignore`
- [ ] Update path is documented (git pull vs image rebuild)

**Standard directory layout (for Kapil sir)**
- [ ] Told Kapil sir what goes in `code/` (git repo URL + branch, or N/A if baked image)
- [ ] Told Kapil sir what goes in `data/` initially (empty OK, or seed files specified)
- [ ] Told Kapil sir what credential files go in `secrets/` and exact filenames
- [ ] App reads `DATA_DIR`, `LOG_DIR`, credential paths from env vars — not hardcoded
- [ ] App writes all persistent outputs to `/app/data/` (maps to `data/` on host)
- [ ] App writes all logs to `/app/logs/` (maps to `logs/` on host) — not just stdout

**Configuration**
- [ ] Every URL, port, and credential is read from env — nothing hardcoded
- [ ] Credential files are bind-mounted, not just referenced by env var
- [ ] A one-command credential check exists and is documented in the README

**Networking**
- [ ] Server binds `0.0.0.0`, not `127.0.0.1`
- [ ] Container is on `corestack-network` (or will be when Kapil sir runs it)
- [ ] URLs in YAML/config use container names, not LAN IPs

**CORS / architecture**
- [ ] Frontend never calls Airflow API directly — all Airflow calls go through your backend
- [ ] Frontend never calls another container directly — all cross-service calls go through your backend

**STACD integration** (if using compute orchestration)
- [ ] API response has `stac_items` as an array, `asset_id` as a list
- [ ] No `stacd` block in the response (STACD builds this itself)
- [ ] YAML files (`dag.yaml`, `algorithm_repo.yaml`, `dataset_repo.yaml`) given to Saharsh
- [ ] API URL in `algorithm_repo.yaml` uses container name + internal port

**Auth & logging**
- [ ] Google OAuth integrated — user email/name available in session after login
- [ ] Every API call is logged with timestamp, user email, user name, endpoint, params, status
- [ ] Logs written to a bind-mounted path, not just stdout

**For Kapil sir**
- [ ] Docker image name and tag
- [ ] Exact `docker run` command (or `docker-compose.yml`)
- [ ] `.env.example` with all variables
- [ ] Nginx location block (path prefix + internal port)
- [ ] Real secrets sent out-of-band (not in repo, not in email)

---

## 11. Getting help

| Who | What they handle |
|---|---|
| **Saharsh** | STACD/Airflow integration, DAG YAML files, STAC item format, catalog browser |
| **Kapil sir** | Docker deployment on act4dws5, nginx config, port assignments, Google OAuth client credentials |
| **Adi sir** | Architecture decisions, data policy (archiving test runs vs real runs) |

For STACD questions, bring: your API response JSON, your proposed YAML files, and the Docker container name your backend will run as on act4dws5.

---

*Last updated: September 2026 — CoRE Stack Tower platform, act4dws5 workstation.*
