# STACD Geospatial Dataflow Management — Deployment Guide

> **Docker Hub Image:** `saharshlaud/corestack-stacd-airflow:latest`  
> This guide walks you through deploying the full STACD stack on any machine with Docker installed, connecting it to your own backend API.

---

## What You're Deploying

Everything runs inside a single Docker container. Only your data folders are mounted from outside.

| Component | Where it runs | Port |
|---|---|---|
| Airflow Webserver + Scheduler | Inside Docker | 8080 |
| STACD Browser (Vue) | Inside Docker | 8081 |
| STAC Browser (Vue) | Inside Docker | 8082 |
| Dynamic Catalog Server | Inside Docker | 8002 |
| Your Backend API | Your own Docker container | Any (e.g. 9000) |

```
Your machine
├── Docker Container (saharshlaud/corestack-stacd-airflow)
│   ├── Airflow Webserver        → localhost:8080
│   ├── STACD Browser (Vue)      → localhost:8081
│   ├── STAC Browser (Vue)       → localhost:8082
│   └── Dynamic Catalog Server   → localhost:8002
│         ↑ reads from mounted data folder
│
├── ~/stacd_testing/stacd_catalog_data/   ← MOUNTED (DAG JSON outputs)
├── ~/stacd_testing/airflow_db/           ← MOUNTED (databases)
└── Your Backend API                      → localhost:9000 (or your port)
```

---

## Prerequisites

- Docker installed and running — nothing else needed on the host
- A Google OAuth Client ID and Secret (for login)
- Your backend API running and accessible

---

## Step 1 — Folder Structure

Create this folder structure on your machine:

```bash
mkdir -p ~/stacd_testing/stacd_catalog_data
mkdir -p ~/stacd_testing/airflow_db

# Create empty placeholder DB files
# (Docker requires files to exist before mounting them as files)
touch ~/stacd_testing/airflow_db/stacd_database.db
touch ~/stacd_testing/airflow_db/airflow.db
```

Your folder should look like:

```
~/stacd_testing/
├── stacd_catalog_data/       ← DAG JSON outputs written here
├── airflow_db/
│   ├── airflow.db            ← Airflow users, roles, DAG history (persists across restarts)
│   └── stacd_database.db     ← STACD datasets and algorithm runs
└── .env                      ← Environment variables (create in Step 2)
```

---

## Step 2 — Create the `.env` File

Create `~/stacd_testing/.env`:

```bash
cat > ~/stacd_testing/.env << 'EOF'
GOOGLE_CLIENT_ID=your_google_client_id_here
GOOGLE_CLIENT_SECRET=your_google_client_secret_here
AIRFLOW_HOME=/opt/airflow
AIRFLOW__WEBSERVER__SECRET_KEY=change_this_to_a_random_string_32chars
CATALOG_OUTPUT_DIR=/opt/airflow/stacd_catalog
CATALOG_BASE_URL=http://localhost:8002
CORESTACK_AUTH_TOKEN=your_backend_jwt_token_here
EOF
```

**Getting Google OAuth credentials:**
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a project → APIs & Services → Credentials → Create OAuth 2.0 Client ID
3. Application type: **Web application**
4. Authorized redirect URIs: `http://localhost:8080/oauth-authorized/google`
5. Copy the Client ID and Client Secret into the `.env` file

**Setting `CORESTACK_AUTH_TOKEN`:**  
This is the JWT token your backend API requires in the `Authorization` header. If your backend doesn't require auth, leave this blank.

**Generating a secret key:**
```bash
python3 -c "import secrets; print(secrets.token_hex(32))"
```
Paste the output as `AIRFLOW__WEBSERVER__SECRET_KEY`.

---

## Step 3 — Pull the Docker Image

```bash
docker pull saharshlaud/corestack-stacd-airflow:latest
```

---

## Step 4 — Start the Container

You need **2 terminals** running simultaneously.

### Terminal 1 — Start the Container

```bash
docker run -it \
  --name stacd-airflow \
  -p 0.0.0.0:8080:8080 \
  -p 0.0.0.0:8081:8081 \
  -p 0.0.0.0:8082:8082 \
  -p 0.0.0.0:8002:8002 \
  -v ~/stacd_testing/stacd_catalog_data:/opt/airflow/stacd_catalog \
  -v ~/stacd_testing/airflow_db/stacd_database.db:/opt/airflow/stacd/database/stacd_database.db \
  -v ~/stacd_testing/airflow_db/airflow.db:/opt/airflow/airflow.db \
  --env-file ~/stacd_testing/.env \
  --add-host=host.docker.internal:host-gateway \
  saharshlaud/corestack-stacd-airflow:latest
```

You should see:
```
Initializing STACD roles...
Initializing access_requests table...
Creating operator roles...
Starting STACD Browser on port 8081...
Starting STAC Browser on port 8082...
Starting Catalog Server on port 8002...
==========================================
  Browsers started.
  STACD Browser : http://localhost:8081
  STAC Browser  : http://localhost:8082
  To start Airflow:
    airflow webserver -p 8080
    airflow scheduler
==========================================
```

**What `--add-host=host.docker.internal:host-gateway` does:** Lets the container reach your backend API on the host machine using the hostname `host.docker.internal`. So if your backend is on port 9000, the DAG calls `http://host.docker.internal:9000/...`.

### Terminal 2 — Start Airflow

```bash
docker exec -it stacd-airflow bash
airflow webserver -p 8080 &
airflow scheduler &
```

Wait for:
```
Listening at: http://0.0.0.0:8080
```

---

## Step 5 — First Time Login and User Setup

### 5a. Open Airflow and Log In

Go to `http://localhost:8080`. You should see a **Sign in with Google** button. Click it and complete the OAuth flow.

### 5b. Find Your Username

After logging in you'll see:
> "Your user has no roles and/or permissions!"

This is expected on first login. Find your username:

```bash
docker exec -it stacd-airflow bash -c "airflow users list"
```

It will look like `google_107905055496200253347`.

### 5c. Assign Admin Role

```bash
# Replace with your username from above
docker exec -it stacd-airflow bash -c \
  "airflow users add-role -u google_107905055496200253347 -r Admin"
```

Go back to `http://localhost:8080` and log in again. You should land on the Airflow home page with full access.

> **Note:** This step only needs to be done once. Since `airflow.db` is mounted, your admin role persists across all future restarts.

---

## Step 6 — Connecting Your Backend API

### How the DAG calls your backend

Each DAG task calls your backend via HTTP POST. The container reaches your backend using `host.docker.internal` as the hostname.

For example, if your backend runs at port `9000`:
```
http://host.docker.internal:9000/api/v1/your_endpoint/
```

### Setting the Auth Token

If your backend requires a JWT token, set it in `.env`:
```
CORESTACK_AUTH_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

The DAG code reads this and passes it as:
```
Authorization: Bearer <CORESTACK_AUTH_TOKEN>
```

Verify it's available inside the container:
```bash
docker exec -it stacd-airflow bash -c "echo \$CORESTACK_AUTH_TOKEN"
```

### Pointing a DAG at your backend

In the YAML config for a DAG:
```yaml
execution_modes:
  api:
    enabled: true
    priority: 1
    url: http://host.docker.internal:9000/api/v1/your_endpoint/
```

---

## Step 7 — Triggering a DAG

1. Open `http://localhost:8080`
2. Find your DAG in the list
3. Click the **▶ Trigger** button
4. Fill in parameters (state, district, block, etc.)
5. Click **Trigger**

When complete, the system automatically:
- Writes STAC JSON files to `~/stacd_testing/stacd_catalog_data/datasets/`
- Writes STACD workflow JSON files to `~/stacd_testing/stacd_catalog_data/dags/`
- Records everything in `stacd_database.db`

---

## Step 8 — Viewing Results in the Browsers

**STAC Browser (datasets view):**
```
http://localhost:8082/?url=http://localhost:8002/datasets/catalog.json
```
Navigate: Root Catalog → State → District → Block → Collection → Individual Items

**STACD Browser (workflow view):**
```
http://localhost:8081/?url=http://localhost:8002/dags/catalog.json
```
Navigate: Root Catalog → DAG → Algorithms → Individual Algorithm → Version details

---

## Stopping and Restarting

### Stop everything
```bash
docker stop stacd-airflow && docker rm stacd-airflow
```

### Restart (all data persists — users, roles, DAG history, JSON files)
```bash
# Terminal 1 - container
docker run -it \
  --name stacd-airflow \
  -p 0.0.0.0:8080:8080 \
  -p 0.0.0.0:8081:8081 \
  -p 0.0.0.0:8082:8082 \
  -p 0.0.0.0:8002:8002 \
  -v ~/stacd_testing/stacd_catalog_data:/opt/airflow/stacd_catalog \
  -v ~/stacd_testing/airflow_db/stacd_database.db:/opt/airflow/stacd/database/stacd_database.db \
  -v ~/stacd_testing/airflow_db/airflow.db:/opt/airflow/airflow.db \
  --env-file ~/stacd_testing/.env \
  --add-host=host.docker.internal:host-gateway \
  saharshlaud/corestack-stacd-airflow:latest

# Terminal 2 - start airflow (inside container)
docker exec -it stacd-airflow bash
airflow webserver -p 8080 &
airflow scheduler &
```

On restart, you do **not** need to re-assign roles — everything persists via the mounted files.

### Moving to a different machine

Copy the entire `~/stacd_testing/` folder to the new machine, then run the same `docker pull` + `docker run` command. All users, roles, DAG history, and JSON files transfer with it.

---

## Port Reference

| Port | Service | URL |
|---|---|---|
| 8080 | Airflow Webserver | http://localhost:8080 |
| 8081 | STACD Browser | http://localhost:8081 |
| 8082 | STAC Browser | http://localhost:8082 |
| 8002 | Catalog Server (inside container) | http://localhost:8002 |

---

## What Each Volume Mount Does

```bash
-v ~/stacd_testing/stacd_catalog_data:/opt/airflow/stacd_catalog
```
DAG outputs (STAC and STACD JSON files) written by Airflow tasks appear on your host. The catalog server inside the container reads from this same path.

```bash
-v ~/stacd_testing/airflow_db/stacd_database.db:/opt/airflow/stacd/database/stacd_database.db
```
The STACD SQLite database — stores dataset instances, algorithm executions, DAG records. Persists across container restarts.

```bash
-v ~/stacd_testing/airflow_db/airflow.db:/opt/airflow/airflow.db
```
The Airflow SQLite database — stores users, roles, DAG run history, XCom data, connections. **This is what keeps your users and roles alive across restarts and machine transfers.**

---

## Troubleshooting

**"Your user has no roles" after login**  
Run Step 5c to assign the Admin role.

**"Can't find AUTH_USER_REGISTRATION_ROLE: STACD_Viewer"**  
The init script didn't run. Inside the container:
```bash
python3 stacd/database/init_stacd_roles.py
```

**"no such table: access_requests"**  
Inside the container:
```bash
python3 stacd/database/init_access_requests.py
```

**"Role CoreStack_Op does not exist"**  
Inside the container:
```bash
airflow roles create CoreStack_Op
airflow roles create Drone_Op
```

**DAG task fails with 401 Unauthorized**  
Your backend requires auth. Set `CORESTACK_AUTH_TOKEN` in `.env` and restart the container.

**DAG task fails with "Connection refused"**  
Your backend isn't reachable. Check:
1. Is your backend running?
2. Is the URL in the YAML using `host.docker.internal` not `localhost`?
3. Did you pass `--add-host=host.docker.internal:host-gateway` in `docker run`?

**Catalog server not responding on port 8002**  
Check if it started correctly:
```bash
docker exec -it stacd-airflow bash -c "curl http://localhost:8002/catalog.json"
```
If it fails, check logs:
```bash
docker logs stacd-airflow | grep -i "catalog\|8002\|flask"
```

**STAC Browser shows empty catalog**  
The catalog server is reading an empty DB. Check:
1. Did the DAG complete successfully?
2. Is the DB file mounted correctly?
```bash
docker inspect stacd-airflow | grep -A5 Mounts
```
3. Check DB has data:
```bash
sqlite3 ~/stacd_testing/airflow_db/stacd_database.db \
  "SELECT count(*) FROM dataset_instances;"
```

**Users lost after restart**  
You're not mounting `airflow.db`. Make sure the third `-v` mount is in your `docker run` command:
```bash
-v ~/stacd_testing/airflow_db/airflow.db:/opt/airflow/airflow.db
```

**`airflow` command not found on host**  
Always run airflow commands inside the container:
```bash
docker exec -it stacd-airflow bash -c "airflow <command>"
```
