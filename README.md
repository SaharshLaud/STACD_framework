# STACD Framework — Setup & Usage Guide

> **STACD** (SpatioTemporal Asset Catalog for Dataflows) is a YAML-driven geospatial workflow management system built on top of Apache Airflow. Users define their workflows as YAML files, and the system handles database initialization, DAG generation, and deployment — all from the Airflow dashboard.

## Table of Contents

- [Prerequisites](#prerequisites)
- [1. Install Apache Airflow](#1-install-apache-airflow)
- [2. Clone the STACD Repository](#2-clone-the-stacd-repository)
- [3. Setup Folder Structure](#3-setup-folder-structure)
- [4. Set Environment Variables](#4-set-environment-variables)
- [5. Initialize Airflow](#5-initialize-airflow)
- [6. Start Airflow](#6-start-airflow)
- [7. Initialize Your Workflow (via Plugin Dashboard)](#7-initialize-your-workflow-via-plugin-dashboard)
- [Airflow Variables](#airflow-variables)
- [8. Trigger Your Workflow](#8-trigger-your-workflow)
  - [Execution Primitives](#execution-primitives)
- [9. Triggering and Monitoring Airflow DAGs via the REST API](#9-triggering-and-monitoring-airflow-dags-via-the-rest-api)
- [10. Updating an Existing Workflow](#10-updating-an-existing-workflow)
- [11. Writing Your Own YAML Workflow](#11-writing-your-own-yaml-workflow)
- [12. Algorithm Response Handling](#12-algorithm-response-handling)
- [Troubleshooting](#troubleshooting)
- [Project Structure Reference](#project-structure-reference)

---

## Prerequisites

- **Python 3.10+**
- **pip** (latest)
- **Git**

---

## 1. Install Apache Airflow

```bash
# Create project folder
mkdir airflow_stacd && cd airflow_stacd

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Set AIRFLOW_HOME (IMPORTANT — all paths are relative to this)
export AIRFLOW_HOME=$(pwd)/airflow
mkdir -p $AIRFLOW_HOME

# Install Airflow 2.10.4 with matching constraints
AIRFLOW_VERSION=2.10.4
PYTHON_VERSION=$(python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")

pip install "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
```

---

## 2. Clone the STACD Repository

```bash
# Clone the dev branch into the project folder
git clone -b dev https://github.com/SaharshLaud/STACD_framework.git
```

---

## 3. Setup Folder Structure

Copy the `stacd` and `plugins` folders from the cloned repo into your Airflow home directory:

```bash
# Copy the STACD core module
cp -r STACD_framework/stacd $AIRFLOW_HOME/

# Copy the Airflow plugins (UI pages for STACD)
cp -r STACD_framework/plugins $AIRFLOW_HOME/
```

Your `$AIRFLOW_HOME` directory should now look like:

```
airflow/
├── stacd/                    # STACD core module
│   ├── database/             # DB models, operations, init script
│   ├── dag_generator/        # YAML → Python DAG generator
│   ├── stac_export/          # STAC-D catalog export
│   ├── generated_dags/       # Output directory for generated DAGs
│   ├── yaml_configs/         # Sample YAML workflow definitions
│   └── simple_docker_runner.py
├── plugins/                  # Airflow plugins (STACD dashboard pages)
│   ├── stacd_admin_plugin.py
│   ├── stacd_lineage_plugin.py
│   └── templates/
├── dags/                     # Airflow DAGs directory (auto-created)
├── airflow.cfg
└── airflow.db
```

---

## 4. Set Environment Variables

```bash
export AIRFLOW_HOME=$(pwd)/airflow
export PYTHONPATH=$AIRFLOW_HOME:$AIRFLOW_HOME/stacd:$PYTHONPATH
```

**To make this permanent**, add to your shell profile:

```bash
echo "export AIRFLOW_HOME=$(pwd)/airflow" >> ~/.bashrc
echo "export PYTHONPATH=\$AIRFLOW_HOME:\$AIRFLOW_HOME/stacd:\$PYTHONPATH" >> ~/.bashrc
source ~/.bashrc
```

---

## 5. Initialize Airflow

```bash
# Initialize the Airflow metadata database
airflow db init

# Create an admin user
airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com
```

You will be prompted to set a password.

---

## 6. Start Airflow

### Standalone mode (recommended for development):

```bash
cd airflow_stacd
source venv/bin/activate
export AIRFLOW_HOME=$(pwd)/airflow

airflow standalone
```

> **Note:** `airflow standalone` starts the webserver, scheduler, and triggerer in a single command. It will print a generated password for the `admin` user on first run.

### Production mode (two terminals):

**Terminal 1 — Scheduler:**
```bash
cd airflow_stacd
source venv/bin/activate
export AIRFLOW_HOME=$(pwd)/airflow
export PYTHONPATH=$AIRFLOW_HOME:$AIRFLOW_HOME/stacd:$PYTHONPATH

airflow scheduler
```

**Terminal 2 — Webserver:**
```bash
cd airflow_stacd
source venv/bin/activate
export AIRFLOW_HOME=$(pwd)/airflow
export PYTHONPATH=$AIRFLOW_HOME:$AIRFLOW_HOME/stacd:$PYTHONPATH

airflow webserver --port 8080
```

---

## 7. Initialize Your Workflow (via Plugin Dashboard)

This is where STACD shines — **no CLI commands needed** for workflow setup.

1. Open the Airflow UI at **[http://localhost:8080](http://localhost:8080)**
2. Log in with your admin credentials
3. In the top navigation bar, go to **STACD → Initialize Workflow**
4. Upload your 3 YAML configuration files:

   | Upload Field | What to Upload | Description |
   |---|---|---|
   | **DAG YAML** | e.g. `corestack_lite_dag.yaml` | Defines the workflow structure — algorithms, datasets, and their dependencies |
   | **Algorithm Repo YAML** | e.g. `corestack_lite_algorithm_repo.yaml` | Defines algorithm instances with API/Docker execution modes |
   | **Dataset Repo YAML** | e.g. `corestack_lite_dataset_repo.yaml` | Defines root datasets (pre-existing data inputs) |

5. Click **"Initialize Workflow"**
6. You should see a green success message:
   ```
   ✅ Database initialized | DAG script generated | DAG 'your_dag_id' deployed to Airflow
   ```

**What happens behind the scenes:**
- Uploaded YAMLs are saved to `$AIRFLOW_HOME/stacd/yaml_configs/`
- The STACD database is initialized with all algorithms, datasets, and the DAG structure
- A Python DAG script is auto-generated from your YAML specification
- The generated DAG is deployed to `$AIRFLOW_HOME/dags/` so Airflow picks it up automatically

> **Note:** Sample YAML files are included in `sample_yaml` for reference.

### Inspect STACD Database (Optional but Recommended)

STACD maintains its own metadata database (stacd_database.db) to track:

Algorithms
Datasets
DAG structure
Lineage relationships

You can explore this database using a web UI via sqlite-web.

- Install sqlite-web
```bash
pip install sqlite-web
```
- Launch Database Viewer
```bash
cd $AIRFLOW_HOME/stacd/database
sqlite_web stacd_database.db --host 0.0.0.0 --port 8085
```

## Airflow Variables

Before triggering any DAG, set the following Airflow Variables either via 
the Airflow UI (Admin → Variables) or via CLI:

### Auth token for CoreStack backend API
```airflow variables set CORESTACK_AUTH_TOKEN <bearer_token>```

Get a fresh token from the CoreStack backend:
```bash
curl -X POST http://<corestack_host>/api/v1/auth/login/ \
  -H "Content-Type: application/json" \
  -d '{"username": "<admin_user>", "password": "<password>"}'
```

#### Copy the `access` value from the response and set it above.
#### Tokens generally expires every 90 days — regenerate and reset the variable when it does.
---

## 8. Trigger Your Workflow

1. Go to the Airflow **DAGs** page (top menu → **DAGs**)
2. Your newly created DAG should appear in the list (e.g., `corestack_lite_dag`)
   - It may take up to 30 seconds for Airflow to detect the new DAG file
3. Toggle the DAG **ON** (the switch on the left)
4. Click the **▶ Trigger DAG** button (play icon on the right)
5. In the trigger dialog, set the required parameters:

   | Parameter | Example Value | Description |
   |---|---|---|
   | `execution_type` | `fullexec` | Execution primitive — see [Execution Primitives](#execution-primitives) below |
   | `state` | `jharkhand` | State name |
   | `district` | `dumka` | District name |
   | `block` | `masalia` | Block/tehsil name |
   | `start_year` | `2020` | Analysis start year |
   | `end_year` | `2021` | Analysis end year |
   | `gee_account_id` | `1` | GEE service account ID — supplied at trigger time via the Airflow UI, not configured during setup |

6. Click **Trigger** and monitor the run in the **Grid** or **Graph** view

### Execution Primitives

The `execution_type` parameter controls which subset of the DAG graph is executed. Five values are supported:

| Execution Type | What It Triggers | When to Use | Required Extra Params |
|---|---|---|---|
| `fullexec` | Runs all root datasets and root algorithms — the complete pipeline from entry points through all downstream tasks | Fresh run for a new region or full recomputation | None |
| `update_algo` | Runs a single specified algorithm and its downstream dependents only | An algorithm's code or version has changed and you want to recompute only its outputs | `updated_algo` (algorithm task ID) |
| `update_dataset` | Fetches the updated root dataset, then runs all algorithms that consume it as input | A root dataset (e.g. a boundary file) has been updated and downstream outputs need recomputation | `updated_dataset` (dataset type ID) |
| `update_dag` | Runs only algorithm nodes that have never had a successful execution in the database — i.e., newly added nodes after a DAG structure update | The DAG YAML was modified to add new algorithm/dataset nodes and you want to run only the new additions without re-running existing nodes | None (auto-detected from DB) |
| `resume_exec` | Queries the database for algorithm tasks that failed in a previous run (matching the same region parameters) and re-runs only those | A previous run partially failed and you want to retry the failed tasks without re-running successful ones | None (auto-detected from DB; uses `state`, `district`, `block`, `start_year`, `end_year` to match) |

> **Note:** If `update_dag` finds no new (unexecuted) algorithm nodes, or `resume_exec` finds no failed tasks for the given parameters, the DAG will raise a `ValueError` at the branching step and the run will fail immediately.

---

## 9. Triggering and Monitoring Airflow DAGs via the REST API

This describes the generic pattern used across our pipelines (drone, bioacoustic, and others)
to trigger an Airflow DAG run and poll it until completion, using Airflow's stable REST API
(`/api/v1/...`). This is standard Airflow behavior — not specific to our setup — so the same
pattern applies to any Airflow deployment with the REST API enabled.

---

### 1. Prerequisites — enabling the API

By default, Airflow's REST API requires authentication. In `airflow.cfg`, under the `[api]`
section:

```ini
[api]
auth_backends = airflow.api.auth.backend.basic_auth
```

This enables HTTP Basic Auth for API requests, using the same username/password as an Airflow
web user. After changing this, the webserver needs to be restarted for it to take effect.

---

### 2. Triggering a DAG run

**Endpoint:**
```
POST /api/v1/dags/{dag_id}/dagRuns
```

**Example:**
```bash
curl -X POST "http://<airflow-host>/api/v1/dags/<dag_id>/dagRuns" \
  -H "Content-Type: application/json" \
  -u "admin:admin" \
  -d '{
        "conf": {
          "param1": "value1",
          "param2": "value2"
        }
      }'
```

**Key points:**

- `<dag_id>` is the DAG's identifier as registered in Airflow (e.g. `drone_pipeline`,
  `cem_pipeline`).
- The request body is a JSON object. The `conf` key holds whatever parameters the DAG's tasks
  expect — this is entirely DAG-specific and is read inside the DAG via
  `dag_run.conf` / `context['dag_run'].conf`.
- The body cannot be empty — an empty `{}` is accepted by some Airflow versions but is best
  avoided; at minimum send `{"conf": {...}}`.
- Authentication is sent as HTTP Basic Auth (`-u user:pass` in curl, or an
  `Authorization: Basic <base64(user:pass)>` header if constructing the request manually).
- If a specific run identifier is not provided in the request, Airflow auto-generates one in
  the form `manual__<ISO-8601-timestamp>` (e.g. `manual__2026-06-19T11:30:24.805888+00:00`).
  This applies to runs triggered through the API in the same way it applies to clicking
  "Trigger DAG" in the UI.

**Response** (on success, HTTP 200):

```json
{
  "dag_run_id": "manual__2026-06-19T11:30:24.805888+00:00",
  "dag_id": "<dag_id>",
  "state": "queued",
  "conf": { "param1": "value1", "param2": "value2" },
  "execution_date": "2026-06-19T11:30:24.805888+00:00",
  ...
}
```

The caller should store `dag_run_id` from this response — it's required for all subsequent
polling/status calls.

**Common gotcha:** if the DAG is paused, the run will be created but will not actually
execute. DAGs can be unpaused via:
```
PATCH /api/v1/dags/{dag_id}
Body: {"is_paused": false}
```

---

### 3. Polling DAG run status

**Endpoint:**
```
GET /api/v1/dags/{dag_id}/dagRuns/{dag_run_id}
```

**Example:**
```bash
curl -X GET "http://<airflow-host>/api/v1/dags/<dag_id>/dagRuns/<dag_run_id>" \
  -u "admin:admin"
```

**Response:**

```json
{
  "dag_run_id": "manual__2026-06-19T11:30:24.805888+00:00",
  "dag_id": "<dag_id>",
  "state": "running",
  ...
}
```

The `state` field is what callers should check. Typical values:

| State | Meaning |
|---|---|
| `queued` | Run created, not yet started |
| `running` | DAG is actively executing tasks |
| `success` | All tasks completed successfully |
| `failed` | At least one task failed (and didn't retry into success) |

**Recommended polling pattern:**

```
POST /dagRuns                         → get dag_run_id
loop:
    GET /dagRuns/{dag_run_id}         → read "state"
    if state in ("success", "failed"): stop
    else: wait N seconds, repeat
```

A short, fixed interval (e.g. every 5 seconds) is a reasonable default for most pipelines.
There's no built-in push/webhook notification for completion via this API — polling is the
standard approach unless a separate mechanism (e.g. Airflow's own callbacks/SLA features, or
an external listener DAG) is set up.

---

### 4. Fetching task-level logs (optional, for debugging)

If a run fails, or for more granular progress info, individual task logs can be retrieved:

**Endpoint:**
```
GET /api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}
```

**Example:**
```bash
curl -X GET "http://<airflow-host>/api/v1/dags/<dag_id>/dagRuns/<dag_run_id>/taskInstances/<task_id>/logs/1" \
  -u "admin:admin"
```

- `{task_id}` is the specific task's identifier within the DAG (as defined in the DAG file).
- `{try_number}` starts at `1` for the first attempt and increments on each retry.
- This returns the raw log text for that task attempt — useful for surfacing error details to
  an end user or for debugging a failed pipeline run without needing direct access to the
  Airflow UI.

---

### 5. Notes on base URL / path prefixes

If Airflow is deployed behind a reverse proxy under a subpath (e.g. `/airflow/` instead of at
the domain root), two things need to stay consistent:

- Airflow's own `AIRFLOW__WEBSERVER__BASE_URL` config must match the externally-visible path,
  so that links and redirects generated by Airflow's webserver resolve correctly.
- All API calls (trigger, poll, logs) must be made against that same externally-visible base
  path — e.g. `http://<host>/airflow/api/v1/dags/...` rather than assuming the API is always at
  the domain root.

When calling the API from inside the same Docker network as the Airflow webserver, it's
usually simpler and more reliable to call the container directly by its Docker network name and
internal port (bypassing any external reverse-proxy path prefix entirely), and only use the
externally-visible path when the caller is outside that network.

---

### 6. Summary — minimal integration pattern

For any backend service that needs to trigger a pipeline and wait for the result:

1. `POST /api/v1/dags/{dag_id}/dagRuns` with a `conf` payload → get back `dag_run_id`.
2. Poll `GET /api/v1/dags/{dag_id}/dagRuns/{dag_run_id}` every few seconds until `state` is
   `success` or `failed`.
3. On `failed`, optionally fetch task logs via the `taskInstances/.../logs/{try_number}`
   endpoint to surface a useful error message.
4. On `success`, proceed with whatever post-processing the calling service needs (e.g. reading
   output files the DAG produced).

This pattern is generic to Airflow's stable REST API and works the same way regardless of what
the DAG itself does internally — it only depends on the DAG's `dag_id` and whatever `conf`
parameters that specific DAG expects.

---

## 10. Updating an Existing Workflow

After initialization, use the STACD plugin pages to update individual components without re-initializing:

| Action | Menu Path | What to Upload |
|---|---|---|
| **Update an Algorithm** | STACD → Register Algorithm | YAML with new `!Algorithm_Instance` definition |
| **Update a Dataset** | STACD → Register Dataset | YAML with new `!Dataset_Instance` definition |
| **Update DAG Structure** | STACD → Update DAG | Two YAMLs: new node definitions + updated DAG structure |
| **View Dataset Lineage** | STACD Lineage → STACD Dataset Lineage | (no upload — browse lineage of executed datasets) |

> **Dataset Lineage** walks backward from any registered dataset instance through the algorithm execution that produced it, then through that algorithm's input datasets, recursively — up to 20 levels deep. The result is rendered as an interactive left-to-right hierarchical graph (powered by vis-network) with a node inspector panel showing full metadata for any clicked node. This is implemented in `stacd_lineage_plugin.py` (Airflow plugin view) backed by the `get_dataset_lineage()` engine in `lineage_queries.py`.

---

## 11. Writing Your Own YAML Workflow

To define a custom workflow, you need 3 YAML files:

### DAG YAML
Defines the workflow graph — which algorithms run, which datasets they consume and produce:
```yaml
--- !DAG
id: my_workflow
name: My Custom Workflow
version: 1.0
group: corestack
description: "Description of the workflow"
params:
  - state
  - district
  - block
  - gee_account_id
alg_type_nodes:
  - My_Algorithm
dataset_type_nodes:
  - Input_Dataset
  - Output_Dataset

--- !Dataset_Type
id: Input_Dataset
name: Input Dataset
description: Root input dataset
format: GEEAsset

--- !Dataset_Type
id: Output_Dataset
name: Output Dataset
description: Produced by My_Algorithm
format: GEEAsset

--- !Algorithm_Type
id: My_Algorithm
name: My_Algorithm
description: Processes input and produces output
params:
  - name: state
    type: string
  - name: district
    type: string
  - name: block
    type: string
input_datasets:
  - Input_Dataset
outputs:
  - Output_Dataset
```

> **`group`** is optional. If omitted it defaults to `corestack`. It determines which RBAC role owns the generated DAG: `corestack` → `CoreStack_Op`, `drone` → `Drone_Op`, `bioacoustic` → `BioAcoustic_Op`.

### Algorithm Repo YAML
Defines how each algorithm is executed (API endpoint, Docker image, or both):

**API mode** — call a remote HTTP endpoint:
```yaml
--- !Algorithm_Instance
type: My_Algorithm
version: "1"
assets:
  code: "https://github.com/your-org/your-repo"
date: 2026-01-01 00:00:00
execution_modes:
  api:
    enabled: true
    priority: 1
    url: "http://localhost:8000/api/v1/my_algorithm/"
```

**Docker mode** — run a Python function inside a container:
```yaml
--- !Algorithm_Instance
type: My_Algorithm
version: "2"
assets:
  code: "https://github.com/your-org/your-repo"
date: 2026-01-01 00:00:00
execution_modes:
  docker:
    enabled: true
    priority: 1
    image: "your-org/your-algo-image:latest"
    module: "computing.lulc.lulc_v3_clip_river_basin"
    function: "lulc_river_basin"
```

**Mixed mode** — if both `api` and `docker` are enabled, the one with the lower `priority` number wins. For example, setting `api.priority: 2` and `docker.priority: 1` will prefer Docker; the API endpoint serves as a fallback if you later disable Docker.

#### Docker Stdout Convention

When running in Docker mode, the container function's return value is captured via a structured stdout protocol. The generated in-container script prints a JSON result between two marker lines:

```
===RESULT_JSON_START===
{"status": "success", "asset_ids": ["projects/..."], "hosting_platform": "GEE", "stac_spec": {...}}
===RESULT_JSON_END===
```

The host-side runner (`simple_docker_runner.py`) streams container logs line-by-line and captures the JSON payload between these markers. All other stdout lines are forwarded to the Airflow task log as-is.

### Dataset Repo YAML
Defines root datasets (pre-existing inputs that are not produced by any algorithm):
```yaml
--- !Dataset_Instance
type_id: Input_Dataset
version: "1"
region: pan_india
asset_id: "projects/your-project/assets/path/to/asset"
metadata:
  source: "Your data source"
  description: "Description of the dataset"
```
---

## 12. Algorithm Response Handling

STACD expects algorithms to respond via HTTP. The framework handles each response code differently — some are treated as graceful non-events, others as hard failures.

### Expected HTTP Response Codes

| HTTP Code | Meaning | Airflow Task State | Dataset Registered? |
|---|---|---|---|
| `200 OK` | Algorithm succeeded, asset produced | ✅ **success** | ✅ Yes |
| `400 Bad Request` | Invalid input parameters | ⬛ **skipped** | ❌ No |
| `404 Not Found` | No data available for this location/params | ⬛ **skipped** | ❌ No |
| `500 Internal Server Error` | Pipeline/computation failure | 🔴 **failed** | ❌ No |

### What Happens Downstream

- If a task is **skipped** (400/404) — its downstream dataset registration task is also skipped. No asset is written to the STAC-D catalog. The overall DAG run continues for other branches.
- If a task **fails** (500) — its downstream dataset task is marked `upstream_failed`. The DAG run is marked as failed overall.
- If a task **succeeds** (200) — the returned `asset_id` is registered as a new `DatasetInstance` in the STACD database and exported as a STAC-D catalog item.

### Expected Response Body

For a `200` response, the algorithm API must return a JSON body in this format:

```json
{
  "asset_id": "projects/your-gee-project/assets/path/to/output",
  "version": "1",
  "hosting_platform": "GEE"
}
```

Any `400`, `404`, or `500` response should include an `error` and `message` field for the logs:

```json
{
  "error": "NO_DATA",
  "message": "No data found for this district"
}
```

---

## Troubleshooting

| Issue | Solution |
|---|---|
| DAG not appearing after initialization | Wait 30 seconds for Airflow to scan the `dags/` folder, then refresh the page |
| "DAG already exists" error on Initialize | The DAG was already initialized. Use **STACD → Update DAG** to modify it, or delete the database file at `$AIRFLOW_HOME/stacd/database/stacd_database.db` to start fresh |
| Import errors in webserver logs | Make sure `PYTHONPATH` includes `$AIRFLOW_HOME/stacd` and restart Airflow |
| `AIRFLOW_HOME` not set correctly | Always run `export AIRFLOW_HOME=$(pwd)/airflow` from the `airflow_stacd` project root before starting Airflow |

---

## Project Structure Reference

```
airflow_stacd/
├── venv/                          # Python virtual environment
└── airflow/                       # $AIRFLOW_HOME
    ├── stacd/                     # STACD core module (clone from repo)
    │   ├── database/
    │   │   ├── models.py          # SQLAlchemy DB models
    │   │   ├── db_operations.py   # Database CRUD operations
    │   │   ├── init_db.py         # DB initialization from YAMLs
    │   │   └── lineage_queries.py # Dataset lineage query engine
    │   ├── dag_generator/
    │   │   ├── stacd_classes.py   # YAML tag classes (!DAG, !Algorithm_Type, etc.)
    │   │   └── stacd_recompute_generator.py  # YAML → Python DAG generator
    │   ├── stac_export/           # STAC-D catalog generation
    │   ├── yaml_configs/          # YAML workflow definitions
    │   └── generated_dags/        # Auto-generated Python DAG files
    ├── plugins/                   # Airflow UI plugins (clone from repo)
    │   ├── stacd_admin_plugin.py  # Initialize, Register, Update views
    │   ├── stacd_lineage_plugin.py# Dataset lineage visualization
    │   └── templates/             # HTML templates for plugin pages
    ├── dags/                      # Deployed DAGs (auto-populated)
    ├── logs/                      # Airflow execution logs
    └── airflow.cfg                # Airflow configuration
```
