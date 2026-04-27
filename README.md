# STACD Framework — Setup & Usage Guide

> **STACD** (SpatioTemporal Asset Catalog for Dataflows) is a YAML-driven geospatial workflow management system built on top of Apache Airflow. Users define their workflows as YAML files, and the system handles database initialization, DAG generation, and deployment — all from the Airflow dashboard.

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

STACD maintains its own metadata database (stacd_recompute.db) to track:

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
sqlite_web stacd_recompute.db --host 0.0.0.0 --port 8085
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
   | `execution_type` | `fullexec` | Run type: `fullexec`, `update_algo`, `update_dataset` |
   | `state` | `jharkhand` | State name |
   | `district` | `dumka` | District name |
   | `block` | `masalia` | Block/tehsil name |
   | `start_year` | `2020` | Analysis start year |
   | `end_year` | `2021` | Analysis end year |

6. Click **Trigger** and monitor the run in the **Grid** or **Graph** view

---

## 9. Updating an Existing Workflow

After initialization, use the STACD plugin pages to update individual components without re-initializing:

| Action | Menu Path | What to Upload |
|---|---|---|
| **Update an Algorithm** | STACD → Register Algorithm | YAML with new `!Algorithm_Instance` definition |
| **Update a Dataset** | STACD → Register Dataset | YAML with new `!Dataset_Instance` definition |
| **Update DAG Structure** | STACD → Update DAG | Two YAMLs: new node definitions + updated DAG structure |
| **View Dataset Lineage** | STACD Lineage → STACD Dataset Lineage | (no upload — browse lineage of executed datasets) |

---

## 10. Writing Your Own YAML Workflow

To define a custom workflow, you need 3 YAML files:

### DAG YAML
Defines the workflow graph — which algorithms run, which datasets they consume and produce:
```yaml
--- !DAG
id: my_workflow
name: My Custom Workflow
version: 1.0
description: "Description of the workflow"
params:
  - state
  - district
  - block
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

### Algorithm Repo YAML
Defines how each algorithm is executed (API endpoint, Docker image, or both):
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

## Troubleshooting

| Issue | Solution |
|---|---|
| DAG not appearing after initialization | Wait 30 seconds for Airflow to scan the `dags/` folder, then refresh the page |
| "DAG already exists" error on Initialize | The DAG was already initialized. Use **STACD → Update DAG** to modify it, or delete the database file at `$AIRFLOW_HOME/stacd/database/stacd_recompute.db` to start fresh |
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
