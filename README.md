# **STACD Framework**

**Spatio-Temporal Asset Catalog with Dependencies** for reproducible geospatial workflows with Apache Airflow, supporting API and Docker execution modes with UUID-based lineage tracking.

---

## **Overview**

STACD Framework is a flexible workflow orchestration system designed for geospatial data processing tasks. It enables teams to define, execute, and track complex geospatial analysis pipelines using Apache Airflow while maintaining full provenance and lineage tracking.

The framework supports **dual execution modes**:

* **API Mode**: Direct REST API calls to a FastAPI backend for rapid execution
* **Docker Mode**: Containerized execution for reproducibility and isolation
* **Mixed Mode**: Combine both modes within a single workflow

---

## **Key Features**

*  **Dual Execution Support**: Choose between API calls or Docker containers per algorithm
*  **UUID-based Lineage**: Track every computation with unique identifiers
*  **Database Integration**: SQLite-based metadata storage for algorithms and datasets
*  **Google Earth Engine**: Built-in support for GEE-based geospatial analysis
*  **YAML Configuration**: Define workflows declaratively with simple YAML files
*  **Dependency Management**: Automatic resolution of algorithm dependencies
*  **Provenance Tracking**: Full computational lineage for reproducibility

---

## **Architecture**

```
STACD Framework
├── Backend (FastAPI)
│   ├── Computing Modules
│   │   ├── LULC Analysis
│   │   ├── Terrain Analysis
│   │   └── LULC×Terrain Cross-Analysis
│   └── REST API Endpoints
├── Database Layer (SQLite)
│   ├── Algorithm Metadata
│   ├── Dataset Records
│   └── Execution Logs
└── Airflow DAGs
    ├── YAML Configs
    ├── DAG Generators
    └── Generated DAGs
```

---

## **Built With**

* **Backend**: FastAPI
* **Workflow Engine**: Apache Airflow
* **Geospatial**: Google Earth Engine
* **Database**: SQLAlchemy + SQLite
* **Containerization**: Docker

---

## **Repository Structure**

```
STACD_framework/
├── airflow_dags/
│   ├── dag_generator/
│   │   ├── stacd_classes.py
│   │   └── stacd_mixed_dag_generator.py
│   ├── generated_dags/
│   └── yaml_configs/
│       ├── stacd_algorithm_repo.yaml
│       ├── stacd_dag.yaml
│       └── stacd_dataset_repo.yaml
├── backend/
│   ├── computing/
│   │   ├── lulc/
│   │   ├── lulcxterrain/
│   │   ├── terrain/
│   │   └── utils/
│   ├── Dockerfile
│   ├── cli.py
│   ├── main.py
│   └── requirements.txt
├── database/
│   ├── db_operations.py
│   ├── init_db.py
│   └── models.py
├── .gitignore
├── LICENSE
└── README.md
```

---

## **Getting Started**

### **Prerequisites**

* Python 3.8+
* Docker
* Apache Airflow 2.0+
* Google Earth Engine account

---

### **Installation**

1. **Clone the repository**

   ```
   git clone https://github.com/SaharshLaud/STACD_framework.git
   cd STACD_framework
   ```

2. **Set up the backend**

   ```
   cd backend
   pip install -r requirements.txt
   ```

3. **Initialize the database**

   ```
   cd ../database
   python init_db.py
   ```

4. **Authenticate with Google Earth Engine**

   ```
   earthengine authenticate
   ```

5. **Configure Airflow**

   ```
   export AIRFLOW_HOME=~/airflow
   cp -r airflow_dags/generated_dags/* $AIRFLOW_HOME/dags/
   ```

---

## **Running the Backend**

### **Option 1: Local Development**

```
cd backend
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### **Option 2: Docker Container**

```
cd backend
docker build -t stacd-backend .
docker run -p 8000:8000 stacd-backend
```

API Docs at: **[http://localhost:8000/docs](http://localhost:8000/docs)**

---

## **Usage**

### **Defining a Workflow**

Edit `airflow_dags/yaml_configs/stacd_dag.yaml`:

```
name: terrain_lulc_analysis
description: Terrain and LULC cross-analysis workflow
tasks:
  - name: generate_terrain_clusters
    algorithm: terrain_clusters
    execution_mode: api
    inputs:
      region: "users/saharshlaud/bihar_boundary"
    outputs:
      - terrain_cluster_map
      
  - name: generate_lulc_vector
    algorithm: lulc_vector
    execution_mode: docker
    inputs:
      region: "users/saharshlaud/bihar_boundary"
    outputs:
      - lulc_vector_map
      
  - name: lulc_on_terrain
    algorithm: lulc_on_slope_cluster
    execution_mode: api
    depends_on:
      - generate_terrain_clusters
      - generate_lulc_vector
    inputs:
      terrain_map: "{{ task_instance.xcom_pull('generate_terrain_clusters')['terrain_cluster_map'] }}"
      lulc_map: "{{ task_instance.xcom_pull('generate_lulc_vector')['lulc_vector_map'] }}"
    outputs:
      - lulc_terrain_analysis
```

---

### **Generating Airflow DAGs**

```
cd airflow_dags/dag_generator
python stacd_mixed_dag_generator.py \
  --dag-config ../yaml_configs/stacd_dag.yaml \
  --algo-config ../yaml_configs/stacd_algorithm_repo.yaml \
  --output ../generated_dags/
```

---

### **Registering Algorithms**

Add entries in `stacd_algorithm_repo.yaml`:

```
algorithms:
  - name: terrain_clusters
    description: Generate terrain cluster map
    module: backend.computing.terrain.terrain_clusters
    function: generate_terrain_clusters
    docker_image: stacd/terrain-clusters:latest
    inputs:
      - name: region
        type: string
        required: true
    outputs:
      - terrain_cluster_map
```

---

### **CLI Usage**

```
cd backend

python cli.py run-algorithm terrain_clusters --region "users/saharshlaud/bihar"

python cli.py list-algorithms

python cli.py describe-algorithm terrain_clusters
```

---

## **Execution Modes**

### **API Mode**

* Fast, easy debugging
* Requires backend running

### **Docker Mode**

* Reproducible
* Isolated

### **Mixed Mode**

* Per-task flexibility
* Most versatile

---

## **Database Schema**

Tables include:

* **algorithms** — Registered algorithms
* **datasets** — Input/output data
* **executions** — Algorithm runs with UUIDs

---

## **API Endpoints**

### Algorithm Execution

* `POST /algorithms/{algorithm_name}/execute`
* `GET /algorithms/{algorithm_name}`
* `GET /algorithms/`


---

## **CoreStack Migration Guide**

Steps:

1. Register algorithms
2. Convert workflow dependencies to YAML
3. Select execution modes
4. Use STACD database for metadata
5. Replace cron jobs with Airflow DAGs

See **docs/corestack_migration_guide.pdf** for full details.

---
### Development Setup

```
pip install -r backend/requirements.txt
pip install pytest black flake8

pytest tests/

black backend/ database/ airflow_dags/

flake8 backend/ database/ airflow_dags/
```

---

## **License**

MIT License — see the LICENSE file.

---

## **Acknowledgments**

* Google Earth Engine
* Apache Airflow community
* CoreStack team

---

Saharsh Laud

