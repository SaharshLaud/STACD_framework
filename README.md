# AIRFLOW INSTALLATION

## 1. Install Airflow (Exact Version)


```bash
# create project folder
mkdir airflow_stacd && cd airflow_stacd

# create venv
python3 -m venv venv
source venv/bin/activate

# upgrade pip
pip install --upgrade pip

# set airflow home (IMPORTANT)
export AIRFLOW_HOME=$(pwd)/airflow
mkdir -p $AIRFLOW_HOME

# install airflow 2.10.4 with constraints
AIRFLOW_VERSION=2.10.4
PYTHON_VERSION=$(python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")

pip install "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
```

---

## 2. Clone Your Repos
Make another folder or if you prefer inside the same airflow folder itself and clone the dev branch.
```bash
git clone -b dev https://github.com/SaharshLaud/STACD_framework
```

---

# 3. Setup Folder Structure

We want Airflow to see your `enhancement` folder exactly like the current system.

```bash

# copy enhancement folder
cp -r STACD-Airflow/enhancement $AIRFLOW_HOME/

# copy plugins folder
cp -r STACD-Airflow/plugins $AIRFLOW_HOME/

```


---

## 4. Set PYTHONPATH

```bash
export AIRFLOW_HOME=$(pwd)/airflow
export PYTHONPATH=$AIRFLOW_HOME:$AIRFLOW_HOME/enhancement:$PYTHONPATH
```

(Optional permanent)

```bash
echo "export AIRFLOW_HOME=$(pwd)/airflow" >> ~/.bashrc
echo "export PYTHONPATH=\$AIRFLOW_HOME:\$AIRFLOW_HOME/enhancement:\$PYTHONPATH" >> ~/.bashrc
```

---

## 5. Initialize Airflow DB

```bash
airflow db init
```

Create user:

```bash
airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com
```

---

## 6. Initialize STACD DB (YAML → DB)

Go to enhancement/database:

```bash
cd $AIRFLOW_HOME/enhancement/database
```

Run:

```bash
python init_db.py \
  --dag corestack_lite_dag.yaml \
  --algo-repo corestack_lite_algorithm_repo.yaml \
  --dataset-repo corestack_lite_dataset_repo.yaml
```

What this does:

* Registers Algorithm Instances
* Registers Algorithm Types
* Registers Dataset Types
* Registers Root datasets
* Registers DAG
* Generates STAC-D items

---

## 7. Generate DAG from YAML

Go:

```bash
cd $AIRFLOW_HOME/enhancement/dag_generator
```

Typical command (you may already have a wrapper, but generic way):

```bash
python stacd_recompute_generator.py \
  --dag-yaml ../yaml_configs/corestack_lite_dag.yaml \
  --output ../generated_dags/corestack_lite_dag_generated.py
```

Then copy DAG to airflow:

```bash
cp ../generated_dags/corestack_lite_dag_generated.py $AIRFLOW_HOME/dags/
```

---

## 8. Run Airflow

### **Standalone mode:**

```bash
airflow standalone
```
OR
### **Production mode:**
Start scheduler:

```bash
airflow scheduler
```

In another terminal:

```bash
source venv/bin/activate
export AIRFLOW_HOME=$(pwd)/airflow
export PYTHONPATH=$AIRFLOW_HOME:$AIRFLOW_HOME/enhancement:$PYTHONPATH

airflow webserver --port 8080
```

---

## 9. Trigger DAG

UI → [http://localhost:8080](http://localhost:8080)

---