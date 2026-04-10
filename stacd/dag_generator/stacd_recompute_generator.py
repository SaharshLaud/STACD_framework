#!/usr/bin/env python3
"""
STACD Selective Recomputation DAG Generator - FULLY GENERIC
- Reads ALL structure from YAML (no hardcoding)
- Generates tasks dynamically based on YAML definitions
- Queries database for active versions at RUNTIME
- Supports fullexec, update_algo, update_dataset
"""

import yaml
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path


# Add paths relative to AIRFLOW_HOME
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", os.path.expanduser("~/airflow"))
sys.path.insert(0, os.path.join(AIRFLOW_HOME, 'stacd/dag_generator'))
sys.path.insert(0, os.path.join(AIRFLOW_HOME, 'stacd/database'))

from stacd_classes import DAG, Algorithm_Type, Algorithm_Instance, Dataset_Type

def load_yaml_file(filepath):
    """Load YAML with custom tags"""
    with open(filepath, 'r') as f:
        content = f.read()
    return list(yaml.load_all(content, Loader=yaml.FullLoader))

def parse_dag_specification(dag_yaml_path):
    """Parse YAML file to get complete DAG structure"""
    dag_docs = load_yaml_file(dag_yaml_path)
    
    dag_obj = None
    algorithm_types = {}
    dataset_types = {}
    
    for doc in dag_docs:
        if isinstance(doc, DAG):
            dag_obj = doc
        elif isinstance(doc, Algorithm_Type):
            algorithm_types[doc.id] = doc
        elif isinstance(doc, Dataset_Type):
            dataset_types[doc.id] = doc
    
    return dag_obj, algorithm_types, dataset_types

def build_dependency_graph(dag_obj, algorithm_types):
    """
    Build a dependency graph from Algorithm_Type definitions.
    Returns: dict mapping algo_id -> {'inputs': [], 'outputs': []}
    """
    dependencies = {}
    
    for algo_id in dag_obj.alg_type_nodes:
        if algo_id in algorithm_types:
            algo_type = algorithm_types[algo_id]
            dependencies[algo_id] = {
                'inputs': algo_type.input_datasets or [],
                'outputs': algo_type.outputs or []
            }
    
    return dependencies

def find_root_algorithms(dependencies, root_datasets):
    """
    Find algorithms that only consume root datasets (not produced datasets).
    
    Root algorithms:
    - Have NO inputs, OR
    - Only consume root datasets (not algorithm outputs)
    """
    roots = []
    for algo_id, deps in dependencies.items():
        inputs = deps['inputs']
        
        # Case 1: No inputs at all
        if not inputs:
            roots.append(algo_id)
            continue
        
        # Case 2: All inputs are root datasets (not produced by algorithms)
        all_inputs_are_root = all(inp in root_datasets for inp in inputs)
        if all_inputs_are_root:
            roots.append(algo_id)
    
    return roots


def find_root_datasets(dag_obj, dependencies):
    """
    Find datasets that are NOT produced by any algorithm (root datasets).
    These are datasets in dataset_type_nodes but not in any algorithm's outputs.
    """
    produced_datasets = set()
    
    # Collect all datasets produced by algorithms
    for algo_id, deps in dependencies.items():
        for output_ds in deps['outputs']:
            produced_datasets.add(output_ds)
    
    # Root datasets are those in dataset_type_nodes but not produced
    root_datasets = []
    for dataset_id in dag_obj.dataset_type_nodes:
        if dataset_id not in produced_datasets:
            root_datasets.append(dataset_id)
    
    return root_datasets


def generate_task_code_for_algorithm(algo_id, algo_type, dependencies):
    """Generate Python code for an algorithm task - Supports API + Docker"""
    
    inputs = dependencies[algo_id]['inputs']
    outputs = dependencies[algo_id]['outputs']
    
    # Build XCom pull logic for inputs
    xcom_pull_code = ""
    if inputs:
        xcom_pull_code = "\n    # Get inputs from upstream dataset tasks\n"
        for input_ds in inputs:
            xcom_pull_code += f"    {input_ds}_asset_id = ti.xcom_pull(task_ids='{input_ds}', key='asset_id')\n"
    
    # Build parameter extraction
    param_extraction = ""
    if algo_type.params:
        param_extraction = "\n    # Extract parameters from context\n"
        for param in algo_type.params:
            if isinstance(param, dict):
                param_name = param.get('name', param.get('id', 'unknown'))
                param_type = param.get('type', 'string')
            else:
                param_name = str(param)
                param_type = 'string'
            
            # Type conversion based on param type
            if param_type == 'integer':
                param_extraction += f"    {param_name} = int(params.get('{param_name}'))\n"
            else:
                param_extraction += f"    {param_name} = params.get('{param_name}')\n"

    
    # Build payload items
    payload_items = ["'execution_id': run_id"]
    
    if algo_type.params:
        for param in algo_type.params:
            if isinstance(param, dict):
                param_name = param.get('name', param.get('id', 'unknown'))
            else:
                param_name = str(param)
            payload_items.append(f"'{param_name}': {param_name}")
    
    if inputs:
        for input_ds in inputs:
            payload_items.append(f"'{input_ds}': {input_ds}_asset_id")
    
    payload_code = "{\n        " + ",\n        ".join(payload_items) + "\n    }" if payload_items else "{'execution_id': run_id}"
    
    # Generate function with Docker support
    func_code = f'''
def execute_{algo_id}(**context):
    """
    Execute {algo_id} - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: {algo_id}")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    run_id = context['run_id']
{xcom_pull_code}{param_extraction}
    
    # Query database for active version
    algo_config = get_active_algorithm_config('{algo_id}')
    version = algo_config['version']
    execution_modes = algo_config['execution_modes']
    
    print(f"📌 Active Version: {{version}}")
    print(f"📋 Execution Modes: {{execution_modes}}")
    
    # Get API and Docker configs
    api_config = execution_modes.get('api', {{}})
    docker_config = execution_modes.get('docker', {{}})
    
    # Determine execution mode based on priority
    api_enabled = api_config.get('enabled', False)
    docker_enabled = docker_config.get('enabled', False)
    
    api_priority = api_config.get('priority', 99) if api_enabled else 99
    docker_priority = docker_config.get('priority', 99) if docker_enabled else 99
    
    use_docker = docker_enabled and (docker_priority < api_priority)
    use_api = api_enabled and not use_docker
    
    # Build algorithm parameters (for both API and Docker)
    algo_params = {payload_code}
    
    print(f"🔍 Parameters: {{algo_params}}")
    
    if use_docker:
        # ===== DOCKER EXECUTION =====
        print(f"🐳 Docker Mode Selected (priority: {{docker_priority}})")
        print(f"   Image: {{docker_config.get('image')}}")
        print(f"   Module: {{docker_config.get('module')}}")
        print(f"   Function: {{docker_config.get('function')}}")
        
        # Import Docker runner
        AIRFLOW_HOME_RT = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
        sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd'))
        from simple_docker_runner import run_in_docker
        
        # Extract function params (remove execution_id for function call)
        function_params = {{k: v for k, v in algo_params.items() if k != 'execution_id'}}



        # Execute in Docker
        docker_result = run_in_docker(
            image=docker_config['image'],
            module_path=docker_config['module'],
            function_name=docker_config['function'],
            function_params=function_params
        )

        # Parse structured result
        asset_ids = docker_result.get('asset_ids', [])
        hosting_platform = docker_result.get('hosting_platform', 'GEE')
        stac_spec = docker_result.get('stac_spec', None)
        asset_id = asset_ids[0] if asset_ids else 'unknown'

        
    elif use_api:
        # ===== API EXECUTION =====
        print(f" API Mode Selected (priority: {{api_priority}})")
        print(f"   URL: {{api_config['url']}}")

        try:
            from airflow.models import Variable
            token = Variable.get("CORESTACK_AUTH_TOKEN", default_var=None)
        except Exception as token_err:
            print(f"WARNING: Could not fetch CORESTACK_AUTH_TOKEN: {{token_err}}")
            token = None

        headers = {{}}
        if token:
            headers["Authorization"] = f"Bearer {{token}}"

        response = requests.post(
            api_config['url'],
            json=algo_params,
            headers=headers,
            timeout=7200  # 2 hours — GEE tasks take long
        )

        response.raise_for_status()
        result = response.json()

        # Parse structured result
        asset_ids = result.get('asset_ids', [])
        hosting_platform = result.get('hosting_platform', 'GEE')
        stac_spec = result.get('stac_spec', None)
        asset_id = asset_ids[0] if asset_ids else result.get('asset_id', 'unknown')

    
    else:
        raise ValueError(f"No execution mode enabled for {algo_id}")
    
    print(f"✓ {algo_id} completed successfully")
    print(f"   Output asset: {{asset_id}}")
    print(f"   Version: {{version}}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='{algo_id}',
            version=version,
            run_id=run_id,
            execution_params=algo_params,
            output_asset_id=asset_id,
            status='success'
        )
    finally:
        db.close()
    
    # Push to XCom for downstream
    ti.xcom_push(key='asset_id', value=asset_id)
    ti.xcom_push(key='version', value=version)
    ti.xcom_push(key='hosting_platform', value=hosting_platform)
    ti.xcom_push(key='stac_spec', value=stac_spec)
    return {{'status': 'success', 'asset_id': asset_id, 'version': version, 'hosting_platform': hosting_platform}}

'''
    return func_code

def generate_task_code_for_dataset(dataset_id, is_root_dataset=False):
    """
    Generate Python code for dataset task
    - Root datasets: Fetch from database
    - Produced datasets: Register after algorithm execution
    """
    
    if is_root_dataset:
        # ===== ROOT DATASET TASK =====
        func_code = f'''
def fetch_{dataset_id}(**context):
    """
    Fetch {dataset_id} root dataset from database
    Root datasets are pre-existing and not produced by algorithms.
    """
    print("="*60)
    print("Fetching Root Dataset: {dataset_id}")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    
    # Extract region parameters
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')

    execution_type = params.get('execution_type', 'fullexec')
    updated_dataset = params.get('updated_dataset', '')
    is_being_updated = (execution_type == 'update_dataset' and updated_dataset == '{dataset_id}')

    print(f"🔍 Looking for {dataset_id} in region: {{state}}/{{district}}/{{block}}")
    if is_being_updated:
        print(f"⚡ update_dataset mode — will use latest registered version")

    # Query database for root dataset (always gets latest version)
    db = STACDDatabase(DB_PATH)
    try:
        root_dataset = db.get_root_dataset(
            dataset_type_id='{dataset_id}',
            state=state,
            district=district,
            block=block
        )
        
        if not root_dataset:
            raise ValueError(f"Root dataset {dataset_id} not found for {{state}}/{{district}}/{{block}}")
        
        asset_id = root_dataset.asset_id
        version = root_dataset.version
        print(f"✓ Found {dataset_id} version {{version}}")
        print(f"   Asset ID: {{asset_id}}")
        if is_being_updated:
            print(f"⚡ Propagating updated dataset v{{version}} to downstream algorithms")
        
    finally:
        db.close()
    
    # Push asset_id for downstream consumption
    ti.xcom_push(key='asset_id', value=asset_id)
    ti.xcom_push(key='version', value=version)
    ti.xcom_push(key='is_updated', value=is_being_updated)
    return {{'status': 'success', 'asset_id': asset_id, 'version': version, 'is_root': True, 'is_updated': is_being_updated}}

'''

    else:  # PRODUCED DATASET TASK
        func_code = f'''
def register_{dataset_id}(**context):
    """Register {dataset_id} dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: {dataset_id}")
    print("=" * 60)
    
    ti = context["ti"]
    run_id = context["run_id"]
    params = context["params"]
    
    # Find upstream algorithm
    upstream_tasks = context["task"].upstream_task_ids
    asset_id = None
    producing_algo = None
    algo_version = None


    for upstream_task_id in upstream_tasks:
        xcom_asset = ti.xcom_pull(task_ids=upstream_task_id, key="asset_id")
        if xcom_asset:
            asset_id = xcom_asset
            producing_algo = upstream_task_id
            algo_version = ti.xcom_pull(task_ids=upstream_task_id, key="version")
            hosting_platform = ti.xcom_pull(task_ids=upstream_task_id, key="hosting_platform") or "GEE"
            stac_spec = ti.xcom_pull(task_ids=upstream_task_id, key="stac_spec")
            break

    if not asset_id:
        raise ValueError(f"No asset_id found from upstream for {dataset_id}")

    print(f"Registering {dataset_id}")
    print(f"Asset ID: {{asset_id}}")
    print(f"Produced by: {{producing_algo}} v{{algo_version}}")
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')
    start_year = params.get('start_year')
    end_year = params.get('end_year')
        
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
    
        instance = db.log_dataset_instance(
            dataset_type_id="{dataset_id}",
            asset_id=asset_id,
            produced_by_algo=producing_algo,
            algo_version=algo_version,
            run_id=run_id,
            meta_info={{"registered_at": str(datetime.now()),
                'state': state,         
                'district': district,   
                'block': block,         
                'start_year': str(start_year), 
                'end_year': str(end_year)   }}
        )



        version = instance.version
        print(f"{dataset_id} registered in database")
        print(f"Version: {{version}}")

        # Augment stac_spec with STAC-D provenance fields
        if stac_spec:
            stac_spec["properties"]["stacd:dag_id"] = DAG_ID
            stac_spec["properties"]["stacd:run_id"] = run_id
            stac_spec["properties"]["stacd:producing_algo"] = producing_algo
            stac_spec["properties"]["stacd:algo_version"] = algo_version
            stac_spec["properties"]["stacd:dataset_type"] = "{dataset_id}"
            stac_spec["properties"]["stacd:instance_id"] = instance.instance_id
            stac_spec["properties"]["stacd:dataset_version"] = version
            stac_spec["id"] = f"{{state}}_{{district}}_{{block}}_{dataset_id}_v{{version}}"

            # Write augmented STAC item to catalog
            print("Writing augmented STAC item...")
            augment_and_write_stac(stac_spec, "{dataset_id}", instance.instance_id, params)
            print(f"STAC item written for {dataset_id}")
        else:
            # Fallback: generate from DB record (original path)
            print("No stac_spec from algo, falling back to DB-based generation...")
            generate_stac_for_dataset(instance, params)
            print(f"STAC item generated for {dataset_id}")
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {{"status": "success", "asset_id": asset_id, "version": version}}
'''
    return func_code






def generate_dag_code(dag_obj, algorithm_types, dataset_types, dependencies):
    """Generate complete DAG Python file"""
    
    # Find root datasets first
    root_datasets = find_root_datasets(dag_obj, dependencies)

    # Then find root algorithms (those consuming only root datasets)
    root_algos = find_root_algorithms(dependencies, root_datasets)

    
    # Generate header
    header = f'''"""
Auto-generated STACD DAG: {dag_obj.name}
Generated at: {datetime.now().isoformat()}
Description: {dag_obj.description}

Supports:
- fullexec: Run all root algorithms (and their downstream automatically)
- update_algo: Run from updated algorithm downstream
- update_dataset: Run from dataset consumers downstream

This DAG is FULLY GENERIC - all logic derived from YAML configuration.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import requests
import sys
import os

# Add database path
AIRFLOW_HOME_RT = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd/database'))
from db_operations import STACDDatabase

# ========== Configuration ==========
DAG_ID = "{dag_obj.id}"
DB_PATH = os.path.join(AIRFLOW_HOME_RT, 'stacd/database/stacd_recompute.db')

# ========== Dependency Graph (for reference) ==========
dependencies = {json.dumps(dependencies, indent=4)}

# ========== Helper Functions ==========

def generate_stac_for_dataset(dataset_instance, params):
    """Generate STAC item for a dataset instance"""
    import sys
    import os
    sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd/stac_export'))
    
    from stac_builder import STACBuilder
    from config import CATALOG_OUTPUT_DIR
    
    builder = STACBuilder(CATALOG_OUTPUT_DIR / "datasets")
    
    # Parse location from asset_id
    location = builder.parse_asset_location(dataset_instance.asset_id)
    if not location:
        print(f"Warning: Could not parse location from {{dataset_instance.asset_id}}")
        return
    
    state, district, block = location["state"], location["district"], location["block"]
    
    # Build dataset dict
    dataset_dict = {{
        "instance_id": dataset_instance.instance_id,
        "dataset_type_id": dataset_instance.dataset_type_id,
        "asset_id": dataset_instance.asset_id,
        "produced_by_algo": dataset_instance.produced_by_algo,
        "algo_version": dataset_instance.algo_version,
        "run_id": dataset_instance.run_id,
        "meta_info": dataset_instance.meta_info,
        "created_at": dataset_instance.created_at.isoformat() + "Z" if dataset_instance.created_at else None
    }}
    
    # Build STAC item
    item = builder.build_item(dataset_dict, state, district, block)
    
    # Write to file
    item_filename = f"{{dataset_instance.dataset_type_id}}_{{dataset_instance.instance_id}}.json"
    item_path = CATALOG_OUTPUT_DIR / "datasets" / state / district / block / item_filename
    builder.write_json(item, item_path)
    
    print(f"STAC item written: {{item_path}}")




    
def augment_and_write_stac(stac_spec, dataset_type_id, instance_id, params):
    """Write the algo-returned stac_spec (already augmented) to the catalog"""
    import sys
    import os
    import json
    sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd/stac_export'))
    from config import CATALOG_OUTPUT_DIR

    state = params.get('state', 'unknown')
    district = params.get('district', 'unknown')
    block = params.get('block', 'unknown')

    output_dir = CATALOG_OUTPUT_DIR / "datasets" / state / district / block
    output_dir.mkdir(parents=True, exist_ok=True)

    item_filename = f"{{dataset_type_id}}_{{instance_id}}.json"
    item_path = output_dir / item_filename

    with open(item_path, 'w') as f:
        json.dump(stac_spec, f, indent=2, ensure_ascii=False)
    print(f"STAC item written: {{item_path}}")





def get_active_algorithm_config(algo_type_id):
    """Query database for active algorithm version config"""
    db = STACDDatabase(DB_PATH)
    try:
        algo_instance = db.get_active_algorithm_version(algo_type_id)
        if not algo_instance:
            raise ValueError(f"No active version found for {{algo_type_id}}")
        
        print(f"Using {{algo_type_id}} version {{algo_instance.version}}")
        print(f"   Execution modes: {{algo_instance.execution_modes}}")
        
        return {{
            'version': algo_instance.version,
            'execution_modes': algo_instance.execution_modes
        }}
    finally:
        db.close()

def determine_execution_path(**context):
    """
    Determine which tasks to run based on execution_type
    - fullexec: Return list of all root algorithms
    - update_algo: Return updated algorithm only
    - update_dataset: Return algorithms consuming updated dataset
    """
    params = context['params']
    execution_type = params.get('execution_type', 'fullexec')
    
    print(f"🔀 Execution type: {{execution_type}}")

    if execution_type == 'fullexec':
        # Run all root datasets AND root algorithms
        root_datasets = {root_datasets}
        root_algos = {root_algos}
        entry_points = root_datasets + root_algos
        print(f"📌 Running root datasets: {{root_datasets}}")
        print(f"📌 Running root algorithms: {{root_algos}}")
        return entry_points
    
    elif execution_type == 'update_algo':
        updated_algo = params.get('updated_algo')
        if not updated_algo:
            raise ValueError("updated_algo parameter required for update_algo execution")
        
        print(f"📌 Running from updated algorithm: {{updated_algo}}")
        return [updated_algo]
    
    elif execution_type == 'update_dataset':
        updated_dataset = params.get('updated_dataset')
        if not updated_dataset:
            raise ValueError("updated_dataset parameter required for update_dataset execution")
        
        # Find algorithms that consume this dataset
        consumers = []
        for algo_id, deps in dependencies.items():
            if updated_dataset in deps['inputs']:
                consumers.append(algo_id)
        
        print(f"📌 Dataset {{updated_dataset}} consumed by: {{consumers}}")
        print(f"📌 Running: [{{updated_dataset}}] → {{consumers}}")

        return [updated_dataset] + consumers if consumers else [updated_dataset]



    elif execution_type == 'update_dag':
        # Run ONLY the newly added algo nodes
        # These are nodes present in current DAG structure but
        # not previously run (i.e. no successful execution in DB)
        db = STACDDatabase(DB_PATH)
        try:
            dag_record = db.get_dag_by_id(DAG_ID)
            if not dag_record:
                raise ValueError(f"DAG {{DAG_ID}} not found in DB")

            # Get all algos that have NEVER had a successful execution for this region
            all_algo_ids = list(dependencies.keys())
            new_algos = []
            for algo_id in all_algo_ids:
                execution = db.get_algorithm_execution(
                    algo_type_id=algo_id
                )
                if not execution or execution.status != 'success':
                    new_algos.append(algo_id)

            if not new_algos:
                raise ValueError("No new algo nodes found. All algos have prior successful executions.")

        finally:
            db.close()

        print(f" New algo nodes to run: {{new_algos}}")
        return new_algos



        

    elif execution_type == 'resume_exec':
        db = STACDDatabase(DB_PATH)
        try:
            failed_algos = db.get_failed_executions(
                dag_id=DAG_ID,
                state=params.get('state'),
                district=params.get('district'),
                block=params.get('block'),
                start_year=params.get('start_year'),
                end_year=params.get('end_year')
            )
        finally:
            db.close()

        if not failed_algos:
            raise ValueError("No failed executions found matching these parameters. Use fullexec to run from scratch.")

        print(f" Resuming from {{len(failed_algos)}} failed task(s): {{failed_algos}}")
        return failed_algos

    
    else:
        raise ValueError(f"Unknown execution_type: {{execution_type}}")

def log_algo_failure(context):
    """Called automatically by Airflow when an algo task fails"""
    ti = context['ti']
    run_id = context['run_id']
    params = context.get('params', {{}})
    algo_type_id = ti.task_id

    execution_params = {{
        'state': params.get('state'),
        'district': params.get('district'),
        'block': params.get('block'),
        'start_year': params.get('start_year'),
        'end_year': params.get('end_year'),
        'execution_id': run_id
    }}

    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        if dag_record:
            db.log_algorithm_execution(
                dag_uuid=dag_record.dag_uuid,
                algo_type_id=algo_type_id,
                version='unknown',
                run_id=run_id,
                execution_params=execution_params,
                output_asset_id=None,
                status='failed'
            )
            print(f"Logged failure for {{algo_type_id}} run_id={{run_id}}")
    except Exception as e:
        print(f"Could not log failure: {{e}}")
    finally:
        db.close()

'''
    
    # Generate algorithm task functions
    algo_functions = "\n# ========== Algorithm Task Functions ==========\n"
    for algo_id in dag_obj.alg_type_nodes:
        if algo_id in algorithm_types:
            algo_functions += generate_task_code_for_algorithm(
                algo_id, algorithm_types[algo_id], dependencies
            )


    # Generate dataset task functions
    dataset_functions = "\n# ========== Dataset Registration Functions ==========\n"
    for dataset_id in dag_obj.dataset_type_nodes:
        is_root = dataset_id in root_datasets
        dataset_functions += generate_task_code_for_dataset(dataset_id, is_root_dataset=is_root)

    
    # Build param definitions from YAML
    param_definitions = "{\n"
    if dag_obj.params:
        for param in dag_obj.params:
            if isinstance(param, dict):
                param_name = param.get('name', param)
                param_type = param.get('type', 'string')
                if param_type == 'integer':
                    default_val = "2020"
                else:
                    default_val = "'default_value'"
            else:
                param_name = param
                default_val = "'default_value'"
            param_definitions += f"        '{param_name}': {default_val},\n"
    
    # Add execution control parameters
    param_definitions += "        'execution_type': 'fullexec',\n"
    param_definitions += "        'updated_algo': '',\n"
    param_definitions += "        'updated_dataset': ''\n"
    param_definitions += "    }"
    
    # Generate DAG instantiation
    dag_definition = f'''
# ========== DAG Definition ==========

default_args = {{
    'owner': 'stacd',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="{dag_obj.description}",
    schedule_interval=None,
    catchup=False,
    tags=['stacd', 'recompute', 'generic'],
    params={param_definitions}
) as dag:
    
    # Branch task to determine execution path
    branch_task = BranchPythonOperator(
        task_id='determine_execution_path',
        python_callable=determine_execution_path,
        provide_context=True
    )
    
    # Create all algorithm tasks
'''
    
    for algo_id in dag_obj.alg_type_nodes:
        dag_definition += f'''
    {algo_id} = PythonOperator(
        task_id='{algo_id}',
        python_callable=execute_{algo_id},
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )
'''
    
    # Create all dataset tasks
    root_datasets_set = set(root_datasets)  # Convert to set for O(1) lookup
    for dataset_id in dag_obj.dataset_type_nodes:
        # Root datasets use fetch_, produced datasets use register_
        callable_name = f"fetch_{dataset_id}" if dataset_id in root_datasets_set else f"register_{dataset_id}"
        
        dag_definition += f'''
    {dataset_id} = PythonOperator(
        task_id='{dataset_id}',
        python_callable={callable_name},
        provide_context=True
    )
'''
        

    
    # Generate dependencies
    dag_definition += "\n    # ============================================================================\n"
    dag_definition += "    # DEPENDENCIES (Generated from YAML input_datasets and outputs)\n"
    dag_definition += "    # ============================================================================\n\n"
    
    # Branch connects to ALL algorithms (for selective execution)
    dag_definition += "    # Branch connects to all algorithms (selective execution)\n"
    for algo_id in dag_obj.alg_type_nodes:
        dag_definition += f"    branch_task >> {algo_id}\n"

    dag_definition += "\n # Branch connects to all root datasets (for update_dataset support)\n"
    for dataset_id in root_datasets:
        dag_definition += f"    branch_task >> {dataset_id}\n"
    
    dag_definition += "\n    # Algorithm -> Dataset dependencies (outputs)\n"
    for algo_id, deps in dependencies.items():
        for output_ds in deps['outputs']:
            dag_definition += f"    {algo_id} >> {output_ds}\n"
    
    dag_definition += "\n    # Dataset -> Algorithm dependencies (inputs)\n"
    for algo_id, deps in dependencies.items():
        for input_ds in deps['inputs']:
            dag_definition += f"    {input_ds} >> {algo_id}\n"

    
    # Combine all parts
    full_code = header + algo_functions + dataset_functions + dag_definition
    
    return full_code

def main():
    """Main entry point"""
    print("="*60)
    print("Generating STACD Recompute DAG (GENERIC)")
    print("="*60)
    
    # Parse YAML
    dag_yaml_path = os.path.join(AIRFLOW_HOME, 'stacd/yaml_configs/stacd_dag.yaml')
    dag_obj, algorithm_types, dataset_types = parse_dag_specification(dag_yaml_path)
    
    # Build dependency graph
    dependencies = build_dependency_graph(dag_obj, algorithm_types)
    
    # Generate DAG code
    dag_code = generate_dag_code(dag_obj, algorithm_types, dataset_types, dependencies)
    
    # Write to file
    output_dir = os.path.join(AIRFLOW_HOME, 'stacd/generated_dags')
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f'{dag_obj.id}_generated.py')
    
    with open(output_file, 'w') as f:
        f.write(dag_code)
    
    print(f"\n✓ DAG generated successfully!")
    print(f"   Output: {output_file}")
    print(f"   DAG ID: {dag_obj.id}")
    print(f"   Algorithms: {', '.join(dag_obj.alg_type_nodes)}")
    print(f"   Datasets: {', '.join(dag_obj.dataset_type_nodes)}")
    print("\n" + "="*60)

if __name__ == "__main__":
    import sys
    
    # Allow passing YAML path as argument
    if len(sys.argv) > 1:
        yaml_path = sys.argv[1]
        print(f"Using custom YAML: {yaml_path}")
    else:
        yaml_path = os.path.join(AIRFLOW_HOME, 'stacd/yaml_configs/stacd_dag.yaml')
        print(f"Using default YAML: {yaml_path}")
    
    # Parse and generate
    dag_obj, algorithm_types, dataset_types = parse_dag_specification(yaml_path)
    dependencies = build_dependency_graph(dag_obj, algorithm_types)
    
    # Generate DAG code
    dag_code = generate_dag_code(dag_obj, algorithm_types, dataset_types, dependencies)
    
    # Write output
    output_dir = os.path.join(AIRFLOW_HOME, 'stacd/generated_dags')
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"{dag_obj.id}_generated.py")
    
    with open(output_file, 'w') as f:
        f.write(dag_code)
    
    print(f"✓ DAG generated: {output_file}")
