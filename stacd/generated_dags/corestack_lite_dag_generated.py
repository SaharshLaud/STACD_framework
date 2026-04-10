"""
Auto-generated STACD DAG: CoRE Stack Lite - Full LULC and Terrain Workflow
Generated at: 2026-04-10T07:17:32.525255
Description: 8-algo workflow hitting corestack-lite Django backend APIs synchronously. Starts with AdminBoundary + MWSLayer before LULC and Terrain processing.

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
sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'enhancement/database'))
from db_operations import STACDDatabase

# ========== Configuration ==========
DAG_ID = "corestack_lite_dag"
DB_PATH = os.path.join(AIRFLOW_HOME_RT, 'enhancement/database/stacd_recompute.db')

# ========== Dependency Graph (for reference) ==========
dependencies = {
    "Admin_Boundary": {
        "inputs": [
            "Pan_India_MWS"
        ],
        "outputs": [
            "Admin_Boundary_Asset"
        ]
    },
    "MWS_Layer": {
        "inputs": [
            "Pan_India_MWS",
            "Admin_Boundary_Asset"
        ],
        "outputs": [
            "MWS_Boundaries"
        ]
    },
    "LULC_Algorithm": {
        "inputs": [
            "MWS_Boundaries"
        ],
        "outputs": [
            "LULC_Raster"
        ]
    },
    "LULC_Vectorization": {
        "inputs": [
            "LULC_Raster"
        ],
        "outputs": [
            "LULC_Vector"
        ]
    },
    "Terrain_Algorithm": {
        "inputs": [
            "MWS_Boundaries"
        ],
        "outputs": [
            "Terrain_Raster"
        ]
    },
    "Terrain_Vectorization": {
        "inputs": [
            "Terrain_Raster"
        ],
        "outputs": [
            "Terrain_Vector"
        ]
    },
    "Terrain_LULC_Slope": {
        "inputs": [
            "LULC_Raster",
            "Terrain_Raster"
        ],
        "outputs": [
            "Terrain_LULC_Vector_Slope"
        ]
    },
    "Terrain_LULC_Plain": {
        "inputs": [
            "LULC_Raster",
            "Terrain_Raster"
        ],
        "outputs": [
            "Terrain_LULC_Vector_Plain"
        ]
    }
}

# ========== Helper Functions ==========

def generate_stac_for_dataset(dataset_instance, params):
    """Generate STAC item for a dataset instance"""
    import sys
    import os
    sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'enhancement/stac_export'))
    
    from stac_builder import STACBuilder
    from config import CATALOG_OUTPUT_DIR
    
    builder = STACBuilder(CATALOG_OUTPUT_DIR / "datasets")
    
    # Parse location from asset_id
    location = builder.parse_asset_location(dataset_instance.asset_id)
    if not location:
        print(f"Warning: Could not parse location from {dataset_instance.asset_id}")
        return
    
    state, district, block = location["state"], location["district"], location["block"]
    
    # Build dataset dict
    dataset_dict = {
        "instance_id": dataset_instance.instance_id,
        "dataset_type_id": dataset_instance.dataset_type_id,
        "asset_id": dataset_instance.asset_id,
        "produced_by_algo": dataset_instance.produced_by_algo,
        "algo_version": dataset_instance.algo_version,
        "run_id": dataset_instance.run_id,
        "meta_info": dataset_instance.meta_info,
        "created_at": dataset_instance.created_at.isoformat() + "Z" if dataset_instance.created_at else None
    }
    
    # Build STAC item
    item = builder.build_item(dataset_dict, state, district, block)
    
    # Write to file
    item_filename = f"{dataset_instance.dataset_type_id}_{dataset_instance.instance_id}.json"
    item_path = CATALOG_OUTPUT_DIR / "datasets" / state / district / block / item_filename
    builder.write_json(item, item_path)
    
    print(f"STAC item written: {item_path}")




    
def augment_and_write_stac(stac_spec, dataset_type_id, instance_id, params):
    """Write the algo-returned stac_spec (already augmented) to the catalog"""
    import sys
    import os
    import json
    sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'enhancement/stac_export'))
    from config import CATALOG_OUTPUT_DIR

    state = params.get('state', 'unknown')
    district = params.get('district', 'unknown')
    block = params.get('block', 'unknown')

    output_dir = CATALOG_OUTPUT_DIR / "datasets" / state / district / block
    output_dir.mkdir(parents=True, exist_ok=True)

    item_filename = f"{dataset_type_id}_{instance_id}.json"
    item_path = output_dir / item_filename

    with open(item_path, 'w') as f:
        json.dump(stac_spec, f, indent=2, ensure_ascii=False)
    print(f"STAC item written: {item_path}")





def get_active_algorithm_config(algo_type_id):
    """Query database for active algorithm version config"""
    db = STACDDatabase(DB_PATH)
    try:
        algo_instance = db.get_active_algorithm_version(algo_type_id)
        if not algo_instance:
            raise ValueError(f"No active version found for {algo_type_id}")
        
        print(f"Using {algo_type_id} version {algo_instance.version}")
        print(f"   Execution modes: {algo_instance.execution_modes}")
        
        return {
            'version': algo_instance.version,
            'execution_modes': algo_instance.execution_modes
        }
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
    
    print(f"🔀 Execution type: {execution_type}")

    if execution_type == 'fullexec':
        # Run all root datasets AND root algorithms
        root_datasets = ['Pan_India_MWS']
        root_algos = ['Admin_Boundary']
        entry_points = root_datasets + root_algos
        print(f"📌 Running root datasets: {root_datasets}")
        print(f"📌 Running root algorithms: {root_algos}")
        return entry_points
    
    elif execution_type == 'update_algo':
        updated_algo = params.get('updated_algo')
        if not updated_algo:
            raise ValueError("updated_algo parameter required for update_algo execution")
        
        print(f"📌 Running from updated algorithm: {updated_algo}")
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
        
        print(f"📌 Dataset {updated_dataset} consumed by: {consumers}")
        print(f"📌 Running: [{updated_dataset}] → {consumers}")

        return [updated_dataset] + consumers if consumers else [updated_dataset]



    elif execution_type == 'update_dag':
        # Run ONLY the newly added algo nodes
        # These are nodes present in current DAG structure but
        # not previously run (i.e. no successful execution in DB)
        db = STACDDatabase(DB_PATH)
        try:
            dag_record = db.get_dag_by_id(DAG_ID)
            if not dag_record:
                raise ValueError(f"DAG {DAG_ID} not found in DB")

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

        print(f" New algo nodes to run: {new_algos}")
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

        print(f" Resuming from {len(failed_algos)} failed task(s): {failed_algos}")
        return failed_algos

    
    else:
        raise ValueError(f"Unknown execution_type: {execution_type}")

def log_algo_failure(context):
    """Called automatically by Airflow when an algo task fails"""
    ti = context['ti']
    run_id = context['run_id']
    params = context.get('params', {})
    algo_type_id = ti.task_id

    execution_params = {
        'state': params.get('state'),
        'district': params.get('district'),
        'block': params.get('block'),
        'start_year': params.get('start_year'),
        'end_year': params.get('end_year'),
        'execution_id': run_id
    }

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
            print(f"Logged failure for {algo_type_id} run_id={run_id}")
    except Exception as e:
        print(f"Could not log failure: {e}")
    finally:
        db.close()


# ========== Algorithm Task Functions ==========

def execute_Admin_Boundary(**context):
    """
    Execute Admin_Boundary - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Admin_Boundary")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    run_id = context['run_id']

    # Get inputs from upstream dataset tasks
    Pan_India_MWS_asset_id = ti.xcom_pull(task_ids='Pan_India_MWS', key='asset_id')

    # Extract parameters from context
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')

    
    # Query database for active version
    algo_config = get_active_algorithm_config('Admin_Boundary')
    version = algo_config['version']
    execution_modes = algo_config['execution_modes']
    
    print(f"📌 Active Version: {version}")
    print(f"📋 Execution Modes: {execution_modes}")
    
    # Get API and Docker configs
    api_config = execution_modes.get('api', {})
    docker_config = execution_modes.get('docker', {})
    
    # Determine execution mode based on priority
    api_enabled = api_config.get('enabled', False)
    docker_enabled = docker_config.get('enabled', False)
    
    api_priority = api_config.get('priority', 99) if api_enabled else 99
    docker_priority = docker_config.get('priority', 99) if docker_enabled else 99
    
    use_docker = docker_enabled and (docker_priority < api_priority)
    use_api = api_enabled and not use_docker
    
    # Build algorithm parameters (for both API and Docker)
    algo_params = {
        'execution_id': run_id,
        'state': state,
        'district': district,
        'block': block,
        'Pan_India_MWS': Pan_India_MWS_asset_id
    }
    
    print(f"🔍 Parameters: {algo_params}")
    
    if use_docker:
        # ===== DOCKER EXECUTION =====
        print(f"🐳 Docker Mode Selected (priority: {docker_priority})")
        print(f"   Image: {docker_config.get('image')}")
        print(f"   Module: {docker_config.get('module')}")
        print(f"   Function: {docker_config.get('function')}")
        
        # Import Docker runner
        AIRFLOW_HOME_RT = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
        sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'enhancement'))
        from simple_docker_runner import run_in_docker
        
        # Extract function params (remove execution_id for function call)
        function_params = {k: v for k, v in algo_params.items() if k != 'execution_id'}



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
        print(f" API Mode Selected (priority: {api_priority})")
        print(f"   URL: {api_config['url']}")

        try:
            from airflow.models import Variable
            token = Variable.get("CORESTACK_AUTH_TOKEN", default_var=None)
        except Exception as token_err:
            print(f"WARNING: Could not fetch CORESTACK_AUTH_TOKEN: {token_err}")
            token = None

        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

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
        raise ValueError(f"No execution mode enabled for Admin_Boundary")
    
    print(f"✓ Admin_Boundary completed successfully")
    print(f"   Output asset: {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Admin_Boundary',
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
    return {'status': 'success', 'asset_id': asset_id, 'version': version, 'hosting_platform': hosting_platform}


def execute_MWS_Layer(**context):
    """
    Execute MWS_Layer - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: MWS_Layer")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    run_id = context['run_id']

    # Get inputs from upstream dataset tasks
    Pan_India_MWS_asset_id = ti.xcom_pull(task_ids='Pan_India_MWS', key='asset_id')
    Admin_Boundary_Asset_asset_id = ti.xcom_pull(task_ids='Admin_Boundary_Asset', key='asset_id')

    # Extract parameters from context
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')

    
    # Query database for active version
    algo_config = get_active_algorithm_config('MWS_Layer')
    version = algo_config['version']
    execution_modes = algo_config['execution_modes']
    
    print(f"📌 Active Version: {version}")
    print(f"📋 Execution Modes: {execution_modes}")
    
    # Get API and Docker configs
    api_config = execution_modes.get('api', {})
    docker_config = execution_modes.get('docker', {})
    
    # Determine execution mode based on priority
    api_enabled = api_config.get('enabled', False)
    docker_enabled = docker_config.get('enabled', False)
    
    api_priority = api_config.get('priority', 99) if api_enabled else 99
    docker_priority = docker_config.get('priority', 99) if docker_enabled else 99
    
    use_docker = docker_enabled and (docker_priority < api_priority)
    use_api = api_enabled and not use_docker
    
    # Build algorithm parameters (for both API and Docker)
    algo_params = {
        'execution_id': run_id,
        'state': state,
        'district': district,
        'block': block,
        'Pan_India_MWS': Pan_India_MWS_asset_id,
        'Admin_Boundary_Asset': Admin_Boundary_Asset_asset_id
    }
    
    print(f"🔍 Parameters: {algo_params}")
    
    if use_docker:
        # ===== DOCKER EXECUTION =====
        print(f"🐳 Docker Mode Selected (priority: {docker_priority})")
        print(f"   Image: {docker_config.get('image')}")
        print(f"   Module: {docker_config.get('module')}")
        print(f"   Function: {docker_config.get('function')}")
        
        # Import Docker runner
        AIRFLOW_HOME_RT = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
        sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'enhancement'))
        from simple_docker_runner import run_in_docker
        
        # Extract function params (remove execution_id for function call)
        function_params = {k: v for k, v in algo_params.items() if k != 'execution_id'}



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
        print(f" API Mode Selected (priority: {api_priority})")
        print(f"   URL: {api_config['url']}")

        try:
            from airflow.models import Variable
            token = Variable.get("CORESTACK_AUTH_TOKEN", default_var=None)
        except Exception as token_err:
            print(f"WARNING: Could not fetch CORESTACK_AUTH_TOKEN: {token_err}")
            token = None

        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

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
        raise ValueError(f"No execution mode enabled for MWS_Layer")
    
    print(f"✓ MWS_Layer completed successfully")
    print(f"   Output asset: {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='MWS_Layer',
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
    return {'status': 'success', 'asset_id': asset_id, 'version': version, 'hosting_platform': hosting_platform}


def execute_LULC_Algorithm(**context):
    """
    Execute LULC_Algorithm - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: LULC_Algorithm")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    run_id = context['run_id']

    # Get inputs from upstream dataset tasks
    MWS_Boundaries_asset_id = ti.xcom_pull(task_ids='MWS_Boundaries', key='asset_id')

    # Extract parameters from context
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')
    start_year = int(params.get('start_year'))
    end_year = int(params.get('end_year'))

    
    # Query database for active version
    algo_config = get_active_algorithm_config('LULC_Algorithm')
    version = algo_config['version']
    execution_modes = algo_config['execution_modes']
    
    print(f"📌 Active Version: {version}")
    print(f"📋 Execution Modes: {execution_modes}")
    
    # Get API and Docker configs
    api_config = execution_modes.get('api', {})
    docker_config = execution_modes.get('docker', {})
    
    # Determine execution mode based on priority
    api_enabled = api_config.get('enabled', False)
    docker_enabled = docker_config.get('enabled', False)
    
    api_priority = api_config.get('priority', 99) if api_enabled else 99
    docker_priority = docker_config.get('priority', 99) if docker_enabled else 99
    
    use_docker = docker_enabled and (docker_priority < api_priority)
    use_api = api_enabled and not use_docker
    
    # Build algorithm parameters (for both API and Docker)
    algo_params = {
        'execution_id': run_id,
        'state': state,
        'district': district,
        'block': block,
        'start_year': start_year,
        'end_year': end_year,
        'MWS_Boundaries': MWS_Boundaries_asset_id
    }
    
    print(f"🔍 Parameters: {algo_params}")
    
    if use_docker:
        # ===== DOCKER EXECUTION =====
        print(f"🐳 Docker Mode Selected (priority: {docker_priority})")
        print(f"   Image: {docker_config.get('image')}")
        print(f"   Module: {docker_config.get('module')}")
        print(f"   Function: {docker_config.get('function')}")
        
        # Import Docker runner
        AIRFLOW_HOME_RT = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
        sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'enhancement'))
        from simple_docker_runner import run_in_docker
        
        # Extract function params (remove execution_id for function call)
        function_params = {k: v for k, v in algo_params.items() if k != 'execution_id'}



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
        print(f" API Mode Selected (priority: {api_priority})")
        print(f"   URL: {api_config['url']}")

        try:
            from airflow.models import Variable
            token = Variable.get("CORESTACK_AUTH_TOKEN", default_var=None)
        except Exception as token_err:
            print(f"WARNING: Could not fetch CORESTACK_AUTH_TOKEN: {token_err}")
            token = None

        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

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
        raise ValueError(f"No execution mode enabled for LULC_Algorithm")
    
    print(f"✓ LULC_Algorithm completed successfully")
    print(f"   Output asset: {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='LULC_Algorithm',
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
    return {'status': 'success', 'asset_id': asset_id, 'version': version, 'hosting_platform': hosting_platform}


def execute_LULC_Vectorization(**context):
    """
    Execute LULC_Vectorization - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: LULC_Vectorization")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    run_id = context['run_id']

    # Get inputs from upstream dataset tasks
    LULC_Raster_asset_id = ti.xcom_pull(task_ids='LULC_Raster', key='asset_id')

    # Extract parameters from context
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')
    start_year = int(params.get('start_year'))
    end_year = int(params.get('end_year'))

    
    # Query database for active version
    algo_config = get_active_algorithm_config('LULC_Vectorization')
    version = algo_config['version']
    execution_modes = algo_config['execution_modes']
    
    print(f"📌 Active Version: {version}")
    print(f"📋 Execution Modes: {execution_modes}")
    
    # Get API and Docker configs
    api_config = execution_modes.get('api', {})
    docker_config = execution_modes.get('docker', {})
    
    # Determine execution mode based on priority
    api_enabled = api_config.get('enabled', False)
    docker_enabled = docker_config.get('enabled', False)
    
    api_priority = api_config.get('priority', 99) if api_enabled else 99
    docker_priority = docker_config.get('priority', 99) if docker_enabled else 99
    
    use_docker = docker_enabled and (docker_priority < api_priority)
    use_api = api_enabled and not use_docker
    
    # Build algorithm parameters (for both API and Docker)
    algo_params = {
        'execution_id': run_id,
        'state': state,
        'district': district,
        'block': block,
        'start_year': start_year,
        'end_year': end_year,
        'LULC_Raster': LULC_Raster_asset_id
    }
    
    print(f"🔍 Parameters: {algo_params}")
    
    if use_docker:
        # ===== DOCKER EXECUTION =====
        print(f"🐳 Docker Mode Selected (priority: {docker_priority})")
        print(f"   Image: {docker_config.get('image')}")
        print(f"   Module: {docker_config.get('module')}")
        print(f"   Function: {docker_config.get('function')}")
        
        # Import Docker runner
        AIRFLOW_HOME_RT = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
        sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'enhancement'))
        from simple_docker_runner import run_in_docker
        
        # Extract function params (remove execution_id for function call)
        function_params = {k: v for k, v in algo_params.items() if k != 'execution_id'}



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
        print(f" API Mode Selected (priority: {api_priority})")
        print(f"   URL: {api_config['url']}")

        try:
            from airflow.models import Variable
            token = Variable.get("CORESTACK_AUTH_TOKEN", default_var=None)
        except Exception as token_err:
            print(f"WARNING: Could not fetch CORESTACK_AUTH_TOKEN: {token_err}")
            token = None

        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

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
        raise ValueError(f"No execution mode enabled for LULC_Vectorization")
    
    print(f"✓ LULC_Vectorization completed successfully")
    print(f"   Output asset: {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='LULC_Vectorization',
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
    return {'status': 'success', 'asset_id': asset_id, 'version': version, 'hosting_platform': hosting_platform}


def execute_Terrain_Algorithm(**context):
    """
    Execute Terrain_Algorithm - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Terrain_Algorithm")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    run_id = context['run_id']

    # Get inputs from upstream dataset tasks
    MWS_Boundaries_asset_id = ti.xcom_pull(task_ids='MWS_Boundaries', key='asset_id')

    # Extract parameters from context
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')

    
    # Query database for active version
    algo_config = get_active_algorithm_config('Terrain_Algorithm')
    version = algo_config['version']
    execution_modes = algo_config['execution_modes']
    
    print(f"📌 Active Version: {version}")
    print(f"📋 Execution Modes: {execution_modes}")
    
    # Get API and Docker configs
    api_config = execution_modes.get('api', {})
    docker_config = execution_modes.get('docker', {})
    
    # Determine execution mode based on priority
    api_enabled = api_config.get('enabled', False)
    docker_enabled = docker_config.get('enabled', False)
    
    api_priority = api_config.get('priority', 99) if api_enabled else 99
    docker_priority = docker_config.get('priority', 99) if docker_enabled else 99
    
    use_docker = docker_enabled and (docker_priority < api_priority)
    use_api = api_enabled and not use_docker
    
    # Build algorithm parameters (for both API and Docker)
    algo_params = {
        'execution_id': run_id,
        'state': state,
        'district': district,
        'block': block,
        'MWS_Boundaries': MWS_Boundaries_asset_id
    }
    
    print(f"🔍 Parameters: {algo_params}")
    
    if use_docker:
        # ===== DOCKER EXECUTION =====
        print(f"🐳 Docker Mode Selected (priority: {docker_priority})")
        print(f"   Image: {docker_config.get('image')}")
        print(f"   Module: {docker_config.get('module')}")
        print(f"   Function: {docker_config.get('function')}")
        
        # Import Docker runner
        AIRFLOW_HOME_RT = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
        sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'enhancement'))
        from simple_docker_runner import run_in_docker
        
        # Extract function params (remove execution_id for function call)
        function_params = {k: v for k, v in algo_params.items() if k != 'execution_id'}



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
        print(f" API Mode Selected (priority: {api_priority})")
        print(f"   URL: {api_config['url']}")

        try:
            from airflow.models import Variable
            token = Variable.get("CORESTACK_AUTH_TOKEN", default_var=None)
        except Exception as token_err:
            print(f"WARNING: Could not fetch CORESTACK_AUTH_TOKEN: {token_err}")
            token = None

        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

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
        raise ValueError(f"No execution mode enabled for Terrain_Algorithm")
    
    print(f"✓ Terrain_Algorithm completed successfully")
    print(f"   Output asset: {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Terrain_Algorithm',
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
    return {'status': 'success', 'asset_id': asset_id, 'version': version, 'hosting_platform': hosting_platform}


def execute_Terrain_Vectorization(**context):
    """
    Execute Terrain_Vectorization - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Terrain_Vectorization")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    run_id = context['run_id']

    # Get inputs from upstream dataset tasks
    Terrain_Raster_asset_id = ti.xcom_pull(task_ids='Terrain_Raster', key='asset_id')

    # Extract parameters from context
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')

    
    # Query database for active version
    algo_config = get_active_algorithm_config('Terrain_Vectorization')
    version = algo_config['version']
    execution_modes = algo_config['execution_modes']
    
    print(f"📌 Active Version: {version}")
    print(f"📋 Execution Modes: {execution_modes}")
    
    # Get API and Docker configs
    api_config = execution_modes.get('api', {})
    docker_config = execution_modes.get('docker', {})
    
    # Determine execution mode based on priority
    api_enabled = api_config.get('enabled', False)
    docker_enabled = docker_config.get('enabled', False)
    
    api_priority = api_config.get('priority', 99) if api_enabled else 99
    docker_priority = docker_config.get('priority', 99) if docker_enabled else 99
    
    use_docker = docker_enabled and (docker_priority < api_priority)
    use_api = api_enabled and not use_docker
    
    # Build algorithm parameters (for both API and Docker)
    algo_params = {
        'execution_id': run_id,
        'state': state,
        'district': district,
        'block': block,
        'Terrain_Raster': Terrain_Raster_asset_id
    }
    
    print(f"🔍 Parameters: {algo_params}")
    
    if use_docker:
        # ===== DOCKER EXECUTION =====
        print(f"🐳 Docker Mode Selected (priority: {docker_priority})")
        print(f"   Image: {docker_config.get('image')}")
        print(f"   Module: {docker_config.get('module')}")
        print(f"   Function: {docker_config.get('function')}")
        
        # Import Docker runner
        AIRFLOW_HOME_RT = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
        sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'enhancement'))
        from simple_docker_runner import run_in_docker
        
        # Extract function params (remove execution_id for function call)
        function_params = {k: v for k, v in algo_params.items() if k != 'execution_id'}



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
        print(f" API Mode Selected (priority: {api_priority})")
        print(f"   URL: {api_config['url']}")

        try:
            from airflow.models import Variable
            token = Variable.get("CORESTACK_AUTH_TOKEN", default_var=None)
        except Exception as token_err:
            print(f"WARNING: Could not fetch CORESTACK_AUTH_TOKEN: {token_err}")
            token = None

        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

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
        raise ValueError(f"No execution mode enabled for Terrain_Vectorization")
    
    print(f"✓ Terrain_Vectorization completed successfully")
    print(f"   Output asset: {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Terrain_Vectorization',
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
    return {'status': 'success', 'asset_id': asset_id, 'version': version, 'hosting_platform': hosting_platform}


def execute_Terrain_LULC_Slope(**context):
    """
    Execute Terrain_LULC_Slope - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Terrain_LULC_Slope")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    run_id = context['run_id']

    # Get inputs from upstream dataset tasks
    LULC_Raster_asset_id = ti.xcom_pull(task_ids='LULC_Raster', key='asset_id')
    Terrain_Raster_asset_id = ti.xcom_pull(task_ids='Terrain_Raster', key='asset_id')

    # Extract parameters from context
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')
    start_year = int(params.get('start_year'))
    end_year = int(params.get('end_year'))

    
    # Query database for active version
    algo_config = get_active_algorithm_config('Terrain_LULC_Slope')
    version = algo_config['version']
    execution_modes = algo_config['execution_modes']
    
    print(f"📌 Active Version: {version}")
    print(f"📋 Execution Modes: {execution_modes}")
    
    # Get API and Docker configs
    api_config = execution_modes.get('api', {})
    docker_config = execution_modes.get('docker', {})
    
    # Determine execution mode based on priority
    api_enabled = api_config.get('enabled', False)
    docker_enabled = docker_config.get('enabled', False)
    
    api_priority = api_config.get('priority', 99) if api_enabled else 99
    docker_priority = docker_config.get('priority', 99) if docker_enabled else 99
    
    use_docker = docker_enabled and (docker_priority < api_priority)
    use_api = api_enabled and not use_docker
    
    # Build algorithm parameters (for both API and Docker)
    algo_params = {
        'execution_id': run_id,
        'state': state,
        'district': district,
        'block': block,
        'start_year': start_year,
        'end_year': end_year,
        'LULC_Raster': LULC_Raster_asset_id,
        'Terrain_Raster': Terrain_Raster_asset_id
    }
    
    print(f"🔍 Parameters: {algo_params}")
    
    if use_docker:
        # ===== DOCKER EXECUTION =====
        print(f"🐳 Docker Mode Selected (priority: {docker_priority})")
        print(f"   Image: {docker_config.get('image')}")
        print(f"   Module: {docker_config.get('module')}")
        print(f"   Function: {docker_config.get('function')}")
        
        # Import Docker runner
        AIRFLOW_HOME_RT = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
        sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'enhancement'))
        from simple_docker_runner import run_in_docker
        
        # Extract function params (remove execution_id for function call)
        function_params = {k: v for k, v in algo_params.items() if k != 'execution_id'}



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
        print(f" API Mode Selected (priority: {api_priority})")
        print(f"   URL: {api_config['url']}")

        try:
            from airflow.models import Variable
            token = Variable.get("CORESTACK_AUTH_TOKEN", default_var=None)
        except Exception as token_err:
            print(f"WARNING: Could not fetch CORESTACK_AUTH_TOKEN: {token_err}")
            token = None

        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

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
        raise ValueError(f"No execution mode enabled for Terrain_LULC_Slope")
    
    print(f"✓ Terrain_LULC_Slope completed successfully")
    print(f"   Output asset: {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Terrain_LULC_Slope',
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
    return {'status': 'success', 'asset_id': asset_id, 'version': version, 'hosting_platform': hosting_platform}


def execute_Terrain_LULC_Plain(**context):
    """
    Execute Terrain_LULC_Plain - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Terrain_LULC_Plain")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    run_id = context['run_id']

    # Get inputs from upstream dataset tasks
    LULC_Raster_asset_id = ti.xcom_pull(task_ids='LULC_Raster', key='asset_id')
    Terrain_Raster_asset_id = ti.xcom_pull(task_ids='Terrain_Raster', key='asset_id')

    # Extract parameters from context
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')
    start_year = int(params.get('start_year'))
    end_year = int(params.get('end_year'))

    
    # Query database for active version
    algo_config = get_active_algorithm_config('Terrain_LULC_Plain')
    version = algo_config['version']
    execution_modes = algo_config['execution_modes']
    
    print(f"📌 Active Version: {version}")
    print(f"📋 Execution Modes: {execution_modes}")
    
    # Get API and Docker configs
    api_config = execution_modes.get('api', {})
    docker_config = execution_modes.get('docker', {})
    
    # Determine execution mode based on priority
    api_enabled = api_config.get('enabled', False)
    docker_enabled = docker_config.get('enabled', False)
    
    api_priority = api_config.get('priority', 99) if api_enabled else 99
    docker_priority = docker_config.get('priority', 99) if docker_enabled else 99
    
    use_docker = docker_enabled and (docker_priority < api_priority)
    use_api = api_enabled and not use_docker
    
    # Build algorithm parameters (for both API and Docker)
    algo_params = {
        'execution_id': run_id,
        'state': state,
        'district': district,
        'block': block,
        'start_year': start_year,
        'end_year': end_year,
        'LULC_Raster': LULC_Raster_asset_id,
        'Terrain_Raster': Terrain_Raster_asset_id
    }
    
    print(f"🔍 Parameters: {algo_params}")
    
    if use_docker:
        # ===== DOCKER EXECUTION =====
        print(f"🐳 Docker Mode Selected (priority: {docker_priority})")
        print(f"   Image: {docker_config.get('image')}")
        print(f"   Module: {docker_config.get('module')}")
        print(f"   Function: {docker_config.get('function')}")
        
        # Import Docker runner
        AIRFLOW_HOME_RT = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
        sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'enhancement'))
        from simple_docker_runner import run_in_docker
        
        # Extract function params (remove execution_id for function call)
        function_params = {k: v for k, v in algo_params.items() if k != 'execution_id'}



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
        print(f" API Mode Selected (priority: {api_priority})")
        print(f"   URL: {api_config['url']}")

        try:
            from airflow.models import Variable
            token = Variable.get("CORESTACK_AUTH_TOKEN", default_var=None)
        except Exception as token_err:
            print(f"WARNING: Could not fetch CORESTACK_AUTH_TOKEN: {token_err}")
            token = None

        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

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
        raise ValueError(f"No execution mode enabled for Terrain_LULC_Plain")
    
    print(f"✓ Terrain_LULC_Plain completed successfully")
    print(f"   Output asset: {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Terrain_LULC_Plain',
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
    return {'status': 'success', 'asset_id': asset_id, 'version': version, 'hosting_platform': hosting_platform}


# ========== Dataset Registration Functions ==========

def fetch_Pan_India_MWS(**context):
    """
    Fetch Pan_India_MWS root dataset from database
    Root datasets are pre-existing and not produced by algorithms.
    """
    print("="*60)
    print("Fetching Root Dataset: Pan_India_MWS")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    
    # Extract region parameters
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')

    execution_type = params.get('execution_type', 'fullexec')
    updated_dataset = params.get('updated_dataset', '')
    is_being_updated = (execution_type == 'update_dataset' and updated_dataset == 'Pan_India_MWS')

    print(f"🔍 Looking for Pan_India_MWS in region: {state}/{district}/{block}")
    if is_being_updated:
        print(f"⚡ update_dataset mode — will use latest registered version")

    # Query database for root dataset (always gets latest version)
    db = STACDDatabase(DB_PATH)
    try:
        root_dataset = db.get_root_dataset(
            dataset_type_id='Pan_India_MWS',
            state=state,
            district=district,
            block=block
        )
        
        if not root_dataset:
            raise ValueError(f"Root dataset Pan_India_MWS not found for {state}/{district}/{block}")
        
        asset_id = root_dataset.asset_id
        version = root_dataset.version
        print(f"✓ Found Pan_India_MWS version {version}")
        print(f"   Asset ID: {asset_id}")
        if is_being_updated:
            print(f"⚡ Propagating updated dataset v{version} to downstream algorithms")
        
    finally:
        db.close()
    
    # Push asset_id for downstream consumption
    ti.xcom_push(key='asset_id', value=asset_id)
    ti.xcom_push(key='version', value=version)
    ti.xcom_push(key='is_updated', value=is_being_updated)
    return {'status': 'success', 'asset_id': asset_id, 'version': version, 'is_root': True, 'is_updated': is_being_updated}


def register_Admin_Boundary_Asset(**context):
    """Register Admin_Boundary_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Admin_Boundary_Asset")
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
        raise ValueError(f"No asset_id found from upstream for Admin_Boundary_Asset")

    print(f"Registering Admin_Boundary_Asset")
    print(f"Asset ID: {asset_id}")
    print(f"Produced by: {producing_algo} v{algo_version}")
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')
    start_year = params.get('start_year')
    end_year = params.get('end_year')
        
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
    
        instance = db.log_dataset_instance(
            dataset_type_id="Admin_Boundary_Asset",
            asset_id=asset_id,
            produced_by_algo=producing_algo,
            algo_version=algo_version,
            run_id=run_id,
            meta_info={"registered_at": str(datetime.now()),
                'state': state,         
                'district': district,   
                'block': block,         
                'start_year': str(start_year), 
                'end_year': str(end_year)   }
        )



        version = instance.version
        print(f"Admin_Boundary_Asset registered in database")
        print(f"Version: {version}")

        # Augment stac_spec with STAC-D provenance fields
        if stac_spec:
            stac_spec["properties"]["stacd:dag_id"] = DAG_ID
            stac_spec["properties"]["stacd:run_id"] = run_id
            stac_spec["properties"]["stacd:producing_algo"] = producing_algo
            stac_spec["properties"]["stacd:algo_version"] = algo_version
            stac_spec["properties"]["stacd:dataset_type"] = "Admin_Boundary_Asset"
            stac_spec["properties"]["stacd:instance_id"] = instance.instance_id
            stac_spec["properties"]["stacd:dataset_version"] = version
            stac_spec["id"] = f"{state}_{district}_{block}_Admin_Boundary_Asset_v{version}"

            # Write augmented STAC item to catalog
            print("Writing augmented STAC item...")
            augment_and_write_stac(stac_spec, "Admin_Boundary_Asset", instance.instance_id, params)
            print(f"STAC item written for Admin_Boundary_Asset")
        else:
            # Fallback: generate from DB record (original path)
            print("No stac_spec from algo, falling back to DB-based generation...")
            generate_stac_for_dataset(instance, params)
            print(f"STAC item generated for Admin_Boundary_Asset")
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_MWS_Boundaries(**context):
    """Register MWS_Boundaries dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: MWS_Boundaries")
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
        raise ValueError(f"No asset_id found from upstream for MWS_Boundaries")

    print(f"Registering MWS_Boundaries")
    print(f"Asset ID: {asset_id}")
    print(f"Produced by: {producing_algo} v{algo_version}")
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')
    start_year = params.get('start_year')
    end_year = params.get('end_year')
        
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
    
        instance = db.log_dataset_instance(
            dataset_type_id="MWS_Boundaries",
            asset_id=asset_id,
            produced_by_algo=producing_algo,
            algo_version=algo_version,
            run_id=run_id,
            meta_info={"registered_at": str(datetime.now()),
                'state': state,         
                'district': district,   
                'block': block,         
                'start_year': str(start_year), 
                'end_year': str(end_year)   }
        )



        version = instance.version
        print(f"MWS_Boundaries registered in database")
        print(f"Version: {version}")

        # Augment stac_spec with STAC-D provenance fields
        if stac_spec:
            stac_spec["properties"]["stacd:dag_id"] = DAG_ID
            stac_spec["properties"]["stacd:run_id"] = run_id
            stac_spec["properties"]["stacd:producing_algo"] = producing_algo
            stac_spec["properties"]["stacd:algo_version"] = algo_version
            stac_spec["properties"]["stacd:dataset_type"] = "MWS_Boundaries"
            stac_spec["properties"]["stacd:instance_id"] = instance.instance_id
            stac_spec["properties"]["stacd:dataset_version"] = version
            stac_spec["id"] = f"{state}_{district}_{block}_MWS_Boundaries_v{version}"

            # Write augmented STAC item to catalog
            print("Writing augmented STAC item...")
            augment_and_write_stac(stac_spec, "MWS_Boundaries", instance.instance_id, params)
            print(f"STAC item written for MWS_Boundaries")
        else:
            # Fallback: generate from DB record (original path)
            print("No stac_spec from algo, falling back to DB-based generation...")
            generate_stac_for_dataset(instance, params)
            print(f"STAC item generated for MWS_Boundaries")
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_LULC_Raster(**context):
    """Register LULC_Raster dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: LULC_Raster")
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
        raise ValueError(f"No asset_id found from upstream for LULC_Raster")

    print(f"Registering LULC_Raster")
    print(f"Asset ID: {asset_id}")
    print(f"Produced by: {producing_algo} v{algo_version}")
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')
    start_year = params.get('start_year')
    end_year = params.get('end_year')
        
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
    
        instance = db.log_dataset_instance(
            dataset_type_id="LULC_Raster",
            asset_id=asset_id,
            produced_by_algo=producing_algo,
            algo_version=algo_version,
            run_id=run_id,
            meta_info={"registered_at": str(datetime.now()),
                'state': state,         
                'district': district,   
                'block': block,         
                'start_year': str(start_year), 
                'end_year': str(end_year)   }
        )



        version = instance.version
        print(f"LULC_Raster registered in database")
        print(f"Version: {version}")

        # Augment stac_spec with STAC-D provenance fields
        if stac_spec:
            stac_spec["properties"]["stacd:dag_id"] = DAG_ID
            stac_spec["properties"]["stacd:run_id"] = run_id
            stac_spec["properties"]["stacd:producing_algo"] = producing_algo
            stac_spec["properties"]["stacd:algo_version"] = algo_version
            stac_spec["properties"]["stacd:dataset_type"] = "LULC_Raster"
            stac_spec["properties"]["stacd:instance_id"] = instance.instance_id
            stac_spec["properties"]["stacd:dataset_version"] = version
            stac_spec["id"] = f"{state}_{district}_{block}_LULC_Raster_v{version}"

            # Write augmented STAC item to catalog
            print("Writing augmented STAC item...")
            augment_and_write_stac(stac_spec, "LULC_Raster", instance.instance_id, params)
            print(f"STAC item written for LULC_Raster")
        else:
            # Fallback: generate from DB record (original path)
            print("No stac_spec from algo, falling back to DB-based generation...")
            generate_stac_for_dataset(instance, params)
            print(f"STAC item generated for LULC_Raster")
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_LULC_Vector(**context):
    """Register LULC_Vector dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: LULC_Vector")
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
        raise ValueError(f"No asset_id found from upstream for LULC_Vector")

    print(f"Registering LULC_Vector")
    print(f"Asset ID: {asset_id}")
    print(f"Produced by: {producing_algo} v{algo_version}")
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')
    start_year = params.get('start_year')
    end_year = params.get('end_year')
        
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
    
        instance = db.log_dataset_instance(
            dataset_type_id="LULC_Vector",
            asset_id=asset_id,
            produced_by_algo=producing_algo,
            algo_version=algo_version,
            run_id=run_id,
            meta_info={"registered_at": str(datetime.now()),
                'state': state,         
                'district': district,   
                'block': block,         
                'start_year': str(start_year), 
                'end_year': str(end_year)   }
        )



        version = instance.version
        print(f"LULC_Vector registered in database")
        print(f"Version: {version}")

        # Augment stac_spec with STAC-D provenance fields
        if stac_spec:
            stac_spec["properties"]["stacd:dag_id"] = DAG_ID
            stac_spec["properties"]["stacd:run_id"] = run_id
            stac_spec["properties"]["stacd:producing_algo"] = producing_algo
            stac_spec["properties"]["stacd:algo_version"] = algo_version
            stac_spec["properties"]["stacd:dataset_type"] = "LULC_Vector"
            stac_spec["properties"]["stacd:instance_id"] = instance.instance_id
            stac_spec["properties"]["stacd:dataset_version"] = version
            stac_spec["id"] = f"{state}_{district}_{block}_LULC_Vector_v{version}"

            # Write augmented STAC item to catalog
            print("Writing augmented STAC item...")
            augment_and_write_stac(stac_spec, "LULC_Vector", instance.instance_id, params)
            print(f"STAC item written for LULC_Vector")
        else:
            # Fallback: generate from DB record (original path)
            print("No stac_spec from algo, falling back to DB-based generation...")
            generate_stac_for_dataset(instance, params)
            print(f"STAC item generated for LULC_Vector")
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Terrain_Raster(**context):
    """Register Terrain_Raster dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Terrain_Raster")
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
        raise ValueError(f"No asset_id found from upstream for Terrain_Raster")

    print(f"Registering Terrain_Raster")
    print(f"Asset ID: {asset_id}")
    print(f"Produced by: {producing_algo} v{algo_version}")
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')
    start_year = params.get('start_year')
    end_year = params.get('end_year')
        
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
    
        instance = db.log_dataset_instance(
            dataset_type_id="Terrain_Raster",
            asset_id=asset_id,
            produced_by_algo=producing_algo,
            algo_version=algo_version,
            run_id=run_id,
            meta_info={"registered_at": str(datetime.now()),
                'state': state,         
                'district': district,   
                'block': block,         
                'start_year': str(start_year), 
                'end_year': str(end_year)   }
        )



        version = instance.version
        print(f"Terrain_Raster registered in database")
        print(f"Version: {version}")

        # Augment stac_spec with STAC-D provenance fields
        if stac_spec:
            stac_spec["properties"]["stacd:dag_id"] = DAG_ID
            stac_spec["properties"]["stacd:run_id"] = run_id
            stac_spec["properties"]["stacd:producing_algo"] = producing_algo
            stac_spec["properties"]["stacd:algo_version"] = algo_version
            stac_spec["properties"]["stacd:dataset_type"] = "Terrain_Raster"
            stac_spec["properties"]["stacd:instance_id"] = instance.instance_id
            stac_spec["properties"]["stacd:dataset_version"] = version
            stac_spec["id"] = f"{state}_{district}_{block}_Terrain_Raster_v{version}"

            # Write augmented STAC item to catalog
            print("Writing augmented STAC item...")
            augment_and_write_stac(stac_spec, "Terrain_Raster", instance.instance_id, params)
            print(f"STAC item written for Terrain_Raster")
        else:
            # Fallback: generate from DB record (original path)
            print("No stac_spec from algo, falling back to DB-based generation...")
            generate_stac_for_dataset(instance, params)
            print(f"STAC item generated for Terrain_Raster")
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Terrain_Vector(**context):
    """Register Terrain_Vector dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Terrain_Vector")
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
        raise ValueError(f"No asset_id found from upstream for Terrain_Vector")

    print(f"Registering Terrain_Vector")
    print(f"Asset ID: {asset_id}")
    print(f"Produced by: {producing_algo} v{algo_version}")
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')
    start_year = params.get('start_year')
    end_year = params.get('end_year')
        
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
    
        instance = db.log_dataset_instance(
            dataset_type_id="Terrain_Vector",
            asset_id=asset_id,
            produced_by_algo=producing_algo,
            algo_version=algo_version,
            run_id=run_id,
            meta_info={"registered_at": str(datetime.now()),
                'state': state,         
                'district': district,   
                'block': block,         
                'start_year': str(start_year), 
                'end_year': str(end_year)   }
        )



        version = instance.version
        print(f"Terrain_Vector registered in database")
        print(f"Version: {version}")

        # Augment stac_spec with STAC-D provenance fields
        if stac_spec:
            stac_spec["properties"]["stacd:dag_id"] = DAG_ID
            stac_spec["properties"]["stacd:run_id"] = run_id
            stac_spec["properties"]["stacd:producing_algo"] = producing_algo
            stac_spec["properties"]["stacd:algo_version"] = algo_version
            stac_spec["properties"]["stacd:dataset_type"] = "Terrain_Vector"
            stac_spec["properties"]["stacd:instance_id"] = instance.instance_id
            stac_spec["properties"]["stacd:dataset_version"] = version
            stac_spec["id"] = f"{state}_{district}_{block}_Terrain_Vector_v{version}"

            # Write augmented STAC item to catalog
            print("Writing augmented STAC item...")
            augment_and_write_stac(stac_spec, "Terrain_Vector", instance.instance_id, params)
            print(f"STAC item written for Terrain_Vector")
        else:
            # Fallback: generate from DB record (original path)
            print("No stac_spec from algo, falling back to DB-based generation...")
            generate_stac_for_dataset(instance, params)
            print(f"STAC item generated for Terrain_Vector")
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Terrain_LULC_Vector_Slope(**context):
    """Register Terrain_LULC_Vector_Slope dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Terrain_LULC_Vector_Slope")
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
        raise ValueError(f"No asset_id found from upstream for Terrain_LULC_Vector_Slope")

    print(f"Registering Terrain_LULC_Vector_Slope")
    print(f"Asset ID: {asset_id}")
    print(f"Produced by: {producing_algo} v{algo_version}")
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')
    start_year = params.get('start_year')
    end_year = params.get('end_year')
        
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
    
        instance = db.log_dataset_instance(
            dataset_type_id="Terrain_LULC_Vector_Slope",
            asset_id=asset_id,
            produced_by_algo=producing_algo,
            algo_version=algo_version,
            run_id=run_id,
            meta_info={"registered_at": str(datetime.now()),
                'state': state,         
                'district': district,   
                'block': block,         
                'start_year': str(start_year), 
                'end_year': str(end_year)   }
        )



        version = instance.version
        print(f"Terrain_LULC_Vector_Slope registered in database")
        print(f"Version: {version}")

        # Augment stac_spec with STAC-D provenance fields
        if stac_spec:
            stac_spec["properties"]["stacd:dag_id"] = DAG_ID
            stac_spec["properties"]["stacd:run_id"] = run_id
            stac_spec["properties"]["stacd:producing_algo"] = producing_algo
            stac_spec["properties"]["stacd:algo_version"] = algo_version
            stac_spec["properties"]["stacd:dataset_type"] = "Terrain_LULC_Vector_Slope"
            stac_spec["properties"]["stacd:instance_id"] = instance.instance_id
            stac_spec["properties"]["stacd:dataset_version"] = version
            stac_spec["id"] = f"{state}_{district}_{block}_Terrain_LULC_Vector_Slope_v{version}"

            # Write augmented STAC item to catalog
            print("Writing augmented STAC item...")
            augment_and_write_stac(stac_spec, "Terrain_LULC_Vector_Slope", instance.instance_id, params)
            print(f"STAC item written for Terrain_LULC_Vector_Slope")
        else:
            # Fallback: generate from DB record (original path)
            print("No stac_spec from algo, falling back to DB-based generation...")
            generate_stac_for_dataset(instance, params)
            print(f"STAC item generated for Terrain_LULC_Vector_Slope")
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Terrain_LULC_Vector_Plain(**context):
    """Register Terrain_LULC_Vector_Plain dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Terrain_LULC_Vector_Plain")
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
        raise ValueError(f"No asset_id found from upstream for Terrain_LULC_Vector_Plain")

    print(f"Registering Terrain_LULC_Vector_Plain")
    print(f"Asset ID: {asset_id}")
    print(f"Produced by: {producing_algo} v{algo_version}")
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')
    start_year = params.get('start_year')
    end_year = params.get('end_year')
        
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
    
        instance = db.log_dataset_instance(
            dataset_type_id="Terrain_LULC_Vector_Plain",
            asset_id=asset_id,
            produced_by_algo=producing_algo,
            algo_version=algo_version,
            run_id=run_id,
            meta_info={"registered_at": str(datetime.now()),
                'state': state,         
                'district': district,   
                'block': block,         
                'start_year': str(start_year), 
                'end_year': str(end_year)   }
        )



        version = instance.version
        print(f"Terrain_LULC_Vector_Plain registered in database")
        print(f"Version: {version}")

        # Augment stac_spec with STAC-D provenance fields
        if stac_spec:
            stac_spec["properties"]["stacd:dag_id"] = DAG_ID
            stac_spec["properties"]["stacd:run_id"] = run_id
            stac_spec["properties"]["stacd:producing_algo"] = producing_algo
            stac_spec["properties"]["stacd:algo_version"] = algo_version
            stac_spec["properties"]["stacd:dataset_type"] = "Terrain_LULC_Vector_Plain"
            stac_spec["properties"]["stacd:instance_id"] = instance.instance_id
            stac_spec["properties"]["stacd:dataset_version"] = version
            stac_spec["id"] = f"{state}_{district}_{block}_Terrain_LULC_Vector_Plain_v{version}"

            # Write augmented STAC item to catalog
            print("Writing augmented STAC item...")
            augment_and_write_stac(stac_spec, "Terrain_LULC_Vector_Plain", instance.instance_id, params)
            print(f"STAC item written for Terrain_LULC_Vector_Plain")
        else:
            # Fallback: generate from DB record (original path)
            print("No stac_spec from algo, falling back to DB-based generation...")
            generate_stac_for_dataset(instance, params)
            print(f"STAC item generated for Terrain_LULC_Vector_Plain")
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

# ========== DAG Definition ==========

default_args = {
    'owner': 'stacd',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="8-algo workflow hitting corestack-lite Django backend APIs synchronously. Starts with AdminBoundary + MWSLayer before LULC and Terrain processing.",
    schedule_interval=None,
    catchup=False,
    tags=['stacd', 'recompute', 'generic'],
    params={
        'state': 'default_value',
        'district': 'default_value',
        'block': 'default_value',
        'start_year': 'default_value',
        'end_year': 'default_value',
        'execution_type': 'fullexec',
        'updated_algo': '',
        'updated_dataset': ''
    }
) as dag:
    
    # Branch task to determine execution path
    branch_task = BranchPythonOperator(
        task_id='determine_execution_path',
        python_callable=determine_execution_path,
        provide_context=True
    )
    
    # Create all algorithm tasks

    Admin_Boundary = PythonOperator(
        task_id='Admin_Boundary',
        python_callable=execute_Admin_Boundary,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    MWS_Layer = PythonOperator(
        task_id='MWS_Layer',
        python_callable=execute_MWS_Layer,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    LULC_Algorithm = PythonOperator(
        task_id='LULC_Algorithm',
        python_callable=execute_LULC_Algorithm,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    LULC_Vectorization = PythonOperator(
        task_id='LULC_Vectorization',
        python_callable=execute_LULC_Vectorization,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Terrain_Algorithm = PythonOperator(
        task_id='Terrain_Algorithm',
        python_callable=execute_Terrain_Algorithm,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Terrain_Vectorization = PythonOperator(
        task_id='Terrain_Vectorization',
        python_callable=execute_Terrain_Vectorization,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Terrain_LULC_Slope = PythonOperator(
        task_id='Terrain_LULC_Slope',
        python_callable=execute_Terrain_LULC_Slope,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Terrain_LULC_Plain = PythonOperator(
        task_id='Terrain_LULC_Plain',
        python_callable=execute_Terrain_LULC_Plain,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Pan_India_MWS = PythonOperator(
        task_id='Pan_India_MWS',
        python_callable=fetch_Pan_India_MWS,
        provide_context=True
    )

    Admin_Boundary_Asset = PythonOperator(
        task_id='Admin_Boundary_Asset',
        python_callable=register_Admin_Boundary_Asset,
        provide_context=True
    )

    MWS_Boundaries = PythonOperator(
        task_id='MWS_Boundaries',
        python_callable=register_MWS_Boundaries,
        provide_context=True
    )

    LULC_Raster = PythonOperator(
        task_id='LULC_Raster',
        python_callable=register_LULC_Raster,
        provide_context=True
    )

    LULC_Vector = PythonOperator(
        task_id='LULC_Vector',
        python_callable=register_LULC_Vector,
        provide_context=True
    )

    Terrain_Raster = PythonOperator(
        task_id='Terrain_Raster',
        python_callable=register_Terrain_Raster,
        provide_context=True
    )

    Terrain_Vector = PythonOperator(
        task_id='Terrain_Vector',
        python_callable=register_Terrain_Vector,
        provide_context=True
    )

    Terrain_LULC_Vector_Slope = PythonOperator(
        task_id='Terrain_LULC_Vector_Slope',
        python_callable=register_Terrain_LULC_Vector_Slope,
        provide_context=True
    )

    Terrain_LULC_Vector_Plain = PythonOperator(
        task_id='Terrain_LULC_Vector_Plain',
        python_callable=register_Terrain_LULC_Vector_Plain,
        provide_context=True
    )

    # ============================================================================
    # DEPENDENCIES (Generated from YAML input_datasets and outputs)
    # ============================================================================

    # Branch connects to all algorithms (selective execution)
    branch_task >> Admin_Boundary
    branch_task >> MWS_Layer
    branch_task >> LULC_Algorithm
    branch_task >> LULC_Vectorization
    branch_task >> Terrain_Algorithm
    branch_task >> Terrain_Vectorization
    branch_task >> Terrain_LULC_Slope
    branch_task >> Terrain_LULC_Plain

 # Branch connects to all root datasets (for update_dataset support)
    branch_task >> Pan_India_MWS

    # Algorithm -> Dataset dependencies (outputs)
    Admin_Boundary >> Admin_Boundary_Asset
    MWS_Layer >> MWS_Boundaries
    LULC_Algorithm >> LULC_Raster
    LULC_Vectorization >> LULC_Vector
    Terrain_Algorithm >> Terrain_Raster
    Terrain_Vectorization >> Terrain_Vector
    Terrain_LULC_Slope >> Terrain_LULC_Vector_Slope
    Terrain_LULC_Plain >> Terrain_LULC_Vector_Plain

    # Dataset -> Algorithm dependencies (inputs)
    Pan_India_MWS >> Admin_Boundary
    Pan_India_MWS >> MWS_Layer
    Admin_Boundary_Asset >> MWS_Layer
    MWS_Boundaries >> LULC_Algorithm
    LULC_Raster >> LULC_Vectorization
    MWS_Boundaries >> Terrain_Algorithm
    Terrain_Raster >> Terrain_Vectorization
    LULC_Raster >> Terrain_LULC_Slope
    Terrain_Raster >> Terrain_LULC_Slope
    LULC_Raster >> Terrain_LULC_Plain
    Terrain_Raster >> Terrain_LULC_Plain
