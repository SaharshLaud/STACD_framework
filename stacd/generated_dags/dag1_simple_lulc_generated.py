"""
Auto-generated STACD DAG: Simple LULC Workflow
Generated at: 2026-02-19T08:33:30.638966
Description: Minimal 2-node workflow: LULC raster generation only

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
sys.path.insert(0, os.path.expanduser('~/airflow/enhancement/database'))
from db_operations import STACDDatabase

# ========== Configuration ==========
DAG_ID = "dag1_simple_lulc"
DB_PATH = "~/airflow/enhancement/database/stacd_recompute.db"

# ========== Dependency Graph (for reference) ==========
dependencies = {
    "LULC_Algorithm": {
        "inputs": [
            "MWS_Boundaries"
        ],
        "outputs": [
            "LULC_Raster"
        ]
    }
}

# ========== Helper Functions ==========

def generate_stac_for_dataset(dataset_instance, params):
    """Generate STAC item for a dataset instance"""
    import sys
    import os
    sys.path.insert(0, os.path.expanduser("~/airflow/enhancement/stac_export"))
    
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
    sys.path.insert(0, os.path.expanduser("~/airflow/enhancement/stac_export"))
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
        root_datasets = ['MWS_Boundaries']
        root_algos = ['LULC_Algorithm']
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
        sys.path.insert(0, os.path.expanduser('~/airflow/enhancement'))
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

        response = requests.post(api_config['url'], json=algo_params, timeout=300)
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


# ========== Dataset Registration Functions ==========

def fetch_MWS_Boundaries(**context):
    """
    Fetch MWS_Boundaries root dataset from database
    Root datasets are pre-existing and not produced by algorithms.
    """
    print("="*60)
    print("Fetching Root Dataset: MWS_Boundaries")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    
    # Extract region parameters
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')

    execution_type = params.get('execution_type', 'fullexec')
    updated_dataset = params.get('updated_dataset', '')
    is_being_updated = (execution_type == 'update_dataset' and updated_dataset == 'MWS_Boundaries')

    print(f"🔍 Looking for MWS_Boundaries in region: {state}/{district}/{block}")
    if is_being_updated:
        print(f"⚡ update_dataset mode — will use latest registered version")

    # Query database for root dataset (always gets latest version)
    db = STACDDatabase(DB_PATH)
    try:
        root_dataset = db.get_root_dataset(
            dataset_type_id='MWS_Boundaries',
            state=state,
            district=district,
            block=block
        )
        
        if not root_dataset:
            raise ValueError(f"Root dataset MWS_Boundaries not found for {state}/{district}/{block}")
        
        asset_id = root_dataset.asset_id
        version = root_dataset.version
        print(f"✓ Found MWS_Boundaries version {version}")
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
    description="Minimal 2-node workflow: LULC raster generation only",
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

    LULC_Algorithm = PythonOperator(
        task_id='LULC_Algorithm',
        python_callable=execute_LULC_Algorithm,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    MWS_Boundaries = PythonOperator(
        task_id='MWS_Boundaries',
        python_callable=fetch_MWS_Boundaries,
        provide_context=True
    )

    LULC_Raster = PythonOperator(
        task_id='LULC_Raster',
        python_callable=register_LULC_Raster,
        provide_context=True
    )

    # ============================================================================
    # DEPENDENCIES (Generated from YAML input_datasets and outputs)
    # ============================================================================

    # Branch connects to all algorithms (selective execution)
    branch_task >> LULC_Algorithm

 # Branch connects to all root datasets (for update_dataset support)
    branch_task >> MWS_Boundaries

    # Algorithm -> Dataset dependencies (outputs)
    LULC_Algorithm >> LULC_Raster

    # Dataset -> Algorithm dependencies (inputs)
    MWS_Boundaries >> LULC_Algorithm
