"""
STACD DAG - New Backend API Workflow Test
Generated: 2025-11-15T19:10:28.157776
DAG UUID: e0476807-d8fe-4b7d-8fc7-c5a73ccc9569
Mode: MIXED (API + Docker) with Database Logging
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests
import subprocess
import json
import uuid
import sys
import os

# Add database path
sys.path.insert(0, os.path.expanduser('~/stacd_database'))
from db_operations import STACDDatabase

# DAG Configuration
DAG_UUID = 'e0476807-d8fe-4b7d-8fc7-c5a73ccc9569'
DB_PATH = os.path.expanduser('~/stacd_database/stacd.db')

default_args = {
    'owner': 'stacd',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'new_backend_api_test_mixed',
    default_args=default_args,
    description='Testing new stacd_backend with API calls (Mixed Mode)',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['stacd', 'mixed-mode', 'yaml-driven'],
    params={
        'state': 'jharkhand',
        'district': 'dumka',
        'block': 'masalia',
        'start_year': 2017,
        'end_year': 2018
    }
)


# ============================================================================
# DATABASE INITIALIZATION
# ============================================================================

def register_dag_if_needed():
    """Register DAG in database if not exists"""
    db = STACDDatabase(DB_PATH)
    existing = db.get_dag(DAG_UUID)
    if not existing:
        db.register_dag(
            dag_uuid=DAG_UUID,
            dag_id='new_backend_api_test_mixed',
            name='New Backend API Workflow Test (Mixed)',
            version='1.0',
            description='Testing new stacd_backend with API calls',
            structure={'nodes': ['LULC_Algorithm', 'LULC_Vectorization', 'Terrain_Algorithm', 'Terrain_Vectorization', 'Terrain_LULC_Slope', 'Terrain_LULC_Plain', 'LULC_Raster', 'LULC_Vector', 'Terrain_Raster', 'Terrain_Vector', 'Terrain_LULC_Vector_Slope', 'Terrain_LULC_Vector_Plain']},
            parameters={'state': 'string', 'district': 'string', 'block': 'string', 
                        'start_year': 'int', 'end_year': 'int'}
        )
        print(f"✓ Registered DAG: {DAG_UUID}")
    db.close()

register_dag_if_needed()


# ============================================================================
# EXECUTION TASKS (Algorithm Nodes - Mixed API/Docker)
# ============================================================================

# LULC_Algorithm (API Mode)
def call_execute_LULC_Algorithm(**context):
    """Execute LULC_Algorithm via API with UUID and DB logging"""
    execution_id = str(uuid.uuid4())
    dag_run_id = context['dag_run'].run_id
    
    payload = {
        "state": context["params"]["state"],
        "district": context["params"]["district"],
        "block": context["params"]["block"],
        "start_year": context["params"]["start_year"],
        "end_year": context["params"]["end_year"],
        "execution_id": execution_id
    }
    
    db = STACDDatabase(DB_PATH)
    algo_type = db.get_active_algorithm_type('LULC_Algorithm')
    if not algo_type:
        db.close()
        raise Exception(f"Algorithm type 'LULC_Algorithm' not found in database")
    
    db.log_algorithm_execution(
        execution_id=execution_id,
        algo_type_id=algo_type.algo_type_id,
        dag_uuid=DAG_UUID,
        dag_run_id=dag_run_id,
        node_name='execute_LULC_Algorithm',
        status='running',
        parameters=payload
    )
    
    print(f"\n============================================================")
    print(f"[ALGORITHM - API] LULC_Algorithm")
    print(f"Execution ID: {execution_id}")
    print(f"DAG UUID: {DAG_UUID}")
    print(f"============================================================")
    
    try:
        response = requests.post("http://localhost:8000/api/v1/lulc_v3/", json=payload, headers={"Content-Type": "application/json"})
        response.raise_for_status()
        result = response.json()
        
        if result.get('status') != 'success':
            raise Exception(f"Failed: {result}")
        
        db.update_algorithm_status(
            execution_id=execution_id,
            status='success',
            execution_time=result.get('execution_time', 0)
        )
        
        print(f"Status: {result.get('status')}")
        print(f"Asset IDs: {result.get('asset_ids', [])}")
        print(f"Time: {result.get('execution_time', 0):.1f}s")
        print(f"✓ Logged to database\n")
        
        context['ti'].xcom_push(key='execution_id', value=execution_id)
        context['ti'].xcom_push(key='asset_ids', value=result.get('asset_ids', []))
        context['ti'].xcom_push(key='execution_time', value=result.get('execution_time', 0))
        
        db.close()
        return result
        
    except Exception as e:
        db.update_algorithm_status(
            execution_id=execution_id,
            status='failed',
            error_message=str(e)
        )
        db.close()
        print(f"❌ Error: {str(e)}")
        raise

execute_LULC_Algorithm = PythonOperator(
    task_id='execute_LULC_Algorithm',
    python_callable=call_execute_LULC_Algorithm,
    provide_context=True,
    dag=dag
)

# LULC_Vectorization (API Mode)
def call_execute_LULC_Vectorization(**context):
    """Execute LULC_Vectorization via API with UUID and DB logging"""
    execution_id = str(uuid.uuid4())
    dag_run_id = context['dag_run'].run_id
    
    payload = {
        "state": context["params"]["state"],
        "district": context["params"]["district"],
        "block": context["params"]["block"],
        "start_year": context["params"]["start_year"],
        "end_year": context["params"]["end_year"],
        "execution_id": execution_id
    }
    
    db = STACDDatabase(DB_PATH)
    algo_type = db.get_active_algorithm_type('LULC_Vectorization')
    if not algo_type:
        db.close()
        raise Exception(f"Algorithm type 'LULC_Vectorization' not found in database")
    
    db.log_algorithm_execution(
        execution_id=execution_id,
        algo_type_id=algo_type.algo_type_id,
        dag_uuid=DAG_UUID,
        dag_run_id=dag_run_id,
        node_name='execute_LULC_Vectorization',
        status='running',
        parameters=payload
    )
    
    print(f"\n============================================================")
    print(f"[ALGORITHM - API] LULC_Vectorization")
    print(f"Execution ID: {execution_id}")
    print(f"DAG UUID: {DAG_UUID}")
    print(f"============================================================")
    
    try:
        response = requests.post("http://localhost:8000/api/v1/lulc_vector/", json=payload, headers={"Content-Type": "application/json"})
        response.raise_for_status()
        result = response.json()
        
        if result.get('status') != 'success':
            raise Exception(f"Failed: {result}")
        
        db.update_algorithm_status(
            execution_id=execution_id,
            status='success',
            execution_time=result.get('execution_time', 0)
        )
        
        print(f"Status: {result.get('status')}")
        print(f"Asset IDs: {result.get('asset_ids', [])}")
        print(f"Time: {result.get('execution_time', 0):.1f}s")
        print(f"✓ Logged to database\n")
        
        context['ti'].xcom_push(key='execution_id', value=execution_id)
        context['ti'].xcom_push(key='asset_ids', value=result.get('asset_ids', []))
        context['ti'].xcom_push(key='execution_time', value=result.get('execution_time', 0))
        
        db.close()
        return result
        
    except Exception as e:
        db.update_algorithm_status(
            execution_id=execution_id,
            status='failed',
            error_message=str(e)
        )
        db.close()
        print(f"❌ Error: {str(e)}")
        raise

execute_LULC_Vectorization = PythonOperator(
    task_id='execute_LULC_Vectorization',
    python_callable=call_execute_LULC_Vectorization,
    provide_context=True,
    dag=dag
)

# Terrain_Algorithm (Docker Mode)
def call_execute_Terrain_Algorithm(**context):
    """Execute Terrain_Algorithm via Docker with UUID and DB logging"""
    execution_id = str(uuid.uuid4())
    dag_run_id = context['dag_run'].run_id
    
    # Build parameters
    params = context['params']
    param_dict = {
        'state': params.get('state'),
        'district': params.get('district'),
        'block': params.get('block'),
        'execution_id': execution_id
    }
    
    # Get algorithm type from database
    db = STACDDatabase(DB_PATH)
    algo_type = db.get_active_algorithm_type('Terrain_Algorithm')
    if not algo_type:
        db.close()
        raise Exception(f"Algorithm type 'Terrain_Algorithm' not found in database")
    
    # Log algorithm start
    db.log_algorithm_execution(
        execution_id=execution_id,
        algo_type_id=algo_type.algo_type_id,
        dag_uuid=DAG_UUID,
        dag_run_id=dag_run_id,
        node_name='execute_Terrain_Algorithm',
        status='running',
        parameters=param_dict
    )
    
    print(f"\n============================================================")
    print(f"[ALGORITHM - DOCKER] Terrain_Algorithm")
    print(f"Execution ID: {execution_id}")
    print(f"DAG UUID: {DAG_UUID}")
    print(f"============================================================")
    
    try:
        # Build Docker command with volume mounts from YAML
        cmd = ['docker', 'run', '--rm']

        cmd.extend(['-v', '/home/sash_unix/.config/earthengine:/root/.config/earthengine:ro'])
        cmd.extend(['saharshlaud/stacd-backend:latest', 'python', 'cli.py', 'Terrain_Algorithm'])
        cmd.extend(['--state', str(param_dict['state'])])
        cmd.extend(['--district', str(param_dict['district'])])
        cmd.extend(['--block', str(param_dict['block'])])
        cmd.extend(['--execution_id', execution_id])
        
        # Execute Docker container
        print(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        # Parse JSON output (last line)
        output_lines = result.stdout.strip().split('\n')
        json_output = output_lines[-1] if output_lines else '{}'
        print(f"Container output: {json_output}")
        result_data = json.loads(json_output)
        
        if result_data.get('status') != 'success':
            raise Exception(f"Algorithm failed: {result_data}")
        
        # Update database with success
        db.update_algorithm_status(
            execution_id=execution_id,
            status='success',
            execution_time=result_data.get('execution_time', 0)
        )
        
        print(f"Status: {result_data.get('status')}")
        print(f"Asset IDs: {result_data.get('asset_ids', [])}")
        print(f"Time: {result_data.get('execution_time', 0):.1f}s")
        print(f"✓ Logged to database\n")
        
        # Push to XCom for dataset nodes
        context['ti'].xcom_push(key='execution_id', value=execution_id)
        context['ti'].xcom_push(key='asset_ids', value=result_data.get('asset_ids', []))
        context['ti'].xcom_push(key='execution_time', value=result_data.get('execution_time', 0))
        
        db.close()
        return result_data
        
    except subprocess.CalledProcessError as e:
        error_msg = f"Docker execution failed: {e.stderr}"
        db.update_algorithm_status(
            execution_id=execution_id,
            status='failed',
            error_message=error_msg
        )
        db.close()
        print(f"❌ Error: {error_msg}")
        raise
    except json.JSONDecodeError as e:
        error_msg = f"Failed to parse Docker output: {str(e)}"
        db.update_algorithm_status(
            execution_id=execution_id,
            status='failed',
            error_message=error_msg
        )
        db.close()
        print(f"❌ Error: {error_msg}")
        raise
    except Exception as e:
        db.update_algorithm_status(
            execution_id=execution_id,
            status='failed',
            error_message=str(e)
        )
        db.close()
        print(f"❌ Error: {str(e)}")
        raise

execute_Terrain_Algorithm = PythonOperator(
    task_id='execute_Terrain_Algorithm',
    python_callable=call_execute_Terrain_Algorithm,
    provide_context=True,
    dag=dag
)

# Terrain_Vectorization (Docker Mode)
def call_execute_Terrain_Vectorization(**context):
    """Execute Terrain_Vectorization via Docker with UUID and DB logging"""
    execution_id = str(uuid.uuid4())
    dag_run_id = context['dag_run'].run_id
    
    # Build parameters
    params = context['params']
    param_dict = {
        'state': params.get('state'),
        'district': params.get('district'),
        'block': params.get('block'),
        'execution_id': execution_id
    }
    
    # Get algorithm type from database
    db = STACDDatabase(DB_PATH)
    algo_type = db.get_active_algorithm_type('Terrain_Vectorization')
    if not algo_type:
        db.close()
        raise Exception(f"Algorithm type 'Terrain_Vectorization' not found in database")
    
    # Log algorithm start
    db.log_algorithm_execution(
        execution_id=execution_id,
        algo_type_id=algo_type.algo_type_id,
        dag_uuid=DAG_UUID,
        dag_run_id=dag_run_id,
        node_name='execute_Terrain_Vectorization',
        status='running',
        parameters=param_dict
    )
    
    print(f"\n============================================================")
    print(f"[ALGORITHM - DOCKER] Terrain_Vectorization")
    print(f"Execution ID: {execution_id}")
    print(f"DAG UUID: {DAG_UUID}")
    print(f"============================================================")
    
    try:
        # Build Docker command with volume mounts from YAML
        cmd = ['docker', 'run', '--rm']

        cmd.extend(['-v', '/home/sash_unix/.config/earthengine:/root/.config/earthengine:ro'])
        cmd.extend(['saharshlaud/stacd-backend:latest', 'python', 'cli.py', 'Terrain_Vectorization'])
        cmd.extend(['--state', str(param_dict['state'])])
        cmd.extend(['--district', str(param_dict['district'])])
        cmd.extend(['--block', str(param_dict['block'])])
        cmd.extend(['--execution_id', execution_id])
        
        # Execute Docker container
        print(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        # Parse JSON output (last line)
        output_lines = result.stdout.strip().split('\n')
        json_output = output_lines[-1] if output_lines else '{}'
        print(f"Container output: {json_output}")
        result_data = json.loads(json_output)
        
        if result_data.get('status') != 'success':
            raise Exception(f"Algorithm failed: {result_data}")
        
        # Update database with success
        db.update_algorithm_status(
            execution_id=execution_id,
            status='success',
            execution_time=result_data.get('execution_time', 0)
        )
        
        print(f"Status: {result_data.get('status')}")
        print(f"Asset IDs: {result_data.get('asset_ids', [])}")
        print(f"Time: {result_data.get('execution_time', 0):.1f}s")
        print(f"✓ Logged to database\n")
        
        # Push to XCom for dataset nodes
        context['ti'].xcom_push(key='execution_id', value=execution_id)
        context['ti'].xcom_push(key='asset_ids', value=result_data.get('asset_ids', []))
        context['ti'].xcom_push(key='execution_time', value=result_data.get('execution_time', 0))
        
        db.close()
        return result_data
        
    except subprocess.CalledProcessError as e:
        error_msg = f"Docker execution failed: {e.stderr}"
        db.update_algorithm_status(
            execution_id=execution_id,
            status='failed',
            error_message=error_msg
        )
        db.close()
        print(f"❌ Error: {error_msg}")
        raise
    except json.JSONDecodeError as e:
        error_msg = f"Failed to parse Docker output: {str(e)}"
        db.update_algorithm_status(
            execution_id=execution_id,
            status='failed',
            error_message=error_msg
        )
        db.close()
        print(f"❌ Error: {error_msg}")
        raise
    except Exception as e:
        db.update_algorithm_status(
            execution_id=execution_id,
            status='failed',
            error_message=str(e)
        )
        db.close()
        print(f"❌ Error: {str(e)}")
        raise

execute_Terrain_Vectorization = PythonOperator(
    task_id='execute_Terrain_Vectorization',
    python_callable=call_execute_Terrain_Vectorization,
    provide_context=True,
    dag=dag
)

# Terrain_LULC_Slope (API Mode)
def call_execute_Terrain_LULC_Slope(**context):
    """Execute Terrain_LULC_Slope via API with UUID and DB logging"""
    execution_id = str(uuid.uuid4())
    dag_run_id = context['dag_run'].run_id
    
    payload = {
        "state": context["params"]["state"],
        "district": context["params"]["district"],
        "block": context["params"]["block"],
        "start_year": context["params"]["start_year"],
        "end_year": context["params"]["end_year"],
        "execution_id": execution_id
    }
    
    db = STACDDatabase(DB_PATH)
    algo_type = db.get_active_algorithm_type('Terrain_LULC_Slope')
    if not algo_type:
        db.close()
        raise Exception(f"Algorithm type 'Terrain_LULC_Slope' not found in database")
    
    db.log_algorithm_execution(
        execution_id=execution_id,
        algo_type_id=algo_type.algo_type_id,
        dag_uuid=DAG_UUID,
        dag_run_id=dag_run_id,
        node_name='execute_Terrain_LULC_Slope',
        status='running',
        parameters=payload
    )
    
    print(f"\n============================================================")
    print(f"[ALGORITHM - API] Terrain_LULC_Slope")
    print(f"Execution ID: {execution_id}")
    print(f"DAG UUID: {DAG_UUID}")
    print(f"============================================================")
    
    try:
        response = requests.post("http://localhost:8000/api/v1/lulc_on_slope_cluster/", json=payload, headers={"Content-Type": "application/json"})
        response.raise_for_status()
        result = response.json()
        
        if result.get('status') != 'success':
            raise Exception(f"Failed: {result}")
        
        db.update_algorithm_status(
            execution_id=execution_id,
            status='success',
            execution_time=result.get('execution_time', 0)
        )
        
        print(f"Status: {result.get('status')}")
        print(f"Asset IDs: {result.get('asset_ids', [])}")
        print(f"Time: {result.get('execution_time', 0):.1f}s")
        print(f"✓ Logged to database\n")
        
        context['ti'].xcom_push(key='execution_id', value=execution_id)
        context['ti'].xcom_push(key='asset_ids', value=result.get('asset_ids', []))
        context['ti'].xcom_push(key='execution_time', value=result.get('execution_time', 0))
        
        db.close()
        return result
        
    except Exception as e:
        db.update_algorithm_status(
            execution_id=execution_id,
            status='failed',
            error_message=str(e)
        )
        db.close()
        print(f"❌ Error: {str(e)}")
        raise

execute_Terrain_LULC_Slope = PythonOperator(
    task_id='execute_Terrain_LULC_Slope',
    python_callable=call_execute_Terrain_LULC_Slope,
    provide_context=True,
    dag=dag
)

# Terrain_LULC_Plain (API Mode)
def call_execute_Terrain_LULC_Plain(**context):
    """Execute Terrain_LULC_Plain via API with UUID and DB logging"""
    execution_id = str(uuid.uuid4())
    dag_run_id = context['dag_run'].run_id
    
    payload = {
        "state": context["params"]["state"],
        "district": context["params"]["district"],
        "block": context["params"]["block"],
        "start_year": context["params"]["start_year"],
        "end_year": context["params"]["end_year"],
        "execution_id": execution_id
    }
    
    db = STACDDatabase(DB_PATH)
    algo_type = db.get_active_algorithm_type('Terrain_LULC_Plain')
    if not algo_type:
        db.close()
        raise Exception(f"Algorithm type 'Terrain_LULC_Plain' not found in database")
    
    db.log_algorithm_execution(
        execution_id=execution_id,
        algo_type_id=algo_type.algo_type_id,
        dag_uuid=DAG_UUID,
        dag_run_id=dag_run_id,
        node_name='execute_Terrain_LULC_Plain',
        status='running',
        parameters=payload
    )
    
    print(f"\n============================================================")
    print(f"[ALGORITHM - API] Terrain_LULC_Plain")
    print(f"Execution ID: {execution_id}")
    print(f"DAG UUID: {DAG_UUID}")
    print(f"============================================================")
    
    try:
        response = requests.post("http://localhost:8000/api/v1/lulc_on_plain_cluster/", json=payload, headers={"Content-Type": "application/json"})
        response.raise_for_status()
        result = response.json()
        
        if result.get('status') != 'success':
            raise Exception(f"Failed: {result}")
        
        db.update_algorithm_status(
            execution_id=execution_id,
            status='success',
            execution_time=result.get('execution_time', 0)
        )
        
        print(f"Status: {result.get('status')}")
        print(f"Asset IDs: {result.get('asset_ids', [])}")
        print(f"Time: {result.get('execution_time', 0):.1f}s")
        print(f"✓ Logged to database\n")
        
        context['ti'].xcom_push(key='execution_id', value=execution_id)
        context['ti'].xcom_push(key='asset_ids', value=result.get('asset_ids', []))
        context['ti'].xcom_push(key='execution_time', value=result.get('execution_time', 0))
        
        db.close()
        return result
        
    except Exception as e:
        db.update_algorithm_status(
            execution_id=execution_id,
            status='failed',
            error_message=str(e)
        )
        db.close()
        print(f"❌ Error: {str(e)}")
        raise

execute_Terrain_LULC_Plain = PythonOperator(
    task_id='execute_Terrain_LULC_Plain',
    python_callable=call_execute_Terrain_LULC_Plain,
    provide_context=True,
    dag=dag
)

# ============================================================================
# VERIFICATION TASKS (Dataset Nodes)
# ============================================================================

# Verify LULC_Raster
def verify_LULC_Raster_func(**context):
    """Process LULC_Raster dataset with UUID and DB logging"""
    ti = context['ti']
    dag_run_id = context['dag_run'].run_id
    dataset_uuid = str(uuid.uuid4())
    
    db = STACDDatabase(DB_PATH)
    dataset_type = db.get_active_dataset_type('LULC_Raster')
    if not dataset_type:
        db.close()
        raise Exception(f"Dataset type 'LULC_Raster' not found in database")
    
    # Pull from upstream algorithm
    execution_id = ti.xcom_pull(key='execution_id', task_ids='execute_LULC_Algorithm') if 'execute_LULC_Algorithm' else 'unknown'
    asset_ids = ti.xcom_pull(key='asset_ids', task_ids='execute_LULC_Algorithm') if 'execute_LULC_Algorithm' else []
    
    db.log_dataset_instance(
        dataset_uuid=dataset_uuid,
        dataset_type_id=dataset_type.dataset_type_id,
        execution_id=execution_id if execution_id else 'placeholder',
        dag_uuid=DAG_UUID,
        node_name='verify_LULC_Raster',
        asset_ids=asset_ids if asset_ids else [],
        region_params=context['params']
    )
    
    print(f"\n============================================================")
    print(f"[DATASET] LULC_Raster")
    print(f"Dataset UUID: {dataset_uuid}")
    print(f"Execution ID: {execution_id}")
    print(f"Asset IDs: {asset_ids}")
    print(f"✓ Logged to database")
    print(f"============================================================\n")
    
    db.close()
    return True

verify_LULC_Raster = PythonOperator(
    task_id='verify_LULC_Raster',
    python_callable=verify_LULC_Raster_func,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    provide_context=True,
    dag=dag
)

# Verify LULC_Vector
def verify_LULC_Vector_func(**context):
    """Process LULC_Vector dataset with UUID and DB logging"""
    ti = context['ti']
    dag_run_id = context['dag_run'].run_id
    dataset_uuid = str(uuid.uuid4())
    
    db = STACDDatabase(DB_PATH)
    dataset_type = db.get_active_dataset_type('LULC_Vector')
    if not dataset_type:
        db.close()
        raise Exception(f"Dataset type 'LULC_Vector' not found in database")
    
    # Pull from upstream algorithm
    execution_id = ti.xcom_pull(key='execution_id', task_ids='execute_LULC_Vectorization') if 'execute_LULC_Vectorization' else 'unknown'
    asset_ids = ti.xcom_pull(key='asset_ids', task_ids='execute_LULC_Vectorization') if 'execute_LULC_Vectorization' else []
    
    db.log_dataset_instance(
        dataset_uuid=dataset_uuid,
        dataset_type_id=dataset_type.dataset_type_id,
        execution_id=execution_id if execution_id else 'placeholder',
        dag_uuid=DAG_UUID,
        node_name='verify_LULC_Vector',
        asset_ids=asset_ids if asset_ids else [],
        region_params=context['params']
    )
    
    print(f"\n============================================================")
    print(f"[DATASET] LULC_Vector")
    print(f"Dataset UUID: {dataset_uuid}")
    print(f"Execution ID: {execution_id}")
    print(f"Asset IDs: {asset_ids}")
    print(f"✓ Logged to database")
    print(f"============================================================\n")
    
    db.close()
    return True

verify_LULC_Vector = PythonOperator(
    task_id='verify_LULC_Vector',
    python_callable=verify_LULC_Vector_func,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    provide_context=True,
    dag=dag
)

# Verify Terrain_Raster
def verify_Terrain_Raster_func(**context):
    """Process Terrain_Raster dataset with UUID and DB logging"""
    ti = context['ti']
    dag_run_id = context['dag_run'].run_id
    dataset_uuid = str(uuid.uuid4())
    
    db = STACDDatabase(DB_PATH)
    dataset_type = db.get_active_dataset_type('Terrain_Raster')
    if not dataset_type:
        db.close()
        raise Exception(f"Dataset type 'Terrain_Raster' not found in database")
    
    # Pull from upstream algorithm
    execution_id = ti.xcom_pull(key='execution_id', task_ids='execute_Terrain_Algorithm') if 'execute_Terrain_Algorithm' else 'unknown'
    asset_ids = ti.xcom_pull(key='asset_ids', task_ids='execute_Terrain_Algorithm') if 'execute_Terrain_Algorithm' else []
    
    db.log_dataset_instance(
        dataset_uuid=dataset_uuid,
        dataset_type_id=dataset_type.dataset_type_id,
        execution_id=execution_id if execution_id else 'placeholder',
        dag_uuid=DAG_UUID,
        node_name='verify_Terrain_Raster',
        asset_ids=asset_ids if asset_ids else [],
        region_params=context['params']
    )
    
    print(f"\n============================================================")
    print(f"[DATASET] Terrain_Raster")
    print(f"Dataset UUID: {dataset_uuid}")
    print(f"Execution ID: {execution_id}")
    print(f"Asset IDs: {asset_ids}")
    print(f"✓ Logged to database")
    print(f"============================================================\n")
    
    db.close()
    return True

verify_Terrain_Raster = PythonOperator(
    task_id='verify_Terrain_Raster',
    python_callable=verify_Terrain_Raster_func,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    provide_context=True,
    dag=dag
)

# Verify Terrain_Vector
def verify_Terrain_Vector_func(**context):
    """Process Terrain_Vector dataset with UUID and DB logging"""
    ti = context['ti']
    dag_run_id = context['dag_run'].run_id
    dataset_uuid = str(uuid.uuid4())
    
    db = STACDDatabase(DB_PATH)
    dataset_type = db.get_active_dataset_type('Terrain_Vector')
    if not dataset_type:
        db.close()
        raise Exception(f"Dataset type 'Terrain_Vector' not found in database")
    
    # Pull from upstream algorithm
    execution_id = ti.xcom_pull(key='execution_id', task_ids='execute_Terrain_Vectorization') if 'execute_Terrain_Vectorization' else 'unknown'
    asset_ids = ti.xcom_pull(key='asset_ids', task_ids='execute_Terrain_Vectorization') if 'execute_Terrain_Vectorization' else []
    
    db.log_dataset_instance(
        dataset_uuid=dataset_uuid,
        dataset_type_id=dataset_type.dataset_type_id,
        execution_id=execution_id if execution_id else 'placeholder',
        dag_uuid=DAG_UUID,
        node_name='verify_Terrain_Vector',
        asset_ids=asset_ids if asset_ids else [],
        region_params=context['params']
    )
    
    print(f"\n============================================================")
    print(f"[DATASET] Terrain_Vector")
    print(f"Dataset UUID: {dataset_uuid}")
    print(f"Execution ID: {execution_id}")
    print(f"Asset IDs: {asset_ids}")
    print(f"✓ Logged to database")
    print(f"============================================================\n")
    
    db.close()
    return True

verify_Terrain_Vector = PythonOperator(
    task_id='verify_Terrain_Vector',
    python_callable=verify_Terrain_Vector_func,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    provide_context=True,
    dag=dag
)

# Verify Terrain_LULC_Vector_Slope
def verify_Terrain_LULC_Vector_Slope_func(**context):
    """Process Terrain_LULC_Vector_Slope dataset with UUID and DB logging"""
    ti = context['ti']
    dag_run_id = context['dag_run'].run_id
    dataset_uuid = str(uuid.uuid4())
    
    db = STACDDatabase(DB_PATH)
    dataset_type = db.get_active_dataset_type('Terrain_LULC_Vector_Slope')
    if not dataset_type:
        db.close()
        raise Exception(f"Dataset type 'Terrain_LULC_Vector_Slope' not found in database")
    
    # Pull from upstream algorithm
    execution_id = ti.xcom_pull(key='execution_id', task_ids='execute_Terrain_LULC_Slope') if 'execute_Terrain_LULC_Slope' else 'unknown'
    asset_ids = ti.xcom_pull(key='asset_ids', task_ids='execute_Terrain_LULC_Slope') if 'execute_Terrain_LULC_Slope' else []
    
    db.log_dataset_instance(
        dataset_uuid=dataset_uuid,
        dataset_type_id=dataset_type.dataset_type_id,
        execution_id=execution_id if execution_id else 'placeholder',
        dag_uuid=DAG_UUID,
        node_name='verify_Terrain_LULC_Vector_Slope',
        asset_ids=asset_ids if asset_ids else [],
        region_params=context['params']
    )
    
    print(f"\n============================================================")
    print(f"[DATASET] Terrain_LULC_Vector_Slope")
    print(f"Dataset UUID: {dataset_uuid}")
    print(f"Execution ID: {execution_id}")
    print(f"Asset IDs: {asset_ids}")
    print(f"✓ Logged to database")
    print(f"============================================================\n")
    
    db.close()
    return True

verify_Terrain_LULC_Vector_Slope = PythonOperator(
    task_id='verify_Terrain_LULC_Vector_Slope',
    python_callable=verify_Terrain_LULC_Vector_Slope_func,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    provide_context=True,
    dag=dag
)

# Verify Terrain_LULC_Vector_Plain
def verify_Terrain_LULC_Vector_Plain_func(**context):
    """Process Terrain_LULC_Vector_Plain dataset with UUID and DB logging"""
    ti = context['ti']
    dag_run_id = context['dag_run'].run_id
    dataset_uuid = str(uuid.uuid4())
    
    db = STACDDatabase(DB_PATH)
    dataset_type = db.get_active_dataset_type('Terrain_LULC_Vector_Plain')
    if not dataset_type:
        db.close()
        raise Exception(f"Dataset type 'Terrain_LULC_Vector_Plain' not found in database")
    
    # Pull from upstream algorithm
    execution_id = ti.xcom_pull(key='execution_id', task_ids='execute_Terrain_LULC_Plain') if 'execute_Terrain_LULC_Plain' else 'unknown'
    asset_ids = ti.xcom_pull(key='asset_ids', task_ids='execute_Terrain_LULC_Plain') if 'execute_Terrain_LULC_Plain' else []
    
    db.log_dataset_instance(
        dataset_uuid=dataset_uuid,
        dataset_type_id=dataset_type.dataset_type_id,
        execution_id=execution_id if execution_id else 'placeholder',
        dag_uuid=DAG_UUID,
        node_name='verify_Terrain_LULC_Vector_Plain',
        asset_ids=asset_ids if asset_ids else [],
        region_params=context['params']
    )
    
    print(f"\n============================================================")
    print(f"[DATASET] Terrain_LULC_Vector_Plain")
    print(f"Dataset UUID: {dataset_uuid}")
    print(f"Execution ID: {execution_id}")
    print(f"Asset IDs: {asset_ids}")
    print(f"✓ Logged to database")
    print(f"============================================================\n")
    
    db.close()
    return True

verify_Terrain_LULC_Vector_Plain = PythonOperator(
    task_id='verify_Terrain_LULC_Vector_Plain',
    python_callable=verify_Terrain_LULC_Vector_Plain_func,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    provide_context=True,
    dag=dag
)

# ============================================================================
# DEPENDENCIES (Generated from YAML input_datasets and outputs)
# ============================================================================

execute_LULC_Algorithm >> verify_LULC_Raster
execute_Terrain_Algorithm >> verify_Terrain_Raster
execute_LULC_Vectorization >> verify_LULC_Vector
execute_Terrain_Vectorization >> verify_Terrain_Vector
execute_Terrain_LULC_Slope >> verify_Terrain_LULC_Vector_Slope
execute_Terrain_LULC_Plain >> verify_Terrain_LULC_Vector_Plain
verify_LULC_Raster >> execute_LULC_Vectorization
verify_Terrain_Raster >> execute_Terrain_Vectorization
verify_LULC_Raster >> execute_Terrain_LULC_Slope
verify_Terrain_Raster >> execute_Terrain_LULC_Slope
verify_LULC_Raster >> execute_Terrain_LULC_Plain
verify_Terrain_Raster >> execute_Terrain_LULC_Plain
