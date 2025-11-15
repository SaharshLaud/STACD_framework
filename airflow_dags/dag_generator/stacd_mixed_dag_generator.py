"""
STACD MIXED DAG Generator with Database Integration
Supports API and Docker execution modes with YAML-driven configuration
Automatically reads volume mounts and execution modes from YAML
"""

import yaml
import uuid as uuid_lib
from datetime import datetime, timedelta
from stacd_classes import DAG, Algorithm_Type, Algorithm_Instance, Dataset_Type
import os


def load_yaml_file(filepath):
    with open(filepath, 'r') as f:
        content = f.read()
    return list(yaml.load_all(content, Loader=yaml.FullLoader))


def parse_dag_specification(dag_yaml_path, algo_repo_path):
    dag_docs = load_yaml_file(dag_yaml_path)
    dag = None
    dataset_types = {}
    algorithm_types = {}
    
    for doc in dag_docs:
        if isinstance(doc, list):
            for item in doc:
                if isinstance(item, DAG):
                    dag = item
                elif isinstance(item, Dataset_Type):
                    dataset_types[item.id] = item
                elif isinstance(item, Algorithm_Type):
                    algorithm_types[item.id] = item
        elif isinstance(doc, DAG):
            dag = doc
        elif isinstance(doc, Dataset_Type):
            dataset_types[doc.id] = doc
        elif isinstance(doc, Algorithm_Type):
            algorithm_types[doc.id] = doc
    
    algo_docs = load_yaml_file(algo_repo_path)
    algorithm_instances = {}
    
    for doc in algo_docs:
        if isinstance(doc, list):
            for item in doc:
                if isinstance(item, Algorithm_Instance):
                    algorithm_instances[item.type] = item
        elif isinstance(doc, Algorithm_Instance):
            algorithm_instances[doc.type] = doc
    
    return dag, algorithm_types, algorithm_instances, dataset_types


def generate_mixed_dag_with_db(dag, algorithm_types, algorithm_instances, dataset_types, output_file):
    """Generate mixed-mode DAG with both API and Docker execution"""
    
    # Generate DAG UUID
    dag_uuid = str(uuid_lib.uuid4())
    
    dag_code = f'''"""
STACD DAG - {dag.name}
Generated: {datetime.now().isoformat()}
DAG UUID: {dag_uuid}
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
DAG_UUID = '{dag_uuid}'
DB_PATH = os.path.expanduser('~/stacd_database/stacd.db')

default_args = {{
    'owner': 'stacd',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}}

dag = DAG(
    '{dag.id}_mixed',
    default_args=default_args,
    description='{dag.description} (Mixed Mode)',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['stacd', 'mixed-mode', 'yaml-driven'],
    params={{
        'state': 'jharkhand',
        'district': 'dumka',
        'block': 'masalia',
        'start_year': 2017,
        'end_year': 2018
    }}
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
            dag_id='{dag.id}_mixed',
            name='{dag.name} (Mixed)',
            version='{dag.version}',
            description='{dag.description}',
            structure={{'nodes': {list(dag.alg_type_nodes + dag.dataset_type_nodes)}}},
            parameters={{'state': 'string', 'district': 'string', 'block': 'string', 
                        'start_year': 'int', 'end_year': 'int'}}
        )
        print(f"✓ Registered DAG: {{DAG_UUID}}")
    db.close()

register_dag_if_needed()


# ============================================================================
# EXECUTION TASKS (Algorithm Nodes - Mixed API/Docker)
# ============================================================================

'''
    
    # Generate execution tasks based on priority
    for alg_type_id in dag.alg_type_nodes:
        if alg_type_id not in algorithm_types or alg_type_id not in algorithm_instances:
            continue
        
        alg_type = algorithm_types[alg_type_id]
        alg_instance = algorithm_instances[alg_type_id]
        
        # Determine execution mode
        exec_mode, exec_config = alg_instance.get_execution_mode()
        
        if not exec_mode:
            print(f"Warning: No valid execution mode for {alg_type_id}, skipping...")
            continue
        
        param_names = [p['name'] for p in alg_type.params]
        task_id = f"execute_{alg_type_id}"
        
        # ========== DOCKER MODE ==========
        if exec_mode == 'docker':
            docker_image = exec_config.get('image', 'saharshlaud/stacd-backend:latest')
            volumes = exec_config.get('volumes', [])
            
            dag_code += f'''# {alg_type.name} (Docker Mode)
def call_{task_id}(**context):
    """Execute {alg_type_id} via Docker with UUID and DB logging"""
    execution_id = str(uuid.uuid4())
    dag_run_id = context['dag_run'].run_id
    
    # Build parameters
    params = context['params']
    param_dict = {{'''
            
            for name in param_names:
                dag_code += f'''
        '{name}': params.get('{name}'),'''
            
            dag_code += f'''
        'execution_id': execution_id
    }}
    
    # Get algorithm type from database
    db = STACDDatabase(DB_PATH)
    algo_type = db.get_active_algorithm_type('{alg_type_id}')
    if not algo_type:
        db.close()
        raise Exception(f"Algorithm type '{alg_type_id}' not found in database")
    
    # Log algorithm start
    db.log_algorithm_execution(
        execution_id=execution_id,
        algo_type_id=algo_type.algo_type_id,
        dag_uuid=DAG_UUID,
        dag_run_id=dag_run_id,
        node_name='{task_id}',
        status='running',
        parameters=param_dict
    )
    
    print(f"\\n{'='*60}")
    print(f"[ALGORITHM - DOCKER] {alg_type_id}")
    print(f"Execution ID: {{execution_id}}")
    print(f"DAG UUID: {{DAG_UUID}}")
    print(f"{'='*60}")
    
    try:
        # Build Docker command with volume mounts from YAML
        cmd = ['docker', 'run', '--rm']
'''
            
            # Add volume mounts from YAML
            for vol in volumes:
                source = os.path.expanduser(vol.get('source', ''))
                target = vol.get('target', '')
                read_only = ':ro' if vol.get('read_only', False) else ''
                dag_code += f'''
        cmd.extend(['-v', '{source}:{target}{read_only}'])'''
            
            dag_code += f'''
        cmd.extend(['{docker_image}', 'python', 'cli.py', '{alg_type_id}'])'''
            
            # Add CLI arguments
            for name in param_names:
                dag_code += f'''
        cmd.extend(['--{name}', str(param_dict['{name}'])])'''
            
            dag_code += f'''
        cmd.extend(['--execution_id', execution_id])
        
        # Execute Docker container
        print(f"Running: {{' '.join(cmd)}}")
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        # Parse JSON output (last line)
        output_lines = result.stdout.strip().split('\\n')
        json_output = output_lines[-1] if output_lines else '{{}}'
        print(f"Container output: {{json_output}}")
        result_data = json.loads(json_output)
        
        if result_data.get('status') != 'success':
            raise Exception(f"Algorithm failed: {{result_data}}")
        
        # Update database with success
        db.update_algorithm_status(
            execution_id=execution_id,
            status='success',
            execution_time=result_data.get('execution_time', 0)
        )
        
        print(f"Status: {{result_data.get('status')}}")
        print(f"Asset IDs: {{result_data.get('asset_ids', [])}}")
        print(f"Time: {{result_data.get('execution_time', 0):.1f}}s")
        print(f"✓ Logged to database\\n")
        
        # Push to XCom for dataset nodes
        context['ti'].xcom_push(key='execution_id', value=execution_id)
        context['ti'].xcom_push(key='asset_ids', value=result_data.get('asset_ids', []))
        context['ti'].xcom_push(key='execution_time', value=result_data.get('execution_time', 0))
        
        db.close()
        return result_data
        
    except subprocess.CalledProcessError as e:
        error_msg = f"Docker execution failed: {{e.stderr}}"
        db.update_algorithm_status(
            execution_id=execution_id,
            status='failed',
            error_message=error_msg
        )
        db.close()
        print(f"❌ Error: {{error_msg}}")
        raise
    except json.JSONDecodeError as e:
        error_msg = f"Failed to parse Docker output: {{str(e)}}"
        db.update_algorithm_status(
            execution_id=execution_id,
            status='failed',
            error_message=error_msg
        )
        db.close()
        print(f"❌ Error: {{error_msg}}")
        raise
    except Exception as e:
        db.update_algorithm_status(
            execution_id=execution_id,
            status='failed',
            error_message=str(e)
        )
        db.close()
        print(f"❌ Error: {{str(e)}}")
        raise

{task_id} = PythonOperator(
    task_id='{task_id}',
    python_callable=call_{task_id},
    provide_context=True,
    dag=dag
)

'''
        
        # ========== API MODE ==========
        else:  # exec_mode == 'api'
            api_url = exec_config.get('url', '')
            
            dag_code += f'''# {alg_type.name} (API Mode)
def call_{task_id}(**context):
    """Execute {alg_type_id} via API with UUID and DB logging"""
    execution_id = str(uuid.uuid4())
    dag_run_id = context['dag_run'].run_id
    
    payload = {{'''
            
            for name in param_names:
                dag_code += f'''
        "{name}": context["params"]["{name}"],'''
            
            dag_code += f'''
        "execution_id": execution_id
    }}
    
    db = STACDDatabase(DB_PATH)
    algo_type = db.get_active_algorithm_type('{alg_type_id}')
    if not algo_type:
        db.close()
        raise Exception(f"Algorithm type '{alg_type_id}' not found in database")
    
    db.log_algorithm_execution(
        execution_id=execution_id,
        algo_type_id=algo_type.algo_type_id,
        dag_uuid=DAG_UUID,
        dag_run_id=dag_run_id,
        node_name='{task_id}',
        status='running',
        parameters=payload
    )
    
    print(f"\\n{'='*60}")
    print(f"[ALGORITHM - API] {alg_type_id}")
    print(f"Execution ID: {{execution_id}}")
    print(f"DAG UUID: {{DAG_UUID}}")
    print(f"{'='*60}")
    
    try:
        response = requests.post("{api_url}", json=payload, headers={{"Content-Type": "application/json"}})
        response.raise_for_status()
        result = response.json()
        
        if result.get('status') != 'success':
            raise Exception(f"Failed: {{result}}")
        
        db.update_algorithm_status(
            execution_id=execution_id,
            status='success',
            execution_time=result.get('execution_time', 0)
        )
        
        print(f"Status: {{result.get('status')}}")
        print(f"Asset IDs: {{result.get('asset_ids', [])}}")
        print(f"Time: {{result.get('execution_time', 0):.1f}}s")
        print(f"✓ Logged to database\\n")
        
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
        print(f"❌ Error: {{str(e)}}")
        raise

{task_id} = PythonOperator(
    task_id='{task_id}',
    python_callable=call_{task_id},
    provide_context=True,
    dag=dag
)

'''
    
    # Generate dataset verification tasks
    dag_code += '''# ============================================================================
# VERIFICATION TASKS (Dataset Nodes)
# ============================================================================

'''
    
    for dataset_id in dag.dataset_type_nodes:
        if dataset_id not in dataset_types:
            continue
        
        # Find producing algorithm
        producing_algo = None
        for algo_id, algo_type in algorithm_types.items():
            if hasattr(algo_type, 'outputs') and algo_type.outputs and dataset_id in algo_type.outputs:
                producing_algo = f"execute_{algo_id}"
                break
        
        task_id = f"verify_{dataset_id}"
        
        dag_code += f'''# Verify {dataset_id}
def verify_{dataset_id}_func(**context):
    """Process {dataset_id} dataset with UUID and DB logging"""
    ti = context['ti']
    dag_run_id = context['dag_run'].run_id
    dataset_uuid = str(uuid.uuid4())
    
    db = STACDDatabase(DB_PATH)
    dataset_type = db.get_active_dataset_type('{dataset_id}')
    if not dataset_type:
        db.close()
        raise Exception(f"Dataset type '{dataset_id}' not found in database")
    
    # Pull from upstream algorithm
    execution_id = ti.xcom_pull(key='execution_id', task_ids='{producing_algo}') if '{producing_algo}' else 'unknown'
    asset_ids = ti.xcom_pull(key='asset_ids', task_ids='{producing_algo}') if '{producing_algo}' else []
    
    db.log_dataset_instance(
        dataset_uuid=dataset_uuid,
        dataset_type_id=dataset_type.dataset_type_id,
        execution_id=execution_id if execution_id else 'placeholder',
        dag_uuid=DAG_UUID,
        node_name='{task_id}',
        asset_ids=asset_ids if asset_ids else [],
        region_params=context['params']
    )
    
    print(f"\\n{'='*60}")
    print(f"[DATASET] {dataset_id}")
    print(f"Dataset UUID: {{dataset_uuid}}")
    print(f"Execution ID: {{execution_id}}")
    print(f"Asset IDs: {{asset_ids}}")
    print(f"✓ Logged to database")
    print(f"{'='*60}\\n")
    
    db.close()
    return True

{task_id} = PythonOperator(
    task_id='{task_id}',
    python_callable=verify_{dataset_id}_func,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    provide_context=True,
    dag=dag
)

'''
    
    # YAML-driven dependencies
    dag_code += '''# ============================================================================
# DEPENDENCIES (Generated from YAML input_datasets and outputs)
# ============================================================================

'''
    
    # Algorithm → Dataset (outputs)
    for algo_id, algo_type in algorithm_types.items():
        if not hasattr(algo_type, 'outputs') or not algo_type.outputs:
            continue
        for output_dataset in algo_type.outputs:
            if output_dataset in dag.dataset_type_nodes:
                dag_code += f"execute_{algo_id} >> verify_{output_dataset}\n"
    
    # Dataset → Algorithm (input_datasets)
    for algo_id, algo_type in algorithm_types.items():
        if not hasattr(algo_type, 'input_datasets') or not algo_type.input_datasets:
            continue
        for input_dataset in algo_type.input_datasets:
            if input_dataset in dag.dataset_type_nodes:
                dag_code += f"verify_{input_dataset} >> execute_{algo_id}\n"
    
    with open(output_file, 'w') as f:
        f.write(dag_code)
    
    print(f"\n✅ Generated MIXED DAG with YAML-driven dependencies + database")
    print(f"   DAG UUID: {dag_uuid}")
    print(f"   Output: {output_file}")
    
    # Print execution mode summary
    print(f"\n📊 Execution Mode Summary:")
    for algo_id in dag.alg_type_nodes:
        if algo_id in algorithm_instances:
            alg_instance = algorithm_instances[algo_id]
            exec_mode, exec_config = alg_instance.get_execution_mode()
            if exec_mode == 'api':
                print(f"  {algo_id}: API (priority {exec_config.get('priority', 'N/A')})")
            elif exec_mode == 'docker':
                volumes = exec_config.get('volumes', [])
                vol_summary = f"{len(volumes)} volume(s)" if volumes else "no volumes"
                print(f"  {algo_id}: Docker ({vol_summary}, priority {exec_config.get('priority', 'N/A')})")
    
    # Print dependency summary
    print(f"\n📊 Dependency Summary:")
    for algo_id, algo_type in algorithm_types.items():
        inputs = algo_type.input_datasets if hasattr(algo_type, 'input_datasets') and algo_type.input_datasets else []
        outputs = algo_type.outputs if hasattr(algo_type, 'outputs') and algo_type.outputs else []
        print(f"  {algo_id}:")
        print(f"    Inputs:  {inputs}")
        print(f"    Outputs: {outputs}")


def main():
    import sys, os
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    yaml_dir = os.path.join(os.path.dirname(base_dir), 'yaml_configs')
    output_dir = os.path.join(os.path.dirname(base_dir), 'generated_dags')
    
    dag_yaml = os.path.join(yaml_dir, 'stacd_dag.yaml')
    algo_repo_yaml = os.path.join(yaml_dir, 'stacd_algorithm_repo.yaml')
    output_file = os.path.join(output_dir, 'generated_mixed_dag.py')
    
    print("="*70)
    print("STACD MIXED DAG Generator - API + Docker + Database")
    print("="*70)
    
    dag, algorithm_types, algorithm_instances, dataset_types = parse_dag_specification(
        dag_yaml, algo_repo_yaml
    )
    
    if not dag:
        print("❌ No DAG found")
        sys.exit(1)
    
    generate_mixed_dag_with_db(dag, algorithm_types, algorithm_instances, dataset_types, output_file)
    
    print("\n🎉 Done! Copy to Airflow:")
    print(f"   cp {output_file} ~/airflow/dags/\n")


if __name__ == "__main__":
    main()
