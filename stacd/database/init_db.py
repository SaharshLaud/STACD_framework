#!/usr/bin/env python3


import yaml
import os
import sys
from datetime import datetime


# Add paths relative to AIRFLOW_HOME
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", os.path.expanduser("~/airflow"))
sys.path.insert(0, os.path.join(AIRFLOW_HOME, 'stacd/database'))
sys.path.insert(0, os.path.join(AIRFLOW_HOME, 'stacd/dag_generator'))
sys.path.insert(0, os.path.join(AIRFLOW_HOME, 'stacd/stac_export'))


from db_operations import STACDDatabase
from stacd_classes import DAG, Algorithm_Type, Algorithm_Instance, Dataset_Type

YAML_CONFIGS_DIR = os.path.join(AIRFLOW_HOME, 'stacd/yaml_configs')


def init_database_from_yamls(
    dag_yaml_path=None,
    algo_repo_path=None,
    dataset_repo_path=None
):
    """Initialize database by reading YAML configurations"""
    
    # Default paths if not provided
    if dag_yaml_path is None:
        dag_yaml_path = os.path.join(YAML_CONFIGS_DIR, 'corestack_lite_dag.yaml')
    if algo_repo_path is None:
        algo_repo_path = os.path.join(YAML_CONFIGS_DIR, 'corestack_lite_algorithm_repo.yaml')
    if dataset_repo_path is None:
        dataset_repo_path = os.path.join(YAML_CONFIGS_DIR, 'corestack_lite_dataset_repo.yaml')
    
    print("="*60)
    print("Initializing STACD Database from YAMLs")
    print("="*60)
    print(f"  DAG YAML:         {dag_yaml_path}")
    print(f"  Algorithm Repo:   {algo_repo_path}")
    print(f"  Dataset Repo:     {dataset_repo_path}")
    print("="*60)
    
    db = STACDDatabase()
    
    # 1. Load and register Algorithm Instances from Algorithm Repo
    print("\n Registering Algorithm Instances...")
    algo_repo_path_full = os.path.expanduser(algo_repo_path)
    
    with open(algo_repo_path_full, 'r') as f:
        algo_instances = list(yaml.load_all(f, Loader=yaml.FullLoader))
    
    for algo_inst in algo_instances:
        if isinstance(algo_inst, Algorithm_Instance):
            db.register_algorithm_instance(
                algo_type_id=algo_inst.type,
                version=algo_inst.version,
                execution_modes=algo_inst.execution_modes,
                assets=algo_inst.assets,
                date=algo_inst.date
            )
            print(f"   Registered: {algo_inst.type} v{algo_inst.version}")
    
    # 2. Load DAG structure and register Algorithm Types
    print("\n Registering Algorithm Types from DAG...")
    dag_yaml_path_full = os.path.expanduser(dag_yaml_path)
    
    with open(dag_yaml_path_full, 'r') as f:
        dag_docs = list(yaml.load_all(f, Loader=yaml.FullLoader))
    
    dag_obj = None
    algorithm_types = []
    dataset_types = []
    
    for doc in dag_docs:
        if isinstance(doc, DAG):
            dag_obj = doc
        elif isinstance(doc, Algorithm_Type):
            algorithm_types.append(doc)
        elif isinstance(doc, Dataset_Type):
            dataset_types.append(doc)
    
    # Register algorithm types
    for algo_type in algorithm_types:
        db.register_algorithm_type(
            algo_type_id=algo_type.id,
            name=algo_type.name,
            description=algo_type.description,
            parameters=algo_type.params,
            input_datasets=algo_type.input_datasets or [],
            outputs=algo_type.outputs or []
        )
        print(f"   Registered algo type: {algo_type.id}")
    
    # 3. Register Dataset Types
    print("\n Registering Dataset Types...")
    for dataset_type in dataset_types:
        db.register_dataset_type(
            dataset_type_id=dataset_type.id,
            name=dataset_type.name,
            description=getattr(dataset_type, 'description', ''),
            format_type=getattr(dataset_type, 'format', 'GEE_Asset')
        )
        print(f"   Registered dataset type: {dataset_type.id}")
    
    # 4. Load and register Root Datasets from Dataset Repo
    print("\n Registering Root Dataset Instances...")
    dataset_repo_path_full = os.path.expanduser(dataset_repo_path)
    if os.path.exists(dataset_repo_path_full):
        with open(dataset_repo_path_full, 'r') as f:
            dataset_instances = list(yaml.load_all(f, Loader=yaml.FullLoader))
        
        from stacd_classes import Dataset_Instance
        
        for dataset_inst in dataset_instances:
            if isinstance(dataset_inst, Dataset_Instance):
                db.register_root_dataset(
                    dataset_type_id=dataset_inst.type_id,
                    asset_id=dataset_inst.asset_id,
                    region=dataset_inst.region,
                    meta_info=dataset_inst.metadata
                )
                print(f"   Registered root dataset: {dataset_inst.type_id} -> {dataset_inst.asset_id}")
    else:
        print(f"  Dataset repo file not found at {dataset_repo_path_full}, skipping...")

    # 5. Register DAG
    if dag_obj:
        print("\n Registering DAG...")
        structure = {
            'alg_type_nodes': dag_obj.alg_type_nodes,
            'dataset_type_nodes': dag_obj.dataset_type_nodes,
            'params': dag_obj.params
        }
        
        dag_record = db.register_dag(
            dag_id=dag_obj.id,
            name=dag_obj.name,
            version=dag_obj.version,
            description=dag_obj.description,
            structure=structure
        )
        print(f"   Export tracking table: ready ✓")
        
        # Generate Algorithm STAC-D Items
        print("\n Generating Algorithm STAC-D Items...")
        from stacd_item_generator import generate_stacd_algorithm_item
        from models import AlgorithmInstance, AlgorithmType
        
        algo_instances = db.session.query(AlgorithmInstance).all()
        for algo_instance in algo_instances:
            algo_type = db.session.query(AlgorithmType).filter_by(
                algo_type_id=algo_instance.algo_type_id
            ).first()
            if algo_type:
                print(f"   {algo_instance.algo_type_id} v{algo_instance.version}")
                generate_stacd_algorithm_item(algo_instance, algo_type)
        
        print(f"\n✓ Database initialization complete!")
        print(f"   DAG ID:   {dag_record.dag_id}")
        print(f"   DAG UUID: {dag_record.dag_uuid}")
    
    db.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Initialize STACD Database from YAML configs")
    parser.add_argument(
        '--dag',
        default='corestack_lite_dag.yaml',
        help='DAG YAML filename (default: corestack_lite_dag.yaml)'
    )
    parser.add_argument(
        '--algo-repo',
        default='corestack_lite_algorithm_repo.yaml',
        help='Algorithm repo YAML filename (default: corestack_lite_algorithm_repo.yaml)'
    )
    parser.add_argument(
        '--dataset-repo',
        default='corestack_lite_dataset_repo.yaml',
        help='Dataset repo YAML filename (default: corestack_lite_dataset_repo.yaml)'
    )
    
    args = parser.parse_args()
    
    dag_yaml_path     = os.path.join(YAML_CONFIGS_DIR, args.dag)
    algo_repo_path    = os.path.join(YAML_CONFIGS_DIR, args.algo_repo)
    dataset_repo_path = os.path.join(YAML_CONFIGS_DIR, args.dataset_repo)
    
    print(f"\n Loading configs...")
    print(f"   DAG:          {args.dag}")
    print(f"   Algo Repo:    {args.algo_repo}")
    print(f"   Dataset Repo: {args.dataset_repo}\n")
    
    init_database_from_yamls(
        dag_yaml_path=dag_yaml_path,
        algo_repo_path=algo_repo_path,
        dataset_repo_path=dataset_repo_path
    )