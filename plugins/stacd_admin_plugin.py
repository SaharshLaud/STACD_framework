"""
STACD Admin Plugin - Upload YAML to Register Algorithms/Datasets
Supports custom YAML tags: !Algorithm_Instance and !Dataset_Instance
"""

from airflow.plugins_manager import AirflowPlugin
from flask_appbuilder import BaseView as AppBuilderBaseView, expose
from flask import request
from werkzeug.utils import secure_filename
import os
import yaml
from pathlib import Path
import sys
from datetime import datetime
import json as _json


# Add paths at module level
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", os.path.expanduser("~/airflow"))
DAG_GENERATOR_PATH = os.path.join(AIRFLOW_HOME, "stacd/dag_generator")
DB_MODULE_PATH = os.path.join(AIRFLOW_HOME, "stacd/database")
STAC_EXPORT_PATH = os.path.join(AIRFLOW_HOME, "stacd/stac_export")
YAML_CONFIGS_PATH = os.path.join(AIRFLOW_HOME, "stacd/yaml_configs")
AIRFLOW_DAGS_DIR = os.path.join(AIRFLOW_HOME, "dags")
GENERATED_DAGS_DIR = os.path.join(AIRFLOW_HOME, "stacd/generated_dags")

for path in [DAG_GENERATOR_PATH, DB_MODULE_PATH, STAC_EXPORT_PATH]:
    if path not in sys.path:
        sys.path.insert(0, path)

DB_PATH = os.path.join(AIRFLOW_HOME, 'stacd/database/stacd_recompute.db')
plugin_dir = os.path.dirname(os.path.abspath(__file__))


class STACDRegisterAlgorithmView(AppBuilderBaseView):
    """Register Algorithm via YAML Upload"""
    
    default_view = "index"
    route_base = "/stacd_register_algorithm"
    template_folder = os.path.join(plugin_dir, "templates")
    
    @expose('/', methods=['GET', 'POST'])
    def index(self):
        message = None
        message_type = None
        
        if request.method == 'POST':
            if 'yaml_file' not in request.files:
                message = 'No file selected'
                message_type = 'danger'
            else:
                file = request.files['yaml_file']
                
                if file.filename == '':
                    message = 'No file selected'
                    message_type = 'danger'
                elif file and self.allowed_file(file.filename):
                    filename = secure_filename(file.filename)
                    filepath = os.path.join(plugin_dir, filename)
                    file.save(filepath)
                    
                    try:
                        result = self.process_yaml(filepath, filename)
                        message = f'✅ {result}'
                        message_type = 'success'
                        print(f"SUCCESS: {message}")
                    except Exception as e:
                        message = f'❌ Error: {str(e)}'
                        message_type = 'danger'
                        print(f"ERROR: {message}")
                        import traceback
                        traceback.print_exc()
                else:
                    message = 'Only YAML files (.yaml or .yml) are allowed'
                    message_type = 'danger'
        
        return self.render_template("stacd_register_algorithm.html", 
                                    message=message, 
                                    message_type=message_type)
    
    def allowed_file(self, filename):
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in ['yaml', 'yml']
    
    def generate_stacd_algo_item(self, algo_type_id, version, execution_modes, assets):
        """Write STAC-D item for a newly registered algorithm version"""
        import json
        from pathlib import Path
        from datetime import datetime

        catalog_dir = Path.home() / "stacd_catalog"

        # Find all DAGs that use this algorithm
        dag_dirs = [d for d in (catalog_dir / "dags").iterdir() if d.is_dir()]

        wrote = []
        for dag_dir in dag_dirs:
            algo_dir = dag_dir / "algorithms" / algo_type_id / "versions"
            # Only write if this algo already exists under this DAG
            if not (dag_dir / "algorithms" / algo_type_id).exists():
                continue

            algo_dir.mkdir(parents=True, exist_ok=True)
            version_item = {
                "stac_version": "1.0.0",
                "stac_extensions": [
                    "https://github.com/saharsh-laud/stacd-spec/v1.0.0/schema.json"
                ],
                "type": "Feature",
                "id": f"{algo_type_id}_v{version}",
                "geometry": None,
                "bbox": None,
                "properties": {
                    "datetime": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "title": f"{algo_type_id} Version {version}",
                    "stacd:type": "algorithm_version",
                    "stacd:algo_type_id": algo_type_id,
                    "stacd:version": str(version),
                    "stacd:is_active": True,
                    "stacd:execution_modes": execution_modes,
                    "stacd:assets": assets,
                    "stacd:registered_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                },
                "assets": {
                    "code": {
                        "href": assets.get("code", ""),
                        "type": "text/html",
                        "title": "Source Code Repository",
                        "roles": ["code"]
                    }
                } if assets and assets.get("code") else {},
                "links": [
                    {
                        "rel": "parent",
                        "href": f"../algorithm.json",
                        "type": "application/json"
                    }
                ]
            }

            version_path = algo_dir / f"v{version}.json"
            with open(version_path, 'w') as f:
                json.dump(version_item, f, indent=2, ensure_ascii=False)
            wrote.append(str(version_path))
            print(f"✓ STAC-D algo version item written: {version_path}")

        return wrote

    
    def process_yaml(self, filepath, filename):
        """Process algorithm YAML with custom tags and insert into database"""
        
        from stacd_classes import Algorithm_Instance
        from db_operations import STACDDatabase
        from models import AlgorithmInstance
        
        print(f"Processing file: {filename}")
        
        with open(filepath, 'r') as f:
            yaml_objects = yaml.load(f, Loader=yaml.FullLoader)
        
        if not isinstance(yaml_objects, list):
            yaml_objects = [yaml_objects]
        
        db = STACDDatabase(DB_PATH)
        inserted = 0
        updated = 0
        deactivated = 0
        
        try:
            for algo_obj in yaml_objects:
                if isinstance(algo_obj, Algorithm_Instance):
                    algo_type_id = algo_obj.type
                    version = int(algo_obj.version)
                    execution_modes = algo_obj.execution_modes
                    assets = algo_obj.assets if hasattr(algo_obj, 'assets') else {}
                    
                    print(f"Processing: {algo_type_id} v{version}")
                    
                    # DEACTIVATE OLD VERSIONS
                    old_active = db.session.query(AlgorithmInstance).filter_by(
                        algo_type_id=algo_type_id,
                        is_active=True
                    ).all()
                    
                    for old in old_active:
                        if old.version != version:
                            old.is_active = False
                            old.updated_at = datetime.utcnow()
                            deactivated += 1
                            print(f"  🔽 Deactivated {algo_type_id} v{old.version}")
                    
                    # Check if this version exists
                    existing = db.session.query(AlgorithmInstance).filter_by(
                        algo_type_id=algo_type_id,
                        version=version
                    ).first()
                    
                    if existing:
                        existing.is_active = True
                        existing.execution_modes = execution_modes
                        existing.assets = assets
                        existing.updated_at = datetime.utcnow()
                        updated += 1
                        print(f"  ✓ Updated & activated {algo_type_id} v{version}")
                    else:
                        new_instance = AlgorithmInstance(
                            algo_type_id=algo_type_id,
                            version=version,
                            is_active=True,
                            execution_modes=execution_modes,
                            assets=assets,
                            date_registered=datetime.utcnow()
                        )
                        db.session.add(new_instance)
                        inserted += 1
                        print(f"  ✓ Inserted {algo_type_id} v{version} as active")
                    
                    db.session.commit()

                    # Generate STAC-D item for this algo version
                    self.generate_stacd_algo_item(algo_type_id, version, execution_modes, assets)
        
        finally:
            db.close()
        
        parts = []
        if inserted:
            parts.append(f"Registered {inserted} new algorithm(s)")
        if updated:
            parts.append(f"Updated {updated} algorithm(s)")
        if deactivated:
            parts.append(f"Deactivated {deactivated} old version(s)")
        
        return " | ".join(parts) if parts else "No changes made"


class STACDRegisterDatasetView(AppBuilderBaseView):
    """Register Dataset via YAML Upload"""
    
    default_view = "index"
    route_base = "/stacd_register_dataset"
    template_folder = os.path.join(plugin_dir, "templates")
    
    @expose('/', methods=['GET', 'POST'])
    def index(self):
        message = None
        message_type = None
        
        if request.method == 'POST':
            if 'yaml_file' not in request.files:
                message = 'No file selected'
                message_type = 'danger'
            else:
                file = request.files['yaml_file']
                
                if file.filename == '':
                    message = 'No file selected'
                    message_type = 'danger'
                elif file and self.allowed_file(file.filename):
                    filename = secure_filename(file.filename)
                    filepath = os.path.join(plugin_dir, filename)
                    file.save(filepath)
                    
                    try:
                        result = self.process_yaml(filepath, filename)
                        message = f'✅ {result}'
                        message_type = 'success'
                        print(f"SUCCESS: {message}")
                    except Exception as e:
                        message = f'❌ Error: {str(e)}'
                        message_type = 'danger'
                        print(f"ERROR: {message}")
                        import traceback
                        traceback.print_exc()
                else:
                    message = 'Only YAML files (.yaml or .yml) are allowed'
                    message_type = 'danger'
        
        return self.render_template("stacd_register_dataset.html",
                                    message=message,
                                    message_type=message_type)
    
    def allowed_file(self, filename):
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in ['yaml', 'yml']
    
    def process_yaml(self, filepath, filename):
        """Process dataset YAML with custom tags and insert into database"""
        
        from stacd_classes import Dataset_Instance
        from db_operations import STACDDatabase
        from models import DatasetInstance
        
        print(f"Processing file: {filename}")
        
        with open(filepath, 'r') as f:
            yaml_objects = yaml.load(f, Loader=yaml.FullLoader)
        
        if not isinstance(yaml_objects, list):
            yaml_objects = [yaml_objects]
        
        db = STACDDatabase(DB_PATH)
        results = []
        
        try:
            for dataset_obj in yaml_objects:
                if isinstance(dataset_obj, Dataset_Instance):
                    dataset_type_id = dataset_obj.type_id
                    version = int(dataset_obj.version)
                    asset_id = dataset_obj.asset_id

                    region = dataset_obj.region   # pass dict directly — SQLAlchemy JSON column handles serialization
                    is_root = True
                    meta_info = dataset_obj.metadata if hasattr(dataset_obj, 'metadata') else {}
                    # meta_info also passed as dict — JSON column handles it


                    # Check if this exact version already exists
                    existing = db.session.query(DatasetInstance).filter_by(
                        dataset_type_id=dataset_type_id,
                        version=version
                    ).first()
                    
                    if existing:
                        # Update existing version
                        existing.asset_id = asset_id
                        existing.region = region
                        existing.meta_info = meta_info
                        db.session.commit()
                        print(f"✓ Updated {dataset_type_id} v{version}")
                        results.append(f"Updated {dataset_type_id} v{version}")
                    else:
                        # Insert new version
                        new_instance = DatasetInstance(
                            dataset_type_id=dataset_type_id,
                            version=version,
                            asset_id=asset_id,
                            region=region,
                            meta_info=meta_info,
                            is_root_dataset=is_root,
                            produced_by_algo=None,
                            algo_version=None
                        )
                        
                        db.session.add(new_instance)
                        db.session.commit()
                        print(f"✓ Registered {dataset_type_id} v{version}")
                        results.append(f"Registered {dataset_type_id} v{version}")
        
        finally:
            db.close()
        
        return " | ".join(results) if results else "No datasets processed"


class STACDUpdateDagView(AppBuilderBaseView):
    """Update DAG structure via two YAML uploads: new nodes + updated DAG"""
    
    default_view = "index"
    route_base = "/stacd_update_dag"
    template_folder = os.path.join(plugin_dir, "templates")

    @expose('/', methods=['GET', 'POST'])
    def index(self):
        message = None
        message_type = None

        if request.method == 'POST':
            nodes_file = request.files.get('nodes_yaml')
            dag_file = request.files.get('dag_yaml')

            if not nodes_file or not dag_file:
                message = 'Both YAML files are required (new nodes + updated DAG)'
                message_type = 'danger'
            elif not self.allowed_file(nodes_file.filename) or not self.allowed_file(dag_file.filename):
                message = 'Only YAML files (.yaml or .yml) are allowed'
                message_type = 'danger'
            else:
                nodes_path = os.path.join(plugin_dir, secure_filename(nodes_file.filename))
                dag_path = os.path.join(plugin_dir, secure_filename(dag_file.filename))
                nodes_file.save(nodes_path)
                dag_file.save(dag_path)

                try:
                    result = self.process_update_dag(nodes_path, dag_path)
                    message = f'✅ {result}'
                    message_type = 'success'
                    print(f"SUCCESS: {message}")
                except Exception as e:
                    message = f'❌ Error: {str(e)}'
                    message_type = 'danger'
                    print(f"ERROR: {message}")
                    import traceback
                    traceback.print_exc()

        return self.render_template("stacd_update_dag.html",
                                    message=message,
                                    message_type=message_type)

    def allowed_file(self, filename):
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in ['yaml', 'yml']

    def process_update_dag(self, nodes_path, dag_yaml_path):
        """
        Full update_dag flow:
        1. Register new AlgorithmType + DatasetType from nodes YAML
        2. Bump DAG version in DB + update structure
        3. Write new versioned STAC-D catalog folder
        4. Regenerate DAG Python file + deploy to ~/airflow/dags/
        """
        from db_operations import STACDDatabase
        from models import AlgorithmType, DatasetType, AlgorithmInstance, DAG
        from stacd_classes import Algorithm_Type, Dataset_Type, DAG as DAG_Class

        stac_export_path = os.path.join(AIRFLOW_HOME, 'stacd/stac_export')
        if stac_export_path not in sys.path:
            sys.path.insert(0, stac_export_path)
        from stacd_item_generator import generate_stacd_dag_item, generate_stacd_algorithm_item

        db = STACDDatabase(DB_PATH)
        results = []

        try:
            # ── STEP 1: Register new AlgorithmType + DatasetType entries ──
            with open(nodes_path, 'r') as f:
                nodes_docs = yaml.load_all(f, Loader=yaml.FullLoader)
                nodes_objects = list(nodes_docs)

            new_algos = 0
            new_datasets = 0

            for obj in nodes_objects:
                if obj is None:
                    continue

                if isinstance(obj, Algorithm_Type):
                    existing = db.session.query(AlgorithmType).filter_by(
                        algo_type_id=obj.id
                    ).first()
                    if not existing:
                        algo_type = AlgorithmType(
                            algo_type_id=obj.id,
                            name=obj.name,
                            description=getattr(obj, 'description', ''),
                            parameters=getattr(obj, 'params', {}),
                            input_datasets=getattr(obj, 'input_datasets', []),
                            outputs=getattr(obj, 'outputs', [])
                        )
                        db.session.add(algo_type)
                        new_algos += 1
                        print(f"  ✓ Registered new AlgorithmType: {obj.id}")
                    else:
                        print(f"  ℹ️  AlgorithmType already exists: {obj.id}")

                elif isinstance(obj, Dataset_Type):
                    existing = db.session.query(DatasetType).filter_by(
                        dataset_type_id=obj.id
                    ).first()
                    if not existing:
                        dataset_type = DatasetType(
                            dataset_type_id=obj.id,
                            name=obj.name,
                            description=getattr(obj, 'description', ''),
                            format=getattr(obj, 'format', 'GEE_Asset')
                        )
                        db.session.add(dataset_type)
                        new_datasets += 1
                        print(f"  ✓ Registered new DatasetType: {obj.id}")
                    else:
                        print(f"  ℹ️  DatasetType already exists: {obj.id}")

            db.session.commit()
            results.append(f"Registered {new_algos} new algo type(s), {new_datasets} new dataset type(s)")

            # ── STEP 2: Parse updated DAG YAML + bump version in DB ──
            dag_gen_path = os.path.join(AIRFLOW_HOME, 'stacd/dag_generator')
            if dag_gen_path not in sys.path:
                sys.path.insert(0, dag_gen_path)
            from stacd_recompute_generator import parse_dag_specification, build_dependency_graph, generate_dag_code

            dag_obj, algorithm_types, dataset_types = parse_dag_specification(dag_yaml_path)

            dag_record = db.session.query(DAG).filter_by(dag_id=dag_obj.id).first()
            if not dag_record:
                raise ValueError(f"DAG '{dag_obj.id}' not found in DB. Run init_db first.")

            old_version = int(float(dag_record.version)) if dag_record.version else 1

            new_version = old_version + 1

            dag_record.version = str(new_version)
            dag_record.structure = {
                'alg_type_nodes': dag_obj.alg_type_nodes,
                'dataset_type_nodes': dag_obj.dataset_type_nodes,
                'params': dag_obj.params or []
            }
            dag_record.updated_at = datetime.utcnow()
            db.session.commit()

            print(f"  ✓ DAG '{dag_obj.id}' bumped from v{old_version} → v{new_version}")
            results.append(f"DAG '{dag_obj.id}' updated to v{new_version}")

            # ── STEP 3: Write new versioned STAC-D catalog folder ──
            generate_stacd_dag_item(dag_record)

            # Write algo items for ALL algos in the new DAG version
            for algo_id in dag_obj.alg_type_nodes:
                algo_type_record = db.session.query(AlgorithmType).filter_by(
                    algo_type_id=algo_id
                ).first()
                algo_instance_record = db.session.query(AlgorithmInstance).filter_by(
                    algo_type_id=algo_id,
                    is_active=True
                ).first()

                if algo_type_record and algo_instance_record:
                    generate_stacd_algorithm_item(algo_instance_record, algo_type_record)
                    print(f"  ✓ STAC-D item written for {algo_id}")
                else:
                    print(f"  ⚠️  No active instance found for {algo_id} — skipping STAC-D item")

            results.append(f"STAC-D catalog written: dag{dag_obj.id}_v{new_version}/")

            # ── STEP 4: Regenerate DAG Python file + deploy ──
            dependencies = build_dependency_graph(dag_obj, algorithm_types)
            dag_code = generate_dag_code(dag_obj, algorithm_types, dataset_types, dependencies)

            airflow_dags_dir = os.path.join(AIRFLOW_HOME, 'dags')
            output_file = os.path.join(airflow_dags_dir, f"{dag_obj.id}_generated.py")
            with open(output_file, 'w') as f:
                f.write(dag_code)

            print(f"  ✓ DAG file regenerated: {output_file}")
            results.append(f"DAG file regenerated and deployed")

        finally:
            db.close()

        return " | ".join(results)


class STACDInitializeWorkflowView(AppBuilderBaseView):
    """One-click workflow initialization: upload 3 YAMLs → DB init + DAG generation + deploy"""

    default_view = "index"
    route_base = "/stacd_initialize_workflow"
    template_folder = os.path.join(plugin_dir, "templates")

    @expose('/', methods=['GET', 'POST'])
    def index(self):
        message = None
        message_type = None

        if request.method == 'POST':
            dag_file = request.files.get('dag_yaml')
            algo_file = request.files.get('algo_repo_yaml')
            dataset_file = request.files.get('dataset_repo_yaml')

            if not dag_file or not algo_file or not dataset_file:
                message = 'All 3 YAML files are required (DAG, Algorithm Repo, Dataset Repo)'
                message_type = 'danger'
            elif not all(self.allowed_file(f.filename) for f in [dag_file, algo_file, dataset_file]):
                message = 'Only YAML files (.yaml or .yml) are allowed'
                message_type = 'danger'
            else:
                # Save uploaded files with their original names
                dag_filename = secure_filename(dag_file.filename)
                algo_filename = secure_filename(algo_file.filename)
                dataset_filename = secure_filename(dataset_file.filename)

                dag_path = os.path.join(YAML_CONFIGS_PATH, dag_filename)
                algo_path = os.path.join(YAML_CONFIGS_PATH, algo_filename)
                dataset_path = os.path.join(YAML_CONFIGS_PATH, dataset_filename)

                dag_file.save(dag_path)
                algo_file.save(algo_path)
                dataset_file.save(dataset_path)

                try:
                    result = self.process_initialize(dag_path, algo_path, dataset_path)
                    message = f'✅ {result}'
                    message_type = 'success'
                    print(f"SUCCESS: {message}")
                except Exception as e:
                    message = f'❌ Error: {str(e)}'
                    message_type = 'danger'
                    print(f"ERROR: {message}")
                    import traceback
                    traceback.print_exc()

        return self.render_template("stacd_initialize_workflow.html",
                                    message=message,
                                    message_type=message_type)

    def allowed_file(self, filename):
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in ['yaml', 'yml']

    def process_initialize(self, dag_path, algo_path, dataset_path):
        """
        Full initialization flow:
        1. Check if DAG already exists in DB (error if so)
        2. Run init_db logic to populate database
        3. Run DAG generator to produce Python DAG file
        4. Deploy generated DAG to ~/airflow/dags/
        """
        from stacd_classes import DAG as DAG_Class
        from db_operations import STACDDatabase
        from models import DAG as DAGModel

        results = []

        # ── PRE-CHECK: Parse DAG YAML to get the dag_id ──
        with open(dag_path, 'r') as f:
            dag_docs = list(yaml.load_all(f, Loader=yaml.FullLoader))

        dag_obj = None
        for doc in dag_docs:
            if isinstance(doc, DAG_Class):
                dag_obj = doc
                break

        if not dag_obj:
            raise ValueError("No !DAG object found in the uploaded DAG YAML file.")

        # ── STEP 1: Check if DAG already exists ──
        db = STACDDatabase(DB_PATH)
        try:
            existing_dag = db.session.query(DAGModel).filter_by(dag_id=dag_obj.id).first()
            if existing_dag:
                raise ValueError(
                    f"DAG '{dag_obj.id}' already exists in the database "
                    f"(UUID: {existing_dag.dag_uuid}). "
                    f"Use 'STACD → Update DAG' to modify an existing workflow."
                )
        finally:
            db.close()

        # ── STEP 2: Initialize database from YAMLs ──
        print("="*60)
        print("STEP 2: Initializing database from YAMLs")
        print("="*60)

        from init_db import init_database_from_yamls
        init_database_from_yamls(
            dag_yaml_path=dag_path,
            algo_repo_path=algo_path,
            dataset_repo_path=dataset_path
        )
        results.append("Database initialized")

        # ── STEP 3: Generate DAG Python file ──
        print("="*60)
        print("STEP 3: Generating DAG Python script")
        print("="*60)

        from stacd_recompute_generator import parse_dag_specification, build_dependency_graph, generate_dag_code

        dag_obj, algorithm_types, dataset_types = parse_dag_specification(dag_path)
        dependencies = build_dependency_graph(dag_obj, algorithm_types)
        dag_code = generate_dag_code(dag_obj, algorithm_types, dataset_types, dependencies)

        # Write to generated_dags directory
        os.makedirs(GENERATED_DAGS_DIR, exist_ok=True)
        generated_filename = f"{dag_obj.id}_generated.py"
        generated_path = os.path.join(GENERATED_DAGS_DIR, generated_filename)

        with open(generated_path, 'w') as f:
            f.write(dag_code)
        print(f"✓ DAG script generated: {generated_path}")
        results.append(f"DAG script generated")

        # ── STEP 4: Deploy to Airflow dags directory ──
        print("="*60)
        print("STEP 4: Deploying DAG to Airflow")
        print("="*60)

        os.makedirs(AIRFLOW_DAGS_DIR, exist_ok=True)
        deployed_path = os.path.join(AIRFLOW_DAGS_DIR, generated_filename)

        import shutil
        shutil.copy2(generated_path, deployed_path)
        print(f"✓ DAG deployed: {deployed_path}")
        results.append(f"DAG '{dag_obj.id}' deployed to Airflow")

        print("="*60)
        print("✓ Workflow initialization complete!")
        print(f"   DAG ID: {dag_obj.id}")
        print(f"   Algorithms: {', '.join(dag_obj.alg_type_nodes)}")
        print(f"   Datasets: {', '.join(dag_obj.dataset_type_nodes)}")
        print("="*60)

        return " | ".join(results)


class STACDAdminPlugin(AirflowPlugin):
    name = "stacd_admin"
    
    appbuilder_views = [
        {
            "name": "Initialize Workflow",
            "category": "STACD",
            "view": STACDInitializeWorkflowView()
        },
        {
            "name": "Register Algorithm",
            "category": "STACD",
            "view": STACDRegisterAlgorithmView()
        },
        {
            "name": "Register Dataset",
            "category": "STACD",
            "view": STACDRegisterDatasetView()
        },
        {
            "name": "Update DAG",
            "category": "STACD",
            "view": STACDUpdateDagView()
        }
    ]
