from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base, DAG, AlgorithmType, AlgorithmInstance, AlgorithmExecution, DatasetType, DatasetInstance
import json
from datetime import datetime
import os
import sys

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", os.path.expanduser("~/airflow"))
sys.path.insert(0, os.path.join(AIRFLOW_HOME, "stacd/stac_export"))

DEFAULT_DB_PATH = os.path.join(AIRFLOW_HOME, 'stacd/database/stacd_recompute.db')


class STACDDatabase:
    def __init__(self, db_path=None):
        import os
        if db_path is None:
            db_path = DEFAULT_DB_PATH
        db_path = os.path.expanduser(db_path)
        self.engine = create_engine(f'sqlite:///{db_path}', echo=False)
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
    
    def close(self):
        self.session.close()
    
    def get_dag_by_id(self, dag_id):
        """Get DAG by dag_id (not uuid)"""
        return self.session.query(DAG).filter_by(dag_id=dag_id).first()
    
    # ========== Algorithm Type Management ==========
    
    def register_algorithm_type(self, algo_type_id, name, description, parameters, input_datasets, outputs):
        """Register or update an algorithm type"""
        algo_type = self.session.query(AlgorithmType).filter_by(algo_type_id=algo_type_id).first()
        
        if algo_type:
            print(f"  Algorithm type '{algo_type_id}' already exists, updating...")
            algo_type.name = name
            algo_type.description = description
            algo_type.parameters = parameters
            algo_type.input_datasets = input_datasets
            algo_type.outputs = outputs
        else:
            algo_type = AlgorithmType(
                algo_type_id=algo_type_id,
                name=name,
                description=description,
                parameters=parameters,
                input_datasets=input_datasets,
                outputs=outputs
            )
            self.session.add(algo_type)
            print(f"  Registered algorithm type: {algo_type_id}")
        
        self.session.commit()
        return algo_type
    
    # ========== Algorithm Instance Management ==========
    
    def register_algorithm_instance(self, algo_type_id, version, execution_modes, assets=None, date=None, supersedes_version=None):
        """
        Register a new algorithm version
        
        Args:
            algo_type_id: The algorithm type (e.g., 'LULC_Algorithm')
            version: Version string (e.g., '1', '2')
            execution_modes: Dict with 'api' and/or 'docker' config
            assets: Dict with code repository info
            date: Registration date
            supersedes_version: If provided, mark this version as inactive
        """
        # Check if version already exists
        existing = self.session.query(AlgorithmInstance).filter_by(
            algo_type_id=algo_type_id,
            version=version
        ).first()
        
        if existing:
            print(f"  ⚠️  Version {version} of {algo_type_id} already exists. Updating...")
            existing.execution_modes = execution_modes
            existing.assets = assets
            existing.is_active = True
            existing.date_registered = date or datetime.utcnow()
            self.session.commit()
            return existing
        
        # If supersedes_version is specified, deactivate the old version
        if supersedes_version:
            old_version = self.session.query(AlgorithmInstance).filter_by(
                algo_type_id=algo_type_id,
                version=supersedes_version
            ).first()
            if old_version:
                old_version.is_active = False
                print(f"  ⏸️  Deactivated version {supersedes_version} of {algo_type_id}")
        
        # Create new instance
        algo_instance = AlgorithmInstance(
            algo_type_id=algo_type_id,
            version=version,
            execution_modes=execution_modes,
            assets=assets or {},
            date_registered=date or datetime.utcnow(),
            is_active=True
        )
        
        self.session.add(algo_instance)
        self.session.commit()        
        print(f"Registered {algo_type_id} version {version} (active)")
        return algo_instance

    
    def get_active_algorithm_version(self, algo_type_id):
        """
        Get the currently active version of an algorithm
        Returns the AlgorithmInstance object
        """
        instance = self.session.query(AlgorithmInstance).filter_by(
            algo_type_id=algo_type_id,
            is_active=True
        ).first()
        
        if not instance:
            print(f"  ⚠️  No active version found for {algo_type_id}")
            return None
        
        return instance
    
    def get_algorithm_version(self, algo_type_id, version):
        """Get a specific version of an algorithm"""
        return self.session.query(AlgorithmInstance).filter_by(
            algo_type_id=algo_type_id,
            version=version
        ).first()
    
    def list_algorithm_versions(self, algo_type_id):
        """List all versions of an algorithm"""
        return self.session.query(AlgorithmInstance).filter_by(
            algo_type_id=algo_type_id
        ).order_by(AlgorithmInstance.version.desc()).all()
    
    # ========== Dataset Type Management ==========
    
    def register_dataset_type(self, dataset_type_id, name, description="", format_type="GEE_Asset"):
        """Register a dataset type"""
        dataset_type = self.session.query(DatasetType).filter_by(dataset_type_id=dataset_type_id).first()
        
        if dataset_type:
            print(f"  Dataset type '{dataset_type_id}' already exists")
        else:
            dataset_type = DatasetType(
                dataset_type_id=dataset_type_id,
                name=name,
                description=description,
                format=format_type
            )
            self.session.add(dataset_type)
            print(f"  Registered dataset type: {dataset_type_id}")
        
        self.session.commit()
        return dataset_type
    

    def register_root_dataset(self, dataset_type_id, asset_id, region, meta_info=None):
        """
        Register a root dataset (pre-existing, not produced by algorithm)
        
        Args:
            dataset_type_id: Type of dataset (e.g., 'MWS_Boundaries')
            asset_id: GEE asset ID or file path
            region: Dict with state/district/block
            meta_info: Additional metadata
        
        Returns:
            DatasetInstance object
        """
        # Check if this root dataset already exists
        existing = self.session.query(DatasetInstance).filter_by(
            dataset_type_id=dataset_type_id,
            asset_id=asset_id,
            is_root_dataset=True
        ).first()
        
        if existing:
            print(f"⚠️  Root dataset already exists: {asset_id}")
            return existing
        
        # Create new root dataset instance
        dataset = DatasetInstance(
            dataset_type_id=dataset_type_id,
            asset_id=asset_id,
            produced_by_algo=None,  # Root datasets aren't produced
            algo_version=None,
            run_id=None,
            region=region,
            meta_info=meta_info or {},
            is_root_dataset=True,
            created_at=datetime.utcnow()
        )
        
        self.session.add(dataset)
        self.session.commit()
        print(f"✅ Registered root dataset: {dataset_type_id} -> {asset_id}")
        return dataset


    def get_root_dataset(self, dataset_type_id, state, district, block):
        """
        Get root dataset for a specific region
        
        Args:
            dataset_type_id: e.g., 'MWS_Boundaries'
            state: State name
            district: District name
            block: Block name
        
        Returns:
            DatasetInstance or None
        """
        datasets = self.session.query(DatasetInstance).filter_by(
            dataset_type_id=dataset_type_id,
            is_root_dataset=True
        ).all()
        
        datasets = sorted(datasets, key=lambda x: x.version, reverse=True)



        for ds in datasets:
            # if ds.region:
            #     region = ds.region
            #     # Handle both dict (JSON column auto-parsed) and string (manually serialized)
            #     if isinstance(region, str):
            #         import json as _j
            #         try:
            #             region = _j.loads(region)
            #         except Exception:
            #             continue
            #     if (region.get('state', '').lower() == state.lower() and
            #         region.get('district', '').lower() == district.lower() and
            #         region.get('block', '').lower() == block.lower()):

            if ds.region:
                region = ds.region
                if isinstance(region, str):
                    # Handle pan_india string directly
                    if region.strip().lower() == 'pan_india':
                        return ds
                    try:
                        region = _j.loads(region)
                    except:
                        pass
                # Handle pan_india as dict key or plain string after JSON parse
                if isinstance(region, dict) and region.get('scope', '').lower() == 'pan_india':
                    return ds
                if (region.get('state', '').lower() == state.lower() and
                    region.get('district', '').lower() == district.lower() and
                    region.get('block', '').lower() == block.lower()):
                    return ds

        print(f"⚠️  No root dataset found for {dataset_type_id} in {state}/{district}/{block}")
        return None

    # ========== DAG Management ==========
    
    def register_dag(self, dag_id, name, version, description, structure):
        """Register or update a DAG"""
        dag = self.session.query(DAG).filter_by(dag_id=dag_id).first()
        
        if dag:
            print(f"DAG {dag_id} already exists with UUID {dag.dag_uuid} — skipping")
            return dag
        else:
            import uuid
            dag_uuid = str(uuid.uuid4())
            dag = DAG(
                dag_uuid=dag_uuid,
                dag_id=dag_id,
                name=name,
                version=version,
                description=description,
                structure=structure
            )
            self.session.add(dag)
            print(f"Registered DAG {dag_id} (UUID: {dag_uuid})")
        
        self.session.commit()
        
        # Generate STAC-D item for DAG
        from stacd_item_generator import generate_stacd_dag_item
        generate_stacd_dag_item(dag)
        
        return dag







    
    # ========== Execution Tracking ==========
    
    def log_algorithm_execution(self, dag_uuid, algo_type_id, version, run_id, execution_params, output_asset_id=None, status="success"):
        """Log an algorithm execution"""
        execution = AlgorithmExecution(
            dag_uuid=dag_uuid,
            algo_type_id=algo_type_id,
            version=version,
            run_id=run_id,
            execution_params=execution_params,
            output_asset_id=output_asset_id,
            executed_at=datetime.utcnow(),
            status=status
        )
        self.session.add(execution)
        self.session.commit()
        return execution
    

    def log_dataset_instance(self, dataset_type_id, asset_id, produced_by_algo, 
                            algo_version, run_id, region=None, meta_info=None):
        """
        Log dataset instance with auto-incrementing version
        """
        # ✅ Get latest version for this dataset type
        latest = self.session.query(DatasetInstance).filter_by(
            dataset_type_id=dataset_type_id
        ).order_by(DatasetInstance.version.desc()).first()
        
        # ✅ Calculate next version
        next_version = (latest.version + 1) if latest else 1
        
        # ✅ Create instance with version
        instance = DatasetInstance(
            dataset_type_id=dataset_type_id,
            version=next_version,  # ← AUTO-INCREMENT
            asset_id=asset_id,
            produced_by_algo=produced_by_algo,
            algo_version=algo_version,
            run_id=run_id,
            region=region,
            meta_info=meta_info or {},
            is_root_dataset=False
        )
        
        self.session.add(instance)
        self.session.commit()
        
        print(f"✅ Registered {dataset_type_id} version {next_version}")
        print(f"   Asset ID: {asset_id}")
        
        return instance
    

    def _parse_location_from_asset(self, asset_id):
        """Extract state/district/block from asset_id"""
        import re
        # Pattern: .../jharkhand/dumka/masalia/...
        pattern = r'/([^/]+)/([^/]+)/([^/]+)/[^/]+$'
        match = re.search(pattern, asset_id)
        if match:
            return {
                'state': match.group(1),
                'district': match.group(2),
                'block': match.group(3)
            }
        return {}

    def _parse_years_from_asset(self, asset_id):
        """Extract year range from asset_id"""
        import re
        # Pattern: 2017-07-01_2018-06-30
        pattern = r'(\d{4})-\d{2}-\d{2}_(\d{4})-\d{2}-\d{2}'
        match = re.search(pattern, asset_id)
        if match:
            return {'start': match.group(1), 'end': match.group(2)}
        return {}
    
    



    def get_dataset_instance(self, instance_id):
        """Get a dataset instance by primary key."""
        return (
            self.session.query(DatasetInstance)
            .filter_by(instance_id=instance_id)
            .first()
        )

    def get_algorithm_execution(self, run_id=None, algo_type_id=None, version=None):
        """
        Get an algorithm execution by run_id + algo_type_id (+ optional version).
        Returns the latest matching execution if multiple exist.
        """
        q = self.session.query(AlgorithmExecution)
        if run_id is not None:
            q = q.filter(AlgorithmExecution.run_id == run_id)
        if algo_type_id is not None:
            q = q.filter(AlgorithmExecution.algo_type_id == algo_type_id)
        if version is not None:
            q = q.filter(AlgorithmExecution.version == version)

        return q.order_by(AlgorithmExecution.executed_at.desc()).first()

    def search_datasets(
        self,
        dataset_type_id=None,
        asset_id_pattern=None,
        state=None,
        district=None,
        block=None,
    ):
        """
        Search dataset instances using basic filters.
        """
        q = self.session.query(DatasetInstance)

        if dataset_type_id:
            q = q.filter(DatasetInstance.dataset_type_id == dataset_type_id)
        if asset_id_pattern:
            from sqlalchemy import literal
            q = q.filter(DatasetInstance.asset_id.like(f"%{asset_id_pattern}%"))
        if state:
            q = q.filter(DatasetInstance.meta_info["state"].astext == state)
        if district:
            q = q.filter(DatasetInstance.meta_info["district"].astext == district)
        if block:
            q = q.filter(DatasetInstance.meta_info["block"].astext == block)

        return q.order_by(DatasetInstance.created_at.desc()).all()
    

    def update_root_dataset(self, dataset_type_id: str, new_asset_id: str,
                            region: dict, metadata: dict = None) -> object:
        """
        Register a new version of an existing root dataset.
        Inserts a new DatasetInstance row with incremented version.
        Does NOT delete the old version — full history is preserved.
        """
        try:
            # Find current max version for this type
            existing = self.session.query(DatasetInstance).filter(
                DatasetInstance.dataset_type_id == dataset_type_id
            ).order_by(DatasetInstance.version.desc()).first()

            new_version = (existing.version + 1) if existing else 1

            meta = {
                'registered_at': str(datetime.now()),
                'state': region.get('state'),
                'district': region.get('district'),
                'block': region.get('block'),
                'source': 'update_dataset_plugin'
            }
            if metadata:
                meta.update(metadata)

            new_instance = DatasetInstance(
                dataset_type_id=dataset_type_id,
                asset_id=new_asset_id,
                version=new_version,
                produced_by_algo=None,
                algo_version=None,
                run_id=None,
                meta_info=meta,       # pass dict directly, JSON column handles it
                region=region,        # pass dict directly, JSON column handles it
                is_root_dataset=True,
                created_at=datetime.now()
            )
            self.session.add(new_instance)
            self.session.commit()

            print(f"Root dataset {dataset_type_id} updated to version {new_version}")
            print(f"  New asset: {new_asset_id}")
            return new_instance

        except Exception as e:
            self.session.rollback()
            raise e


    def get_failed_executions(self, dag_id, state, district, block,
                            start_year=None, end_year=None):
        """
        Find ALL failed algo executions for a given dag + region + year combo
        from the most recent failed run_id.
        Returns list of algo_type_ids that failed.
        """
        dag_record = self.session.query(DAG).filter_by(dag_id=dag_id).first()
        if not dag_record:
            print(f"⚠️  No DAG record found for dag_id={dag_id}")
            return []

        failed_executions = self.session.query(AlgorithmExecution).filter_by(
            dag_uuid=dag_record.dag_uuid,
            status='failed'
        ).order_by(AlgorithmExecution.executed_at.desc()).all()

        if not failed_executions:
            print(f"⚠️  No failed executions found for {dag_id}")
            return []

        # Find the most recent run_id that had failures matching this region
        target_run_id = None
        for execution in failed_executions:
            params = execution.execution_params or {}
            if isinstance(params, str):
                try:
                    params = json.loads(params)
                except Exception:
                    continue

            if params.get('state', '').lower() != state.lower():
                continue
            if params.get('district', '').lower() != district.lower():
                continue
            if params.get('block', '').lower() != block.lower():
                continue
            if start_year and str(params.get('start_year', '')) != str(start_year):
                continue
            if end_year and str(params.get('end_year', '')) != str(end_year):
                continue

            # Found a matching failure — lock onto this run_id
            target_run_id = execution.run_id
            break

        if not target_run_id:
            print(f"⚠️  No matching failed execution for {state}/{district}/{block}")
            return []

        # Now collect ALL failed algos from that same run_id
        failed_algos = []
        all_from_run = self.session.query(AlgorithmExecution).filter_by(
            dag_uuid=dag_record.dag_uuid,
            run_id=target_run_id,
            status='failed'
        ).all()

        for execution in all_from_run:
            if execution.algo_type_id not in failed_algos:
                failed_algos.append(execution.algo_type_id)

        print(f"✓ Found {len(failed_algos)} failed task(s) in run {target_run_id}: {failed_algos}")
        return failed_algos

