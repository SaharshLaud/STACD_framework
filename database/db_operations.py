"""
STACD Database Operations
Helper functions for CRUD operations
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base, DAG, AlgorithmType, AlgorithmInstance, DatasetType, DatasetInstance
from datetime import datetime


class STACDDatabase:
    """Database connection and operations"""
    
    def __init__(self, db_path='stacd.db'):
        """Initialize database connection"""
        self.engine = create_engine(f'sqlite:///{db_path}')
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
    
    def close(self):
        """Close database connection"""
        self.session.close()
    
    # ========================================================================
    # DAG Operations
    # ========================================================================
    
    def register_dag(self, dag_uuid, dag_id, name, version, description=None, 
                     structure=None, parameters=None):
        """Register a new DAG"""
        dag = DAG(
            dag_uuid=dag_uuid,
            dag_id=dag_id,
            name=name,
            version=version,
            description=description,
            structure=structure,
            parameters=parameters
        )
        self.session.add(dag)
        self.session.commit()
        return dag
    
    def get_dag(self, dag_uuid):
        """Get DAG by UUID"""
        return self.session.query(DAG).filter_by(dag_uuid=dag_uuid).first()
    
    # ========================================================================
    # Algorithm Type Operations
    # ========================================================================
    
    def register_algorithm_type(self, algo_name, version, params_schema=None,
                                api_url=None, docker_image=None, execution_mode='api',
                                description=None):
        """Register a new algorithm type"""
        algo_type = AlgorithmType(
            algo_name=algo_name,
            version=version,
            description=description,
            params_schema=params_schema,
            api_url=api_url,
            docker_image=docker_image,
            execution_mode=execution_mode
        )
        self.session.add(algo_type)
        self.session.commit()
        return algo_type
    
    def get_active_algorithm_type(self, algo_name):
        """Get active version of algorithm type"""
        return self.session.query(AlgorithmType).filter_by(
            algo_name=algo_name, is_active=True
        ).first()
    
    # ========================================================================
    # Algorithm Instance Operations
    # ========================================================================
    
    def log_algorithm_execution(self, execution_id, algo_type_id, dag_uuid, 
                                 node_name, status, dag_run_id=None, 
                                 parameters=None, execution_time=None, 
                                 error_message=None):
        """Log algorithm execution"""
        instance = AlgorithmInstance(
            execution_id=execution_id,
            algo_type_id=algo_type_id,
            dag_uuid=dag_uuid,
            dag_run_id=dag_run_id,
            node_name=node_name,
            status=status,
            parameters=parameters,
            execution_time=execution_time,
            error_message=error_message
        )
        if status in ['success', 'failed']:
            instance.completed_at = datetime.utcnow()
        
        self.session.add(instance)
        self.session.commit()
        return instance
    
    def update_algorithm_status(self, execution_id, status, execution_time=None, 
                                 error_message=None):
        """Update algorithm execution status"""
        instance = self.session.query(AlgorithmInstance).filter_by(
            execution_id=execution_id
        ).first()
        
        if instance:
            instance.status = status
            if execution_time:
                instance.execution_time = execution_time
            if error_message:
                instance.error_message = error_message
            if status in ['success', 'failed']:
                instance.completed_at = datetime.utcnow()
            self.session.commit()
        
        return instance
    
    # ========================================================================
    # Dataset Type Operations
    # ========================================================================
    
    def register_dataset_type(self, dataset_name, version, schema=None,
                              produced_by_algo=None, storage_pattern=None,
                              description=None):
        """Register a new dataset type"""
        dataset_type = DatasetType(
            dataset_name=dataset_name,
            version=version,
            description=description,
            schema=schema,
            produced_by_algo=produced_by_algo,
            storage_pattern=storage_pattern
        )
        self.session.add(dataset_type)
        self.session.commit()
        return dataset_type
    
    def get_active_dataset_type(self, dataset_name):
        """Get active version of dataset type"""
        return self.session.query(DatasetType).filter_by(
            dataset_name=dataset_name, is_active=True
        ).first()
    
    # ========================================================================
    # Dataset Instance Operations
    # ========================================================================
    
    def log_dataset_instance(self, dataset_uuid, dataset_type_id, execution_id,
                             dag_uuid, node_name, asset_ids, region_params=None):
        """Log dataset instance"""
        instance = DatasetInstance(
            dataset_uuid=dataset_uuid,
            dataset_type_id=dataset_type_id,
            execution_id=execution_id,
            dag_uuid=dag_uuid,
            node_name=node_name,
            asset_ids=asset_ids,
            region_params=region_params
        )
        self.session.add(instance)
        self.session.commit()
        return instance
