"""
STACD Database Operations (Simplified)
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Algorithm, AlgorithmExecution, Dataset
from datetime import datetime

class STACDDatabase:
    def __init__(self, db_path='stacd.db'):
        self.engine = create_engine(f'sqlite:///{db_path}', echo=False)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
    
    # ========== Algorithm Operations ==========
    
    def add_algorithm(self, node_name, display_name, description, output_type):
        """Add algorithm to registry"""
        algo = Algorithm(
            node_name=node_name,
            display_name=display_name,
            description=description,
            output_type=output_type
        )
        self.session.add(algo)
        self.session.commit()
        return algo
    
    def get_algorithm_by_node(self, node_name):
        """Get algorithm by node name"""
        return self.session.query(Algorithm).filter_by(node_name=node_name).first()
    
    # ========== Execution Operations ==========
    
    def log_execution(self, execution_id, node_name, status, parameters, 
                    execution_time=None, error_message=None, 
                    dag_run_id=None, task_id=None):
        """Log or update algorithm execution"""
        algo = self.get_algorithm_by_node(node_name)
        if not algo:
            raise ValueError(f"Algorithm '{node_name}' not found in registry")
        
        # Check if execution already exists
        existing = self.session.query(AlgorithmExecution).filter_by(execution_id=execution_id).first()
        
        if existing:
            # UPDATE existing execution
            existing.status = status
            existing.execution_time = execution_time
            existing.error_message = error_message
            self.session.commit()
            return existing
        else:
            # INSERT new execution
            execution = AlgorithmExecution(
                execution_id=execution_id,
                algorithm_id=algo.id,
                node_name=node_name,
                status=status,
                parameters=parameters,
                execution_time=execution_time,
                error_message=error_message,
                dag_run_id=dag_run_id,
                task_id=task_id
            )
            self.session.add(execution)
            self.session.commit()
            return execution

    def get_execution(self, execution_id):
        """Get execution by ID"""
        return self.session.query(AlgorithmExecution).filter_by(execution_id=execution_id).first()
    
    # ========== Dataset Operations ==========
    
    def add_dataset(self, dataset_uuid, dataset_type, version, parameters, 
                   asset_ids, location, execution_id=None, 
                   input_dataset_uuid=None, is_prepopulated=False):
        """Add dataset instance"""
        dataset = Dataset(
            dataset_uuid=dataset_uuid,
            execution_id=execution_id,
            dataset_type=dataset_type,
            version=version,
            parameters=parameters,
            asset_ids=asset_ids,
            location=location,
            input_dataset_uuid=input_dataset_uuid,  # ← Track dependency here!
            is_prepopulated=is_prepopulated
        )
        self.session.add(dataset)
        self.session.commit()
        return dataset
    
    def get_dataset(self, dataset_uuid):
        """Get dataset by UUID"""
        return self.session.query(Dataset).filter_by(dataset_uuid=dataset_uuid).first()
    
    # ========== Query Operations ==========
    
    def get_recent_executions(self, node_name=None, limit=10):
        """Get recent executions"""
        query = self.session.query(AlgorithmExecution)
        if node_name:
            query = query.filter_by(node_name=node_name)
        return query.order_by(AlgorithmExecution.created_at.desc()).limit(limit).all()
    
    def get_dataset_lineage(self, dataset_uuid):
        """Get what created this dataset and what it was used to create"""
        dataset = self.get_dataset(dataset_uuid)
        if not dataset:
            return None
        
        # Find datasets that used this as input
        downstream = self.session.query(Dataset).filter_by(input_dataset_uuid=dataset_uuid).all()
        
        return {
            'dataset': dataset,
            'created_by': dataset.execution,
            'input_dataset': dataset.input_dataset,
            'used_to_create': downstream
        }
    
    def close(self):
        """Close database connection"""
        self.session.close()
