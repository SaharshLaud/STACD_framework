from sqlalchemy import Column, String, Integer, DateTime, JSON, Boolean, ForeignKey, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()

class DAG(Base):
    __tablename__ = 'dags'
    
    dag_uuid = Column(String, primary_key=True)
    dag_id = Column(String, nullable=False)
    name = Column(String, nullable=False)
    version = Column(String, default="1")
    description = Column(Text)
    structure = Column(JSON)  # Store DAG topology
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False, index=True)

class AlgorithmType(Base):
    __tablename__ = 'algorithm_types'
    
    algo_type_id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(Text)
    parameters = Column(JSON)
    input_datasets = Column(JSON)  # List of dataset_type_ids
    outputs = Column(JSON)  # List of dataset_type_ids
    created_at = Column(DateTime, default=datetime.utcnow)

class AlgorithmInstance(Base):
    """Stores configuration for each algorithm version"""
    __tablename__ = 'algorithm_instances'
    
    instance_id = Column(Integer, primary_key=True, autoincrement=True)
    algo_type_id = Column(String, ForeignKey('algorithm_types.algo_type_id'), nullable=False)
    version = Column(String, nullable=False)  # "1", "2", etc.
    is_active = Column(Boolean, default=True)  # Only one version active at a time
    
    # Execution modes stored as JSON
    execution_modes = Column(JSON, nullable=False)  # { "api": {...}, "docker": {...} }
    
    # Assets information (code repo, Docker image, etc.)
    assets = Column(JSON, nullable=True)  # { "code": "https://...", "image": "..." }
    
    # Date when this version was registered
    date_registered = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)  
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class AlgorithmExecution(Base):
    """Stores individual algorithm executions"""
    __tablename__ = 'algorithm_executions'
    
    execution_id = Column(Integer, primary_key=True, autoincrement=True)
    dag_uuid = Column(String, ForeignKey('dags.dag_uuid'), nullable=False)
    algo_type_id = Column(String, ForeignKey('algorithm_types.algo_type_id'), nullable=False)
    version = Column(String, nullable=False)  # Track which version was used
    run_id = Column(String, nullable=False)  # Airflow run_id
    execution_params = Column(JSON)  # Parameters used for this execution
    output_asset_id = Column(String, nullable=True)  # GEE asset ID or output path
    status = Column(String, default="success")  # success, failed
    executed_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True) 

class DatasetType(Base):
    __tablename__ = 'dataset_types'
    
    dataset_type_id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(Text)
    format = Column(String)  # e.g., "GeoTIFF", "Shapefile", "GEE_Asset"
    created_at = Column(DateTime, default=datetime.utcnow)

class DatasetInstance(Base):
    """Stores individual dataset instances (both produced and root datasets)"""
    __tablename__ = 'dataset_instances'
    
    instance_id = Column(Integer, primary_key=True, autoincrement=True)
    dataset_type_id = Column(String, ForeignKey('dataset_types.dataset_type_id'), nullable=False)
    version = Column(Integer, nullable=False, default=1)
    asset_id = Column(String, nullable=False)  # GEE asset ID or file path
    produced_by_algo = Column(String, nullable=True)  # Algorithm type that produced this
    algo_version = Column(String, nullable=True)  # Version of algorithm used
    run_id = Column(String, nullable=True)  # Airflow run_id for lineage
    meta_info = Column(JSON)  # Additional metadata (CHANGED from 'metadata')
    region = Column(JSON) # Store state/district/block for root datasets
    is_root_dataset = Column(Boolean, default=False)  # Flag for root datasets
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)  #index=True
