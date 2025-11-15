"""
STACD Database Models (Simplified 3-Table Design)
"""
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, DateTime, JSON, ForeignKey, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()

class Algorithm(Base):
    """Algorithm definitions (pre-populated from YAML)"""
    __tablename__ = 'algorithms'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    node_name = Column(String(100), unique=True, nullable=False, index=True)
    display_name = Column(String(200), nullable=False)
    description = Column(Text)
    output_type = Column(String(100))  # What dataset type does it produce?
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    executions = relationship("AlgorithmExecution", back_populates="algorithm")
    
    def __repr__(self):
        return f"<Algorithm(node_name='{self.node_name}')>"


class AlgorithmExecution(Base):
    """Runtime algorithm executions with UUIDs"""
    __tablename__ = 'algorithm_executions'
    
    execution_id = Column(String(36), primary_key=True)  # UUID from backend
    algorithm_id = Column(Integer, ForeignKey('algorithms.id'), nullable=False)
    node_name = Column(String(100), nullable=False, index=True)
    status = Column(String(20), nullable=False)  # success, failed, running
    parameters = Column(JSON)  # Input parameters
    execution_time = Column(Float)  # Seconds
    error_message = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Airflow metadata (optional)
    dag_run_id = Column(String(250))
    task_id = Column(String(250))
    
    # Relationships
    algorithm = relationship("Algorithm", back_populates="executions")
    output_datasets = relationship("Dataset", back_populates="execution", foreign_keys="Dataset.execution_id")
    
    def __repr__(self):
        return f"<Execution(id='{self.execution_id[:8]}...', node='{self.node_name}', status='{self.status}')>"


class Dataset(Base):
    """Dataset instances with embedded dependency tracking"""
    __tablename__ = 'datasets'
    
    dataset_uuid = Column(String(36), primary_key=True)  # UUID from backend
    execution_id = Column(String(36), ForeignKey('algorithm_executions.execution_id'), nullable=True)  # Null for pre-existing
    
    # Dataset metadata
    dataset_type = Column(String(100), nullable=False, index=True)
    version = Column(String(20), default="1")
    parameters = Column(JSON)
    asset_ids = Column(JSON)  # List of GEE asset paths
    location = Column(String(200))
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    is_prepopulated = Column(Boolean, default=False)
    
    # ✨ DEPENDENCY TRACKING (embedded in this table)
    input_dataset_uuid = Column(String(36), ForeignKey('datasets.dataset_uuid'), nullable=True)  # What dataset was used as input?
    
    # Relationships
    execution = relationship("AlgorithmExecution", back_populates="output_datasets", foreign_keys=[execution_id])
    input_dataset = relationship("Dataset", remote_side=[dataset_uuid], foreign_keys=[input_dataset_uuid])  # Self-referential
    
    def __repr__(self):
        return f"<Dataset(uuid='{self.dataset_uuid[:8]}...', type='{self.dataset_type}')>"


def create_database(db_path='stacd.db'):
    """Create all tables"""
    engine = create_engine(f'sqlite:///{db_path}', echo=False)
    Base.metadata.create_all(engine)
    print(f"✅ Database created: {db_path}")
    return engine
