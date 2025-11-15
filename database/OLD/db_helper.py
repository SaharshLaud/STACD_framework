"""
Database helper for STACD Backend
Provides simple functions to log executions and datasets
"""
import sys
import os

# Add stacd_database to path (works from anywhere)
STACD_DB_DIR = os.path.expanduser('~/stacd_database')
if STACD_DB_DIR not in sys.path:
    sys.path.insert(0, STACD_DB_DIR)

from db_operations import STACDDatabase

# Database path
DB_PATH = os.path.join(STACD_DB_DIR, 'stacd.db')

def log_algorithm_execution(execution_id, node_name, status, parameters, 
                           execution_time=None, error_message=None):
    """
    Log algorithm execution to database
    
    Args:
        execution_id: UUID string
        node_name: Algorithm node name (e.g., "lulc_river_basin")
        status: "success" or "failed"
        parameters: Dict of input parameters
        execution_time: Execution time in seconds (optional)
        error_message: Error message if failed (optional)
    
    Returns:
        AlgorithmExecution object
    """
    try:
        db = STACDDatabase(DB_PATH)
        execution = db.log_execution(
            execution_id=execution_id,
            node_name=node_name,
            status=status,
            parameters=parameters,
            execution_time=execution_time,
            error_message=error_message
        )
        db.close()
        print(f"✅ Logged execution to DB: {execution_id[:8]}... ({status})")
        return execution
    except Exception as e:
        print(f"❌ Database logging failed: {e}")
        # Don't crash the API if database fails
        return None


def log_dataset(dataset_uuid, execution_id, dataset_type, version, 
               parameters, asset_ids, location, input_dataset_uuid=None):
    """
    Log dataset creation to database
    
    Args:
        dataset_uuid: UUID string for the dataset
        execution_id: UUID of the execution that created this dataset
        dataset_type: Type of dataset (e.g., "LULC_Raster")
        version: Dataset version
        parameters: Dict of creation parameters
        asset_ids: List of GEE asset IDs
        location: Location string (e.g., "jharkhand/dumka/masalia")
        input_dataset_uuid: UUID of input dataset if applicable (optional)
    
    Returns:
        Dataset object
    """
    try:
        db = STACDDatabase(DB_PATH)
        dataset = db.add_dataset(
            dataset_uuid=dataset_uuid,
            execution_id=execution_id,
            dataset_type=dataset_type,
            version=version,
            parameters=parameters,
            asset_ids=asset_ids,
            location=location,
            input_dataset_uuid=input_dataset_uuid,
            is_prepopulated=False
        )
        db.close()
        print(f"✅ Logged dataset to DB: {dataset_uuid[:8]}... ({dataset_type})")
        return dataset
    except Exception as e:
        print(f"❌ Dataset logging failed: {e}")
        return None
