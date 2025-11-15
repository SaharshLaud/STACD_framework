"""
Initialize STACD Database
Creates tables and populates registry with existing algorithms/datasets
"""

from db_operations import STACDDatabase


def initialize_database():
    """Initialize database and populate registries"""
    
    print("="*60)
    print("Initializing STACD Database")
    print("="*60)
    
    db = STACDDatabase('stacd.db')
    
    # ========================================================================
    # Register Algorithm Types (from stacd_algorithm_repo.yaml)
    # ========================================================================
    
    print("\n📝 Registering Algorithm Types...")
    
    algorithms = [
        {
            'algo_name': 'LULC_Algorithm',
            'version': '1.0',
            'api_url': 'http://localhost:8000/api/v1/lulc_v3/',
            'params_schema': {'state': 'string', 'district': 'string', 'block': 'string', 
                             'start_year': 'int', 'end_year': 'int'},
            'description': 'LULC river basin algorithm'
        },
        {
            'algo_name': 'LULC_Vectorization',
            'version': '1.0',
            'api_url': 'http://localhost:8000/api/v1/lulc_vector/',
            'params_schema': {'state': 'string', 'district': 'string', 'block': 'string', 
                             'start_year': 'int', 'end_year': 'int'},
            'description': 'LULC vectorization algorithm'
        },
        {
            'algo_name': 'Terrain_Algorithm',
            'version': '1.0',
            'api_url': 'http://localhost:8000/api/v1/generate_terrain_raster/',
            'params_schema': {'state': 'string', 'district': 'string', 'block': 'string'},
            'description': 'Terrain raster generation'
        },
        {
            'algo_name': 'Terrain_Vectorization',
            'version': '1.0',
            'api_url': 'http://localhost:8000/api/v1/generate_terrain_clusters/',
            'params_schema': {'state': 'string', 'district': 'string', 'block': 'string'},
            'description': 'Terrain cluster generation'
        },
        {
            'algo_name': 'Terrain_LULC_Slope',
            'version': '1.0',
            'api_url': 'http://localhost:8000/api/v1/lulc_on_slope_cluster/',
            'params_schema': {'state': 'string', 'district': 'string', 'block': 'string', 
                             'start_year': 'int', 'end_year': 'int'},
            'description': 'LULC on slope cluster'
        },
        {
            'algo_name': 'Terrain_LULC_Plain',
            'version': '1.0',
            'api_url': 'http://localhost:8000/api/v1/lulc_on_plain_cluster/',
            'params_schema': {'state': 'string', 'district': 'string', 'block': 'string', 
                             'start_year': 'int', 'end_year': 'int'},
            'description': 'LULC on plain cluster'
        }
    ]
    
    for algo in algorithms:
        db.register_algorithm_type(**algo)
        print(f"  ✓ {algo['algo_name']} v{algo['version']}")
    
    # ========================================================================
    # Register Dataset Types
    # ========================================================================
    
    print("\n📊 Registering Dataset Types...")
    
    datasets = [
        {'dataset_name': 'LULC_Raster', 'version': '1.0', 'produced_by_algo': 'LULC_Algorithm'},
        {'dataset_name': 'LULC_Vector', 'version': '1.0', 'produced_by_algo': 'LULC_Vectorization'},
        {'dataset_name': 'Terrain_Raster', 'version': '1.0', 'produced_by_algo': 'Terrain_Algorithm'},
        {'dataset_name': 'Terrain_Vector', 'version': '1.0', 'produced_by_algo': 'Terrain_Vectorization'},
        {'dataset_name': 'Terrain_LULC_Vector_Slope', 'version': '1.0', 'produced_by_algo': 'Terrain_LULC_Slope'},
        {'dataset_name': 'Terrain_LULC_Vector_Plain', 'version': '1.0', 'produced_by_algo': 'Terrain_LULC_Plain'}
    ]
    
    for ds in datasets:
        db.register_dataset_type(**ds)
        print(f"  ✓ {ds['dataset_name']} v{ds['version']}")
    
    db.close()
    
    print("\n✅ Database initialized successfully!")
    print(f"   Location: stacd.db")
    print("="*60)


if __name__ == '__main__':
    initialize_database()
