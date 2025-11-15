"""
Initialize STACD Database
"""
from models import create_database
from db_operations import STACDDatabase
import uuid

def init_stacd_database(db_path='stacd.db'):
    """Initialize database with algorithms"""
    
    print("🔧 Creating database...")
    create_database(db_path)
    
    db = STACDDatabase(db_path)
    
    print("📝 Adding algorithms...")
    
    algorithms = [
        {
            "node_name": "lulc_river_basin",
            "display_name": "LULC River Basin Clipping",
            "description": "Clips LULC data to river basin boundaries",
            "output_type": "LULC_Raster"
        },
        {
            "node_name": "vectorise_lulc",
            "display_name": "LULC Vectorization",
            "description": "Converts raster LULC to vector format",
            "output_type": "LULC_Vector"
        },
        {
            "node_name": "terrain_raster",
            "display_name": "Terrain Raster Generation",
            "description": "Generates terrain raster from DEM",
            "output_type": "Terrain_Raster"
        },
        {
            "node_name": "generate_terrain_clusters",
            "display_name": "Terrain Clustering",
            "description": "Generates terrain clusters",
            "output_type": "Terrain_Clusters"
        },
        {
            "node_name": "lulc_on_slope_cluster",
            "display_name": "LULC × Terrain Slope Analysis",
            "description": "Analyzes LULC on slope clusters",
            "output_type": "LULCxTerrain_Slope"
        },
        {
            "node_name": "lulc_on_plain_cluster",
            "display_name": "LULC × Terrain Plain Analysis",
            "description": "Analyzes LULC on plain clusters",
            "output_type": "LULCxTerrain_Plain"
        }
    ]
    
    for algo_data in algorithms:
        db.add_algorithm(**algo_data)
        print(f"  ✅ {algo_data['node_name']}")
    
    db.close()
    print(f"\n🎉 Database ready: {db_path}")
    print("\nNext steps:")
    print("1. Test database with: python test_db.py")
    print("2. Integrate with backend")
    print("3. Integrate with DAG")

if __name__ == "__main__":
    init_stacd_database('stacd.db')
