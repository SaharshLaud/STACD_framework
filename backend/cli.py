"""
CLI wrapper for STACD backend algorithms (Docker execution)
Usage: python cli.py <algorithm_name> --state XX --district XX --block XX ...
"""

import sys
import json
import argparse
import time
import traceback
import io

# Import all algorithm functions
from computing.lulc.lulc_v3_clip_river_basin import lulc_river_basin
from computing.lulc.lulc_vector import vectorise_lulc
from computing.terrain.terrain_raster import terrain_raster
from computing.terrain.terrain_clusters import generate_terrain_clusters
from computing.lulcxterrain.lulc_on_slope_cluster import lulc_on_slope_cluster
from computing.lulcxterrain.lulc_on_plain_cluster import lulc_on_plain_cluster

# Map Airflow algorithm names to actual functions
ALGORITHMS = {
    'LULC_Algorithm': lulc_river_basin,
    'LULC_Vectorization': vectorise_lulc,
    'Terrain_Algorithm': terrain_raster,
    'Terrain_Vectorization': generate_terrain_clusters,
    'Terrain_LULC_Slope': lulc_on_slope_cluster,
    'Terrain_LULC_Plain': lulc_on_plain_cluster,
}

def main():
    parser = argparse.ArgumentParser(description='STACD Algorithm CLI')
    parser.add_argument('algorithm', choices=ALGORITHMS.keys(), help='Algorithm to run')
    parser.add_argument('--state', required=True, help='State name')
    parser.add_argument('--district', required=True, help='District name')
    parser.add_argument('--block', required=True, help='Block name')
    parser.add_argument('--start_year', type=int, help='Start year (for LULC algorithms)')
    parser.add_argument('--end_year', type=int, help='End year (for LULC algorithms)')
    parser.add_argument('--execution_id', required=True, help='Execution UUID')
    
    args = parser.parse_args()
    
    try:
        start_time = time.time()
        
        # Redirect stdout to capture algorithm prints
        old_stdout = sys.stdout
        sys.stdout = io.StringIO()
        
        try:
            # Get the correct function
            algorithm_func = ALGORITHMS[args.algorithm]
            
            # Build parameters based on algorithm type
            if args.algorithm in ['LULC_Algorithm', 'LULC_Vectorization', 'Terrain_LULC_Slope', 'Terrain_LULC_Plain']:
                result = algorithm_func(
                    state=args.state,
                    district=args.district,
                    block=args.block,
                    start_year=args.start_year,
                    end_year=args.end_year
                )
            else:
                result = algorithm_func(
                    state=args.state,
                    district=args.district,
                    block=args.block
                )
            
            # Capture algorithm output (for debugging)
            algorithm_output = sys.stdout.getvalue()
        finally:
            # Restore stdout
            sys.stdout = old_stdout
        
        execution_time = time.time() - start_time
        
        # Write algorithm debug output to stderr (visible in logs but not parsed)
        if algorithm_output.strip():
            print(algorithm_output, file=sys.stderr)
        
        # Format output for Airflow (ONLY JSON to stdout)
        output = {
            'status': 'success',
            'execution_id': args.execution_id,
            'execution_time': execution_time,
            'asset_ids': [result] if isinstance(result, str) else (result if isinstance(result, list) else []),
            'result': str(result)
        }
        
        # Print ONLY JSON to stdout (Airflow captures this)
        print(json.dumps(output))
        sys.exit(0)
        
    except Exception as e:
        error_result = {
            'status': 'failed',
            'execution_id': args.execution_id,
            'error': str(e),
            'traceback': traceback.format_exc()
        }
        print(json.dumps(error_result), file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
