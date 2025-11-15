from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import os
import sys
import time
import ee
from datetime import datetime
import asyncio

# Add paths
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.expanduser('~/stacd_database'))

# Import algorithms
from computing.lulc.lulc_v3_clip_river_basin import lulc_river_basin
from computing.lulc.lulc_vector import vectorise_lulc
from computing.terrain.terrain_raster import terrain_raster
from computing.terrain.terrain_clusters import generate_terrain_clusters
from computing.lulcxterrain.lulc_on_slope_cluster import lulc_on_slope_cluster
from computing.lulcxterrain.lulc_on_plain_cluster import lulc_on_plain_cluster

# Initialize FastAPI
app = FastAPI(
    title="STACD Backend API",
    description="STACD-compliant REST API for geospatial workflows",
    version="2.0.0"
)

# ============================================================================
# REQUEST MODELS (with execution_id from Airflow)
# ============================================================================

class LULCRequest(BaseModel):
    execution_id: str  # NEW: UUID from Airflow
    state: str
    district: str
    block: str
    start_year: int
    end_year: int

class LULCVectorRequest(BaseModel):
    execution_id: str  # NEW: UUID from Airflow
    state: str
    district: str
    block: str
    start_year: int
    end_year: int

class TerrainRequest(BaseModel):
    execution_id: str  # NEW: UUID from Airflow
    state: str
    district: str
    block: str

class TerrainClustersRequest(BaseModel):
    execution_id: str  # NEW: UUID from Airflow
    state: str
    district: str
    block: str

class LULCxTerrainRequest(BaseModel):
    execution_id: str  # NEW: UUID from Airflow
    state: str
    district: str
    block: str
    start_year: int
    end_year: int

# ============================================================================
# RESPONSE MODEL (Simplified - no dataset info)
# ============================================================================

class STACDResponse(BaseModel):
    status: str  # "success" or "failed"
    message: str
    execution_id: str  # Echo back the UUID
    node_type: str  # Algorithm node name
    asset_ids: Optional[List[str]] = None  # GEE asset paths
    execution_time: float = 0.0

# ============================================================================
# ROOT ENDPOINT
# ============================================================================

@app.get("/")
async def root():
    return {
        "service": "STACD Backend API",
        "version": "2.0.0",
        "status": "running"
    }

@app.get("/health")
async def health():
    return {"status": "healthy"}

# ============================================================================
# ALGORITHM ENDPOINTS (Refactored - No Dataset Logging)
# ============================================================================

@app.post("/api/v1/lulc_v3/", response_model=STACDResponse)
async def execute_lulc_algorithm(request: LULCRequest):
    """
    LULC River Basin Algorithm
    Blocks until GEE assets are created
    """
    execution_id = request.execution_id
    node_type = "lulc_river_basin"
    start_time = time.time()
    
    try:        
        print(f"\n[{execution_id}] Starting LULC Algorithm")
        
        # Initialize GEE
        try:
            ee.Initialize(project='ee-saharshlaud')
        except:
            pass
        
        print(f"[{execution_id}] Calling lulc_river_basin()...")
        
        # Execute algorithm
        result = lulc_river_basin(
            request.state, request.district, request.block,
            request.start_year, request.end_year
        )
        
        # Build expected asset paths (for polling)
        state_lower = request.state.lower()
        district_lower = request.district.lower()
        block_lower = request.block.lower()
        
        expected_assets = []
        for year in range(request.start_year, request.end_year + 1):
            asset_path = (
                f"projects/ee-saharshlaud/assets/apps/mws/"
                f"{state_lower}/{district_lower}/{block_lower}/"
                f"{district_lower}_{block_lower}_{year}-07-01_{year + 1}-06-30_LULCmap_10m"
            )
            expected_assets.append(asset_path)
        
        # Poll for assets
        max_wait_time = 7200  # 2 hours
        check_interval = 120  # 2 minutes
        elapsed = 0
        poll_count = 0
        
        while elapsed < max_wait_time:
            poll_count += 1
            all_exist = True
            
            for asset_path in expected_assets:
                try:
                    ee.data.getAsset(asset_path)
                except:
                    all_exist = False
                    break
            
            print(f"[{execution_id}] Poll #{poll_count}: Checking {len(expected_assets)} assets...")
            
            if all_exist:
                execution_time = time.time() - start_time
                print(f"[{execution_id}]  SUCCESS: All assets created in {execution_time:.1f}s")
                
                
                # Return ONLY asset IDs (Airflow will log datasets)
                return STACDResponse(
                    status="success",
                    message=f"LULC algorithm completed - {len(expected_assets)} assets created",
                    execution_id=execution_id,
                    node_type=node_type,
                    asset_ids=expected_assets,
                    execution_time=execution_time
                )
            
            await asyncio.sleep(check_interval)
            elapsed += check_interval
        
        raise Exception(f"Timeout: Assets not created after {max_wait_time}s")
        
    except Exception as e:
        execution_time = time.time() - start_time
        error_msg = str(e)
        
        print(f"[{execution_id}]  FAILED: {error_msg}")
        

        
        return STACDResponse(
            status="failed",
            message=f"LULC algorithm failed: {error_msg}",
            execution_id=execution_id,
            node_type=node_type,
            execution_time=execution_time
        )


@app.post("/api/v1/lulc_vector/", response_model=STACDResponse)
async def execute_lulc_vector_algorithm(request: LULCVectorRequest):
    """
    LULC Vectorization Algorithm
    Blocks until vector assets are created
    """
    execution_id = request.execution_id
    node_type = "vectorise_lulc"
    start_time = time.time()
    
    try:

        
        print(f"\n[{execution_id}] Starting LULC Vector Algorithm")
        
        # Initialize GEE
        try:
            ee.Initialize(project='ee-saharshlaud')
        except:
            pass
        
        print(f"[{execution_id}] Calling vectorise_lulc()...")
        
        # Execute algorithm
        asset_id = vectorise_lulc(
            request.state, request.district, request.block,
            request.start_year, request.end_year
        )
        
        execution_time = time.time() - start_time
        print(f"[{execution_id}]  SUCCESS: Vector created in {execution_time:.1f}s")
        

        return STACDResponse(
            status="success",
            message="LULC vectorization completed",
            execution_id=execution_id,
            node_type=node_type,
            asset_ids=[asset_id],
            execution_time=execution_time
        )
        
    except Exception as e:
        execution_time = time.time() - start_time
        error_msg = str(e)
        
        print(f"[{execution_id}]  FAILED: {error_msg}")
        

        
        return STACDResponse(
            status="failed",
            message=f"LULC vectorization failed: {error_msg}",
            execution_id=execution_id,
            node_type=node_type,
            execution_time=execution_time
        )


@app.post("/api/v1/generate_terrain_raster/", response_model=STACDResponse)
async def execute_terrain_raster_algorithm(request: TerrainRequest):
    """
    Terrain Raster Generation Algorithm
    Blocks until terrain raster is created
    """
    execution_id = request.execution_id
    node_type = "terrain_raster"
    start_time = time.time()
    
    try:

        
        print(f"\n[{execution_id}] Starting Terrain Raster Algorithm")
        
        # Initialize GEE
        try:
            ee.Initialize(project='ee-saharshlaud')
        except:
            pass
        
        print(f"[{execution_id}] Calling terrain_raster()...")
        
        # Execute algorithm
        asset_id = terrain_raster(request.state, request.district, request.block)
        
        execution_time = time.time() - start_time
        print(f"[{execution_id}]  SUCCESS: Terrain raster created in {execution_time:.1f}s")

        
        return STACDResponse(
            status="success",
            message="Terrain raster generation completed",
            execution_id=execution_id,
            node_type=node_type,
            asset_ids=[asset_id],
            execution_time=execution_time
        )
        
    except Exception as e:
        execution_time = time.time() - start_time
        error_msg = str(e)
        
        print(f"[{execution_id}]  FAILED: {error_msg}")
        

        return STACDResponse(
            status="failed",
            message=f"Terrain raster generation failed: {error_msg}",
            execution_id=execution_id,
            node_type=node_type,
            execution_time=execution_time
        )


@app.post("/api/v1/generate_terrain_clusters/", response_model=STACDResponse)
async def execute_terrain_clusters_algorithm(request: TerrainClustersRequest):
    """
    Terrain Clustering Algorithm
    Blocks until terrain clusters are created
    """
    execution_id = request.execution_id
    node_type = "generate_terrain_clusters"
    start_time = time.time()
    
    try:

        
        print(f"\n[{execution_id}] Starting Terrain Clusters Algorithm")
        
        # Initialize GEE
        try:
            ee.Initialize(project='ee-saharshlaud')
        except:
            pass
        
        print(f"[{execution_id}] Calling generate_terrain_clusters()...")
        
        # Execute algorithm
        asset_id = generate_terrain_clusters(request.state, request.district, request.block)
        
        execution_time = time.time() - start_time
        print(f"[{execution_id}]  SUCCESS: Terrain clusters created in {execution_time:.1f}s")
        
   
        
        return STACDResponse(
            status="success",
            message="Terrain clustering completed",
            execution_id=execution_id,
            node_type=node_type,
            asset_ids=[asset_id],
            execution_time=execution_time
        )
        
    except Exception as e:
        execution_time = time.time() - start_time
        error_msg = str(e)
        
        print(f"[{execution_id}]  FAILED: {error_msg}")
        

        
        return STACDResponse(
            status="failed",
            message=f"Terrain clustering failed: {error_msg}",
            execution_id=execution_id,
            node_type=node_type,
            execution_time=execution_time
        )


@app.post("/api/v1/lulc_on_slope_cluster/", response_model=STACDResponse)
async def execute_lulc_slope_algorithm(request: LULCxTerrainRequest):
    """
    LULC×Terrain Slope Clustering Algorithm
    Blocks until slope clusters are created
    """
    execution_id = request.execution_id
    node_type = "lulc_on_slope_cluster"
    start_time = time.time()
    
    try:

        print(f"\n[{execution_id}] Starting LULC×Terrain Slope Algorithm")
        
        # Initialize GEE
        try:
            ee.Initialize(project='ee-saharshlaud')
        except:
            pass
        
        print(f"[{execution_id}] Calling lulc_on_slope_cluster()...")
        
        # Execute algorithm
        asset_id = lulc_on_slope_cluster(
            request.state, request.district, request.block,
            request.start_year, request.end_year
        )
        
        execution_time = time.time() - start_time
        print(f"[{execution_id}]  SUCCESS: Slope clusters created in {execution_time:.1f}s")
        

        
        return STACDResponse(
            status="success",
            message="LULC×Terrain slope clustering completed",
            execution_id=execution_id,
            node_type=node_type,
            asset_ids=[asset_id],
            execution_time=execution_time
        )
        
    except Exception as e:
        execution_time = time.time() - start_time
        error_msg = str(e)
        
        print(f"[{execution_id}]  FAILED: {error_msg}")
        

        return STACDResponse(
            status="failed",
            message=f"LULC×Terrain slope clustering failed: {error_msg}",
            execution_id=execution_id,
            node_type=node_type,
            execution_time=execution_time
        )


@app.post("/api/v1/lulc_on_plain_cluster/", response_model=STACDResponse)
async def execute_lulc_plain_algorithm(request: LULCxTerrainRequest):
    """
    LULC×Terrain Plain Clustering Algorithm
    Blocks until plain clusters are created
    """
    execution_id = request.execution_id
    node_type = "lulc_on_plain_cluster"
    start_time = time.time()
    
    try:

        
        print(f"\n[{execution_id}] Starting LULC×Terrain Plain Algorithm")
        
        # Initialize GEE
        try:
            ee.Initialize(project='ee-saharshlaud')
        except:
            pass
        
        print(f"[{execution_id}] Calling lulc_on_plain_cluster()...")
        
        # Execute algorithm
        asset_id = lulc_on_plain_cluster(
            request.state, request.district, request.block,
            request.start_year, request.end_year
        )
        
        execution_time = time.time() - start_time
        print(f"[{execution_id}]  SUCCESS: Plain clusters created in {execution_time:.1f}s")
        

        
        return STACDResponse(
            status="success",
            message="LULC×Terrain plain clustering completed",
            execution_id=execution_id,
            node_type=node_type,
            asset_ids=[asset_id],
            execution_time=execution_time
        )
        
    except Exception as e:
        execution_time = time.time() - start_time
        error_msg = str(e)
        
        print(f"[{execution_id}]  FAILED: {error_msg}")

        
        return STACDResponse(
            status="failed",
            message=f"LULC×Terrain plain clustering failed: {error_msg}",
            execution_id=execution_id,
            node_type=node_type,
            execution_time=execution_time
        )


# ============================================================================
# RUN SERVER
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
