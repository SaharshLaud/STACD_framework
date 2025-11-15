from computing.utils.gee_utils import (
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists, 
    export_raster_asset_to_gee,
)
import ee
from computing.terrain.terrain_utils import generate_terrain_classified_raster

def terrain_raster(state, district, block):
    print("Inside terrain_raster")
    ee.Initialize(project='ee-saharshlaud')
    
    description = (
        "terrain_raster_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
    )
    
    # ✅ OUTPUT: Write to YOUR project (ee-saharshlaud)
    asset_id = get_gee_asset_path(
        state, district, block, 
        "projects/ee-saharshlaud/assets/apps/mws/"
    ) + description
    
    if not is_gee_asset_exists(asset_id):
        # ✅ INPUT: Read from SHARED project (ee-corestackdev) - FIXED!
        roi_boundary = ee.FeatureCollection(
            get_gee_asset_path(
                state, district, block, 
                asset_path="projects/ee-corestackdev/assets/apps/mws/"  # ✅ CHANGED THIS LINE!
            )
            + "filtered_mws_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_uid"
        )
        
        mwsheds_lf_rasters = ee.ImageCollection(
            roi_boundary.map(generate_terrain_classified_raster)
        )
        
        mwsheds_lf_raster = mwsheds_lf_rasters.mosaic()
        
        task_id = export_raster_asset_to_gee(
            image=mwsheds_lf_raster.clip(roi_boundary.geometry()),
            description=description,
            asset_id=asset_id,
            scale=30,
            region=roi_boundary.geometry(),
        )
        
        task_id_list = check_task_status([task_id])
        print("terrain_raster task_id_list", task_id_list)
    
    return asset_id
