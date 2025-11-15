"""
GEE utilities - reads from ee-corestackdev, writes to ee-saharshlaud
"""
import ee
import time
import re

def valid_gee_text(description):
    """Clean text for GEE asset names"""
    description = re.sub(r"[^a-zA-Z0-9 .,:;_-]", "", description)
    return description.replace(" ", "_")

def get_gee_asset_path(state, district=None, block=None,
                       asset_path="projects/ee-corestackdev/assets/apps/mws/"):
    """
    Construct GEE asset path
    DEFAULT: Uses ee-corestackdev (public/shared assets for INPUT)
    """
    gee_path = asset_path + valid_gee_text(state.lower()) + "/"
    if district:
        gee_path += valid_gee_text(district.lower()) + "/"
    if block:
        gee_path += valid_gee_text(block.lower()) + "/"
    return gee_path

def is_gee_asset_exists(path):
    """Check if GEE asset exists"""
    try:
        asset = ee.data.getAsset(path)
        print(f"{path} exists")
        return True
    except:
        return False

def export_raster_asset_to_gee(image, asset_id, scale=10, region=None, 
                               description=None, pyramiding_policy=None):
    """
    Export image to GEE asset
    Writes to YOUR project (ee-saharshlaud)
    """
    try:
        export_task = ee.batch.Export.image.toAsset(
            image=image,
            assetId=asset_id,
            scale=scale,
            region=region,
            maxPixels=1e13,
            description=description or asset_id.split('/')[-1],
            pyramidingPolicy=pyramiding_policy or {}
        )
        export_task.start()
        print(f"→ Export started: {asset_id}")
        return export_task.id
    except Exception as e:
        print(f"✗ Export failed: {e}")
        raise

def check_task_status(task_ids):
    """Check status of GEE tasks"""
    if isinstance(task_ids, str):
        task_ids = [task_ids]
    
    statuses = []
    for task_id in task_ids:
        try:
            task = ee.data.getTaskStatus(task_id)[0]
            statuses.append(task)
        except Exception as e:
            print(f"Error checking task {task_id}: {e}")
    return statuses

def make_asset_public(asset_id):
    """Make asset publicly readable"""
    try:
        acl = ee.data.getAssetAcl(asset_id)
        acl["all_users_can_read"] = True
        ee.data.setAssetAcl(asset_id, acl)
        print(f"✓ Asset made public: {asset_id}")
    except Exception as e:
        print(f"⚠ Could not make asset public: {e}")

# Constants
GEE_PATHS = {
    'MWS': {
        'GEE_ASSET_PATH': 'projects/ee-saharshlaud/assets/apps/mws/'  # YOUR output path
    },
    'INPUT_PATH': 'projects/ee-corestackdev/assets/apps/mws/',  # Shared input path
}

GEE_PROJECT = 'ee-saharshlaud'
GEE_SCALE = 10
