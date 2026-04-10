"""
STAC Builder - Generates STAC JSON structures from STACD database
"""
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from config import (
    CATALOG_BASE_URL,
    GEE_BASE_URL,
    STACD_EXTENSION_SCHEMA,
    DATASET_TYPE_TITLES,
    DATASET_TYPE_ROLES,
    SPATIAL_EXTENTS,
    DEFAULT_BBOX,
    DATETIME_FORMAT,
    STAC_CATALOG_CONFIG,  # Changed from CATALOG_CONFIG
    STACD_BROWSER_URL
)

class STACBuilder:
    """Builds STAC catalog structure from STACD database entries"""
    
    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def parse_asset_location(self, asset_id: str) -> Optional[Dict[str, str]]:
        """
        Extract location hierarchy from asset_id
        Example: 'projects/ee-saharshlaud/assets/apps/mws/jharkhand/dumka/masalia/...'
        Returns: {'state': 'jharkhand', 'district': 'dumka', 'block': 'masalia'}
        """
        # Pattern to extract state/district/block from asset path
        pattern = r'/([^/]+)/([^/]+)/([^/]+)/[^/]+$'
        match = re.search(pattern, asset_id)
        if match:
            return {
                'state': match.group(1),
                'district': match.group(2),
                'block': match.group(3)
            }
        
        # Fallback: try simpler pattern
        pattern_simple = r'/(jharkhand|bihar|odisha|[a-z]+)/([a-z]+)/([a-z]+)/'
        match = re.search(pattern_simple, asset_id, re.IGNORECASE)
        if match:
            return {
                'state': match.group(1).lower(),
                'district': match.group(2).lower(),
                'block': match.group(3).lower()
            }
        
        return None
    
    def parse_year_from_asset(self, asset_id: str) -> Optional[Dict[str, str]]:
        """
        Extract year range from asset_id
        Example: 'dumka_masalia_2017-07-01_2018-06-30_LULCmap_10m'
        Returns: {'start': '2017', 'end': '2018'}
        """
        pattern = r'(\d{4})-\d{2}-\d{2}_(\d{4})-\d{2}-\d{2}'
        match = re.search(pattern, asset_id)
        if match:
            return {
                'start': match.group(1),
                'end': match.group(2)
            }
        return None
    
    def get_spatial_extent(self, state: str, district: str, block: str) -> Dict:
        """Get bbox and geometry for a location"""
        try:
            extent = SPATIAL_EXTENTS.get(state, {}).get(district, {}).get(block, {})
            if extent:
                return extent
        except:
            pass
        
        # Return default
        return {
            "bbox": DEFAULT_BBOX,
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [DEFAULT_BBOX[0], DEFAULT_BBOX[1]],
                    [DEFAULT_BBOX[2], DEFAULT_BBOX[1]],
                    [DEFAULT_BBOX[2], DEFAULT_BBOX[3]],
                    [DEFAULT_BBOX[0], DEFAULT_BBOX[3]],
                    [DEFAULT_BBOX[0], DEFAULT_BBOX[1]]
                ]]
            }
        }
    
    def build_root_catalog(self) -> Dict:
        """Build root STAC catalog for datasets"""
        return {
            "stac_version": STAC_CATALOG_CONFIG["stac_version"],
            "type": STAC_CATALOG_CONFIG["type"],
            "id": STAC_CATALOG_CONFIG["id"],
            "title": STAC_CATALOG_CONFIG["title"],
            "description": STAC_CATALOG_CONFIG["description"],
            "links": [
                {
                    "rel": "self",
                    "href": f"{CATALOG_BASE_URL}/datasets/catalog.json",
                    "type": "application/json"
                },
                {
                    "rel": "root",
                    "href": f"{CATALOG_BASE_URL}/catalog.json",
                    "type": "application/json"
                }
            ]
        }
    
    def build_state_catalog(self, state: str, districts: List[str]) -> Dict:
        """Build state-level catalog"""
        catalog = {
            "stac_version": "1.0.0",
            "type": "Catalog",
            "id": state,
            "title": f"{state.title()} State",
            "description": f"Collections for {state.title()} state",
            "links": [
                {
                    "rel": "self",
                    "href": f"{CATALOG_BASE_URL}/datasets/{state}/catalog.json",
                    "type": "application/json"
                },
                {
                    "rel": "parent",
                    "href": f"{CATALOG_BASE_URL}/datasets/catalog.json",
                    "type": "application/json"
                },
                {
                    "rel": "root",
                    "href": f"{CATALOG_BASE_URL}/catalog.json",
                    "type": "application/json"
                }
            ]
        }
        
        # Add child links for each district
        for district in sorted(districts):
            catalog["links"].append({
                "rel": "child",
                "href": f"./{district}/catalog.json",
                "type": "application/json",
                "title": f"{district.title()} District"
            })
        
        return catalog
    
    def build_district_catalog(self, state: str, district: str, blocks: List[str]) -> Dict:
        """Build district-level catalog"""
        catalog = {
            "stac_version": "1.0.0",
            "type": "Catalog",
            "id": f"{state}_{district}",
            "title": f"{district.title()} District",
            "description": f"Collections for {district.title()} district in {state.title()} state",
            "links": [
                {
                    "rel": "self",
                    "href": f"{CATALOG_BASE_URL}/datasets/{state}/{district}/catalog.json",
                    "type": "application/json"
                },
                {
                    "rel": "parent",
                    "href": f"{CATALOG_BASE_URL}/datasets/{state}/catalog.json",
                    "type": "application/json"
                },
                {
                    "rel": "root",
                    "href": f"{CATALOG_BASE_URL}/catalog.json",
                    "type": "application/json"
                }
            ]
        }
        
        # Add child links for each block
        for block in sorted(blocks):
            catalog["links"].append({
                "rel": "child",
                "href": f"./{block}/collection.json",
                "type": "application/json",
                "title": f"{block.title()} Block"
            })
        
        return catalog
    
    def build_collection(self, state: str, district: str, block: str, items: List[Dict]) -> Dict:
        """Build STAC Collection for a block"""
        # Get spatial extent
        extent_info = self.get_spatial_extent(state, district, block)
        
        # Calculate temporal extent from items
        datetimes = []
        for item in items:
            if 'created_at' in item and item['created_at']:
                datetimes.append(item['created_at'])
        
        temporal_start = min(datetimes) if datetimes else "2017-01-01T00:00:00Z"
        temporal_end = max(datetimes) if datetimes else datetime.now().strftime(DATETIME_FORMAT)
        
        collection = {
            "stac_version": "1.0.0",
            "stac_extensions": [STACD_EXTENSION_SCHEMA],
            "type": "Collection",
            "id": f"{state}_{district}_{block}_workflows",
            "title": f"{block.title()} Block Geospatial Datasets",
            "description": f"STACD-generated geospatial datasets for {block.title()} block, {district.title()} district, {state.title()} state. Includes LULC, terrain classifications, and derived products with complete algorithm lineage.",
            "license": "proprietary",
            "extent": {
                "spatial": {
                    "bbox": [extent_info["bbox"]]
                },
                "temporal": {
                    "interval": [[temporal_start, temporal_end]]
                }
            },
            "links": [
                {
                    "rel": "self",
                    "href": f"{CATALOG_BASE_URL}/datasets/{state}/{district}/{block}/collection.json",
                    "type": "application/json"
                },
                {
                    "rel": "parent",
                    "href": f"{CATALOG_BASE_URL}/datasets/{state}/{district}/catalog.json",
                    "type": "application/json"
                },
                {
                    "rel": "root",
                    "href": f"{CATALOG_BASE_URL}/catalog.json",
                    "type": "application/json"
                },
                {
                    "rel": "alternate",
                    "href": f"{CATALOG_BASE_URL}/dags/catalog.json",
                    "type": "application/json",
                    "title": "View in STACD Browser (Workflows)"
                }
            ],
            "summaries": {
                "stacd:algorithms": list(set([item.get('produced_by_algo') for item in items if item.get('produced_by_algo')])),
                "stacd:dataset_types": list(set([item.get('dataset_type_id') for item in items if item.get('dataset_type_id')]))
            }
        }
        
        # Add item links
        for item in items:
            item_filename = f"{item['dataset_type_id']}_{item['instance_id']}.json"
            collection["links"].append({
                "rel": "item",
                "href": f"./{item_filename}",
                "type": "application/json"
            })
        
        return collection
    


    def build_item(self, dataset: Dict, state: str, district: str, block: str) -> Dict:
        """Build STAC Item from dataset_instance"""
        # Get spatial extent
        extent_info = self.get_spatial_extent(state, district, block)
        
        # Parse metadata (handle both string and dict)
        meta_info_raw = dataset.get('meta_info', '{}')
        if isinstance(meta_info_raw, str):
            try:
                meta_info = json.loads(meta_info_raw)
            except:
                meta_info = {}
        else:
            meta_info = meta_info_raw if meta_info_raw else {}
        
        # Parse year from asset_id
        year_info = self.parse_year_from_asset(dataset['asset_id'])
        
        # Determine datetime
        if year_info:
            datetime_str = f"{year_info['start']}-07-01T00:00:00Z"
        else:
            datetime_str = dataset.get('created_at', datetime.now().strftime(DATETIME_FORMAT))
        
        # Use dataset_type_id as clean title (e.g. "LULC_Raster", "LULC_Vector")
        dataset_type = dataset['dataset_type_id']
        clean_title = DATASET_TYPE_TITLES.get(dataset_type, dataset_type.replace('_', ' '))

        # Resolve producing algorithm and its DAG for STACD back-link
        producing_algo = dataset.get('produced_by_algo')
        dag_id         = meta_info.get('dag_id') or dataset.get('dag_id')
        dag_version    = meta_info.get('dag_version') or dataset.get('dag_version', '1')
        versioned_dag  = f"{dag_id}_v{dag_version}" if dag_id else None

        # Build STAC Item
        item = {
            "stac_version": "1.0.0",
            "stac_extensions": [STACD_EXTENSION_SCHEMA],
            "type": "Feature",
            "id": f"{state}_{district}_{block}_{dataset_type}_{dataset['instance_id']}",
            "geometry": extent_info["geometry"],
            "bbox": extent_info["bbox"],
            "properties": {
                "datetime": datetime_str,
                # FIX: clean title from dataset type, not algo name
                "title": clean_title,
                "description": f"{clean_title} for {block.title()}, {district.title()}, {state.title()}",
                "created": dataset.get('created_at'),
                "stacd:algorithm": producing_algo,
                "stacd:algo_version": dataset.get('algo_version'),
                "stacd:run_id": dataset.get('run_id'),
                "stacd:dataset_type": dataset_type,
                "stacd:instance_id": dataset.get('instance_id'),
                "stacd:dag_id": dag_id,
                "stacd:dag_version": dag_version,
                "stacd:registered_at": meta_info.get('registered_at'),
            },
            "assets": {
                "data": {
                    "href": f"{GEE_BASE_URL}{dataset['asset_id']}",
                    "type": "application/geo+json" if "Vector" in dataset_type else "image/tiff; application=geotiff; profile=cloud-optimized",
                    "title": "Primary Data Asset",
                    "roles": DATASET_TYPE_ROLES.get(dataset_type, ["data"]),
                    "gee:asset_id": dataset['asset_id']
                }
            },
            "links": [
                {
                    "rel": "self",
                    "href": f"{CATALOG_BASE_URL}/datasets/{state}/{district}/{block}/{dataset_type}_{dataset['instance_id']}.json",
                    "type": "application/json"
                },
                {
                    "rel": "parent",
                    "href": f"{CATALOG_BASE_URL}/datasets/{state}/{district}/{block}/collection.json",
                    "type": "application/json"
                },
                {
                    "rel": "collection",
                    "href": f"{CATALOG_BASE_URL}/datasets/{state}/{district}/{block}/collection.json",
                    "type": "application/json"
                },
                {
                    "rel": "root",
                    "href": f"{CATALOG_BASE_URL}/catalog.json",
                    "type": "application/json"
                }
            ]
        }
        
        # FIX: Add STACD back-link to the producing algorithm if we know the DAG
        if producing_algo and versioned_dag:
            item["links"].append({
                "rel": "derived_from",
                "href": f"{CATALOG_BASE_URL}/dags/{versioned_dag}/algorithms/{producing_algo}/algorithm.json",
                "type": "application/json",
                "title": f"Producing Algorithm: {producing_algo} (View in STACD Browser)"
            })
        
        # Add year info if available
        if year_info:
            item["properties"]["stacd:start_year"] = year_info['start']
            item["properties"]["stacd:end_year"] = year_info['end']
        
        return item


    def write_json(self, data: Dict, filepath: Path):
        """Write JSON file with pretty formatting"""
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"  ✓ {filepath.relative_to(self.output_dir.parent)}")

if __name__ == "__main__":
    print("STAC Builder module loaded successfully")
    print("Use export_catalog.py to generate the catalog")
