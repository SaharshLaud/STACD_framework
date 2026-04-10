"""
STACD Builder - Generates STACD-compliant catalogs for DAGs and Algorithms
Separate from STAC catalog which handles datasets
"""
import json
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
from config import (
    CATALOG_BASE_URL, STACD_BROWSER_URL, STACD_EXTENSION_SCHEMA,
    STACD_CATALOG_CONFIG, DATETIME_FORMAT
)

class STACDBuilder:
    """Builds STACD catalog structure for DAGs and Algorithms"""
    
    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir) / "dags"
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def build_stacd_root_catalog(self, dags: List[Dict]) -> Dict:
        """Build root STACD catalog listing all DAGs"""
        catalog = {
            "stac_version": STACD_CATALOG_CONFIG["stac_version"],
            "type": STACD_CATALOG_CONFIG["type"],
            "id": STACD_CATALOG_CONFIG["id"],
            "title": STACD_CATALOG_CONFIG["title"],
            "description": STACD_CATALOG_CONFIG["description"],
            "links": [
                {
                    "rel": "self",
                    "href": f"{CATALOG_BASE_URL}/dags/catalog.json",
                    "type": "application/json"
                },
                {
                    "rel": "root",
                    "href": f"{CATALOG_BASE_URL}/catalog.json",
                    "type": "application/json"
                }
            ]
        }

        # Add child links for each DAG
        for dag in dags:
            version = dag.get('version', '1')
            versioned_id = f"{dag['dag_id']}_v{version}"
            catalog["links"].append({
                "rel": "child",
                "href": f"./{versioned_id}/dag.json",
                "type": "application/json",
                "title": f"{dag['name']} (v{version})"
            })

        

        
        return catalog
    



    def build_dag_catalog(self, dag: Dict, algorithms: List[Dict], executions: List[Dict]) -> Dict:
        """Build individual DAG catalog with structure and algorithm links"""
        
        dag_structure = dag.get('structure', {})
        version = str(int(float(dag.get('version', 1))))

        versioned_id = f"{dag['dag_id']}_v{version}"

        nodes = []
        edges = []
        
        for algo_id in dag_structure.get('alg_type_nodes', []):
            nodes.append({"id": algo_id, "type": "algorithm", "label": algo_id})
        
        for ds_id in dag_structure.get('dataset_type_nodes', []):
            nodes.append({"id": ds_id, "type": "dataset", "label": ds_id})
        
        for algo in algorithms:
            algo_id = algo['algo_type_id']
            for input_ds in algo.get('input_datasets', []):
                edges.append({"source": input_ds, "target": algo_id, "type": "consumes"})
            for output_ds in algo.get('outputs', []):
                edges.append({"source": algo_id, "target": output_ds, "type": "produces"})
        
        dag_catalog = {
            "stac_version": "1.0.0",
            "stac_extensions": [STACD_EXTENSION_SCHEMA],
            "type": "Catalog",
            "id": versioned_id,
            "title": f"{dag['name']} (v{version})",
            "description": dag.get('description', ''),
            "stacd:type": "dag",
            "stacd:uuid": dag['dag_uuid'],
            "stacd:dag_id": dag['dag_id'],
            "stacd:version": version,
            "stacd:created_at": dag.get('created_at'),
            "stacd:graph": {"nodes": nodes, "edges": edges},
            "stacd:parameters": dag_structure.get('params', []),
            "stacd:execution_count": len(executions),
            "links": [
                {
                    "rel": "self",
                    "href": f"{CATALOG_BASE_URL}/dags/{versioned_id}/dag.json",
                    "type": "application/json"
                },
                {
                    "rel": "parent",
                    "href": f"{CATALOG_BASE_URL}/dags/catalog.json",
                    "type": "application/json"
                },
                {
                    "rel": "root",
                    "href": f"{CATALOG_BASE_URL}/catalog.json",
                    "type": "application/json"
                },
                {
                    "rel": "child",
                    "href": "./algorithms/catalog.json",
                    "type": "application/json",
                    "title": "Algorithms"
                }
            ]
        }
        
        return dag_catalog


    def build_algorithms_catalog(self, dag_id: str, algorithms: List[Dict], version: str = '1') -> Dict:
        """Build algorithms catalog for a DAG version"""
        versioned_id = f"{dag_id}_v{version}"
        catalog = {
            "stac_version": "1.0.0",
            "stac_extensions": [STACD_EXTENSION_SCHEMA],
            "type": "Catalog",
            "id": f"{versioned_id}_algorithms",
            "title": "Algorithms",
            "description": f"Algorithms used in {dag_id} DAG v{version}",
            "stacd:type": "algorithm_catalog",
            "links": [
                {
                    "rel": "self",
                    "href": f"{CATALOG_BASE_URL}/dags/{versioned_id}/algorithms/catalog.json",
                    "type": "application/json"
                },
                {
                    "rel": "parent",
                    "href": f"{CATALOG_BASE_URL}/dags/{versioned_id}/dag.json",
                    "type": "application/json"
                },
                {
                    "rel": "root",
                    "href": f"{CATALOG_BASE_URL}/catalog.json",
                    "type": "application/json"
                }
            ]
        }
        
        for algo in algorithms:
            catalog["links"].append({
                "rel": "child",
                "href": f"./{algo['algo_type_id']}/algorithm.json",
                "type": "application/json",
                "title": algo['name']
            })
        
        return catalog



    def build_algorithm_catalog(self, dag_id: str, algorithm: Dict, versions: List[Dict], dag_version: str = '1') -> Dict:
        """Build individual algorithm catalog with versions"""
        versioned_dag_id = f"{dag_id}_v{dag_version}"
        
        catalog = {
            "stac_version": "1.0.0",
            "stac_extensions": [STACD_EXTENSION_SCHEMA],
            "type": "Catalog",
            "id": algorithm['algo_type_id'],
            "title": algorithm['name'],
            "description": algorithm.get('description', ''),
            "stacd:type": "algorithm",
            "stacd:dag_id": dag_id,
            "stacd:dag_version": dag_version,
            "stacd:parameters": algorithm.get('parameters', {}),
            "stacd:input_datasets": algorithm.get('input_datasets', []),
            "stacd:output_datasets": algorithm.get('outputs', []),
            "stacd:version_count": len(versions),
            "stacd:active_version": next((v['version'] for v in versions if v.get('is_active')), None),
            "links": [
                {
                    "rel": "self",
                    "href": f"{CATALOG_BASE_URL}/dags/{versioned_dag_id}/algorithms/{algorithm['algo_type_id']}/algorithm.json",
                    "type": "application/json"
                },
                {
                    "rel": "parent",
                    "href": f"{CATALOG_BASE_URL}/dags/{versioned_dag_id}/algorithms/catalog.json",
                    "type": "application/json"
                },
                {
                    "rel": "root",
                    "href": f"{CATALOG_BASE_URL}/catalog.json",
                    "type": "application/json"
                }
            ]
        }
        
        # Add version items
        for version in versions:
            catalog["links"].append({
                "rel": "item",
                "href": f"./versions/v{version['version']}.json",
                "type": "application/json",
                "title": f"Version {version['version']} {'(active)' if version.get('is_active') else ''}"
            })
        
        return catalog


    def build_algorithm_version(self, dag_id: str, algo_type_id: str, version_data: Dict, dag_version: str = '1') -> Dict:
        """Build algorithm version item"""
        versioned_dag_id = f"{dag_id}_v{dag_version}"
        
        exec_modes = version_data.get('execution_modes', {})
        if isinstance(exec_modes, str):
            exec_modes = json.loads(exec_modes)
        
        item = {
            "stac_version": "1.0.0",
            "stac_extensions": [STACD_EXTENSION_SCHEMA],
            "type": "Feature",
            "id": f"{algo_type_id}_v{version_data['version']}",
            "geometry": None,
            "properties": {
                "datetime": version_data.get('date_registered', datetime.now().isoformat() + 'Z'),
                "title": f"{algo_type_id} v{version_data['version']}",
                "stacd:algorithm_id": algo_type_id,
                "stacd:dag_id": dag_id,
                "stacd:dag_version": dag_version,
                "stacd:version": version_data['version'],
                "stacd:is_active": version_data.get('is_active', False),
                "stacd:execution_modes": exec_modes,
                "stacd:assets": version_data.get('assets', {}),
                "stacd:registered_at": version_data.get('date_registered')
            },
            "links": [
                {
                    "rel": "self",
                    "href": f"{CATALOG_BASE_URL}/dags/{versioned_dag_id}/algorithms/{algo_type_id}/versions/v{version_data['version']}.json",
                    "type": "application/json"
                },
                {
                    "rel": "parent",
                    "href": f"{CATALOG_BASE_URL}/dags/{versioned_dag_id}/algorithms/{algo_type_id}/algorithm.json",
                    "type": "application/json"
                },
                {
                    "rel": "collection",
                    "href": f"{CATALOG_BASE_URL}/dags/{versioned_dag_id}/algorithms/{algo_type_id}/algorithm.json",
                    "type": "application/json"
                }
            ]
        }
        
        return item



    
    def write_json(self, data: Dict, filepath: Path):
        """Write JSON file with pretty formatting"""
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"  ✓ {filepath.relative_to(self.output_dir.parent)}")
    