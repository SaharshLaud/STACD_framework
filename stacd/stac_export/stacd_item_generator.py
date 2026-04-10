#!/usr/bin/env python3
"""
STAC-D Item Generator
Generates STAC-D items for DAGs and Algorithms when they are registered
"""

import json
from pathlib import Path
from datetime import datetime
import os
import sys

# Add paths before any local imports
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", os.path.expanduser("~/airflow"))
sys.path.insert(0, os.path.join(AIRFLOW_HOME, "stacd/database"))
sys.path.insert(0, os.path.join(AIRFLOW_HOME, "stacd/stac_export"))

from models import AlgorithmType
from config import CATALOG_BASE_URL


def generate_stacd_dag_item(dag):
    """
    Generate STAC-D item for a DAG
    Called when DAG is registered in database
    """
    from config import CATALOG_OUTPUT_DIR
    from db_operations import STACDDatabase

    version = str(int(float(dag.version))) if dag.version else '1'
    versioned_id = f"{dag.dag_id}_v{version}"

    output_dir = Path(CATALOG_OUTPUT_DIR) / "dags" / versioned_id
    output_dir.mkdir(parents=True, exist_ok=True)

    # Need a db session to build edges
    db = STACDDatabase()
    try:
        edges = _build_dag_edges(dag, db)
    finally:
        db.close()

    dag_item = {
        "stac_version": "1.0.0",
        "stac_extensions": ["https://github.com/saharsh-laud/stacd-spec/v1.0.0/schema.json"],
        "type": "Catalog",
        "id": versioned_id,
        "title": f"{dag.name} (v{version})",
        "description": dag.description or "",
        "stacd:type": "dag",
        "stacd:dag_id": dag.dag_id,
        "stacd:uuid": dag.dag_uuid,
        "stacd:version": version,
        "stacd:created_at": dag.created_at.isoformat() + "Z" if dag.created_at else None,
        "stacd:graph": {
            "nodes": _build_dag_nodes(dag.structure),
            "edges": edges
        },
        "stacd:parameters": dag.structure.get("params", {}),
        "links": [
            {"rel": "self", "href": "./dag.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            {"rel": "root", "href": "../../catalog.json", "type": "application/json"},
            {"rel": "child", "href": "./algorithms/catalog.json", "type": "application/json", "title": "Algorithms"}
        ]
    }

    dag_path = output_dir / "dag.json"
    with open(dag_path, 'w') as f:
        json.dump(dag_item, f, indent=2, ensure_ascii=False)

    print(f"✓ Config loaded")
    print(f"Generated STAC-D DAG item: {dag_path}")

    # Write algorithms/catalog.json so static server can serve it
    _write_algorithms_catalog(dag, versioned_id, output_dir, CATALOG_OUTPUT_DIR)


def _write_algorithms_catalog(dag, versioned_id, dag_output_dir, catalog_output_dir):
    """Write algorithms/catalog.json to disk for static serving"""
    algo_ids = dag.structure.get('alg_type_nodes', [])

    algo_catalog_dir = dag_output_dir / "algorithms"
    algo_catalog_dir.mkdir(parents=True, exist_ok=True)

    catalog = {
        "stac_version": "1.0.0",
        "stac_extensions": ["https://github.com/saharsh-laud/stacd-spec/v1.0.0/schema.json"],
        "type": "Catalog",
        "id": f"{versioned_id}_algorithms",
        "title": f"Algorithms - {versioned_id.replace('_', ' ').title()}",
        "description": f"All algorithms in {versioned_id}",
        "links": [
            {"rel": "self", "href": "./catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../dag.json", "type": "application/json"},
            {"rel": "root", "href": "../../../catalog.json", "type": "application/json"}
        ]
    }

    for algo_id in sorted(algo_ids):
        catalog["links"].append({
            "rel": "child",
            "href": f"./{algo_id}/algorithm.json",
            "type": "application/json",
            "title": algo_id.replace('_', ' ')
        })

    catalog_path = algo_catalog_dir / "catalog.json"
    with open(catalog_path, 'w') as f:
        json.dump(catalog, f, indent=2, ensure_ascii=False)

    print(f"✓ Written algorithms/catalog.json: {catalog_path}")


def generate_stacd_algorithm_item(algo_instance, algo_type):
    """
    Generate STAC-D item for an algorithm version
    Called when algorithm instance is registered
    """
    from config import CATALOG_OUTPUT_DIR
    from db_operations import STACDDatabase

    db = STACDDatabase()
    try:
        from models import DAG
        all_dags = db.session.query(DAG).all()

        for dag in all_dags:
            algo_type_nodes = dag.structure.get('alg_type_nodes', [])
            if algo_instance.algo_type_id not in algo_type_nodes:
                continue

            dag_version = str(int(float(dag.version))) if dag.version else '1'
            versioned_dag_id = f"{dag.dag_id}_v{dag_version}"






            # ── algorithm.json ──────────────────────────────────────────
            output_dir = Path(CATALOG_OUTPUT_DIR) / "dags" / versioned_dag_id / "algorithms" / algo_instance.algo_type_id
            output_dir.mkdir(parents=True, exist_ok=True)

            # Build STAC forward-links for input and output datasets
            stac_dataset_links = []
            for input_ds in (algo_type.input_datasets or []):
                stac_dataset_links.append({
                    "rel": "stacd:input",
                    "href": f"{CATALOG_BASE_URL}/datasets/catalog.json",
                    "type": "application/json",
                    "title": f"Input Dataset: {input_ds} (Browse in STAC Browser)",
                    "stacd:dataset_type": input_ds
                })
            for output_ds in (algo_type.outputs or []):
                stac_dataset_links.append({
                    "rel": "stacd:output",
                    "href": f"{CATALOG_BASE_URL}/datasets/catalog.json",
                    "type": "application/json",
                    "title": f"Output Dataset: {output_ds} (Browse in STAC Browser)",
                    "stacd:dataset_type": output_ds
                })

            algo_catalog = {
                "stac_version": "1.0.0",
                "stac_extensions": ["https://github.com/saharsh-laud/stacd-spec/v1.0.0/schema.json"],
                "type": "Catalog",
                "id": algo_instance.algo_type_id,
                "title": algo_type.name,
                "description": algo_type.description or "",
                "stacd:type": "algorithm",
                "stacd:dag_id": dag.dag_id,
                "stacd:dag_version": dag_version,
                "stacd:parameters": algo_type.parameters or {},
                "stacd:input_datasets": algo_type.input_datasets or [],
                "stacd:output_datasets": algo_type.outputs or [],
                "links": [
                    {"rel": "self",   "href": "./algorithm.json",        "type": "application/json"},
                    {"rel": "parent", "href": "../catalog.json",         "type": "application/json"},
                    {"rel": "root",   "href": "../../../../catalog.json","type": "application/json"},
                    # FIX: link to STAC browser datasets root so user can navigate there
                    {
                        "rel": "alternate",
                        "href": f"{CATALOG_BASE_URL}/datasets/catalog.json",
                        "type": "application/json",
                        "title": "View Output Datasets in STAC Browser"
                    },
                    *stac_dataset_links   # per-dataset typed links
                ]
            }

            algo_path = output_dir / "algorithm.json"
            with open(algo_path, 'w') as f:
                json.dump(algo_catalog, f, indent=2, ensure_ascii=False)


            # ── versions/v{n}.json ───────────────────────────────────────
            version_dir = output_dir / "versions"
            version_dir.mkdir(exist_ok=True)

            version_item = {
                "stac_version": "1.0.0",
                "stac_extensions": ["https://github.com/saharsh-laud/stacd-spec/v1.0.0/schema.json"],
                "type": "Feature",
                "id": f"{algo_instance.algo_type_id}_v{algo_instance.version}",
                "geometry": None,
                "properties": {
                    "datetime": algo_instance.date_registered.isoformat() + "Z" if algo_instance.date_registered else None,
                    "title": f"{algo_type.name} v{algo_instance.version}",
                    "stacd:algorithm_id": algo_instance.algo_type_id,
                    "stacd:dag_id": dag.dag_id,
                    "stacd:dag_version": dag_version,
                    "stacd:version": algo_instance.version,
                    "stacd:is_active": algo_instance.is_active,
                    "stacd:execution_modes": algo_instance.execution_modes,
                    "stacd:assets": algo_instance.assets or {},
                    "stacd:registered_at": algo_instance.date_registered.isoformat() + "Z" if algo_instance.date_registered else None
                },
                "links": [
                    {"rel": "self", "href": f"./v{algo_instance.version}.json", "type": "application/json"},
                    {"rel": "parent", "href": "../algorithm.json", "type": "application/json"},
                    {"rel": "collection", "href": "../algorithm.json", "type": "application/json"}
                ]
            }

            version_path = version_dir / f"v{algo_instance.version}.json"
            with open(version_path, 'w') as f:
                json.dump(version_item, f, indent=2, ensure_ascii=False)

            print(f"✓ STAC-D algorithm item written: {version_path}")

    finally:
        db.close()


def _build_dag_nodes(structure):
    """Build graph nodes from DAG structure"""
    nodes = []

    for algo_id in structure.get('alg_type_nodes', []):
        nodes.append({
            "id": algo_id,
            "type": "algorithm",
            "label": algo_id
        })

    for ds_id in structure.get('dataset_type_nodes', []):
        nodes.append({
            "id": ds_id,
            "type": "dataset",
            "label": ds_id
        })

    return nodes


def _build_dag_edges(dag, db):
    """Build edges from algorithm input/output relationships"""
    edges = []
    algo_ids = dag.structure.get('alg_type_nodes', [])

    for algo_id in algo_ids:
        algo_type = db.session.query(AlgorithmType)\
                              .filter_by(algo_type_id=algo_id)\
                              .first()
        if not algo_type:
            continue

        for input_ds in (algo_type.input_datasets or []):
            edges.append({
                "source": input_ds,
                "target": algo_id,
                "type": "input"
            })

        for output_ds in (algo_type.outputs or []):
            edges.append({
                "source": algo_id,
                "target": output_ds,
                "type": "output"
            })

    return edges
