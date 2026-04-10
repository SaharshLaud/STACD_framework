#!/usr/bin/env python3
"""
Lineage Query Engine for STACD

Builds dataset lineage on the fly using:
- DatasetInstance (produced_by_algo, algo_version, run_id, asset_id)
- AlgorithmExecution (execution_params with input asset IDs)
"""

import os
import sys
from typing import Dict, Any, List, Set

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", os.path.expanduser("~/airflow"))
sys.path.insert(0, os.path.join(AIRFLOW_HOME, "stacd/database"))

from db_operations import STACDDatabase
from models import DatasetInstance, AlgorithmExecution


def _find_dataset_by_asset(db: STACDDatabase, asset_id: str) -> DatasetInstance | None:
    """
    Try instance_id first, fallback to asset_id + latest version.
    """
    if not asset_id:
        return None
    
    # Check if this is actually an instance_id (e.g., from params like "LULC_Raster_instance_id": "3")
    try:
        instance_id = int(asset_id)
        ds = db.session.query(DatasetInstance).get(instance_id)
        if ds:
            return ds  # Exact match!
    except ValueError:
        pass  # Not an integer, try asset_id
    
    # Fallback: latest version by asset_id
    q = (
        db.session.query(DatasetInstance)
        .filter(DatasetInstance.asset_id == asset_id)
        .order_by(DatasetInstance.version.desc())
    )
    return q.first()



def get_dataset_lineage(
    instance_id: int,
    max_depth: int = 20,
) -> Dict[str, Any]:
    """
    Build lineage for a dataset instance as a tree structure.

    Returns a dict:
    {
      "root_instance_id": <int>,
      "nodes": [...],
      "edges": [...]
    }

    Where:
      nodes: [
        {
          "id": "ds:<instance_id>" or "algo:<execution_id>",
          "type": "dataset" | "algorithm",
          "label": "...",
          "data": {...}
        },
        ...
      ]

      edges: [
        {"source": "<node_id>", "target": "<node_id>", "type": "data"|"exec"},
        ...
      ]
    }
    """
    db = STACDDatabase()
    try:
        visited: Set[str] = set()
        nodes: List[Dict[str, Any]] = []
        edges: List[Dict[str, str]] = []

        def add_node(node_id: str, node_type: str, label: str, data: Dict[str, Any]):
            if node_id in visited:
                return
            visited.add(node_id)
            nodes.append(
                {
                    "id": node_id,
                    "type": node_type,
                    "label": label,
                    "data": data,
                }
            )

        def add_edge(source: str, target: str, edge_type: str):
            edges.append(
                {
                    "source": source,
                    "target": target,
                    "type": edge_type,
                }
            )

        def walk_dataset(ds: DatasetInstance, depth: int):
            if depth > max_depth:
                return

            ds_node_id = f"ds:{ds.instance_id}"
            ds_label = f"{ds.dataset_type_id} v{ds.version}"
            add_node(
                ds_node_id,
                "dataset",
                ds_label,
                {
                    "instance_id": ds.instance_id,
                    "dataset_type_id": ds.dataset_type_id,
                    "version": ds.version,
                    "asset_id": ds.asset_id,
                    "produced_by_algo": ds.produced_by_algo,
                    "algo_version": ds.algo_version,
                    "run_id": ds.run_id,
                    "is_root_dataset": ds.is_root_dataset,
                    "meta_info": ds.meta_info,
                },
            )

            # If root dataset, stop here
            if ds.is_root_dataset or not ds.produced_by_algo:
                return

            # Find the corresponding AlgorithmExecution
            exec_q = (
                db.session.query(AlgorithmExecution)
                .filter(
                    AlgorithmExecution.dag_uuid == ds.run_id,  # if you stored dag_uuid=run_id, adjust if needed
                    AlgorithmExecution.algo_type_id == ds.produced_by_algo,
                    AlgorithmExecution.version == ds.algo_version,
                )
            )

            execution = exec_q.first()
            if not execution:
                # Fallback: try just run_id + algo_type_id
                exec_q = (
                    db.session.query(AlgorithmExecution)
                    .filter(
                        AlgorithmExecution.run_id == ds.run_id,
                        AlgorithmExecution.algo_type_id == ds.produced_by_algo,
                    )
                    .order_by(AlgorithmExecution.executed_at.desc())
                )
                execution = exec_q.first()

            if not execution:
                return

            algo_node_id = f"algo:{execution.execution_id}"
            algo_label = f"{execution.algo_type_id} v{execution.version}"
            add_node(
                algo_node_id,
                "algorithm",
                algo_label,
                {
                    "execution_id": execution.execution_id,
                    "algo_type_id": execution.algo_type_id,
                    "version": execution.version,
                    "run_id": execution.run_id,
                    "execution_params": execution.execution_params,
                    "output_asset_id": execution.output_asset_id,
                    "status": execution.status,
                    "executed_at": execution.executed_at.isoformat()
                    if execution.executed_at
                    else None,
                },
            )

            # Edge: algorithm produces dataset
            add_edge(algo_node_id, ds_node_id, "exec")






            # Now walk upstream datasets from execution_params
            params = execution.execution_params or {}
            for key, value in params.items():
                # Skip non-assets
                if not isinstance(value, str):
                    continue
                if key in ("execution_id", "state", "district", "block", "start_year", "end_year"):
                    continue
                
                # PRIORITY 1: Check for _instance_id keys (e.g., "LULC_Raster_instance_id": "3")
                if "_instance_id" in key:
                    try:
                        input_instance_id = int(value)
                        parent_ds = db.session.query(DatasetInstance).get(input_instance_id)
                    except (ValueError, TypeError):
                        parent_ds = None
                else:
                    # PRIORITY 2: Try asset_id
                    parent_ds = _find_dataset_by_asset(db, value)
                
                if not parent_ds:
                    continue
                
                parent_node_id = f"ds:{parent_ds.instance_id}"
                # Edge: dataset → algorithm (input)
                add_edge(parent_node_id, algo_node_id, "data")
                walk_dataset(parent_ds, depth + 1)



        root_ds = db.session.query(DatasetInstance).filter_by(instance_id=instance_id).first()
        if not root_ds:
            return {
                "root_instance_id": instance_id,
                "nodes": [],
                "edges": [],
                "error": f"DatasetInstance {instance_id} not found",
            }

        walk_dataset(root_ds, depth=0)

        return {
            "root_instance_id": instance_id,
            "nodes": nodes,
            "edges": edges,
        }

    finally:
        db.close()
