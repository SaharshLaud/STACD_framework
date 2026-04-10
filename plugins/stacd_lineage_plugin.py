import os
import json
import sys

from flask import request
from flask_appbuilder import expose
from airflow.www.app import csrf
from airflow.www.views import AirflowBaseView
from airflow.plugins_manager import AirflowPlugin

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", os.path.expanduser("~/airflow"))
sys.path.insert(0, os.path.join(AIRFLOW_HOME, "stacd/database"))
from lineage_queries import get_dataset_lineage
from db_operations import STACDDatabase
from models import DatasetInstance


class STACDLineageView(AirflowBaseView):
    # Base URL for this view: /stacd_lineage/
    route_base = "/stacd_lineage"
    default_view = "search"

    @expose("/", methods=["GET"])
    @csrf.exempt
    def search(self):
        """
        Simple search form: list latest dataset instances and links to lineage view.
        """
        print(">>> STACDLineageView.search called")

        db = STACDDatabase()
        try:
            datasets = (
                db.session.query(DatasetInstance)
                .order_by(DatasetInstance.created_at.desc())
                .limit(50)
                .all()
            )

            rows = []
            for ds in datasets:
                rows.append(
                    {
                        "instance_id": ds.instance_id,
                        "dataset_type_id": ds.dataset_type_id,
                        "version": ds.version,
                        "asset_id": ds.asset_id,
                        "created_at": ds.created_at.isoformat() if ds.created_at else "",
                    }
                )

            base_url = request.url_root.rstrip("/") + self.route_base

            html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>STACD Dataset Lineage - Search</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; }}
        h1 {{ color: #0d6efd; border-bottom: 1px solid #ddd; padding-bottom: 10px; }}
        table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f8f9fa; }}
        a.button {{
            display: inline-block;
            padding: 6px 10px;
            border-radius: 4px;
            background-color: #0d6efd;
            color: #fff;
            text-decoration: none;
            font-size: 12px;
        }}
        a.button:hover {{ background-color: #0b5ed7; }}
    </style>
</head>
<body>
    <h1>STACD Dataset Lineage - Search</h1>

    <p>Showing latest dataset instances from <code>datasetinstances</code> table.</p>

    <table>
        <thead>
            <tr>
                <th>Instance ID</th>
                <th>Dataset Type</th>
                <th>Version</th>
                <th>Asset ID</th>
                <th>Created At</th>
                <th>Lineage</th>
            </tr>
        </thead>
        <tbody>
"""
            for row in rows:
                html += f"""
            <tr>
                <td>{row['instance_id']}</td>
                <td>{row['dataset_type_id']}</td>
                <td>{row['version']}</td>
                <td style="max-width:400px; overflow-wrap:anywhere;">{row['asset_id']}</td>
                <td>{row['created_at']}</td>
                <td>
                    <a class="button" href="{base_url}/dataset/{row['instance_id']}">
                        View Lineage
                    </a>
                </td>
            </tr>
"""
            html += """
        </tbody>
    </table>
</body>
</html>
"""
            return html

        finally:
            db.close()

    @expose("/dataset/<int:instance_id>", methods=["GET"])
    @csrf.exempt
    def dataset_lineage(self, instance_id):
        """
        Enhanced lineage graph with Airflow-style design, spacing, and node inspector.
        """
        print(f">>> STACDLineageView.dataset_lineage called for {instance_id}")

        lineage = get_dataset_lineage(instance_id)
        nodes = lineage.get("nodes", [])
        edges = lineage.get("edges", [])
        error = lineage.get("error")

        # Enhanced vis-network nodes with full data for inspector
        nodes_js = []
        edges_js = []
        node_details = {}  # id -> full details for inspector
        
        # Find target node (the one matching root_instance_id)
        target_node_id = None
        for node in nodes:
            if node["data"]["instance_id"] == instance_id:
                target_node_id = node["id"]
                break
        
        for node in nodes:
            node_id = node["id"]
            node_type = node["type"]
            data = node["data"]
            
            # Determine node role for special coloring
            is_root = data.get("is_root_dataset", False)
            is_target = (node_id == target_node_id)
            
            # Colors by role + type
            colors = {
                # Root nodes (light green)
                "root_dataset": {"bg": "#E8F5E8", "border": "#388E3C"},
                "root_algorithm": {"bg": "#F0F8E8", "border": "#4CAF50"},
                # Target/final node (purple highlight!)
                "target_dataset": {"bg": "#F3E5F5", "border": "#9C27B0"},
                "target_algorithm": {"bg": "#F1E5F5", "border": "#AB47BC"},
                # Intermediate nodes
                "dataset": {"bg": "#E3F2FD", "border": "#1976D2"},
                "algorithm": {"bg": "#FFF3E0", "border": "#F57C00"}
            }
            
            color_key = "root_" + node_type if is_root else ("target_" + node_type if is_target else node_type)
            node_color = colors.get(color_key, colors["dataset"])
            
            # Label with version
            version = data.get("version", "?")
            label_lines = [node.get("label", node_id).split(" v")[0], f"\n v{version}"]
            short_label = " ".join(label_lines)  # Multi-line label
            
            # Sanitize ID
            vis_id = node_id.replace(":", "_").replace("-", "_").replace(".", "_")
            
            nodes_js.append({
                "id": vis_id,
                "label": short_label,
                "title": f"{node.get('label', node_id)} Click for full details",
                "color": {"background": node_color["bg"], "border": node_color["border"]},
                "shape": "box",
                "margin": 15,  # Bigger margin for multi-line
                "font": {"size": 11, "multi": "true", "bold": {"color": "#333"}}
            })
            node_details[vis_id] = node


        for edge in edges:
            src = edge["source"].replace(":", "_").replace("-", "_").replace(".", "_")
            tgt = edge["target"].replace(":", "_").replace("-", "_").replace(".", "_")
            edge_type = edge.get("type", "unknown")
            edges_js.append({
                "from": src,
                "to": tgt,
                "arrows": "to",
                "color": {"color": "#999", "inherit": "false"},
                "smooth": {"type": "curvedCW"}
            })

        nodes_json = json.dumps(nodes_js)
        edges_json = json.dumps(edges_js)
        nodes_details_json = json.dumps(node_details)

        base_url = request.url_root.rstrip("/") + self.route_base

        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Dataset Lineage - Instance {instance_id}</title>
    <script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
    <style>
        body {{ 
            font-family: -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif;
            margin: 0; 
            padding: 20px; 
            background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
        }}
        h1 {{ 
            color: #1a1a1a; 
            border-bottom: 1px solid #e1e5e9; 
            padding-bottom: 15px; 
            margin-bottom: 30px;
        }}
        .container {{ display: flex; gap: 20px; }}
        #mynetwork {{ 
            flex: 1; 
            height: 700px; 
            border: 1px solid #d1d5db; 
            border-radius: 8px; 
            background: white;
            box-shadow: 0 4px 6px -1px rgba(0, 0,0,0.1);
        }}
        .inspector {{ 
            width: 350px; 
            background: white; 
            border-radius: 8px; 
            box-shadow: 0 4px 6px -1px rgba(0, 0,0,0.1);
            padding: 20px;
        }}
        .inspector h3 {{ 
            margin-top: 0; 
            color: #374151; 
            border-bottom: 1px solid #e5e7eb; 
            padding-bottom: 10px;
        }}
        .inspector pre {{ 
            background: #f9fafb; 
            padding: 15px; 
            border-radius: 6px; 
            font-size: 12px; 
            max-height: 300px; 
            overflow-y: auto;
            margin: 0;
        }}
        .stats {{ 
            display: flex; 
            justify-content: space-between; 
            margin-bottom: 20px; 
            padding: 15px; 
            background: white; 
            border-radius: 8px; 
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        }}
        .error {{ color: #dc2626; background-color: #fef2f2; padding: 15px; border-radius: 8px; margin-bottom: 20px; }}
    </style>
</head>
<body>
    <h1>Dataset Lineage - Instance {instance_id}</h1>
"""

        if error:
            html += f'<div class="error">{error}</div>'

        html += f"""
    <div class="stats">
        <div><strong>{len(nodes)} Nodes</strong></div>
        <div><strong>{len(edges)} Edges</strong></div>
        <div>Drag • Zoom • Click node for details</div>
    </div>
    
    <div class="container">
        <div id="mynetwork"></div>
        <div class="inspector">
            <h3>Node Inspector</h3>
            <div id="inspector-content">
                <p>Click any node to see details</p>
            </div>
        </div>
    </div>

    <script>
        var nodes = new vis.DataSet({nodes_json});
        var edges = new vis.DataSet({edges_json});
        var container = document.getElementById('mynetwork');
        var data = {{ nodes: nodes, edges: edges }};
        
        var options = {{
            layout: {{
                hierarchical: {{
                    enabled: true,
                    direction: 'LR',
                    sortMethod: 'directed',
                    levelSeparation: 250,
                    nodeSpacing: 200,
                    treeSpacing: 300
                }}
            }},
            physics: {{ 
                enabled: false,
                hierarchicalRepulsion: {{
                    nodeDistance: 200
                }}
            }},
            interaction: {{ 
                dragNodes: true, 
                zoomView: true,
                hover: true
            }},
            nodes: {{
                margin: 12,
                borderWidth: 2,
                shadow: true
            }},
            edges: {{
                shadow: true,
                width: 2
            }}
        }};
        
        var network = new vis.Network(container, data, options);
        
        // Node inspector
        var nodeDetails = {nodes_details_json};
        network.on("selectNode", function(params) {{
            var nodeId = params.nodes[0];
            var details = nodeDetails[nodeId];
            if (details) {{
                var html = '<strong>' + details.label + '</strong><br>' +
                          '<strong>ID:</strong> ' + details.id + '<br>' +
                          '<strong>Type:</strong> ' + details.type + '<br><br>' +
                          '<pre>' + JSON.stringify(details.data, null, 2) + '</pre>';
                document.getElementById('inspector-content').innerHTML = html;
            }}
        }});
        
        network.on("deselectNode", function() {{
            document.getElementById('inspector-content').innerHTML = '<p>Click any node to see details</p>';
        }});
    </script>

    <p style="margin-top: 30px;"><a href="{base_url}">← Back to search</a></p>
</body>
</html>
"""
        return html



class STACDLineagePlugin(AirflowPlugin):
    name = "stacd_lineage_plugin"

    appbuilder_views = [
        {
            "view": STACDLineageView(),
            "name": "STACD Dataset Lineage",
            "category": "STACD Lineage",
            "category_icon": "fa-random",
        }
    ]
