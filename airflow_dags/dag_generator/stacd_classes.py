"""
STACD YAML Classes with MIXED execution mode support
Supports both API and Docker execution with priority-based selection
"""

import yaml
from datetime import datetime


class DAG(yaml.YAMLObject):
    yaml_tag = '!DAG'
    
    def __init__(self, id, name, version, description, alg_type_nodes, dataset_type_nodes):
        self.id = id
        self.name = name
        self.version = version
        self.description = description
        self.alg_type_nodes = alg_type_nodes
        self.dataset_type_nodes = dataset_type_nodes


class Algorithm_Type(yaml.YAMLObject):
    yaml_tag = '!Algorithm_Type'
    
    def __init__(self, id, name, description, params, input_datasets=None, outputs=None):
        self.id = id
        self.name = name
        self.description = description
        self.params = params
        self.input_datasets = input_datasets or []
        self.outputs = outputs or []


class Algorithm_Instance(yaml.YAMLObject):
    yaml_tag = '!Algorithm_Instance'
    
    def __init__(self, type, version, assets, date, execution_modes):
        self.type = type
        self.version = version
        self.assets = assets
        self.date = date
        self.execution_modes = execution_modes  # Dict with 'api' and 'docker' keys
    
    def get_execution_mode(self):
        """
        Determine which execution mode to use based on enabled flags and priority.
        Returns: ('api', api_config) or ('docker', docker_config) or (None, None)
        """
        api_config = self.execution_modes.get('api', {})
        docker_config = self.execution_modes.get('docker', {})
        
        api_enabled = api_config.get('enabled', False)
        docker_enabled = docker_config.get('enabled', False)
        
        # If only one is enabled, use that
        if api_enabled and not docker_enabled:
            return 'api', api_config
        if docker_enabled and not api_enabled:
            return 'docker', docker_config
        
        # If both enabled, use priority (lower number = higher priority)
        if api_enabled and docker_enabled:
            api_priority = api_config.get('priority', 999)
            docker_priority = docker_config.get('priority', 999)
            
            if api_priority < docker_priority:
                return 'api', api_config
            else:
                return 'docker', docker_config
        
        # Neither enabled
        return None, None


class Dataset_Type(yaml.YAMLObject):
    yaml_tag = '!Dataset_Type'
    
    def __init__(self, id, name, description, format):
        self.id = id
        self.name = name
        self.description = description
        self.format = format
