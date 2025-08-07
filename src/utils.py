"""
Utility functions for JSON analysis and SQL generation
"""
import json
import re
from typing import Any, Dict, List, Tuple
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def get_snowflake_type(python_type: str) -> str:
    """Map Python types to Snowflake types"""
    type_mapping = {
        'str': 'VARCHAR',
        'int': 'NUMBER',
        'float': 'NUMBER',
        'bool': 'BOOLEAN',
        'datetime': 'TIMESTAMP',
        'date': 'DATE',
        'dict': 'OBJECT',
        'list': 'ARRAY',
        'NoneType': 'VARIANT',
        'variant': 'VARIANT'
    }
    return type_mapping.get(python_type, 'VARIANT')


def find_arrays(schema: Dict[str, Dict]) -> List[Dict]:
    """Find all array fields in the schema"""
    arrays = []
    for path, details in schema.items():
        if details['type'] == 'list':
            arrays.append({
                'path': path,
                'full_path': details['full_path'],
                'array_hierarchy': details['array_hierarchy'],
                'depth': details['depth'],
                'length': details.get('array_length', 0),  # Fix for the length error
                'item_type': details.get('item_type', 'unknown'),  # Add item_type
                'is_in_array': details.get('is_array_item', False),  # Fix for is_in_array error
                'parent_arrays': details['array_hierarchy']  # Add parent_arrays mapping
            })
    return sorted(arrays, key=lambda x: x['depth'])


def find_nested_objects(schema: Dict[str, Dict]) -> List[Dict]:
    """Find all nested object fields in the schema"""
    nested_objects = []
    for path, details in schema.items():
        if details['is_nested_object']:
            nested_objects.append({
                'path': path,
                'full_path': details['full_path'],
                'array_hierarchy': details['array_hierarchy'],
                'depth': details['depth']
            })
    return sorted(nested_objects, key=lambda x: x['depth'])


def find_queryable_fields(schema: Dict[str, Dict]) -> List[Dict]:
    """Find all queryable (leaf) fields in the schema"""
    queryable_fields = []
    for path, details in schema.items():
        if details['is_queryable']:
            queryable_fields.append({
                'path': path,
                'type': details['type'],
                'snowflake_type': details['snowflake_type'],
                'array_hierarchy': details['array_hierarchy'],
                'depth': details['depth'],
                'sample_value': details.get('sample_value', 'N/A')
            })
    return sorted(queryable_fields, key=lambda x: (x['depth'], x['path']))


def prettify_json(json_str: str) -> str:
    """Prettify JSON string with proper formatting"""
    try:
        json_obj = json.loads(json_str)
        return json.dumps(json_obj, indent=2, ensure_ascii=False)
    except json.JSONDecodeError as e:
        logger.error(f"Error prettifying JSON: {e}")
        return json_str


def validate_json_input(json_text: str) -> Tuple[bool, str, Any]:
    """Validate JSON input and return status, message, and parsed object"""
    try:
        json_obj = json.loads(json_text)
        return True, "Valid JSON", json_obj
    except json.JSONDecodeError as e:
        return False, f"Invalid JSON: {str(e)}", None


def export_analysis_results(schema: Dict[str, Dict]) -> Dict[str, pd.DataFrame]:
    """Export analysis results as DataFrames for download"""
    results = {}
    
    # All paths DataFrame
    all_paths_data = []
    for path, details in schema.items():
        all_paths_data.append({
            'Path': path,
            'Type': details['type'],
            'Snowflake Type': details['snowflake_type'],
            'Depth': details['depth'],
            'Is Queryable': details['is_queryable'],
            'Is Array Item': details['is_array_item'],
            'Sample Value': details.get('sample_value', 'N/A')
        })
    results['all_paths'] = pd.DataFrame(all_paths_data)
    
    # Arrays DataFrame
    arrays = find_arrays(schema)
    arrays_data = []
    for array in arrays:
        arrays_data.append({
            'Array Path': array['path'],
            'Depth': array['depth'],
            'Array Hierarchy': ' -> '.join(array['array_hierarchy']) if array['array_hierarchy'] else 'Root'
        })
    results['arrays'] = pd.DataFrame(arrays_data)
    
    # Queryable fields DataFrame
    queryable = find_queryable_fields(schema)
    queryable_data = []
    for field in queryable:
        queryable_data.append({
            'Field Path': field['path'],
            'Python Type': field['type'],
            'Snowflake Type': field['snowflake_type'],
            'Depth': field['depth'],
            'Sample Value': field.get('sample_value', 'N/A')
        })
    results['queryable_fields'] = pd.DataFrame(queryable_data)
    
    return results
