"""
JSON structure analysis module for Snowflake SQL generation
COMPREHENSIVE VERSION - Includes all potential missing keys
"""
import streamlit as st
from typing import Any, Dict, List
import logging
from utils import get_snowflake_type
from config import config

logger = logging.getLogger(__name__)


@st.cache_data(ttl=3600)
def analyze_json_structure(json_obj: Any, parent_path: str = "", max_depth: int = None) -> Dict[str, Dict]:
    """
    Analyze JSON structure and return comprehensive metadata with caching
    """
    if max_depth is None:
        max_depth = config.JSON_ANALYSIS_MAX_DEPTH
    
    schema = {}
    
    def traverse_json(obj: Any, path: str = "", array_hierarchy: List[str] = [], depth: int = 0):
        if depth > max_depth:
            return
            
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_path = f"{path}.{key}" if path else key
                current_type = type(value).__name__
                
                # Determine if this is queryable (leaf node or simple type)
                is_queryable = not isinstance(value, (dict, list)) or (
                    isinstance(value, list) and len(value) > 0 and not isinstance(value[0], (dict, list))
                )
                
                # Comprehensive schema entry with all possible keys
                schema_entry = {
                    "type": current_type,
                    "snowflake_type": get_snowflake_type(current_type),
                    "array_hierarchy": array_hierarchy.copy(),
                    "depth": len(new_path.split('.')),
                    "full_path": new_path,
                    "parent_path": path,
                    # Array-related keys - multiple versions for compatibility
                    "is_array_item": len(array_hierarchy) > 0,
                    "in_array": len(array_hierarchy) > 0,
                    "is_in_array": len(array_hierarchy) > 0,
                    "array_context": array_hierarchy[-1] if array_hierarchy else None,
                    "parent_arrays": array_hierarchy.copy(),
                    # Object and queryability keys
                    "is_nested_object": isinstance(value, dict),
                    "is_queryable": is_queryable,
                    "is_leaf": is_queryable,
                    "is_primitive": not isinstance(value, (dict, list)),
                    # Value and context keys
                    "sample_value": str(value)[:100] + "..." if len(str(value)) > 100 else str(value),
                    "contexts": [path] if path else ["root"],
                    # Additional metadata
                    "nested_level": depth,
                    "path_components": new_path.split('.'),
                    "field_name": key,
                    # Array-specific metadata
                    "array_depth": len(array_hierarchy),
                    "direct_parent": path.split('.')[-1] if path else None,
                    # Type-specific metadata
                    "python_type": current_type,
                    "is_null": value is None,
                    "is_empty": (isinstance(value, (list, dict, str)) and len(value) == 0) if value is not None else False
                }
                
                # Handle type conflicts
                if new_path in schema:
                    existing_entry = schema[new_path]
                    existing_type = existing_entry["type"]
                    if existing_type != current_type:
                        if current_type in ['str', 'int', 'float'] and existing_type == 'NoneType':
                            schema[new_path].update({
                                "type": current_type,
                                "snowflake_type": get_snowflake_type(current_type),
                                "python_type": current_type
                            })
                        elif existing_type in ['str', 'int', 'float'] and current_type == 'NoneType':
                            pass  # Keep existing
                        else:
                            schema[new_path].update({
                                "type": "variant",
                                "snowflake_type": "VARIANT",
                                "python_type": "variant"
                            })
                    
                    # Merge contexts
                    existing_contexts = set(existing_entry.get("contexts", []))
                    new_contexts = set(schema_entry["contexts"])
                    schema[new_path]["contexts"] = list(existing_contexts.union(new_contexts))
                else:
                    schema[new_path] = schema_entry
                
                traverse_json(value, new_path, array_hierarchy, depth + 1)
                
        elif isinstance(obj, list) and obj:
            if path:
                # Comprehensive array schema entry
                array_schema_entry = {
                    "type": "list",
                    "snowflake_type": "ARRAY",
                    "array_hierarchy": array_hierarchy.copy(),
                    "depth": len(path.split('.')) if path else 0,
                    "full_path": path,
                    "parent_path": ".".join(path.split('.')[:-1]) if '.' in path else "",
                    # Array-related keys - multiple versions for compatibility
                    "is_array_item": len(array_hierarchy) > 0,
                    "in_array": len(array_hierarchy) > 0,
                    "is_in_array": len(array_hierarchy) > 0,
                    "array_context": array_hierarchy[-1] if array_hierarchy else None,
                    "parent_arrays": array_hierarchy.copy(),
                    # Object and queryability keys
                    "is_nested_object": False,
                    "is_queryable": True,
                    "is_leaf": False,
                    "is_primitive": False,
                    # Array-specific keys
                    "sample_value": f"Array with {len(obj)} items",
                    "array_length": len(obj),
                    "is_array": True,
                    # Additional metadata
                    "nested_level": depth,
                    "path_components": path.split('.') if path else [],
                    "field_name": path.split('.')[-1] if path else None,
                    "array_depth": len(array_hierarchy),
                    "direct_parent": ".".join(path.split('.')[:-1]) if '.' in path else None,
                    # Type-specific metadata
                    "python_type": "list",
                    "is_null": False,
                    "is_empty": len(obj) == 0,
                    "contexts": [".".join(path.split('.')[:-1]) if '.' in path else "root"]
                }
                
                schema[path] = array_schema_entry
            
            new_hierarchy = array_hierarchy + ([path] if path else [])
            
            # Analyze multiple array elements for better coverage
            sample_size = min(len(obj), 3)
            for i in range(sample_size):
                if isinstance(obj[i], (dict, list)):
                    traverse_json(obj[i], path, new_hierarchy, depth + 1)
                else:
                    # For primitive arrays, update schema info
                    if path in schema:
                        item_type = type(obj[i]).__name__
                        schema[path].update({
                            "item_type": item_type,
                            "item_snowflake_type": get_snowflake_type(item_type),
                            "item_python_type": item_type,
                            "has_primitive_items": True
                        })
                
    try:
        traverse_json(json_obj, parent_path)
        logger.info(f"Successfully analyzed JSON structure with {len(schema)} paths")
        return schema
    except Exception as e:
        logger.error(f"Error analyzing JSON structure: {e}")
        return {}
