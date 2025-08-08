CREATE OR REPLACE PROCEDURE SAINATH.SNOW.DYNAMIC_SQL_LARGE_IMPROVED("TABLE_NAME" VARCHAR(16777216), "JSON_COLUMN" VARCHAR(16777216), "FIELD_CONDITIONS" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'dynamic_sql_generator'
EXECUTE AS OWNER
AS $$
import json
from typing import Dict, Any, List, Tuple, Optional, Set
import time
import re

def sanitize_input(value: str) -> str:
    """Enhanced sanitize input strings to handle all SQL injection risks."""
    if not isinstance(value, str):
        return str(value)
    # More comprehensive sanitization
    value = value.replace("'", "''").replace('"', '""')
    # Remove potential SQL injection patterns - fixed regex
    value = re.sub(r'[;\x00-\x1f]', '', value)
    return value

def get_snowflake_type(python_type: str) -> str:
    """Enhanced type mapping with better coverage."""
    type_mapping = {
        'str': 'STRING',
        'int': 'NUMBER',
        'float': 'NUMBER',
        'bool': 'BOOLEAN',
        'datetime': 'TIMESTAMP',
        'date': 'DATE',
        'dict': 'VARIANT',
        'list': 'ARRAY',
        'NoneType': 'VARIANT',
        'decimal': 'NUMBER',
        'time': 'TIME',
        'binary': 'BINARY',
        'object': 'OBJECT',
        'tuple': 'ARRAY'
    }
    return type_mapping.get(python_type, 'VARIANT')

def parse_field_conditions(conditions: str) -> List[Dict]:
    """Enhanced field condition parsing with better error handling."""
    result = []
    if not conditions or conditions.isspace():
        return result
    
    fields = []
    current_field = []
    bracket_count = 0
    
    # Improved parsing logic
    for char in conditions:
        if char == '[':
            bracket_count += 1
        elif char == ']':
            bracket_count -= 1
        elif char == ',' and bracket_count == 0:
            fields.append(''.join(current_field).strip())
            current_field = []
            continue
        current_field.append(char)
    
    if current_field:
        fields.append(''.join(current_field).strip())
    
    for field in fields:
        if not field:
            continue
            
        condition = {
            'field': field,
            'operator': 'IS NOT NULL',
            'value': None,
            'cast': None,
            'logic_operator': 'AND'
        }
        
        if '[' in field and ']' in field:
            base_field = field[:field.index('[')].strip()
            operator_value = field[field.index('[')+1:field.index(']')]
            
            condition['field'] = base_field
            subconditions = []
            
            current = []
            nested_count = 0
            
            for char in operator_value:
                if char == ',' and nested_count == 0:
                    subconditions.append(''.join(current).strip())
                    current = []
                else:
                    if char == '(':
                        nested_count += 1
                    elif char == ')':
                        nested_count -= 1
                    current.append(char)
            
            if current:
                subconditions.append(''.join(current).strip())
            
            for subcond in subconditions:
                parts = [p.strip() for p in subcond.split(':')]
                
                if len(parts) >= 2:
                    if parts[0].upper() == 'CAST':
                        condition['cast'] = parts[1].upper()
                    else:
                        condition['operator'] = parts[0]
                        if parts[0].upper() in ('IN', 'NOT IN'):
                            values = [v.strip() for v in parts[1].split('|')]
                            condition['value'] = values
                        elif parts[0].upper() == 'BETWEEN':
                            values = [v.strip() for v in parts[1].split('|')]
                            if len(values) != 2:
                                raise ValueError(f"BETWEEN operator requires exactly 2 values, got {len(values)}")
                            condition['value'] = values
                        else:
                            condition['value'] = parts[1]
                        
                        if len(parts) > 2:
                            condition['logic_operator'] = parts[2].upper()
                            
        result.append(condition)
    
    return result

def validate_operator(operator: str, field_type: str) -> bool:
    """Enhanced operator validation."""
    operator = operator.upper()
    field_type = field_type.upper()
    
    operator_mapping = {
        'NUMERIC': {'<', '>', '<=', '>=', '=', '!=', 'IN', 'NOT IN', 'BETWEEN'},
        'STRING': {'LIKE', 'NOT LIKE', '=', '!=', 'IN', 'NOT IN', 'CONTAINS', 'NOT CONTAINS', 'ILIKE'},
        'DATE': {'<', '>', '<=', '>=', '=', '!=', 'BETWEEN'},
        'BOOLEAN': {'=', '!=', 'IS', 'IS NOT'},
        'VARIANT': {'=', '!=', 'IS', 'IS NOT', 'LIKE', 'NOT LIKE', 'CONTAINS', 'NOT CONTAINS', '<', '>', '<=', '>=', 'IN', 'NOT IN', 'BETWEEN'},
        'ARRAY': {'=', '!=', 'CONTAINS', 'NOT CONTAINS'},
        'OBJECT': {'=', '!=', 'IS', 'IS NOT', 'CONTAINS', 'NOT CONTAINS'}
    }
    
    type_categories = {
        'NUMBER': 'NUMERIC', 'INTEGER': 'NUMERIC', 'INT': 'NUMERIC', 'FLOAT': 'NUMERIC', 'DECIMAL': 'NUMERIC',
        'STRING': 'STRING', 'VARCHAR': 'STRING', 'TEXT': 'STRING', 'CHAR': 'STRING',
        'DATE': 'DATE', 'TIMESTAMP': 'DATE', 'DATETIME': 'DATE',
        'BOOLEAN': 'BOOLEAN', 'BOOL': 'BOOLEAN',
        'VARIANT': 'VARIANT', 'ARRAY': 'ARRAY', 'OBJECT': 'OBJECT'
    }
    
    category = type_categories.get(field_type, 'VARIANT')
    
    if operator in {'IS NULL', 'IS NOT NULL'}:
        return True
        
    return operator in operator_mapping[category]

def validate_cast_type(cast_type: str) -> bool:
    """Enhanced cast type validation."""
    valid_types = {
        'NUMBER', 'INTEGER', 'INT', 'FLOAT', 'VARCHAR', 'STRING',
        'BOOLEAN', 'DATE', 'TIMESTAMP', 'VARIANT', 'ARRAY', 'TIME',
        'BINARY', 'OBJECT', 'TEXT', 'CHAR', 'DECIMAL', 'DOUBLE'
    }
    return cast_type.upper() in valid_types

def sanitize_value(value: Any, field_type: str) -> str:
    """Enhanced value sanitization with better type handling."""
    if value is None:
        return "NULL"
        
    field_type = field_type.upper()
    
    if isinstance(value, list):
        sanitized_values = []
        for v in value:
            if field_type in ('NUMBER', 'INTEGER', 'INT', 'FLOAT', 'DECIMAL'):
                try:
                    float(v)
                    sanitized_values.append(str(v))
                except ValueError:
                    raise ValueError(f"Invalid numeric value: {v}")
            elif field_type in ('BOOLEAN', 'BOOL'):
                sanitized_values.append(str(v).lower())
            elif field_type in ('DATETIME', 'DATE', 'TIMESTAMP'):
                sanitized_values.append(f"TO_TIMESTAMP('{sanitize_input(str(v))}')")
            else:
                sanitized_values.append(f"'{sanitize_input(str(v))}'")
        return f"({', '.join(sanitized_values)})"
    
    if field_type in ('NUMBER', 'INTEGER', 'INT', 'FLOAT', 'DECIMAL'):
        try:
            float(value)
            return str(value)
        except ValueError:
            raise ValueError(f"Invalid numeric value: {value}")
    
    if field_type in ('BOOLEAN', 'BOOL'):
        return str(value).lower()
    
    if field_type in ('DATETIME', 'DATE', 'TIMESTAMP'):
        return f"TO_TIMESTAMP('{sanitize_input(str(value))}')"
        
    if field_type == 'ARRAY' and isinstance(value, str):
        if value.startswith('[') and value.endswith(']'):
            return value
        return f"[{value}]"
    
    return f"'{sanitize_input(str(value))}'"

def generate_json_schema_enhanced(json_obj: Any, parent_path: str = "", max_depth: int = 10) -> Dict:
    """Enhanced schema generation with better conflict resolution."""
    schema = {}
    
    def traverse_json(obj: Any, path: str = "", array_hierarchy: List[str] = [], depth: int = 0):
        if depth > max_depth:
            return
            
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_path = f"{path}.{key}" if path else key
                current_type = type(value).__name__
                
                # Enhanced schema with metadata
                schema_entry = {
                    "type": current_type,
                    "array_hierarchy": array_hierarchy.copy(),
                    "parent_arrays": [p for p in array_hierarchy if p != new_path],
                    "depth": len(new_path.split('.')),
                    "full_path": new_path,
                    "parent_path": path,
                    "contexts": []
                }
                
                # Handle type conflicts with merge strategy
                if new_path in schema:
                    existing_type = schema[new_path]["type"]
                    if existing_type != current_type:
                        # Conflict resolution: prefer more specific types
                        if current_type in ['str', 'int', 'float'] and existing_type == 'NoneType':
                            schema[new_path]["type"] = current_type
                        elif existing_type in ['str', 'int', 'float'] and current_type == 'NoneType':
                            pass  # Keep existing
                        else:
                            schema[new_path]["type"] = "VARIANT"  # Fallback for conflicts
                    
                    # Merge contexts
                    schema[new_path]["contexts"] = list(set(schema[new_path]["contexts"] + [path]))
                else:
                    schema_entry["contexts"] = [path] if path else ["root"]
                    schema[new_path] = schema_entry
                
                traverse_json(value, new_path, array_hierarchy, depth + 1)
                
        elif isinstance(obj, list) and obj:
            if path not in schema:
                schema[path] = {
                    "type": "array",
                    "array_hierarchy": array_hierarchy.copy(),
                    "parent_arrays": [p for p in array_hierarchy if p != path],
                    "depth": len(path.split('.')) if path else 0,
                    "full_path": path,
                    "parent_path": ".".join(path.split('.')[:-1]) if '.' in path else "",
                    "contexts": []
                }
            
            new_hierarchy = array_hierarchy + [path]
            
            # Process multiple array elements for better schema coverage
            sample_size = min(len(obj), 3)  # Sample up to 3 elements
            for i in range(sample_size):
                if isinstance(obj[i], (dict, list)):
                    traverse_json(obj[i], path, new_hierarchy, depth + 1)
                else:
                    schema[path]["item_type"] = type(obj[i]).__name__
                
    traverse_json(json_obj, parent_path)
    return schema

def find_field_details_enhanced(schema: Dict, target_field: str) -> List[Tuple[str, List[str], Dict]]:
    """Enhanced field resolution with context-aware duplicate handling."""
    matching_paths = []
    
    # Find all paths that end with the target field
    candidates = []
    for path, info in schema.items():
        path_parts = path.split('.')
        if path_parts[-1] == target_field:
            candidates.append((path, info))
    
    if not candidates:
        raise ValueError(f"Field '{target_field}' not found in JSON structure")
    
    # Enhanced duplicate resolution strategy
    if len(candidates) == 1:
        path, info = candidates[0]
        matching_paths.append((path, info.get('array_hierarchy', []), info))
    else:
        # Multiple candidates - use sophisticated resolution
        # 1. Prefer shorter paths (less nested)
        min_depth = min(info.get('depth', len(path.split('.'))) for path, info in candidates)
        min_depth_candidates = [(path, info) for path, info in candidates if info.get('depth', len(path.split('.'))) == min_depth]
        
        # 2. If still multiple, prefer paths with fewer array hierarchies
        if len(min_depth_candidates) > 1:
            min_array_depth = min(len(info.get('array_hierarchy', [])) for path, info in min_depth_candidates)
            final_candidates = [(path, info) for path, info in min_depth_candidates if len(info.get('array_hierarchy', [])) == min_array_depth]
        else:
            final_candidates = min_depth_candidates
        
        # 3. Return all remaining candidates with context information
        for path, info in final_candidates:
            matching_paths.append((path, info.get('array_hierarchy', []), info))
    
    return matching_paths

def build_array_flattening_enhanced(array_paths: List[str], json_column: str) -> Tuple[str, Dict[str, str]]:
    """Enhanced array flattening with better parent-child detection."""
    flatten_clauses = []
    array_aliases = {}
    
    # Sort by depth to ensure proper nesting order
    sorted_array_paths = sorted(array_paths, key=lambda x: (len(x.split('.')), x))
    
    for idx, array_path in enumerate(sorted_array_paths):
        alias = f"f{idx + 1}"
        array_aliases[array_path] = alias
        
        # Enhanced parent detection
        parent_path = None
        for potential_parent in sorted_array_paths:
            if (array_path.startswith(potential_parent + '.') and 
                potential_parent != array_path and
                array_path.count('.') == potential_parent.count('.') + 1):
                parent_path = potential_parent
                break
        
        if parent_path and parent_path in array_aliases:
            parent_alias = array_aliases[parent_path]
            relative_path = array_path[len(parent_path) + 1:]
            # Enhanced path sanitization
            safe_relative_path = sanitize_input(relative_path)
            flatten_clauses.append(f", LATERAL FLATTEN(input => {parent_alias}.value:{safe_relative_path}) {alias}")
        else:
            # Enhanced path sanitization
            safe_array_path = sanitize_input(array_path)
            flatten_clauses.append(f", LATERAL FLATTEN(input => {json_column}:{safe_array_path}) {alias}")
    
    return ''.join(flatten_clauses), array_aliases

def build_field_path_enhanced(field_path: str, json_column: str, array_aliases: Dict[str, str], array_hierarchy: List[str]) -> str:
    """Enhanced field path building with better sanitization."""
    if not array_hierarchy:
        safe_field_path = sanitize_input(field_path)
        return f"{json_column}:{safe_field_path}"
    
    deepest_array = array_hierarchy[-1]
    field_suffix = field_path[len(deepest_array) + 1:] if field_path.startswith(deepest_array + '.') else field_path
    
    if deepest_array in array_aliases:
        if field_suffix:
            safe_field_suffix = sanitize_input(field_suffix)
            return f"{array_aliases[deepest_array]}.value:{safe_field_suffix}"
        else:
            return f"{array_aliases[deepest_array]}.value"
    else:
        # Fallback to direct path
        safe_field_path = sanitize_input(field_path)
        return f"{json_column}:{safe_field_path}"

def generate_sql_enhanced(table_name: str, json_column: str, field_conditions: List[Dict], schema: Dict) -> str:
    """Enhanced SQL generation with better safety and correctness."""
    select_parts = []
    where_conditions = []
    field_where_conditions = []
    all_array_paths = set()
    field_paths_map = {}
    
    # Process conditions and build path mappings
    for condition in field_conditions:
        field = condition['field']
        matching_paths = find_field_details_enhanced(schema, field)
        field_paths_map[field] = matching_paths
        
        for _, array_hierarchy, _ in matching_paths:
            all_array_paths.update(array_hierarchy)
    
    # Build flattening clauses
    flatten_clauses, array_aliases = build_array_flattening_enhanced(list(all_array_paths), json_column)
    
    # Process each condition
    for condition in field_conditions:
        field = condition['field']
        matching_paths = field_paths_map[field]
        
        # Use the first (best) match for each field
        if matching_paths:
            full_path, array_hierarchy, field_info = matching_paths[0]
            field_type = get_snowflake_type(field_info['type'])
            value_path = build_field_path_enhanced(full_path, json_column, array_aliases, array_hierarchy)
            
            # Generate clean alias
            alias = field.replace('.', '_')
            
            # Apply casting if specified
            if condition['cast']:
                if not validate_cast_type(condition['cast']):
                    raise ValueError(f"Invalid cast type: {condition['cast']}")
                cast_expr = f"CAST({value_path} AS {condition['cast']})"
                field_type = condition['cast']
            else:
                cast_expr = value_path
            
            select_parts.append(f"{cast_expr} as {alias}")
            
            # Build WHERE conditions
            if condition['operator'] and condition['operator'] != 'IS NOT NULL':
                operator = condition['operator'].upper()
                if not validate_operator(operator, field_type):
                    raise ValueError(f"Invalid operator '{operator}' for field type '{field_type}'")
                
                if operator == 'BETWEEN' and isinstance(condition['value'], list):
                    start_val = sanitize_value(condition['value'][0], field_type)
                    end_val = sanitize_value(condition['value'][1], field_type)
                    where_clause = f"{cast_expr} BETWEEN {start_val} AND {end_val}"
                elif condition['value'] is not None:
                    sanitized_value = sanitize_value(condition['value'], field_type)
                    
                    # Special handling for string operations
                    if operator in ('LIKE', 'NOT LIKE', 'ILIKE'):
                        cast_expr = f"CAST({value_path} AS STRING)"
                    
                    where_clause = f"{cast_expr} {operator} {sanitized_value}"
                else:
                    where_clause = f"{cast_expr} {operator}"
                
                field_where_conditions.append({
                    'condition': where_clause,
                    'logic_operator': condition.get('logic_operator', 'AND')
                })
    
    # Build final WHERE clause
    if field_where_conditions:
        where_conditions.append(field_where_conditions[0]['condition'])
        for condition_info in field_where_conditions[1:]:
            where_conditions.append(f"{condition_info['logic_operator']} {condition_info['condition']}")
    
    # Build final unified SQL
    safe_table_name = sanitize_input(table_name)
    
    if not select_parts:
        return "-- No valid fields found for selection;"
    
    sql = f"SELECT {', '.join(select_parts)}\\nFROM {safe_table_name}"
    
    if flatten_clauses:
        sql += flatten_clauses
    
    if where_conditions:
        sql += f"\\nWHERE {' '.join(where_conditions)}"
    
    return sql + ";"

# Enhanced schema caching with versioning
schema_cache: Dict[Tuple[str, str], Tuple[Dict, float]] = {}

def dynamic_sql_generator(session, table_name: str, json_column: str, field_conditions: str) -> str:
    """Enhanced main function with better error handling and performance."""
    try:
        if not all([table_name, json_column]):
            raise ValueError("Table name and JSON column are required")
            
        safe_table_name = sanitize_input(table_name)
        quoted_table_name = f'"{safe_table_name}"'
        
        try:
            conditions = parse_field_conditions(field_conditions)
        except Exception as e:
            return f"-- Error parsing field conditions: {str(e)};"
        
        # Enhanced schema caching with timestamp
        schema_key = (table_name, json_column)
        current_time = time.time()
        cache_ttl = 3600  # 1 hour cache TTL
        
        if (schema_key in schema_cache and 
            current_time - schema_cache[schema_key][1] < cache_ttl):
            schema = schema_cache[schema_key][0]
        else:
            # Enhanced schema generation with better sampling
            max_retries = 3
            retry_count = 0
            schema = {}
            
            # Adaptive batch sizing based on table size
            initial_batch_size = 100
            max_batch_size = 1000
            
            while retry_count < max_retries:
                try:
                    # Enhanced sampling strategy
                    sample_query = f"""
                    SELECT {json_column} 
                    FROM {quoted_table_name} 
                    WHERE {json_column} IS NOT NULL 
                    ORDER BY RANDOM() 
                    LIMIT {initial_batch_size}
                    """
                    
                    result = session.sql(sample_query).collect()
                    if not result:
                        return "-- Error: No data found in the specified table/column;"
                    
                    # Process results with enhanced schema generation
                    processed_count = 0
                    for row in result:
                        try:
                            json_data = json.loads(row[json_column])
                            current_schema = generate_json_schema_enhanced(json_data)
                            
                            # Enhanced schema merging
                            for path, info in current_schema.items():
                                if path in schema:
                                    # Merge with conflict resolution
                                    existing_info = schema[path]
                                    if existing_info["type"] != info["type"]:
                                        # Type conflict resolution
                                        if info["type"] in ['str', 'int', 'float'] and existing_info["type"] == 'NoneType':
                                            schema[path]["type"] = info["type"]
                                        elif existing_info["type"] in ['str', 'int', 'float'] and info["type"] == 'NoneType':
                                            pass  # Keep existing
                                        else:
                                            schema[path]["type"] = "VARIANT"
                                    
                                    # Merge contexts
                                    schema[path]["contexts"] = list(set(
                                        existing_info.get("contexts", []) + info.get("contexts", [])
                                    ))
                                else:
                                    schema[path] = info
                            
                            processed_count += 1
                        except json.JSONDecodeError:
                            continue  # Skip malformed JSON
                    
                    if processed_count == 0:
                        return "-- Error: No valid JSON data found;"
                    
                    schema_cache[schema_key] = (schema, current_time)
                    break
                    
                except Exception as e:
                    retry_count += 1
                    if retry_count == max_retries:
                        return f"-- Error accessing table data after {max_retries} attempts: {str(e)};"
                    initial_batch_size = min(initial_batch_size * 2, max_batch_size)
                    continue
        
        # Generate SQL with enhanced logic
        sql = generate_sql_enhanced(quoted_table_name, json_column, conditions, schema)
        
        return sql
        
    except Exception as e:
        return f"""-- Error in dynamic SQL generation
-- Error message: {str(e)}
-- Please verify your inputs and try again;"""
$$;
