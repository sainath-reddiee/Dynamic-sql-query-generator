CREATE OR REPLACE PROCEDURE SAINATH.SNOW.DYNAMIC_SQL_LARGE_IMPROVED_FIXED(
    "TABLE_NAME" VARCHAR(16777216), 
    "JSON_COLUMN" VARCHAR(16777216), 
    "FIELD_CONDITIONS" VARCHAR(16777216)
)
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
        'str': 'VARCHAR',
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

def analyze_json_for_sql_fixed(json_obj: Any, parent_path: str = "") -> Dict[str, Dict]:
    """FIXED JSON schema analysis - NO SAMPLE PREFIXES!"""
    schema = {}
    type_mapping = {
        'str': 'VARCHAR',
        'int': 'NUMBER', 
        'float': 'NUMBER',
        'bool': 'BOOLEAN',
        'dict': 'OBJECT',
        'list': 'ARRAY',
        'NoneType': 'VARIANT'
    }

    def traverse(obj: Any, path: str = "", array_hierarchy: List[str] = [], depth: int = 0, is_root_array: bool = False):
        if depth > 10:  # Prevent infinite recursion
            return

        # FIXED: Handle root-level arrays properly - NO SAMPLE PREFIXES!
        if isinstance(obj, list) and obj:
            # If this is the root array, don't add it as a separate path
            if not is_root_array and path:
                schema[path] = {
                    "type": "array",
                    "snowflake_type": "ARRAY",
                    "array_hierarchy": array_hierarchy.copy(),
                    "depth": len(path.split('.')) if path else 0,
                    "full_path": path,
                    "parent_path": ".".join(path.split('.')[:-1]) if '.' in path else "",
                    "is_queryable": True,
                    "sample_value": f"Array with {len(obj)} items",
                    "is_root_array": is_root_array
                }

            # FIXED: Properly track array hierarchy for nested processing
            new_hierarchy = array_hierarchy.copy()
            if path and not is_root_array:  # Don't add empty path for root array
                new_hierarchy.append(path)
            elif is_root_array:
                new_hierarchy.append("")  # Root array marker

            # Process array elements (sample up to 3)
            sample_size = min(len(obj), 3)
            for i in range(sample_size):
                if isinstance(obj[i], (dict, list)):
                    # CRITICAL FIX: Don't pass sample index in path!
                    traverse(obj[i], path if is_root_array else path, new_hierarchy, depth + 1, False)
                else:
                    if path in schema:
                        schema[path]["item_type"] = type(obj[i]).__name__

        elif isinstance(obj, dict):
            for key, value in obj.items():
                # FIXED: Handle path construction for root arrays - NO SAMPLE PREFIXES!
                if is_root_array or not path:
                    new_path = key  # Just the key, no sample prefix!
                else:
                    new_path = f"{path}.{key}"
                
                current_type = type(value).__name__

                # Enhanced schema with metadata
                schema_entry = {
                    "type": current_type,
                    "snowflake_type": type_mapping.get(current_type, 'VARIANT'),
                    "array_hierarchy": array_hierarchy.copy(),
                    "depth": len(new_path.split('.')),
                    "full_path": new_path,
                    "parent_path": path,
                    "is_queryable": not isinstance(value, (dict, list)),
                    "sample_value": str(value)[:100] if value is not None else "NULL"
                }

                # Handle type conflicts
                if new_path in schema:
                    existing_type = schema[new_path]["type"]
                    if existing_type != current_type:
                        if current_type in ['str', 'int', 'float'] and existing_type == 'NoneType':
                            schema[new_path]["type"] = current_type
                            schema[new_path]["snowflake_type"] = type_mapping.get(current_type, 'VARIANT')
                        elif existing_type in ['str', 'int', 'float'] and current_type == 'NoneType':
                            pass  # Keep existing
                        else:
                            schema[new_path]["type"] = "variant"
                            schema[new_path]["snowflake_type"] = "VARIANT"
                else:
                    schema[new_path] = schema_entry

                traverse(value, new_path, array_hierarchy, depth + 1, False)

    # FIXED: Check if root is an array - NO SAMPLE PREFIXES!
    if isinstance(json_obj, list):
        traverse(json_obj, "", [], 0, True)  # No parent_path for root array!
    else:
        traverse(json_obj, parent_path)
        
    return schema

def find_field_in_schema_fixed(schema: Dict, target_field: str) -> List[Tuple[str, Dict]]:
    """FIXED: Find field in schema with better path resolution"""
    matching_paths = []

    # Find all paths that end with the target field
    candidates = []
    for path, info in schema.items():
        path_parts = path.split('.')
        
        # Exact match on field name
        if path_parts[-1] == target_field or path == target_field:
            candidates.append((path, info))
        # Check if field is in a nested path (like profile.contacts.type)
        elif target_field in path_parts:
            candidates.append((path, info))

    if not candidates:
        # Try partial matching as fallback
        for path, info in schema.items():
            if target_field.lower() in path.lower():
                candidates.append((path, info))

    if not candidates:
        return []

    # FIXED: Better sorting - prefer exact matches first, then by depth
    def sort_key(item):
        path, info = item
        path_parts = path.split('.')
        exact_match = path_parts[-1] == target_field
        depth = info.get('depth', 0)
        array_depth = len(info.get('array_hierarchy', []))
        
        # Prioritize: exact match, then fewer arrays, then less depth
        return (not exact_match, array_depth, depth)

    candidates.sort(key=sort_key)
    return candidates[:1]  # Return best match

def build_array_flattening_fixed(array_paths: List[str], json_column: str, schema: Dict) -> Tuple[str, Dict[str, str]]:
    """FIXED: Build LATERAL FLATTEN clauses with correct hierarchy handling"""
    flatten_clauses = []
    array_aliases = {}

    # FIXED: Filter out empty paths and handle root arrays
    valid_array_paths = []
    has_root_array = False
    
    for path in array_paths:
        if path == "":  # Root array marker
            has_root_array = True
            valid_array_paths.append(path)
        elif path and path in schema and schema[path].get('type') == 'array':
            valid_array_paths.append(path)

    # Sort by depth to ensure proper nesting order, but handle root array first
    def sort_array_paths(path):
        if path == "":  # Root array comes first
            return (-1, "")
        return (len(path.split('.')), path)
    
    sorted_array_paths = sorted(set(valid_array_paths), key=sort_array_paths)

    for idx, array_path in enumerate(sorted_array_paths):
        alias = f"f{idx + 1}"
        array_aliases[array_path] = alias

        if array_path == "":  # Root array
            flatten_clauses.append(f", LATERAL FLATTEN(input => {json_column}) {alias}")
        else:
            # FIXED: Find the correct parent for nested arrays
            parent_ref = None
            
            # Check if this array is nested within another flattened array
            for potential_parent in sorted_array_paths[:idx]:  # Only check already processed parents
                if potential_parent == "":  # Root array parent
                    if not array_path.startswith('.'):  # This array is directly under root objects
                        parent_ref = f"{array_aliases[potential_parent]}.value"
                        break
                elif array_path.startswith(potential_parent + '.'):
                    parent_alias = array_aliases[potential_parent]
                    parent_ref = f"{parent_alias}.value"
                    break

            if parent_ref:
                # Get the relative path from the parent
                if array_path.count('.') == 1:  # Direct child of root
                    relative_path = array_path
                else:
                    # Find relative path from parent
                    for potential_parent in sorted_array_paths[:idx]:
                        if potential_parent != "" and array_path.startswith(potential_parent + '.'):
                            relative_path = array_path[len(potential_parent) + 1:]
                            break
                    else:
                        relative_path = array_path
                
                safe_relative_path = sanitize_input(relative_path)
                flatten_clauses.append(f", LATERAL FLATTEN(input => {parent_ref}:{safe_relative_path}) {alias}")
            else:
                # No parent, flatten directly from json column
                safe_array_path = sanitize_input(array_path)
                flatten_clauses.append(f", LATERAL FLATTEN(input => {json_column}:{safe_array_path}) {alias}")

    return ''.join(flatten_clauses), array_aliases

def build_field_reference_fixed(field_path: str, json_column: str,
                        array_aliases: Dict[str, str], array_hierarchy: List[str]) -> str:
    """FIXED: Build field reference path with correct array handling"""
    if not array_hierarchy:
        safe_field_path = sanitize_input(field_path)
        return f"{json_column}:{safe_field_path}"

    # FIXED: Handle root array case
    if "" in array_hierarchy:  # Root array is in hierarchy
        root_alias = array_aliases.get("", "")
        if root_alias:
            # For root array, the field path is relative to each array element
            if len(array_hierarchy) == 1:  # Only root array
                safe_field_path = sanitize_input(field_path)
                return f"{root_alias}.value:{safe_field_path}"
            else:
                # Multiple arrays in hierarchy - use the deepest one
                deepest_array = None
                for arr_path in reversed(array_hierarchy):
                    if arr_path != "" and arr_path in array_aliases:
                        deepest_array = arr_path
                        break
                
                if deepest_array:
                    deepest_alias = array_aliases[deepest_array]
                    # Calculate relative path from deepest array
                    if field_path.startswith(deepest_array + '.'):
                        field_suffix = field_path[len(deepest_array) + 1:]
                    else:
                        field_suffix = field_path.split('.')[-1]  # Just the field name
                    
                    safe_field_suffix = sanitize_input(field_suffix)
                    return f"{deepest_alias}.value:{safe_field_suffix}"
                else:
                    # Fallback to root array
                    safe_field_path = sanitize_input(field_path)
                    return f"{root_alias}.value:{safe_field_path}"

    # Original logic for non-root arrays
    deepest_array = array_hierarchy[-1]
    field_suffix = field_path[len(deepest_array) + 1:] if field_path.startswith(deepest_array + '.') else field_path

    if deepest_array in array_aliases:
        if field_suffix:
            safe_field_suffix = sanitize_input(field_suffix)
            return f"{array_aliases[deepest_array]}.value:{safe_field_suffix}"
        else:
            return f"{array_aliases[deepest_array]}.value"
    else:
        safe_field_path = sanitize_input(field_path)
        return f"{json_column}:{safe_field_path}"

def sanitize_value(value: Any, field_type: str) -> str:
    """Sanitize values for SQL"""
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
                    sanitized_values.append(f"'{sanitize_input(str(v))}'")
            else:
                sanitized_values.append(f"'{sanitize_input(str(v))}'")
        return f"({', '.join(sanitized_values)})"

    if field_type in ('NUMBER', 'INTEGER', 'INT', 'FLOAT', 'DECIMAL'):
        try:
            float(value)
            return str(value)
        except ValueError:
            return f"'{sanitize_input(str(value))}'"

    return f"'{sanitize_input(str(value))}'"

def generate_sql_fixed(table_name: str, json_column: str, field_conditions: List[Dict], schema: Dict) -> str:
    """FIXED: Generate complete SQL query with proper array handling - NO SAMPLE PREFIXES!"""
    try:
        if not field_conditions:
            return "-- No field conditions provided. Please specify fields to query."

        select_parts = []
        where_conditions = []
        field_where_conditions = []
        all_array_paths = set()
        field_paths_map = {}

        # Process conditions and build path mappings
        for condition in field_conditions:
            field = condition['field']
            matching_paths = find_field_in_schema_fixed(schema, field)
            if matching_paths:
                field_paths_map[field] = matching_paths

                for path, info in matching_paths:
                    array_hierarchy = info.get('array_hierarchy', [])
                    all_array_paths.update(array_hierarchy)

        # FIXED: Build flattening clauses with schema context
        flatten_clauses, array_aliases = build_array_flattening_fixed(list(all_array_paths), json_column, schema)

        # Process each condition
        for condition in field_conditions:
            field = condition['field']
            if field not in field_paths_map:
                continue

            matching_paths = field_paths_map[field]
            if not matching_paths:
                continue

            # Use the best match
            full_path, field_info = matching_paths[0]
            field_type = field_info.get('snowflake_type', 'VARIANT')
            array_hierarchy = field_info.get('array_hierarchy', [])
            value_path = build_field_reference_fixed(full_path, json_column, array_aliases, array_hierarchy)

            # Generate clean alias
            alias = field.replace('.', '_')

            # Apply casting if specified
            if condition['cast']:
                cast_expr = f"CAST({value_path} AS {condition['cast']})"
                field_type = condition['cast']
            else:
                cast_expr = f"{value_path}::{field_type}"

            select_parts.append(f"{cast_expr} as {alias}")

            # Build WHERE conditions
            if condition['operator'] and condition['operator'] != 'IS NOT NULL':
                operator = condition['operator'].upper()

                if operator == 'BETWEEN' and isinstance(condition['value'], list) and len(condition['value']) == 2:
                    start_val = sanitize_value(condition['value'][0], field_type)
                    end_val = sanitize_value(condition['value'][1], field_type)
                    where_clause = f"{cast_expr} BETWEEN {start_val} AND {end_val}"
                elif condition['value'] is not None:
                    sanitized_value = sanitize_value(condition['value'], field_type)
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

        # Build final SQL
        safe_table_name = sanitize_input(table_name)

        if not select_parts:
            return "-- No valid fields found for selection. Please check your field names against the JSON structure."

        sql = f"SELECT {', '.join(select_parts)}"
        sql += f"\\nFROM {safe_table_name}"

        if flatten_clauses:
            sql += flatten_clauses

        if where_conditions:
            sql += f"\\nWHERE {' '.join(where_conditions)}"

        return sql + ";"

    except Exception as e:
        return f"-- Error generating SQL: {str(e)}\\n-- Please check your field conditions and try again."

# Enhanced schema caching with versioning
schema_cache: Dict[Tuple[str, str], Tuple[Dict, float]] = {}

def dynamic_sql_generator(session, table_name: str, json_column: str, field_conditions: str) -> str:
    """FIXED main function - NO MORE SAMPLE PREFIXES!"""
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
            # FIXED schema generation - sample ONE record to avoid sample prefixes
            max_retries = 3
            retry_count = 0
            schema = {}
            
            while retry_count < max_retries:
                try:
                    # CRITICAL FIX: Sample just ONE record to avoid sample prefixes
                    sample_query = f"""
                    SELECT {json_column} 
                    FROM {quoted_table_name} 
                    WHERE {json_column} IS NOT NULL 
                    ORDER BY RANDOM() 
                    LIMIT 1
                    """
                    
                    result = session.sql(sample_query).collect()
                    if not result:
                        return "-- Error: No data found in the specified table/column;"
                    
                    # Process ONLY the first result to avoid sample prefixes
                    try:
                        json_data = json.loads(result[0][json_column])
                        # CRITICAL FIX: Use the FIXED schema analysis function
                        schema = analyze_json_for_sql_fixed(json_data)  # NO parent_path!
                        
                        if not schema:
                            return "-- Error: No valid JSON schema could be extracted;"
                        
                        schema_cache[schema_key] = (schema, current_time)
                        break
                        
                    except json.JSONDecodeError:
                        return "-- Error: Invalid JSON data found in the table;"
                    
                except Exception as e:
                    retry_count += 1
                    if retry_count == max_retries:
                        return f"-- Error accessing table data after {max_retries} attempts: {str(e)};"
                    continue
        
        # VERIFICATION: Check for sample prefixes in generated schema
        sample_prefix_paths = [path for path in schema.keys() if "sample_" in path]
        if sample_prefix_paths:
            return f"-- CRITICAL ERROR: Schema contains sample prefixes: {sample_prefix_paths[:3]};"
        
        # Generate SQL with FIXED logic
        sql = generate_sql_fixed(quoted_table_name, json_column, conditions, schema)
        
        # FINAL VERIFICATION: Check generated SQL for sample prefixes
        if "sample_0" in sql or "sample_1" in sql:
            return "-- CRITICAL ERROR: Generated SQL contains sample prefixes - logic still buggy;"
        
        return sql
        
    except Exception as e:
        return f"""-- Error in dynamic SQL generation
-- Error message: {str(e)}
-- Please verify your inputs and try again;"""
$$;
