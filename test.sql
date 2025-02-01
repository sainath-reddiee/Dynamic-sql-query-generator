CREATE OR REPLACE PROCEDURE DYNAMIC_SQL_LARGE_SAINATH("TABLE_NAME" VARCHAR(16777216), "JSON_COLUMN" VARCHAR(16777216), "FIELD_CONDITIONS" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'dynamic_sql_generator'
EXECUTE AS OWNER
AS $$
import json
from typing import Dict, List, Tuple

def get_snowflake_type(python_type: str) -> str:
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
        'object': 'OBJECT'
    }
    return type_mapping.get(python_type, 'VARIANT')

def validate_operator(operator: str, field_type: str) -> bool:
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
        'NUMBER': 'NUMERIC',
        'INTEGER': 'NUMERIC',
        'INT': 'NUMERIC',
        'FLOAT': 'NUMERIC',
        'DECIMAL': 'NUMERIC',
        'STRING': 'STRING',
        'VARCHAR': 'STRING',
        'TEXT': 'STRING',
        'CHAR': 'STRING',
        'DATE': 'DATE',
        'TIMESTAMP': 'DATE',
        'DATETIME': 'DATE',
        'BOOLEAN': 'BOOLEAN',
        'BOOL': 'BOOLEAN',
        'VARIANT': 'VARIANT',
        'ARRAY': 'ARRAY',
        'OBJECT': 'OBJECT'
    }
    
    category = type_categories.get(field_type, 'VARIANT')
    
    if operator in {'IS NULL', 'IS NOT NULL'}:
        return True
        
    return operator in operator_mapping[category]

def validate_cast_type(cast_type: str) -> bool:
    valid_types = {
        'NUMBER', 'INTEGER', 'INT', 'FLOAT', 'VARCHAR', 'STRING',
        'BOOLEAN', 'DATE', 'TIMESTAMP', 'VARIANT', 'ARRAY', 'TIME',
        'BINARY', 'OBJECT', 'TEXT', 'CHAR'
    }
    return cast_type.upper() in valid_types

def sanitize_value(value: Any, field_type: str) -> str:
    if value is None:
        return "NULL"
    
    field_type = field_type.upper()
    
    # Handle list of values (for IN operator)
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
                sanitized_values.append(f"TO_TIMESTAMP('{v}')")
            else:
                # Escape single quotes in string values
                sanitized_values.append(f"'{str(v).replace("'", "''")}'")
        return f"({', '.join(sanitized_values)})"
    
    # Handle single value
    if field_type in ('NUMBER', 'INTEGER', 'INT', 'FLOAT', 'DECIMAL'):
        try:
            float(value)
            return str(value)
        except ValueError:
            raise ValueError(f"Invalid numeric value: {value}")
    
    if field_type in ('BOOLEAN', 'BOOL'):
        return str(value).lower()
    
    if field_type in ('DATETIME', 'DATE', 'TIMESTAMP'):
        return f"TO_TIMESTAMP('{value}')"
    
    if field_type == 'ARRAY' and isinstance(value, str):
        if value.startswith('[') and value.endswith(']'):
            return value
        return f"[{value}]"
    
    # Escape single quotes in string values
    return f"'{str(value).replace("'", "''")}'"

def parse_field_conditions(conditions: str) -> List[Dict]:
    # Parse field conditions (same as before)
    result = []
    if not conditions or conditions.isspace():
        return result
    
    # Logic to parse conditions (omitted for brevity)
    return result

def build_array_flattening(array_paths: List[str], json_column: str) -> Tuple[str, Dict[str, str]]:
    flatten_clauses = []
    array_aliases = {}
    
    # Sort array paths by depth to ensure parent arrays are flattened first
    sorted_array_paths = sorted(array_paths, key=lambda x: len(x.split('.')))
    
    for idx, array_path in enumerate(sorted_array_paths):
        # Generate a unique alias for each array level
        alias = f"f{idx + 1}"
        array_aliases[array_path] = alias
        
        # Find the parent array path
        parent_path = None
        for p in sorted_array_paths:
            if array_path.startswith(p + '.') and p != array_path:
                parent_path = p
                break
        
        if parent_path:
            # If there is a parent array, use its alias and construct the relative path
            parent_alias = array_aliases[parent_path]
            relative_path = array_path[len(parent_path) + 1:]
            flatten_clauses.append(f", LATERAL FLATTEN(input => {parent_alias}.value:{relative_path}) {alias}")
        else:
            # If no parent array, flatten directly from the JSON column
            flatten_clauses.append(f", LATERAL FLATTEN(input => {json_column}:{array_path}) {alias}")
    
    return ''.join(flatten_clauses), array_aliases

def generate_sql(table_name: str, json_column: str, field_conditions: List[Dict], schema: Dict) -> str:
    select_parts = []
    where_conditions = []
    field_where_conditions = {}  # Group WHERE conditions by field name
    all_array_paths = set()
    field_paths_map = {}
    
    # Find all possible paths for each field and their types
    for condition in field_conditions:
        field = condition['field']
        matching_paths = find_field_details(schema, field)
        field_paths_map[field] = matching_paths
        
        # Add array paths from all matches
        for _, array_hierarchy in matching_paths:
            all_array_paths.update(array_hierarchy)
    
    flatten_clauses, array_aliases = build_array_flattening(list(all_array_paths), json_column)
    
    # Process each field's conditions
    for condition in field_conditions:
        field = condition['field']
        matching_paths = field_paths_map[field]
        field_conditions_list = []  # Store all conditions for this field
        
        for idx, (full_path, array_hierarchy) in enumerate(matching_paths):
            # Get field type from schema
            field_type = schema[full_path]
            
            value_path = build_field_path(full_path, json_column, array_aliases, array_hierarchy)
            alias = f"{field}_{idx + 1}" if len(matching_paths) > 1 else field
            
            if condition['cast']:
                if not validate_cast_type(condition['cast']):
                    raise ValueError(f"Invalid cast type: {condition['cast']}")
                cast_expr = f"CAST({value_path} AS {condition['cast']})"
                field_type = condition['cast']  # Use cast type for value sanitization
            else:
                cast_expr = value_path
            
            select_parts.append(f"{cast_expr} as {alias}")
            
            # Validate operator against field type
            if condition['operator'] != 'IS NOT NULL':
                if not validate_operator(condition['operator'], field_type):
                    raise ValueError(f"Invalid operator '{condition['operator']}' for field type '{field_type}'")
            
            # Build WHERE condition
            where_clause = f"{cast_expr} {condition['operator']}"
            if condition['operator'] != 'IS NOT NULL':
                operator = condition['operator'].upper()
                if operator == 'BETWEEN' and isinstance(condition['value'], list):
                    start_val = sanitize_value(condition['value'][0], field_type)
                    end_val = sanitize_value(condition['value'][1], field_type)
                    where_clause = f"{cast_expr} BETWEEN {start_val} AND {end_val}"
                else:
                    sanitized_value = sanitize_value(condition['value'], field_type)
                    where_clause = f"{cast_expr} {operator} {sanitized_value}"
            
            field_conditions_list.append(where_clause)
        
        # Group conditions for the same field with OR
        if len(field_conditions_list) > 1:
            grouped_condition = f"({' OR '.join(field_conditions_list)})"
        else:
            grouped_condition = field_conditions_list[0]
            
        # Store the grouped condition with its logic operator
        if field not in field_where_conditions:
            field_where_conditions[field] = {
                'condition': grouped_condition,
                'logic_operator': condition['logic_operator']
            }
    
    # Build final WHERE clause
    first_condition = True
    for field, condition_info in field_where_conditions.items():
        if first_condition:
            where_conditions.append(condition_info['condition'])
            first_condition = False
        else:
            where_conditions.append(f"{condition_info['logic_operator']} {condition_info['condition']}")
    
    sql = f"SELECT {', '.join(select_parts)}\nFROM {table_name}"
    
    if flatten_clauses:
        sql += flatten_clauses
    
    if where_conditions:
        sql += f"\nWHERE {' '.join(where_conditions)}"
    
    return sql + ";"

def find_field_details(schema: Dict, target_field: str) -> List[Tuple[str, List[str]]]:
    matching_paths = []
    min_depth = float('inf')
    
    # First find the minimum depth of all matching paths
    for path, info in schema.items():
        path_parts = path.split('.')
        if path_parts[-1] == target_field:
            depth = len(path_parts)
            min_depth = min(min_depth, depth)
    
    # Then collect only the paths with minimum depth
    for path, info in schema.items():
        path_parts = path.split('.')
        if path_parts[-1] == target_field:
            if len(path_parts) == min_depth:
                matching_paths.append((path, []))
    
    if not matching_paths:
        raise ValueError(f"Field '{target_field}' not found in JSON structure")
    
    return matching_paths

def build_field_path(field_path: str, json_column: str, array_aliases: Dict[str, str], array_hierarchy: List[str]) -> str:
    if not array_hierarchy:
        return f"{json_column}:{field_path}"
    
    deepest_array = array_hierarchy[-1]
    field_suffix = field_path[len(deepest_array) + 1:] if field_path.startswith(deepest_array + '.') else field_path
    
    return f"{array_aliases[deepest_array]}.value{':' + field_suffix if field_suffix else ''}"

def dynamic_sql_generator(session, table_name: str, json_column: str, field_conditions: str) -> str:
    try:
        if not all([table_name, json_column]):
            raise ValueError("Table name and JSON column are required")
            
        quoted_table_name = f'"{table_name}"'
        
        # Parse field conditions
        conditions = parse_field_conditions(field_conditions)
        
        # Infer schema using Snowflake's INFER_SCHEMA function
        schema_result = session.sql(f"""
            SELECT COLUMN_NAME, TYPE
            FROM TABLE(INFER_SCHEMA(
                LOCATION => (SELECT ARRAY_AGG({json_column}) FROM {quoted_table_name}),
                FILE_FORMAT => 'JSON'
            ))
        """).collect()
        
        if not schema_result:
            return "-- Error: Unable to infer schema from JSON data;"
        
        # Convert inferred schema to a dictionary
        schema = {row['COLUMN_NAME']: row['TYPE'] for row in schema_result}
        
        # Generate SQL using the inferred schema
        sql = generate_sql(quoted_table_name, json_column, conditions, schema)
        
        return sql
        
    except Exception as e:
        return f"""-- Error in dynamic SQL generation
-- Error message: {str(e)}
-- Please verify your inputs and try again;"""
$$;
