CREATE OR REPLACE PROCEDURE SAINATH.SNOW.DYNAMIC_SQL_LARGE("TABLE_NAME" VARCHAR(16777216), "JSON_COLUMN" VARCHAR(16777216), "FIELD_CONDITIONS" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'dynamic_sql_generator'
EXECUTE AS OWNER
AS '
import json
from typing import Dict, Any, List, Tuple, Optional
import time

def get_snowflake_type(python_type: str) -> str:
    type_mapping = {
        ''str'': ''STRING'',
        ''int'': ''NUMBER'',
        ''float'': ''NUMBER'',
        ''bool'': ''BOOLEAN'',
        ''datetime'': ''TIMESTAMP'',
        ''date'': ''DATE'',
        ''dict'': ''VARIANT'',
        ''list'': ''ARRAY'',
        ''NoneType'': ''VARIANT'',
        ''decimal'': ''NUMBER'',
        ''time'': ''TIME'',
        ''binary'': ''BINARY'',
        ''object'': ''OBJECT''
    }
    return type_mapping.get(python_type, ''VARIANT'')

def parse_field_conditions(conditions: str) -> List[Dict]:
    result = []
    if not conditions or conditions.isspace():
        return result
    
    fields = []
    current_field = []
    bracket_count = 0
    
    for char in conditions:
        if char == ''['':
            bracket_count += 1
        elif char == '']'':
            bracket_count -= 1
        elif char == '','' and bracket_count == 0:
            fields.append(''''.join(current_field).strip())
            current_field = []
            continue
        current_field.append(char)
    
    if current_field:
        fields.append(''''.join(current_field).strip())
    
    for field in fields:
        if not field:
            continue
            
        condition = {
            ''field'': field,
            ''operator'': ''IS NOT NULL'',
            ''value'': None,
            ''cast'': None,
            ''logic_operator'': ''AND''
        }
        
        if ''['' in field and '']'' in field:
            base_field = field[:field.index(''['')].strip()
            operator_value = field[field.index(''['')+1:field.index('']'')]
            
            condition[''field''] = base_field
            subconditions = []
            
            current = []
            nested_count = 0
            
            for char in operator_value:
                if char == '','' and nested_count == 0:
                    subconditions.append(''''.join(current).strip())
                    current = []
                else:
                    if char == ''('':
                        nested_count += 1
                    elif char == '')'':
                        nested_count -= 1
                    current.append(char)
            
            if current:
                subconditions.append(''''.join(current).strip())
            
            for subcond in subconditions:
                parts = [p.strip() for p in subcond.split('':'')]
                
                if len(parts) >= 2:
                    if parts[0].upper() == ''CAST'':
                        condition[''cast''] = parts[1].upper()
                    else:
                        condition[''operator''] = parts[0]
                        # Handle multiple values for IN and NOT IN operators
                        if parts[0].upper() in (''IN'', ''NOT IN''):
                            values = [v.strip() for v in parts[1].split(''|'')]
                            condition[''value''] = values
                        # Handle BETWEEN operator
                        elif parts[0].upper() == ''BETWEEN'':
                            values = [v.strip() for v in parts[1].split(''|'')]
                            if len(values) != 2:
                                raise ValueError(f"BETWEEN operator requires exactly 2 values, got {len(values)}")
                            condition[''value''] = values
                        else:
                            condition[''value''] = parts[1]
                        
                        if len(parts) > 2:
                            condition[''logic_operator''] = parts[2].upper()
                            
        result.append(condition)
    
    return result

def validate_operator(operator: str, field_type: str) -> bool:
    operator = operator.upper()
    field_type = field_type.upper()
    
    operator_mapping = {
        ''NUMERIC'': {''<'', ''>'', ''<='', ''>='', ''='', ''!='', ''IN'', ''NOT IN'', ''BETWEEN''},
        ''STRING'': {''LIKE'', ''NOT LIKE'', ''='', ''!='', ''IN'', ''NOT IN'', ''CONTAINS'', ''NOT CONTAINS'', ''ILIKE''},
        ''DATE'': {''<'', ''>'', ''<='', ''>='', ''='', ''!='', ''BETWEEN''},
        ''BOOLEAN'': {''='', ''!='', ''IS'', ''IS NOT''},
        ''VARIANT'': {''='', ''!='', ''IS'', ''IS NOT'', ''LIKE'', ''NOT LIKE'', ''CONTAINS'', ''NOT CONTAINS'', ''<'', ''>'', ''<='', ''>='', ''IN'', ''NOT IN'', ''BETWEEN''},
        ''ARRAY'': {''='', ''!='', ''CONTAINS'', ''NOT CONTAINS''},
        ''OBJECT'': {''='', ''!='', ''IS'', ''IS NOT'', ''CONTAINS'', ''NOT CONTAINS''}
    }
    
    type_categories = {
        ''NUMBER'': ''NUMERIC'',
        ''INTEGER'': ''NUMERIC'',
        ''INT'': ''NUMERIC'',
        ''FLOAT'': ''NUMERIC'',
        ''DECIMAL'': ''NUMERIC'',
        ''STRING'': ''STRING'',
        ''VARCHAR'': ''STRING'',
        ''TEXT'': ''STRING'',
        ''CHAR'': ''STRING'',
        ''DATE'': ''DATE'',
        ''TIMESTAMP'': ''DATE'',
        ''DATETIME'': ''DATE'',
        ''BOOLEAN'': ''BOOLEAN'',
        ''BOOL'': ''BOOLEAN'',
        ''VARIANT'': ''VARIANT'',
        ''ARRAY'': ''ARRAY'',
        ''OBJECT'': ''OBJECT''
    }
    
    category = type_categories.get(field_type, ''VARIANT'')
    
    if operator in {''IS NULL'', ''IS NOT NULL''}:
        return True
        
    return operator in operator_mapping[category]

def validate_cast_type(cast_type: str) -> bool:
    valid_types = {
        ''NUMBER'', ''INTEGER'', ''INT'', ''FLOAT'', ''VARCHAR'', ''STRING'',
        ''BOOLEAN'', ''DATE'', ''TIMESTAMP'', ''VARIANT'', ''ARRAY'', ''TIME'',
        ''BINARY'', ''OBJECT'', ''TEXT'', ''CHAR''
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
            if field_type in (''NUMBER'', ''INTEGER'', ''INT'', ''FLOAT'', ''DECIMAL''):
                try:
                    float(v)
                    sanitized_values.append(str(v))
                except ValueError:
                    raise ValueError(f"Invalid numeric value: {v}")
            elif field_type in (''BOOLEAN'', ''BOOL''):
                sanitized_values.append(str(v).lower())
            elif field_type in (''DATETIME'', ''DATE'', ''TIMESTAMP''):
                sanitized_values.append(f"TO_TIMESTAMP(''{v}'')")
            else:
                sanitized_values.append(f"''{str(v).replace(chr(39), chr(39)+chr(39))}''")
        return f"({'', ''.join(sanitized_values)})"
    
    # Handle single value
    if field_type in (''NUMBER'', ''INTEGER'', ''INT'', ''FLOAT'', ''DECIMAL''):
        try:
            float(value)
            return str(value)
        except ValueError:
            raise ValueError(f"Invalid numeric value: {value}")
    
    if field_type in (''BOOLEAN'', ''BOOL''):
        return str(value).lower()
    
    if field_type in (''DATETIME'', ''DATE'', ''TIMESTAMP''):
        return f"TO_TIMESTAMP(''{value}'')"
        
    if field_type == ''ARRAY'' and isinstance(value, str):
        if value.startswith(''['') and value.endswith('']''):
            return value
        return f"[{value}]"
    
    return f"''{str(value).replace(chr(39), chr(39)+chr(39))}''"

def generate_json_schema(json_obj: Any, parent_path: str = "") -> Dict:
    schema = {}
    
    def traverse_json(obj: Any, path: str = "", array_hierarchy: List[str] = []):
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_path = f"{path}.{key}" if path else key
                schema[new_path] = {
                    "type": type(value).__name__,
                    "array_hierarchy": array_hierarchy.copy(),
                    "parent_arrays": [p for p in array_hierarchy if p != new_path]
                }
                traverse_json(value, new_path, array_hierarchy)
                
        elif isinstance(obj, list) and obj:
            schema[path] = {
                "type": "array",
                "array_hierarchy": array_hierarchy.copy(),
                "parent_arrays": [p for p in array_hierarchy if p != path]
            }
            
            new_hierarchy = array_hierarchy + [path]
            
            if isinstance(obj[0], (dict, list)):
                traverse_json(obj[0], path, new_hierarchy)
            else:
                schema[path]["item_type"] = type(obj[0]).__name__
                
    traverse_json(json_obj, parent_path)
    return schema

def find_field_details(schema: Dict, target_field: str) -> List[Tuple[str, List[str]]]:
    matching_paths = []
    
    # First look for exact matches
    if target_field in schema:
        matching_paths.append((target_field, schema[target_field].get(''array_hierarchy'', [])))
    
    # Then look for the field as part of a longer path
    for path, info in schema.items():
        path_parts = path.split(''.'')
        if path_parts[-1] == target_field and path != target_field:
            matching_paths.append((path, info.get(''array_hierarchy'', [])))
    
    if not matching_paths:
        raise ValueError(f"Field ''{target_field}'' not found in JSON structure")
    
    return matching_paths

def build_array_flattening(array_paths: List[str], json_column: str) -> Tuple[str, Dict[str, str]]:
    flatten_clauses = []
    array_aliases = {}
    
    sorted_array_paths = sorted(array_paths, key=lambda x: len(x.split(''.'')))
    
    for idx, array_path in enumerate(sorted_array_paths):
        alias = f"f{idx + 1}"
        array_aliases[array_path] = alias
        
        parent_path = next((p for p in sorted_array_paths if array_path.startswith(p + ''.'') and p != array_path), None)
        
        if parent_path:
            parent_alias = array_aliases[parent_path]
            relative_path = array_path[len(parent_path) + 1:]
            flatten_clauses.append(f", LATERAL FLATTEN(input => {parent_alias}.value:{relative_path}) {alias}")
        else:
            flatten_clauses.append(f", LATERAL FLATTEN(input => {json_column}:{array_path}) {alias}")
    
    return ''''.join(flatten_clauses), array_aliases

def build_field_path(field_path: str, json_column: str, array_aliases: Dict[str, str], array_hierarchy: List[str]) -> str:
    if not array_hierarchy:
        return f"{json_column}:{field_path}"
    
    deepest_array = array_hierarchy[-1]
    field_suffix = field_path[len(deepest_array) + 1:] if field_path.startswith(deepest_array + ''.'') else field_path
    
    return f"{array_aliases[deepest_array]}.value{'':'' + field_suffix if field_suffix else ''''}"

def generate_sql(table_name: str, json_column: str, field_conditions: List[Dict], schema: Dict) -> str:
    select_parts = []
    where_conditions = []
    all_array_paths = set()
    field_paths_map = {}
    
    # Find all possible paths for each field
    for condition in field_conditions:
        field = condition[''field'']
        matching_paths = find_field_details(schema, field)
        field_paths_map[field] = matching_paths
        
        # Add array paths from all matches
        for _, array_hierarchy in matching_paths:
            all_array_paths.update(array_hierarchy)
    
    flatten_clauses, array_aliases = build_array_flattening(list(all_array_paths), json_column)
    
    first_condition = True
    for condition in field_conditions:
        field = condition[''field'']
        matching_paths = field_paths_map[field]
        
        # Use all matching paths in SELECT
        for idx, (full_path, array_hierarchy) in enumerate(matching_paths):
            value_path = build_field_path(full_path, json_column, array_aliases, array_hierarchy)
            alias = f"{field}_{idx + 1}" if len(matching_paths) > 1 else field
            
            if condition[''cast'']:
                if not validate_cast_type(condition[''cast'']):
                    raise ValueError(f"Invalid cast type: {condition[''cast'']}")
                field_type = condition[''cast'']
                cast_expr = f"CAST({value_path} AS {condition[''cast'']})"
            else:
                field_type = get_snowflake_type(schema[full_path][''type''])
                cast_expr = value_path
            
            select_parts.append(f"{cast_expr} as {alias}")
            
            # Add WHERE conditions for each path
            if condition[''operator''] != ''IS NOT NULL'':
                operator = condition[''operator''].upper()
                if not validate_operator(operator, field_type):
                    raise ValueError(f"Invalid operator ''{operator}'' for field type ''{field_type}''")
                
                # Handle BETWEEN operator
                if operator == ''BETWEEN'' and isinstance(condition[''value''], list):
                    start_val = sanitize_value(condition[''value''][0], field_type)
                    end_val = sanitize_value(condition[''value''][1], field_type)
                    where_clause = f"{cast_expr} BETWEEN {start_val} AND {end_val}"
                else:
                    # Handle other operators including IN, NOT IN
                    sanitized_value = sanitize_value(condition[''value''], field_type)
                    
                    if operator in (''LIKE'', ''NOT LIKE'', ''ILIKE''):
                        cast_expr = f"CAST({value_path} AS STRING)"
                    
                    where_clause = f"{cast_expr} {operator} {sanitized_value}"
                
                if first_condition:
                    where_conditions.append(where_clause)
                    first_condition = False
                else:
                    where_conditions.append(f"{condition[''logic_operator'']} {where_clause}")
    
    sql = f"SELECT {'', ''.join(select_parts)}\\nFROM {table_name}"
    
    if flatten_clauses:
        sql += flatten_clauses
    
    if where_conditions:
        sql += f"\\nWHERE {'' ''.join(where_conditions)}"
    
    return sql + ";"
	
# Cache to store the generated JSON schema
schema_cache: Dict[Tuple[str, str], Dict] = {}

def dynamic_sql_generator(session, table_name: str, json_column: str, field_conditions: str) -> str:
    try:
        if not all([table_name, json_column]):
            raise ValueError("Table name and JSON column are required")
            
        quoted_table_name = f''"{table_name}"''
        
        try:
            conditions = parse_field_conditions(field_conditions)
        except Exception as e:
            return f"-- Error parsing field conditions: {str(e)};"
        
        # Check the cache for the JSON schema
        schema_key = (table_name, json_column)
        if schema_key in schema_cache:
            schema = schema_cache[schema_key]
        else:
            # Fetch and parse the JSON data in batches
            max_retries = 3
            retry_count = 0
            batch_size = 100
            schema = {}
            
            while retry_count < max_retries:
                try:
                    result = session.sql(f"SELECT {json_column} FROM {quoted_table_name} WHERE {json_column} IS NOT NULL LIMIT {batch_size}").collect()
                    if not result:
                        return "-- Error: No data found in the specified table/column;"
                    
                    for row in result:
                        json_data = json.loads(row[json_column])
                        schema.update(generate_json_schema(json_data))
                    
                    # Cache the generated schema
                    schema_cache[schema_key] = schema
                    break
                except Exception as e:
                    retry_count += 1
                    if retry_count == max_retries:
                        return f"-- Error accessing table data after {max_retries} attempts: {str(e)};"
                    continue
        
        sql = generate_sql(quoted_table_name, json_column, conditions, schema)
        
        return sql
        
    except Exception as e:
        return f"""-- Error in dynamic SQL generation
-- Error message: {str(e)}
-- Please verify your inputs and try again;"""
';
