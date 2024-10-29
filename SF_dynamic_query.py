CREATE OR REPLACE PROCEDURE SAINATH.SNOW.SF_DYNAMIC_SQL("TABLE_NAME" VARCHAR(16777216), "JSON_COLUMN" VARCHAR(16777216), "FIELD_NAMES" VARCHAR(16777216), "INCLUDE_METADATA" BOOLEAN)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'generate_sql_queries'
EXECUTE AS OWNER
AS '
import json
from typing import Dict, Any, List, Tuple

def generate_json_schema(json_obj: Any) -> Dict:
    """
    Generate a complete schema of the JSON structure with array path tracking
    """
    schema = {}
    
    def traverse_json(obj: Any, path: str = "", parent_arrays: List[str] = []):
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_path = f"{path}.{key}" if path else key
                schema[new_path] = {
                    "type": type(value).__name__,
                    "array_path": parent_arrays.copy()
                }
                traverse_json(value, new_path, parent_arrays)
        elif isinstance(obj, list) and obj:
            schema[path] = {
                "type": "array",
                "array_path": parent_arrays.copy()
            }
            if isinstance(obj[0], (dict, list)):
                new_arrays = parent_arrays + [path]
                traverse_json(obj[0], path, new_arrays)
            else:
                schema[path]["item_type"] = type(obj[0]).__name__
    
    traverse_json(json_obj)
    return schema

def find_field_path(schema: Dict, target_field: str) -> Tuple[str, List[str]]:
    """
    Find the correct path to a field and its array hierarchy
    """
    possible_paths = []
    for path, info in schema.items():
        path_parts = path.split(''.'')
        if path_parts[-1] == target_field:
            possible_paths.append((path, info))
    
    if not possible_paths:
        return None, []
    
    # Sort by the number of array paths and then by the length of the full path
    best_path = sorted(
        possible_paths, 
        key=lambda x: (len(x[1][''array_path'']), len(x[0].split(''.''))),
        reverse=True
    )[0]
    
    return best_path[0], best_path[1][''array_path'']

def generate_sql_for_path(
    table_name: str,
    json_column: str,
    field_path: str,
    array_paths: List[str],
    include_metadata: bool
) -> str:
    """
    Generate SQL query with proper array flattening
    """
    select_parts = []
    
    if include_metadata:
        select_parts.extend([
            f"''{field_path}'' as FIELD_PATH",
            f"''{''array'' if array_paths else ''scalar''}'' as FIELD_TYPE"
        ])
    
    path_parts = field_path.split(''.'')
    final_field = path_parts[-1]
    
    if array_paths:
        # Get the remaining path after the last array
        last_array = array_paths[-1]
        last_array_parts = last_array.split(''.'')
        remaining_path = ''.''.join(path_parts[len(last_array_parts):])
        
        # If there''s a remaining path, append it to the value reference
        value_path = f"f{len(array_paths)}.value"
        if remaining_path:
            value_path += f":{remaining_path}"
            
        select_parts.append(f"{value_path} as {final_field}")
        
        sql = f"SELECT {'', ''.join(select_parts)}\\nFROM {table_name}"
        
        # Generate FLATTEN operations
        for idx, array_path in enumerate(array_paths):
            alias = f"f{idx+1}"
            if idx == 0:
                sql += f"\\n  ,LATERAL FLATTEN(input => {json_column}:{array_path}) {alias}"
            else:
                prev_alias = f"f{idx}"
                prev_array_parts = array_paths[idx-1].split(''.'')
                current_array_parts = array_path.split(''.'')
                relative_path = ''.''.join(current_array_parts[len(prev_array_parts):])
                sql += f"\\n  ,LATERAL FLATTEN(input => {prev_alias}.value{'':'' + relative_path if relative_path else ''''}) {alias}"
    else:
        select_parts.append(f"{json_column}:{field_path} as {final_field}")
        sql = f"SELECT {'', ''.join(select_parts)}\\nFROM {table_name}"
    
    return sql

def generate_sql_queries(
    session,
    table_name: str,
    json_column: str,
    field_names: str,
    include_metadata: bool = False
) -> str:
    """
    Main function to generate SQL queries
    """
    quoted_table_name = f''"{table_name}"''
    
    try:
        result = session.sql(
            f''SELECT {json_column} FROM {quoted_table_name} LIMIT 1''
        ).collect()
        
        if not result:
            return "Error: No data found in the specified table/column"
        
        try:
            json_data = json.loads(result[0][json_column])
        except json.JSONDecodeError:
            return "Error: Invalid JSON format in the column data"
        
        schema = generate_json_schema(json_data)
        fields = [f.strip() for f in field_names.split('','')]
        sql_queries = []
        
        for field in fields:
            try:
                field_path, array_paths = find_field_path(schema, field)
                
                if not field_path:
                    sql_queries.append(
                        f"-- Error: Field ''{field}'' not found in the JSON structure"
                    )
                    continue
                
                sql = generate_sql_for_path(
                    quoted_table_name,
                    json_column,
                    field_path,
                    array_paths,
                    include_metadata
                )
                sql_queries.append(f"{sql};")
                
            except Exception as e:
                sql_queries.append(
                    f"-- Error generating SQL for ''{field}'': {str(e)}"
                )
        
        return "\\n\\n".join(sql_queries)
        
    except Exception as e:
        return f"-- Error: Failed to process table data: {str(e)}"
';
