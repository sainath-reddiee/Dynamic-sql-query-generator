"""
Pure Python implementation of dynamic SQL generation
Replicates the logic from your Snowflake stored procedure
"""
import json
from typing import Dict, Any, List, Tuple, Optional
import re
import logging

logger = logging.getLogger(__name__)

class PythonSQLGenerator:
    def __init__(self):
        self.type_mapping = {
            'str': 'VARCHAR',
            'int': 'NUMBER',
            'float': 'NUMBER',
            'bool': 'BOOLEAN',
            'dict': 'OBJECT',
            'list': 'ARRAY',
            'NoneType': 'VARIANT'
        }

    def sanitize_input(self, value: str) -> str:
        """Sanitize input strings to prevent SQL injection"""
        if not isinstance(value, str):
            return str(value)
        # Replace quotes and remove dangerous characters
        value = value.replace("'", "''").replace('"', '""')
        value = re.sub(r'[;\x00-\x1f]', '', value)
        return value

    def analyze_json_for_sql(self, json_obj: Any, parent_path: str = "") -> Dict[str, Dict]:
        """Analyze JSON and create SQL-ready schema - matches your Snowflake procedure logic"""
        schema = {}

        def traverse(obj: Any, path: str = "", array_hierarchy: List[str] = [], depth: int = 0):
            if depth > 10:  # Prevent infinite recursion
                return

            if isinstance(obj, dict):
                for key, value in obj.items():
                    new_path = f"{path}.{key}" if path else key
                    current_type = type(value).__name__

                    # Enhanced schema with metadata (matching your procedure)
                    schema_entry = {
                        "type": current_type,
                        "snowflake_type": self.type_mapping.get(current_type, 'VARIANT'),
                        "array_hierarchy": array_hierarchy.copy(),
                        "depth": len(new_path.split('.')),
                        "full_path": new_path,
                        "parent_path": path,
                        "is_queryable": not isinstance(value, (dict, list)),
                        "sample_value": str(value)[:100] if value is not None else "NULL"
                    }

                    # Handle type conflicts (matching your procedure logic)
                    if new_path in schema:
                        existing_type = schema[new_path]["type"]
                        if existing_type != current_type:
                            if current_type in ['str', 'int', 'float'] and existing_type == 'NoneType':
                                schema[new_path]["type"] = current_type
                                schema[new_path]["snowflake_type"] = self.type_mapping.get(current_type, 'VARIANT')
                            elif existing_type in ['str', 'int', 'float'] and current_type == 'NoneType':
                                pass  # Keep existing
                            else:
                                schema[new_path]["type"] = "variant"
                                schema[new_path]["snowflake_type"] = "VARIANT"
                    else:
                        schema[new_path] = schema_entry

                    traverse(value, new_path, array_hierarchy, depth + 1)

            elif isinstance(obj, list) and obj:
                # Add array info to schema
                if path not in schema:
                    schema[path] = {
                        "type": "array",
                        "snowflake_type": "ARRAY",
                        "array_hierarchy": array_hierarchy.copy(),
                        "depth": len(path.split('.')) if path else 0,
                        "full_path": path,
                        "parent_path": ".".join(path.split('.')[:-1]) if '.' in path else "",
                        "is_queryable": True,
                        "sample_value": f"Array with {len(obj)} items"
                    }

                new_hierarchy = array_hierarchy + [path]

                # Process array elements (sample up to 3 like your procedure)
                sample_size = min(len(obj), 3)
                for i in range(sample_size):
                    if isinstance(obj[i], (dict, list)):
                        traverse(obj[i], path, new_hierarchy, depth + 1)
                    else:
                        schema[path]["item_type"] = type(obj[i]).__name__

        traverse(json_obj, parent_path)
        return schema

    def parse_field_conditions(self, conditions: str) -> List[Dict]:
        """Parse field conditions - replicates your Snowflake procedure logic"""
        if not conditions or not conditions.strip():
            return []

        result = []
        fields = []
        current_field = []
        bracket_count = 0

        # Parse comma-separated fields with bracket handling
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

            # Parse conditions like: field[operator:value] or field[CAST:TYPE]
            if '[' in field and ']' in field:
                base_field = field[:field.index('[')].strip()
                operator_value = field[field.index('[')+1:field.index(']')]
                condition['field'] = base_field

                # Handle multiple subconditions
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
                            condition['operator'] = parts[0].upper()
                            if parts[0].upper() in ('IN', 'NOT IN'):
                                values = [v.strip() for v in parts[1].split('|')]
                                condition['value'] = values
                            elif parts[0].upper() == 'BETWEEN':
                                values = [v.strip() for v in parts[1].split('|')]
                                condition['value'] = values
                            else:
                                condition['value'] = parts[1]

                            if len(parts) > 2:
                                condition['logic_operator'] = parts[2].upper()

            result.append(condition)

        return result

    def find_field_in_schema(self, schema: Dict, target_field: str) -> List[Tuple[str, Dict]]:
        """Find field in schema - matches your procedure's field resolution"""
        matching_paths = []

        # Find all paths that end with the target field
        candidates = []
        for path, info in schema.items():
            path_parts = path.split('.')
            if path_parts[-1] == target_field or path == target_field:
                candidates.append((path, info))

        if not candidates:
            # Try partial matching
            for path, info in schema.items():
                if target_field.lower() in path.lower():
                    candidates.append((path, info))

        if not candidates:
            return []

        # Prefer shorter paths (less nested)
        candidates.sort(key=lambda x: (x[1].get('depth', 0), len(x[1].get('array_hierarchy', []))))

        return candidates[:1]  # Return best match

    def build_array_flattening(self, array_paths: List[str], json_column: str) -> Tuple[str, Dict[str, str]]:
        """Build LATERAL FLATTEN clauses - matches your procedure logic"""
        flatten_clauses = []
        array_aliases = {}

        # Sort by depth to ensure proper nesting order
        sorted_array_paths = sorted(set(array_paths), key=lambda x: (len(x.split('.')), x))

        for idx, array_path in enumerate(sorted_array_paths):
            alias = f"f{idx + 1}"
            array_aliases[array_path] = alias

            # Check for parent-child relationship
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
                safe_relative_path = self.sanitize_input(relative_path)
                flatten_clauses.append(f", LATERAL FLATTEN(input => {parent_alias}.value:{safe_relative_path}) {alias}")
            else:
                safe_array_path = self.sanitize_input(array_path)
                flatten_clauses.append(f", LATERAL FLATTEN(input => {json_column}:{safe_array_path}) {alias}")

        return ''.join(flatten_clauses), array_aliases

    def build_field_reference(self, field_path: str, json_column: str,
                            array_aliases: Dict[str, str], array_hierarchy: List[str]) -> str:
        """Build field reference path - matches your procedure logic"""
        if not array_hierarchy:
            safe_field_path = self.sanitize_input(field_path)
            return f"{json_column}:{safe_field_path}"

        deepest_array = array_hierarchy[-1]
        field_suffix = field_path[len(deepest_array) + 1:] if field_path.startswith(deepest_array + '.') else field_path

        if deepest_array in array_aliases:
            if field_suffix:
                safe_field_suffix = self.sanitize_input(field_suffix)
                return f"{array_aliases[deepest_array]}.value:{safe_field_suffix}"
            else:
                return f"{array_aliases[deepest_array]}.value"
        else:
            safe_field_path = self.sanitize_input(field_path)
            return f"{json_column}:{safe_field_path}"

    def sanitize_value(self, value: Any, field_type: str) -> str:
        """Sanitize values for SQL - matches your procedure logic"""
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
                        sanitized_values.append(f"'{self.sanitize_input(str(v))}'")
                else:
                    sanitized_values.append(f"'{self.sanitize_input(str(v))}'")
            return f"({', '.join(sanitized_values)})"

        if field_type in ('NUMBER', 'INTEGER', 'INT', 'FLOAT', 'DECIMAL'):
            try:
                float(value)
                return str(value)
            except ValueError:
                return f"'{self.sanitize_input(str(value))}'"

        return f"'{self.sanitize_input(str(value))}'"

    def generate_dynamic_sql(self, table_name: str, json_column: str,
                           field_conditions: str, schema: Dict) -> str:
        """Generate complete SQL query - matches your Snowflake procedure logic"""
        try:
            conditions = self.parse_field_conditions(field_conditions)

            if not conditions:
                return "-- No field conditions provided. Please specify fields to query."

            select_parts = []
            where_conditions = []
            field_where_conditions = []
            all_array_paths = set()
            field_paths_map = {}

            # Process conditions and build path mappings
            for condition in conditions:
                field = condition['field']
                matching_paths = self.find_field_in_schema(schema, field)
                if matching_paths:
                    field_paths_map[field] = matching_paths

                    for path, info in matching_paths:
                        array_hierarchy = info.get('array_hierarchy', [])
                        all_array_paths.update(array_hierarchy)

            # Build flattening clauses
            flatten_clauses, array_aliases = self.build_array_flattening(list(all_array_paths), json_column)

            # Process each condition
            for condition in conditions:
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
                value_path = self.build_field_reference(full_path, json_column, array_aliases, array_hierarchy)

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
                        start_val = self.sanitize_value(condition['value'][0], field_type)
                        end_val = self.sanitize_value(condition['value'][1], field_type)
                        where_clause = f"{cast_expr} BETWEEN {start_val} AND {end_val}"
                    elif condition['value'] is not None:
                        sanitized_value = self.sanitize_value(condition['value'], field_type)
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
            safe_table_name = self.sanitize_input(table_name)

            if not select_parts:
                return "-- No valid fields found for selection. Please check your field names against the JSON structure."

            sql = f"SELECT {', '.join(select_parts)}"
            sql += f"\nFROM {safe_table_name}"

            if flatten_clauses:
                sql += flatten_clauses

            if where_conditions:
                sql += f"\nWHERE {' '.join(where_conditions)}"

            return sql + ";"

        except Exception as e:
            logger.error(f"Error generating SQL: {e}")
            return f"-- Error generating SQL: {str(e)}\\n-- Please check your field conditions and try again."


def generate_sql_from_json_data(json_data: Any, table_name: str,
                               json_column: str, field_conditions: str) -> str:
    """
    Main function to generate SQL from JSON data
    This replaces your Snowflake procedure call in Streamlit
    """
    try:
        generator = PythonSQLGenerator()
        schema = generator.analyze_json_for_sql(json_data)
        sql = generator.generate_dynamic_sql(table_name, json_column, field_conditions, schema)
        return sql
    except Exception as e:
        logger.error(f"Failed to generate SQL from JSON data: {e}")
        return f"-- Error: Failed to generate SQL\\n-- {str(e)}"
