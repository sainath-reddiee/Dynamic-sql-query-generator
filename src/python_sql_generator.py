import json
from typing import Dict, Any, List, Optional, Tuple
import re
import logging

logger = logging.getLogger(__name__)


class PythonSQLGenerator:
    """
    FIXED: SQL Generator with proper LATERAL FLATTEN alias management
    - No duplicate aliases when extracting multiple fields from same array
    - Works with any JSON structure dynamically
    """
    
    def __init__(self):
        self.flatten_counter = 0
        self.array_hierarchy = []
        self.flatten_alias_map = {}  # NEW: Track aliases to prevent duplicates
    
    def analyze_json_for_sql(self, json_obj: Any, parent_path: str = "") -> Dict[str, Dict]:
        """
        DYNAMIC: Analyze ANY JSON structure for SQL generation
        """
        schema = {}
        
        def traverse_json(obj: Any, path: str = "", depth: int = 0, in_array_context: List[str] = []):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    new_path = f"{path}.{key}" if path else key
                    current_type = type(value).__name__
                    
                    # DYNAMIC: Determine queryability based on actual structure
                    is_queryable = not isinstance(value, (dict, list)) or (
                        isinstance(value, list) and len(value) > 0 and not isinstance(value[0], (dict, list))
                    )
                    
                    schema_entry = {
                        "type": current_type,
                        "snowflake_type": self._get_snowflake_type(current_type),
                        "is_queryable": is_queryable,
                        "is_array": isinstance(value, list),
                        "is_nested_object": isinstance(value, dict),
                        "array_context": in_array_context.copy(),
                        "depth": depth,
                        "full_path": new_path,
                        "sample_value": str(value)[:100] if len(str(value)) <= 100 else str(value)[:100] + "..."
                    }
                    
                    schema[new_path] = schema_entry
                    
                    # DYNAMIC: Recursively analyze any nested structures
                    if isinstance(value, dict):
                        traverse_json(value, new_path, depth + 1, in_array_context)
                    elif isinstance(value, list) and value:
                        # DYNAMIC: Analyze array elements regardless of content
                        if isinstance(value[0], (dict, list)):
                            new_array_context = in_array_context + [new_path]
                            traverse_json(value[0], new_path, depth + 1, new_array_context)
                        
            elif isinstance(obj, list) and obj:
                # DYNAMIC: Handle array elements of any type
                if isinstance(obj[0], (dict, list)):
                    new_array_context = in_array_context + [path] if path else in_array_context
                    traverse_json(obj[0], path, depth, new_array_context)
        
        traverse_json(json_obj, parent_path)
        return schema
    
    def _get_snowflake_type(self, python_type: str) -> str:
        """DYNAMIC: Map any Python type to appropriate Snowflake type"""
        type_mapping = {
            'str': 'VARCHAR',
            'int': 'NUMBER',
            'float': 'FLOAT',
            'bool': 'BOOLEAN',
            'list': 'ARRAY',
            'dict': 'OBJECT',
            'NoneType': 'VARCHAR'
        }
        return type_mapping.get(python_type, 'VARIANT')
    
    def generate_dynamic_sql(self, table_name: str, json_column: str, field_conditions: str, schema: Dict[str, Dict]) -> str:
        """
        FIXED: Generate SQL with proper alias management - no duplicates!
        """
        try:
            # Reset alias tracking for each SQL generation
            self.flatten_alias_map = {}
            
            # DYNAMIC: Parse any field conditions format
            fields_info = self._parse_field_conditions_dynamic(field_conditions, schema)
            if not fields_info:
                return "-- Error: No valid fields found in conditions"
            
            # FIXED: Build SQL components with proper alias management
            select_clause = self._build_select_clause_fixed(fields_info, schema, json_column)
            from_clause = self._build_from_clause_fixed(table_name, json_column, fields_info, schema)
            where_clause = self._build_where_clause_fixed(fields_info, schema, json_column)
            
            sql_parts = [select_clause, from_clause]
            if where_clause.strip():
                sql_parts.append(where_clause)
            
            return '\n'.join(sql_parts) + ';'
            
        except Exception as e:
            logger.error(f"Dynamic SQL generation failed: {e}")
            return f"-- Error generating SQL: {str(e)}"
    
    def _parse_field_conditions_dynamic(self, field_conditions: str, schema: Dict[str, Dict]) -> List[Dict]:
        """DYNAMIC: Parse any field conditions format"""
        fields_info = []
        
        # DYNAMIC: Split and process any condition format
        conditions = [c.strip() for c in field_conditions.split(',')]
        
        for condition in conditions:
            if not condition:
                continue
                
            # DYNAMIC: Parse field[operator:value] or just field
            if '[' in condition and ']' in condition:
                field_path = condition.split('[')[0].strip()
                condition_part = condition[condition.index('[') + 1:condition.rindex(']')]
                
                if ':' in condition_part:
                    operator, value = condition_part.split(':', 1)
                    operator = operator.strip()
                    value = value.strip()
                else:
                    operator = condition_part.strip()
                    value = None
            else:
                field_path = condition.strip()
                operator = None
                value = None
            
            # DYNAMIC: Find matching schema entry by any path pattern
            schema_entry = None
            matched_path = None
            
            # Try exact match first
            if field_path in schema:
                schema_entry = schema[field_path]
                matched_path = field_path
            else:
                # Try partial matches - check if field_path matches end of any schema path
                for schema_path, details in schema.items():
                    if (schema_path == field_path or 
                        schema_path.endswith('.' + field_path) or
                        field_path in schema_path):
                        schema_entry = details
                        matched_path = schema_path
                        break
            
            # Only include queryable fields
            if schema_entry and schema_entry.get('is_queryable', False):
                fields_info.append({
                    'field_path': matched_path,
                    'original_field': field_path,
                    'operator': operator,
                    'value': value,
                    'schema_entry': schema_entry
                })
        
        return fields_info
    
    def _build_select_clause_fixed(self, fields_info: List[Dict], schema: Dict[str, Dict], json_column: str) -> str:
        """
        FIXED: Build SELECT clause with proper alias references
        """
        select_parts = []
        
        for field_info in fields_info:
            field_path = field_info['field_path']
            schema_entry = field_info['schema_entry']
            snowflake_type = schema_entry.get('snowflake_type', 'VARCHAR')
            
            # DYNAMIC: Handle any array context structure
            array_context = schema_entry.get('array_context', [])
            
            if array_context:
                # FIXED: Get the correct flatten alias for this array context
                flatten_alias = self._get_flatten_alias(array_context)
                
                # DYNAMIC: Calculate relative path from the flattened context
                relative_path = self._calculate_relative_path_dynamic(field_path, array_context)
                sql_path = f"{flatten_alias}.value:{relative_path}::{snowflake_type}"
            else:
                # DYNAMIC: Field is at root level - use direct JSON path
                json_path = field_path.replace('.', ':')
                sql_path = f"{json_column}:{json_path}::{snowflake_type}"
            
            # DYNAMIC: Create alias from field name (last part of path)
            alias = field_path.split('.')[-1]
            select_parts.append(f"{sql_path} as {alias}")
        
        return f"SELECT {', '.join(select_parts)}"
    
    def _build_from_clause_fixed(self, table_name: str, json_column: str, fields_info: List[Dict], schema: Dict[str, Dict]) -> str:
        """
        CRITICAL FIX: Build FROM clause WITHOUT duplicate LATERAL FLATTEN aliases
        Groups fields by array context and reuses aliases efficiently!
        """
        from_parts = [table_name]
        
        # FIXED: Collect unique array contexts and determine flatten requirements
        required_flattens = {}  # array_context_tuple -> flatten_info
        
        for field_info in fields_info:
            array_context = field_info['schema_entry'].get('array_context', [])
            
            # Process each level of nesting
            for i, context in enumerate(array_context):
                context_tuple = tuple(array_context[:i+1])
                
                if context_tuple not in required_flattens:
                    required_flattens[context_tuple] = {
                        'level': i,
                        'array_path': context,
                        'full_context': array_context[:i+1]
                    }
        
        # FIXED: Sort by nesting level and build LATERAL FLATTEN clauses
        sorted_flattens = sorted(required_flattens.items(), key=lambda x: x[1]['level'])
        
        for context_tuple, flatten_info in sorted_flattens:
            level = flatten_info['level']
            array_path = flatten_info['array_path']
            
            # FIXED: Get or create flatten alias (reuse existing if available)
            flatten_alias = self._get_flatten_alias(flatten_info['full_context'])
            
            if level == 0:
                # DYNAMIC: First level - flatten from the main JSON column
                clean_path = array_path.replace('.', ':')
                flatten_input = f"{json_column}:{clean_path}"
            else:
                # FIXED: Subsequent levels - use correct parent alias
                parent_context = tuple(flatten_info['full_context'][:level])
                parent_alias = self._get_flatten_alias(list(parent_context))
                
                # DYNAMIC: Calculate relative path from parent to current array
                parent_array_path = flatten_info['full_context'][level-1]
                
                if array_path.startswith(parent_array_path + '.'):
                    relative_path = array_path[len(parent_array_path) + 1:]
                else:
                    relative_path = array_path.split('.')[-1]
                
                flatten_input = f"{parent_alias}.value:{relative_path}"
            
            from_parts.append(f"LATERAL FLATTEN(input => {flatten_input}) {flatten_alias}")
        
        return f"FROM {', '.join(from_parts)}"
    
    def _get_flatten_alias(self, array_context: List[str]) -> str:
        """
        FIXED: Get or create flatten alias, reusing existing aliases for same context
        This prevents duplicate LATERAL FLATTEN clauses!
        """
        context_key = tuple(array_context)
        
        if context_key in self.flatten_alias_map:
            return self.flatten_alias_map[context_key]
        
        # Create new alias
        alias = f"f{len(array_context)}"
        self.flatten_alias_map[context_key] = alias
        
        return alias
    
    def _calculate_relative_path_dynamic(self, full_path: str, array_context: List[str]) -> str:
        """DYNAMIC: Calculate relative path within flattened context for any structure"""
        if not array_context:
            return full_path.replace('.', ':')
        
        # DYNAMIC: Find the deepest array context that contains this field
        deepest_array = array_context[-1]
        
        # DYNAMIC: Remove array path to get relative path
        if full_path.startswith(deepest_array + '.'):
            relative_path = full_path[len(deepest_array) + 1:]
        else:
            # DYNAMIC: Fallback - use the field name
            relative_path = full_path.split('.')[-1]
        
        return relative_path.replace('.', ':')
    
    def _build_where_clause_fixed(self, fields_info: List[Dict], schema: Dict[str, Dict], json_column: str) -> str:
        """
        FIXED: Build WHERE clause with proper alias references
        """
        where_conditions = []
        
        for field_info in fields_info:
            operator = field_info.get('operator')
            value = field_info.get('value')
            
            if not operator:
                continue
            
            field_path = field_info['field_path']
            schema_entry = field_info['schema_entry']
            array_context = schema_entry.get('array_context', [])
            
            # FIXED: Build SQL reference for WHERE clause with correct aliases
            if array_context:
                flatten_alias = self._get_flatten_alias(array_context)
                relative_path = self._calculate_relative_path_dynamic(field_path, array_context)
                sql_ref = f"{flatten_alias}.value:{relative_path}"
            else:
                json_path = field_path.replace('.', ':')
                sql_ref = f"{json_column}:{json_path}"
            
            # DYNAMIC: Build condition based on any operator
            if operator.upper() == 'IS NOT NULL':
                where_conditions.append(f"{sql_ref} IS NOT NULL")
            elif operator == '=':
                where_conditions.append(f"{sql_ref}::VARCHAR = '{value}'")
            elif operator == '>':
                where_conditions.append(f"{sql_ref}::NUMBER > {value}")
            elif operator == '<':
                where_conditions.append(f"{sql_ref}::NUMBER < {value}")
            elif operator.upper() == 'IN':
                values_list = [f"'{v.strip()}'" for v in value.split('|')]
                where_conditions.append(f"{sql_ref}::VARCHAR IN ({', '.join(values_list)})")
            elif operator.upper() == 'LIKE':
                where_conditions.append(f"{sql_ref}::VARCHAR LIKE '%{value}%'")
        
        return f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""


def generate_sql_from_json_data(json_data: Any, table_name: str, json_column: str, field_conditions: str) -> str:
    """
    FIXED: Generate SQL from any JSON data structure with proper alias management
    This is the main function called by your application
    """
    try:
        generator = PythonSQLGenerator()
        
        # DYNAMIC: Analyze any JSON structure
        schema = generator.analyze_json_for_sql(json_data)
        
        if not schema:
            return "-- Error: Could not analyze JSON structure"
        
        # FIXED: Generate SQL with proper alias management
        return generator.generate_dynamic_sql(table_name, json_column, field_conditions, schema)
        
    except Exception as e:
        logger.error(f"SQL generation from JSON data failed: {e}")
        return f"-- Error: {str(e)}"


# Main function for external usage
if __name__ == "__main__":
    logger.info("Python SQL Generator module loaded successfully")
