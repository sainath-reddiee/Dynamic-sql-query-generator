import json
from typing import Dict, Any, List, Optional, Tuple
import re
import logging

logger = logging.getLogger(__name__)


class PythonSQLGenerator:
    def __init__(self):
        self.flatten_counter = 0
        self.array_hierarchy = []
        self.flatten_alias_map = {}
        self.multi_level_fields = {}  # Track fields that appear at multiple levels
        self.path_usage_tracker = {}
    
    def analyze_json_for_sql(self, json_obj: Any, parent_path: str = "") -> Dict[str, Dict]:
        schema = {}
        field_name_tracker = {}  # Track where each field name appears
        
        def traverse_json(obj: Any, path: str = "", depth: int = 0, in_array_context: List[str] = []):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    new_path = f"{path}.{key}" if path else key
                    current_type = type(value).__name__
                    
                    # Track field name occurrences for multi-level detection
                    if key not in field_name_tracker:
                        field_name_tracker[key] = []
                    field_name_tracker[key].append({
                        'full_path': new_path,
                        'depth': depth,
                        'in_array': len(in_array_context) > 0,
                        'array_context': in_array_context.copy(),
                        'parent_path': path,
                        'context_description': self._get_context_description(new_path, in_array_context)
                    })
                    
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
                        "field_name": key,
                        "parent_context": path.split('.')[-1] if path else 'root',
                        "sample_value": str(value)[:100] if len(str(value)) <= 100 else str(value)[:100] + "...",
                        "context_description": self._get_context_description(new_path, in_array_context)
                    }
                    
                    schema[new_path] = schema_entry
                    
                    if isinstance(value, dict):
                        traverse_json(value, new_path, depth + 1, in_array_context)
                    elif isinstance(value, list) and value:
                        if isinstance(value[0], (dict, list)):
                            new_array_context = in_array_context + [new_path]
                            traverse_json(value[0], new_path, depth + 1, new_array_context)
                        
            elif isinstance(obj, list) and obj:
                if isinstance(obj[0], (dict, list)):
                    new_array_context = in_array_context + [path] if path else in_array_context
                    traverse_json(obj[0], path, depth, new_array_context)
        
        traverse_json(json_obj, parent_path)
        
        # Generate multi-level field information
        self.multi_level_fields = self._create_multi_level_field_map(field_name_tracker, schema)
        
        return schema
    
    def _get_context_description(self, full_path: str, array_context: List[str]) -> str:
        """Generate human-readable context description"""
        parts = full_path.split('.')
        field_name = parts[-1]
        
        if not array_context:
            if len(parts) == 1:
                return f"{field_name}_company_level"
            else:
                parent = parts[-2]
                return f"{field_name}_under_{parent}"
        elif len(array_context) == 1:
            array_name = array_context[0].split('.')[-1]
            return f"{field_name}_in_each_{array_name.rstrip('s')}"
        else:
            nested_arrays = [ctx.split('.')[-1] for ctx in array_context]
            return f"{field_name}_in_nested_{nested_arrays[-1].rstrip('s')}"
    
    def _create_multi_level_field_map(self, field_name_tracker: Dict, schema: Dict) -> Dict:
        """Create multi-level field map for fields that appear at multiple levels"""
        multi_level_map = {}
        
        for field_name, occurrences in field_name_tracker.items():
            queryable_occurrences = [
                occ for occ in occurrences 
                if schema[occ['full_path']]['is_queryable']
            ]
            
            if len(queryable_occurrences) > 1:
                # Field appears at multiple levels - create multi-level entry
                multi_level_map[field_name] = {
                    'total_occurrences': len(queryable_occurrences),
                    'paths': []
                }
                
                for occ in queryable_occurrences:
                    full_path = occ['full_path']
                    schema_entry = schema[full_path]
                    
                    multi_level_map[field_name]['paths'].append({
                        'full_path': full_path,
                        'alias': schema_entry['context_description'],
                        'depth': occ['depth'],
                        'array_context': occ['array_context'],
                        'context_description': occ['context_description'],
                        'schema_entry': schema_entry
                    })
                
                # Sort by depth for consistent ordering
                multi_level_map[field_name]['paths'].sort(key=lambda x: (x['depth'], x['full_path']))
        
        return multi_level_map
    
    def get_multi_level_field_info(self) -> Dict:
        """Get multi-level field information for UI display"""
        return self.multi_level_fields
    
    def _get_snowflake_type(self, python_type: str) -> str:
        """Map Python type to Snowflake type"""
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
        ENHANCED: Generate SQL with multi-level field detection
        """
        try:
            # Reset tracking for each SQL generation
            self.flatten_alias_map = {}
            self.path_usage_tracker = {}
            
            # Parse field conditions with multi-level detection
            fields_info, warnings = self._parse_field_conditions_with_multi_level(field_conditions, schema)
            
            if not fields_info:
                return "-- Error: No valid fields found in conditions"
            
            # Build SQL components
            select_clause = self._build_select_clause_multi_level(fields_info, schema, json_column)
            from_clause = self._build_from_clause_optimized(table_name, json_column, fields_info, schema)
            where_clause = self._build_where_clause_multi_level(fields_info, schema, json_column)
            
            sql_parts = [select_clause, from_clause]
            if where_clause.strip():
                sql_parts.append(where_clause)
            
            return '\n'.join(sql_parts) + ';'
            
        except Exception as e:
            logger.error(f"Enhanced SQL generation failed: {e}")
            return f"-- Error generating SQL: {str(e)}"
    
    def generate_sql_with_warnings(self, table_name: str, json_column: str, field_conditions: str, schema: Dict[str, Dict]) -> Tuple[str, List[str]]:
        """
        ENHANCED: Generate SQL with multi-level field warnings
        Returns: (sql, warnings)
        """
        try:
            # Reset tracking
            self.flatten_alias_map = {}
            self.path_usage_tracker = {}
            
            # Parse field conditions with multi-level detection
            fields_info, parsing_warnings = self._parse_field_conditions_with_multi_level(field_conditions, schema)
            
            # Get multi-level field warnings
            multi_level_warnings = self._get_multi_level_warnings(field_conditions)
            
            # Combine all warnings
            all_warnings = parsing_warnings + multi_level_warnings
            
            if not fields_info:
                return "-- Error: No valid fields found in conditions", ["❌ No valid fields found"]
            
            # Build SQL components
            select_clause = self._build_select_clause_multi_level(fields_info, schema, json_column)
            from_clause = self._build_from_clause_optimized(table_name, json_column, fields_info, schema)
            where_clause = self._build_where_clause_multi_level(fields_info, schema, json_column)
            
            sql_parts = [select_clause, from_clause]
            if where_clause.strip():
                sql_parts.append(where_clause)
            
            return '\n'.join(sql_parts) + ';', all_warnings
            
        except Exception as e:
            logger.error(f"Enhanced SQL generation failed: {e}")
            return f"-- Error generating SQL: {str(e)}", [f"❌ Generation error: {str(e)}"]
    
    def _parse_field_conditions_with_multi_level(self, field_conditions: str, schema: Dict[str, Dict]) -> Tuple[List[Dict], List[str]]:
        """
        ENHANCED: Parse field conditions with multi-level detection
        Returns: (parsed_fields, warnings)
        """
        fields_info = []
        warnings = []
        
        conditions = [c.strip() for c in field_conditions.split(',')]
        
        for condition in conditions:
            if not condition:
                continue
                
            # Parse field[operator:value] or just field
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
            
            # Multi-level field resolution
            resolved_fields, resolution_warnings = self._resolve_field_multi_level(field_path, schema)
            
            if resolved_fields:
                for resolved_field in resolved_fields:
                    fields_info.append({
                        'field_path': resolved_field['full_path'],
                        'original_field': field_path,
                        'suggested_alias': resolved_field['alias'],
                        'operator': operator,
                        'value': value,
                        'schema_entry': resolved_field['schema_entry'],
                        'is_multi_level': resolved_field.get('is_multi_level', False),
                        'level_description': resolved_field.get('context_description', '')
                    })
                    
                    # Track usage
                    self.path_usage_tracker[resolved_field['full_path']] = True
                
                if resolution_warnings:
                    warnings.extend(resolution_warnings)
            else:
                warnings.append(f"⚠️ Field '{field_path}' not found or not queryable")
        
        return fields_info, warnings
    
    def _resolve_field_multi_level(self, field_input: str, schema: Dict[str, Dict]) -> Tuple[List[Dict], List[str]]:
        """
        ENHANCED: Resolve field with multi-level detection
        If user specifies simple field name, return ALL occurrences
        """
        warnings = []
        
        # Try exact path match first
        if field_input in schema and schema[field_input]['is_queryable']:
            return [{
                'full_path': field_input,
                'alias': field_input.split('.')[-1],
                'schema_entry': schema[field_input],
                'is_multi_level': False
            }], []
        
        # Extract simple field name
        simple_field_name = field_input.split('.')[-1]
        
        # Check if this field has multiple levels
        if simple_field_name in self.multi_level_fields and field_input == simple_field_name:
            # User wants ALL occurrences of this field
            multi_level_info = self.multi_level_fields[simple_field_name]
            resolved_fields = []
            
            for path_info in multi_level_info['paths']:
                resolved_fields.append({
                    'full_path': path_info['full_path'],
                    'alias': path_info['alias'],
                    'schema_entry': path_info['schema_entry'],
                    'is_multi_level': True,
                    'context_description': path_info['context_description']
                })
            
            warnings.append(
                f"✅ Found '{simple_field_name}' at {multi_level_info['total_occurrences']} levels. "
                f"Including ALL occurrences with descriptive aliases: {', '.join([f['alias'] for f in resolved_fields])}"
            )
            
            return resolved_fields, warnings
        
        # Try partial matching for more complex patterns
        matching_paths = []
        for path, details in schema.items():
            if details['is_queryable']:
                if path.endswith('.' + field_input) or field_input in path:
                    matching_paths.append({
                        'full_path': path,
                        'alias': details['context_description'],
                        'schema_entry': details,
                        'is_multi_level': False
                    })
        
        if matching_paths:
            if len(matching_paths) == 1:
                warnings.append(f"ℹ️ Matched '{field_input}' → '{matching_paths[0]['full_path']}'")
            else:
                warnings.append(f"✅ Found '{field_input}' at {len(matching_paths)} locations. Including ALL occurrences.")
            
            return matching_paths, warnings
        
        return [], []
    
    def _get_multi_level_warnings(self, field_conditions: str) -> List[str]:
        """Get specific warnings for multi-level field usage"""
        warnings = []
        
        conditions = [c.strip() for c in field_conditions.split(',')]
        
        for condition in conditions:
            if not condition:
                continue
                
            field_name = condition.split('[')[0].strip() if '[' in condition else condition.strip()
            simple_field_name = field_name.split('.')[-1]
            
            if simple_field_name in self.multi_level_fields and field_name == simple_field_name:
                multi_level_info = self.multi_level_fields[simple_field_name]
                aliases = [path['alias'] for path in multi_level_info['paths']]
                
                warnings.append(
                    f"🎯 Multi-level field '{field_name}' expanded to {multi_level_info['total_occurrences']} columns: {', '.join(aliases)}"
                )
        
        return warnings
    
    def _build_select_clause_multi_level(self, fields_info: List[Dict], schema: Dict[str, Dict], json_column: str) -> str:
        """
        ENHANCED: Build SELECT clause for multi-level fields
        """
        select_parts = []
        used_aliases = set()
        
        for field_info in fields_info:
            field_path = field_info['field_path']
            suggested_alias = field_info['suggested_alias']
            schema_entry = field_info['schema_entry']
            snowflake_type = schema_entry.get('snowflake_type', 'VARCHAR')
            array_context = schema_entry.get('array_context', [])
            
            # Ensure unique alias
            final_alias = self._ensure_unique_alias(suggested_alias, used_aliases)
            used_aliases.add(final_alias)
            
            if array_context:
                flatten_alias = self._get_flatten_alias(array_context)
                relative_path = self._calculate_relative_path_dynamic(field_path, array_context)
                sql_path = f"{flatten_alias}.value:{relative_path}::{snowflake_type}"
            else:
                json_path = field_path.replace('.', ':')
                sql_path = f"{json_column}:{json_path}::{snowflake_type}"
            
            select_parts.append(f"{sql_path} as {final_alias}")
        
        return f"SELECT {', '.join(select_parts)}"
    
    def _build_where_clause_multi_level(self, fields_info: List[Dict], schema: Dict[str, Dict], json_column: str) -> str:
        """
        ENHANCED: Build WHERE clause for multi-level fields
        When user specifies condition on multi-level field, apply to ALL levels
        """
        where_conditions = []
        
        # Group conditions by original field to handle multi-level
        condition_groups = {}
        for field_info in fields_info:
            original_field = field_info.get('original_field')
            operator = field_info.get('operator')
            value = field_info.get('value')
            
            if not operator:
                continue
                
            if original_field not in condition_groups:
                condition_groups[original_field] = []
            
            condition_groups[original_field].append({
                'field_info': field_info,
                'operator': operator,
                'value': value
            })
        
        for original_field, conditions in condition_groups.items():
            if len(conditions) > 1:
                # Multi-level field - create OR condition for all levels
                level_conditions = []
                
                for cond in conditions:
                    field_info = cond['field_info']
                    operator = cond['operator']
                    value = cond['value']
                    
                    field_path = field_info['field_path']
                    schema_entry = field_info['schema_entry']
                    array_context = schema_entry.get('array_context', [])
                    
                    if array_context:
                        flatten_alias = self._get_flatten_alias(array_context)
                        relative_path = self._calculate_relative_path_dynamic(field_path, array_context)
                        sql_ref = f"{flatten_alias}.value:{relative_path}"
                    else:
                        json_path = field_path.replace('.', ':')
                        sql_ref = f"{json_column}:{json_path}"
                    
                    # Build condition based on operator
                    if operator.upper() == 'IS NOT NULL':
                        level_conditions.append(f"{sql_ref} IS NOT NULL")
                    elif operator == '=':
                        level_conditions.append(f"{sql_ref}::VARCHAR = '{value}'")
                    elif operator == '>':
                        level_conditions.append(f"{sql_ref}::NUMBER > {value}")
                    elif operator == '<':
                        level_conditions.append(f"{sql_ref}::NUMBER < {value}")
                    elif operator.upper() == 'IN':
                        values_list = [f"'{v.strip()}'" for v in value.split('|')]
                        level_conditions.append(f"{sql_ref}::VARCHAR IN ({', '.join(values_list)})")
                    elif operator.upper() == 'LIKE':
                        level_conditions.append(f"{sql_ref}::VARCHAR LIKE '%{value}%'")
                
                if level_conditions:
                    where_conditions.append(f"({' OR '.join(level_conditions)})")
            else:
                # Single level field
                cond = conditions[0]
                field_info = cond['field_info']
                operator = cond['operator']
                value = cond['value']
                
                field_path = field_info['field_path']
                schema_entry = field_info['schema_entry']
                array_context = schema_entry.get('array_context', [])
                
                if array_context:
                    flatten_alias = self._get_flatten_alias(array_context)
                    relative_path = self._calculate_relative_path_dynamic(field_path, array_context)
                    sql_ref = f"{flatten_alias}.value:{relative_path}"
                else:
                    json_path = field_path.replace('.', ':')
                    sql_ref = f"{json_column}:{json_path}"
                
                # Build condition based on operator
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
    
    def _ensure_unique_alias(self, preferred_alias: str, used_aliases: set) -> str:
        """Ensure alias is unique by adding counter if needed"""
        if preferred_alias not in used_aliases:
            return preferred_alias
        
        counter = 1
        while f"{preferred_alias}_{counter}" in used_aliases:
            counter += 1
        
        return f"{preferred_alias}_{counter}"
    
    def _get_flatten_alias(self, array_context: List[str]) -> str:
    """Get or create flatten alias, ensuring uniqueness"""
    context_key = tuple(array_context)
    
    if context_key in self.flatten_alias_map:
        return self.flatten_alias_map[context_key]
    
    # Generate unique alias
    counter = 1
    while True:
        alias = f"f{counter}"
        if alias not in self.flatten_alias_map.values():
            break
        counter += 1
    
    self.flatten_alias_map[context_key] = alias
    return alias
    
    def _build_from_clause_optimized(self, table_name: str, json_column: str, fields_info: List[Dict], schema: Dict[str, Dict]) -> str:
    """
    OPTIMIZED: Build FROM clause without duplicate LATERAL FLATTEN aliases
    """
    from_parts = [table_name]
    required_flattens = {}
    
    # Collect all required flattens
    for field_info in fields_info:
        array_context = field_info['schema_entry'].get('array_context', [])
        
        for i, context in enumerate(array_context):
            context_tuple = tuple(array_context[:i+1])
            
            if context_tuple not in required_flattens:
                required_flattens[context_tuple] = {
                    'level': i,
                    'array_path': context,
                    'full_context': array_context[:i+1]
                }
    
    # Sort flattens by level to ensure proper order
    sorted_flattens = sorted(required_flattens.items(), key=lambda x: x[1]['level'])
    
    # Build LATERAL FLATTEN clauses
    for context_tuple, flatten_info in sorted_flattens:
        level = flatten_info['level']
        array_path = flatten_info['array_path']
        flatten_alias = self._get_flatten_alias(flatten_info['full_context'])
        
        if level == 0:
            # Top-level array
            clean_path = array_path.replace('.', ':')
            flatten_input = f"{json_column}:{clean_path}"
        else:
            # Nested array
            parent_context = tuple(flatten_info['full_context'][:level])
            parent_alias = self._get_flatten_alias(list(parent_context))
            parent_array_path = flatten_info['full_context'][level-1]
            
            if array_path.startswith(parent_array_path + '.'):
                relative_path = array_path[len(parent_array_path) + 1:]
            else:
                relative_path = array_path.split('.')[-1]
            
            flatten_input = f"{parent_alias}.value:{relative_path}"
        
        from_parts.append(f"LATERAL FLATTEN(input => {flatten_input}) {flatten_alias}")
    
    return f"FROM {', '.join(from_parts)}"
    
    def _calculate_relative_path_dynamic(self, full_path: str, array_context: List[str]) -> str:
        """Calculate relative path within flattened context"""
        if not array_context:
            return full_path.replace('.', ':')
        
        deepest_array = array_context[-1]
        
        if full_path.startswith(deepest_array + '.'):
            relative_path = full_path[len(deepest_array) + 1:]
        else:
            relative_path = full_path.split('.')[-1]
        
        return relative_path.replace('.', ':')


def generate_sql_from_json_data(json_data: Any, table_name: str, json_column: str, field_conditions: str) -> str:
    """
    ENHANCED: Generate SQL from JSON data with multi-level field support
    This is the main function called by your application - maintains backward compatibility
    """
    try:
        generator = PythonSQLGenerator()
        schema = generator.analyze_json_for_sql(json_data)
        
        if not schema:
            return "-- Error: Could not analyze JSON structure"
        
        return generator.generate_dynamic_sql(table_name, json_column, field_conditions, schema)
        
    except Exception as e:
        logger.error(f"SQL generation from JSON data failed: {e}")
        return f"-- Error: {str(e)}"


def generate_sql_from_json_data_with_warnings(json_data: Any, table_name: str, json_column: str, field_conditions: str) -> Tuple[str, List[str], Dict]:
    """
    NEW: Enhanced version that returns SQL, warnings, and multi-level info
    Use this for enhanced UI features
    """
    try:
        generator = PythonSQLGenerator()
        schema = generator.analyze_json_for_sql(json_data)
        
        if not schema:
            return "-- Error: Could not analyze JSON structure", ["❌ Schema analysis failed"], {}
        
        sql, warnings = generator.generate_sql_with_warnings(table_name, json_column, field_conditions, schema)
        multi_level_info = generator.get_multi_level_field_info()
        
        return sql, warnings, multi_level_info
        
    except Exception as e:
        logger.error(f"Enhanced SQL generation failed: {e}")
        return f"-- Error: {str(e)}", [f"❌ Generation error: {str(e)}"], {}


def analyze_json_structure_simple(json_data: Any) -> Dict[str, Any]:
    """Simple JSON structure analysis for basic use cases"""
    try:
        generator = PythonSQLGenerator()
        schema = generator.analyze_json_for_sql(json_data)
        
        # Return simplified structure info
        structure_info = {
            'total_fields': len(schema),
            'queryable_fields': sum(1 for details in schema.values() if details.get('is_queryable', False)),
            'nested_objects': sum(1 for details in schema.values() if details.get('is_nested_object', False)),
            'arrays': sum(1 for details in schema.values() if details.get('is_array', False)),
            'field_paths': list(schema.keys()),
            'multi_level_fields': len(generator.get_multi_level_field_info())
        }
        
        return structure_info
        
    except Exception as e:
        logger.error(f"Simple JSON analysis failed: {e}")
        return {
            'error': str(e),
            'total_fields': 0,
            'queryable_fields': 0,
            'nested_objects': 0,
            'arrays': 0,
            'field_paths': [],
            'multi_level_fields': 0
        }


def get_field_suggestions_simple(json_data: Any, max_suggestions: int = 5) -> List[str]:
    """Get simple field suggestions from JSON data"""
    try:
        generator = PythonSQLGenerator()
        schema = generator.analyze_json_for_sql(json_data)
        multi_level_info = generator.get_multi_level_field_info()
        
        suggestions = []
        
        # Prioritize multi-level fields
        for field_name in multi_level_info.keys():
            if len(suggestions) >= max_suggestions:
                break
            suggestions.append(field_name)
        
        # Add high-frequency queryable fields
        queryable_fields = [
            (path, details) for path, details in schema.items()
            if details.get('is_queryable', False) and details.get('frequency', 0) > 0.5
        ]
        
        # Sort by frequency and add to suggestions
        queryable_fields.sort(key=lambda x: x[1].get('frequency', 0), reverse=True)
        
        for path, details in queryable_fields:
            field_name = path.split('.')[-1]
            if field_name not in multi_level_info and len(suggestions) < max_suggestions:
                suggestions.append(path)
        
        return suggestions[:max_suggestions]
        
    except Exception as e:
        logger.error(f"Simple field suggestions failed: {e}")
        return []


def validate_field_conditions_format(field_conditions: str) -> Tuple[bool, List[str]]:
    """Validate field conditions format"""
    try:
        errors = []
        conditions = [c.strip() for c in field_conditions.split(',') if c.strip()]
        
        for condition in conditions:
            # Check basic format
            if '[' in condition:
                if not condition.endswith(']'):
                    errors.append(f"Condition '{condition}' missing closing bracket")
                    continue
                
                field_part = condition.split('[')[0].strip()
                condition_part = condition[condition.index('[') + 1:condition.rindex(']')]
                
                if not field_part:
                    errors.append(f"Empty field name in condition '{condition}'")
                
                if not condition_part:
                    errors.append(f"Empty condition in '{condition}'")
                
                # Check for valid operators
                if ':' in condition_part:
                    operator = condition_part.split(':', 1)[0].strip().upper()
                    valid_operators = ['IS NOT NULL', '=', '>', '<', 'IN', 'LIKE', 'NOT LIKE', '>=', '<=', '!=']
                    
                    if operator not in valid_operators:
                        errors.append(f"Unknown operator '{operator}' in condition '{condition}'")
            
            elif not condition.replace('.', '').replace('_', '').isalnum():
                # Basic field name validation
                errors.append(f"Field name '{condition}' contains invalid characters")
        
        return len(errors) == 0, errors
        
    except Exception as e:
        logger.error(f"Field condition validation failed: {e}")
        return False, [f"Validation error: {str(e)}"]


def extract_json_sample_values(json_data: Any, field_path: str, max_samples: int = 10) -> List[Any]:
    """Extract sample values for a specific field path from JSON data"""
    try:
        def extract_values(obj, path_parts, current_depth=0):
            if current_depth >= len(path_parts):
                return [obj] if obj is not None else []
            
            current_key = path_parts[current_depth]
            values = []
            
            if isinstance(obj, dict) and current_key in obj:
                values.extend(extract_values(obj[current_key], path_parts, current_depth + 1))
            elif isinstance(obj, list):
                for item in obj:
                    values.extend(extract_values(item, path_parts, current_depth))
            
            return values
        
        path_parts = field_path.split('.')
        sample_values = extract_values(json_data, path_parts)
        
        # Remove duplicates while preserving order
        unique_values = []
        seen = set()
        
        for value in sample_values[:max_samples * 2]:  # Get extra to account for duplicates
            str_value = str(value)
            if str_value not in seen:
                seen.add(str_value)
                unique_values.append(value)
                
                if len(unique_values) >= max_samples:
                    break
        
        return unique_values
        
    except Exception as e:
        logger.error(f"Sample value extraction failed for {field_path}: {e}")
        return []


def get_json_depth_info(json_data: Any) -> Dict[str, Any]:
    """Get depth information about JSON structure"""
    try:
        def calculate_depth(obj, current_depth=0):
            if isinstance(obj, dict):
                if not obj:
                    return current_depth
                return max(calculate_depth(v, current_depth + 1) for v in obj.values())
            elif isinstance(obj, list):
                if not obj:
                    return current_depth
                return max(calculate_depth(item, current_depth) for item in obj if item is not None)
            else:
                return current_depth
        
        max_depth = calculate_depth(json_data)
        
        # Count objects at each level
        def count_by_depth(obj, current_depth=0, counts=None):
            if counts is None:
                counts = {}
            
            level = f"depth_{current_depth}"
            counts[level] = counts.get(level, 0) + 1
            
            if isinstance(obj, dict):
                for value in obj.values():
                    count_by_depth(value, current_depth + 1, counts)
            elif isinstance(obj, list):
                for item in obj:
                    if item is not None:
                        count_by_depth(item, current_depth + 1, counts)
            
            return counts
        
        depth_counts = count_by_depth(json_data)
        
        return {
            'max_depth': max_depth,
            'depth_distribution': depth_counts,
            'complexity': 'High' if max_depth > 5 else 'Medium' if max_depth > 2 else 'Low'
        }
        
    except Exception as e:
        logger.error(f"JSON depth analysis failed: {e}")
        return {
            'max_depth': 0,
            'depth_distribution': {},
            'complexity': 'Unknown',
            'error': str(e)
        }


def compare_json_schemas(schema1: Dict, schema2: Dict) -> Dict[str, Any]:
    """Compare two JSON schemas and return differences"""
    try:
        comparison = {
            'common_fields': [],
            'schema1_only': [],
            'schema2_only': [],
            'type_differences': [],
            'similarity_score': 0.0
        }
        
        paths1 = set(schema1.keys())
        paths2 = set(schema2.keys())
        
        comparison['common_fields'] = list(paths1.intersection(paths2))
        comparison['schema1_only'] = list(paths1 - paths2)
        comparison['schema2_only'] = list(paths2 - paths1)
        
        # Check type differences for common fields
        for path in comparison['common_fields']:
            type1 = schema1[path].get('snowflake_type', 'UNKNOWN')
            type2 = schema2[path].get('snowflake_type', 'UNKNOWN')
            
            if type1 != type2:
                comparison['type_differences'].append({
                    'path': path,
                    'schema1_type': type1,
                    'schema2_type': type2
                })
        
        # Calculate similarity score
        total_unique_fields = len(paths1.union(paths2))
        common_fields_count = len(comparison['common_fields'])
        
        if total_unique_fields > 0:
            comparison['similarity_score'] = common_fields_count / total_unique_fields
        
        return comparison
        
    except Exception as e:
        logger.error(f"Schema comparison failed: {e}")
        return {
            'error': str(e),
            'common_fields': [],
            'schema1_only': [],
            'schema2_only': [],
            'type_differences': [],
            'similarity_score': 0.0
        }


def optimize_sql_performance(sql: str, optimization_level: str = 'medium') -> Tuple[str, List[str]]:
    """Apply basic SQL optimizations"""
    try:
        optimized_sql = sql
        optimizations_applied = []
        
        if optimization_level in ['medium', 'high']:
            # Remove unnecessary type casting where possible
            if '::VARCHAR' in optimized_sql and 'LIKE' not in optimized_sql.upper():
                # Only keep VARCHAR casting where needed for string operations
                lines = optimized_sql.split('\n')
                for i, line in enumerate(lines):
                    if '::VARCHAR' in line and 'IS NOT NULL' in line:
                        lines[i] = line.replace('::VARCHAR', '')
                        if 'Removed unnecessary VARCHAR casting' not in optimizations_applied:
                            optimizations_applied.append('Removed unnecessary VARCHAR casting for NOT NULL checks')
                
                optimized_sql = '\n'.join(lines)
        
        if optimization_level == 'high':
            # Additional high-level optimizations could be added here
            pass
        
        return optimized_sql, optimizations_applied
        
    except Exception as e:
        logger.error(f"SQL optimization failed: {e}")
        return sql, [f"Optimization failed: {str(e)}"]


# Backward compatibility functions
def get_field_disambiguation_warnings(field_conditions: str, schema: Dict) -> List[str]:
    """Backward compatibility wrapper for multi-level warnings"""
    try:
        generator = PythonSQLGenerator()
        generator.multi_level_fields = generator._create_multi_level_field_map(
            {}, schema  # Simplified call for compatibility
        )
        return generator._get_multi_level_warnings(field_conditions)
    except Exception as e:
        logger.error(f"Compatibility warning generation failed: {e}")
        return [f"Warning generation error: {str(e)}"]


def create_sql_execution_plan(sql: str) -> Dict[str, Any]:
    """Create a basic execution plan summary"""
    try:
        plan = {
            'operations': [],
            'estimated_complexity': 'Medium',
            'recommendations': []
        }
        
        sql_upper = sql.upper()
        
        # Analyze SQL structure
        if 'LATERAL FLATTEN' in sql_upper:
            flatten_count = sql_upper.count('LATERAL FLATTEN')
            plan['operations'].append(f'LATERAL FLATTEN operations: {flatten_count}')
            
            if flatten_count > 2:
                plan['estimated_complexity'] = 'High'
                plan['recommendations'].append('Consider limiting JSON depth or filtering data earlier')
        
        if 'WHERE' not in sql_upper:
            plan['recommendations'].append('Consider adding WHERE conditions to limit data scanned')
        
        if sql_upper.count('SELECT') > 1:
            plan['operations'].append('Complex query with subqueries or CTEs')
        
        # Count projected columns
        select_part = sql.split('FROM')[0] if 'FROM' in sql.upper() else sql
        column_count = select_part.count(',') + 1 if 'SELECT' in select_part.upper() else 0
        
        if column_count > 10:
            plan['recommendations'].append('Consider selecting only necessary columns for better performance')
        
        plan['operations'].append(f'Projected columns: {column_count}')
        
        if len(plan['operations']) <= 2 and not plan['recommendations']:
            plan['estimated_complexity'] = 'Low'
        
        return plan
        
    except Exception as e:
        logger.error(f"Execution plan creation failed: {e}")
        return {
            'operations': ['Analysis failed'],
            'estimated_complexity': 'Unknown',
            'recommendations': [f'Plan analysis error: {str(e)}'],
            'error': str(e)
        }


if __name__ == "__main__":
    logger.info("Enhanced Python SQL Generator with Multi-Level Field Support loaded successfully")
