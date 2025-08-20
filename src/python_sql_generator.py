import json
from typing import Dict, Any, List, Optional, Tuple
import re
import logging

logger = logging.getLogger(__name__)


class PythonSQLGenerator:
    """
    ENHANCED: SQL Generator with intelligent path disambiguation
    - Handles duplicate field names at different hierarchy levels
    - Provides smart field resolution and alias suggestions
    - Generates clean, unambiguous SQL with proper LATERAL FLATTEN management
    """
    
    def __init__(self):
        self.flatten_counter = 0
        self.array_hierarchy = []
        self.flatten_alias_map = {}
        self.field_disambiguation_map = {}  # Track field name conflicts
        self.path_usage_tracker = {}  # Track which paths are actually used
    
    def analyze_json_for_sql(self, json_obj: Any, parent_path: str = "") -> Dict[str, Dict]:
        """
        ENHANCED: Analyze JSON structure with comprehensive disambiguation tracking
        """
        schema = {}
        field_name_tracker = {}  # Track where each field name appears
        
        def traverse_json(obj: Any, path: str = "", depth: int = 0, in_array_context: List[str] = []):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    new_path = f"{path}.{key}" if path else key
                    current_type = type(value).__name__
                    
                    # Track field name occurrences for disambiguation
                    if key not in field_name_tracker:
                        field_name_tracker[key] = []
                    field_name_tracker[key].append({
                        'full_path': new_path,
                        'depth': depth,
                        'in_array': len(in_array_context) > 0,
                        'array_context': in_array_context.copy(),
                        'parent_path': path,
                        'parent_context': path.split('.')[-1] if path else 'root'
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
                        "hierarchy_level": self._get_hierarchy_description(new_path, in_array_context),
                        "context_path": self._get_context_path(new_path)
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
        
        # Generate comprehensive disambiguation information
        self.field_disambiguation_map = self._create_enhanced_disambiguation_map(field_name_tracker, schema)
        
        return schema
    
    def _get_context_path(self, full_path: str) -> str:
        """Get context path for better disambiguation"""
        parts = full_path.split('.')
        if len(parts) <= 2:
            return full_path
        # Return parent context for disambiguation
        return '.'.join(parts[:-1])
    
    def _get_hierarchy_description(self, full_path: str, array_context: List[str]) -> str:
        """Generate human-readable hierarchy description"""
        parts = full_path.split('.')
        
        if not array_context:
            if len(parts) == 1:
                return "Root Level"
            else:
                return f"Under {parts[-2]}"
        elif len(array_context) == 1:
            array_name = array_context[0].split('.')[-1]
            return f"In {array_name} Array"
        else:
            nested_arrays = [ctx.split('.')[-1] for ctx in array_context]
            return f"In Nested Arrays: {' → '.join(nested_arrays)}"
    
    def _create_enhanced_disambiguation_map(self, field_name_tracker: Dict, schema: Dict) -> Dict:
        """Create comprehensive disambiguation map for conflicting field names"""
        disambiguation_map = {}
        
        for field_name, occurrences in field_name_tracker.items():
            if len(occurrences) > 1:
                # Field name appears multiple times - needs disambiguation
                disambiguated_options = []
                
                for i, occurrence in enumerate(occurrences):
                    full_path = occurrence['full_path']
                    depth = occurrence['depth']
                    parent_context = occurrence['parent_context']
                    hierarchy = schema[full_path]['hierarchy_level']
                    
                    # Create smart alias based on context
                    suggested_alias = self._generate_smart_alias(full_path, field_name, parent_context, disambiguated_options)
                    
                    disambiguated_options.append({
                        'full_path': full_path,
                        'suggested_alias': suggested_alias,
                        'hierarchy_description': hierarchy,
                        'depth': depth,
                        'parent_context': parent_context,
                        'queryable': schema[full_path]['is_queryable'],
                        'sample_value': schema[full_path]['sample_value'],
                        'priority_score': self._calculate_priority_score(full_path, depth, schema[full_path])
                    })
                
                # Sort by priority (lower depth = higher priority)
                disambiguated_options.sort(key=lambda x: (x['depth'], x['full_path']))
                
                disambiguation_map[field_name] = {
                    'conflict_count': len(occurrences),
                    'options': disambiguated_options,
                    'recommended_option': disambiguated_options[0] if disambiguated_options else None
                }
        
        return disambiguation_map
    
    def _generate_smart_alias(self, full_path: str, field_name: str, parent_context: str, existing_options: List[Dict]) -> str:
        """Generate smart alias that avoids conflicts"""
        path_parts = full_path.split('.')
        
        # Strategy 1: Use parent context
        if parent_context and parent_context != 'root':
            candidate = f"{parent_context}_{field_name}"
        else:
            candidate = f"root_{field_name}"
        
        # Strategy 2: If still conflicts, use full context path
        if any(opt.get('suggested_alias') == candidate for opt in existing_options):
            if len(path_parts) >= 2:
                context_path = '_'.join(path_parts[-2:])  # Take last 2 parts
                candidate = context_path
            else:
                candidate = f"top_{field_name}"
        
        # Strategy 3: Add counter if still conflicts
        base_candidate = candidate
        counter = 1
        while any(opt.get('suggested_alias') == candidate for opt in existing_options):
            candidate = f"{base_candidate}_{counter}"
            counter += 1
        
        return candidate
    
    def _calculate_priority_score(self, full_path: str, depth: int, schema_entry: Dict) -> int:
        """Calculate priority score (lower = higher priority)"""
        score = depth * 10  # Depth penalty
        
        # Prefer queryable fields
        if not schema_entry.get('is_queryable', False):
            score += 100
        
        # Prefer non-nested objects
        if schema_entry.get('is_nested_object', False):
            score += 50
        
        return score
    
    def get_field_disambiguation_info(self) -> Dict:
        """Get disambiguation information for UI display"""
        return self.field_disambiguation_map
    
    def get_disambiguation_warnings(self, field_conditions: str) -> List[str]:
        """Get specific warnings for the current field conditions"""
        warnings = []
        
        # Parse field conditions to identify potential conflicts
        conditions = [c.strip() for c in field_conditions.split(',')]
        
        for condition in conditions:
            if not condition:
                continue
                
            # Extract field name
            field_name = condition.split('[')[0].strip() if '[' in condition else condition.strip()
            simple_field_name = field_name.split('.')[-1]
            
            # Check for conflicts
            if simple_field_name in self.field_disambiguation_map:
                conflict_info = self.field_disambiguation_map[simple_field_name]
                
                if field_name == simple_field_name:  # User specified simple name
                    if conflict_info['conflict_count'] > 1:
                        recommended = conflict_info['recommended_option']
                        other_options = [opt for opt in conflict_info['options'] if opt != recommended]
                        
                        warnings.append(
                            f"⚠️ '{field_name}' is ambiguous ({conflict_info['conflict_count']} matches found). "
                            f"Using '{recommended['full_path']}' ({recommended['hierarchy_description']}). "
                            f"Other options: {', '.join([opt['full_path'] for opt in other_options[:2]])}"
                        )
        
        return warnings
    
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
        ENHANCED: Generate SQL with intelligent disambiguation
        """
        try:
            # Reset alias tracking for each SQL generation
            self.flatten_alias_map = {}
            self.path_usage_tracker = {}
            
            # Parse field conditions with disambiguation
            fields_info, warnings = self._parse_field_conditions_with_disambiguation(field_conditions, schema)
            
            if not fields_info:
                return "-- Error: No valid fields found in conditions"
            
            # Build SQL components with proper alias management
            select_clause = self._build_select_clause_enhanced(fields_info, schema, json_column)
            from_clause = self._build_from_clause_optimized(table_name, json_column, fields_info, schema)
            where_clause = self._build_where_clause_enhanced(fields_info, schema, json_column)
            
            sql_parts = [select_clause, from_clause]
            if where_clause.strip():
                sql_parts.append(where_clause)
            
            return '\n'.join(sql_parts) + ';'
            
        except Exception as e:
            logger.error(f"Enhanced SQL generation failed: {e}")
            return f"-- Error generating SQL: {str(e)}"
    
    def generate_sql_with_warnings(self, table_name: str, json_column: str, field_conditions: str, schema: Dict[str, Dict]) -> Tuple[str, List[str]]:
        """
        ENHANCED: Generate SQL with comprehensive disambiguation warnings
        Returns: (sql, warnings)
        """
        try:
            # Reset alias tracking
            self.flatten_alias_map = {}
            self.path_usage_tracker = {}
            
            # Parse field conditions with disambiguation
            fields_info, parsing_warnings = self._parse_field_conditions_with_disambiguation(field_conditions, schema)
            
            # Get disambiguation warnings
            disambiguation_warnings = self.get_disambiguation_warnings(field_conditions)
            
            # Combine all warnings
            all_warnings = parsing_warnings + disambiguation_warnings
            
            if not fields_info:
                return "-- Error: No valid fields found in conditions", ["❌ No valid fields found"]
            
            # Build SQL components
            select_clause = self._build_select_clause_enhanced(fields_info, schema, json_column)
            from_clause = self._build_from_clause_optimized(table_name, json_column, fields_info, schema)
            where_clause = self._build_where_clause_enhanced(fields_info, schema, json_column)
            
            sql_parts = [select_clause, from_clause]
            if where_clause.strip():
                sql_parts.append(where_clause)
            
            return '\n'.join(sql_parts) + ';', all_warnings
            
        except Exception as e:
            logger.error(f"Enhanced SQL generation failed: {e}")
            return f"-- Error generating SQL: {str(e)}", [f"❌ Generation error: {str(e)}"]
    
    def _parse_field_conditions_with_disambiguation(self, field_conditions: str, schema: Dict[str, Dict]) -> Tuple[List[Dict], List[str]]:
        """
        ENHANCED: Parse field conditions with intelligent disambiguation
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
            
            # Intelligent field resolution with disambiguation
            resolved_field, resolution_warning = self._resolve_field_with_smart_disambiguation(field_path, schema)
            
            if resolved_field:
                fields_info.append({
                    'field_path': resolved_field['full_path'],
                    'original_field': field_path,
                    'suggested_alias': resolved_field.get('suggested_alias', field_path.split('.')[-1]),
                    'operator': operator,
                    'value': value,
                    'schema_entry': resolved_field['schema_entry'],
                    'disambiguation_used': resolved_field.get('disambiguation_used', False)
                })
                
                # Track usage
                self.path_usage_tracker[resolved_field['full_path']] = True
                
                if resolution_warning:
                    warnings.append(resolution_warning)
            else:
                warnings.append(f"⚠️ Field '{field_path}' not found or not queryable")
        
        return fields_info, warnings
    
    def _resolve_field_with_smart_disambiguation(self, field_input: str, schema: Dict[str, Dict]) -> Tuple[Optional[Dict], Optional[str]]:
        """
        ENHANCED: Resolve field with comprehensive smart disambiguation
        """
        # Try exact path match first
        if field_input in schema and schema[field_input]['is_queryable']:
            return {
                'full_path': field_input,
                'schema_entry': schema[field_input],
                'disambiguation_used': False
            }, None
        
        # Extract simple field name
        simple_field_name = field_input.split('.')[-1]
        
        # Check if this is a simple field name that has conflicts
        if simple_field_name in self.field_disambiguation_map:
            conflict_info = self.field_disambiguation_map[simple_field_name]
            queryable_options = [opt for opt in conflict_info['options'] if opt['queryable']]
            
            if field_input == simple_field_name:  # User specified just the field name
                if len(queryable_options) == 1:
                    # Only one queryable option - auto-resolve
                    option = queryable_options[0]
                    return {
                        'full_path': option['full_path'],
                        'suggested_alias': option['suggested_alias'],
                        'schema_entry': schema[option['full_path']],
                        'disambiguation_used': True
                    }, f"ℹ️ Auto-resolved '{field_input}' → '{option['full_path']}' ({option['hierarchy_description']})"
                
                elif len(queryable_options) > 1:
                    # Multiple options - return the highest priority (lowest depth)
                    best_option = min(queryable_options, key=lambda x: x.get('priority_score', x['depth']))
                    other_options = [opt['full_path'] for opt in queryable_options if opt != best_option]
                    
                    return {
                        'full_path': best_option['full_path'],
                        'suggested_alias': best_option['suggested_alias'],
                        'schema_entry': schema[best_option['full_path']],
                        'disambiguation_used': True
                    }, f"⚠️ '{field_input}' is ambiguous. Using '{best_option['full_path']}' ({best_option['hierarchy_description']}). Other options: {', '.join(other_options[:2])}"
        
        # Try partial matching for more complex patterns
        matching_paths = []
        for path, details in schema.items():
            if details['is_queryable']:
                # Exact field name match at end of path
                if path.endswith('.' + field_input) or field_input in path:
                    matching_paths.append((path, details))
        
        if len(matching_paths) == 1:
            path, details = matching_paths[0]
            return {
                'full_path': path,
                'schema_entry': details,
                'disambiguation_used': True
            }, f"ℹ️ Matched '{field_input}' → '{path}'"
        
        elif len(matching_paths) > 1:
            # Multiple matches - return the one with highest priority (shortest path, shallowest)
            best_match = min(matching_paths, key=lambda x: (x[1]['depth'], len(x[0])))
            other_matches = [path for path, _ in matching_paths if path != best_match[0]]
            
            return {
                'full_path': best_match[0],
                'schema_entry': best_match[1],
                'disambiguation_used': True
            }, f"⚠️ '{field_input}' matched multiple paths. Using '{best_match[0]}' (highest priority). Other matches: {', '.join(other_matches[:2])}"
        
        return None, None
    
    def _build_select_clause_enhanced(self, fields_info: List[Dict], schema: Dict[str, Dict], json_column: str) -> str:
        """
        ENHANCED: Build SELECT clause with smart aliases and conflict resolution
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
    
    def _ensure_unique_alias(self, preferred_alias: str, used_aliases: set) -> str:
        """Ensure alias is unique by adding counter if needed"""
        if preferred_alias not in used_aliases:
            return preferred_alias
        
        counter = 1
        while f"{preferred_alias}_{counter}" in used_aliases:
            counter += 1
        
        return f"{preferred_alias}_{counter}"
    
    def _build_where_clause_enhanced(self, fields_info: List[Dict], schema: Dict[str, Dict], json_column: str) -> str:
        """
        ENHANCED: Build WHERE clause with proper references
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
    
    def _get_flatten_alias(self, array_context: List[str]) -> str:
        """Get or create flatten alias, reusing existing aliases"""
        context_key = tuple(array_context)
        
        if context_key in self.flatten_alias_map:
            return self.flatten_alias_map[context_key]
        
        alias = f"f{len(array_context)}"
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
    ENHANCED: Generate SQL from JSON data with disambiguation support
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
    NEW: Enhanced version that returns SQL, warnings, and disambiguation info
    Use this for enhanced UI features
    """
    try:
        generator = PythonSQLGenerator()
        schema = generator.analyze_json_for_sql(json_data)
        
        if not schema:
            return "-- Error: Could not analyze JSON structure", ["❌ Schema analysis failed"], {}
        
        sql, warnings = generator.generate_sql_with_warnings(table_name, json_column, field_conditions, schema)
        disambiguation_info = generator.get_field_disambiguation_info()
        
        return sql, warnings, disambiguation_info
        
    except Exception as e:
        logger.error(f"Enhanced SQL generation failed: {e}")
        return f"-- Error: {str(e)}", [f"❌ Generation error: {str(e)}"], {}


if __name__ == "__main__":
    logger.info("Enhanced Python SQL Generator module loaded successfully")
