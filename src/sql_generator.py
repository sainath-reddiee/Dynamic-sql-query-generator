"""
SQL generation module for Snowflake procedures
"""
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)


def generate_procedure_examples(schema: Dict[str, Dict]) -> List[str]:
    """Generate example procedure calls based on the schema"""
    examples = []
    
    # Example 1: Basic field selection
    basic_fields = [path for path, details in schema.items() 
                   if details['is_queryable'] and details['depth'] <= 2][:3]
    if basic_fields:
        field_conditions = " AND ".join([f"{field} IS NOT NULL" for field in basic_fields])
        examples.append(f"CALL DYNAMIC_SQL_LARGE('your_table', 'json_column', '{field_conditions}');")
    
    # Example 2: Array filtering
    arrays = [path for path, details in schema.items() if details['type'] == 'list'][:2]
    if arrays:
        array_conditions = " AND ".join([f"{array}_item IS NOT NULL" for array in arrays])
        examples.append(f"CALL DYNAMIC_SQL_LARGE('your_table', 'json_column', '{array_conditions}');")
    
    # Example 3: Complex filtering
    if len(basic_fields) > 1:
        complex_condition = f"{basic_fields[0]} = 'value1' AND {basic_fields[1]} > 100"
        examples.append(f"CALL DYNAMIC_SQL_LARGE('your_table', 'json_column', '{complex_condition}');")
    
    return examples


def generate_sql_preview(schema: Dict[str, Dict], field_conditions: str) -> str:
    """Generate a preview of the SQL that would be created"""
    
    # Build SELECT clause
    select_fields = []
    lateral_joins = []
    
    for path, details in schema.items():
        if details['is_queryable']:
            if details['array_hierarchy']:
                # Field comes from array, needs LATERAL FLATTEN
                array_path = details['array_hierarchy'][-1]
                if array_path not in [join.split('LATERAL FLATTEN')[1].split(' AS')[0].strip() for join in lateral_joins]:
                    lateral_joins.append(f"LATERAL FLATTEN(input => json_column:{array_path}) AS {array_path}_flat")
                
                field_ref = f"{array_path}_flat.value:{path.replace(array_path + '.', '')}"
                select_fields.append(f"{field_ref}::{details['snowflake_type']} AS {path.replace('.', '_')}")
            else:
                # Regular field
                select_fields.append(f"json_column:{path}::{details['snowflake_type']} AS {path.replace('.', '_')}")
    
    # Limit to first 10 fields for readability
    if len(select_fields) > 10:
        select_fields = select_fields[:10]
        select_fields.append("-- ... and more fields")
    
    # Build the SQL
    sql_parts = [
        "SELECT",
        "    " + ",\n    ".join(select_fields),
        "FROM your_table"
    ]
    
    # Add LATERAL FLATTEN joins
    for join in lateral_joins[:3]:  # Limit joins for readability
        sql_parts.append(f"    {join}")
    
    if lateral_joins and len(lateral_joins) > 3:
        sql_parts.append("    -- ... and more LATERAL FLATTEN joins")
    
    # Add WHERE clause if conditions provided
    if field_conditions.strip():
        sql_parts.append(f"WHERE {field_conditions}")
    
    return "\n".join(sql_parts) + ";"