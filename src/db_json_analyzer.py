"""
Database-driven JSON analysis for Snowflake
Analyzes JSON structure directly from database columns
"""
import streamlit as st
import json
from typing import Dict, Any, List, Tuple, Optional
import logging
from json_analyzer import analyze_json_structure

logger = logging.getLogger(__name__)


def sample_json_from_database(conn_manager, table_name: str, json_column: str, 
                            sample_size: int = 5) -> Tuple[Optional[List[Dict]], Optional[str]]:
    """
    Sample JSON data directly from the database to infer schema
    
    Args:
        conn_manager: Database connection manager
        table_name: Name of the table containing JSON data
        json_column: Name of the column containing JSON data
        sample_size: Number of records to sample for schema inference
    
    Returns:
        Tuple of (sample_data_list, error_message)
    """
    if not conn_manager.is_connected:
        return None, "❌ Not connected to database"
    
    try:
        # Query to sample JSON data from the specified column
        sample_query = f"""
        SELECT {json_column}
        FROM {table_name}
        WHERE {json_column} IS NOT NULL
        LIMIT {sample_size}
        """
        
        result_df, error_msg = conn_manager.execute_query(sample_query)
        
        if result_df is None:
            return None, f"❌ Failed to sample data: {error_msg}"
        
        if result_df.empty:
            return None, f"❌ No data found in {table_name}.{json_column}"
        
        # Extract JSON data from the results
        json_samples = []
        for _, row in result_df.iterrows():
            json_value = row[json_column]
            
            if json_value is not None:
                try:
                    # Handle different JSON storage formats
                    if isinstance(json_value, str):
                        parsed_json = json.loads(json_value)
                    elif isinstance(json_value, dict):
                        parsed_json = json_value
                    else:
                        # Convert to string and try to parse
                        parsed_json = json.loads(str(json_value))
                    
                    json_samples.append(parsed_json)
                    
                except (json.JSONDecodeError, TypeError) as e:
                    logger.warning(f"Failed to parse JSON from row: {e}")
                    continue
        
        if not json_samples:
            return None, f"❌ No valid JSON found in sampled records from {table_name}.{json_column}"
        
        return json_samples, None
        
    except Exception as e:
        return None, f"❌ Database sampling failed: {str(e)}"


def analyze_database_json_schema(conn_manager, table_name: str, json_column: str) -> Tuple[Optional[Dict], Optional[str], Dict]:
    """
    Analyze JSON schema directly from database column
    
    Returns:
        Tuple of (schema_dict, error_message, metadata)
    """
    metadata = {
        'table_name': table_name,
        'json_column': json_column,
        'sample_size': 0,
        'unique_schemas': 0,
        'analysis_success': False
    }
    
    # Sample JSON data from database
    json_samples, error_msg = sample_json_from_database(conn_manager, table_name, json_column, sample_size=10)
    
    if json_samples is None:
        return None, error_msg, metadata
    
    metadata['sample_size'] = len(json_samples)
    
    # Combine all samples to create comprehensive schema
    combined_schema = {}
    unique_structures = set()
    
    for i, json_sample in enumerate(json_samples):
        try:
            # Analyze each JSON sample
            sample_schema = analyze_json_structure(json_sample, parent_path=f"sample_{i}")
            
            # Track unique JSON structures
            structure_signature = str(sorted(sample_schema.keys()))
            unique_structures.add(structure_signature)
            
            # Merge schemas (union of all fields found)
            for path, details in sample_schema.items():
                if path in combined_schema:
                    # Handle type conflicts by promoting to VARIANT
                    existing_type = combined_schema[path].get('type')
                    current_type = details.get('type')
                    
                    if existing_type != current_type:
                        combined_schema[path]['type'] = 'variant'
                        combined_schema[path]['snowflake_type'] = 'VARIANT'
                        combined_schema[path]['has_type_conflicts'] = True
                    
                    # Merge sample values (keep multiple examples)
                    existing_samples = combined_schema[path].get('sample_values', [])
                    new_sample = details.get('sample_value', '')
                    if new_sample not in existing_samples:
                        existing_samples.append(new_sample)
                        combined_schema[path]['sample_values'] = existing_samples[:5]  # Keep max 5 examples
                else:
                    # New field found
                    combined_schema[path] = details.copy()
                    combined_schema[path]['sample_values'] = [details.get('sample_value', '')]
                    combined_schema[path]['found_in_samples'] = 1
                
                # Track how many samples contain this field
                combined_schema[path]['found_in_samples'] = combined_schema[path].get('found_in_samples', 0) + 1
                combined_schema[path]['frequency'] = combined_schema[path]['found_in_samples'] / len(json_samples)
                
        except Exception as e:
            logger.warning(f"Failed to analyze JSON sample {i}: {e}")
            continue
    
    metadata['unique_schemas'] = len(unique_structures)
    metadata['analysis_success'] = len(combined_schema) > 0
    
    if not combined_schema:
        return None, "❌ No valid JSON schema could be extracted from database samples", metadata
    
    return combined_schema, None, metadata


def generate_database_driven_sql(conn_manager, table_name: str, json_column: str, 
                                field_conditions: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Generate SQL by first analyzing the database JSON structure, then creating the query
    
    Returns:
        Tuple of (generated_sql, error_message)
    """
    try:
        # Step 1: Analyze JSON schema from database
        with st.spinner("🔍 Analyzing JSON structure from database..."):
            schema, schema_error, metadata = analyze_database_json_schema(conn_manager, table_name, json_column)
            
            if schema is None:
                return None, f"Schema analysis failed: {schema_error}"
            
            # Display schema analysis results
            st.success(f"✅ **Schema Analysis Complete**")
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Records Sampled", metadata['sample_size'])
            with col2:
                st.metric("JSON Fields Found", len(schema))
            with col3:
                st.metric("Unique Structures", metadata['unique_schemas'])
            with col4:
                st.metric("Analysis Success", "✅" if metadata['analysis_success'] else "❌")
        
        # Step 2: Generate SQL using the discovered schema
        from python_sql_generator import PythonSQLGenerator
        
        generator = PythonSQLGenerator()
        
        with st.spinner("🔨 Generating SQL from discovered schema..."):
            generated_sql = generator.generate_dynamic_sql(table_name, json_column, field_conditions, schema)
            
            if generated_sql.strip().startswith("-- Error"):
                return None, f"SQL generation failed: {generated_sql}"
            
            return generated_sql, None
            
    except Exception as e:
        return None, f"Database-driven analysis failed: {str(e)}"


def render_database_json_preview(schema: Dict, metadata: Dict):
    """
    Render a preview of the discovered JSON schema from database
    """
    st.markdown("### 📊 Discovered JSON Schema")
    
    # Schema summary
    st.markdown("**Schema Summary:**")
    schema_summary = []
    
    for path, details in sorted(schema.items(), key=lambda x: x[1].get('frequency', 0), reverse=True):
        frequency = details.get('frequency', 0)
        field_type = details.get('snowflake_type', 'VARIANT')
        sample_values = details.get('sample_values', ['N/A'])
        
        schema_summary.append({
            'Field Path': path,
            'Snowflake Type': field_type,
            'Frequency': f"{frequency:.1%}",
            'Sample Values': ', '.join(str(v)[:30] + '...' if len(str(v)) > 30 else str(v) 
                                     for v in sample_values[:2])
        })
    
    # Show top 20 fields
    import pandas as pd
    schema_df = pd.DataFrame(schema_summary[:20])
    st.dataframe(schema_df, use_container_width=True)
    
    if len(schema_summary) > 20:
        st.info(f"Showing top 20 fields. Total fields discovered: {len(schema_summary)}")
    
    # Field frequency chart
    if len(schema_summary) > 1:
        st.markdown("**Field Frequency Distribution:**")
        frequency_data = [(item['Field Path'][:30], float(item['Frequency'].rstrip('%'))/100) 
                         for item in schema_summary[:10]]
        
        chart_df = pd.DataFrame(frequency_data, columns=['Field', 'Frequency'])
        st.bar_chart(chart_df.set_index('Field'))


def render_suggested_field_conditions(schema: Dict) -> List[str]:
    """
    Generate suggested field conditions based on discovered schema
    """
    suggestions = []
    
    # Most frequent fields (good candidates for filtering)
    frequent_fields = [
        (path, details) for path, details in schema.items() 
        if details.get('frequency', 0) > 0.8 and details.get('is_queryable', False)
    ]
    
    # Sort by frequency and take top 5
    frequent_fields.sort(key=lambda x: x[1].get('frequency', 0), reverse=True)
    
    for path, details in frequent_fields[:5]:
        field_type = details.get('snowflake_type', 'VARIANT')
        sample_values = details.get('sample_values', [])
        
        # Generate appropriate condition based on type and sample values
        if field_type in ['VARCHAR', 'STRING']:
            if sample_values and len(set(sample_values)) < len(sample_values):
                # Looks like categorical data
                unique_vals = list(set(sample_values))[:3]
                suggestions.append(f"{path}[IN:{' | '.join(unique_vals)}]")
            else:
                suggestions.append(f"{path}[IS NOT NULL]")
        elif field_type in ['NUMBER', 'INTEGER']:
            if sample_values:
                try:
                    numeric_vals = [float(v) for v in sample_values if str(v).replace('.', '').replace('-', '').isdigit()]
                    if numeric_vals:
                        avg_val = sum(numeric_vals) / len(numeric_vals)
                        suggestions.append(f"{path}[>:{avg_val:.0f}]")
                except:
                    suggestions.append(f"{path}[IS NOT NULL]")
        else:
            suggestions.append(f"{path}[IS NOT NULL]")
    
    return suggestions
