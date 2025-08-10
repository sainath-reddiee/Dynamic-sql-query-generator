"""
Enhanced Database-driven JSON analysis for Snowflake
Fixes session context issues and improves error handling
"""
import streamlit as st
import json
from typing import Dict, Any, List, Tuple, Optional
import logging

logger = logging.getLogger(__name__)


def sample_json_from_database_enhanced(conn_manager, table_name: str, json_column: str, 
                                     sample_size: int = 5) -> Tuple[Optional[List[Dict]], Optional[str]]:
    """
    Enhanced JSON sampling with proper table name handling and session context management
    """
    if not conn_manager.is_connected:
        return None, "❌ Not connected to database"
    
    try:
        # Ensure database context is established
        if hasattr(conn_manager, 'ensure_session_context'):
            if not conn_manager.ensure_session_context():
                return None, "❌ Failed to establish database session context"
        
        # Smart table name handling
        resolved_table_name = resolve_table_name(conn_manager, table_name)
        
        # Query to sample JSON data with explicit table qualification
        sample_query = f"""
        SELECT {json_column}
        FROM {resolved_table_name}
        WHERE {json_column} IS NOT NULL
        LIMIT {sample_size}
        """
        
        st.info(f"🔍 Sampling query: `{sample_query}`")
        
        result_df, error_msg = conn_manager.execute_query(sample_query)
        
        if result_df is None:
            # Provide more specific error guidance
            if "does not exist" in str(error_msg):
                return None, f"❌ Table or column not found. Please verify:\n• Table exists: {resolved_table_name}\n• Column exists: {json_column}\n• You have SELECT permissions"
            return None, f"❌ Failed to sample data: {error_msg}"
        
        if result_df.empty:
            return None, f"❌ No data found in {resolved_table_name}.{json_column}. Check if the column contains non-null values."
        
        # Extract and parse JSON data
        json_samples = []
        parsing_errors = 0
        
        for idx, row in result_df.iterrows():
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
                    parsing_errors += 1
                    logger.warning(f"Failed to parse JSON from row {idx}: {e}")
                    continue
        
        if not json_samples:
            if parsing_errors > 0:
                return None, f"❌ No valid JSON found in sampled records. {parsing_errors} parsing errors occurred. Ensure {json_column} contains valid JSON data."
            else:
                return None, f"❌ No valid JSON found in sampled records from {resolved_table_name}.{json_column}"
        
        # Report parsing success/issues
        if parsing_errors > 0:
            st.warning(f"⚠️ {parsing_errors} rows had JSON parsing issues, but {len(json_samples)} were successfully parsed.")
        else:
            st.success(f"✅ Successfully parsed {len(json_samples)} JSON records from {resolved_table_name}")
        
        return json_samples, None
        
    except Exception as e:
        error_msg = str(e)
        
        # Provide specific error guidance based on common issues
        if "does not have a current database" in error_msg:
            return None, "❌ Database context error. Try disconnecting and reconnecting with proper database/schema settings."
        elif "does not exist" in error_msg:
            return None, f"❌ Table {table_name} does not exist or you don't have access to it. Check the table name and your permissions."
        elif "Invalid identifier" in error_msg:
            return None, f"❌ Invalid column name '{json_column}' in table {table_name}. Verify the column exists and is spelled correctly."
        elif "permission" in error_msg.lower() or "access" in error_msg.lower():
            return None, f"❌ Permission denied accessing {table_name}. Contact your database administrator for SELECT permissions."
        else:
            return None, f"❌ Database sampling failed: {error_msg}"


def resolve_table_name(conn_manager, table_name: str) -> str:
    """
    Intelligently resolve table name to fully qualified format
    """
    # If already fully qualified (database.schema.table), return as-is
    if table_name.count('.') == 2:
        return table_name
    
    # Get connection parameters for context
    database = conn_manager.connection_params.get('database', '')
    schema = conn_manager.connection_params.get('schema', 'PUBLIC')
    
    # Handle different cases
    if '.' not in table_name:
        # Just table name provided
        return f"{database}.{schema}.{table_name}"
    elif table_name.count('.') == 1:
        # schema.table provided, add database
        return f"{database}.{table_name}"
    else:
        # Should not reach here, but return as-is
        return table_name


def analyze_database_json_schema_enhanced(conn_manager, table_name: str, json_column: str, 
                                        sample_size: int = 10) -> Tuple[Optional[Dict], Optional[str], Dict]:
    """
    Enhanced JSON schema analysis with better error handling and progress tracking
    """
    metadata = {
        'table_name': table_name,
        'json_column': json_column,
        'sample_size': 0,
        'unique_schemas': 0,
        'analysis_success': False,
        'resolved_table_name': resolve_table_name(conn_manager, table_name)
    }
    
    # Create progress tracking
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    try:
        # Step 1: Sample JSON data from database
        status_text.text("🔍 Sampling JSON data from database...")
        progress_bar.progress(20)
        
        json_samples, error_msg = sample_json_from_database_enhanced(
            conn_manager, table_name, json_column, sample_size
        )
        
        if json_samples is None:
            progress_bar.empty()
            status_text.empty()
            return None, error_msg, metadata
        
        metadata['sample_size'] = len(json_samples)
        progress_bar.progress(40)
        
        # Step 2: Analyze JSON structure
        status_text.text("🔬 Analyzing JSON structure...")
        
        from json_analyzer import analyze_json_structure
        
        combined_schema = {}
        unique_structures = set()
        
        for i, json_sample in enumerate(json_samples):
            try:
                # Update progress
                progress_percentage = 40 + (i / len(json_samples)) * 40
                progress_bar.progress(int(progress_percentage))
                
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
        
        progress_bar.progress(80)
        status_text.text("✅ Finalizing schema analysis...")
        
        metadata['unique_schemas'] = len(unique_structures)
        metadata['analysis_success'] = len(combined_schema) > 0
        
        progress_bar.progress(100)
        status_text.text("✅ Schema analysis complete!")
        
        # Clean up progress indicators
        import time
        time.sleep(1)
        progress_bar.empty()
        status_text.empty()
        
        if not combined_schema:
            return None, "❌ No valid JSON schema could be extracted from database samples", metadata
        
        return combined_schema, None, metadata
        
    except Exception as e:
        progress_bar.empty()
        status_text.empty()
        logger.error(f"Schema analysis failed: {e}")
        return None, f"❌ Schema analysis failed: {str(e)}", metadata


def generate_database_driven_sql_enhanced(conn_manager, table_name: str, json_column: str, 
                                        field_conditions: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Enhanced database-driven SQL generation with correct method calls.
    """
    try:
        # Step 1: Analyze JSON schema from database with progress tracking
        st.info("🔍 **Step 1:** Analyzing JSON structure from your database...")
        
        schema, schema_error, metadata = analyze_database_json_schema_enhanced(
            conn_manager, table_name, json_column, sample_size=10
        )
        
        if schema is None:
            return None, f"Schema analysis failed: {schema_error}"
        
        # Display comprehensive analysis results
        st.success("✅ **Step 1 Complete:** JSON Schema Analysis")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("📊 Records Sampled", metadata['sample_size'])
        with col2:
            st.metric("🏷️ JSON Fields Found", len(schema))
        with col3:
            st.metric("🔄 Unique Structures", metadata['unique_schemas'])
        with col4:
            success_indicator = "✅" if metadata['analysis_success'] else "❌"
            st.metric("📈 Analysis Status", success_indicator)
        
        st.info(f"📋 **Analyzed Table:** `{metadata['resolved_table_name']}`")
        
        # Step 2: Generate SQL using the discovered schema
        st.info("🔨 **Step 2:** Generating optimized SQL from discovered schema...")
        
        # Correctly import the class and instantiate it
        from python_sql_generator import PythonSQLGenerator
        generator = PythonSQLGenerator()
        
        # Use the resolved table name for SQL generation
        resolved_table_name = metadata['resolved_table_name']
        
        # This is the corrected method call
        generated_sql = generator.generate_dynamic_sql(
            resolved_table_name, json_column, field_conditions, schema
        )
        
        if generated_sql.strip().startswith("-- Error"):
            return None, f"SQL generation failed: {generated_sql}"
        
        st.success("✅ **Step 2 Complete:** Optimized SQL Generated")
        
        return generated_sql, None
        
    except Exception as e:
        # Added exc_info=True to provide a full error traceback in your logs for easier debugging
        logger.error(f"Enhanced database-driven analysis failed: {e}", exc_info=True)
        return None, f"❌ Database-driven analysis failed: {str(e)}"


def render_enhanced_database_json_preview(schema: Dict, metadata: Dict):
    """
    Enhanced preview of the discovered JSON schema with better visualization
    """
    st.markdown("### 📊 Discovered JSON Schema Analysis")
    
    # Enhanced schema summary with better formatting
    st.markdown("**📈 Schema Overview:**")
    
    # Create tabs for different views
    tab1, tab2, tab3 = st.tabs(["🏷️ **Field Details**", "📊 **Frequency Chart**", "🔍 **Type Distribution**"])
    
    with tab1:
        schema_summary = []
        
        for path, details in sorted(schema.items(), key=lambda x: x[1].get('frequency', 0), reverse=True):
            frequency = details.get('frequency', 0)
            field_type = details.get('snowflake_type', 'VARIANT')
            sample_values = details.get('sample_values', ['N/A'])
            found_in = details.get('found_in_samples', 0)
            
            # Create more readable sample values
            readable_samples = []
            for val in sample_values[:3]:  # Show max 3 samples
                val_str = str(val)
                if len(val_str) > 50:
                    val_str = val_str[:47] + "..."
                readable_samples.append(val_str)
            
            schema_summary.append({
                'Field Path': path,
                'Snowflake Type': field_type,
                'Frequency': f"{frequency:.1%}",
                'Found In Samples': f"{found_in}/{metadata['sample_size']}",
                'Sample Values': ' | '.join(readable_samples) if readable_samples else 'N/A',
                'Queryable': '✅' if details.get('is_queryable', False) else '❌'
            })
        
        # Show all fields in a paginated view
        import pandas as pd
        schema_df = pd.DataFrame(schema_summary)
        
        # Add search/filter functionality
        search_term = st.text_input("🔍 Search fields:", placeholder="Type to filter fields...")
        if search_term:
            schema_df = schema_df[schema_df['Field Path'].str.contains(search_term, case=False, na=False)]
        
        st.dataframe(schema_df, use_container_width=True, height=400)
        
        # Export option
        csv_data = schema_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            "📥 Export Schema Analysis",
            data=csv_data,
            file_name=f"json_schema_analysis_{metadata['table_name']}_{metadata['json_column']}.csv",
            mime="text/csv"
        )
    
    with tab2:
        # Field frequency distribution chart
        if len(schema_summary) > 1:
            frequency_data = []
            for item in schema_summary[:15]:  # Top 15 fields
                field_name = item['Field Path']
                if len(field_name) > 25:
                    field_name = field_name[:22] + "..."
                frequency_val = float(item['Frequency'].rstrip('%')) / 100
                frequency_data.append((field_name, frequency_val))
            
            chart_df = pd.DataFrame(frequency_data, columns=['Field', 'Frequency'])
            st.bar_chart(chart_df.set_index('Field'), height=400)
            
            st.info("💡 **Tip:** Higher frequency fields are found in more JSON records and are better candidates for filtering.")
        else:
            st.info("Not enough fields for frequency visualization")
    
    with tab3:
        # Type distribution
        type_counts = {}
        for details in schema.values():
            field_type = details.get('snowflake_type', 'VARIANT')
            type_counts[field_type] = type_counts.get(field_type, 0) + 1
        
        if type_counts:
            type_df = pd.DataFrame(list(type_counts.items()), columns=['Data Type', 'Count'])
            st.bar_chart(type_df.set_index('Data Type'))
            
            # Type summary table
            st.markdown("**Type Distribution:**")
            type_df['Percentage'] = (type_df['Count'] / type_df['Count'].sum() * 100).round(1)
            type_df['Percentage'] = type_df['Percentage'].astype(str) + '%'
            st.dataframe(type_df, use_container_width=True)


def render_enhanced_field_suggestions(schema: Dict) -> List[str]:
    """
    Generate intelligent field condition suggestions based on discovered schema
    """
    suggestions = []
    
    # Categorize fields by type and frequency
    high_frequency_fields = []
    categorical_fields = []
    numeric_fields = []
    
    for path, details in schema.items():
        frequency = details.get('frequency', 0)
        field_type = details.get('snowflake_type', 'VARIANT')
        sample_values = details.get('sample_values', [])
        is_queryable = details.get('is_queryable', False)
        
        if not is_queryable:
            continue
            
        if frequency > 0.7:  # High frequency fields
            high_frequency_fields.append((path, details))
        
        if field_type in ['VARCHAR', 'STRING'] and sample_values:
            # Check if it looks like categorical data
            unique_vals = list(set([str(v) for v in sample_values]))
            if len(unique_vals) <= 3 and all(len(str(v)) < 20 for v in unique_vals):
                categorical_fields.append((path, unique_vals))
        
        if field_type in ['NUMBER', 'INTEGER'] and sample_values:
            numeric_fields.append((path, details))
    
    # Generate suggestions based on analysis
    
    # 1. High frequency NOT NULL checks
    for path, details in high_frequency_fields[:3]:
        suggestions.append(f"{path}[IS NOT NULL]")
    
    # 2. Categorical field suggestions
    for path, unique_vals in categorical_fields[:3]:
        if len(unique_vals) > 1:
            suggestions.append(f"{path}[IN:{' | '.join(unique_vals)}]")
        else:
            suggestions.append(f"{path}[=:{unique_vals[0]}]")
    
    # 3. Numeric field range suggestions
    for path, details in numeric_fields[:2]:
        sample_values = details.get('sample_values', [])
        try:
            numeric_vals = [float(v) for v in sample_values if str(v).replace('.', '').replace('-', '').isdigit()]
            if numeric_vals:
                avg_val = sum(numeric_vals) / len(numeric_vals)
                suggestions.append(f"{path}[>:{avg_val:.0f}]")
        except:
            suggestions.append(f"{path}[IS NOT NULL]")
    
    # 4. Complex combinations for advanced users
    if len(high_frequency_fields) >= 2:
        field1, field2 = high_frequency_fields[0][0], high_frequency_fields[1][0]
        suggestions.append(f"{field1}[IS NOT NULL], {field2}[IS NOT NULL]")
    
    return suggestions[:8]  # Return top 8 suggestions


def test_database_connectivity(conn_manager) -> Tuple[bool, str]:
    """
    Test database connectivity with comprehensive diagnostics
    """
    if not conn_manager.is_connected:
        return False, "❌ Not connected to database"
    
    try:
        # Test basic connectivity
        test_query = "SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_USER(), CURRENT_ROLE()"
        result_df, error = conn_manager.execute_query(test_query)
        
        if result_df is not None and not result_df.empty:
            row = result_df.iloc[0]
            database = row.iloc[0] if row.iloc[0] else "None"
            schema = row.iloc[1] if row.iloc[1] else "None"
            user = row.iloc[2] if row.iloc[2] else "Unknown"
            role = row.iloc[3] if row.iloc[3] else "Default"
            
            status_msg = f"""✅ **Database Connection Status:**
- **Database:** {database}
- **Schema:** {schema}  
- **User:** {user}
- **Role:** {role}
- **Status:** Connected and Ready"""
            
            return True, status_msg
        else:
            return False, f"❌ Connection test failed: {error}"
            
    except Exception as e:
        return False, f"❌ Connection test error: {str(e)}"
