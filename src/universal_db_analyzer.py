import streamlit as st
import json
from typing import Dict, Any, List, Tuple, Optional
import logging

logger = logging.getLogger(__name__)


def execute_custom_sql_query(conn_manager, custom_query: str) -> Tuple[Optional[Any], Optional[str]]:
    """
    MOVED TO TOP: Execute custom SQL queries (helpful for listing tables, etc.)
    """
    if not conn_manager.is_connected:
        return None, "❌ Not connected to database"

    try:
        # Handle enhanced connectors with session context
        if hasattr(conn_manager, 'ensure_session_context'):
            if not conn_manager.ensure_session_context():
                return None, "❌ Failed to establish database session context"

        st.info(f"🔍 Executing query: `{custom_query[:100]}{'...' if len(custom_query) > 100 else ''}`")

        result_df, error_msg = conn_manager.execute_query(custom_query)

        if result_df is None:
            return None, f"❌ Query failed: {error_msg}"

        if result_df.empty:
            return None, "❌ Query returned no results"

        return result_df, None

    except Exception as e:
        logger.error(f"Custom SQL execution failed: {e}")
        return None, f"❌ Custom SQL execution failed: {str(e)}"


def list_available_tables(conn_manager) -> Tuple[Optional[List[str]], Optional[str]]:
    """
    Helper function to list available tables using custom SQL
    """
    list_tables_query = """
    SELECT 
        TABLE_CATALOG as DATABASE_NAME,
        TABLE_SCHEMA as SCHEMA_NAME, 
        TABLE_NAME,
        TABLE_TYPE
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA'
    ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
    LIMIT 50
    """
    
    result_df, error = execute_custom_sql_query(conn_manager, list_tables_query)
    
    if result_df is None:
        return None, error
    
    # Format table names with full qualification
    table_list = []
    for _, row in result_df.iterrows():
        db = row.get('DATABASE_NAME', '')
        schema = row.get('SCHEMA_NAME', '')
        table = row.get('TABLE_NAME', '')
        table_type = row.get('TABLE_TYPE', '')
        
        full_name = f"{db}.{schema}.{table}"
        table_list.append(f"{full_name} ({table_type})")
    
    return table_list, None


def sample_json_from_database(conn_manager, table_name: str, json_column: str, 
                            sample_size: int = 5) -> Tuple[Optional[List[Dict]], Optional[str]]:
    """
    Universal JSON sampling that works with both standard and enhanced connectors
    """
    if not conn_manager.is_connected:
        return None, "❌ Not connected to database"

    try:
        # Handle enhanced connectors with session context
        if hasattr(conn_manager, 'ensure_session_context'):
            if not conn_manager.ensure_session_context():
                return None, "❌ Failed to establish database session context"

        # Smart table name resolution
        resolved_table_name = resolve_table_name_universal(conn_manager, table_name)

        # Query to sample JSON data
        sample_query = f"""
        SELECT {json_column}
        FROM {resolved_table_name}
        WHERE {json_column} IS NOT NULL
        LIMIT {sample_size}
        """

        st.info(f"🔍 Sampling query: `{sample_query}`")

        result_df, error_msg = conn_manager.execute_query(sample_query)

        if result_df is None:
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
                        parsed_json = json.loads(str(json_value))

                    json_samples.append(parsed_json)

                except (json.JSONDecodeError, TypeError) as e:
                    parsing_errors += 1
                    logger.warning(f"Failed to parse JSON from row {idx}: {e}")
                    continue

        if not json_samples:
            if parsing_errors > 0:
                return None, f"❌ No valid JSON found in sampled records. {parsing_errors} parsing errors occurred."
            else:
                return None, f"❌ No valid JSON found in sampled records from {resolved_table_name}.{json_column}"

        # Report success
        if parsing_errors > 0:
            st.warning(f"⚠️ {parsing_errors} rows had JSON parsing issues, but {len(json_samples)} were successfully parsed.")
        else:
            st.success(f"✅ Successfully parsed {len(json_samples)} JSON records from {resolved_table_name}")

        return json_samples, None

    except Exception as e:
        error_msg = str(e)

        # Provide specific error guidance
        if "does not have a current database" in error_msg:
            return None, "❌ Database context error. Try disconnecting and reconnecting with proper database/schema settings."
        elif "does not exist" in error_msg:
            return None, f"❌ Table {table_name} does not exist or you don't have access to it."
        elif "Invalid identifier" in error_msg:
            return None, f"❌ Invalid column name '{json_column}' in table {table_name}."
        elif "permission" in error_msg.lower() or "access" in error_msg.lower():
            return None, f"❌ Permission denied accessing {table_name}."
        else:
            return None, f"❌ Database sampling failed: {error_msg}"


def resolve_table_name_universal(conn_manager, table_name: str) -> str:
    """
    Universal table name resolution that works with both connector types
    """
    # If already fully qualified, return as-is
    if table_name.count('.') == 2:
        return table_name

    # Get connection parameters
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
        return table_name


def analyze_database_json_schema_universal(conn_manager, table_name: str, json_column: str, 
                                         sample_size: int = 10) -> Tuple[Optional[Dict], Optional[str], Dict]:
    """
    ENHANCED UNIVERSAL JSON schema analysis with disambiguation support
    """
    metadata = {
        'table_name': table_name,
        'json_column': json_column,
        'sample_size': 0,
        'unique_schemas': 0,
        'analysis_success': False,
        'resolved_table_name': resolve_table_name_universal(conn_manager, table_name),
        'field_conflicts': 0,
        'disambiguation_info': {}
    }

    # Create progress tracking
    progress_bar = st.progress(0)
    status_text = st.empty()

    try:
        # Step 1: Sample JSON data from database
        status_text.text("🔍 Sampling JSON data from database...")
        progress_bar.progress(20)

        json_samples, error_msg = sample_json_from_database(
            conn_manager, table_name, json_column, sample_size
        )

        if json_samples is None:
            progress_bar.empty()
            status_text.empty()
            return None, error_msg, metadata

        metadata['sample_size'] = len(json_samples)
        progress_bar.progress(40)

        # Step 2: Analyze JSON structure using the ENHANCED SQL generator logic
        status_text.text("🔬 Analyzing JSON structure with disambiguation tracking...")

        # ENHANCED: Import the enhanced SQL generator
        from python_sql_generator import PythonSQLGenerator

        # Create analyzer instance
        generator = PythonSQLGenerator()
        
        # ENHANCED: Use the first sample as the base structure
        base_json = json_samples[0]
        combined_schema = generator.analyze_json_for_sql(base_json)

        # Process additional samples for enhanced metadata
        unique_structures = set()
        structure_signature = str(sorted(combined_schema.keys()))
        unique_structures.add(structure_signature)

        for i, json_sample in enumerate(json_samples[1:], 1):
            try:
                # Update progress
                progress_percentage = 40 + (i / len(json_samples)) * 40
                progress_bar.progress(int(progress_percentage))

                # ENHANCED: Analyze additional samples
                sample_schema = generator.analyze_json_for_sql(json_sample)

                # Track unique structures
                structure_signature = str(sorted(sample_schema.keys()))
                unique_structures.add(structure_signature)

                # Merge schemas for enhanced metadata
                for path, details in sample_schema.items():
                    if path in combined_schema:
                        # Handle type conflicts
                        existing_type = combined_schema[path].get('type')
                        current_type = details.get('type')

                        if existing_type != current_type:
                            combined_schema[path]['type'] = 'variant'
                            combined_schema[path]['snowflake_type'] = 'VARIANT'
                            combined_schema[path]['has_type_conflicts'] = True

                        # Track sample values
                        existing_samples = combined_schema[path].get('sample_values', [])
                        new_sample = details.get('sample_value', '')
                        if new_sample not in existing_samples:
                            existing_samples.append(new_sample)
                            combined_schema[path]['sample_values'] = existing_samples[:5]
                    else:
                        # New field found
                        combined_schema[path] = details.copy()
                        combined_schema[path]['sample_values'] = [details.get('sample_value', '')]
                        combined_schema[path]['found_in_samples'] = 1

                    # Track frequency across samples
                    combined_schema[path]['found_in_samples'] = combined_schema[path].get('found_in_samples', 0) + 1
                    combined_schema[path]['frequency'] = combined_schema[path]['found_in_samples'] / len(json_samples)

            except Exception as e:
                logger.warning(f"Failed to analyze JSON sample {i}: {e}")
                continue

        progress_bar.progress(80)
        status_text.text("✅ Finalizing schema analysis with disambiguation...")

        # Ensure all fields have proper metadata
        for path, details in combined_schema.items():
            if 'sample_values' not in details:
                details['sample_values'] = [details.get('sample_value', '')]
            if 'found_in_samples' not in details:
                details['found_in_samples'] = 1
            if 'frequency' not in details:
                details['frequency'] = 1.0

        # ENHANCED: Get disambiguation information
        disambiguation_info = generator.get_field_disambiguation_info()
        metadata['field_conflicts'] = len(disambiguation_info)
        metadata['disambiguation_info'] = disambiguation_info
        metadata['unique_schemas'] = len(unique_structures)
        metadata['analysis_success'] = len(combined_schema) > 0

        progress_bar.progress(100)
        status_text.text("✅ Schema analysis complete with disambiguation tracking!")

        # Clean up progress indicators
        import time
        time.sleep(1)
        progress_bar.empty()
        status_text.empty()

        if not combined_schema:
            return None, "❌ No valid JSON schema could be extracted from database samples", metadata

        # ENHANCED: Display disambiguation summary
        if disambiguation_info:
            st.warning(f"🚨 Found {len(disambiguation_info)} field names with multiple locations in your database JSON")
            
            with st.expander("🔍 Field Conflict Summary", expanded=False):
                for field_name, conflict_data in disambiguation_info.items():
                    st.markdown(f"**Field: `{field_name}`** ({conflict_data['conflict_count']} occurrences)")
                    for i, option in enumerate(conflict_data['options'][:3]):
                        status = "🎯 Recommended" if i == 0 else "⚪ Alternative"
                        queryable = "✅" if option['queryable'] else "❌"
                        st.markdown(f"- {status} `{option['full_path']}` ({option['hierarchy_description']}) - Queryable: {queryable}")
        else:
            st.success("🎯 No field name conflicts - all fields have unique names!")

        # VERIFICATION: Check for any remaining issues
        sample_prefix_paths = [path for path in combined_schema.keys() if "sample_" in path]
        if sample_prefix_paths:
            st.error(f"🚨 **BUG DETECTED:** Found {len(sample_prefix_paths)} paths with 'sample_' prefixes!")
            return None, "Schema analysis contains invalid sample prefixes", metadata
        else:
            st.success("🎯 **VERIFIED:** All JSON paths are clean with disambiguation support")

        return combined_schema, None, metadata

    except Exception as e:
        progress_bar.empty()
        status_text.empty()
        logger.error(f"Schema analysis failed: {e}")
        return None, f"❌ Schema analysis failed: {str(e)}", metadata


def generate_database_driven_sql(conn_manager, table_name: str, json_column: str, 
                               field_conditions: str) -> Tuple[Optional[str], Optional[str]]:
    """
    ENHANCED UNIVERSAL database-driven SQL generation with disambiguation support
    """
    try:
        # Step 1: Analyze JSON schema from database with disambiguation
        st.info("🔍 **Step 1:** Analyzing JSON structure with disambiguation tracking...")

        schema, schema_error, metadata = analyze_database_json_schema_universal(
            conn_manager, table_name, json_column, sample_size=10
        )

        if schema is None:
            return None, f"Schema analysis failed: {schema_error}"

        # Display enhanced analysis results
        st.success("✅ **Step 1 Complete:** Enhanced JSON Schema Analysis with Disambiguation")

        # Show enhanced metrics
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("📊 Records Sampled", metadata['sample_size'])
        with col2:
            st.metric("🏷️ JSON Fields Found", len(schema))
        with col3:
            st.metric("🔄 Unique Structures", metadata['unique_schemas'])
        with col4:
            conflict_indicator = f"🚨 {metadata['field_conflicts']}" if metadata['field_conflicts'] > 0 else "✅ 0"
            st.metric("⚠️ Field Conflicts", conflict_indicator)
        with col5:
            success_indicator = "✅" if metadata['analysis_success'] else "❌"
            st.metric("📈 Analysis Status", success_indicator)

        st.info(f"📋 **Analyzed Table:** `{metadata['resolved_table_name']}`")

        # Step 2: Generate SQL using the ENHANCED logic with disambiguation
        st.info("🔨 **Step 2:** Generating SQL with Enhanced Disambiguation Logic...")

        # ENHANCED: Use the enhanced SQL generator with warnings
        from python_sql_generator import PythonSQLGenerator
        
        generator = PythonSQLGenerator()

        # Use resolved table name
        resolved_table_name = metadata['resolved_table_name']

        # ENHANCED: Generate SQL with warnings and disambiguation info
        generated_sql, warnings = generator.generate_sql_with_warnings(
            resolved_table_name, json_column, field_conditions, schema
        )

        # Display disambiguation warnings
        if warnings:
            st.markdown("#### 🔔 Disambiguation & Resolution Alerts")
            for warning in warnings:
                if warning.startswith('⚠️'):
                    st.warning(warning)
                elif warning.startswith('ℹ️'):
                    st.info(warning)
                else:
                    st.info(warning)

        if generated_sql.strip().startswith("-- Error"):
            return None, f"SQL generation failed: {generated_sql}"

        # ENHANCED VERIFICATION: Check generated SQL for issues
        issues_found = []
        
        if "sample_0" in generated_sql or "sample_1" in generated_sql:
            issues_found.append("Contains 'sample_' prefixes")
            
        # Check for duplicate LATERAL FLATTEN aliases
        lateral_flattens = [line for line in generated_sql.split('\n') if 'LATERAL FLATTEN' in line]
        flatten_aliases = []
        for line in lateral_flattens:
            if ') f' in line:
                alias = line.split(') f')[1].split()[0]
                flatten_aliases.append(alias)
        
        duplicate_aliases = [alias for alias in set(flatten_aliases) if flatten_aliases.count(alias) > 1]
        if duplicate_aliases:
            issues_found.append(f"Duplicate aliases: {duplicate_aliases}")

        if issues_found:
            st.error(f"🚨 **SQL ISSUES DETECTED:** {'; '.join(issues_found)}")
            st.code(generated_sql, language="sql")
            return None, f"Generated SQL has issues: {'; '.join(issues_found)}"
        else:
            st.success("🎯 **VERIFIED:** Generated SQL is clean with disambiguation support")

        # ENHANCED: Show disambiguation details if used
        disambiguation_info = generator.get_field_disambiguation_info()
        if disambiguation_info and warnings:
            with st.expander("🔍 Field Resolution Details", expanded=False):
                st.markdown("**Enhanced Disambiguation Summary:**")
                conditions = [c.strip() for c in field_conditions.split(',')]
                for condition in conditions:
                    if condition:
                        field_name = condition.split('[')[0].strip()
                        simple_name = field_name.split('.')[-1]
                        
                        if simple_name in disambiguation_info:
                            conflict_data = disambiguation_info[simple_name]
                            st.markdown(f"**Field: `{field_name}`**")
                            for opt in conflict_data['options']:
                                status = "✅ Selected" if opt['full_path'] in generated_sql else "⏸️ Available"
                                priority = "🎯 Recommended" if opt == conflict_data.get('recommended_option') else "⚪ Alternative"
                                st.markdown(f"- {status} {priority} `{opt['full_path']}` ({opt['hierarchy_description']})")

        st.success("✅ **Step 2 Complete:** Enhanced SQL Generated with Disambiguation Support!")
        
        return generated_sql, None

    except Exception as e:
        logger.error(f"Enhanced database-driven analysis failed: {e}")
        return None, f"❌ Enhanced database-driven analysis failed: {str(e)}"


def generate_sql_from_json_python_mode_enhanced(json_data: Any, table_name: str, json_column: str, field_conditions: str) -> Tuple[str, List[str]]:
    """
    ENHANCED: Python mode SQL generation with disambiguation support
    Returns: (sql, warnings)
    """
    try:
        # ENHANCED: Import the enhanced SQL generator
        from python_sql_generator import PythonSQLGenerator
        
        generator = PythonSQLGenerator()
        
        # ENHANCED: Analyze JSON structure with disambiguation
        schema = generator.analyze_json_for_sql(json_data)
        
        if not schema:
            return "-- Error: Could not analyze JSON structure", ["❌ Schema analysis failed"]
        
        # ENHANCED: Generate SQL with warnings and disambiguation
        generated_sql, warnings = generator.generate_sql_with_warnings(table_name, json_column, field_conditions, schema)
        
        # VERIFICATION: Check for issues
        if "sample_" in generated_sql:
            return f"-- Error: Generated SQL contains sample prefixes:\n{generated_sql}", ["❌ SQL contains invalid prefixes"]
        
        if generated_sql.strip().startswith("-- Error"):
            return generated_sql, ["❌ SQL generation failed"]
            
        return generated_sql, warnings
        
    except Exception as e:
        logger.error(f"Enhanced Python mode SQL generation failed: {e}")
        return f"-- Error in enhanced Python mode: {str(e)}", [f"❌ Generation error: {str(e)}"]


def render_enhanced_database_json_preview(schema: Dict, metadata: Dict):
    """
    Enhanced preview of the discovered JSON schema with disambiguation support
    """
    st.markdown("### 📊 Enhanced JSON Schema Analysis with Disambiguation")

    # ENHANCED: Show disambiguation summary first
    disambiguation_info = metadata.get('disambiguation_info', {})
    if disambiguation_info:
        st.markdown("#### 🚨 Field Disambiguation Summary")
        st.warning(f"Found {len(disambiguation_info)} field names appearing in multiple locations")
        
        # Quick disambiguation overview
        for field_name, conflict_data in disambiguation_info.items():
            with st.expander(f"🔍 Field: `{field_name}` ({conflict_data['conflict_count']} locations)", expanded=False):
                recommended = conflict_data.get('recommended_option', {})
                if recommended:
                    st.success(f"🎯 **Recommended:** `{recommended.get('full_path', 'N/A')}` ({recommended.get('hierarchy_description', 'N/A')})")
                
                st.markdown("**All Options:**")
                for i, opt in enumerate(conflict_data['options']):
                    priority_icon = "🎯" if i == 0 else "⚪"
                    queryable_icon = "✅" if opt['queryable'] else "❌"
                    st.markdown(f"- {priority_icon} `{opt['full_path']}` - {opt['hierarchy_description']} - Queryable: {queryable_icon}")

    # VERIFICATION: Check for sample prefixes in schema
    sample_prefix_count = sum(1 for path in schema.keys() if "sample_" in path)
    if sample_prefix_count > 0:
        st.error(f"🚨 **BUG DETECTED:** {sample_prefix_count} paths contain 'sample_' prefixes!")
    else:
        st.success("🎯 **VERIFIED:** All paths are clean with disambiguation support")

    # Create tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs(["🏷️ **Field Details**", "📊 **Frequency Chart**", "🔍 **Type Distribution**", "⚠️ **Disambiguation Guide**"])

    with tab1:
        schema_summary = []

        for path, details in sorted(schema.items(), key=lambda x: x[1].get('frequency', 0), reverse=True):
            frequency = details.get('frequency', 0)
            field_type = details.get('snowflake_type', 'VARIANT')
            sample_values = details.get('sample_values', ['N/A'])
            found_in = details.get('found_in_samples', 0)
            hierarchy = details.get('hierarchy_level', 'Unknown')

            # Create readable sample values
            readable_samples = []
            for val in sample_values[:3]:
                val_str = str(val)
                if len(val_str) > 50:
                    val_str = val_str[:47] + "..."
                readable_samples.append(val_str)

            # Enhanced status with disambiguation info
            path_status = "✅ Clean"
            field_name = path.split('.')[-1]
            if field_name in disambiguation_info:
                conflict_count = disambiguation_info[field_name]['conflict_count']
                path_status = f"⚠️ Conflicts ({conflict_count})"

            schema_summary.append({
                'Field Path': path,
                'Field Name': field_name,
                'Status': path_status,
                'Hierarchy': hierarchy,
                'Snowflake Type': field_type,
                'Frequency': f"{frequency:.1%}",
                'Found In Samples': f"{found_in}/{metadata['sample_size']}",
                'Sample Values': ' | '.join(readable_samples) if readable_samples else 'N/A',
                'Queryable': '✅' if details.get('is_queryable', False) else '❌'
            })

        # Show fields in dataframe
        import pandas as pd
        schema_df = pd.DataFrame(schema_summary)

        # Add enhanced search functionality
        search_term = st.text_input("🔍 Search fields:", placeholder="Type to filter fields...")
        if search_term:
            schema_df = schema_df[schema_df['Field Path'].str.contains(search_term, case=False, na=False)]

        # Filter options
        col_filter1, col_filter2 = st.columns(2)
        with col_filter1:
            show_conflicts_only = st.checkbox("Show only conflicted fields", value=False)
            if show_conflicts_only:
                schema_df = schema_df[schema_df['Status'].str.contains('Conflicts')]
        with col_filter2:
            show_queryable_only = st.checkbox("Show only queryable fields", value=False)
            if show_queryable_only:
                schema_df = schema_df[schema_df['Queryable'] == '✅']

        st.dataframe(schema_df, use_container_width=True, height=400)

        # Enhanced export option
        csv_data = schema_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            "📥 Export Enhanced Schema Analysis",
            data=csv_data,
            file_name=f"enhanced_json_schema_analysis_{metadata['table_name']}_{metadata['json_column']}.csv",
            mime="text/csv"
        )

    with tab2:
        # Frequency chart
        if len(schema_summary) > 1:
            frequency_data = []
            for item in schema_summary[:15]:
                field_name = item['Field Path']
                if len(field_name) > 25:
                    field_name = field_name[:22] + "..."
                frequency_val = float(item['Frequency'].rstrip('%')) / 100
                frequency_data.append((field_name, frequency_val))

            chart_df = pd.DataFrame(frequency_data, columns=['Field', 'Frequency'])
            st.bar_chart(chart_df.set_index('Field'), height=400)
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
            
            # Type summary
            st.markdown("**Type Distribution:**")
            type_df['Percentage'] = (type_df['Count'] / type_df['Count'].sum() * 100).round(1)
            type_df['Percentage'] = type_df['Percentage'].astype(str) + '%'
            st.dataframe(type_df, use_container_width=True)

    with tab4:
        # Enhanced disambiguation guide
        st.markdown("#### 🎯 Smart Field Resolution Guide")
        
        if disambiguation_info:
            st.markdown("**How to handle field conflicts in your queries:**")
            
            for field_name, conflict_data in disambiguation_info.items():
                st.markdown(f"**Field: `{field_name}`**")
                
                col_guide1, col_guide2 = st.columns(2)
                
                with col_guide1:
                    st.markdown("**❌ Ambiguous (auto-resolved):**")
                    st.code(f"{field_name}", language="text")
                    recommended = conflict_data.get('recommended_option', {})
                    if recommended:
                        st.caption(f"→ Resolves to: {recommended.get('full_path', 'N/A')}")
                
                with col_guide2:
                    st.markdown("**✅ Explicit (recommended):**")
                    for opt in conflict_data['options'][:2]:
                        if opt['queryable']:
                            st.code(f"{opt['full_path']}", language="text")
                            st.caption(f"→ {opt['hierarchy_description']}")
                
                st.markdown("---")
                
            st.info("💡 **Tip:** Always use explicit paths (like `company.name`) instead of simple field names (like `name`) to avoid ambiguity.")
        else:
            st.success("🎉 **No field conflicts detected!** You can use simple field names without ambiguity.")
            
            # Show some example fields
            example_fields = [path for path, details in schema.items() if details.get('is_queryable', False)][:5]
            if example_fields:
                st.markdown("**Example queryable fields:**")
                for field in example_fields:
                    st.code(field, language="text")


def render_enhanced_field_suggestions(schema: Dict, disambiguation_info: Dict = None) -> List[str]:
    """
    Generate intelligent field suggestions with disambiguation awareness
    """
    suggestions = []

    # Categorize fields with disambiguation awareness
    high_frequency_fields = []
    categorical_fields = []
    numeric_fields = []
    conflicted_fields = set()

    # Track conflicted field names
    if disambiguation_info:
        for field_name, conflict_data in disambiguation_info.items():
            conflicted_fields.add(field_name)

    for path, details in schema.items():
        # Skip any paths with sample prefixes
        if "sample_" in path:
            continue
            
        frequency = details.get('frequency', 0)
        field_type = details.get('snowflake_type', 'VARIANT')
        sample_values = details.get('sample_values', [])
        is_queryable = details.get('is_queryable', False)
        field_name = path.split('.')[-1]

        if not is_queryable:
            continue

        # For conflicted fields, always use full path
        suggestion_path = path if field_name in conflicted_fields else path

        if frequency > 0.7:
            high_frequency_fields.append((suggestion_path, details))

        if field_type in ['VARCHAR', 'STRING'] and sample_values:
            unique_vals = list(set([str(v) for v in sample_values]))
            if len(unique_vals) <= 3 and all(len(str(v)) < 20 for v in unique_vals):
                categorical_fields.append((suggestion_path, unique_vals))

        if field_type in ['NUMBER', 'INTEGER'] and sample_values:
            numeric_fields.append((suggestion_path, details))

    # Generate enhanced suggestions with disambiguation awareness

    # High frequency fields (always use full path for conflicted fields)
    for path, details in high_frequency_fields[:3]:
        field_name = path.split('.')[-1]
        if field_name in conflicted_fields:
            suggestions.append(f"{path}[IS NOT NULL]  # Using full path to avoid ambiguity")
        else:
            suggestions.append(f"{path}[IS NOT NULL]")

    # Categorical fields
    for path, unique_vals in categorical_fields[:3]:
        field_name = path.split('.')[-1]
        if len(unique_vals) > 1:
            if field_name in conflicted_fields:
                suggestions.append(f"{path}[IN:{' | '.join(unique_vals)}]  # Full path used")
            else:
                suggestions.append(f"{path}[IN:{' | '.join(unique_vals)}]")
        else:
            if field_name in conflicted_fields:
                suggestions.append(f"{path}[=:{unique_vals[0]}]  # Full path used")
            else:
                suggestions.append(f"{path}[=:{unique_vals[0]}]")

    # Numeric fields
    for path, details in numeric_fields[:2]:
        field_name = path.split('.')[-1]
        sample_values = details.get('sample_values', [])
        try:
            numeric_vals = [float(v) for v in sample_values if str(v).replace('.', '').replace('-', '').isdigit()]
            if numeric_vals:
                avg_val = sum(numeric_vals) / len(numeric_vals)
                if field_name in conflicted_fields:
                    suggestions.append(f"{path}[>:{avg_val:.0f}]  # Full path used")
                else:
                    suggestions.append(f"{path}[>:{avg_val:.0f}]")
        except:
            if field_name in conflicted_fields:
                suggestions.append(f"{path}[IS NOT NULL]  # Full path used")
            else:
                suggestions.append(f"{path}[IS NOT NULL]")

    # Enhanced combinations with disambiguation awareness
    if len(high_frequency_fields) >= 2:
        field1, field2 = high_frequency_fields[0][0], high_frequency_fields[1][0]
        suggestions.append(f"{field1}[IS NOT NULL], {field2}[IS NOT NULL]")

    # Add disambiguation examples if conflicts exist
    if disambiguation_info:
        conflict_example = list(disambiguation_info.keys())[0]
        options = disambiguation_info[conflict_example]['options'][:2]
        suggestions.append(f"# Disambiguation example:")
        suggestions.append(f"{', '.join([opt['full_path'] for opt in options if opt['queryable']])}")

    return suggestions[:10]


def test_database_connectivity(conn_manager) -> Tuple[bool, str]:
    """
    Test database connectivity with enhanced diagnostics
    """
    if not conn_manager.is_connected:
        return False, "❌ Not connected to database"

    try:
        test_query = "SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_USER(), CURRENT_ROLE()"
        result_df, error = conn_manager.execute_query(test_query)

        if result_df is not None and not result_df.empty:
            row = result_df.iloc[0]
            database = row.iloc[0] if row.iloc[0] else "None"
            schema = row.iloc[1] if row.iloc[1] else "None"
            user = row.iloc[2] if row.iloc[2] else "Unknown"
            role = row.iloc[3] if row.iloc[3] else "Default"

            status_msg = f"""✅ **Enhanced Database Connection Status:**
- **Database:** {database}
- **Schema:** {schema}  
- **User:** {user}
- **Role:** {role}
- **Features:** Enhanced SQL Generation with Disambiguation
- **Status:** Connected and Ready for Smart Analysis
"""
            return True, status_msg
        else:
            return False, f"❌ Connection test failed: {error}"

    except Exception as e:
        return False, f"❌ Connection test error: {str(e)}"


def render_disambiguation_helper_ui(field_conditions: str, disambiguation_info: Dict):
    """
    Render UI helper for field disambiguation
    """
    if not disambiguation_info:
        return
    
    st.markdown("#### 🎯 Smart Field Helper")
    
    # Parse current field conditions
    conditions = [c.strip() for c in field_conditions.split(',') if c.strip()]
    
    # Check for potential ambiguity
    potentially_ambiguous = []
    for condition in conditions:
        field_name = condition.split('[')[0].strip()
        simple_field_name = field_name.split('.')[-1]
        
        if simple_field_name in disambiguation_info and field_name == simple_field_name:
            potentially_ambiguous.append((condition, simple_field_name))
    
    if potentially_ambiguous:
        st.warning("⚠️ Some fields may be ambiguous. Consider using explicit paths:")
        
        for condition, simple_name in potentially_ambiguous:
            conflict_data = disambiguation_info[simple_name]
            st.markdown(f"**Field `{simple_name}` in condition `{condition}`:**")
            
            cols = st.columns(len(conflict_data['options']) + 1)
            
            with cols[0]:
                st.markdown("**Options:**")
            
            for i, option in enumerate(conflict_data['options'][:3]):
                if option['queryable']:
                    with cols[i + 1]:
                        priority = "🎯 Recommended" if i == 0 else f"⚪ Option {i + 1}"
                        if st.button(f"{priority}\n`{option['full_path']}`", key=f"disambig_{simple_name}_{i}"):
                            # Replace the ambiguous condition with explicit path
                            new_condition = condition.replace(simple_name, option['full_path'])
                            new_conditions = [new_condition if c == condition else c for c in conditions]
                            st.session_state.unified_field_conditions = ', '.join(new_conditions)
                            st.rerun()
                        st.caption(option['hierarchy_description'])


# Enhanced version aliases for backward compatibility
def generate_database_driven_sql_enhanced(conn_manager, table_name: str, json_column: str, 
                                        field_conditions: str) -> Tuple[Optional[str], Optional[str]]:
    """Enhanced version that's now the main implementation"""
    return generate_database_driven_sql(conn_manager, table_name, json_column, field_conditions)


def analyze_database_json_schema_enhanced(conn_manager, table_name: str, json_column: str, 
                                        sample_size: int = 10) -> Tuple[Optional[Dict], Optional[str], Dict]:
    """Enhanced version that's now the main implementation"""
    return analyze_database_json_schema_universal(conn_manager, table_name, json_column, sample_size)
