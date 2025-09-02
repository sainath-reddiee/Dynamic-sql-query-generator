import streamlit as st
import json
from typing import Dict, Any, List, Tuple, Optional
import logging

logger = logging.getLogger(__name__)


def execute_custom_sql_query(conn_manager, custom_query: str) -> Tuple[Optional[Any], Optional[str]]:
    """Execute custom SQL queries (helpful for listing tables, etc.)"""
    if not conn_manager.is_connected:
        return None, "❌ Not connected to database"

    try:
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
    """Helper function to list available tables using custom SQL"""
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
    """Universal JSON sampling that works with both standard and enhanced connectors"""
    if not conn_manager.is_connected:
        return None, "❌ Not connected to database"

    try:
        if hasattr(conn_manager, 'ensure_session_context'):
            if not conn_manager.ensure_session_context():
                return None, "❌ Failed to establish database session context"

        resolved_table_name = resolve_table_name_universal(conn_manager, table_name)

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

        json_samples = []
        parsing_errors = 0

        for idx, row in result_df.iterrows():
            json_value = row[json_column]

            if json_value is not None:
                try:
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

        if parsing_errors > 0:
            st.warning(f"⚠️ {parsing_errors} rows had JSON parsing issues, but {len(json_samples)} were successfully parsed.")
        else:
            st.success(f"✅ Successfully parsed {len(json_samples)} JSON records from {resolved_table_name}")

        return json_samples, None

    except Exception as e:
        error_msg = str(e)

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
    """Universal table name resolution that works with both connector types"""
    if table_name.count('.') == 2:
        return table_name

    database = conn_manager.connection_params.get('database', '')
    schema = conn_manager.connection_params.get('schema', 'PUBLIC')

    if '.' not in table_name:
        return f"{database}.{schema}.{table_name}"
    elif table_name.count('.') == 1:
        return f"{database}.{table_name}"
    else:
        return table_name


def analyze_database_json_schema_universal(conn_manager, table_name: str, json_column: str, 
                                         sample_size: int = 10) -> Tuple[Optional[Dict], Optional[str], Dict]:
    """ENHANCED UNIVERSAL JSON schema analysis with multi-level field support"""
    metadata = {
        'table_name': table_name,
        'json_column': json_column,
        'sample_size': 0,
        'unique_schemas': 0,
        'analysis_success': False,
        'resolved_table_name': resolve_table_name_universal(conn_manager, table_name),
        'multi_level_fields': 0,
        'multi_level_info': {}
    }

    progress_bar = st.progress(0)
    status_text = st.empty()

    try:
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

        status_text.text("🔬 Analyzing JSON structure with multi-level field detection...")

        from python_sql_generator import PythonSQLGenerator

        generator = PythonSQLGenerator()
        base_json = json_samples[0]
        combined_schema = generator.analyze_json_for_sql(base_json)

        unique_structures = set()
        structure_signature = str(sorted(combined_schema.keys()))
        unique_structures.add(structure_signature)

        for i, json_sample in enumerate(json_samples[1:], 1):
            try:
                progress_percentage = 40 + (i / len(json_samples)) * 40
                progress_bar.progress(int(progress_percentage))

                sample_schema = generator.analyze_json_for_sql(json_sample)
                structure_signature = str(sorted(sample_schema.keys()))
                unique_structures.add(structure_signature)

                for path, details in sample_schema.items():
                    if path in combined_schema:
                        existing_type = combined_schema[path].get('type')
                        current_type = details.get('type')

                        if existing_type != current_type:
                            combined_schema[path]['type'] = 'variant'
                            combined_schema[path]['snowflake_type'] = 'VARIANT'
                            combined_schema[path]['has_type_conflicts'] = True

                        existing_samples = combined_schema[path].get('sample_values', [])
                        new_sample = details.get('sample_value', '')
                        if new_sample not in existing_samples:
                            existing_samples.append(new_sample)
                            combined_schema[path]['sample_values'] = existing_samples[:5]
                    else:
                        combined_schema[path] = details.copy()
                        combined_schema[path]['sample_values'] = [details.get('sample_value', '')]
                        combined_schema[path]['found_in_samples'] = 1

                    combined_schema[path]['found_in_samples'] = combined_schema[path].get('found_in_samples', 0) + 1
                    combined_schema[path]['frequency'] = combined_schema[path]['found_in_samples'] / len(json_samples)

            except Exception as e:
                logger.warning(f"Failed to analyze JSON sample {i}: {e}")
                continue

        progress_bar.progress(80)
        status_text.text("✅ Finalizing schema analysis with multi-level field detection...")

        for path, details in combined_schema.items():
            if 'sample_values' not in details:
                details['sample_values'] = [details.get('sample_value', '')]
            if 'found_in_samples' not in details:
                details['found_in_samples'] = 1
            if 'frequency' not in details:
                details['frequency'] = 1.0

        multi_level_info = generator.get_multi_level_field_info()
        metadata['multi_level_fields'] = len(multi_level_info)
        metadata['multi_level_info'] = multi_level_info
        metadata['unique_schemas'] = len(unique_structures)
        metadata['analysis_success'] = len(combined_schema) > 0

        progress_bar.progress(100)
        status_text.text("✅ Schema analysis complete with multi-level field detection!")

        import time
        time.sleep(1)
        progress_bar.empty()
        status_text.empty()

        if not combined_schema:
            return None, "❌ No valid JSON schema could be extracted from database samples", metadata

        if multi_level_info:
            st.success(f"🎯 Found {len(multi_level_info)} fields appearing at multiple levels - users can now specify simple field names!")
            
            with st.expander("🔍 Multi-Level Field Summary", expanded=False):
                for field_name, field_data in multi_level_info.items():
                    st.markdown(f"**Field: `{field_name}`** ({field_data['total_occurrences']} locations)")
                    for path_info in field_data['paths']:
                        st.markdown(f"- `{path_info['full_path']}` → alias: `{path_info['alias']}` ({path_info['context_description']})")
        else:
            st.info("ℹ️ All fields have unique names - no multi-level detection needed")

        return combined_schema, None, metadata

    except Exception as e:
        progress_bar.empty()
        status_text.empty()
        logger.error(f"Schema analysis failed: {e}")
        return None, f"❌ Schema analysis failed: {str(e)}", metadata


def generate_database_driven_sql(conn_manager, table_name: str, json_column: str, 
                               field_conditions: str) -> Tuple[Optional[str], Optional[str]]:
    """ENHANCED UNIVERSAL database-driven SQL generation with multi-level field support"""
    try:
        st.info("🔍 **Step 1:** Analyzing JSON structure with multi-level field detection...")

        schema, schema_error, metadata = analyze_database_json_schema_universal(
            conn_manager, table_name, json_column, sample_size=10
        )

        if schema is None:
            return None, f"Schema analysis failed: {schema_error}"

        st.success("✅ **Step 1 Complete:** Enhanced JSON Schema Analysis with Multi-Level Support")

        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("📊 Records Sampled", metadata['sample_size'])
        with col2:
            st.metric("🏷️ JSON Fields Found", len(schema))
        with col3:
            st.metric("🔄 Unique Structures", metadata['unique_schemas'])
        with col4:
            multi_indicator = f"✅ {metadata['multi_level_fields']}" if metadata['multi_level_fields'] > 0 else "➖ 0"
            st.metric("🎯 Multi-Level Fields", multi_indicator)
        with col5:
            success_indicator = "✅" if metadata['analysis_success'] else "❌"
            st.metric("📈 Analysis Status", success_indicator)

        st.info(f"📋 **Analyzed Table:** `{metadata['resolved_table_name']}`")

        st.info("🔨 **Step 2:** Generating SQL with Multi-Level Field Support...")

        from python_sql_generator import PythonSQLGenerator
        
        generator = PythonSQLGenerator()
        resolved_table_name = metadata['resolved_table_name']

        generated_sql, warnings = generator.generate_sql_with_warnings(
            resolved_table_name, json_column, field_conditions, schema
        )

        if warnings:
            st.markdown("#### 🔔 Multi-Level Field Resolution Alerts")
            for warning in warnings:
                if warning.startswith('✅'):
                    st.success(warning)
                elif warning.startswith('🎯'):
                    st.info(warning)
                elif warning.startswith('⚠️'):
                    st.warning(warning)
                else:
                    st.info(warning)

        if generated_sql.strip().startswith("-- Error"):
            return None, f"SQL generation failed: {generated_sql}"

        multi_level_info = generator.get_multi_level_field_info()
        if multi_level_info and warnings:
            with st.expander("🔍 Multi-Level Field Expansion Details", expanded=False):
                st.markdown("**How simple field names were expanded:**")
                conditions = [c.strip() for c in field_conditions.split(',')]
                for condition in conditions:
                    if condition:
                        field_name = condition.split('[')[0].strip()
                        simple_name = field_name.split('.')[-1]
                        
                        if simple_name in multi_level_info:
                            field_data = multi_level_info[simple_name]
                            st.markdown(f"**Input: `{field_name}`**")
                            st.markdown(f"**Expanded to {field_data['total_occurrences']} columns:**")
                            for path_info in field_data['paths']:
                                status = "✅ Included" if path_info['full_path'] in generated_sql else "⏸️ Available"
                                st.markdown(f"- {status} `{path_info['full_path']}` → `{path_info['alias']}`")

        st.success("✅ **Step 2 Complete:** Enhanced SQL Generated with Multi-Level Field Support!")
        
        return generated_sql, None

    except Exception as e:
        logger.error(f"Enhanced database-driven analysis failed: {e}")
        return None, f"❌ Enhanced database-driven analysis failed: {str(e)}"


def generate_sql_from_json_python_mode_enhanced(json_data: Any, table_name: str, json_column: str, field_conditions: str) -> Tuple[str, List[str]]:
    """ENHANCED: Python mode SQL generation with multi-level field support"""
    try:
        from python_sql_generator import PythonSQLGenerator
        
        generator = PythonSQLGenerator()
        schema = generator.analyze_json_for_sql(json_data)
        
        if not schema:
            return "-- Error: Could not analyze JSON structure", ["❌ Schema analysis failed"]
        
        generated_sql, warnings = generator.generate_sql_with_warnings(table_name, json_column, field_conditions, schema)
        
        if generated_sql.strip().startswith("-- Error"):
            return generated_sql, ["❌ SQL generation failed"]
            
        return generated_sql, warnings
        
    except Exception as e:
        logger.error(f"Enhanced Python mode SQL generation failed: {e}")
        return f"-- Error in enhanced Python mode: {str(e)}", [f"❌ Generation error: {str(e)}"]


def render_enhanced_database_json_preview(schema: Dict, metadata: Dict):
    """Enhanced preview of the discovered JSON schema with multi-level field support"""
    st.markdown("### 📊 Enhanced JSON Schema Analysis with Multi-Level Fields")

    multi_level_info = metadata.get('multi_level_info', {})
    if multi_level_info:
        st.markdown("#### 🎯 Multi-Level Field Summary")
        st.success(f"Found {len(multi_level_info)} field names appearing in multiple locations")
        
        for field_name, field_data in multi_level_info.items():
            with st.expander(f"🔍 Field: `{field_name}` ({field_data['total_occurrences']} locations)", expanded=False):
                st.markdown("**When you specify just `{}`, you'll get ALL these columns:**".format(field_name))
                
                for i, path_info in enumerate(field_data['paths']):
                    priority_icon = "🎯" if i == 0 else "✅"
                    st.markdown(f"- {priority_icon} `{path_info['full_path']}` → alias: `{path_info['alias']}`")
                    st.caption(f"Context: {path_info['context_description']}")

    tab1, tab2, tab3, tab4 = st.tabs(["🏷️ **Field Details**", "📊 **Frequency Chart**", "🔍 **Type Distribution**", "🎯 **Multi-Level Guide**"])

    with tab1:
        schema_summary = []

        for path, details in sorted(schema.items(), key=lambda x: x[1].get('frequency', 0), reverse=True):
            frequency = details.get('frequency', 0)
            field_type = details.get('snowflake_type', 'VARIANT')
            sample_values = details.get('sample_values', ['N/A'])
            found_in = details.get('found_in_samples', 0)
            context_desc = details.get('context_description', 'Unknown')

            readable_samples = []
            for val in sample_values[:3]:
                val_str = str(val)
                if len(val_str) > 50:
                    val_str = val_str[:47] + "..."
                readable_samples.append(val_str)

            path_status = "✅ Clean"
            field_name = path.split('.')[-1]
            if field_name in multi_level_info:
                level_count = multi_level_info[field_name]['total_occurrences']
                path_status = f"🎯 Multi-Level ({level_count})"

            schema_summary.append({
                'Field Path': path,
                'Field Name': field_name,
                'Status': path_status,
                'Context': context_desc,
                'Snowflake Type': field_type,
                'Frequency': f"{frequency:.1%}",
                'Found In Samples': f"{found_in}/{metadata['sample_size']}",
                'Sample Values': ' | '.join(readable_samples) if readable_samples else 'N/A',
                'Queryable': '✅' if details.get('is_queryable', False) else '❌'
            })

        import pandas as pd
        schema_df = pd.DataFrame(schema_summary)

        search_term = st.text_input("🔍 Search fields:", placeholder="Type to filter fields...")
        if search_term:
            schema_df = schema_df[schema_df['Field Path'].str.contains(search_term, case=False, na=False)]

        col_filter1, col_filter2 = st.columns(2)
        with col_filter1:
            show_multi_level_only = st.checkbox("Show only multi-level fields", value=False)
            if show_multi_level_only:
                schema_df = schema_df[schema_df['Status'].str.contains('Multi-Level')]
        with col_filter2:
            show_queryable_only = st.checkbox("Show only queryable fields", value=False)
            if show_queryable_only:
                schema_df = schema_df[schema_df['Queryable'] == '✅']

        st.dataframe(schema_df, use_container_width=True, height=400)

        csv_data = schema_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            "📥 Export Enhanced Schema Analysis",
            data=csv_data,
            file_name=f"multi_level_json_schema_analysis_{metadata['table_name']}_{metadata['json_column']}.csv",
            mime="text/csv"
        )

    with tab2:
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
        type_counts = {}
        for details in schema.values():
            field_type = details.get('snowflake_type', 'VARIANT')
            type_counts[field_type] = type_counts.get(field_type, 0) + 1

        if type_counts:
            type_df = pd.DataFrame(list(type_counts.items()), columns=['Data Type', 'Count'])
            st.bar_chart(type_df.set_index('Data Type'))
            
            st.markdown("**Type Distribution:**")
            type_df['Percentage'] = (type_df['Count'] / type_df['Count'].sum() * 100).round(1)
            type_df['Percentage'] = type_df['Percentage'].astype(str) + '%'
            st.dataframe(type_df, use_container_width=True)

    with tab4:
        st.markdown("#### 🎯 Multi-Level Field Usage Guide")
        
        if multi_level_info:
            st.markdown("**🎉 Great news! You can now use simple field names and get ALL occurrences:**")
            
            for field_name, field_data in multi_level_info.items():
                st.markdown(f"**Field: `{field_name}`**")
                
                col_guide1, col_guide2 = st.columns(2)
                
                with col_guide1:
                    st.markdown("**✅ Simple Usage (Recommended):**")
                    st.code(f"{field_name}[IS NOT NULL]", language="text")
                    st.caption(f"→ Gets ALL {field_data['total_occurrences']} occurrences automatically!")
                
                with col_guide2:
                    st.markdown("**📋 Expanded Columns:**")
                    for path_info in field_data['paths'][:3]:
                        st.code(f"{path_info['alias']}", language="text")
                        st.caption(f"← from {path_info['full_path']}")
                
                st.markdown("---")
                
            st.success("💡 **Tip:** Just type simple field names (like `name`) and the system will automatically find and include ALL occurrences with meaningful aliases!")
        else:
            st.info("ℹ️ **No multi-level fields detected.** All field names are unique, so you can use them directly without any ambiguity.")


def render_enhanced_field_suggestions(schema: Dict, multi_level_info: Dict = None) -> List[str]:
    """Generate intelligent field suggestions with multi-level awareness"""
    suggestions = []

    high_frequency_fields = []
    categorical_fields = []
    numeric_fields = []
    multi_level_fields = set()

    if multi_level_info:
        for field_name in multi_level_info.keys():
            multi_level_fields.add(field_name)

    for path, details in schema.items():
        frequency = details.get('frequency', 0)
        field_type = details.get('snowflake_type', 'VARIANT')
        sample_values = details.get('sample_values', [])
        is_queryable = details.get('is_queryable', False)
        field_name = path.split('.')[-1]

        if not is_queryable:
            continue

        suggestion_field = field_name if field_name in multi_level_fields else path

        if frequency > 0.7:
            high_frequency_fields.append((suggestion_field, details))

        if field_type in ['VARCHAR', 'STRING'] and sample_values:
            unique_vals = list(set([str(v) for v in sample_values]))
            if len(unique_vals) <= 3 and all(len(str(v)) < 20 for v in unique_vals):
                categorical_fields.append((suggestion_field, unique_vals))

        if field_type in ['NUMBER', 'INTEGER'] and sample_values:
            numeric_fields.append((suggestion_field, details))

    for field, details in high_frequency_fields[:3]:
        if field in multi_level_fields:
            suggestions.append(f"{field}[IS NOT NULL]  # Gets ALL levels automatically")
        else:
            suggestions.append(f"{field}[IS NOT NULL]")

    for field, unique_vals in categorical_fields[:3]:
        if len(unique_vals) > 1:
            if field in multi_level_fields:
                suggestions.append(f"{field}[IN:{' | '.join(unique_vals)}]  # Multi-level search")
            else:
                suggestions.append(f"{field}[IN:{' | '.join(unique_vals)}]")

    for field, details in numeric_fields[:2]:
        sample_values = details.get('sample_values', [])
        try:
            numeric_vals = [float(v) for v in sample_values if str(v).replace('.', '').replace('-', '').isdigit()]
            if numeric_vals:
                avg_val = sum(numeric_vals) / len(numeric_vals)
                if field in multi_level_fields:
                    suggestions.append(f"{field}[>:{avg_val:.0f}]  # Multi-level comparison")
                else:
                    suggestions.append(f"{field}[>:{avg_val:.0f}]")
        except:
            suggestions.append(f"{field}[IS NOT NULL]")

    if multi_level_info:
        example_field = list(multi_level_info.keys())[0]
        suggestions.append(f"# Multi-level example - just use simple name:")
        suggestions.append(f"{example_field}[IS NOT NULL]  # Automatically expands to all levels")

    return suggestions[:10]


def test_database_connectivity(conn_manager) -> Tuple[bool, str]:
    """Test database connectivity with enhanced diagnostics"""
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
- **Features:** Multi-Level Field Detection & Automatic Expansion
- **Status:** Connected and Ready for Smart Multi-Level Analysis
"""
            return True, status_msg
        else:
            return False, f"❌ Connection test failed: {error}"

    except Exception as e:
        return False, f"❌ Connection test error: {str(e)}"


def render_multi_level_helper_ui(field_conditions: str, multi_level_info: Dict):
    """Render UI helper for multi-level field usage"""
    if not multi_level_info:
        return
    
    st.markdown("#### 🎯 Multi-Level Field Assistant")
    
    conditions = [c.strip() for c in field_conditions.split(',') if c.strip()]
    
    expandable_fields = []
    for condition in conditions:
        field_name = condition.split('[')[0].strip()
        simple_field_name = field_name.split('.')[-1]
        
        if simple_field_name in multi_level_info and field_name == simple_field_name:
            expandable_fields.append((condition, simple_field_name))
    
    if expandable_fields:
        st.success("✅ Great! These fields will automatically expand to multiple levels:")
        
        for condition, simple_name in expandable_fields:
            field_data = multi_level_info[simple_name]
            st.markdown(f"**`{condition}` will become:**")
            
            cols = st.columns(min(4, len(field_data['paths'])))
            
            for i, path_info in enumerate(field_data['paths'][:4]):
                with cols[i % 4]:
                    st.code(path_info['alias'], language="text")
                    st.caption(f"from {path_info['full_path']}")
    
    available_multi_level = [name for name in multi_level_info.keys() 
                           if name not in [c.split('[')[0].strip() for c in conditions]]
    
    if available_multi_level:
        st.markdown("**💡 Other multi-level fields you can use:**")
        for field_name in available_multi_level[:5]:
            field_data = multi_level_info[field_name]
            if st.button(f"Add `{field_name}` ({field_data['total_occurrences']} levels)", key=f"add_multi_{field_name}"):
                current_conditions = st.session_state.get('unified_field_conditions', field_conditions)
                new_condition = f"{field_name}[IS NOT NULL]"
                if current_conditions.strip():
                    st.session_state.unified_field_conditions = f"{current_conditions}, {new_condition}"
                else:
                    st.session_state.unified_field_conditions = new_condition
                st.rerun()
            st.caption(f"Expands to: {', '.join([p['alias'] for p in field_data['paths'][:3]])}")


# Enhanced version aliases for backward compatibility
def generate_database_driven_sql_enhanced(conn_manager, table_name: str, json_column: str, 
                                        field_conditions: str) -> Tuple[Optional[str], Optional[str]]:
    """Enhanced version that's now the main implementation"""
    return generate_database_driven_sql(conn_manager, table_name, json_column, field_conditions)


def analyze_database_json_schema_enhanced(conn_manager, table_name: str, json_column: str, 
                                        sample_size: int = 10) -> Tuple[Optional[Dict], Optional[str], Dict]:
    """Enhanced version that's now the main implementation"""
    return analyze_database_json_schema_universal(conn_manager, table_name, json_column, sample_size)


def generate_sql_from_json_data_enhanced(json_data: Any, table_name: str, json_column: str, field_conditions: str) -> Tuple[str, List[str], Dict]:
    """Enhanced wrapper for multi-level field support in JSON mode"""
    try:
        from python_sql_generator import PythonSQLGenerator
        
        generator = PythonSQLGenerator()
        schema = generator.analyze_json_for_sql(json_data)
        
        if not schema:
            return "-- Error: Could not analyze JSON structure", ["❌ Schema analysis failed"], {}
        
        sql, warnings = generator.generate_sql_with_warnings(table_name, json_column, field_conditions, schema)
        multi_level_info = generator.get_multi_level_field_info()
        
        return sql, warnings, multi_level_info
        
    except Exception as e:
        logger.error(f"Enhanced JSON mode SQL generation failed: {e}")
        return f"-- Error: {str(e)}", [f"❌ Generation error: {str(e)}"], {}


def validate_json_structure(json_data: Any) -> Tuple[bool, str]:
    """Validate JSON structure for analysis"""
    try:
        if json_data is None:
            return False, "JSON data is None"
        
        if isinstance(json_data, str):
            try:
                json.loads(json_data)
                return True, "Valid JSON string"
            except json.JSONDecodeError as e:
                return False, f"Invalid JSON string: {str(e)}"
        
        if isinstance(json_data, (dict, list)):
            return True, "Valid JSON object/array"
        
        return False, f"Unsupported JSON type: {type(json_data)}"
        
    except Exception as e:
        return False, f"JSON validation error: {str(e)}"


def get_json_sample_preview(json_data: Any, max_depth: int = 3, max_items: int = 5) -> str:
    """Generate a preview of JSON structure for UI display"""
    try:
        def truncate_json(obj, depth=0):
            if depth > max_depth:
                return "..."
            
            if isinstance(obj, dict):
                if not obj:
                    return {}
                items = list(obj.items())[:max_items]
                result = {}
                for k, v in items:
                    result[k] = truncate_json(v, depth + 1)
                if len(obj) > max_items:
                    result["..."] = f"({len(obj) - max_items} more items)"
                return result
            
            elif isinstance(obj, list):
                if not obj:
                    return []
                items = obj[:max_items]
                result = [truncate_json(item, depth + 1) for item in items]
                if len(obj) > max_items:
                    result.append(f"...({len(obj) - max_items} more items)")
                return result
            
            else:
                str_val = str(obj)
                if len(str_val) > 50:
                    return str_val[:47] + "..."
                return obj
        
        preview = truncate_json(json_data)
        return json.dumps(preview, indent=2, ensure_ascii=False)
        
    except Exception as e:
        return f"Preview error: {str(e)}"


def extract_field_names_from_conditions(field_conditions: str) -> List[str]:
    """Extract field names from field conditions string"""
    try:
        field_names = []
        conditions = [c.strip() for c in field_conditions.split(',') if c.strip()]
        
        for condition in conditions:
            if '[' in condition:
                field_name = condition.split('[')[0].strip()
            else:
                field_name = condition.strip()
            
            if field_name:
                field_names.append(field_name)
        
        return field_names
        
    except Exception as e:
        logger.error(f"Failed to extract field names: {e}")
        return []


def format_sql_for_display(sql: str) -> str:
    """Format SQL for better display in UI"""
    try:
        # Basic SQL formatting
        formatted = sql.strip()
        
        # Add proper line breaks
        formatted = formatted.replace('SELECT', 'SELECT\n  ')
        formatted = formatted.replace(', ', ',\n  ')
        formatted = formatted.replace('FROM', '\nFROM\n  ')
        formatted = formatted.replace('LATERAL FLATTEN', '\n  LATERAL FLATTEN')
        formatted = formatted.replace('WHERE', '\nWHERE\n  ')
        formatted = formatted.replace(' AND ', '\n  AND ')
        formatted = formatted.replace(' OR ', '\n  OR ')
        
        return formatted
        
    except Exception as e:
        logger.error(f"SQL formatting failed: {e}")
        return sql


def get_database_session_info(conn_manager) -> Dict[str, str]:
    """Get detailed database session information"""
    try:
        if not conn_manager.is_connected:
            return {"status": "Not connected"}
        
        info_query = """
        SELECT 
            CURRENT_DATABASE() as database_name,
            CURRENT_SCHEMA() as schema_name,
            CURRENT_USER() as user_name,
            CURRENT_ROLE() as role_name,
            CURRENT_WAREHOUSE() as warehouse_name,
            CURRENT_SESSION() as session_id
        """
        
        result_df, error = conn_manager.execute_query(info_query)
        
        if result_df is not None and not result_df.empty:
            row = result_df.iloc[0]
            return {
                "status": "Connected",
                "database": str(row.get('DATABASE_NAME', 'Unknown')),
                "schema": str(row.get('SCHEMA_NAME', 'Unknown')),
                "user": str(row.get('USER_NAME', 'Unknown')),
                "role": str(row.get('ROLE_NAME', 'Unknown')),
                "warehouse": str(row.get('WAREHOUSE_NAME', 'Unknown')),
                "session_id": str(row.get('SESSION_ID', 'Unknown'))
            }
        else:
            return {"status": "Connected but info unavailable", "error": str(error)}
            
    except Exception as e:
        logger.error(f"Failed to get database session info: {e}")
        return {"status": "Error", "error": str(e)}


def sanitize_table_name(table_name: str) -> str:
    """Sanitize table name to prevent SQL injection"""
    try:
        # Remove potentially dangerous characters
        import re
        sanitized = re.sub(r'[^\w\.\-]', '', table_name)
        
        # Ensure it's not empty after sanitization
        if not sanitized:
            raise ValueError("Table name becomes empty after sanitization")
            
        return sanitized
        
    except Exception as e:
        logger.error(f"Table name sanitization failed: {e}")
        raise ValueError(f"Invalid table name: {table_name}")


def sanitize_column_name(column_name: str) -> str:
    """Sanitize column name to prevent SQL injection"""
    try:
        # Remove potentially dangerous characters
        import re
        sanitized = re.sub(r'[^\w\.\-]', '', column_name)
        
        # Ensure it's not empty after sanitization
        if not sanitized:
            raise ValueError("Column name becomes empty after sanitization")
            
        return sanitized
        
    except Exception as e:
        logger.error(f"Column name sanitization failed: {e}")
        raise ValueError(f"Invalid column name: {column_name}")


def check_database_permissions(conn_manager, table_name: str) -> Tuple[bool, str]:
    """Check if user has necessary permissions on the table"""
    try:
        if not conn_manager.is_connected:
            return False, "Not connected to database"
        
        # Try a simple SELECT to check permissions
        resolved_table_name = resolve_table_name_universal(conn_manager, table_name)
        test_query = f"SELECT 1 FROM {resolved_table_name} LIMIT 1"
        
        result_df, error = conn_manager.execute_query(test_query)
        
        if result_df is not None:
            return True, "Permissions verified"
        else:
            if "permission" in str(error).lower() or "access" in str(error).lower():
                return False, f"Permission denied: {error}"
            elif "does not exist" in str(error).lower():
                return False, f"Table does not exist: {error}"
            else:
                return False, f"Permission check failed: {error}"
                
    except Exception as e:
        logger.error(f"Permission check failed: {e}")
        return False, f"Permission check error: {str(e)}"


def get_table_column_info(conn_manager, table_name: str) -> Tuple[Optional[List[Dict]], Optional[str]]:
    """Get detailed column information for a table"""
    try:
        if not conn_manager.is_connected:
            return None, "Not connected to database"
        
        resolved_table_name = resolve_table_name_universal(conn_manager, table_name)
        parts = resolved_table_name.split('.')
        
        if len(parts) == 3:
            database, schema, table = parts
        else:
            return None, f"Invalid table name format: {resolved_table_name}"
        
        columns_query = f"""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            COMMENT
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_CATALOG = '{database}'
          AND TABLE_SCHEMA = '{schema}'
          AND TABLE_NAME = '{table}'
        ORDER BY ORDINAL_POSITION
        """
        
        result_df, error = conn_manager.execute_query(columns_query)
        
        if result_df is None:
            return None, f"Failed to get column info: {error}"
        
        if result_df.empty:
            return None, f"No column information found for table {resolved_table_name}"
        
        columns = []
        for _, row in result_df.iterrows():
            columns.append({
                'name': row.get('COLUMN_NAME', ''),
                'type': row.get('DATA_TYPE', ''),
                'nullable': row.get('IS_NULLABLE', 'YES') == 'YES',
                'default': row.get('COLUMN_DEFAULT', ''),
                'comment': row.get('COMMENT', '')
            })
        
        return columns, None
        
    except Exception as e:
        logger.error(f"Failed to get table column info: {e}")
        return None, f"Column info error: {str(e)}"


def estimate_query_cost(conn_manager, sql: str) -> Tuple[Optional[Dict], Optional[str]]:
    """Estimate the cost/complexity of executing a query"""
    try:
        if not conn_manager.is_connected:
            return None, "Not connected to database"
        
        # Use EXPLAIN to get query plan without executing
        explain_query = f"EXPLAIN {sql}"
        
        result_df, error = conn_manager.execute_query(explain_query)
        
        if result_df is None:
            return None, f"Failed to get query plan: {error}"
        
        # Parse the explain output for cost estimation
        plan_info = {
            'estimated_complexity': 'Medium',
            'operations': [],
            'warnings': []
        }
        
        if not result_df.empty:
            plan_text = str(result_df.iloc[0].iloc[0]) if len(result_df.iloc[0]) > 0 else ""
            
            # Simple heuristics for complexity estimation
            if 'LATERAL FLATTEN' in plan_text:
                plan_info['operations'].append('LATERAL FLATTEN detected')
                if plan_text.count('LATERAL FLATTEN') > 2:
                    plan_info['estimated_complexity'] = 'High'
                    plan_info['warnings'].append('Multiple LATERAL FLATTEN operations may be expensive')
            
            if 'FULL SCAN' in plan_text.upper():
                plan_info['operations'].append('Full table scan')
                plan_info['warnings'].append('Full table scan detected - consider adding filters')
            
            if len(plan_info['operations']) == 0:
                plan_info['estimated_complexity'] = 'Low'
        
        return plan_info, None
        
    except Exception as e:
        logger.error(f"Query cost estimation failed: {e}")
        return None, f"Cost estimation error: {str(e)}"


def render_query_execution_metrics(execution_time: float, row_count: int, warnings: List[str]):
    """Render query execution metrics in the UI"""
    try:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                "⏱️ Execution Time",
                f"{execution_time:.2f}s",
                delta=f"{'Fast' if execution_time < 5 else 'Slow' if execution_time > 30 else 'Normal'}"
            )
        
        with col2:
            st.metric(
                "📊 Rows Returned", 
                f"{row_count:,}",
                delta=f"{'Small' if row_count < 1000 else 'Large' if row_count > 10000 else 'Medium'}"
            )
        
        with col3:
            warning_count = len([w for w in warnings if w.startswith('⚠️')])
            st.metric(
                "⚠️ Warnings",
                warning_count,
                delta=f"{'Good' if warning_count == 0 else 'Review needed'}"
            )
        
        if warnings:
            with st.expander("📋 Execution Details", expanded=False):
                for warning in warnings:
                    if warning.startswith('⚠️'):
                        st.warning(warning)
                    elif warning.startswith('ℹ️'):
                        st.info(warning)
                    else:
                        st.text(warning)
    
    except Exception as e:
        logger.error(f"Failed to render execution metrics: {e}")
        st.error(f"Failed to display metrics: {str(e)}")
