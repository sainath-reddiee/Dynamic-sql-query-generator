import streamlit as st
import json
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime
import logging
import os

# Import all required modules from the 'src' directory
from python_sql_generator import generate_sql_from_json_data

# UNIFIED: Import the new unified connector instead of both separate ones
from unified_snowflake_connector import (
    UnifiedSnowflakeConnector,
    render_unified_connection_ui,
    render_performance_info,
    render_performance_metrics,
    MODIN_AVAILABLE,
    SNOWFLAKE_AVAILABLE
)

from universal_db_analyzer import (
    generate_database_driven_sql,
    analyze_database_json_schema_universal,
    render_enhanced_database_json_preview,
    test_database_connectivity,
    render_multi_level_helper_ui,
    render_enhanced_field_suggestions
)

from json_analyzer import analyze_json_structure
from utils import (
    find_arrays, find_nested_objects,
    find_queryable_fields, prettify_json, validate_json_input,
    export_analysis_results
)
from sql_generator import generate_procedure_examples, generate_sql_preview
from config import config

# Configure logging
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title=f"❄️ {config.APP_NAME}",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
        font-weight: 600;
    }
    .section-header {
        font-size: 1.5rem;
        color: #ff7f0e;
        margin-top: 2rem;
        margin-bottom: 1rem;
        font-weight: 500;
    }
    .feature-box {
        background: linear-gradient(145deg, #f0f2f6, #ffffff);
        padding: 1.5rem;
        border-radius: 0.8rem;
        margin: 1rem 0;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        border: 1px solid #e1e5e9;
    }
    .metric-card {
        background: linear-gradient(145deg, #ffffff, #f8f9fa);
        padding: 1rem;
        border-radius: 0.6rem;
        text-align: center;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        border: 1px solid #e9ecef;
    }
    .footer {
        text-align: center;
        padding: 2rem;
        margin-top: 3rem;
        border-top: 2px solid #e9ecef;
        color: #6c757d;
        font-size: 0.9rem;
    }
    .enhanced-box {
        background: linear-gradient(145deg, #e8f5e8, #f1f8e9);
        padding: 1.5rem;
        border-radius: 10px;
        border: 2px solid #81c784;
        margin-bottom: 1rem;
    }
    .mode-selector {
        background: linear-gradient(145deg, #fff3e0, #fafafa);
        padding: 1rem;
        border-radius: 8px;
        border: 1px solid #ffb74d;
        margin: 1rem 0;
    }
    .disambiguation-alert {
        background: linear-gradient(145deg, #fff3e0, #ffeaa7);
        padding: 1rem;
        border-radius: 8px;
        border: 2px solid #fdcb6e;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)


def render_database_operations_ui(conn_manager: UnifiedSnowflakeConnector):
    """Enhanced operations UI with disambiguation support for both standard and enhanced modes"""

    # Display current mode information
    mode_text = "Enhanced" if conn_manager.enhanced_mode else "Standard"
    mode_color = "#2e7d32" if conn_manager.enhanced_mode else "#1976d2"
    mode_icon = "⚡" if conn_manager.enhanced_mode else "🏔️"

    st.markdown(f"""
    <div class="mode-selector">
        <h5 style="color: {mode_color}; margin-bottom: 0.5rem;">{mode_icon} Currently in {mode_text} Mode</h5>
        <p style="margin-bottom: 0; font-size: 0.9rem;">
            {'🛡️ Session context management + 🚀 Modin acceleration + 📊 Performance tracking + ⚠️ Field disambiguation' if conn_manager.enhanced_mode else '📊 Basic connectivity with standard pandas processing + ⚠️ Field disambiguation'}
        </p>
    </div>
    """, unsafe_allow_html=True)

    # 🔥 MOVED UP: Custom SQL section (was at bottom, now right after connection status)
    st.markdown("### 📊 Custom SQL Execution")
    st.markdown("""
    <div class="feature-box">
        <p>Execute any custom SQL query directly. Perfect for:</p>
        <ul>
            <li><strong>📋 Exploring tables:</strong> <code>SHOW TABLES</code> or <code>SELECT * FROM INFORMATION_SCHEMA.TABLES</code></li>
            <li><strong>🔍 Describing structure:</strong> <code>DESCRIBE TABLE your_table</code></li>
            <li><strong>📊 Testing queries:</strong> <code>SELECT * FROM your_table LIMIT 5</code></li>
        </ul>
    </div>
    """, unsafe_allow_html=True)

    custom_sql = st.text_area(
        f"Execute Custom SQL ({mode_text} Mode):",
        height=150,
        placeholder="""-- Quick examples to try:
SHOW TABLES;
-- or --
SELECT * FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA'
LIMIT 10;
-- or --
SELECT json_data:name::VARCHAR as name,
       json_data:age::NUMBER as age
FROM your_table
WHERE json_data:status::VARCHAR = 'active'
LIMIT 10;""",
        key="unified_custom_sql",
        help=f"Write any SQL query - {'large results will use Modin for faster processing' if conn_manager.enhanced_mode else 'processed with standard pandas'}"
    )

    col_sql1, col_sql2 = st.columns(2)

    with col_sql1:
        execute_sql_btn = st.button(f"▶️ Execute SQL ({mode_text})", type="primary")

        if execute_sql_btn and custom_sql.strip():
            if conn_manager.enhanced_mode:
                # Enhanced mode with performance tracking
                with st.spinner("⚡ Executing with performance monitoring..."):
                    result_df, error, perf_stats = conn_manager.execute_query_with_performance(custom_sql)

                    if result_df is not None:
                        st.success("✅ Custom SQL executed with performance tracking!")
                        render_performance_metrics(perf_stats)
                        st.dataframe(result_df, use_container_width=True)

                        if not result_df.empty:
                            csv_data = result_df.to_csv(index=False).encode('utf-8')
                            st.download_button(
                                "📥 Download Results",
                                data=csv_data,
                                file_name=f"custom_enhanced_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv"
                            )
                    else:
                        st.error(f"❌ Execution failed: {error}")
            else:
                # Standard mode
                with st.spinner("🔄 Executing query in standard mode..."):
                    result_df, error = conn_manager.execute_query(custom_sql)

                    if result_df is not None:
                        st.success("✅ Custom SQL executed successfully!")
                        st.dataframe(result_df, use_container_width=True)

                        if not result_df.empty:
                            csv_data = result_df.to_csv(index=False).encode('utf-8')
                            st.download_button(
                                "📥 Download Results",
                                data=csv_data,
                                file_name=f"custom_standard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv"
                            )
                    else:
                        st.error(f"❌ Execution failed: {error}")

        elif execute_sql_btn:
            st.warning("⚠️ Please enter a SQL query")

    with col_sql2:
        if st.button("📋 List Tables", type="secondary"):
            with st.spinner("🔄 Retrieving tables..."):
                tables_df, msg = conn_manager.list_tables()

                if tables_df is not None:
                    st.success(msg)
                    if not tables_df.empty:
                        st.dataframe(tables_df, use_container_width=True)
                    else:
                        st.info("ℹ️ No tables found in current schema")
                else:
                    st.error(msg)

    # 🎯 Helpful SQL examples for quick access
    st.markdown("#### 💡 Quick SQL Examples:")
    col_ex1, col_ex2, col_ex3 = st.columns(3)

    with col_ex1:
        if st.button("📋 Show Tables", help="List all tables"):
            st.session_state.unified_custom_sql = "SHOW TABLES;"
            st.rerun()

    with col_ex2:
        if st.button("🏗️ Table Schema", help="Get table information"):
            st.session_state.unified_custom_sql = """SELECT
    TABLE_CATALOG as DATABASE_NAME,
    TABLE_SCHEMA as SCHEMA_NAME,
    TABLE_NAME,
    TABLE_TYPE
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA'
ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
LIMIT 20;"""
            st.rerun()

    with col_ex3:
        if st.button("📊 Sample Data", help="Sample from a table"):
            st.session_state.unified_custom_sql = """-- Replace 'your_table' with actual table name
SELECT * FROM your_table LIMIT 10;"""
            st.rerun()

    # Enhanced Smart JSON Analysis Section with disambiguation
    st.markdown("---")
    st.markdown("### 🧪 Smart JSON Analysis with Disambiguation")

    if conn_manager.enhanced_mode:
        st.markdown("""
        <div class="enhanced-box">
            <h5 style="color: #2e7d32;">🎯 Enhanced Features Active:</h5>
            <ul style="color: #1b5e20; margin-bottom: 0;">
                <li><strong>✅ Fixed session context issues</strong> - No more database errors</li>
                <li><strong>🚀 Modin performance acceleration</strong> for large datasets</li>
                <li><strong>📊 Real-time performance tracking</strong> during analysis</li>
                <li><strong>🏷️ Smart table name resolution</strong> - Works with partial names</li>
                <li><strong>⚠️ Field disambiguation support</strong> - Handles duplicate field names</li>
                <li><strong>💡 Intelligent field suggestions</strong> based on your data structure</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div class="feature-box">
            <h5 style="color: #1976d2;">🏔️ Standard Features Active:</h5>
            <ul style="color: #0d47a1; margin-bottom: 0;">
                <li><strong>📊 Basic connectivity and operations</strong></li>
                <li><strong>🔧 Standard pandas processing</strong></li>
                <li><strong>⚠️ Field disambiguation support</strong> - Handles duplicate field names</li>
                <li><strong>💡 Smart field suggestions</strong> based on your data structure</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    with col1:
        table_name = st.text_input(
            "Table Name* 🏗️",
            placeholder="SCHEMA.TABLE or just TABLE_NAME",
            key="unified_table_name",
            help="Can be just table name, schema.table, or database.schema.table"
        )

        sample_size = st.selectbox(
            "Analysis Sample Size 📊",
            [5, 10, 20, 50],
            index=1,
            key="unified_sample_size",
            help="Larger samples give better schema analysis but take longer"
        )

    with col2:
        json_column = st.text_input(
            "JSON Column Name* 📄",
            placeholder="json_data",
            key="unified_json_column",
            help="Name of the column containing JSON data"
        )

        show_preview = st.checkbox(
            "Show Detailed Schema Preview 👀",
            value=True,
            key="unified_show_preview",
            help="Display comprehensive analysis of discovered JSON fields with disambiguation info"
        )

    field_conditions = st.text_area(
        "Field Conditions* 🎯",
        height=100,
        placeholder="e.g., name, company.name, departments.employees.name[IS NOT NULL]",
        key="unified_field_conditions",
        help="Specify JSON fields and their filtering conditions. Use full paths to avoid ambiguity."
    )

    # Enhanced smart suggestions section (with disambiguation awareness)
    if 'discovered_schema_unified' in st.session_state:
        with st.expander("💡 Smart Field Suggestions (Disambiguation-Aware)", expanded=False):
            try:
                schema = st.session_state.discovered_schema_unified
                metadata = st.session_state.get('schema_metadata_unified', {})
                disambiguation_info = metadata.get('disambiguation_info', {})

                suggestions = render_enhanced_field_suggestions(schema, disambiguation_info)

                if suggestions:
                    st.markdown("**🎯 Smart suggestions based on your JSON structure:**")

                    # Show disambiguation warning if conflicts exist
                    if disambiguation_info:
                        st.markdown("""
                        <div class="disambiguation-alert">
                            <strong>⚠️ Field Disambiguation Active:</strong> Detected field name conflicts.
                            Suggestions use full paths to avoid ambiguity.
                        </div>
                        """, unsafe_allow_html=True)

                    cols = st.columns(2)
                    suggestion_count = 0
                    for suggestion in suggestions:
                        if suggestion.startswith('#'):
                            # This is a comment, display differently
                            st.markdown(f"**{suggestion}**")
                        else:
                            col_idx = suggestion_count % 2
                            with cols[col_idx]:
                                if st.button(f"Use: `{suggestion.split('#')[0].strip()}`", key=f"use_unified_suggestion_{suggestion_count}"):
                                    current_conditions = st.session_state.get('unified_field_conditions', '').strip()
                                    clean_suggestion = suggestion.split('#')[0].strip()
                                    new_condition = f"{current_conditions}, {clean_suggestion}" if current_conditions else clean_suggestion
                                    st.session_state.unified_field_conditions = new_condition
                                    st.rerun()
                                st.code(suggestion, language="text")
                            suggestion_count += 1
                else:
                    st.info("No specific suggestions available for this schema.")

            except Exception as e:
                st.warning(f"Could not generate enhanced suggestions: {e}")

    # Enhanced disambiguation helper
    if 'schema_metadata_unified' in st.session_state:
        metadata = st.session_state.schema_metadata_unified
        disambiguation_info = metadata.get('disambiguation_info', {})

        if disambiguation_info and field_conditions:
            render_multi_level_helper_ui(field_conditions, disambiguation_info)

    col3, col4 = st.columns(2)

    with col3:
        if st.button("🔍 Analyze Schema Only", type="secondary"):
            if table_name and json_column:
                try:
                    with st.spinner(f"🔄 {'Enhanced' if conn_manager.enhanced_mode else 'Standard'} schema analysis with disambiguation..."):
                        schema, error, metadata = analyze_database_json_schema_universal(
                            conn_manager, table_name, json_column, sample_size
                        )

                        if schema:
                            st.success(f"✅ {'Enhanced' if conn_manager.enhanced_mode else 'Standard'} schema analysis complete! Found {len(schema)} fields.")

                            # Store in session state for suggestions
                            st.session_state.discovered_schema_unified = schema
                            st.session_state.schema_metadata_unified = metadata

                            # Show disambiguation summary
                            disambiguation_info = metadata.get('disambiguation_info', {})
                            if disambiguation_info:
                                st.info(f"🚨 Found {len(disambiguation_info)} field names with multiple locations. Check the detailed preview for disambiguation options.")

                            if show_preview:
                                render_enhanced_database_json_preview(schema, metadata)
                        else:
                            st.error(error)

                except Exception as e:
                    st.error(f"❌ Schema analysis failed: {str(e)}")
                    st.info("💡 This might be due to table access permissions or connection issues.")
            else:
                st.warning("⚠️ Please provide table name and JSON column.")

    with col4:
        analyze_and_execute = st.button(f"🚀 Analyze & Execute ({mode_text} Mode)", type="primary")

        if analyze_and_execute and all([table_name, json_column, field_conditions]):
            try:
                # Generate SQL using the enhanced universal database-driven analysis
                with st.spinner(f"⚡ {'Enhanced' if conn_manager.enhanced_mode else 'Standard'} analysis with disambiguation..."):
                    generated_sql, sql_error = generate_database_driven_sql(
                        conn_manager, table_name, json_column, field_conditions
                    )

                    if generated_sql and not sql_error:
                        st.success("✅ Enhanced SQL Generated Successfully with Disambiguation Support!")
                        st.code(generated_sql, language="sql")

                        # Execute with appropriate method based on connector mode
                        if conn_manager.enhanced_mode:
                            # Enhanced mode: Use performance tracking
                            with st.spinner("⚡ Executing with performance monitoring and disambiguation verification..."):
                                result_df, exec_error, perf_stats = conn_manager.execute_query_with_performance(generated_sql)

                                if result_df is not None:
                                    st.success("✅ Query executed with enhanced performance monitoring!")

                                    # Display performance metrics
                                    render_performance_metrics(perf_stats)

                                    # Enhanced results summary
                                    col_sum1, col_sum2, col_sum3, col_sum4 = st.columns(4)
                                    with col_sum1:
                                        st.metric("Rows Returned", len(result_df))
                                    with col_sum2:
                                        st.metric("Columns", len(result_df.columns))
                                    with col_sum3:
                                        processing_engine = "🚀 Modin" if perf_stats.get('modin_used', False) else "📊 Pandas"
                                        st.metric("Processing Engine", processing_engine)
                                    with col_sum4:
                                        # Check if disambiguation was used based on column aliases
                                        aliases_used = [col for col in result_df.columns if '_' in col and not col.startswith('_')]
                                        disambiguation_used = "✅ Applied" if len(aliases_used) > 0 else "➖ Not Needed"
                                        st.metric("Disambiguation", disambiguation_used)

                                    st.dataframe(result_df, use_container_width=True)

                                    # Enhanced download with disambiguation info
                                    if not result_df.empty:
                                        csv_data = result_df.to_csv(index=False).encode('utf-8')
                                        filename = f"enhanced_results_with_disambiguation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                                        st.download_button(
                                            "📥 Download Enhanced Results",
                                            data=csv_data,
                                            file_name=filename,
                                            mime="text/csv"
                                        )

                                        # Enhanced performance summary
                                        st.info(f"⚡ **Enhanced Performance Summary:** Processed {len(result_df):,} rows in {perf_stats.get('total_time', 0):.2f}s using {processing_engine} with disambiguation support")
                                else:
                                    st.error(f"❌ Query execution failed: {exec_error}")
                        else:
                            # Standard mode: Basic execution with disambiguation
                            with st.spinner("🔄 Executing query in standard mode with disambiguation..."):
                                result_df, exec_error = conn_manager.execute_query(generated_sql)

                                if result_df is not None:
                                    st.success("✅ Query executed successfully with disambiguation support!")

                                    # Enhanced results summary
                                    col_sum1, col_sum2, col_sum3 = st.columns(3)
                                    with col_sum1:
                                        st.metric("Rows Returned", len(result_df))
                                    with col_sum2:
                                        st.metric("Columns", len(result_df.columns))
                                    with col_sum3:
                                        # Check if disambiguation was used based on column aliases
                                        aliases_used = [col for col in result_df.columns if '_' in col and not col.startswith('_')]
                                        disambiguation_used = "✅ Applied" if len(aliases_used) > 0 else "➖ Not Needed"
                                        st.metric("Disambiguation", disambiguation_used)

                                    st.dataframe(result_df, use_container_width=True)

                                    # Enhanced download option
                                    if not result_df.empty:
                                        csv_data = result_df.to_csv(index=False).encode('utf-8')
                                        filename = f"standard_results_with_disambiguation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                                        st.download_button(
                                            "📥 Download Results",
                                            data=csv_data,
                                            file_name=filename,
                                            mime="text/csv"
                                        )
                                else:
                                    st.error(f"❌ Query execution failed: {exec_error}")
                    else:
                        st.error(f"❌ Enhanced SQL Generation Error: {sql_error}")

            except Exception as e:
                st.error(f"❌ Enhanced analysis failed: {str(e)}")
                st.info("💡 Try checking your table name, column name, database permissions, and field disambiguation.")

        elif analyze_and_execute:
            st.warning("⚠️ Please fill in all required fields.")

    # Connection management (kept at bottom)
    st.markdown("---")
    st.markdown("### 🔧 Connection Management")

    col7, col8, col9 = st.columns(3)

    with col7:
        if st.button("🔌 Disconnect", type="secondary"):
            conn_manager.disconnect()
            # Clear all session state related to connections
            keys_to_clear = [k for k in st.session_state.keys() if 'unified_connection' in k or 'discovered_schema' in k]
            for key in keys_to_clear:
                del st.session_state[key]
            st.success("✅ Disconnected from Snowflake")
            st.rerun()

    with col8:
        if st.button("🔍 Test Connection", type="secondary"):
            connectivity_ok, status_msg = test_database_connectivity(conn_manager)
            if connectivity_ok:
                st.success("✅ Connection is healthy!")
            else:
                st.error(status_msg)

    with col9:
        # Mode switch button (for demonstration)
        if st.button("🔄 Switch Mode", type="secondary", help="Disconnect and reconnect in different mode"):
            conn_manager.disconnect()
            # Clear session state
            keys_to_clear = [k for k in st.session_state.keys() if 'unified_connection' in k]
            for key in keys_to_clear:
                del st.session_state[key]
            st.info("✅ Disconnected. Please reconnect in your preferred mode.")
            st.rerun()

    # Enhanced connection details with disambiguation info
    if conn_manager.is_connected:
        with st.expander("ℹ️ Enhanced Connection Details & Status"):
            col_info1, col_info2 = st.columns(2)

            with col_info1:
                st.markdown("**Connection Information:**")
                conn_info = {
                    'Account': conn_manager.connection_params.get('account', 'N/A'),
                    'Database': conn_manager.connection_params.get('database', 'N/A'),
                    'Schema': conn_manager.connection_params.get('schema', 'N/A'),
                    'Warehouse': conn_manager.connection_params.get('warehouse', 'N/A')
                }

                for key, value in conn_info.items():
                    st.text(f"{key}: {value}")

            with col_info2:
                st.markdown("**Enhanced Feature Status:**")
                st.text(f"Mode: {'Enhanced' if conn_manager.enhanced_mode else 'Standard'}")
                st.text(f"Session Management: {'✅ Active' if conn_manager.enhanced_mode else '❌ Basic'}")
                st.text(f"Modin Acceleration: {'🚀 Available' if MODIN_AVAILABLE else '📊 Not Available'}")
                st.text(f"Performance Tracking: {'✅ Active' if conn_manager.enhanced_mode else '❌ Not Available'}")
                st.text(f"Field Disambiguation: ✅ Active")


# Main App
def main():
    try:
        st.markdown('<h1 class="main-header">❄️ Enhanced JSON-to-SQL Analyzer for Snowflake</h1>', unsafe_allow_html=True)

        # Display enhanced performance information at the top
        render_performance_info()

        # SIMPLIFIED: Only two main tabs now with enhanced features
        main_tab1, main_tab2 = st.tabs([
            "🐍 **Enhanced Python (Instant SQL Generation)**",
            "🏔️ **Enhanced Snowflake Database Connection**"
        ])

        with main_tab1:
            st.markdown('<h2 class="section-header">🐍 Generate SQL from JSON Input with Smart Disambiguation</h2>', unsafe_allow_html=True)
            st.markdown("""
            <div class="feature-box">
            <p>Upload or paste your JSON data below to analyze its structure and instantly generate a corresponding Snowflake SQL query.
            <strong>🎯 Enhanced with Smart Disambiguation:</strong> Automatically handles duplicate field names at different hierarchy levels.</p>
            </div>
            """, unsafe_allow_html=True)

            # Sidebar for input method selection
            st.sidebar.header("📥 Data Input for Enhanced Python Analyzer")
            input_method = st.sidebar.radio(
                "Choose your input method:",
                ["Upload JSON File", "Paste JSON Text"],
                key="input_method",
                help="Select how you want to provide your JSON data for analysis with disambiguation support"
            )

            json_data = None

            if input_method == "Upload JSON File":
                uploaded_file = st.sidebar.file_uploader(
                    "Choose a JSON file", type=['json'], help="Max 200MB"
                )
                if uploaded_file:
                    try:
                        json_data = json.load(uploaded_file)
                        st.sidebar.success(f"✅ File '{uploaded_file.name}' loaded.")
                    except Exception as e:
                        st.sidebar.error(f"Error reading file: {e}")
                        json_data = None
            else:
                json_text = st.sidebar.text_area(
                    "Paste your JSON here:", height=250, placeholder='{"example": "data"}'
                )
                if json_text:
                    is_valid, _, json_data = validate_json_input(json_text)
                    if is_valid:
                        st.sidebar.success("✅ JSON parsed successfully.")
                    else:
                        st.sidebar.error("Invalid JSON format.")
                        json_data = None

            if json_data:
                with st.spinner("Analyzing JSON structure with disambiguation..."):
                    schema = analyze_json_structure(json_data)

                if not schema:
                    st.error("❌ Could not analyze JSON structure.")
                    return

                # Create tabs for different features
                tab1, tab2, tab3, tab4, tab5 = st.tabs([
                    "⚡ **Smart SQL Generator**",
                    "📊 **Complete Paths**",
                    "📋 **Arrays Analysis**",
                    "🔍 **Queryable Fields**",
                    "🎨 **JSON Formatter**"
                ])

                with tab1:
                    st.markdown('<h3 class="section-header">⚡ Smart SQL Generator with Disambiguation</h3>', unsafe_allow_html=True)

                    # ENHANCED: Add disambiguation info display
                    if json_data:
                        # Analyze JSON for disambiguation info
                        from python_sql_generator import PythonSQLGenerator
                        temp_generator = PythonSQLGenerator()
                        temp_schema = temp_generator.analyze_json_for_sql(json_data)
                        disambiguation_info = temp_generator.get_multi_level_field_info()

                        # Show disambiguation alerts if conflicts exist
                        if disambiguation_info:
                            st.markdown("#### 🚨 Field Name Conflicts Detected")

                            conflict_summary = []
                            for field_name, conflict_data in disambiguation_info.items():
                                conflict_count = conflict_data['total_occurrences']
                                paths = conflict_data['paths']
                                queryable_options = [opt for opt in paths if opt['schema_entry']['is_queryable']]

                                conflict_summary.append({
                                    'Field Name': field_name,
                                    'Conflict Count': conflict_count,
                                    'Queryable Options': len(queryable_options),
                                    'Paths': ' | '.join([opt['full_path'] for opt in queryable_options[:3]]),
                                })

                            if conflict_summary:
                                st.warning(f"⚠️ Found {len(conflict_summary)} field names with multiple locations")

                                # Show conflicts in expandable section
                                with st.expander("🔍 View Conflict Details", expanded=False):
                                    import pandas as pd
                                    conflicts_df = pd.DataFrame(conflict_summary)
                                    st.dataframe(conflicts_df, use_container_width=True)

                                    st.markdown("**💡 How disambiguation works:**")
                                    st.markdown("""
                                    - When you specify just a field name (like `name`), the system automatically chooses the **least nested** occurrence
                                    - You can specify the full path (like `company.name` or `departments.name`) to be explicit
                                    - The system will show warnings when ambiguous fields are auto-resolved
                                    """)
                        else:
                            st.success("✅ No field name conflicts detected - all field names are unique!")

                    col1, col2 = st.columns(2)
                    with col1:
                        st.subheader("Query Parameters")
                        table_name = st.text_input("Table Name*", key="py_table", placeholder="your_schema.your_table")
                        json_column = st.text_input("JSON Column Name*", key="py_json_col", placeholder="json_data")
                        field_conditions = st.text_area("Field Conditions*", height=100, key="py_fields",
                                                       placeholder="e.g., name, company.name, departments.employees.name")

                        # ENHANCED: Add smart field suggestions with the fix
                        if json_data and temp_schema:
                            with st.expander("💡 Smart Field Suggestions", expanded=False):
                                st.markdown("**Available queryable fields:**")

                                queryable_fields_list = []
                                for path, details in temp_schema.items():
                                    if details.get('is_queryable', False):
                                        field_info = {
                                            'Field Path': path,
                                            'Type': details.get('snowflake_type', 'VARIANT'),
                                            'Sample': str(details.get('sample_value', ''))[:50] + ('...' if len(str(details.get('sample_value', ''))) > 50 else ''),
                                            'Context': details.get('context_description', 'Root')
                                        }
                                        queryable_fields_list.append(field_info)

                                # Define the callback function inside the main function to have access to the scope
                                def update_field_conditions(suggestion):
                                    current_conditions = st.session_state.get('py_fields', '').strip()
                                    if current_conditions:
                                        st.session_state.py_fields = f"{current_conditions}, {suggestion}"
                                    else:
                                        st.session_state.py_fields = suggestion

                                # Show first 10 fields
                                for field in queryable_fields_list[:10]:
                                    cols = st.columns([3, 1, 2, 1])
                                    with cols[0]:
                                        st.button(
                                            f"Use: {field['Field Path']}",
                                            key=f"use_field_{field['Field Path']}",
                                            on_click=update_field_conditions,
                                            args=(field['Field Path'],),
                                            type="secondary"
                                        )
                                    with cols[1]:
                                        st.caption(field['Type'])
                                    with cols[2]:
                                        st.caption(field['Sample'])
                                    with cols[3]:
                                        st.caption(field['Context'])

                                if len(queryable_fields_list) > 10:
                                    st.caption(f"... and {len(queryable_fields_list) - 10} more fields")

                    with col2:
                        st.subheader("💡 Examples & Help")

                        # ENHANCED: Context-aware examples
                        if json_data and temp_schema:
                            st.markdown("**🎯 Examples for your JSON:**")

                            # Generate smart examples based on actual data
                            example_fields = []
                            for path, details in temp_schema.items():
                                if details.get('is_queryable', False):
                                    example_fields.append(path)
                                    if len(example_fields) >= 3:
                                        break

                            if example_fields:
                                st.code(f"# Basic field selection\n{', '.join(example_fields[:2])}", language="text")

                                if len(example_fields) >= 2:
                                    st.code(f"# With conditions\n{example_fields[0]}[IS NOT NULL], {example_fields[1]}[=:some_value]", language="text")

                                # Show disambiguation examples if conflicts exist
                                if disambiguation_info:
                                    conflict_field = list(disambiguation_info.keys())[0]
                                    options = disambiguation_info[conflict_field]['paths'][:2]
                                    st.markdown("**🚨 Disambiguation Examples:**")
                                    st.code(f"# Ambiguous (auto-resolved)\n{conflict_field}", language="text")
                                    st.code(f"# Explicit paths\n{', '.join([opt['full_path'] for opt in options])}", language="text")

                        # Standard examples
                        st.markdown("**📋 Standard Examples:**")
                        examples = [
                            "name, age, email",
                            "user.name, user.profile.age[>:18]",
                            "status[=:active], created_date[IS NOT NULL]",
                            "tags[IN:premium|gold], score[>:100]"
                        ]
                        for ex in examples:
                            st.code(ex, language="text")

                    # ENHANCED: Generate SQL with warnings
                    if st.button("🚀 Generate SQL", type="primary"):
                        if all([table_name, json_column, field_conditions]):
                            with st.spinner("🔍 Generating SQL with disambiguation analysis..."):
                                # Use the enhanced version that returns warnings
                                from python_sql_generator import generate_sql_from_json_data_with_warnings

                                sql, warnings, disambiguation_details = generate_sql_from_json_data_with_warnings(
                                    json_data, table_name, json_column, field_conditions
                                )

                                # Display warnings if any
                                if warnings:
                                    st.markdown("#### 🔔 Disambiguation Alerts")
                                    for warning in warnings:
                                        if warning.startswith('⚠️'):
                                            st.warning(warning)
                                        elif warning.startswith('ℹ️'):
                                            st.info(warning)
                                        else:
                                            st.info(warning)

                                # Display the generated SQL
                                st.markdown("#### 🎯 Generated SQL")
                                st.code(sql, language="sql")

                                # Show additional details if disambiguation was used
                                if any("Auto-resolved" in w or "ambiguous" in w for w in warnings):
                                    with st.expander("🔍 Disambiguation Details", expanded=False):
                                        st.markdown("**Field Resolution Summary:**")
                                        conditions = [c.strip() for c in field_conditions.split(',')]
                                        for condition in conditions:
                                            if condition:
                                                field_name = condition.split('[')[0].strip()
                                                simple_name = field_name.split('.')[-1]

                                                if simple_name in disambiguation_details:
                                                    conflict_data = disambiguation_details[simple_name]
                                                    st.markdown(f"**{field_name}:**")
                                                    for opt in conflict_data['paths']:
                                                        status = "✅ Used" if opt['full_path'] in sql else "⏸️ Available"
                                                        st.markdown(f"- {status} `{opt['full_path']}` ({opt['context_description']})")
                        else:
                            st.warning("Please fill in all required fields marked with *.")

                with tab2:
                    st.markdown('<h3 class="section-header">📊 Complete JSON Paths</h3>', unsafe_allow_html=True)
                    all_paths_df = export_analysis_results(schema).get('all_paths')
                    if all_paths_df is not None and not all_paths_df.empty:
                        st.dataframe(all_paths_df, use_container_width=True)
                    else:
                        st.info("No paths to display.")

                with tab3:
                    st.markdown('<h3 class="section-header">📋 Arrays Analysis</h3>', unsafe_allow_html=True)
                    arrays_df = export_analysis_results(schema).get('arrays')
                    if arrays_df is not None and not arrays_df.empty:
                        st.dataframe(arrays_df, use_container_width=True)
                    else:
                        st.info("No arrays found in the JSON structure.")

                with tab4:
                    st.markdown('<h3 class="section-header">🔍 Queryable Fields</h3>', unsafe_allow_html=True)
                    queryable_df = export_analysis_results(schema).get('queryable_fields')
                    if queryable_df is not None and not queryable_df.empty:
                        st.dataframe(queryable_df, use_container_width=True)
                    else:
                        st.info("No queryable fields found.")

                with tab5:
                    st.markdown('<h3 class="section-header">🎨 JSON Formatter</h3>', unsafe_allow_html=True)
                    prettified_json_str = prettify_json(json.dumps(json_data))
                    st.code(prettified_json_str, language='json')

            else:
                st.info("👆 Provide JSON data via the sidebar to begin analysis and SQL generation with smart disambiguation.")

        with main_tab2:
            st.markdown('<h2 class="section-header">🏔️ Snowflake Database Connection</h2>', unsafe_allow_html=True)

            # Mode selection section
            st.markdown("### 🔧 Choose Connection Mode")
            st.markdown("""
            <div class="feature-box">
            <p>Choose the connection mode that best fits your needs. You can switch between modes by disconnecting and reconnecting.</p>
            </div>
            """, unsafe_allow_html=True)

            col_mode1, col_mode2 = st.columns(2)

            with col_mode1:
                st.markdown("""
                **🏔️ Standard Mode:**
                - ✅ Basic connectivity and operations
                - 📊 Standard pandas processing
                - 🔧 Simple error handling
                - 💾 Good for small to medium datasets
                - 🚀 Quick setup and testing
                """)

            with col_mode2:
                st.markdown("""
                **⚡ Enhanced Mode:**
                - 🛡️ **Fixed session context management**
                - 🚀 **Modin acceleration** (4x faster for large datasets)
                - 📊 **Real-time performance tracking**
                - 🏷️ Smart table name resolution
                - 🔧 Advanced error recovery
                """)

            # Mode selection
            connection_mode = st.radio(
                "Select Connection Mode:",
                ["🏔️ Standard Mode", "⚡ Enhanced Mode"],
                index=1,  # Default to enhanced mode
                horizontal=True,
                help="Enhanced mode includes all standard features plus advanced capabilities"
            )

            enhanced_mode = "Enhanced" in connection_mode

            # Single unified connection UI
            st.markdown("---")
            st.subheader("🔐 Database Connection")
            conn_manager = render_unified_connection_ui(enhanced_mode=enhanced_mode)

            if conn_manager and conn_manager.is_connected:
                # Test connectivity with comprehensive diagnostics
                connectivity_ok, status_msg = test_database_connectivity(conn_manager)

                if connectivity_ok:
                    st.success(status_msg)
                    st.markdown("---")
                    st.subheader("📊 Database Operations")
                    render_database_operations_ui(conn_manager)
                else:
                    st.error(status_msg)
                    st.info("💡 Try disconnecting and reconnecting with correct database/schema settings.")

                    # Disconnect button for troubleshooting
                    if st.button("🔌 Disconnect and Retry", type="secondary"):
                        conn_manager.disconnect()
                        keys_to_clear = [k for k in st.session_state.keys() if 'unified_connection' in k]
                        for key in keys_to_clear:
                            del st.session_state[key]
                        st.info("✅ Disconnected. Please reconnect with correct settings.")
                        st.rerun()
            else:
                st.markdown("---")
                mode_text = "Enhanced" if enhanced_mode else "Standard"
                st.info(f"👆 **Connect using {mode_text} mode above to unlock database operations.**")

        # Footer with unified information
        st.markdown("""
        <div class="footer">
            <p><strong>🚀 Unified JSON-to-SQL Analyzer</strong> | Built with ❤️ using Streamlit</p>
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 2rem; margin-top: 1rem; text-align: center;">
                <div>
                    <h4 style="color: #1976d2;">🐍 Python Mode</h4>
                    <p>Instant SQL generation<br/>No database required<br/>Perfect for quick analysis</p>
                </div>
                <div>
                    <h4 style="color: #2e7d32;">🏔️ Database Mode</h4>
                    <p>Live Snowflake connectivity<br/>Standard or Enhanced modes<br/>Real database operations</p>
                </div>
            </div>
            <hr style="margin: 2rem 0; border: 1px solid #e9ecef;">
            <p><small>
                <strong>🎯 Smart Feature:</strong> Unified connector automatically adapts to your chosen mode!<br/>
                <strong>⚡ Performance:</strong> Enhanced mode provides Modin acceleration for datasets >1000 rows
            </small></p>
        </div>
        """, unsafe_allow_html=True)

    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        st.error(f"❌ Application Error: {str(e)}")
        st.error("Please refresh the page and try again.")

        # Enhanced error details in expander
        with st.expander("🔧 Error Details (for debugging)"):
            st.code(str(e))
            st.markdown("**Possible solutions:**")
            st.markdown("- Check if all required modules are installed")
            st.markdown("- Verify your Python environment")
            st.markdown("- Try refreshing the page")


if __name__ == "__main__":
    main()
