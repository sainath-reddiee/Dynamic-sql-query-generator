import streamlit as st
import json
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime
import logging
import os

# Import all required modules from the 'src' directory
from python_sql_generator import generate_sql_from_json_data

# Import both regular and enhanced connectors
from snowflake_connector import render_snowflake_connection_ui, render_snowflake_operations_ui

# Import the NEW enhanced connector with Modin support
from enhanced_snowflake_connector import (
    EnhancedSnowflakeConnectionManager, 
    render_enhanced_snowflake_connection_ui,
    render_enhanced_performance_info,
    render_performance_metrics,
    MODIN_AVAILABLE,
    SNOWFLAKE_AVAILABLE
)

from universal_db_analyzer import (
    generate_database_driven_sql,
    generate_database_driven_sql_enhanced,
    analyze_database_json_schema_enhanced,
    render_enhanced_database_json_preview,
    test_database_connectivity
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
</style>
""", unsafe_allow_html=True)


def render_enhanced_database_operations_ui(conn_manager):
    """Render enhanced database operations UI with Modin performance tracking"""
    
    # Enhanced JSON Analysis Section
    st.markdown("### 🧪 Smart JSON Analysis (Enhanced with Modin)")
    st.markdown("""
    <div class="enhanced-box">
        <h5 style="color: #2e7d32;">🎯 Enhanced Features:</h5>
        <ul style="color: #1b5e20;">
            <li><strong>✅ Fixed session context issues</strong> - No more database errors</li>
            <li><strong>🚀 Modin performance acceleration</strong> for large datasets</li>
            <li><strong>📊 Real-time progress tracking</strong> during analysis</li>
            <li><strong>🏷️ Smart table name resolution</strong> - Works with partial names</li>
            <li><strong>💡 Intelligent field suggestions</strong> based on your data</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        table_name_enh = st.text_input(
            "Table Name* 🏗️",
            placeholder="SCHEMA.TABLE or just TABLE_NAME",
            key="enh_table_name",
            help="Can be just table name, schema.table, or database.schema.table"
        )
        
        sample_size = st.selectbox(
            "Analysis Sample Size 📊",
            [5, 10, 20, 50],
            index=1,
            key="enh_sample_size",
            help="Larger samples give better schema analysis but take longer"
        )
    
    with col2:
        json_column_enh = st.text_input(
            "JSON Column Name* 📄",
            placeholder="json_data",
            key="enh_json_column",
            help="Name of the column containing JSON data"
        )
        
        show_preview = st.checkbox(
            "Show Detailed Schema Preview 👀",
            value=True,
            key="enh_show_preview",
            help="Display comprehensive analysis of discovered JSON fields"
        )
    
    field_conditions_enh = st.text_area(
        "Field Conditions* 🎯",
        height=100,
        placeholder="e.g., name, age[>:18], status[=:active]",
        key="enh_field_conditions",
        help="Specify JSON fields and their filtering conditions"
    )
    
    # Smart suggestions section
    if 'discovered_schema_enhanced' in st.session_state:
        with st.expander("💡 Smart Field Suggestions (Based on Your Data)"):
            try:
                from enhanced_db_json_analyzer import render_enhanced_field_suggestions
                suggestions = render_enhanced_field_suggestions(st.session_state.discovered_schema_enhanced)
                
                if suggestions:
                    st.markdown("**🎯 Suggested field conditions based on your JSON data:**")
                    cols = st.columns(2)
                    for i, suggestion in enumerate(suggestions[:8]):
                        col_idx = i % 2
                        with cols[col_idx]:
                            if st.button(f"Use: `{suggestion}`", key=f"use_enh_suggestion_{i}"):
                                st.session_state.enh_field_conditions = suggestion
                                st.rerun()
                            st.code(suggestion, language="text")
                else:
                    st.info("No specific suggestions available for this schema.")
            except Exception as e:
                st.warning(f"Could not generate suggestions: {e}")
    
    col3, col4 = st.columns(2)
    
    with col3:
        if st.button("🔍 Analyze Schema Only", type="secondary"):
            if table_name_enh and json_column_enh:
                try:
                    with st.spinner("🔄 Enhanced schema analysis in progress..."):
                        schema, error, metadata = analyze_database_json_schema_enhanced(
                            conn_manager, table_name_enh, json_column_enh, sample_size
                        )
                        
                        if schema:
                            st.success(f"✅ Enhanced schema analysis complete! Found {len(schema)} fields.")
                            
                            # Store in session state for suggestions
                            st.session_state.discovered_schema_enhanced = schema
                            st.session_state.schema_metadata_enhanced = metadata
                            
                            if show_preview:
                                render_enhanced_database_json_preview(schema, metadata)
                        else:
                            st.error(error)
                            
                except Exception as e:
                    st.error(f"❌ Enhanced schema analysis failed: {str(e)}")
                    st.info("💡 This might be due to table access permissions or connection issues.")
            else:
                st.warning("⚠️ Please provide table name and JSON column.")
    
    with col4:
        if st.button("🚀 Analyze & Execute (Performance Mode)", type="primary"):
            if all([table_name_enh, json_column_enh, field_conditions_enh]):
                try:
                    # Use the enhanced database-driven analysis with performance monitoring
                    with st.spinner("⚡ Enhanced analysis with Modin acceleration..."):
                        generated_sql, sql_error = generate_database_driven_sql_enhanced(
                            conn_manager, table_name_enh, json_column_enh, field_conditions_enh
                        )
                        
                        if generated_sql and not sql_error:
                            st.success("✅ Enhanced SQL Generated Successfully!")
                            st.code(generated_sql, language="sql")
                            
                            # Execute with performance monitoring
                            with st.spinner("⚡ Executing with performance tracking..."):
                                result_df, exec_error, perf_stats = conn_manager.execute_query_with_performance(generated_sql)
                                
                                if result_df is not None:
                                    st.success("✅ Query executed with performance monitoring!")
                                    
                                    # Display performance metrics
                                    render_performance_metrics(perf_stats)
                                    
                                    # Results summary
                                    col_sum1, col_sum2, col_sum3 = st.columns(3)
                                    with col_sum1:
                                        st.metric("Rows Returned", len(result_df))
                                    with col_sum2:
                                        st.metric("Columns", len(result_df.columns))
                                    with col_sum3:
                                        processing_engine = "🚀 Modin" if perf_stats.get('modin_used', False) else "📊 Pandas"
                                        st.metric("Processing Engine", processing_engine)
                                    
                                    st.dataframe(result_df, use_container_width=True)
                                    
                                    # Enhanced download with performance info
                                    if not result_df.empty:
                                        csv_data = result_df.to_csv(index=False).encode('utf-8')
                                        filename = f"enhanced_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                                        st.download_button(
                                            "📥 Download Enhanced Results",
                                            data=csv_data,
                                            file_name=filename,
                                            mime="text/csv"
                                        )
                                        
                                        # Performance summary
                                        st.info(f"⚡ **Performance Summary:** Processed {len(result_df):,} rows in {perf_stats.get('total_time', 0):.2f}s using {processing_engine}")
                                else:
                                    st.error(f"❌ Query execution failed: {exec_error}")
                        else:
                            st.error(f"❌ SQL Generation Error: {sql_error}")
                            
                except Exception as e:
                    st.error(f"❌ Enhanced analysis failed: {str(e)}")
                    st.info("💡 Try checking your table name, column name, and database permissions.")
            else:
                st.warning("⚠️ Please fill in all required fields.")
    
    # Custom SQL section with performance monitoring
    st.markdown("---")
    st.markdown("### 📊 Custom SQL with Performance Monitoring")
    
    custom_sql_enh = st.text_area(
        "Execute Custom SQL with Modin Performance Tracking:",
        height=150,
        placeholder="""SELECT json_data:name::VARCHAR as name,
       json_data:age::NUMBER as age
FROM your_table
WHERE json_data:status::VARCHAR = 'active'
LIMIT 10;""",
        key="enh_custom_sql",
        help="Write any SQL query - large results will use Modin for faster processing"
    )
    
    col5, col6 = st.columns(2)
    
    with col5:
        if st.button("⚡ Execute with Performance Tracking", type="secondary"):
            if custom_sql_enh:
                with st.spinner("🔄 Executing with enhanced performance monitoring..."):
                    result_df, error, perf_stats = conn_manager.execute_query_with_performance(custom_sql_enh)
                    
                    if result_df is not None:
                        st.success("✅ Custom SQL executed successfully!")
                        render_performance_metrics(perf_stats)
                        st.dataframe(result_df, use_container_width=True)
                        
                        if not result_df.empty:
                            csv_data = result_df.to_csv(index=False).encode('utf-8')
                            st.download_button(
                                "📥 Download Custom Results",
                                data=csv_data,
                                file_name=f"custom_query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv"
                            )
                    else:
                        st.error(f"❌ Execution failed: {error}")
            else:
                st.warning("⚠️ Please enter a SQL query")
    
    with col6:
        if st.button("📊 Execute Standard Mode", type="secondary"):
            if custom_sql_enh:
                with st.spinner("🔄 Executing in standard mode..."):
                    result_df, error = conn_manager.execute_query(custom_sql_enh)
                    
                    if result_df is not None:
                        st.success("✅ Query executed in standard mode!")
                        st.dataframe(result_df, use_container_width=True)
                    else:
                        st.error(f"❌ Execution failed: {error}")
            else:
                st.warning("⚠️ Please enter a SQL query")


# Main App
def main():
    try:
        st.markdown('<h1 class="main-header">❄️ Enhanced JSON-to-SQL Analyzer for Snowflake</h1>', unsafe_allow_html=True)

        # Display performance information at the top
        render_enhanced_performance_info()

        # Top-level tabs for separated functionality
        main_tab1, main_tab2, main_tab3 = st.tabs([
            "🐍 **Pure Python (Instant SQL Generation)**",
            "🏔️ **Standard Snowflake Connection**",
            "⚡ **Enhanced Snowflake (High Performance + Modin)**"
        ])

        with main_tab1:
            st.markdown('<h2 class="section-header">🐍 Generate SQL from JSON Input</h2>', unsafe_allow_html=True)
            st.markdown("""
            <div class="feature-box">
            <p>Upload or paste your JSON data below to analyze its structure and instantly generate a corresponding Snowflake SQL query. No database connection is required for this feature.</p>
            </div>
            """, unsafe_allow_html=True)

            # Sidebar for input method selection
            st.sidebar.header("📥 Data Input for Python Analyzer")
            input_method = st.sidebar.radio(
                "Choose your input method:",
                ["Upload JSON File", "Paste JSON Text"],
                key="input_method",
                help="Select how you want to provide your JSON data for analysis"
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
                with st.spinner("Analyzing JSON structure..."):
                    schema = analyze_json_structure(json_data)

                if not schema:
                    st.error("❌ Could not analyze JSON structure.")
                    return

                # Create tabs for different features
                tab1, tab2, tab3, tab4, tab5 = st.tabs([
                    "⚡ **SQL Generator**",
                    "📊 **Complete Paths**",
                    "📋 **Arrays Analysis**",
                    "🔍 **Queryable Fields**",
                    "🎨 **JSON Formatter**"
                ])

                with tab1:
                    st.markdown('<h3 class="section-header">⚡ SQL Generator</h3>', unsafe_allow_html=True)
                    col1, col2 = st.columns(2)
                    with col1:
                        st.subheader("Query Parameters")
                        table_name = st.text_input("Table Name*", key="py_table", placeholder="your_schema.your_table")
                        json_column = st.text_input("JSON Column Name*", key="py_json_col", placeholder="json_data")
                        field_conditions = st.text_area("Field Conditions*", height=100, key="py_fields", placeholder="e.g., name, age[>:18]")

                    with col2:
                        st.subheader("💡 Examples")
                        examples = generate_procedure_examples(schema)
                        if examples:
                            for ex in examples:
                                st.code(ex, language="sql")
                        else:
                            st.info("No examples to generate based on this JSON.")
                    
                    if st.button("🚀 Generate SQL", type="primary"):
                        if all([table_name, json_column, field_conditions]):
                            sql = generate_sql_from_json_data(json_data, table_name, json_column, field_conditions)
                            st.code(sql, language="sql")
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
                st.info("👆 Provide JSON data via the sidebar to begin analysis and SQL generation.")

        with main_tab2:
            st.markdown('<h2 class="section-header">🏔️ Standard Snowflake Connection</h2>', unsafe_allow_html=True)
            st.markdown("""
            <div class="feature-box">
            <p>Connect to your Snowflake database using the standard connector. Good for basic operations and smaller datasets.</p>
            </div>
            """, unsafe_allow_html=True)

            st.subheader("🔐 Step 1: Database Connection")
            conn_manager = render_snowflake_connection_ui()

            if conn_manager and conn_manager.is_connected:
                st.markdown("---")
                st.subheader("📊 Step 2: Database Operations")
                render_snowflake_operations_ui(conn_manager, json_data=None)
            else:
                st.markdown("---")
                st.info("👆 **Connect to your Snowflake database above to unlock database operations.**")

        with main_tab3:
            st.markdown('<h2 class="section-header">⚡ Enhanced Snowflake Connection (High Performance + Modin)</h2>', unsafe_allow_html=True)
            
            # Enhanced features description
            st.markdown("""
            <div class="enhanced-box">
                <h4 style="color: #2e7d32;">🚀 Enhanced Features:</h4>
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; margin-top: 1rem;">
                    <div>
                        <h5 style="color: #1976d2;">🛠️ Connection Enhancements:</h5>
                        <ul style="margin-bottom: 0;">
                            <li><strong>✅ Fixes "database context" errors</strong></li>
                            <li><strong>🎯 Automatic session management</strong></li>
                            <li><strong>🛡️ Smart error recovery</strong></li>
                            <li><strong>📊 Connection diagnostics</strong></li>
                        </ul>
                    </div>
                    <div>
                        <h5 style="color: #1976d2;">⚡ Performance Features:</h5>
                        <ul style="margin-bottom: 0;">
                            <li><strong>🚀 Modin pandas integration</strong> - Up to 4x faster</li>
                            <li><strong>📈 Real-time performance metrics</strong></li>
                            <li><strong>🔍 Database-driven JSON analysis</strong></li>
                            <li><strong>💡 Smart field suggestions</strong></li>
                        </ul>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)

            # Enhanced connection UI
            st.subheader("🔐 Enhanced Database Connection")
            enhanced_conn_manager = render_enhanced_snowflake_connection_ui()

            if enhanced_conn_manager and enhanced_conn_manager.is_connected:
                # Test connectivity with comprehensive diagnostics
                connectivity_ok, status_msg = test_database_connectivity(enhanced_conn_manager)
                
                if connectivity_ok:
                    st.success(status_msg)
                    st.markdown("---")
                    st.subheader("⚡ Enhanced Database Operations")
                    render_enhanced_database_operations_ui(enhanced_conn_manager)
                else:
                    st.error(status_msg)
                    st.info("💡 Try disconnecting and reconnecting with correct database/schema settings.")
                    
                    # Disconnect button for troubleshooting
                    if st.button("🔌 Disconnect and Retry", type="secondary"):
                        enhanced_conn_manager.disconnect()
                        if 'enhanced_snowflake_connection' in st.session_state:
                            st.session_state.enhanced_snowflake_connection = None
                        st.info("✅ Disconnected. Please reconnect with correct settings.")
                        st.rerun()
            else:
                st.markdown("---")
                st.info("👆 **Connect using the enhanced connector above to unlock high-performance database operations with Modin acceleration.**")

        # Footer with enhanced information
        st.markdown("""
        <div class="footer">
            <p><strong>🚀 Enhanced JSON-to-SQL Analyzer</strong> | Built with ❤️ using Streamlit</p>
            <div style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 2rem; margin-top: 1rem; text-align: center;">
                <div>
                    <h4 style="color: #1976d2;">🐍 Python Mode</h4>
                    <p>Instant SQL generation<br/>No database required</p>
                </div>
                <div>
                    <h4 style="color: #ff7f0e;">🏔️ Standard Mode</h4>
                    <p>Basic Snowflake connectivity<br/>Standard operations</p>
                </div>
                <div>
                    <h4 style="color: #2e7d32;">⚡ Enhanced Mode</h4>
                    <p>High-performance with Modin<br/>Advanced error handling</p>
                </div>
            </div>
            <hr style="margin: 2rem 0; border: 1px solid #e9ecef;">
            <p><small>
                <strong>🎯 Smart Feature:</strong> Enhanced mode automatically analyzes your actual JSON data and fixes common connection issues!<br/>
                <strong>⚡ Performance:</strong> Modin acceleration available for datasets > 1000 rows
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
