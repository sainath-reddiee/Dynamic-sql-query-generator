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
from enhanced_snowflake_connector import (
    EnhancedSnowflakeConnectionManager, 
    render_enhanced_performance_info,
    render_performance_metrics,
    MODIN_AVAILABLE,
    SNOWFLAKE_AVAILABLE
)

# Import database-driven analysis
from db_json_analyzer import generate_database_driven_sql

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
</style>
""", unsafe_allow_html=True)

# Main App
def main():
    try:
        st.markdown('<h1 class="main-header">❄️ JSON-to-SQL Analyzer for Snowflake</h1>', unsafe_allow_html=True)

        # Display performance information at the top
        render_enhanced_performance_info()

        # Top-level tabs for separated functionality
        main_tab1, main_tab2, main_tab3 = st.tabs([
            "🐍 **Pure Python (Instant SQL Generation)**",
            "🏔️ **Snowflake Database (Standard Connection)**",
            "⚡ **Enhanced Snowflake (High Performance)**"
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
            <p>Connect to your Snowflake database using the standard connector. Analyzes JSON data directly from your database tables - no file upload required!</p>
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
            st.markdown('<h2 class="section-header">⚡ Enhanced Snowflake Connection (High Performance)</h2>', unsafe_allow_html=True)
            
            if MODIN_AVAILABLE:
                st.success("🚀 **Modin Performance Mode Available** - Optimized for large datasets!")
            else:
                st.warning("⚠️ **Standard Mode** - Install Modin for enhanced performance: `pip install modin[all]`")

            st.markdown("""
            <div class="feature-box">
            <p><strong>Enhanced features:</strong></p>
            <ul>
            <li>🚀 <strong>Modin pandas</strong> - Up to 4x faster operations on large datasets</li>
            <li>⚡ <strong>Snowflake Modin plugin</strong> - Direct integration with Snowflake</li>
            <li>📈 <strong>Performance monitoring</strong> - Real-time metrics</li>
            <li>🎯 <strong>Optimized queries</strong> - Better memory management</li>
            <li>🔍 <strong>Database-driven analysis</strong> - Analyzes your actual JSON data</li>
            </ul>
            </div>
            """, unsafe_allow_html=True)

            # Enhanced connection manager
            enhanced_conn_manager = EnhancedSnowflakeConnectionManager()

            # Connection form for enhanced connector
            st.subheader("🔐 Enhanced Database Connection")
            
            with st.form("enhanced_snowflake_connection", clear_on_submit=False):
                col1, col2 = st.columns(2)
                
                with col1:
                    account = st.text_input("Account Identifier*", key="enh_account")
                    user = st.text_input("Username*", key="enh_user")
                    password = st.text_input("Password*", type="password", key="enh_password")
                
                with col2:
                    warehouse = st.text_input("Warehouse*", key="enh_warehouse")
                    database = st.text_input("Database*", key="enh_database")
                    schema = st.text_input("Schema*", value="PUBLIC", key="enh_schema")
                
                col3, col4 = st.columns(2)
                with col3:
                    test_btn = st.form_submit_button("🧪 Test Enhanced Connection")
                with col4:
                    connect_btn = st.form_submit_button("⚡ Connect with Performance Mode")

                if test_btn or connect_btn:
                    connection_params = {
                        'account': account, 'user': user, 'password': password,
                        'warehouse': warehouse, 'database': database, 'schema': schema
                    }
                    
                    if test_btn:
                        success, message = enhanced_conn_manager.test_connection(connection_params)
                        if success:
                            st.success(message)
                        else:
                            st.error(message)
                    
                    if connect_btn:
                        if enhanced_conn_manager.connect(connection_params):
                            st.session_state.enhanced_connection = enhanced_conn_manager
                            st.success("⚡ **Enhanced connection established!**")

            # Enhanced operations
            if 'enhanced_connection' in st.session_state:
                enhanced_conn = st.session_state.enhanced_connection
                if enhanced_conn.is_connected:
                    st.markdown("---")
                    st.subheader("⚡ Enhanced Database Operations")
                    
                    # Database-driven JSON analysis with performance monitoring
                    st.markdown("### 🧪 Smart JSON Analysis (Performance Mode)")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        enh_table = st.text_input("Table Name*", key="enh_table", placeholder="SCHEMA.TABLE")
                        enh_json_col = st.text_input("JSON Column*", key="enh_json_col", placeholder="json_data")
                    
                    with col2:
                        enh_conditions = st.text_area("Field Conditions*", key="enh_conditions", 
                                                    placeholder="name, age[>:18], status[=:active]", height=100)
                    
                    if st.button("⚡ Analyze & Execute (Performance Mode)", type="primary"):
                        if all([enh_table, enh_json_col, enh_conditions]):
                            try:
                                with st.spinner("🔄 Enhanced analysis with performance monitoring..."):
                                    # Use database-driven analysis
                                    generated_sql, sql_error = generate_database_driven_sql(
                                        enhanced_conn, enh_table, enh_json_col, enh_conditions
                                    )
                                    
                                    if generated_sql and not sql_error:
                                        st.success("✅ Enhanced SQL Generated!")
                                        st.code(generated_sql, language="sql")
                                        
                                        # Execute with performance monitoring
                                        df, error, perf_stats = enhanced_conn.execute_query_with_performance(generated_sql)
                                        
                                        if df is not None:
                                            st.success("✅ Query executed with performance monitoring!")
                                            render_performance_metrics(perf_stats)
                                            st.dataframe(df, use_container_width=True)
                                            
                                            # Enhanced download with performance info
                                            if not df.empty:
                                                csv_data = df.to_csv(index=False).encode('utf-8')
                                                st.download_button(
                                                    "📥 Download Enhanced Results",
                                                    data=csv_data,
                                                    file_name=f"enhanced_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                                    mime="text/csv"
                                                )
                                        else:
                                            st.error(error)
                                    else:
                                        st.error(f"SQL Generation Error: {sql_error}")
                            except Exception as e:
                                st.error(f"Enhanced analysis failed: {str(e)}")
                        else:
                            st.warning("Please fill in all required fields.")
                    
                    st.markdown("---")
                    
                    # Custom SQL with performance monitoring
                    st.markdown("### 📊 Custom SQL with Performance Monitoring")
                    sql_input = st.text_area("Execute SQL with Performance Monitoring:", height=150)
                    
                    if st.button("⚡ Execute with Performance Tracking"):
                        if sql_input:
                            with st.spinner("Executing with performance monitoring..."):
                                df, error, perf_stats = enhanced_conn.execute_query_with_performance(sql_input)
                                
                                if df is not None:
                                    st.success("✅ Query executed successfully!")
                                    render_performance_metrics(perf_stats)
                                    st.dataframe(df, use_container_width=True)
                                else:
                                    st.error(error)

        # Footer
        st.markdown("""
        <div class="footer">
            <p>Built with ❤️ using Streamlit | Enhanced with Snowflake Modin Performance | Designed for JSON Analysis</p>
            <p><small>🎯 <strong>Smart Feature:</strong> Database operations now analyze your actual JSON data automatically!</small></p>
        </div>
        """, unsafe_allow_html=True)

    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        st.error(f"❌ Application Error: {str(e)}")
        st.error("Please refresh the page and try again.")

if __name__ == "__main__":
    main()
