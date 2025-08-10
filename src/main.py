import streamlit as st
import json
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime
import logging
import os

# Import all required modules from the 'src' directory
from python_sql_generator import generate_sql_from_json_data
from snowflake_connector import render_snowflake_connection_ui, render_snowflake_operations_ui
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

        # Top-level tabs for separated functionality
        main_tab1, main_tab2 = st.tabs([
            "🐍 **Pure Python (Instant SQL Generation)**",
            "🏔️ **Snowflake Database (Connect & Execute)**"
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
            st.markdown('<h2 class="section-header">🏔️ Connect to Snowflake & Execute Queries</h2>', unsafe_allow_html=True)
            st.markdown("""
            <div class="feature-box">
            <p>Connect directly to your Snowflake database to run queries on your existing tables. This feature is for direct database interaction and does not require you to upload any JSON files.</p>
            </div>
            """, unsafe_allow_html=True)

            st.subheader("🔐 Step 1: Database Connection")
            conn_manager = render_snowflake_connection_ui()

            if conn_manager and conn_manager.is_connected:
                st.markdown("---")
                st.subheader("📊 Step 2: Database Operations")
                # json_data is not needed for direct DB operations, so we pass None
                render_snowflake_operations_ui(conn_manager, json_data=None)
            else:
                st.markdown("---")
                st.info("👆 **Connect to your Snowflake database above to unlock database operations.**")

        # Footer
        st.markdown("""
        <div class="footer">
            <p>Built with ❤️ using Streamlit | Designed for Snowflake JSON Analysis</p>
        </div>
        """, unsafe_allow_html=True)

    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        st.error(f"❌ Application Error: {str(e)}")
        st.error("Please refresh the page and try again.")

if __name__ == "__main__":
    main()
