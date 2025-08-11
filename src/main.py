import streamlit as st
import json
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime
import logging
import os

# Import all required modules from the 'src' directory
from python_sql_generator import generate_sql_from_json_data

# Import the NEW enhanced connector with Modin support
from enhanced_snowflake_connector import (
    render_enhanced_snowflake_connection_ui,
    render_enhanced_performance_info,
    render_performance_metrics,
    MODIN_AVAILABLE,
    SNOWFLAKE_AVAILABLE
)

# Import the universal analyzer with all necessary functions
from universal_db_analyzer import (
    generate_database_driven_sql_enhanced,
    analyze_database_json_schema_enhanced,
    render_enhanced_database_json_preview,
    render_enhanced_field_suggestions,
    test_database_connectivity
)

from json_analyzer import analyze_json_structure
from utils import (
    prettify_json, validate_json_input,
    export_analysis_results
)
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
    .enhanced-box {
        background: linear-gradient(145deg, #e8f5e8, #f1f8e9);
        padding: 1.5rem;
        border-radius: 10px;
        border: 2px solid #81c784;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)


def render_enhanced_database_operations_ui(conn_manager, key_prefix=''):
    """Render enhanced database operations UI with unique keys"""
    
    st.markdown("### 🧪 Smart JSON Analysis (FIXED Universal Logic)")
    
    col1, col2 = st.columns(2)
    
    with col1:
        table_name_enh = st.text_input(
            "Table Name* 🏗️",
            placeholder="SCHEMA.TABLE or just TABLE_NAME",
            key=f"{key_prefix}_table_name",
            help="Can be just table name, schema.table, or database.schema.table"
        )
        
        sample_size = st.selectbox(
            "Analysis Sample Size 📊",
            [5, 10, 20, 50],
            index=1,
            key=f"{key_prefix}_sample_size",
            help="Larger samples give better schema analysis but take longer"
        )
    
    with col2:
        json_column_enh = st.text_input(
            "JSON Column Name* 📄",
            placeholder="json_data",
            key=f"{key_prefix}_json_column",
            help="Name of the column containing JSON data"
        )
        
        show_preview = st.checkbox(
            "Show Detailed Schema Preview 👀",
            value=True,
            key=f"{key_prefix}_show_preview",
            help="Display comprehensive analysis of discovered JSON fields"
        )
    
    field_conditions_enh = st.text_area(
        "Field Conditions* 🎯",
        height=100,
        placeholder="e.g., name, age[>:18], status[=:active]",
        key=f"{key_prefix}_field_conditions",
        help="Specify JSON fields and their filtering conditions"
    )
    
    if f'discovered_schema_{key_prefix}' in st.session_state:
        with st.expander("💡 Smart Field Suggestions (Based on Your Data)"):
            suggestions = render_enhanced_field_suggestions(st.session_state[f'discovered_schema_{key_prefix}'])
            
            if suggestions:
                st.markdown("**🎯 Suggested field conditions based on your JSON data:**")
                cols = st.columns(2)
                for i, suggestion in enumerate(suggestions[:8]):
                    with cols[i % 2]:
                        if st.button(f"Use: `{suggestion}`", key=f"{key_prefix}_use_suggestion_{i}"):
                            st.session_state[f'{key_prefix}_field_conditions'] = suggestion
                            st.rerun()
            else:
                st.info("No specific suggestions available for this schema.")

    col3, col4 = st.columns(2)
    
    with col3:
        if st.button("🔍 Analyze Schema Only", type="secondary", key=f"{key_prefix}_analyze_schema"):
            if table_name_enh and json_column_enh:
                with st.spinner("🔄 Schema analysis in progress..."):
                    schema, error, metadata = analyze_database_json_schema_enhanced(
                        conn_manager, table_name_enh, json_column_enh, sample_size
                    )
                    if schema:
                        st.success(f"✅ Schema analysis complete! Found {len(schema)} fields.")
                        st.session_state[f'discovered_schema_{key_prefix}'] = schema
                        st.session_state[f'schema_metadata_{key_prefix}'] = metadata
                        if show_preview:
                            render_enhanced_database_json_preview(schema, metadata)
                    else:
                        st.error(error)
            else:
                st.warning("⚠️ Please provide table name and JSON column.")
    
    with col4:
        if st.button("🚀 Analyze & Generate SQL", type="primary", key=f"{key_prefix}_analyze_execute"):
            if all([table_name_enh, json_column_enh, field_conditions_enh]):
                with st.spinner("⚡ Generating SQL with fixed universal logic..."):
                    generated_sql, sql_error = generate_database_driven_sql_enhanced(
                        conn_manager, table_name_enh, json_column_enh, field_conditions_enh
                    )
                    if generated_sql and not sql_error:
                        st.success("✅ SQL Generated Successfully!")
                        st.code(generated_sql, language="sql")
                    else:
                        st.error(f"❌ SQL Generation Error: {sql_error}")
            else:
                st.warning("⚠️ Please fill in all required fields.")

# Main App
def main():
    try:
        st.markdown('<h1 class="main-header">❄️ Enhanced JSON-to-SQL Analyzer for Snowflake</h1>', unsafe_allow_html=True)

        render_enhanced_performance_info()

        main_tab1, main_tab2, main_tab3 = st.tabs([
            "🐍 **Pure Python (Instant SQL Generation)**",
            "🏔️ **Standard Snowflake Connection**",
            "⚡ **Enhanced Snowflake (High Performance + Modin)**"
        ])

        with main_tab1:
            st.markdown('<h2 class="section-header">🐍 Generate SQL from JSON Input</h2>', unsafe_allow_html=True)
            # ... (Python tab logic remains the same)
            st.sidebar.header("📥 Data Input for Python Analyzer")
            input_method = st.sidebar.radio(
                "Choose your input method:",
                ["Upload JSON File", "Paste JSON Text"],
                key="input_method"
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
            else:
                json_text = st.sidebar.text_area(
                    "Paste your JSON here:", height=250, placeholder='{"example": "data"}'
                )
                if json_text:
                    is_valid, _, json_data = validate_json_input(json_text)
                    if not is_valid:
                        st.sidebar.error("Invalid JSON format.")
                        json_data = None
            
            if json_data:
                schema = analyze_json_structure(json_data)
                tab1, tab2, tab3 = st.tabs(["⚡ **SQL Generator**", "📊 **Complete Paths**", "🎨 **JSON Formatter**"])

                with tab1:
                    st.markdown('<h3 class="section-header">⚡ SQL Generator</h3>', unsafe_allow_html=True)
                    table_name = st.text_input("Table Name*", key="py_table", placeholder="your_schema.your_table")
                    json_column = st.text_input("JSON Column Name*", key="py_json_col", placeholder="json_data")
                    field_conditions = st.text_area("Field Conditions*", height=100, key="py_fields", placeholder="e.g., name, age[>:18]")
                    
                    if st.button("🚀 Generate SQL", type="primary"):
                        if all([table_name, json_column, field_conditions]):
                            sql = generate_sql_from_json_data(json_data, table_name, json_column, field_conditions)
                            st.code(sql, language="sql")
                        else:
                            st.warning("Please fill in all required fields marked with *.")

                with tab2:
                    st.markdown('<h3 class="section-header">📊 Complete JSON Paths</h3>', unsafe_allow_html=True)
                    all_paths_df = export_analysis_results(schema).get('all_paths')
                    st.dataframe(all_paths_df, use_container_width=True)
                
                with tab3:
                    st.markdown('<h3 class="section-header">🎨 JSON Formatter</h3>', unsafe_allow_html=True)
                    st.code(prettify_json(json.dumps(json_data)), language='json')
            else:
                st.info("👆 Provide JSON data via the sidebar to begin analysis.")


        # --- FIX: Both Standard and Enhanced tabs now use the same enhanced logic ---
        with main_tab2:
            st.markdown('<h2 class="section-header">🏔️ Standard Snowflake Connection (Using Enhanced Logic)</h2>', unsafe_allow_html=True)
            st.markdown("""
            <div class="feature-box">
            <p>Connect to your Snowflake database. This tab now uses the same <strong>fixed and enhanced logic</strong> as the high-performance tab to ensure correctness and reliability.</p>
            </div>
            """, unsafe_allow_html=True)
            
            st.subheader("🔐 Database Connection")
            # FIXED: Pass a unique key_prefix for this tab
            std_conn_manager = render_enhanced_snowflake_connection_ui(key_prefix='std')

            if std_conn_manager and std_conn_manager.is_connected:
                connectivity_ok, status_msg = test_database_connectivity(std_conn_manager)
                if connectivity_ok:
                    st.success(status_msg)
                    st.markdown("---")
                    st.subheader("⚡ Database Operations")
                    render_enhanced_database_operations_ui(std_conn_manager, key_prefix='std')
                else:
                    st.error(status_msg)
            else:
                st.info("👆 **Connect to your Snowflake database above to unlock database operations.**")
        
        with main_tab3:
            st.markdown('<h2 class="section-header">⚡ Enhanced Snowflake Connection (High Performance + Modin)</h2>', unsafe_allow_html=True)
            st.markdown("""
            <div class="enhanced-box">
                <h4 style="color: #2e7d32;">🚀 Performance Features:</h4>
                <p>This tab provides additional performance metrics and is optimized for large datasets using Modin acceleration when available.</p>
            </div>
            """, unsafe_allow_html=True)
            
            st.subheader("🔐 Enhanced Database Connection")
            # FIXED: Pass a unique key_prefix for this tab
            enh_conn_manager = render_enhanced_snowflake_connection_ui(key_prefix='enh')

            if enh_conn_manager and enh_conn_manager.is_connected:
                connectivity_ok, status_msg = test_database_connectivity(enh_conn_manager)
                if connectivity_ok:
                    st.success(status_msg)
                    st.markdown("---")
                    st.subheader("⚡ High-Performance Database Operations")
                    render_enhanced_database_operations_ui(enh_conn_manager, key_prefix='enh')
                else:
                    st.error(status_msg)
            else:
                st.info("👆 **Connect using the enhanced connector above to unlock high-performance database operations.**")

    except Exception as e:
        logger.error(f"Application error: {str(e)}", exc_info=True)
        st.error(f"❌ Application Error: {str(e)}")
        with st.expander("🔧 Error Details"):
            st.code(str(e))


if __name__ == "__main__":
    main()
