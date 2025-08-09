import streamlit as st
import json
import pandas as pd
from typing import Dict, Any, List, Set, Tuple
import re
from datetime import datetime
import logging
import os
from python_sql_generator import generate_sql_from_json_data
from enhanced_snowflake_connector import EnhancedSnowflakeConnectionManager, render_enhanced_performance_info, render_performance_metrics
from snowflake_connector import render_snowflake_connection_ui, render_snowflake_operations_ui

# Import from our modules
from json_analyzer import analyze_json_structure
from utils import (
    get_snowflake_type, find_arrays, find_nested_objects,
    find_queryable_fields, prettify_json, validate_json_input,
    export_analysis_results
)
from sql_generator import generate_procedure_examples, generate_sql_preview

# Import configuration
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
    .json-path {
        font-family: 'Courier New', monospace;
        background: linear-gradient(90deg, #e8f4fd, #f0f8ff);
        padding: 0.3rem 0.6rem;
        border-radius: 0.4rem;
        color: #0066cc;
        border: 1px solid #b3d9ff;
        display: inline-block;
    }
    .array-indicator {
        background: linear-gradient(90deg, #ffe6cc, #fff0e6);
        color: #cc6600;
        padding: 0.2rem 0.5rem;
        border-radius: 0.4rem;
        font-size: 0.85rem;
        font-weight: 500;
        border: 1px solid #ffcc99;
    }
    .nested-indicator {
        background: linear-gradient(90deg, #e6f3ff, #f0f8ff);
        color: #0080ff;
        padding: 0.2rem 0.5rem;
        border-radius: 0.4rem;
        font-size: 0.85rem;
        font-weight: 500;
        border: 1px solid #b3d9ff;
    }
    .queryable-indicator {
        background: linear-gradient(90deg, #e6ffe6, #f0fff0);
        color: #00b300;
        padding: 0.2rem 0.5rem;
        border-radius: 0.4rem;
        font-size: 0.85rem;
        font-weight: 500;
        border: 1px solid #99ff99;
    }
    .metric-card {
        background: linear-gradient(145deg, #ffffff, #f8f9fa);
        padding: 1rem;
        border-radius: 0.6rem;
        text-align: center;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        border: 1px solid #e9ecef;
    }
    .warning-box {
        background: linear-gradient(145deg, #fff3cd, #fcf8e3);
        border: 1px solid #ffeaa7;
        padding: 1rem;
        border-radius: 0.5rem;
        color: #856404;
        margin: 1rem 0;
    }
    .success-box {
        background: linear-gradient(145deg, #d1ecf1, #bee5eb);
        border: 1px solid #86cfda;
        padding: 1rem;
        border-radius: 0.5rem;
        color: #0c5460;
        margin: 1rem 0;
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
        # Header
        st.markdown('<h1 class="main-header">❄️ JSON-to-SQL Analyzer for Snowflake</h1>', unsafe_allow_html=True)

        # Description
        st.markdown("""
        <div class="feature-box">
        <h3>🎯 About This Tool</h3>
        <p>Analyze complex JSON structures and generate dynamic SQL for Snowflake databases. Upload your JSON files or paste JSON data to:</p>
        <ul>
            <li>📊 <strong>Analyze JSON Structure</strong> - Understand your data hierarchy and types</li>
            <li>🔍 <strong>Identify Queryable Fields</strong> - Find fields suitable for SQL queries</li>
            <li>📋 <strong>Detect Arrays & Nested Objects</strong> - Handle complex data structures</li>
            <li>✨ <strong>Format & Export</strong> - Beautify JSON and export analysis results</li>
        </ul>
        </div>
        """, unsafe_allow_html=True)

        # Sidebar for input method selection
        st.sidebar.header("📥 Data Input")
        input_method = st.sidebar.radio(
            "Choose your input method:",
            ["Upload JSON File", "Paste JSON Text"],
            help="Select how you want to provide your JSON data"
        )

        json_data = None
        schema = None

        # Input handling
        if input_method == "Upload JSON File":
            uploaded_file = st.sidebar.file_uploader(
                "Choose a JSON file",
                type=['json'],
                help="Upload a JSON file to analyze its structure (max 200MB)"
            )
            if uploaded_file is not None:
                try:
                    if uploaded_file.size > 200 * 1024 * 1024:
                        st.sidebar.error("❌ File too large. Please upload a file smaller than 200MB.")
                        return
                    json_data = json.load(uploaded_file)
                    st.sidebar.markdown(f'<div class="success-box">✅ File "{uploaded_file.name}" loaded successfully!</div>', unsafe_allow_html=True)
                except Exception as e:
                    st.sidebar.error(f"❌ Error reading file: {str(e)}")
        else:  # Paste JSON Text
            json_text = st.sidebar.text_area(
                "Paste your JSON here:",
                height=200,
                placeholder='{"example": "data"}'
            )
            if json_text.strip():
                is_valid, message, json_data = validate_json_input(json_text)
                if not is_valid:
                    st.sidebar.error(f"❌ {message}")
                    json_data = None # Ensure json_data is None on error
                else:
                    st.sidebar.markdown('<div class="success-box">✅ JSON parsed successfully!</div>', unsafe_allow_html=True)

        # The main tabs are now created regardless of whether JSON is loaded
        tab_list = [
            "📊 Complete Paths",
            "📋 Arrays Analysis",
            "🏗️ Nested Objects",
            "🔍 Queryable Fields",
            "🎨 JSON Formatter",
            "⚡ SQL Generator"
        ]
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(tab_list)

        # Analyze JSON structure once if data is available
        if json_data:
            with st.spinner("Analyzing JSON structure..."):
                schema = analyze_json_structure(json_data)
            if not schema:
                st.error("❌ Failed to analyze JSON structure. Please check your data and try again.")
                # Do not return, allow other tabs to function
        
        # Content for Analysis Tabs (1-5)
        with tab1:
            st.markdown('<h2 class="section-header">📊 Complete JSON Paths</h2>', unsafe_allow_html=True)
            if schema:
                # UI for displaying paths
                pass # ... (rest of the logic for this tab)
            else:
                st.info("👆 Upload or paste JSON data to see the analysis.")

        with tab2:
            st.markdown('<h2 class="section-header">📋 Arrays Analysis</h2>', unsafe_allow_html=True)
            if schema:
                # UI for displaying array analysis
                pass # ... (rest of the logic for this tab)
            else:
                st.info("👆 Upload or paste JSON data to see the analysis.")

        with tab3:
            st.markdown('<h2 class="section-header">🏗️ Nested Objects</h2>', unsafe_allow_html=True)
            if schema:
                # UI for displaying nested objects
                pass # ... (rest of the logic for this tab)
            else:
                st.info("👆 Upload or paste JSON data to see the analysis.")

        with tab4:
            st.markdown('<h2 class="section-header">🔍 Queryable Fields</h2>', unsafe_allow_html=True)
            if schema:
                # UI for displaying queryable fields
                pass # ... (rest of the logic for this tab)
            else:
                st.info("👆 Upload or paste JSON data to see the analysis.")
        
        with tab5:
            st.markdown('<h2 class="section-header">🎨 JSON Formatter</h2>', unsafe_allow_html=True)
            if json_data:
                # UI for JSON formatter
                pass # ... (rest of the logic for this tab)
            else:
                st.info("👆 Upload or paste JSON data to use the formatter.")

        # Content for Tab 6 (SQL Generator) - Always visible
        with tab6:
            st.markdown('<h2 class="section-header">⚡ SQL Generator</h2>', unsafe_allow_html=True)

            approach = st.radio(
                "Select SQL Generation Method:",
                ["🐍 Pure Python (Instant)", "🏔️ Snowflake Database (Live)"],
                help="Choose 'Pure Python' to generate SQL from uploaded data, or 'Snowflake' to connect and analyze live database data."
            )
            st.markdown("---")

            if approach == "🐍 Pure Python (Instant)":
                st.markdown("#### 🐍 Pure Python SQL Generation")
                if not json_data:
                    st.warning("Please upload or paste JSON data first to use this generator.")
                else:
                    table_name = st.text_input("Table Name:", key="python_table")
                    json_column_name = st.text_input("JSON Column Name:", key="python_column")
                    field_conditions = st.text_area("Field Conditions:", key="python_conditions")
                    if st.button("🚀 Generate SQL", key="python_generate"):
                        if all([table_name, json_column_name, field_conditions]):
                            sql = generate_sql_from_json_data(json_data, table_name, json_column_name, field_conditions)
                            st.code(sql, language="sql")
                        else:
                            st.warning("Please fill in all fields.")
            else:  # Snowflake Database approach
                st.markdown("#### 🏔️ Snowflake Database Integration")
                
                # The connection UI is always available
                conn_manager = render_snowflake_connection_ui()

                if conn_manager and conn_manager.is_connected:
                    st.markdown("---")
                    st.subheader("📊 Database Operations")
                    # The operations UI is now self-contained and manages its own data fetching
                    render_snowflake_operations_ui(conn_manager)
                # ** THE FIX IS HERE: This block is now correctly indented. **
                elif 'snowflake_connection' not in st.session_state:
                    st.info("👆 Connect to your Snowflake database to proceed.")


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
