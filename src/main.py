import streamlit as st
import json
import pandas as pd
from typing import Dict, Any, List, Set, Tuple
import re
from datetime import datetime
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Mock imports for missing modules - replace with actual imports when available
try:
    from python_sql_generator import generate_sql_from_json_data
except ImportError:
    def generate_sql_from_json_data(json_data, table_name, json_column_name, field_conditions):
        return f"-- SQL Generation not available\n-- Table: {table_name}\n-- Column: {json_column_name}\n-- Conditions: {field_conditions}"

try:
    from enhanced_snowflake_connector import EnhancedSnowflakeConnectionManager, render_enhanced_performance_info, render_performance_metrics
    from snowflake_connector import render_snowflake_connection_ui, render_snowflake_operations_ui
except ImportError:
    def render_snowflake_connection_ui():
        st.warning("Snowflake connector modules not available. Please ensure all dependencies are installed.")
        return None
    def render_snowflake_operations_ui(conn_manager):
        st.info("Snowflake operations UI not available.")

try:
    from json_analyzer import analyze_json_structure
except ImportError:
    def analyze_json_structure(json_data):
        """Mock JSON structure analyzer"""
        def traverse_json(obj, path="", schema=None):
            if schema is None:
                schema = {}
            
            if isinstance(obj, dict):
                for key, value in obj.items():
                    current_path = f"{path}.{key}" if path else key
                    schema[current_path] = {
                        'type': type(value).__name__,
                        'snowflake_type': get_snowflake_type(value),
                        'is_array': isinstance(value, list),
                        'is_nested': isinstance(value, (dict, list)),
                        'is_queryable': not isinstance(value, (dict, list))
                    }
                    if isinstance(value, (dict, list)):
                        traverse_json(value, current_path, schema)
            elif isinstance(obj, list) and obj:
                # Analyze first item in array for structure
                traverse_json(obj[0], path, schema)
            
            return schema
        
        return traverse_json(json_data)

try:
    from utils import (
        get_snowflake_type, find_arrays, find_nested_objects,
        find_queryable_fields, prettify_json, validate_json_input,
        export_analysis_results
    )
except ImportError:
    # Mock utility functions
    def get_snowflake_type(value):
        """Convert Python type to Snowflake type"""
        type_mapping = {
            str: "VARCHAR",
            int: "NUMBER",
            float: "FLOAT",
            bool: "BOOLEAN",
            dict: "OBJECT",
            list: "ARRAY"
        }
        return type_mapping.get(type(value), "VARIANT")
    
    def find_arrays(schema):
        return {path: info for path, info in schema.items() if info.get('is_array', False)}
    
    def find_nested_objects(schema):
        return {path: info for path, info in schema.items() if info.get('is_nested', False) and not info.get('is_array', False)}
    
    def find_queryable_fields(schema):
        return {path: info for path, info in schema.items() if info.get('is_queryable', False)}
    
    def prettify_json(json_data):
        return json.dumps(json_data, indent=2, ensure_ascii=False)
    
    def validate_json_input(json_text):
        try:
            data = json.loads(json_text)
            return True, "Valid JSON", data
        except json.JSONDecodeError as e:
            return False, f"Invalid JSON: {str(e)}", None
    
    def export_analysis_results(schema, format_type="json"):
        if format_type == "json":
            return json.dumps(schema, indent=2)
        elif format_type == "csv":
            df = pd.DataFrame.from_dict(schema, orient='index')
            return df.to_csv(index=True)

try:
    from sql_generator import generate_procedure_examples, generate_sql_preview
except ImportError:
    def generate_procedure_examples():
        return "-- Procedure examples not available"
    
    def generate_sql_preview(schema):
        return "-- SQL preview not available"

try:
    from config import config
except ImportError:
    class Config:
        APP_NAME = "JSON-to-SQL Analyzer"
    config = Config()

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
        margin: 0.2rem;
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

def render_path_analysis(schema):
    """Render the complete paths analysis"""
    if not schema:
        st.info("No schema data available")
        return
    
    st.markdown("### 📈 Overview")
    
    # Metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown('<div class="metric-card"><h3>📊</h3><p>Total Paths</p><h2>{}</h2></div>'.format(len(schema)), unsafe_allow_html=True)
    with col2:
        arrays = len([p for p in schema.values() if p.get('is_array')])
        st.markdown('<div class="metric-card"><h3>📋</h3><p>Arrays</p><h2>{}</h2></div>'.format(arrays), unsafe_allow_html=True)
    with col3:
        nested = len([p for p in schema.values() if p.get('is_nested') and not p.get('is_array')])
        st.markdown('<div class="metric-card"><h3>🏗️</h3><p>Objects</p><h2>{}</h2></div>'.format(nested), unsafe_allow_html=True)
    with col4:
        queryable = len([p for p in schema.values() if p.get('is_queryable')])
        st.markdown('<div class="metric-card"><h3>🔍</h3><p>Queryable</p><h2>{}</h2></div>'.format(queryable), unsafe_allow_html=True)
    
    st.markdown("---")
    st.markdown("### 🗂️ All JSON Paths")
    
    # Create DataFrame for display
    path_data = []
    for path, info in schema.items():
        indicators = []
        if info.get('is_array'):
            indicators.append("Array")
        if info.get('is_nested') and not info.get('is_array'):
            indicators.append("Nested")
        if info.get('is_queryable'):
            indicators.append("Queryable")
        
        path_data.append({
            "Path": path,
            "Python Type": info.get('type', 'Unknown'),
            "Snowflake Type": info.get('snowflake_type', 'VARIANT'),
            "Properties": " | ".join(indicators) if indicators else "Basic"
        })
    
    df = pd.DataFrame(path_data)
    st.dataframe(df, use_container_width=True)

def render_arrays_analysis(schema):
    """Render arrays analysis"""
    arrays = find_arrays(schema)
    
    if not arrays:
        st.info("🎉 No arrays found in your JSON structure!")
        return
    
    st.markdown(f"### 📋 Found {len(arrays)} Array(s)")
    
    for path, info in arrays.items():
        with st.expander(f"📋 {path}"):
            col1, col2 = st.columns([2, 1])
            with col1:
                st.markdown(f'<span class="json-path">{path}</span>', unsafe_allow_html=True)
                st.write(f"**Snowflake Type:** `{info.get('snowflake_type', 'ARRAY')}`")
                st.write("**Usage Example:**")
                st.code(f"""
-- Flatten array elements
SELECT 
    f.value as array_element
FROM your_table t,
LATERAL FLATTEN(input => t.json_column:{path.replace('.', ':')}) f;
                """, language="sql")
            with col2:
                st.markdown('<span class="array-indicator">🔢 ARRAY</span>', unsafe_allow_html=True)

def render_nested_objects_analysis(schema):
    """Render nested objects analysis"""
    nested = find_nested_objects(schema)
    
    if not nested:
        st.info("🎉 No nested objects found in your JSON structure!")
        return
    
    st.markdown(f"### 🏗️ Found {len(nested)} Nested Object(s)")
    
    for path, info in nested.items():
        with st.expander(f"🏗️ {path}"):
            col1, col2 = st.columns([2, 1])
            with col1:
                st.markdown(f'<span class="json-path">{path}</span>', unsafe_allow_html=True)
                st.write(f"**Snowflake Type:** `{info.get('snowflake_type', 'OBJECT')}`")
                st.write("**Usage Example:**")
                st.code(f"""
-- Access nested object properties
SELECT 
    json_column:{path.replace('.', ':')} as nested_object,
    json_column:{path.replace('.', ':')}.property_name as specific_property
FROM your_table;
                """, language="sql")
            with col2:
                st.markdown('<span class="nested-indicator">📦 OBJECT</span>', unsafe_allow_html=True)

def render_queryable_fields_analysis(schema):
    """Render queryable fields analysis"""
    queryable = find_queryable_fields(schema)
    
    if not queryable:
        st.info("No directly queryable fields found.")
        return
    
    st.markdown(f"### 🔍 Found {len(queryable)} Queryable Field(s)")
    
    # Group by Snowflake type for better organization
    type_groups = {}
    for path, info in queryable.items():
        sf_type = info.get('snowflake_type', 'VARIANT')
        if sf_type not in type_groups:
            type_groups[sf_type] = []
        type_groups[sf_type].append((path, info))
    
    for sf_type, fields in type_groups.items():
        with st.expander(f"📊 {sf_type} Fields ({len(fields)})"):
            for path, info in fields:
                col1, col2, col3 = st.columns([3, 1, 1])
                with col1:
                    st.markdown(f'<span class="json-path">{path}</span>', unsafe_allow_html=True)
                with col2:
                    st.markdown(f'<span class="queryable-indicator">✅ {sf_type}</span>', unsafe_allow_html=True)
                with col3:
                    st.code(f":{path.replace('.', ':')}")

def render_json_formatter(json_data):
    """Render JSON formatter tab"""
    st.markdown("### 🎨 JSON Formatting Options")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.markdown("**⚙️ Format Settings**")
        indent_size = st.selectbox("Indentation:", [2, 4, 8], index=0)
        sort_keys = st.checkbox("Sort Keys", value=False)
        ensure_ascii = st.checkbox("Ensure ASCII", value=False)
        
        if st.button("🎨 Format JSON"):
            try:
                formatted = json.dumps(
                    json_data, 
                    indent=indent_size, 
                    sort_keys=sort_keys, 
                    ensure_ascii=ensure_ascii
                )
                st.session_state['formatted_json'] = formatted
                st.success("✅ JSON formatted successfully!")
            except Exception as e:
                st.error(f"❌ Error formatting JSON: {str(e)}")
    
    with col2:
        st.markdown("**📊 JSON Statistics**")
        
        def count_json_elements(obj, counts=None):
            if counts is None:
                counts = {'objects': 0, 'arrays': 0, 'strings': 0, 'numbers': 0, 'booleans': 0, 'nulls': 0}
            
            if isinstance(obj, dict):
                counts['objects'] += 1
                for value in obj.values():
                    count_json_elements(value, counts)
            elif isinstance(obj, list):
                counts['arrays'] += 1
                for item in obj:
                    count_json_elements(item, counts)
            elif isinstance(obj, str):
                counts['strings'] += 1
            elif isinstance(obj, (int, float)):
                counts['numbers'] += 1
            elif isinstance(obj, bool):
                counts['booleans'] += 1
            elif obj is None:
                counts['nulls'] += 1
            
            return counts
        
        stats = count_json_elements(json_data)
        for key, value in stats.items():
            st.metric(key.title(), value)
    
    # Display formatted JSON
    if 'formatted_json' in st.session_state:
        st.markdown("### 📄 Formatted JSON")
        st.code(st.session_state['formatted_json'], language="json")
        
        # Download button
        st.download_button(
            label="💾 Download Formatted JSON",
            data=st.session_state['formatted_json'],
            file_name=f"formatted_json_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json"
        )

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
                placeholder='{"example": "data", "nested": {"value": 123}, "array": [1, 2, 3]}'
            )
            if json_text.strip():
                is_valid, message, json_data = validate_json_input(json_text)
                if not is_valid:
                    st.sidebar.error(f"❌ {message}")
                    json_data = None
                else:
                    st.sidebar.markdown('<div class="success-box">✅ JSON parsed successfully!</div>', unsafe_allow_html=True)

        # Create tabs
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
        
        # Tab 1: Complete Paths
        with tab1:
            st.markdown('<h2 class="section-header">📊 Complete JSON Paths</h2>', unsafe_allow_html=True)
            if schema:
                render_path_analysis(schema)
            else:
                st.info("👆 Upload or paste JSON data to see the complete path analysis.")

        # Tab 2: Arrays Analysis
        with tab2:
            st.markdown('<h2 class="section-header">📋 Arrays Analysis</h2>', unsafe_allow_html=True)
            if schema:
                render_arrays_analysis(schema)
            else:
                st.info("👆 Upload or paste JSON data to see the arrays analysis.")

        # Tab 3: Nested Objects
        with tab3:
            st.markdown('<h2 class="section-header">🏗️ Nested Objects</h2>', unsafe_allow_html=True)
            if schema:
                render_nested_objects_analysis(schema)
            else:
                st.info("👆 Upload or paste JSON data to see the nested objects analysis.")

        # Tab 4: Queryable Fields
        with tab4:
            st.markdown('<h2 class="section-header">🔍 Queryable Fields</h2>', unsafe_allow_html=True)
            if schema:
                render_queryable_fields_analysis(schema)
            else:
                st.info("👆 Upload or paste JSON data to see the queryable fields analysis.")
        
        # Tab 5: JSON Formatter
        with tab5:
            st.markdown('<h2 class="section-header">🎨 JSON Formatter</h2>', unsafe_allow_html=True)
            if json_data:
                render_json_formatter(json_data)
            else:
                st.info("👆 Upload or paste JSON data to use the formatter.")

        # Tab 6: SQL Generator
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
                    col1, col2 = st.columns(2)
                    with col1:
                        table_name = st.text_input("Table Name:", value="my_table", key="python_table")
                        json_column_name = st.text_input("JSON Column Name:", value="json_data", key="python_column")
                    with col2:
                        field_conditions = st.text_area(
                            "Field Conditions:", 
                            value="name = 'example'\nage > 25",
                            key="python_conditions",
                            help="Enter conditions one per line"
                        )
                    
                    if st.button("🚀 Generate SQL", key="python_generate"):
                        if all([table_name, json_column_name]):
                            try:
                                sql = generate_sql_from_json_data(json_data, table_name, json_column_name, field_conditions)
                                st.markdown("#### Generated SQL:")
                                st.code(sql, language="sql")
                                
                                # Download button for SQL
                                st.download_button(
                                    label="💾 Download SQL",
                                    data=sql,
                                    file_name=f"generated_sql_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql",
                                    mime="text/plain"
                                )

                                # Mocked query result for pure python approach
                                st.markdown("#### Mocked Query Results:")
                                st.info("This is a mocked result to demonstrate the output format. In a real environment, this would be the result of executing the query on Snowflake.")
                                mock_data = {
                                    'NAME': ['example_user'],
                                    'AGE': [30]
                                }
                                mock_df = pd.DataFrame(mock_data)
                                st.dataframe(mock_df, use_container_width=True)

                            except Exception as e:
                                st.error(f"❌ Error generating SQL: {str(e)}")
                        else:
                            st.warning("Please fill in the table name and JSON column name.")
            else:  # Snowflake Database approach
                st.markdown("#### 🏔️ Snowflake Database Integration")
                
                # The connection UI
                conn_manager = render_snowflake_connection_ui()

                if conn_manager and hasattr(conn_manager, 'is_connected') and conn_manager.is_connected:
                    st.markdown("---")
                    st.subheader("📊 Database Operations")
                    render_snowflake_operations_ui(conn_manager)
                else:
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
