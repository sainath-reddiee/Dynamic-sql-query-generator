import streamlit as st
import json
import pandas as pd
from typing import Dict, Any, List, Set, Tuple
import re
from datetime import datetime
import logging
import os
from python_sql_generator import generate_sql_from_json_data

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
        
        # Input handling
        if input_method == "Upload JSON File":
            st.sidebar.markdown("### 📁 File Upload")
            uploaded_file = st.sidebar.file_uploader(
                "Choose a JSON file",
                type=['json'],
                help="Upload a JSON file to analyze its structure (max 200MB)"
            )
            
            if uploaded_file is not None:
                try:
                    # Check file size
                    if uploaded_file.size > 200 * 1024 * 1024:  # 200MB limit
                        st.sidebar.error("❌ File too large. Please upload a file smaller than 200MB.")
                        return
                    
                    json_data = json.load(uploaded_file)
                    st.sidebar.markdown(
                        f'<div class="success-box">✅ File "{uploaded_file.name}" loaded successfully!</div>', 
                        unsafe_allow_html=True
                    )
                    
                    # Show file stats
                    file_stats = f"""
                    **File Stats:**
                    - Size: {uploaded_file.size:,} bytes
                    - Type: {uploaded_file.type}
                    """
                    st.sidebar.markdown(file_stats)
                    
                except json.JSONDecodeError as e:
                    st.sidebar.error(f"❌ Invalid JSON file: {str(e)}")
                except Exception as e:
                    st.sidebar.error(f"❌ Error reading file: {str(e)}")
        
        else:  # Paste JSON Text
            st.sidebar.markdown("### ✏️ Text Input")
            json_text = st.sidebar.text_area(
                "Paste your JSON here:",
                height=200,
                help="Paste JSON text directly into this area",
                placeholder='{"example": "data", "nested": {"field": "value"}}'
            )
            
            if json_text.strip():
                is_valid, message, json_data = validate_json_input(json_text)
                if is_valid:
                    st.sidebar.markdown(
                        '<div class="success-box">✅ JSON parsed successfully!</div>', 
                        unsafe_allow_html=True
                    )
                else:
                    st.sidebar.error(f"❌ {message}")
        
        # Main content area
        if json_data is not None:
            # Analyze the JSON structure
            with st.spinner("Analyzing JSON structure..."):
                schema = analyze_json_structure(json_data)
            
            if not schema:
                st.error("❌ Failed to analyze JSON structure. Please check your data and try again.")
                return
            
            # Summary metrics
            st.markdown('<h2 class="section-header">📊 Analysis Summary</h2>', unsafe_allow_html=True)
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                total_paths = len(schema)
                st.markdown(f'<div class="metric-card"><h3>{total_paths}</h3><p>Total Paths</p></div>', unsafe_allow_html=True)
            
            with col2:
                arrays_count = len(find_arrays(schema))
                st.markdown(f'<div class="metric-card"><h3>{arrays_count}</h3><p>Arrays Found</p></div>', unsafe_allow_html=True)
            
            with col3:
                nested_count = len(find_nested_objects(schema))
                st.markdown(f'<div class="metric-card"><h3>{nested_count}</h3><p>Nested Objects</p></div>', unsafe_allow_html=True)
            
            with col4:
                queryable_count = len(find_queryable_fields(schema))
                st.markdown(f'<div class="metric-card"><h3>{queryable_count}</h3><p>Queryable Fields</p></div>', unsafe_allow_html=True)
            
            # Create tabs for different features
            tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
                "📊 Complete Paths", 
                "📋 Arrays Analysis", 
                "🏗️ Nested Objects", 
                "🔍 Queryable Fields",
                "🎨 JSON Formatter",
                "⚡ SQL Generator"
            ])
            
            with tab1:
                st.markdown('<h2 class="section-header">📊 Complete JSON Paths</h2>', unsafe_allow_html=True)
                
                if schema:
                    # Filter options
                    col1, col2 = st.columns(2)
                    with col1:
                        depth_filter = st.selectbox(
                            "Filter by depth:",
                            ["All"] + [str(i) for i in range(1, max(info['depth'] for info in schema.values()) + 1)]
                        )
                    with col2:
                        type_filter = st.selectbox(
                            "Filter by type:",
                            ["All"] + sorted(list(set(info['type'] for info in schema.values())))
                        )
                    
                    # Create a DataFrame for better display
                    paths_data = []
                    for path, info in schema.items():
                        # Apply filters
                        if depth_filter != "All" and info['depth'] != int(depth_filter):
                            continue
                        if type_filter != "All" and info['type'] != type_filter:
                            continue
                            
                        paths_data.append({
                            'Path': path,
                            'Type': info['type'],
                            'Snowflake Type': info['snowflake_type'],
                            'Depth': info['depth'],
                            'In Array': '✅' if info['is_array_item'] else '❌',
                            'Queryable': '✅' if info['is_queryable'] else '❌',
                            'Sample Value': info['sample_value']
                        })
                    
                    if paths_data:
                        df = pd.DataFrame(paths_data)
                        st.dataframe(df, use_container_width=True, height=400)
                        
                        # Download option
                        csv = df.to_csv(index=False)
                        st.download_button(
                            label="📥 Download paths as CSV",
                            data=csv,
                            file_name=f"json_paths_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
                    else:
                        st.info("No paths match the selected filters.")
                else:
                    st.warning("No paths found in the JSON structure.")
            
            with tab2:
                st.markdown('<h2 class="section-header">📋 Arrays Analysis</h2>', unsafe_allow_html=True)
                
                arrays = find_arrays(schema)
                if arrays:
                    st.markdown(f'<div class="success-box">Found {len(arrays)} array(s) in your JSON structure</div>', unsafe_allow_html=True)
                    
                    for i, array in enumerate(arrays):
                        with st.expander(f"Array {i+1}: {array['path']}", expanded=i < 3):
                            col1, col2, col3, col4 = st.columns(4)
                            
                            with col1:
                                st.metric("Depth Level", array['depth'])
                            with col2:
                                st.metric("Array Length", array['length'])
                            with col3:
                                st.write("**Item Type:**")
                                st.code(array['item_type'])
                            with col4:
                                st.write("**Parent Arrays:**")
                                if array['parent_arrays']:
                                    for parent in array['parent_arrays']:
                                        st.markdown(f'<span class="array-indicator">{parent}</span>', unsafe_allow_html=True)
                                else:
                                    st.write("Root level")
                            
                            st.markdown(f'**Full Path:** <span class="json-path">{array["path"]}</span>', unsafe_allow_html=True)
                            
                            # Show usage example
                            st.write("**SQL Usage Example:**")
                            st.code(f"LATERAL FLATTEN(input => json_column:{array['path']}) as flattened_{i+1}")
                else:
                    st.info("No arrays found in the JSON structure.")
            
            with tab3:
                st.markdown('<h2 class="section-header">🏗️ Nested Objects</h2>', unsafe_allow_html=True)
                
                nested_objects = find_nested_objects(schema)
                if nested_objects:
                    st.markdown(f'<div class="success-box">Found {len(nested_objects)} nested object(s)</div>', unsafe_allow_html=True)
                    
                    for i, obj in enumerate(nested_objects):
                        with st.expander(f"Nested Object {i+1}: {obj['path']}", expanded=i < 3):
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                st.metric("Nesting Depth", obj['depth'])
                                st.write("**In Array Context:**")
                                if obj['is_in_array']:
                                    st.markdown('<span class="array-indicator">Yes</span>', unsafe_allow_html=True)
                                else:
                                    st.markdown('<span class="nested-indicator">No</span>', unsafe_allow_html=True)
                            
                            with col2:
                                st.write("**Parent Arrays:**")
                                if obj['parent_arrays']:
                                    for parent in obj['parent_arrays']:
                                        st.markdown(f'<span class="array-indicator">{parent}</span>', unsafe_allow_html=True)
                                else:
                                    st.write("None")
                            
                            st.markdown(f'**Full Path:** <span class="json-path">{obj["path"]}</span>', unsafe_allow_html=True)
                            
                            # Show access example
                            st.write("**Access Example:**")
                            st.code(f"json_column:{obj['path']}")
                else:
                    st.info("No nested objects found in the JSON structure.")
            
            with tab4:
                st.markdown('<h2 class="section-header">🔍 Queryable Fields</h2>', unsafe_allow_html=True)
                
                queryable_fields = find_queryable_fields(schema)
                if queryable_fields:
                    st.markdown(f'<div class="success-box">Found {len(queryable_fields)} queryable field(s)</div>', unsafe_allow_html=True)
                    
                    # Filter options
                    col1, col2 = st.columns(2)
                    with col1:
                        type_filter = st.selectbox(
                            "Filter by type:",
                            ["All"] + list(set([f['type'] for f in queryable_fields])),
                            key="queryable_type_filter"
                        )
                    with col2:
                        array_filter = st.selectbox(
                            "Filter by array context:",
                            ["All", "In Arrays Only", "Not in Arrays"],
                            key="queryable_array_filter"
                        )
                    
                    # Apply filters
                    filtered_fields = queryable_fields
                    if type_filter != "All":
                        filtered_fields = [f for f in filtered_fields if f['type'] == type_filter]
                    if array_filter == "In Arrays Only":
                        filtered_fields = [f for f in filtered_fields if f['in_array']]
                    elif array_filter == "Not in Arrays":
                        filtered_fields = [f for f in filtered_fields if not f['in_array']]
                    
                    if filtered_fields:
                        # Display fields in a more compact way
                        for i, field in enumerate(filtered_fields):
                            with st.expander(f"Field {i+1}: {field['path']}", expanded=i < 5):
                                col1, col2, col3 = st.columns(3)
                                
                                with col1:
                                    st.write("**Types:**")
                                    st.code(f"Python: {field['type']}")
                                    st.code(f"Snowflake: {field['snowflake_type']}")
                                
                                with col2:
                                    st.metric("Depth", field['depth'])
                                    st.write("**In Array:**")
                                    if field['in_array']:
                                        st.markdown('<span class="array-indicator">Yes</span>', unsafe_allow_html=True)
                                    else:
                                        st.markdown('<span class="queryable-indicator">No</span>', unsafe_allow_html=True)
                                
                                with col3:
                                    st.write("**Sample Value:**")
                                    st.code(field['sample_value'])
                                
                                if field['array_context']:
                                    st.write("**Array Context:**")
                                    for ctx in field['array_context']:
                                        st.markdown(f'<span class="array-indicator">{ctx}</span>', unsafe_allow_html=True)
                                
                                # Show SQL access pattern
                                st.write("**SQL Access Pattern:**")
                                st.code(f"json_column:{field['path']} as {field['path'].split('.')[-1]}")
                    else:
                        st.info("No fields match the selected filters.")
                else:
                    st.info("No queryable fields found in the JSON structure.")
            
            with tab5:
                st.markdown('<h2 class="section-header">🎨 JSON Formatter</h2>', unsafe_allow_html=True)
                
                # Format options
                col1, col2 = st.columns(2)
                with col1:
                    indent_size = st.selectbox("Indentation:", [2, 4, 8], index=0)
                with col2:
                    sort_keys = st.checkbox("Sort keys alphabetically", value=False)
                
                # Display original JSON
                st.subheader("Original JSON:")
                original_json = json.dumps(json_data, separators=(',', ':'))
                st.text_area("Compact JSON", original_json, height=150, disabled=True, key="original_json")
                
                # Display prettified JSON
                st.subheader("Formatted JSON:")
                try:
                    prettified = json.dumps(json_data, indent=indent_size, ensure_ascii=False, sort_keys=sort_keys)
                    st.text_area("Formatted JSON", prettified, height=300, disabled=True, key="prettified_json")
                    
                    # JSON statistics
                    st.subheader("JSON Statistics:")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Original Size", f"{len(original_json):,} chars")
                    with col2:
                        st.metric("Formatted Size", f"{len(prettified):,} chars")
                    with col3:
                        compression_ratio = (1 - len(original_json) / len(prettified)) * 100
                        st.metric("Size Increase", f"{compression_ratio:.1f}%")
                    
                    # Download options
                    st.subheader("Download Options:")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.download_button(
                            label="📥 Download Compact JSON",
                            data=original_json,
                            file_name=f"compact_json_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                            mime="application/json"
                        )
                    with col2:
                        st.download_button(
                            label="📥 Download Formatted JSON",
                            data=prettified,
                            file_name=f"formatted_json_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                            mime="application/json"
                        )
                        
                except Exception as e:
                    st.error(f"Error formatting JSON: {str(e)}")
            
            with tab6:
                st.markdown('<h2 class="section-header">⚡ SQL Generator</h2>', unsafe_allow_html=True)
                
                # Procedure Parameters Section
                col1, col2 = st.columns([1, 1])
                
                with col1:
                    st.subheader("🧪 Query Parameters")
                    
                    # Input for table and column names
                    table_name = st.text_input(
                        "Table Name:",
                        placeholder="your_schema.your_table",
                        help="Full table name including schema"
                    )
                    
                    json_column_name = st.text_input(
                        "JSON Column Name:",
                        placeholder="json_data",
                        help="Name of the column containing JSON data"
                    )
                    
                    field_conditions = st.text_area(
                        "Field Conditions:",
                        height=100,
                        help="Specify fields and conditions for the dynamic SQL",
                        placeholder="e.g., name, age[>:18], status[=:active]"
                    )
                    
                    # Generate SQL button
                    generate_sql_btn = st.button("🚀 Generate SQL", type="primary")
                
                with col2:
                    st.subheader("💡 Examples Based on Your Data")
                    examples = generate_procedure_examples(schema)
                    
                    if examples:
                        st.markdown("**Quick Examples (click to use):**")
                        
                        # Extract just the field conditions from examples for easier use
                        queryable = find_queryable_fields(schema)
                        if queryable:
                            # Example 1: First 3 fields
                            example_fields_1 = ", ".join([f['path'].split('.')[-1] for f in queryable[:3]])
                            if st.button(f"📋 Basic: {example_fields_1}", key="ex1"):
                                st.session_state.field_conditions = example_fields_1
                                st.rerun()
                            
                            # Example 2: With conditions
                            if len(queryable) > 1:
                                field1 = queryable[0]['path'].split('.')[-1]
                                field2 = queryable[1]['path'].split('.')[-1]
                                example_with_conditions = f"{field1}, {field2}[IS NOT NULL]"
                                if st.button(f"📋 Filtered: {example_with_conditions}", key="ex2"):
                                    st.session_state.field_conditions = example_with_conditions
                                    st.rerun()
                            
                            # Example 3: Array fields
                            array_fields = [f for f in queryable if f['in_array']]
                            if array_fields:
                                array_example = ", ".join([f['path'].split('.')[-1] for f in array_fields[:2]])
                                if st.button(f"📋 Arrays: {array_example}", key="ex3"):
                                    st.session_state.field_conditions = array_example
                                    st.rerun()
                    
                    # Use session state for field conditions if set
                    if 'field_conditions' in st.session_state:
                        field_conditions = st.session_state.field_conditions
                        del st.session_state.field_conditions
                
                # Generate SQL when button is clicked
                if generate_sql_btn and all([table_name, json_column_name, field_conditions]):
                    try:
                        with st.spinner("🔄 Generating SQL from your JSON structure..."):
                            generated_sql = generate_sql_from_json_data(
                                json_data, table_name, json_column_name, field_conditions
                            )
                        
                        st.markdown("---")
                        st.subheader("🎯 Generated SQL Query")
                        st.code(generated_sql, language="sql")
                        
                        # Download generated SQL
                        st.download_button(
                            label="📥 Download Generated SQL",
                            data=generated_sql,
                            file_name=f"generated_sql_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql",
                            mime="text/sql",
                            key="download_generated_sql"
                        )
                        
                        # Show field analysis used
                        with st.expander("🔍 Analysis Details"):
                            st.markdown("**Fields analyzed from your JSON:**")
                            analyzed_fields = []
                            for path, details in schema.items():
                                if details.get('is_queryable', False):
                                    analyzed_fields.append({
                                        'Field': path,
                                        'Type': details['snowflake_type'],
                                        'In Array': '✅' if details.get('in_array', False) else '❌',
                                        'Sample': details.get('sample_value', 'N/A')[:50]
                                    })
                            
                            if analyzed_fields:
                                st.dataframe(pd.DataFrame(analyzed_fields), use_container_width=True)
                        
                    except Exception as e:
                        st.error(f"❌ Error generating SQL: {str(e)}")
                        st.error("Please check your field conditions and try again.")
                
                elif generate_sql_btn:
                    st.warning("⚠️ Please fill in all required fields (Table Name, JSON Column, Field Conditions)")
                
                # Show the equivalent Snowflake procedure call
                st.markdown("---")
                st.subheader("🏔️ Equivalent Snowflake Procedure Call")
                
                if all([table_name, json_column_name, field_conditions]):
                    procedure_call = f"""CALL SAINATH.SNOW.DYNAMIC_SQL_LARGE_IMPROVED(
    '{table_name}',
    '{json_column_name}',
    '{field_conditions}'
);"""
                    st.code(procedure_call, language="sql")
                    
                    st.download_button(
                        label="📥 Download Procedure Call",
                        data=procedure_call,
                        file_name=f"procedure_call_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql",
                        mime="text/sql",
                        key="download_procedure_call"
                    )
                    
                    st.info("💡 **Note:** The SQL above is generated directly from your JSON data. The procedure call shows the equivalent Snowflake stored procedure that would produce similar results.")
                else:
                    st.info("Fill in the parameters above to see both the generated SQL and equivalent procedure call.")
        
        # Instructions and help
        if json_data is not None:
            st.markdown("---")
            st.markdown("""
            <div class="feature-box">
            <h4>📚 Parameter Format Guide</h4>
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem;">
                <div>
                    <h5>Basic Syntax:</h5>
                    <ul>
                        <li><code>field_name</code> - Simple field extraction</li>
                        <li><code>field_name[operator:value]</code> - Field with condition</li>
                        <li><code>field_name[CAST:TYPE]</code> - Type casting</li>
                        <li><code>field1, field2</code> - Multiple fields</li>
                    </ul>
                </div>
                <div>
                    <h5>Advanced Usage:</h5>
                    <ul>
                        <li><code>field[=:value:OR]</code> - Custom logic operator</li>
                        <li><code>field[IN:val1|val2|val3]</code> - IN clause</li>
                        <li><code>field[BETWEEN:10|100]</code> - Range queries</li>
                        <li><code>field[LIKE:%pattern%]</code> - Pattern matching</li>
                    </ul>
                </div>
            </div>
            <p><strong>Supported operators:</strong> =, !=, >, <, >=, <=, LIKE, NOT LIKE, IN, NOT IN, BETWEEN, CONTAINS, IS NULL, IS NOT NULL</p>
            <p><strong>Supported types for casting:</strong> STRING, NUMBER, BOOLEAN, DATE, TIMESTAMP, VARIANT</p>
            </div>
            """, unsafe_allow_html=True)
            
            # Export all results
            st.markdown("---")
            st.subheader("📤 Export Analysis Results")
            
            export_results = export_analysis_results(schema)
            if export_results:
                col1, col2, col3 = st.columns(3)
                
                for i, (name, df) in enumerate(export_results.items()):
                    col_idx = i % 3
                    if col_idx == 0:
                        with col1:
                            csv_data = df.to_csv(index=False)
                            st.download_button(
                                label=f"📥 {name.replace('_', ' ').title()}",
                                data=csv_data,
                                file_name=f"{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv"
                            )
                    elif col_idx == 1:
                        with col2:
                            csv_data = df.to_csv(index=False)
                            st.download_button(
                                label=f"📥 {name.replace('_', ' ').title()}",
                                data=csv_data,
                                file_name=f"{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv"
                            )
                    else:
                        with col3:
                            csv_data = df.to_csv(index=False)
                            st.download_button(
                                label=f"📥 {name.replace('_', ' ').title()}",
                                data=csv_data,
                                file_name=f"{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv"
                            )
        else:
            # Welcome screen with enhanced design
            st.markdown("""
            <div class="feature-box">
            <h3>🧪 JSON Structure Analytics Features</h3>
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 2rem; margin-top: 1rem;">
                <div>
                    <h4>📊 Analysis Capabilities</h4>
                    <ul>
                        <li>📋 <strong>Complete Path Analysis</strong> - View all JSON paths with types and metadata</li>
                        <li>🔍 <strong>Queryable Field Detection</strong> - Identify fields suitable for SQL queries</li>
                        <li>📦 <strong>Array Structure Analysis</strong> - Find arrays requiring LATERAL FLATTEN</li>
                        <li>🏗️ <strong>Nested Object Mapping</strong> - Understand complex data hierarchies</li>
                    </ul>
                </div>
                <div>
                    <h4>⚡ Utility Features</h4>
                    <ul>
                        <li>🎨 <strong>JSON Formatter</strong> - Beautify and format JSON with custom options</li>
                        <li>🧪 <strong>SQL Preview Generator</strong> - Generate SQL previews from your parameters</li>
                        <li>📥 <strong>Export Options</strong> - Download analysis results in CSV format</li>
                        <li>🏔️ <strong>Snowflake Integration</strong> - Ready-to-use procedure calls</li>
                    </ul>
                </div>
            </div>
            </div>
            
            <div class="feature-box">
            <h3>🚀 Getting Started</h3>
            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 1rem; margin-top: 1rem;">
                <div style="text-align: center; padding: 1rem; background: #f8f9fa; border-radius: 0.5rem;">
                    <h4>1️⃣ Input Data</h4>
                    <p>Upload a JSON file or paste JSON text using the sidebar options</p>
                </div>
                <div style="text-align: center; padding: 1rem; background: #f8f9fa; border-radius: 0.5rem;">
                    <h4>2️⃣ Analyze</h4>
                    <p>Explore the different tabs to understand your JSON structure</p>
                </div>
                <div style="text-align: center; padding: 1rem; background: #f8f9fa; border-radius: 0.5rem;">
                    <h4>3️⃣ Generate SQL</h4>
                    <p>Use the SQL Generator tab to create Snowflake queries</p>
                </div>
                <div style="text-align: center; padding: 1rem; background: #f8f9fa; border-radius: 0.5rem;">
                    <h4>4️⃣ Export</h4>
                    <p>Download analysis results and SQL code for your projects</p>
                </div>
            </div>
            </div>
            
            <div style="text-align: center; margin: 2rem 0;">
                <h4 style="color: #1f77b4;">👆 Choose an input method from the sidebar to get started!</h4>
            </div>
            """, unsafe_allow_html=True)
        
        # Footer
        st.markdown("""
        <div class="footer">
            <p>Built with ❤️ using Streamlit | Designed for Snowflake JSON Analysis</p>
            <p>💡 Pro Tip: For large JSON files, consider sampling your data first to improve performance</p>
        </div>
        """, unsafe_allow_html=True)
        
    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        st.error(f"❌ Application Error: {str(e)}")
        st.error("Please refresh the page and try again.")

if __name__ == "__main__":
    main()
