import streamlit as st
import json
import pandas as pd
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import logging
import os
import random

try:
    from python_sql_generator import generate_sql_from_json_data
except ImportError:
    st.error("❌ Missing python_sql_generator module")
    st.stop()

try:
    from unified_snowflake_connector import (
        UnifiedSnowflakeConnector,
        render_unified_connection_ui,
        render_performance_info,
        render_performance_metrics,
        MODIN_AVAILABLE,
        SNOWFLAKE_AVAILABLE
    )
except ImportError:
    st.warning("⚠️ Unified Snowflake connector not available - database features will be limited")
    UnifiedSnowflakeConnector = None
    render_unified_connection_ui = None
    render_performance_info = lambda: st.info("Performance info not available")
    render_performance_metrics = lambda x: None
    MODIN_AVAILABLE = False
    SNOWFLAKE_AVAILABLE = False

try:
    from universal_db_analyzer import (
        generate_database_driven_sql,
        analyze_database_json_schema_universal,
        render_enhanced_database_json_preview,
        test_database_connectivity,
        render_multi_level_helper_ui,
        render_enhanced_field_suggestions
    )
except ImportError:
    st.warning("⚠️ Universal DB analyzer not available - some database features will be limited")
    generate_database_driven_sql = None
    analyze_database_json_schema_universal = None
    render_enhanced_database_json_preview = lambda x, y: None
    test_database_connectivity = lambda x: (False, "Test not available")
    render_multi_level_helper_ui = lambda x, y: None
    render_enhanced_field_suggestions = lambda x, y: []

try:
    from json_analyzer import analyze_json_structure
    from utils import (
        find_arrays, find_nested_objects,
        find_queryable_fields, prettify_json, validate_json_input,
        export_analysis_results
    )
    from sql_generator import generate_procedure_examples, generate_sql_preview
    from config import config
except ImportError as e:
    st.warning(f"⚠️ Some utility modules not available: {e}")
    class Config:
        APP_NAME = "JSON-to-SQL Analyzer & Generator"
    config = Config()

logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="JSON-to-SQL Analyzer & Generator 🚀",
    page_icon="❄️",
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
    .execution-mode-box {
        background: linear-gradient(145deg, #e3f2fd, #f8f9ff);
        padding: 1.5rem;
        border-radius: 8px;
        border: 2px solid #64b5f6;
        margin: 0.5rem 0;
    }
    .field-counter-box {
        background: linear-gradient(145deg, #e8f5e8, #f0f8ff);
        padding: 1rem;
        border-radius: 6px;
        border: 1px solid #81c784;
        margin: 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

def get_json_data_from_sidebar() -> Optional[Dict]:
    st.sidebar.markdown("## 📁 JSON Data Input")
    
    input_method = st.sidebar.radio(
        "Choose input method:",
        ["📝 Paste JSON", "📁 Upload File"],
        key="json_input_method"
    )
    
    json_data = None
    
    if input_method == "📝 Paste JSON":
        # Check if we should show prettified version
        display_text = ""
        if st.session_state.get('show_prettified', False):
            display_text = st.session_state.get('prettified_json', '')
            # Reset the flag
            st.session_state.show_prettified = False
        
        json_text = st.sidebar.text_area(
            "Paste your JSON data:",
            value=display_text,  # Use the prettified text if available
            height=200,
            placeholder='{\n  "name": "John Doe",\n  "age": 30,\n  "email": "john@example.com"\n}',
            key="json_input_text"
        )
        
        if st.sidebar.button("🎨 Prettify JSON"):
            if json_text.strip():
                try:
                    parsed_json = json.loads(json_text)
                    pretty_json = json.dumps(parsed_json, indent=4)
                    # FIXED: Use a different session state key
                    st.session_state.prettified_json = pretty_json
                    st.session_state.show_prettified = True
                    st.rerun()
                except json.JSONDecodeError:
                    st.sidebar.warning("⚠️ Invalid JSON. Cannot prettify.")
        
        if json_text.strip():
            try:
                json_data = json.loads(json_text)
                st.sidebar.success("✅ Valid JSON loaded")
            except json.JSONDecodeError as e:
                st.sidebar.error(f"❌ Invalid JSON: {e}")
                return None
    
    elif input_method == "📁 Upload File":
        uploaded_file = st.sidebar.file_uploader(
            "Choose a JSON file:",
            type=['json'],
            key="json_file_upload"
        )
        
        if uploaded_file is not None:
            try:
                json_data = json.load(uploaded_file)
                st.sidebar.success("✅ JSON file loaded successfully")
            except json.JSONDecodeError as e:
                st.sidebar.error(f"❌ Invalid JSON file: {e}")
                return None
            except Exception as e:
                st.sidebar.error(f"❌ Error reading file: {e}")
                return None
    
    if json_data:
        with st.sidebar.expander("👀 JSON Preview", expanded=False):
            st.json(json_data, expanded=False)
        st.session_state['json_data'] = json_data
    
    return json_data

def safe_get_session_state(key: str, default: Any = None) -> Any:
    try:
        return st.session_state.get(key, default)
    except Exception:
        return default


def parse_field_conditions_enhanced(field_conditions: str) -> List[str]:
    if not field_conditions or not field_conditions.strip():
        return []
    try:
        st.info(f"🔍 **Debug: Parsing input:** `{field_conditions}`")
        raw_fields = [f.strip() for f in field_conditions.split(',') if f.strip()]
        st.info(f"📋 **Debug: Raw fields after split:** {raw_fields}")
        parsed_fields = []
        for i, field in enumerate(raw_fields):
            if field:  # Only add non-empty fields
                parsed_fields.append(field)
                st.info(f"✅ **Field {i+1}:** `{field}`")
        st.success(f"🎯 **Total fields parsed: {len(parsed_fields)}** - {parsed_fields}")
        
        return parsed_fields
        
    except Exception as e:
        st.error(f"❌ Field parsing error: {str(e)}")
        return []


def count_expected_columns_from_conditions(field_conditions: str, temp_schema: Dict = None, disambiguation_info: Dict = None) -> int:
    try:
        parsed_fields = parse_field_conditions_enhanced(field_conditions)
        total_expected_columns = 0
        
        st.markdown("#### 🔢 Expected Column Count Analysis")
        
        for field in parsed_fields:
            field_name = field.split('[')[0].strip()  # Remove conditions like [IS NOT NULL]
            simple_name = field_name.split('.')[-1]  # Get simple field name
            
            if disambiguation_info and simple_name in disambiguation_info:
                field_data = disambiguation_info[simple_name]
                field_count = len(field_data['paths'])
                total_expected_columns += field_count
                st.info(f"🎯 **{field_name}** → **{field_count} columns** (multi-level field)")
                for path_info in field_data['paths']:
                    st.caption(f"   - `{path_info['alias']}` from `{path_info['full_path']}`")
            else:
                total_expected_columns += 1
                st.info(f"📍 **{field_name}** → **1 column** (single location)")
        
        st.success(f"🎯 **Total Expected Columns: {total_expected_columns}**")
        return total_expected_columns
        
    except Exception as e:
        st.error(f"❌ Column count analysis failed: {str(e)}")
        return 0


def generate_export_content(sql, export_format, table_name, field_conditions=None):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    if export_format == "SQL File":
        return f"""-- Generated SQL Query for JSON Analysis
-- Table: {table_name}
-- Fields: {field_conditions or 'N/A'}
-- Generated: {timestamp}

{sql}
"""
    
    elif export_format == "dbt Model":
        # Extract table parts for proper dbt structure
        table_parts = table_name.split('.')
        if len(table_parts) >= 3:
            database_name = table_parts[0]
            schema_name = table_parts[1] 
            table_name_only = table_parts[2]
        elif len(table_parts) == 2:
            database_name = "your_database"  # Default placeholder
            schema_name = table_parts[0]
            table_name_only = table_parts[1]
        else:
            database_name = "your_database"
            schema_name = "your_schema"
            table_name_only = table_parts[0]
        
        model_name = table_name_only.lower().replace('-', '_').replace(' ', '_')
        source_name = f"{schema_name.lower()}_tables"
        
        # Convert raw SQL to use dbt source() function
        dbt_sql = sql.replace(f"FROM {table_name}", f"FROM {{{{ source('{source_name}', '{table_name_only}') }}}}")
        if table_name not in dbt_sql:  # If full table name wasn't found, try just table name
            dbt_sql = dbt_sql.replace(f"FROM {table_name_only}", f"FROM {{{{ source('{source_name}', '{table_name_only}') }}}}")
        
        # Generate the complete dbt model with YAML
        model_sql = f"""{{{{
  config(
    materialized='view',
    description='JSON analysis model for {table_name}',
    tags=['json_analysis', 'generated']
  )
}}}}

-- dbt model: {model_name}
-- Generated from JSON analysis
-- Source table: {table_name}
-- Fields analyzed: {field_conditions or 'N/A'}
-- Generated: {timestamp}

{dbt_sql}
"""
        
        # Generate accompanying schema.yml
        schema_yml = f"""# schema.yml - Add this to your dbt project
version: 2

sources:
  - name: {source_name}
    description: "Source tables from {schema_name} schema"
    database: {database_name}
    schema: {schema_name}
    tables:
      - name: {table_name_only}
        description: "JSON data table for analysis"
        columns:
          - name: json_data
            description: "JSON column containing structured data"
            tests:
              - not_null

models:
  - name: {model_name}
    description: "Analyzed JSON fields from {table_name}"
    columns:"""
        
        # Add column documentation based on field conditions
        if field_conditions:
            fields = [f.strip().split('[')[0] for f in field_conditions.split(',') if f.strip()]
            for field in fields[:10]:  # Limit to avoid too long YAML
                clean_field = field.replace('.', '_').lower()
                schema_yml += f"""
      - name: {clean_field}
        description: "Extracted from JSON path: {field}" """
        
        # Combine model and schema YAML
        complete_dbt_export = f"""-- ==============================================
-- dbt MODEL FILE: models/{model_name}.sql
-- ==============================================

{model_sql}

-- ==============================================
-- SCHEMA CONFIG: Add to your schema.yml file
-- ==============================================

{schema_yml}

-- ==============================================
-- INSTALLATION INSTRUCTIONS
-- ==============================================

-- 1. Save the model SQL above as 'models/{model_name}.sql'
-- 2. Add the schema YAML configuration to your schema.yml file
-- 3. Run: dbt run --models {model_name}
-- 4. Test: dbt test --models {model_name}

-- OPTIONAL: Create staging model first
-- models/staging/stg_{model_name}.sql:
--
-- {{ {{ config(materialized='view') }} }}
--
-- SELECT * FROM {{ {{ source('{source_name}', '{table_name_only}') }} }}
-- WHERE json_data IS NOT NULL
"""
        
        return complete_dbt_export
    
    elif export_format == "Jupyter Notebook":
        # Enhanced Jupyter notebook with proper structure
        notebook_content = {
            "cells": [
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": [
                        f"# JSON-to-SQL Analysis Notebook\n",
                        f"**Generated:** {timestamp}\n",
                        f"**Table:** {table_name}\n",
                        f"**Fields:** {field_conditions or 'N/A'}\n\n",
                        "This notebook contains the generated SQL query for analyzing JSON data."
                    ]
                },
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": [
                        "# Import required libraries\n",
                        "import pandas as pd\n",
                        "import snowflake.connector\n",
                        "from sqlalchemy import create_engine\n",
                        "\n",
                        "# Snowflake connection parameters\n",
                        "connection_params = {\n",
                        "    'account': 'your_account',\n",
                        "    'user': 'your_username',\n",
                        "    'password': 'your_password',\n",
                        "    'database': 'your_database',\n",
                        "    'schema': 'your_schema',\n",
                        "    'warehouse': 'your_warehouse'\n",
                        "}"
                    ]
                },
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": [
                        f"# Generated SQL Query\n",
                        f"sql_query = '''\n{sql}\n'''\n\n",
                        "print('Generated SQL Query:')\n",
                        "print(sql_query)"
                    ]
                },
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": [
                        "# Execute query and get results\n",
                        "# conn = snowflake.connector.connect(**connection_params)\n",
                        "# df = pd.read_sql(sql_query, conn)\n",
                        "# print(f'Results shape: {df.shape}')\n",
                        "# df.head()"
                    ]
                }
            ],
            "metadata": {
                "kernelspec": {
                    "display_name": "Python 3",
                    "language": "python",
                    "name": "python3"
                },
                "language_info": {
                    "name": "python",
                    "version": "3.8.0"
                }
            },
            "nbformat": 4,
            "nbformat_minor": 4
        }
        return json.dumps(notebook_content, indent=2)

    else:
        return f"# Unknown export format: {export_format}\n{sql}"

def get_file_extension(export_format):
    extensions = {
        "SQL File": "sql", 
        "dbt Model": "sql",
        "Jupyter Notebook": "ipynb"
    }
    return extensions.get(export_format, "txt")

def get_mime_type(export_format):
    mime_types = { 
        "SQL File": "text/sql", 
        "dbt Model": "text/sql", 
        "Jupyter Notebook": "application/json" 
    }
    return mime_types.get(export_format, "text/plain")


def render_enhanced_disambiguation_info(json_data):
    try:
        from python_sql_generator import PythonSQLGenerator
        temp_generator = PythonSQLGenerator()
        temp_schema = temp_generator.analyze_json_for_sql(json_data)
        disambiguation_info = temp_generator.get_multi_level_field_info()

        if disambiguation_info:
            st.markdown("#### 🚨 Field Name Conflicts Detected")
            conflict_summary = []
            for field_name, conflict_data in disambiguation_info.items():
                paths = conflict_data['paths']
                queryable_options = [opt for opt in paths if opt['schema_entry']['is_queryable']]
                conflict_summary.append({ 
                    'Field Name': field_name, 
                    'Conflict Count': conflict_data['total_occurrences'], 
                    'Queryable Options': len(queryable_options), 
                    'Paths': ' | '.join([opt['full_path'] for opt in queryable_options[:3]]) 
                })
            
            if conflict_summary:
                st.warning(f"⚠️ Found {len(conflict_summary)} field names with multiple locations")
                with st.expander("🔍 View Conflict Details", expanded=False):
                    st.dataframe(pd.DataFrame(conflict_summary), use_container_width=True)
                    st.markdown("**💡 How disambiguation works:**")
                    st.markdown("""- Specify the full path (e.g., `company.name`) to be explicit.""")
        else:
            st.success("✅ No field name conflicts detected.")
        
        return temp_schema, disambiguation_info
    except Exception as e:
        st.warning(f"Could not analyze disambiguation info: {e}")
        return {}, {}


def render_enhanced_python_field_suggestions(temp_schema, disambiguation_info):
    if temp_schema:
        with st.expander("💡 Smart Field Suggestions (Click to Use)", expanded=True):
            queryable_fields_list = []
            for path, details in temp_schema.items():
                if details.get('is_queryable', False):
                    queryable_fields_list.append({
                        'Field Path': path, 
                        'Type': details.get('snowflake_type', 'VARIANT'), 
                        'Sample': str(details.get('sample_value', ''))[:50]
                    })
            
            suggestion_cols = st.columns(2)
            for i, field in enumerate(queryable_fields_list[:12]):
                with suggestion_cols[i % 2]:
                    if st.button(f"➕ {field['Field Path']}", key=f"use_field_{field['Field Path']}_{i}", help=f"Type: {field['Type']} | Sample: {field['Sample']}", type="secondary"):
                        current_conditions = st.session_state.get('py_fields', '').strip()
                        st.session_state.py_fields = f"{current_conditions}, {field['Field Path']}" if current_conditions else field['Field Path']
                        st.rerun()
            
            if len(queryable_fields_list) > 12:
                st.caption(f"... and {len(queryable_fields_list) - 12} more fields available")


def generate_enhanced_sql_python_mode(json_data, table_name, json_column, field_conditions):
    try:
        from python_sql_generator import generate_sql_from_json_data_with_warnings
        sql, warnings, disambiguation_details = generate_sql_from_json_data_with_warnings(json_data, table_name, json_column, field_conditions)
        return sql, warnings, disambiguation_details
    except ImportError:
        try:
            sql = generate_sql_from_json_data(json_data, table_name, json_column, field_conditions)
            
            select_count = sql.count("json_data:$")
            st.info(f"🔍 **Debug: Generated SQL has {select_count} field extractions**")
            
            return sql, [], {}
        except Exception as e:
            return f"-- Error: {str(e)}", [f"❌ Generation error: {str(e)}"], {}
    except Exception as e:
        return f"-- Error: {str(e)}", [f"❌ Generation error: {str(e)}"], {}


def render_disambiguation_details(sql, warnings, field_conditions, disambiguation_details):
    if warnings and any("Auto-resolved" in w or "ambiguous" in w or "Multi-level" in w for w in warnings):
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


def render_enhanced_snowflake_ui(conn_manager):
    if not conn_manager:
        st.error("❌ Connection manager not available")
        return

    # Mode display
    mode_text = "Enhanced" if hasattr(conn_manager, 'enhanced_mode') and conn_manager.enhanced_mode else "Standard"
    mode_color = "#2e7d32" if hasattr(conn_manager, 'enhanced_mode') and conn_manager.enhanced_mode else "#1976d2"
    mode_icon = "⚡" if hasattr(conn_manager, 'enhanced_mode') and conn_manager.enhanced_mode else "🏔️"

    st.markdown(f"""
    <div class="mode-selector">
        <h5 style="color: {mode_color}; margin-bottom: 0.5rem;">{mode_icon} Currently in {mode_text} Mode</h5>
        <p style="margin-bottom: 0; font-size: 0.9rem;">
            {'🛡️ Session context management + 🚀 Modin acceleration + 📊 Performance tracking' if hasattr(conn_manager, 'enhanced_mode') and conn_manager.enhanced_mode else '📊 Basic connectivity with standard pandas processing'}
        </p>
    </div>
    """, unsafe_allow_html=True)

    # Create tabs similar to Python mode structure
    snowflake_tab1, snowflake_tab2, snowflake_tab3 = st.tabs([
        "🧪 **Smart JSON Analysis (Enhanced)**", 
        "📊 **Custom SQL Execution**", 
        "🔧 **Connection Management**"
    ])

    with snowflake_tab1:
        render_smart_json_analysis_ui(conn_manager)

    with snowflake_tab2:
        render_custom_sql_execution_ui(conn_manager)

    with snowflake_tab3:
        render_connection_management_ui(conn_manager)


def render_smart_json_analysis_ui(conn_manager):
    st.markdown("### 🧪 Smart JSON Analysis with Live Database")
    
    if hasattr(conn_manager, 'enhanced_mode') and conn_manager.enhanced_mode:
        st.markdown("""
        <div class="enhanced-box">
            <h5 style="color: #2e7d32;">🎯 Enhanced Database Features Active:</h5>
            <ul style="color: #1b5e20; margin-bottom: 0;">
                <li><strong>✅ Live database sampling</strong> - Real-time JSON schema analysis</li>
                <li><strong>🚀 Modin acceleration</strong> for large datasets</li>
                <li><strong>📊 Real-time performance tracking</strong> during analysis</li>
                <li><strong>⚠️ Field disambiguation support</strong> - Handles duplicate field names</li>
                <li><strong>💡 Intelligent field suggestions</strong> based on your actual data</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div class="feature-box">
            <h5 style="color: #1976d2;">🏔️ Standard Database Features Active:</h5>
            <ul style="color: #0d47a1; margin-bottom: 0;">
                <li><strong>📊 Live database connectivity</strong></li>
                <li><strong>🔧 Standard pandas processing</strong></li>
                <li><strong>⚠️ Field disambiguation support</strong> - Handles duplicate field names</li>
                <li><strong>💡 Smart field suggestions</strong> based on actual data structure</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("### 📝 Database Query Configuration")
    input_col1, input_col2 = st.columns(2)
    
    with input_col1:
        table_name = st.text_input(
            "Table Name* 🏗️",
            placeholder="SCHEMA.TABLE or just TABLE_NAME",
            key="sf_table_name",
            help="Can be just table name, schema.table, or database.schema.table"
        )

        field_conditions = st.text_area(
            "Field Conditions* 🎯",
            height=120,
            placeholder="e.g., name, company.name, departments.employees.name[IS NOT NULL]",
            key="sf_field_conditions",
            help="Specify JSON fields and their filtering conditions. Use full paths to avoid ambiguity."
        )

    with input_col2:
        json_column = st.text_input(
            "JSON Column Name* 📄",
            placeholder="json_data",
            key="sf_json_column",
            help="Name of the column containing JSON data"
        )

        execution_mode = st.radio(
            "Choose Action:", 
            ["🔍 Analyze Schema Only", "🚀 Analyze & Execute", "📋 Export Generated SQL"], 
            key="sf_execution_mode",
            help="Choose how to handle the database analysis"
        )

    # Field parsing debug for Snowflake mode
    if field_conditions:
        with st.expander("🔍 Field Parsing Debug (Snowflake Mode)", expanded=True):
            parsed_fields = parse_field_conditions_enhanced(field_conditions)
            st.markdown(f"""
            <div class="field-counter-box">
                <h6 style="color: #2e7d32; margin-bottom: 0.5rem;">📊 Field Analysis Summary</h6>
                <p style="margin-bottom: 0; font-size: 0.9rem;">
                    <strong>Input:</strong> {field_conditions}<br/>
                    <strong>Parsed Fields:</strong> {len(parsed_fields)} fields detected<br/>
                    <strong>Fields:</strong> {', '.join([f'`{f}`' for f in parsed_fields])}
                </p>
            </div>
            """, unsafe_allow_html=True)

    # Sample size configuration
    col_config1, col_config2 = st.columns(2)
    with col_config1:
        sample_size = st.selectbox(
            "Analysis Sample Size 📊",
            [5, 10, 20, 50],
            index=1,
            key="sf_sample_size",
            help="Larger samples give better schema analysis but take longer"
        )
    
    with col_config2:
        show_preview = st.checkbox(
            "Show Detailed Schema Preview 👀",
            value=True,
            key="sf_show_preview",
            help="Display comprehensive analysis of discovered JSON fields with disambiguation info"
        )

    # Smart suggestions section (similar to Python mode)
    if 'discovered_schema_sf' in st.session_state:
        render_snowflake_field_suggestions()

    # Enhanced "Action Buttons" similar to Python mode
    st.markdown("### 🚀 Analysis & Execution")
    mode_display = execution_mode.split(' ', 1)[1] if ' ' in execution_mode else execution_mode
    
    action_col1, action_col2 = st.columns([1, 2])
    with action_col1:
        execute_btn = st.button(f"🚀 {mode_display}", type="primary", use_container_width=True)
    with action_col2:
        st.markdown(f"""<div class="execution-mode-box" style="text-align:center;"><h6 style="margin-bottom: 0rem; color: #1976d2;">🎯 Current Mode: {mode_display}</h6></div>""", unsafe_allow_html=True)

    if execute_btn:
        if not all([table_name, json_column, field_conditions]):
            st.error("❌ Please fill in all required fields marked with *.")
        else:
            execute_snowflake_analysis(conn_manager, table_name, json_column, field_conditions, sample_size, execution_mode, show_preview)


def render_snowflake_field_suggestions():
    with st.expander("💡 Smart Field Suggestions (Database-Aware)", expanded=False):
        try:
            schema = st.session_state.discovered_schema_sf
            metadata = st.session_state.get('schema_metadata_sf', {})
            disambiguation_info = metadata.get('disambiguation_info', {})

            if render_enhanced_field_suggestions:
                suggestions = render_enhanced_field_suggestions(schema, disambiguation_info)

                if suggestions:
                    st.markdown("**🎯 Smart suggestions based on your database JSON structure:**")

                    # Show disambiguation warning if conflicts exist
                    if disambiguation_info:
                        st.markdown("""
                        <div class="disambiguation-alert">
                            <strong>⚠️ Database Field Disambiguation Active:</strong> Detected field name conflicts in your database.
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
                                if st.button(f"Use: `{suggestion.split('#')[0].strip()}`", key=f"use_sf_suggestion_{suggestion_count}"):
                                    current_conditions = st.session_state.get('sf_field_conditions', '').strip()
                                    clean_suggestion = suggestion.split('#')[0].strip()
                                    new_condition = f"{current_conditions}, {clean_suggestion}" if current_conditions else clean_suggestion
                                    st.session_state.sf_field_conditions = new_condition
                                    st.rerun()
                                st.code(suggestion, language="text")
                            suggestion_count += 1
                else:
                    st.info("No specific suggestions available for this database schema.")
            else:
                st.info("Enhanced field suggestions not available.")

        except Exception as e:
            st.warning(f"Could not generate database suggestions: {e}")


def execute_snowflake_analysis(conn_manager, table_name, json_column, field_conditions, sample_size, execution_mode, show_preview):
    try:
        if execution_mode == "🔍 Analyze Schema Only":
            with st.spinner(f"🔄 Database schema analysis with field disambiguation..."):
                if analyze_database_json_schema_universal:
                    schema, error, metadata = analyze_database_json_schema_universal(
                        conn_manager, table_name, json_column, sample_size
                    )
                else:
                    schema, error, metadata = None, "Function not available", {}

                if schema:
                    st.success(f"✅ Database schema analysis complete! Found {len(schema)} fields.")

                    st.session_state.discovered_schema_sf = schema
                    st.session_state.schema_metadata_sf = metadata

                    # Show disambiguation summary
                    disambiguation_info = metadata.get('disambiguation_info', {})
                    if disambiguation_info:
                        st.info(f"🚨 Found {len(disambiguation_info)} field names with multiple locations in your database. Check the detailed preview for disambiguation options.")

                    # Count expected columns for this analysis
                    count_expected_columns_from_conditions(field_conditions, schema, disambiguation_info)

                    if show_preview and render_enhanced_database_json_preview:
                        render_enhanced_database_json_preview(schema, metadata)
                else:
                    st.error(f"❌ Database schema analysis failed: {error}")

        elif execution_mode == "🚀 Analyze & Execute":
            with st.spinner("⚡ Generating and executing database-driven SQL..."):
                if generate_database_driven_sql:
                    generated_sql, sql_error = generate_database_driven_sql(
                        conn_manager, table_name, json_column, field_conditions
                    )
                else:
                    generated_sql, sql_error = None, "Function not available"

                if generated_sql and not sql_error:
                    st.success("✅ Database SQL Generated Successfully with Disambiguation Support!")
                    st.code(generated_sql, language="sql")

                    # Count SELECT clauses in generated SQL for debugging
                    select_count = generated_sql.count("json_data:$") + generated_sql.count("value:")
                    st.info(f"🔍 **Debug: Generated SQL has {select_count} field extractions**")

                    # Execute the generated SQL
                    if hasattr(conn_manager, 'enhanced_mode') and conn_manager.enhanced_mode:
                        with st.spinner("⚡ Executing with performance monitoring..."):
                            if hasattr(conn_manager, 'execute_query_with_performance'):
                                result_df, exec_error, perf_stats = conn_manager.execute_query_with_performance(generated_sql)
                            else:
                                result_df, exec_error = conn_manager.execute_query(generated_sql)
                                perf_stats = {}

                            if result_df is not None:
                                st.success("✅ Database query executed with enhanced performance monitoring!")
                                if perf_stats and render_performance_metrics:
                                    render_performance_metrics(perf_stats)

                                # Display results with metrics
                                col_sum1, col_sum2, col_sum3, col_sum4 = st.columns(4)
                                with col_sum1: 
                                    st.metric("Rows Returned", len(result_df))
                                with col_sum2: 
                                    st.metric("Columns Generated", len(result_df.columns))
                                with col_sum3:
                                    processing_engine = "🚀 Modin" if perf_stats.get('modin_used', False) else "📊 Pandas"
                                    st.metric("Processing Engine", processing_engine)
                                with col_sum4:
                                    aliases_used = [col for col in result_df.columns if '_' in col and not col.startswith('_')]
                                    disambiguation_used = "✅ Applied" if len(aliases_used) > 0 else "➖ Not Needed"
                                    st.metric("Disambiguation", disambiguation_used)

                                st.dataframe(result_df, use_container_width=True)

                                if not result_df.empty:
                                    csv_data = result_df.to_csv(index=False).encode('utf-8')
                                    filename = f"database_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                                    st.download_button("📥 Download Database Results", data=csv_data, file_name=filename, mime="text/csv")

                                st.info(f"⚡ **Database Performance Summary:** Processed {len(result_df):,} rows in {perf_stats.get('total_time', 0):.2f}s using {processing_engine}")
                            else:
                                st.error(f"❌ Database query execution failed: {exec_error}")
                    else:
                        # Standard mode execution
                        with st.spinner("🔄 Executing database query in standard mode..."):
                            result_df, exec_error = conn_manager.execute_query(generated_sql)
                            if result_df is not None:
                                st.success("✅ Database query executed successfully!")
                                
                                col_sum1, col_sum2, col_sum3 = st.columns(3)
                                with col_sum1: 
                                    st.metric("Rows Returned", len(result_df))
                                with col_sum2: 
                                    st.metric("Columns Generated", len(result_df.columns))
                                with col_sum3:
                                    aliases_used = [col for col in result_df.columns if '_' in col and not col.startswith('_')]
                                    disambiguation_used = "✅ Applied" if len(aliases_used) > 0 else "➖ Not Needed"
                                    st.metric("Disambiguation", disambiguation_used)
                                
                                st.dataframe(result_df, use_container_width=True)
                                if not result_df.empty: 
                                    st.download_button("📥 Download Results", data=result_df.to_csv(index=False).encode('utf-8'), file_name=f"database_results.csv", mime="text/csv")
                            else: 
                                st.error(f"❌ Database query execution failed: {exec_error}")
                else: 
                    st.error(f"❌ Database SQL Generation Error: {sql_error}")

        elif execution_mode == "📋 Export Generated SQL":
            with st.spinner("📋 Generating SQL for export..."):
                if generate_database_driven_sql:
                    generated_sql, sql_error = generate_database_driven_sql(
                        conn_manager, table_name, json_column, field_conditions
                    )
                else:
                    generated_sql, sql_error = None, "Function not available"

                if generated_sql and not sql_error:
                    st.success("✅ Database SQL Generated for Export!")
                    
                    # Export format selection
                    export_format = st.selectbox(
                        "Choose Export Format:", 
                        ["SQL File", "Python Script", "dbt Model", "Jupyter Notebook", "PowerBI Template"], 
                        key="sf_export_format"
                    )
                    
                    export_content = generate_export_content(generated_sql, export_format, table_name, field_conditions)
                    
                    with st.expander("👀 Export Content Preview", expanded=True):
                        st.code(export_content, language="sql" if "sql" in export_format.lower() else "python")
                    
                    st.download_button(
                        f"📥 Download {export_format}", 
                        data=export_content, 
                        file_name=f"database_export.{get_file_extension(export_format)}", 
                        mime=get_mime_type(export_format)
                    )
                else:
                    st.error(f"❌ Database SQL Generation Error: {sql_error}")

    except Exception as e:
        st.error(f"❌ Database analysis failed: {str(e)}")
        st.info("💡 Try checking your table name, column name, database permissions, and field disambiguation.")


def render_custom_sql_execution_ui(conn_manager):
    st.markdown("### 📊 Custom SQL Execution")
    st.markdown("""
    <div class="feature-box">
        <p>Execute any custom SQL query directly. Perfect for:</p>
        <ul>
            <li><strong>📋 Exploring tables:</strong> <code>SHOW TABLES</code> or <code>SELECT * FROM INFORMATION_SCHEMA.TABLES</code></li>
            <li><strong>🔍 Describing structure:</strong> <code>DESCRIBE TABLE your_table</code></li>
            <li><strong>📊 Testing queries:</strong> <code>SELECT * FROM your_table LIMIT 10</code></li>
        </ul>
    </div>
    """, unsafe_allow_html=True)

    # Initialize session state if not exists
    if "last_custom_sql" not in st.session_state:
        st.session_state.last_custom_sql = ""

    # Quick example buttons
    st.markdown("#### 💡 Quick SQL Examples:")
    col_ex1, col_ex2, col_ex3 = st.columns(3)
    
    example_sql = None
    
    with col_ex1:
        if st.button("📋 Show Tables", help="List all tables", key="sf_show_tables"):
            example_sql = "SHOW TABLES;"
    
    with col_ex2:
        if st.button("🏗️ Table Schema", help="Get table information", key="sf_table_schema"):
            example_sql = """SELECT
    TABLE_CATALOG as DATABASE_NAME,
    TABLE_SCHEMA as SCHEMA_NAME,
    TABLE_NAME,
    TABLE_TYPE
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA'
ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
LIMIT 20;"""

    with col_ex3:
        if st.button("📊 Sample Data", help="Sample from a table", key="sf_sample_data"):
            example_sql = """-- Replace 'your_table' with actual table name
SELECT * FROM your_table LIMIT 10;"""

    # Text area for custom SQL
    initial_value = example_sql if example_sql else st.session_state.last_custom_sql
    
    custom_sql = st.text_area(
        f"Execute Custom SQL:",
        value=initial_value,
        height=150,
        placeholder="""-- Quick examples to try:
SHOW TABLES;
-- or --
SELECT * FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA'
LIMIT 10;""",
        key="sf_custom_sql_input"
    )
    
    # Store the current SQL for next time
    if custom_sql:
        st.session_state.last_custom_sql = custom_sql

    col_sql1, col_sql2 = st.columns(2)

    with col_sql1:
        execute_sql_btn = st.button("▶️ Execute Custom SQL", type="primary", key="sf_execute_custom")

        if execute_sql_btn and custom_sql.strip():
            try:
                mode_text = "Enhanced" if hasattr(conn_manager, 'enhanced_mode') and conn_manager.enhanced_mode else "Standard"
                
                if hasattr(conn_manager, 'enhanced_mode') and conn_manager.enhanced_mode:
                    # Enhanced mode with performance tracking
                    with st.spinner("⚡ Executing with performance monitoring..."):
                        if hasattr(conn_manager, 'execute_query_with_performance'):
                            result_df, error, perf_stats = conn_manager.execute_query_with_performance(custom_sql)
                        else:
                            result_df, error = conn_manager.execute_query(custom_sql)
                            perf_stats = {}

                        if result_df is not None:
                            st.success("✅ Custom SQL executed with performance tracking!")
                            if perf_stats and render_performance_metrics:
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
            except Exception as e:
                st.error(f"❌ Error executing SQL: {str(e)}")

        elif execute_sql_btn:
            st.warning("⚠️ Please enter a SQL query")

    with col_sql2:
        if st.button("📋 List Available Tables", type="secondary", key="sf_list_tables"):
            try:
                with st.spinner("🔄 Retrieving tables..."):
                    if hasattr(conn_manager, 'list_tables'):
                        tables_df, msg = conn_manager.list_tables()
                    else:
                        # Fallback query
                        tables_df, error = conn_manager.execute_query("SHOW TABLES")
                        msg = "Tables retrieved" if tables_df is not None else f"Error: {error}"

                    if tables_df is not None:
                        st.success(msg)
                        if not tables_df.empty:
                            st.dataframe(tables_df, use_container_width=True)
                        else:
                            st.info("ℹ️ No tables found in current schema")
                    else:
                        st.error(msg)
            except Exception as e:
                st.error(f"❌ Error listing tables: {str(e)}")


def render_connection_management_ui(conn_manager):
    st.markdown("### 🔧 Connection Management")
    
    col7, col8, col9 = st.columns(3)
    with col7:
        if st.button("🔌 Disconnect", type="secondary", key="sf_disconnect"):
            try:
                conn_manager.disconnect()
                keys_to_clear = [k for k in st.session_state.keys() if 'sf_' in k or 'discovered_schema' in k]
                for key in keys_to_clear: 
                    del st.session_state[key]
                st.success("✅ Disconnected from Snowflake")
                st.rerun()
            except Exception as e: 
                st.error(f"❌ Error disconnecting: {str(e)}")
    
    with col8:
        if st.button("🔍 Test Connection", type="secondary", key="sf_test_connection"):
            try:
                if test_database_connectivity:
                    connectivity_ok, status_msg = test_database_connectivity(conn_manager)
                else:
                    try:
                        test_df, error = conn_manager.execute_query("SELECT 1 as test_connection")
                        connectivity_ok = test_df is not None
                        status_msg = "Connection is healthy!" if connectivity_ok else f"Connection failed: {error}"
                    except Exception as e:
                        connectivity_ok = False
                        status_msg = f"Connection test failed: {str(e)}"
                if connectivity_ok: 
                    st.success("✅ Connection is healthy!")
                else: 
                    st.error(status_msg)
            except Exception as e: 
                st.error(f"❌ Error testing connection: {str(e)}")
    
    with col9:
        if st.button("🔄 Switch Mode", type="secondary", help="Disconnect and reconnect in different mode", key="sf_switch_mode"):
            try:
                conn_manager.disconnect()
                keys_to_clear = [k for k in st.session_state.keys() if 'unified_connection' in k]
                for key in keys_to_clear: 
                    del st.session_state[key]
                st.info("✅ Disconnected. Please reconnect in your preferred mode.")
                st.rerun()
            except Exception as e: 
                st.error(f"❌ Error switching mode: {str(e)}")
    
    # Connection details
    if hasattr(conn_manager, 'is_connected') and conn_manager.is_connected:
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
                enhanced_mode = hasattr(conn_manager, 'enhanced_mode') and conn_manager.enhanced_mode
                st.text(f"Mode: {'Enhanced' if enhanced_mode else 'Standard'}")
                st.text(f"Session Management: {'✅ Active' if enhanced_mode else '❌ Basic'}")
                st.text(f"Modin Acceleration: {'🚀 Available' if MODIN_AVAILABLE else '📊 Not Available'}")
                st.text(f"Performance Tracking: {'✅ Active' if enhanced_mode else '❌ Not Available'}")
                st.text(f"Field Disambiguation: ✅ Active")


def main():
    try:
        # Add cache clearing functionality at the top
        if st.sidebar.button("🔄 Clear Cache & Refresh", help="Clear all caches and restart the app"):
            st.cache_data.clear()
            st.cache_resource.clear()
            # Clear all session state
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.sidebar.success("✅ Cache cleared! Refreshing...")
            st.rerun()
        
        # Force fresh content rendering
        st.empty()  # Clear any cached content
        
        st.markdown('<h1 class="main-header">❄️ Dynamic JSON-to-SQL Analyzer & Genrator for Snowflake</h1>', unsafe_allow_html=True)
        json_data = get_json_data_from_sidebar()
        if render_performance_info:
            render_performance_info()

        main_tab1, main_tab2 = st.tabs(["🐍 **Python Mode (Instant SQL Generation)**", "🏔️ **Snowflake Mode (Live Analysis)**"])

        with main_tab1:
            st.markdown('<h2 class="section-header">🐍 SQL Generator from Sample JSON</h2>', unsafe_allow_html=True)
            st.markdown("""
            <div class="feature-box">
                <p>Analyze your sample JSON structure and instantly generate a portable SQL query. Perfect for development, testing, and creating shareable scripts.</p>
                <ul>
                    <li>✅ <strong>Instant SQL Generation</strong> from the JSON you provide.</li>
                    <li>🧠 <strong>Smart Field Disambiguation</strong> for handling duplicate field names.</li>
                    <li>📋 <strong>Export to Multiple Formats</strong> like Python, dbt, and Jupyter.</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)

            if json_data:
                temp_schema, disambiguation_info = render_enhanced_disambiguation_info(json_data)
                
                st.markdown("### 📝 Query Configuration")
                input_col1, input_col2 = st.columns(2)
                with input_col1:
                    table_name = st.text_input("Table Name*", key="py_table", placeholder="your_schema.your_table", help="Snowflake table containing your JSON data")
                    field_conditions = st.text_area("Field Conditions*", height=120, key="py_fields", placeholder="e.g., name, company.name, user.profile.age", help="Specify JSON fields and optional conditions.")
                with input_col2:
                    json_column = st.text_input("JSON Column Name*", key="py_json_col", placeholder="json_data", help="Name of the column containing JSON data in your table")
                    execution_mode = st.radio("Choose Action:", ["📝 Generate SQL Only", "📋 Export for External Use"], key="py_execution_mode", help="Choose how to handle the generated SQL")

                # Field parsing debug for Python mode
                if field_conditions:
                    with st.expander("🔍 Field Parsing Debug (Python Mode)", expanded=True):
                        parsed_fields = parse_field_conditions_enhanced(field_conditions)
                        expected_columns = count_expected_columns_from_conditions(field_conditions, temp_schema, disambiguation_info)

                if execution_mode == "📋 Export for External Use":
                    export_format = st.selectbox("Export Format:", ["SQL File", "dbt Model", "Jupyter Notebook"], key="py_export_format")

                st.markdown("### 🚀 Generation & Execution")
                mode_display = execution_mode.split(' ', 1)[1]
                
                sub_col1, sub_col2 = st.columns([1, 2])
                with sub_col1:
                    generate_btn = st.button(f"🚀 {mode_display}", type="primary", use_container_width=True)
                with sub_col2:
                    st.markdown(f"""<div class="execution-mode-box" style="text-align:center;"><h6 style="margin-bottom: 0rem; color: #1976d2;">🎯 Current Mode: {mode_display}</h6></div>""", unsafe_allow_html=True)

                if temp_schema:
                    with st.expander("📊 JSON Structure Info", expanded=False):
                        queryable_count = sum(1 for details in temp_schema.values() if details.get('is_queryable', False))
                        st.metric("Queryable Fields", queryable_count)
                        st.metric("Total Fields", len(temp_schema))
                        if disambiguation_info:
                            st.metric("Name Conflicts", len(disambiguation_info))
                
                render_enhanced_python_field_suggestions(temp_schema, disambiguation_info)

                if generate_btn:
                    if not all([table_name, json_column, field_conditions]):
                        st.error("❌ Please fill in all required fields marked with *.")
                    else:
                        with st.spinner("🔍 Generating SQL with enhanced field parsing..."):
                            try:
                                sql, warnings, disambiguation_details = generate_enhanced_sql_python_mode(json_data, table_name, json_column, field_conditions)
                                
                                # Debug: Show actual column count in generated SQL
                                actual_select_count = sql.count("json_data:$") + sql.count("value:")
                                st.info(f"🔍 **Generated SQL Analysis:** Found {actual_select_count} field extractions in the query")
                                
                                if warnings:
                                    st.markdown("#### 🔔 Disambiguation Alerts")
                                    for warning in warnings: 
                                        st.warning(warning)
                                
                                st.markdown("---")
                                if execution_mode == "📝 Generate SQL Only":
                                    st.success("✅ SQL Generated Successfully!")
                                    st.code(sql, language="sql")
                                    st.download_button("📋 Download SQL Query", data=sql, file_name="generated_query.sql", mime="text/sql")
                                elif execution_mode == "📋 Export for External Use":
                                    export_format_val = safe_get_session_state('py_export_format', 'SQL File')
                                    
                                    if export_format_val == "dbt Model":
                                        st.success("📋 Complete dbt Model Package generated!")
                                        st.info("🎯 **This includes:** Model SQL + Schema YAML + Installation instructions")
                                        
                                        export_content = generate_export_content(sql, export_format_val, table_name, field_conditions)
                                        
                                        with st.expander("👀 Complete dbt Package Preview", expanded=True):
                                            st.code(export_content, language="sql")
                                        
                                        # Parse model name from table
                                        model_name = table_name.split('.')[-1].lower().replace('-', '_').replace(' ', '_')
                                        filename = f"dbt_model_{model_name}.sql"
                                        
                                        st.download_button(
                                            f"📥 Download Complete dbt Package", 
                                            data=export_content, 
                                            file_name=filename, 
                                            mime="text/sql"
                                        )
                                        
                                        st.markdown(f"""
                                        ### 📋 dbt Setup Instructions:
                                        1. **Save the model:** Copy the model SQL to `models/{model_name}.sql`
                                        2. **Update schema.yml:** Add the source and model configuration 
                                        3. **Run the model:** `dbt run --models {model_name}`
                                        4. **Test the model:** `dbt test --models {model_name}`
                                        """)
                                    else:
                                        # Handle other export formats as before
                                        st.success(f"📋 {export_format_val} generated successfully!")
                                        export_content = generate_export_content(sql, export_format_val, table_name, field_conditions)
                                        
                                        with st.expander("👀 Export Content Preview", expanded=True):
                                            st.code(export_content, language="sql" if "sql" in export_format_val.lower() else "python")
                                        
                                        st.download_button(
                                            f"📥 Download {export_format_val}", 
                                            data=export_content, 
                                            file_name=f"export.{get_file_extension(export_format_val)}", 
                                            mime=get_mime_type(export_format_val)
                                        )
                                render_disambiguation_details(sql, warnings, field_conditions, disambiguation_details)
                            except Exception as e:
                                st.error(f"❌ SQL generation error: {str(e)}")

                with st.expander("💡 Examples & Help", expanded=False):
                    if temp_schema:
                        example_col1, example_col2 = st.columns(2)
                        with example_col1:
                            st.markdown("**🎯 Examples for your JSON:**")
                            example_fields = []
                            for path, details in temp_schema.items():
                                if details.get('is_queryable', False):
                                    example_fields.append(path)
                                    if len(example_fields) >= 3: 
                                        break
                            if example_fields:
                                st.code(f"# Basic field selection\n{', '.join(example_fields[:2])}", language="text")
                                if len(example_fields) >= 3: 
                                    st.code(f"# Multiple fields\n{', '.join(example_fields)}", language="text")
                                if disambiguation_info: 
                                    st.markdown("**🚨 Disambiguation Examples:**")
                                    conflict_field = list(disambiguation_info.keys())[0]
                                    options = disambiguation_info[conflict_field]['paths'][:2]
                                    st.code(f"# Ambiguous (auto-resolved)\n{conflict_field}", language="text")
                                    st.code(f"# Explicit paths\n{', '.join([opt['full_path'] for opt in options])}", language="text")
                        with example_col2:
                            st.markdown("**📋 General Examples:**")
                            examples = ["name, age, email", "user.name, user.profile.age[>:18]", "status[=:active], created_date[IS NOT NULL]", "tags[IN:premium|gold], score[>:100]"]
                            for ex in examples: 
                                st.code(ex, language="text")
                    else:
                        st.markdown("**📋 Standard Examples:**")
                        example_cols = st.columns(2)
                        with example_cols[0]: 
                            examples1 = ["name, age, email", "user.name, user.profile.age[>:18]"]
                            for ex in examples1: 
                                st.code(ex, language="text")
                        with example_cols[1]: 
                            examples2 = ["status[=:active], created_date[IS NOT NULL]", "tags[IN:premium|gold], score[>:100]"]
                            for ex in examples2: 
                                st.code(ex, language="text")
            else:
                st.info("👆 Provide JSON data via the sidebar to begin.")

        with main_tab2:
            st.markdown('<h2 class="section-header">❄️  Snowflake Database Connection</h2>', unsafe_allow_html=True)
            st.markdown("""<div class="feature-box"><p>Choose the connection mode that best fits your needs. Enhanced Snowflake UI now matches Python mode experience!</p></div>""", unsafe_allow_html=True)
            
            # Force fresh rendering of mode descriptions
            col_mode1, col_mode2 = st.columns(2)
            with col_mode1: 
                # Clear any cached content and render fresh
                st.markdown("""**🏔️ Standard Mode:**
- ✅ Basic connectivity
- 📊 Standard pandas processing
- 🔧 Simple error handling
- 💾 Good for small to medium datasets""")
            with col_mode2: 
                st.markdown("""**⚡ Enhanced Mode:**
- 🛡️ **Fixed session context management**
- 🚀 **Modin acceleration**
- 📊 **Real-time performance tracking**
- 🏷️ Smart table name resolution""")
            
            connection_mode = st.radio("Select Connection Mode:", ["🏔️ Standard Mode", "⚡ Enhanced Mode"], index=1, horizontal=True, help="Enhanced mode includes all standard features plus advanced capabilities")
            enhanced_mode = "Enhanced" in connection_mode
            
            st.markdown("---")
            st.subheader("🔐 Database Connection")
            if render_unified_connection_ui:
                # Force fresh connection UI rendering
                conn_manager = render_unified_connection_ui(enhanced_mode=enhanced_mode)
            else:
                st.error("❌ Connection UI not available")
                conn_manager = None
            
            if conn_manager and conn_manager.is_connected:
                if test_database_connectivity:
                    connectivity_ok, status_msg = test_database_connectivity(conn_manager)
                else:
                    connectivity_ok = True
                    status_msg = "Connection appears active"
                
                if connectivity_ok:
                    st.success(status_msg)
                    st.markdown("---")
                    st.subheader("📊 Enhanced Database Operations")
                    render_enhanced_snowflake_ui(conn_manager)
                else: 
                    st.error(status_msg)
                    st.info("💡 Try disconnecting and reconnecting with correct database/schema settings.")
            else: 
                st.markdown("---")
                mode_text = "Enhanced" if enhanced_mode else "Standard"
                st.info(f"👆 **Connect using {mode_text} mode above to unlock enhanced database operations.**")
        
        st.markdown("""
    <div class="footer" style="max-width: 900px; margin: auto; padding: 2rem;">
        <p style="text-align: center;">
            <strong>🚀 Dynamic JSON-to-SQL Analyzer & Generator</strong> | Built with ❤️ using Streamlit
        </p>
        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 2rem; margin-top: 1rem;">
            <div style="text-align: left;">
                <h4 style="color: #1976d2;">🐍 Python Mode</h4>
                <p>Instant SQL generation<br/>Enhanced field parsing<br/>Debug multi-field support</p>
            </div>
            <div style="text-align: left;">
                <h4 style="color: #9c27b0;">🚀 Key Features</h4>
                <p>Fixed multi-field parsing<br/>Smart field disambiguation<br/>Enhanced debugging tools</p>
            </div>
        </div>
        <hr style="margin: 2rem 0; border: 1px solid #e9ecef;">
        <p style="font-size: 0.9rem; text-align: center;">
            <strong>⚡ Enhanced UI:</strong> Snowflake mode now matches Python mode experience<br/>
            <strong>📋 Debug Tools:</strong> Field parsing analysis and column count verification
        </p>
    </div>
""", unsafe_allow_html=True)


    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        st.error(f"❌ Application Error: {str(e)}")
        # Show debug info
        st.error("🔍 **Debug Info:** If you see cached content, click 'Clear Cache & Refresh' in the sidebar")


if __name__ == "__main__":
    main()
