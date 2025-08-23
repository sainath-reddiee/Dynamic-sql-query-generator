import streamlit as st
import json
import pandas as pd
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import logging
import os
import random

# Import all required modules from the 'src' directory
try:
    from python_sql_generator import generate_sql_from_json_data
except ImportError:
    st.error("❌ Missing python_sql_generator module")
    st.stop()

# UNIFIED: Import the new unified connector instead of both separate ones
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
    # Create fallback config
    class Config:
        APP_NAME = "JSON-to-SQL Analyzer"
    config = Config()

# Configure logging
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title=f"❄️ {getattr(config, 'APP_NAME', 'JSON-to-SQL Analyzer')}",
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
    .execution-mode-box {
        background: linear-gradient(145deg, #e3f2fd, #f8f9ff);
        padding: 1rem;
        border-radius: 8px;
        border: 2px solid #64b5f6;
        margin: 0.5rem 0;
    }
    .mock-results-box {
        background: linear-gradient(145deg, #f3e5f5, #fafafa);
        padding: 1rem;
        border-radius: 8px;
        border: 2px solid #ba68c8;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)


def get_json_data_from_sidebar() -> Optional[Dict]:
    """Handle JSON input from sidebar with error handling"""
    
    st.sidebar.markdown("## 📁 JSON Data Input")
    
    input_method = st.sidebar.radio(
        "Choose input method:",
        ["📝 Paste JSON", "📁 Upload File"],
        key="json_input_method"
    )
    
    json_data = None
    
    if input_method == "📝 Paste JSON":
        json_text = st.sidebar.text_area(
            "Paste your JSON data:",
            height=200,
            placeholder='{\n  "name": "John Doe",\n  "age": 30,\n  "email": "john@example.com"\n}',
            key="json_input_text"
        )
        
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
    
    # Display JSON preview in sidebar
    if json_data:
        with st.sidebar.expander("👀 JSON Preview", expanded=False):
            st.json(json_data, expanded=False)
            
        # Store in session state
        st.session_state['json_data'] = json_data
    
    return json_data


def safe_get_session_state(key: str, default: Any = None) -> Any:
    """Safely get value from session state with default"""
    try:
        return st.session_state.get(key, default)
    except Exception:
        return default


def generate_mock_results_from_json(json_data, field_conditions, num_rows=10):
    """Generate mock results based on JSON structure and field conditions"""
    import pandas as pd
    
    # Parse field conditions to get column names
    fields = []
    for condition in field_conditions.split(','):
        field = condition.strip().split('[')[0].strip()
        if field:
            fields.append(field)
    
    if not fields:
        return pd.DataFrame({"message": ["No fields specified"]})
    
    mock_data = {}
    
    # Helper function to extract sample value from JSON structure
    def find_sample_value(data, field_path):
        try:
            parts = field_path.split('.')
            current = data
            
            for part in parts:
                if isinstance(current, dict) and part in current:
                    current = current[part]
                elif isinstance(current, list) and current:
                    # Take first item from array
                    current = current[0]
                    if isinstance(current, dict) and part in current:
                        current = current[part]
                    else:
                        return None
                else:
                    return None
            return current
        except:
            return None
    
    for field in fields:
        # Try to find actual sample from JSON
        sample_value = find_sample_value(json_data, field)
        
        if sample_value is not None:
            # Generate similar mock data based on sample
            if isinstance(sample_value, (int, float)):
                # Numeric data - generate similar range
                base_val = float(sample_value)
                mock_data[field] = [round(base_val + random.uniform(-base_val*0.5, base_val*0.5), 2) for _ in range(num_rows)]
            elif isinstance(sample_value, str):
                if '@' in sample_value:
                    # Email-like
                    domains = ['example.com', 'test.org', 'demo.net', 'sample.io']
                    mock_data[field] = [f"user{i}@{random.choice(domains)}" for i in range(num_rows)]
                elif any(keyword in field.lower() for keyword in ['date', 'time', 'created', 'updated']):
                    # Date fields
                    base_date = datetime.now()
                    mock_data[field] = [
                        (base_date - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d %H:%M:%S')
                        for _ in range(num_rows)
                    ]
                elif any(keyword in field.lower() for keyword in ['name', 'title']):
                    # Name-like fields
                    names = ['John Doe', 'Jane Smith', 'Bob Johnson', 'Alice Brown', 'Charlie Davis', 
                             'Emma Wilson', 'Michael Chen', 'Sarah Miller', 'David Lee', 'Lisa Garcia']
                    mock_data[field] = [random.choice(names) for _ in range(num_rows)]
                else:
                    # Generic string - use sample as base
                    base_str = str(sample_value).replace(' ', '_')
                    mock_data[field] = [f"{base_str}_{i}" for i in range(num_rows)]
            elif isinstance(sample_value, bool):
                mock_data[field] = [random.choice([True, False]) for _ in range(num_rows)]
            else:
                # Convert to string representation
                mock_data[field] = [f"sample_{i}" for i in range(num_rows)]
        else:
            # No sample found - generate based on field name
            field_lower = field.lower()
            if any(keyword in field_lower for keyword in ['age', 'count', 'score', 'number', 'quantity']):
                mock_data[field] = [random.randint(1, 100) for _ in range(num_rows)]
            elif any(keyword in field_lower for keyword in ['email', 'mail']):
                domains = ['example.com', 'test.org', 'demo.net']
                mock_data[field] = [f"user{i}@{random.choice(domains)}" for i in range(num_rows)]
            elif any(keyword in field_lower for keyword in ['date', 'time', 'created', 'updated']):
                base_date = datetime.now()
                mock_data[field] = [
                    (base_date - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')
                    for _ in range(num_rows)
                ]
            elif any(keyword in field_lower for keyword in ['name', 'title', 'label']):
                options = ['Sample Item', 'Test Entry', 'Demo Record', 'Example Data', 'Mock Value']
                mock_data[field] = [f"{random.choice(options)} {i+1}" for i in range(num_rows)]
            elif any(keyword in field_lower for keyword in ['status', 'state', 'type']):
                statuses = ['active', 'inactive', 'pending', 'completed', 'processing']
                mock_data[field] = [random.choice(statuses) for _ in range(num_rows)]
            elif any(keyword in field_lower for keyword in ['price', 'cost', 'amount', 'value']):
                mock_data[field] = [round(random.uniform(10.0, 1000.0), 2) for _ in range(num_rows)]
            else:
                mock_data[field] = [f"mock_value_{i+1}" for i in range(num_rows)]
    
    return pd.DataFrame(mock_data)


def generate_export_content(sql, export_format, table_name, field_conditions=None):
    """Generate different export formats"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    if export_format == "SQL File":
        return f"""-- Generated SQL Query for JSON Analysis
-- Table: {table_name}
-- Fields: {field_conditions or 'N/A'}
-- Generated: {timestamp}
-- 
-- This query extracts and analyzes JSON data from Snowflake
-- Modify connection parameters and table references as needed

{sql}

-- Additional Notes:
-- 1. Ensure your Snowflake account has access to the specified table
-- 2. Adjust JSON field paths based on your actual data structure
-- 3. Consider adding appropriate WHERE clauses for performance
-- 4. Test with LIMIT clause first for large datasets
"""
    
    elif export_format == "Python Script":
        return f"""#!/usr/bin/env python3
\"\"\"
Generated Python script for Snowflake JSON query execution
Table: {table_name}
Fields: {field_conditions or 'N/A'}
Generated: {timestamp}

Requirements:
    pip install snowflake-connector-python pandas

Usage:
    1. Update the connection configuration below
    2. Run: python this_script.py
\"\"\"

import snowflake.connector
import pandas as pd
from datetime import datetime
import sys

# Snowflake connection configuration
# IMPORTANT: Update these values with your actual connection details
CONN_CONFIG = {{
    'account': 'your_account.region',  # e.g., 'abc123.us-west-2.snowflakecomputing.com'
    'user': 'your_username',
    'password': 'your_password',  # Consider using environment variables
    'database': 'your_database',
    'schema': 'your_schema',
    'warehouse': 'your_warehouse',
    'role': 'your_role'  # Optional
}}

# Generated SQL Query
QUERY = \"\"\"
{sql.strip()}
\"\"\"

def execute_snowflake_query():
    \"\"\"Execute the generated query and return results\"\"\"
    try:
        print(f"🔗 Connecting to Snowflake...")
        conn = snowflake.connector.connect(**CONN_CONFIG)
        
        print(f"📊 Executing query...")
        df = pd.read_sql(QUERY, conn)
        
        print(f"✅ Success! Retrieved {{len(df)}} rows with {{len(df.columns)}} columns")
        
        # Display basic info
        print(f"\\n📋 Column Summary:")
        for col in df.columns:
            print(f"  - {{col}}: {{df[col].dtype}}")
        
        # Show first few rows
        print(f"\\n🔍 First 5 rows:")
        print(df.head().to_string())
        
        # Save to CSV
        output_file = f"snowflake_results_{{datetime.now().strftime('%Y%m%d_%H%M%S')}}.csv"
        df.to_csv(output_file, index=False)
        print(f"\\n💾 Results saved to: {{output_file}}")
        
        conn.close()
        return df
        
    except snowflake.connector.errors.DatabaseError as e:
        print(f"❌ Database Error: {{e}}")
        return None
    except Exception as e:
        print(f"❌ Error: {{e}}")
        return None

def main():
    \"\"\"Main execution function\"\"\"
    print("🚀 Starting Snowflake JSON Query Execution")
    print(f"📅 Generated: {timestamp}")
    print(f"🏗️  Table: {table_name}")
    print(f"🎯 Fields: {field_conditions or 'N/A'}")
    print("-" * 50)
    
    # Validate configuration
    if CONN_CONFIG['account'] == 'your_account.region':
        print("⚠️  WARNING: Please update CONN_CONFIG with your actual Snowflake details!")
        response = input("Continue anyway? (y/N): ")
        if response.lower() != 'y':
            print("Exiting...")
            sys.exit(1)
    
    # Execute query
    results = execute_snowflake_query()
    
    if results is not None:
        print("\\n🎉 Query execution completed successfully!")
    else:
        print("\\n❌ Query execution failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
"""
    
    elif export_format == "dbt Model":
        model_name = table_name.split('.')[-1].lower().replace('-', '_')
        return f"""{{{{
  config(
    materialized='view',
    description='JSON analysis model for {table_name}'
  )
}}}}

--
-- dbt model for JSON field extraction and analysis
-- Source Table: {table_name}
-- Fields: {field_conditions or 'N/A'}
-- Generated: {timestamp}
--
-- Usage:
--   dbt run --models {model_name}
--

{sql.rstrip(';')}

--
-- Post-hook suggestions:
-- {{ config(post_hook="GRANT SELECT ON {{{{ this }}}} TO ROLE analytics_role") }}
--
-- Additional transformations can be added here:
-- - Add data quality checks
-- - Apply business logic transformations  
-- - Add calculated fields
-- - Join with other models
--
"""
    
    elif export_format == "Jupyter Notebook":
        # FIX: Replaced the placeholder notebook with the full, detailed version.
        notebook_content = {
            "cells": [
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": [
                        "# ❄️ Snowflake JSON Analysis Notebook\n",
                        f"**Generated:** {timestamp}\n",
                        f"**Table:** {table_name}\n",
                        f"**Fields:** {field_conditions or 'N/A'}\n\n",
                        "This notebook contains the generated SQL query for JSON analysis in Snowflake.\n",
                        "Update the connection parameters and run the cells below to execute the query and analyze the results."
                    ]
                },
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": ["## 📦 1. Install Required Packages"]
                },
                {
                    "cell_type": "code",
                    "execution_count": None, "metadata": {}, "outputs": [],
                    "source": [
                        "!pip install snowflake-connector-python pandas matplotlib seaborn jupyter"
                    ]
                },
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": ["## ⚙️ 2. Import Libraries & Configure"]
                },
                {
                    "cell_type": "code",
                    "execution_count": None, "metadata": {}, "outputs": [],
                    "source": [
                        "import snowflake.connector\n",
                        "import pandas as pd\n",
                        "import matplotlib.pyplot as plt\n",
                        "import seaborn as sns\n",
                        "from datetime import datetime\n\n",
                        "pd.set_option('display.max_columns', 100)\n",
                        "print('✅ Libraries imported successfully!')"
                    ]
                },
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": [
                        "## 🔗 3. Snowflake Connection Setup\n",
                        "**Important:** Update the `conn_params` dictionary below with your actual Snowflake credentials."
                    ]
                },
                {
                    "cell_type": "code",
                    "execution_count": None, "metadata": {}, "outputs": [],
                    "source": [
                        "# IMPORTANT: Update these with your actual credentials\n",
                        "conn_params = {\n",
                        "    'account': 'your_account.region',\n",
                        "    'user': 'your_username',\n",
                        "    'password': 'your_password',  # Consider using getpass for security\n",
                        "    'database': 'your_database',\n",
                        "    'schema': 'your_schema',\n",
                        "    'warehouse': 'your_warehouse'\n",
                        "}\n\n",
                        "print('🔧 Connection parameters configured.')"
                    ]
                },
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": ["## 📋 4. Generated SQL Query"]
                },
                {
                    "cell_type": "code",
                    "execution_count": None, "metadata": {}, "outputs": [],
                    "source": [
                        f"# Generated SQL Query\n",
                        f'query = """\n{sql.strip()}\n"""\n\n',
                        "print('📝 Query loaded successfully.')\n",
                        "print('-' * 20 + ' QUERY PREVIEW ' + '-' * 20)\n",
                        "print(query[:500] + ('...' if len(query) > 500 else ''))"
                    ]
                },
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": ["## 🚀 5. Execute Query & Fetch Data"]
                },
                {
                    "cell_type": "code",
                    "execution_count": None, "metadata": {}, "outputs": [],
                    "source": [
                        "df = None\n",
                        "try:\n",
                        "    print('🔗 Connecting to Snowflake...')\n",
                        "    with snowflake.connector.connect(**conn_params) as conn:\n",
                        "        print('📊 Executing query...')\n",
                        "        df = pd.read_sql(query, conn)\n",
                        "        print(f'✅ Success! Retrieved {{len(df):,}} rows with {{len(df.columns)}} columns.')\n",
                        "except Exception as e:\n",
                        "    print(f'❌ Execution Error: {{e}}')"
                    ]
                },
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": ["## 👀 6. Data Preview & Analysis"]
                },
                {
                    "cell_type": "code",
                    "execution_count": None, "metadata": {}, "outputs": [],
                    "source": [
                        "if df is not None:\n",
                        "    print('--- First 5 Rows ---')\n",
                        "    display(df.head())\n\n",
                        "    print('\\n--- Data Types & Non-Null Counts ---')\n",
                        "    display(df.info())\n\n",
                        "    print('\\n--- Numerical Statistics ---')\n",
                        "    display(df.describe())\n",
                        "else:\n",
                        "    print('❌ No data available to display.')"
                    ]
                },
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": ["## 📈 7. Data Visualization (Optional)"]
                },
                {
                    "cell_type": "code",
                    "execution_count": None, "metadata": {}, "outputs": [],
                    "source": [
                        "if df is not None and not df.empty:\n",
                        "    # Select first numerical and categorical column for plotting\n",
                        "    num_cols = df.select_dtypes(include=['number']).columns\n",
                        "    cat_cols = df.select_dtypes(include=['object', 'category']).columns\n\n",
                        "    fig, axes = plt.subplots(1, 2, figsize=(16, 6))\n",
                        "    fig.suptitle('Basic Data Visualization', fontsize=16)\n\n",
                        "    if len(num_cols) > 0:\n",
                        "        sns.histplot(df[num_cols[0]], kde=True, ax=axes[0])\n",
                        "        axes[0].set_title(f'Distribution of {{num_cols[0]}}')\n",
                        "    else:\n",
                        "        axes[0].text(0.5, 0.5, 'No numerical data to plot', ha='center')\n\n",
                        "    if len(cat_cols) > 0:\n",
                        "        # Plot top 10 categories\n",
                        "        top_10 = df[cat_cols[0]].value_counts().nlargest(10)\n",
                        "        sns.barplot(x=top_10.index, y=top_10.values, ax=axes[1])\n",
                        "        axes[1].set_title(f'Top 10 Categories in {{cat_cols[0]}}')\n",
                        "        axes[1].tick_params(axis='x', rotation=45)\n",
                        "    else:\n",
                        "        axes[1].text(0.5, 0.5, 'No categorical data to plot', ha='center')\n\n",
                        "    plt.tight_layout(rect=[0, 0, 1, 0.96])\n",
                        "    plt.show()\n",
                        "else:\n",
                        "    print('❌ No data to visualize.')"
                    ]
                },
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": ["## 💾 8. Export Results"]
                },
                {
                    "cell_type": "code",
                    "execution_count": None, "metadata": {}, "outputs": [],
                    "source": [
                        "if df is not None and not df.empty:\n",
                        "    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')\n",
                        "    csv_filename = f'snowflake_results_{timestamp_str}.csv'\n",
                        "    df.to_csv(csv_filename, index=False)\n",
                        "    print(f'✅ Results exported to: {csv_filename}')\n",
                        "else:\n",
                        "    print('❌ No data to export.')"
                    ]
                }
            ],
            "metadata": {
                "kernelspec": {
                    "display_name": "Python 3", "language": "python", "name": "python3"
                },
                "language_info": {
                    "codemirror_mode": {"name": "ipython", "version": 3},
                    "file_extension": ".py",
                    "mimetype": "text/x-python",
                    "name": "python",
                    "nbconvert_exporter": "python",
                    "pygments_lexer": "ipython3",
                    "version": "3.9.7"
                }
            },
            "nbformat": 4,
            "nbformat_minor": 4
        }
        return json.dumps(notebook_content, indent=2)

    elif export_format == "PowerBI Template":
        return f"""# Power BI Data Source Template
# Generated: {timestamp}
# Table: {table_name}
# Fields: {field_conditions or 'N/A'}

# 1. Open Power BI Desktop
# 2. Get Data -> More -> Database -> Snowflake
# 3. Enter your Snowflake server details
# 4. Use Advanced options and paste the query below

# Snowflake Connection Details:
# Server: your_account.region.snowflakecomputing.com
# Database: your_database
# Schema: your_schema
# Warehouse: your_warehouse

# Custom SQL Query:
{sql}

# Additional Power BI Setup Steps:
# 1. After connecting, you can:
#    - Rename columns in the Query Editor
#    - Change data types if needed
#    - Add calculated columns
#    - Create relationships with other tables
#
# 2. Recommended visualizations for JSON data:
#    - Table visual for detailed view
#    - Cards for key metrics
#    - Charts for numeric fields
#    - Slicers for filtering
#
# 3. Consider setting up:
#    - Data refresh schedule
#    - Row-level security if needed
#    - Performance optimization
"""

    else:
        return f"# Unknown export format: {export_format}\n# Generated: {timestamp}\n\n{sql}"


def get_file_extension(export_format):
    """Get file extension for export format"""
    extensions = {
        "SQL File": "sql",
        "Python Script": "py",
        "dbt Model": "sql",
        "Jupyter Notebook": "ipynb",
        "PowerBI Template": "txt"
    }
    return extensions.get(export_format, "txt")


def get_mime_type(export_format):
    """Get MIME type for export format"""
    mime_types = {
        "SQL File": "text/sql",
        "Python Script": "text/x-python",
        "dbt Model": "text/sql",
        "Jupyter Notebook": "application/json",
        "PowerBI Template": "text/plain"
    }
    return mime_types.get(export_format, "text/plain")


def render_enhanced_disambiguation_info(json_data):
    """Render enhanced disambiguation info for Python mode"""
    
    try:
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

                with st.expander("🔍 View Conflict Details", expanded=False):
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
        
        return temp_schema, disambiguation_info
    
    except Exception as e:
        st.warning(f"Could not analyze disambiguation info: {e}")
        return {}, {}


def render_enhanced_python_field_suggestions(temp_schema, disambiguation_info):
    """Enhanced field suggestions for Python mode with disambiguation"""
    
    if temp_schema:
        with st.expander("💡 Smart Field Suggestions (Click to Use)", expanded=False):
            st.markdown("**🎯 Available queryable fields - click to add:**")

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

            # Show fields in a grid layout
            suggestion_cols = st.columns(2)
            for i, field in enumerate(queryable_fields_list[:12]):  # Show more fields
                col_idx = i % 2
                with suggestion_cols[col_idx]:
                    if st.button(
                        f"➕ {field['Field Path']}",
                        key=f"use_field_{field['Field Path']}_{i}",
                        help=f"Type: {field['Type']} | Sample: {field['Sample']}",
                        type="secondary"
                    ):
                        current_conditions = st.session_state.get('py_fields', '').strip()
                        if current_conditions:
                            st.session_state.py_fields = f"{current_conditions}, {field['Field Path']}"
                        else:
                            st.session_state.py_fields = field['Field Path']
                        st.rerun()

            if len(queryable_fields_list) > 12:
                st.caption(f"... and {len(queryable_fields_list) - 12} more fields available")


def generate_enhanced_sql_python_mode(json_data, table_name, json_column, field_conditions):
    """Enhanced SQL generation for Python mode with warnings and disambiguation"""
    
    try:
        # Use the enhanced version that returns warnings
        from python_sql_generator import generate_sql_from_json_data_with_warnings

        sql, warnings, disambiguation_details = generate_sql_from_json_data_with_warnings(
            json_data, table_name, json_column, field_conditions
        )

        return sql, warnings, disambiguation_details
        
    except ImportError:
        # Fallback to basic generation
        try:
            from python_sql_generator import generate_sql_from_json_data
            sql = generate_sql_from_json_data(json_data, table_name, json_column, field_conditions)
            return sql, [], {}
        except Exception as e:
            return f"-- Error: {str(e)}", [f"❌ Generation error: {str(e)}"], {}
    except Exception as e:
        return f"-- Error: {str(e)}", [f"❌ Generation error: {str(e)}"], {}


def render_disambiguation_details(sql, warnings, field_conditions, disambiguation_details):
    """Render disambiguation details in expandable section"""
    
    # Show additional details if disambiguation was used
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


def render_database_operations_ui(conn_manager):
    """Enhanced operations UI with fixed session state handling"""
    
    if not conn_manager:
        st.error("❌ Connection manager not available")
        return

    # Display current mode information
    mode_text = "Enhanced" if hasattr(conn_manager, 'enhanced_mode') and conn_manager.enhanced_mode else "Standard"
    mode_color = "#2e7d32" if hasattr(conn_manager, 'enhanced_mode') and conn_manager.enhanced_mode else "#1976d2"
    mode_icon = "⚡" if hasattr(conn_manager, 'enhanced_mode') and conn_manager.enhanced_mode else "🏔️"

    st.markdown(f"""
    <div class="mode-selector">
        <h5 style="color: {mode_color}; margin-bottom: 0.5rem;">{mode_icon} Currently in {mode_text} Mode</h5>
        <p style="margin-bottom: 0; font-size: 0.9rem;">
            {'🛡️ Session context management + 🚀 Modin acceleration + 📊 Performance tracking + ⚠️ Field disambiguation' if hasattr(conn_manager, 'enhanced_mode') and conn_manager.enhanced_mode else '📊 Basic connectivity with standard pandas processing + ⚠️ Field disambiguation'}
        </p>
    </div>
    """, unsafe_allow_html=True)

    # Custom SQL section with session state fix
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

    # Initialize session state if not exists
    if "last_custom_sql" not in st.session_state:
        st.session_state.last_custom_sql = ""

    # Quick example buttons BEFORE text area
    st.markdown("#### 💡 Quick SQL Examples:")
    col_ex1, col_ex2, col_ex3 = st.columns(3)
    
    example_sql = None
    
    with col_ex1:
        if st.button("📋 Show Tables", help="List all tables"):
            example_sql = "SHOW TABLES;"
    
    with col_ex2:
        if st.button("🏗️ Table Schema", help="Get table information"):
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
        if st.button("📊 Sample Data", help="Sample from a table"):
            example_sql = """-- Replace 'your_table' with actual table name
SELECT * FROM your_table LIMIT 10;"""

    # Text area with fixed session state handling
    initial_value = example_sql if example_sql else st.session_state.last_custom_sql
    
    custom_sql = st.text_area(
        f"Execute Custom SQL ({mode_text} Mode):",
        value=initial_value,
        height=150,
        placeholder="""-- Quick examples to try:
SHOW TABLES;
-- or --
SELECT * FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA'
LIMIT 10;""",
        key="custom_sql_input",
        help=f"Write any SQL query - {'large results will use Modin for faster processing' if hasattr(conn_manager, 'enhanced_mode') and conn_manager.enhanced_mode else 'processed with standard pandas'}"
    )
    
    # Store the current SQL for next time
    if custom_sql:
        st.session_state.last_custom_sql = custom_sql

    col_sql1, col_sql2 = st.columns(2)

    with col_sql1:
        execute_sql_btn = st.button(f"▶️ Execute SQL ({mode_text})", type="primary")

        if execute_sql_btn and custom_sql.strip():
            try:
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
        if st.button("📋 List Tables", type="secondary"):
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

    # Enhanced Smart JSON Analysis Section with disambiguation
    st.markdown("---")
    st.markdown("### 🧪 Smart JSON Analysis with Disambiguation")

    if hasattr(conn_manager, 'enhanced_mode') and conn_manager.enhanced_mode:
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

                if render_enhanced_field_suggestions:
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
                else:
                    st.info("Enhanced field suggestions not available.")

            except Exception as e:
                st.warning(f"Could not generate enhanced suggestions: {e}")

    # Enhanced disambiguation helper
    if 'schema_metadata_unified' in st.session_state:
        metadata = st.session_state.schema_metadata_unified
        disambiguation_info = metadata.get('disambiguation_info', {})

        if disambiguation_info and field_conditions and render_multi_level_helper_ui:
            render_multi_level_helper_ui(field_conditions, disambiguation_info)

    # Enhanced "Analyze Schema Only" and "Analyze & Execute" buttons
    col3, col4 = st.columns(2)

    with col3:
        if st.button("🔍 Analyze Schema Only", type="secondary"):
            if table_name and json_column:
                try:
                    with st.spinner(f"🔄 {'Enhanced' if hasattr(conn_manager, 'enhanced_mode') and conn_manager.enhanced_mode else 'Standard'} schema analysis with disambiguation..."):
                        if analyze_database_json_schema_universal:
                            schema, error, metadata = analyze_database_json_schema_universal(
                                conn_manager, table_name, json_column, sample_size
                            )
                        else:
                            # Fallback for missing function
                            schema, error, metadata = None, "Function not available", {}

                        if schema:
                            st.success(f"✅ {'Enhanced' if hasattr(conn_manager, 'enhanced_mode') and conn_manager.enhanced_mode else 'Standard'} schema analysis complete! Found {len(schema)} fields.")

                            # Store in session state for suggestions
                            st.session_state.discovered_schema_unified = schema
                            st.session_state.schema_metadata_unified = metadata

                            # Show disambiguation summary
                            disambiguation_info = metadata.get('disambiguation_info', {})
                            if disambiguation_info:
                                st.info(f"🚨 Found {len(disambiguation_info)} field names with multiple locations. Check the detailed preview for disambiguation options.")

                            if show_preview and render_enhanced_database_json_preview:
                                render_enhanced_database_json_preview(schema, metadata)
                        else:
                            st.error(error)

                except Exception as e:
                    st.error(f"❌ Schema analysis failed: {str(e)}")
                    st.info("💡 This might be due to table access permissions or connection issues.")
            else:
                st.warning("⚠️ Please provide table name and JSON column.")

    with col4:
        mode_text = "Enhanced" if hasattr(conn_manager, 'enhanced_mode') and conn_manager.enhanced_mode else "Standard"
        analyze_and_execute = st.button(f"🚀 Analyze & Execute ({mode_text} Mode)", type="primary")

        if analyze_and_execute and all([table_name, json_column, field_conditions]):
            try:
                # Generate SQL using the enhanced universal database-driven analysis
                with st.spinner(f"⚡ {'Enhanced' if hasattr(conn_manager, 'enhanced_mode') and conn_manager.enhanced_mode else 'Standard'} analysis with disambiguation..."):
                    if generate_database_driven_sql:
                        generated_sql, sql_error = generate_database_driven_sql(
                            conn_manager, table_name, json_column, field_conditions
                        )
                    else:
                        generated_sql, sql_error = None, "Function not available"

                    if generated_sql and not sql_error:
                        st.success("✅ Enhanced SQL Generated Successfully with Disambiguation Support!")
                        st.code(generated_sql, language="sql")

                        # Execute with appropriate method based on connector mode
                        if hasattr(conn_manager, 'enhanced_mode') and conn_manager.enhanced_mode:
                            # Enhanced mode: Use performance tracking
                            with st.spinner("⚡ Executing with performance monitoring and disambiguation verification..."):
                                if hasattr(conn_manager, 'execute_query_with_performance'):
                                    result_df, exec_error, perf_stats = conn_manager.execute_query_with_performance(generated_sql)
                                else:
                                    result_df, exec_error = conn_manager.execute_query(generated_sql)
                                    perf_stats = {}

                                if result_df is not None:
                                    st.success("✅ Query executed with enhanced performance monitoring!")

                                    # Display performance metrics
                                    if perf_stats and render_performance_metrics:
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

    # Connection management
    st.markdown("---")
    st.markdown("### 🔧 Connection Management")

    col7, col8, col9 = st.columns(3)

    with col7:
        if st.button("🔌 Disconnect", type="secondary"):
            try:
                conn_manager.disconnect()
                # Clear all session state related to connections
                keys_to_clear = [k for k in st.session_state.keys() if 'unified_connection' in k or 'discovered_schema' in k]
                for key in keys_to_clear:
                    del st.session_state[key]
                st.success("✅ Disconnected from Snowflake")
                st.rerun()
            except Exception as e:
                st.error(f"❌ Error disconnecting: {str(e)}")

    with col8:
        if st.button("🔍 Test Connection", type="secondary"):
            try:
                if test_database_connectivity:
                    connectivity_ok, status_msg = test_database_connectivity(conn_manager)
                else:
                    # Fallback test
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
        if st.button("🔄 Switch Mode", type="secondary", help="Disconnect and reconnect in different mode"):
            try:
                conn_manager.disconnect()
                # Clear session state
                keys_to_clear = [k for k in st.session_state.keys() if 'unified_connection' in k]
                for key in keys_to_clear:
                    del st.session_state[key]
                st.info("✅ Disconnected. Please reconnect in your preferred mode.")
                st.rerun()
            except Exception as e:
                st.error(f"❌ Error switching mode: {str(e)}")

    # Enhanced connection details with disambiguation info
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


# Main App
def main():
    try:
        st.markdown('<h1 class="main-header">❄️ Enhanced JSON-to-SQL Analyzer for Snowflake</h1>', unsafe_allow_html=True)

        # Get JSON data from sidebar
        json_data = get_json_data_from_sidebar()

        # Display enhanced performance information at the top
        if render_performance_info:
            render_performance_info()

        # Main tabs
        main_tab1, main_tab2 = st.tabs([
            "🐍 **Enhanced Python (Instant SQL Generation)**",
            "🏔️ **Enhanced Snowflake Database Connection**"
        ])

        with main_tab1:
            st.markdown('<h2 class="section-header">🐍 Enhanced SQL Generator with Multiple Execution Options</h2>', unsafe_allow_html=True)
            
            # Enhanced description
            st.markdown("""
            <div class="feature-box">
                <p><strong>🎯 Enhanced Python SQL Generator:</strong> Analyze JSON structure, generate SQL with smart disambiguation, 
                and choose from multiple execution options!</p>
                <ul>
                    <li>✅ <strong>Instant SQL Generation</strong> from JSON structure</li>
                    <li>🧠 <strong>Smart Field Disambiguation</strong> for duplicate field names</li>
                    <li>⚡ <strong>Multiple Execution Options</strong> - Database, Mock, Export</li>
                    <li>📊 <strong>Mock Results Preview</strong> without database connection</li>
                    <li>📋 <strong>Export to Multiple Formats</strong> - Python, dbt, Jupyter, PowerBI</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)

            if json_data:
                # Enhanced analysis with disambiguation
                temp_schema, disambiguation_info = render_enhanced_disambiguation_info(json_data)

                # Layout for query configuration
                col_left, col_right = st.columns([2, 1])
                
                with col_left:
                    st.markdown("### 📝 Query Configuration")
                    
                    input_col1, input_col2 = st.columns(2)
                    
                    with input_col1:
                        table_name = st.text_input(
                            "Table Name*", 
                            key="py_table", 
                            placeholder="your_schema.your_table",
                            help="Snowflake table containing your JSON data"
                        )
                        
                        field_conditions = st.text_area(
                            "Field Conditions*", 
                            height=120, 
                            key="py_fields",
                            placeholder="e.g., name, company.name, departments.employees.name",
                            help="Specify JSON fields and optional conditions. Use full paths to avoid ambiguity."
                        )
                    
                    with input_col2:
                        json_column = st.text_input(
                            "JSON Column Name*", 
                            key="py_json_col", 
                            placeholder="json_data",
                            help="Name of the column containing JSON data in your table"
                        )
                        
                        # Enhanced execution mode selection
                        st.markdown("**🚀 Execution Options:**")
                        execution_mode = st.radio(
                            "Choose execution mode:",
                            [
                                "📝 Generate SQL Only",
                                "🏔️ Execute on Snowflake", 
                                "🔬 Mock Execution (Preview)",
                                "📋 Export for External Use"
                            ],
                            key="py_execution_mode",
                            help="Choose how to handle the generated SQL"
                        )

                    # Mode-specific options
                    if execution_mode == "🏔️ Execute on Snowflake":
                        limit_results = st.selectbox(
                            "Result Limit",
                            [10, 50, 100, 500, "No Limit"],
                            index=1,
                            key="py_result_limit",
                            help="Limit number of rows returned for preview"
                        )
                    elif execution_mode == "🔬 Mock Execution (Preview)":
                        mock_rows = st.selectbox(
                            "Mock Result Rows:",
                            [5, 10, 20, 50],
                            index=1,
                            key="py_mock_rows",
                            help="Number of sample rows to generate"
                        )
                    elif execution_mode == "📋 Export for External Use":
                        export_format = st.selectbox(
                            "Export Format:",
                            ["SQL File", "Python Script", "dbt Model", "Jupyter Notebook", "PowerBI Template"],
                            key="py_export_format",
                            help="Choose export format for your generated SQL"
                        )

                    # Enhanced field suggestions with disambiguation
                    render_enhanced_python_field_suggestions(temp_schema, disambiguation_info)

                with col_right:
                    st.markdown("### 🚀 Generation & Execution")
                    
                    # Execution mode display
                    mode_display = execution_mode.split(' ', 1)[1] if ' ' in execution_mode else execution_mode
                    st.markdown(f"""
                    <div class="execution-mode-box">
                        <h6 style="margin-bottom: 0.5rem; color: #1976d2;">🎯 Current Mode: {mode_display}</h6>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    st.markdown("---")
                    generate_btn = st.button(
                        f"🚀 {mode_display}",
                        type="primary",
                        use_container_width=True,
                        help=f"Generate SQL and {mode_display.lower()}"
                    )
                    
                    # Connection status for database execution
                    if execution_mode == "🏔️ Execute on Snowflake":
                        st.markdown("**🔗 Database Connection:**")
                        
                        conn_available = False
                        conn_manager = None
                        
                        # Look for active unified connection
                        if 'unified_connection_manager' in st.session_state:
                            conn_manager = st.session_state.unified_connection_manager
                            if conn_manager and hasattr(conn_manager, 'is_connected') and conn_manager.is_connected:
                                conn_available = True
                        
                        if conn_available:
                            st.success("✅ Connected to Snowflake")
                            if hasattr(conn_manager, 'connection_params'):
                                st.caption(f"Database: {conn_manager.connection_params.get('database', 'N/A')}")
                                st.caption(f"Schema: {conn_manager.connection_params.get('schema', 'N/A')}")
                        else:
                            st.warning("⚠️ No active connection")
                            st.caption("💡 Connect via Database tab first")
                    elif execution_mode == "🔬 Mock Execution (Preview)":
                        st.markdown("**🔬 Mock Preview:**")
                        st.info("✨ No database needed")
                        st.caption("Generate sample results based on JSON structure")
                    elif execution_mode == "📋 Export for External Use":
                        st.markdown("**📋 Export Ready:**")
                        st.info("📦 Generate portable code")
                        st.caption(f"Export as {export_format}")
                    
                    # Quick stats about current JSON
                    if temp_schema:
                        st.markdown("---")
                        st.markdown("**📊 JSON Structure Info:**")
                        
                        queryable_count = sum(1 for details in temp_schema.values() if details.get('is_queryable', False))
                        total_fields = len(temp_schema)
                        conflict_count = len(disambiguation_info) if disambiguation_info else 0
                        
                        st.metric("Queryable Fields", queryable_count)
                        st.metric("Total Fields", total_fields)
                        if conflict_count > 0:
                            st.metric("Name Conflicts", conflict_count)

                # Enhanced SQL generation with multiple execution modes
                if generate_btn:
                    if not all([table_name, json_column, field_conditions]):
                        st.error("❌ Please fill in all required fields marked with *.")
                    elif not json_data:
                        st.error("❌ Please provide JSON data via the sidebar first.")
                    else:
                        with st.spinner("🔍 Generating SQL with disambiguation analysis..."):
                            try:
                                # Generate SQL with enhanced features
                                sql, warnings, disambiguation_details = generate_enhanced_sql_python_mode(
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
                                        elif warning.startswith('✅'):
                                            st.success(warning)
                                        elif warning.startswith('🎯'):
                                            st.info(warning)
                                        else:
                                            st.info(warning)

                                # Handle different execution modes
                                if execution_mode == "📝 Generate SQL Only":
                                    st.success("✅ SQL Generated Successfully!")
                                    st.code(sql, language="sql")
                                    
                                    st.download_button(
                                        "📋 Download SQL Query",
                                        data=sql,
                                        file_name=f"generated_query_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql",
                                        mime="text/sql",
                                        help="Download the generated SQL query"
                                    )
                                    
                                elif execution_mode == "🏔️ Execute on Snowflake":
                                    if conn_available and conn_manager:
                                        # Apply result limit if specified
                                        limit_value = safe_get_session_state('py_result_limit', 50)
                                        if limit_value != "No Limit":
                                            if not sql.upper().rstrip(';').endswith('LIMIT'):
                                                limited_sql = sql.rstrip(';') + f' LIMIT {limit_value};'
                                            else:
                                                limited_sql = sql
                                        else:
                                            limited_sql = sql

                                        st.markdown("---")
                                        st.markdown("### 🎯 Query Results")

                                        # Show SQL in expandable section
                                        with st.expander("📜 Generated SQL Query", expanded=False):
                                            st.code(limited_sql, language="sql")

                                        with st.spinner("⚡ Executing query and fetching results..."):
                                            try:
                                                # Use the appropriate execution method based on connection mode
                                                if hasattr(conn_manager, 'enhanced_mode') and conn_manager.enhanced_mode:
                                                    # Enhanced mode with performance tracking
                                                    if hasattr(conn_manager, 'execute_query_with_performance'):
                                                        result_df, exec_error, perf_stats = conn_manager.execute_query_with_performance(limited_sql)
                                                    else:
                                                        result_df, exec_error = conn_manager.execute_query(limited_sql)
                                                        perf_stats = {}
                                                    
                                                    if result_df is not None:
                                                        st.success("✅ Query executed successfully with enhanced performance tracking!")
                                                        
                                                        # Display enhanced performance metrics
                                                        perf_col1, perf_col2, perf_col3, perf_col4 = st.columns(4)
                                                        with perf_col1:
                                                            st.metric("Rows Returned", len(result_df))
                                                        with perf_col2:
                                                            st.metric("Columns", len(result_df.columns))
                                                        with perf_col3:
                                                            processing_engine = "🚀 Modin" if perf_stats.get('modin_used', False) else "📊 Pandas"
                                                            st.metric("Engine", processing_engine)
                                                        with perf_col4:
                                                            execution_time = perf_stats.get('total_time', 0)
                                                            st.metric("Execution Time", f"{execution_time:.2f}s")

                                                        # Show results with enhanced formatting
                                                        st.markdown("#### 📊 Results Preview:")
                                                        st.dataframe(result_df, use_container_width=True)

                                                        # Enhanced download options
                                                        if not result_df.empty:
                                                            download_col1, download_col2 = st.columns(2)
                                                            
                                                            with download_col1:
                                                                csv_data = result_df.to_csv(index=False).encode('utf-8')
                                                                st.download_button(
                                                                    "📥 Download CSV",
                                                                    data=csv_data,
                                                                    file_name=f"python_sql_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                                                    mime="text/csv"
                                                                )
                                                            
                                                            with download_col2:
                                                                json_data_export = result_df.to_json(orient='records', indent=2)
                                                                st.download_button(
                                                                    "📥 Download JSON",
                                                                    data=json_data_export,
                                                                    file_name=f"python_sql_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                                                                    mime="application/json"
                                                                )

                                                        # Show enhanced summary
                                                        if len(result_df) > 0:
                                                            st.info(f"✨ **Enhanced Summary:** Successfully processed {len(result_df):,} rows with {len(result_df.columns)} columns in {execution_time:.2f}s using {processing_engine}")
                                                        
                                                    else:
                                                        st.error(f"❌ Query execution failed: {exec_error}")
                                                        
                                                else:
                                                    # Standard mode
                                                    result_df, exec_error = conn_manager.execute_query(limited_sql)
                                                    
                                                    if result_df is not None:
                                                        st.success("✅ Query executed successfully!")
                                                        
                                                        # Display standard metrics
                                                        metric_col1, metric_col2 = st.columns(2)
                                                        with metric_col1:
                                                            st.metric("Rows Returned", len(result_df))
                                                        with metric_col2:
                                                            st.metric("Columns", len(result_df.columns))

                                                        # Show results
                                                        st.markdown("#### 📊 Results Preview:")
                                                        st.dataframe(result_df, use_container_width=True)

                                                        # Download options
                                                        if not result_df.empty:
                                                            csv_data = result_df.to_csv(index=False).encode('utf-8')
                                                            st.download_button(
                                                                "📥 Download Results (CSV)",
                                                                data=csv_data,
                                                                file_name=f"python_sql_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                                                mime="text/csv"
                                                            )
                                                        
                                                    else:
                                                        st.error(f"❌ Query execution failed: {exec_error}")

                                            except Exception as e:
                                                st.error(f"❌ Execution error: {str(e)}")
                                                st.info("💡 Check your table name, column name, and database permissions.")

                                    else:
                                        st.warning("⚠️ **Database connection required for query execution**")
                                        st.markdown("""
                                        **To execute queries:**
                                        1. 🏔️ Go to the **Snowflake Database Connection** tab
                                        2. 🔐 Enter your connection details  
                                        3. 🔗 Connect to your database
                                        4. 🔄 Return here to generate and execute queries
                                        """)

                                elif execution_mode == "🔬 Mock Execution (Preview)":
                                    st.success("🔬 Mock execution preview generated!")
                                    
                                    # Show SQL in expandable section
                                    with st.expander("📜 Generated SQL Query", expanded=False):
                                        st.code(sql, language="sql")
                                    
                                    st.markdown("---")
                                    st.markdown("### 🎭 Mock Results Preview")
                                    st.markdown("""
                                    <div class="mock-results-box">
                                        <p><strong>🎯 Mock Data Generation:</strong> These results are generated based on your JSON structure 
                                        and field patterns. Actual database results may vary.</p>
                                    </div>
                                    """, unsafe_allow_html=True)
                                    
                                    # Generate mock results
                                    mock_rows_count = safe_get_session_state('py_mock_rows', 10)
                                    mock_df = generate_mock_results_from_json(
                                        json_data, field_conditions, mock_rows_count
                                    )
                                    
                                    if not mock_df.empty:
                                        # Display mock metrics
                                        mock_col1, mock_col2, mock_col3 = st.columns(3)
                                        with mock_col1:
                                            st.metric("Mock Rows", len(mock_df))
                                        with mock_col2:
                                            st.metric("Mock Columns", len(mock_df.columns))
                                        with mock_col3:
                                            st.metric("Data Source", "🎭 Generated")
                                        
                                        # Show mock results
                                        st.markdown("#### 🎭 Sample Results:")
                                        st.dataframe(mock_df, use_container_width=True)
                                        
                                        # Download mock results
                                        mock_csv = mock_df.to_csv(index=False).encode('utf-8')
                                        st.download_button(
                                            "📥 Download Mock Results (CSV)",
                                            data=mock_csv,
                                            file_name=f"mock_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                            mime="text/csv"
                                        )
                                        
                                        st.info("💡 **Next Steps:** Use this structure as a reference for your actual database queries!")
                                    
                                elif execution_mode == "📋 Export for External Use":
                                    export_format_val = safe_get_session_state('py_export_format', 'SQL File')
                                    
                                    st.success(f"📋 {export_format_val} generated successfully!")
                                    
                                    # Generate export content
                                    export_content = generate_export_content(
                                        sql, export_format_val, table_name, field_conditions
                                    )
                                    
                                    # Show export content preview
                                    with st.expander("👀 Export Content Preview", expanded=False):
                                        if export_format_val == "Jupyter Notebook":
                                            st.json(json.loads(export_content), expanded=False)
                                        else:
                                            st.code(export_content, language="sql" if "sql" in export_format_val.lower() else "python")
                                    
                                    # Download export
                                    file_extension = get_file_extension(export_format_val)
                                    mime_type = get_mime_type(export_format_val)
                                    
                                    st.download_button(
                                        f"📥 Download {export_format_val}",
                                        data=export_content,
                                        file_name=f"export_{table_name.replace('.', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{file_extension}",
                                        mime=mime_type,
                                        help=f"Download generated {export_format_val}"
                                    )
                                    
                                    # Show usage instructions
                                    st.markdown("---")
                                    st.markdown("### 💡 Usage Instructions")
                                    
                                    if export_format_val == "Python Script":
                                        st.info("""
                                        **🐍 Python Script Usage:**
                                        1. Update connection parameters in the script
                                        2. Install required packages: `pip install snowflake-connector-python pandas`
                                        3. Run: `python your_script.py`
                                        """)
                                    elif export_format_val == "dbt Model":
                                        st.info("""
                                        **🔧 dbt Model Usage:**
                                        1. Save as `.sql` file in your dbt models directory
                                        2. Run: `dbt run --models your_model_name`
                                        3. Test: `dbt test --models your_model_name`
                                        """)
                                    elif export_format_val == "Jupyter Notebook":
                                        st.info("""
                                        **📓 Jupyter Notebook Usage:**
                                        1. Open with Jupyter Lab/Notebook
                                        2. Update connection parameters in the cells
                                        3. Run cells sequentially for full analysis
                                        """)
                                    elif export_format_val == "PowerBI Template":
                                        st.info("""
                                        **📊 Power BI Usage:**
                                        1. Open Power BI Desktop
                                        2. Get Data → Snowflake
                                        3. Use Advanced options and paste the provided query
                                        """)

                                # Show additional disambiguation details if present
                                render_disambiguation_details(sql, warnings, field_conditions, disambiguation_details)
                            
                            except Exception as e:
                                st.error(f"❌ SQL generation error: {str(e)}")
                                st.info("💡 Please check your JSON structure and field conditions.")

                # Examples section
                st.markdown("---")
                st.markdown("### 💡 Examples & Help")
                
                # Context-aware examples
                if temp_schema:
                    example_col1, example_col2 = st.columns(2)
                    
                    with example_col1:
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
                    
                    with example_col2:
                        st.markdown("**📋 General Examples:**")
                        examples = [
                            "name, age, email",
                            "user.name, user.profile.age[>:18]", 
                            "status[=:active], created_date[IS NOT NULL]",
                            "tags[IN:premium|gold], score[>:100]"
                        ]
                        for ex in examples:
                            st.code(ex, language="text")
                else:
                    # Standard examples when no JSON is loaded
                    st.markdown("**📋 Standard Examples:**")
                    example_cols = st.columns(2)
                    
                    with example_cols[0]:
                        examples1 = [
                            "name, age, email",
                            "user.name, user.profile.age[>:18]"
                        ]
                        for ex in examples1:
                            st.code(ex, language="text")
                            
                    with example_cols[1]:
                        examples2 = [
                            "status[=:active], created_date[IS NOT NULL]",
                            "tags[IN:premium|gold], score[>:100]"
                        ]
                        for ex in examples2:
                            st.code(ex, language="text")

                # Advanced help section
                with st.expander("🔧 Advanced Help & Tips", expanded=False):
                    help_col1, help_col2 = st.columns(2)
                    
                    with help_col1:
                        st.markdown("**🎯 Field Condition Operators:**")
                        st.markdown("""
                        - `[IS NOT NULL]` - Field must have a value
                        - `[=:value]` - Field equals specific value  
                        - `[>:100]` / `[<:100]` - Numeric comparisons
                        - `[IN:val1|val2]` - Field in list of values
                        - `[LIKE:pattern]` - Text pattern matching
                        """)
                        
                    with help_col2:
                        st.markdown("**💡 Execution Mode Tips:**")
                        st.markdown("""
                        - **📝 SQL Only:** Generate queries without execution
                        - **🏔️ Snowflake:** Live database execution with results
                        - **🔬 Mock:** Preview structure without database
                        - **📋 Export:** Portable code for external tools
                        """)

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
            <p><strong>🚀 Enhanced JSON-to-SQL Analyzer</strong> | Built with ❤️ using Streamlit</p>
            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 2rem; margin-top: 1rem; text-align: center;">
                <div>
                    <h4 style="color: #1976d2;">🐍 Python Mode</h4>
                    <p>Multiple execution options<br/>Mock results & exports<br/>No database required</p>
                </div>
                <div>
                    <h4 style="color: #2e7d32;">🏔️ Database Mode</h4>
                    <p>Live Snowflake connectivity<br/>Enhanced performance modes<br/>Real database operations</p>
                </div>
                <div>
                    <h4 style="color: #9c27b0;">🚀 New Features</h4>
                    <p>Mock execution preview<br/>Export to 5+ formats<br/>Smart disambiguation</p>
                </div>
            </div>
            <hr style="margin: 2rem 0; border: 1px solid #e9ecef;">
            <p><small>
                <strong>🎯 Smart Features:</strong> Field disambiguation, mock results, and portable exports!<br/>
                <strong>⚡ Performance:</strong> Enhanced mode provides Modin acceleration for datasets >1000 rows<br/>
                <strong>📋 Export Options:</strong> SQL, Python, dbt, Jupyter Notebook, Power BI templates
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
