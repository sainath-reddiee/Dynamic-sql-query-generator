"""
Snowflake Database Connector for direct database operations
Independent module that doesn't interfere with pure Python SQL generation
"""
import streamlit as st
import pandas as pd
import json
from typing import Dict, Any, Optional, Tuple
import logging
from datetime import datetime
from python_sql_generator import generate_sql_from_json_data

# Try to import snowflake connector, handle gracefully if not available
try:
    import snowflake.connector
    from snowflake.connector import DictCursor
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False

logger = logging.getLogger(__name__)


class SnowflakeConnectionManager:
    """Manages Snowflake database connections and operations"""

    def __init__(self):
        self.connection = None
        self.connection_params = {}
        self.is_connected = False

    def test_connection(self, connection_params: Dict[str, str]) -> Tuple[bool, str]:
        """Test Snowflake connection parameters"""
        if not SNOWFLAKE_AVAILABLE:
            return False, "❌ Snowflake connector not available. Install with: pip install snowflake-connector-python"

        try:
            # Test connection
            test_conn = snowflake.connector.connect(**connection_params)
            test_cursor = test_conn.cursor()
            test_cursor.execute("SELECT CURRENT_VERSION()")
            version = test_cursor.fetchone()
            test_cursor.close()
            test_conn.close()

            return True, f"✅ Connected successfully! Snowflake version: {version[0]}"

        except Exception as e:
            error_msg = str(e)
            if "Authentication" in error_msg:
                return False, "❌ Authentication failed. Please check your username and password."
            elif "Account" in error_msg:
                return False, "❌ Account identifier is invalid. Please check your account name."
            elif "Database" in error_msg:
                return False, "❌ Database or schema not found. Please verify they exist."
            elif "Network" in error_msg or "timeout" in error_msg.lower():
                return False, "❌ Network connection failed. Check your internet connection."
            else:
                return False, f"❌ Connection failed: {error_msg}"

    def connect(self, connection_params: Dict[str, str]) -> bool:
        """Establish persistent connection"""
        if not SNOWFLAKE_AVAILABLE:
            st.error("❌ Snowflake connector not available. Please install snowflake-connector-python")
            return False

        try:
            self.connection = snowflake.connector.connect(**connection_params)
            self.connection_params = connection_params.copy()
            self.is_connected = True
            return True
        except Exception as e:
            st.error(f"❌ Failed to establish connection: {str(e)}")
            return False

    def disconnect(self):
        """Close the connection"""
        if self.connection:
            try:
                self.connection.close()
            except:
                pass
            finally:
                self.connection = None
                self.is_connected = False
                self.connection_params = {}

    def execute_query(self, sql: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """Execute SQL query and return DataFrame"""
        if not self.is_connected:
            return None, "❌ Not connected to database"

        try:
            cursor = self.connection.cursor(DictCursor)
            cursor.execute(sql)

            # Get column names
            columns = [desc[0] for desc in cursor.description] if cursor.description else []

            # Fetch results
            rows = cursor.fetchall()
            cursor.close()

            if rows and columns:
                df = pd.DataFrame(rows, columns=columns)
                return df, None
            else:
                return pd.DataFrame(), None

        except Exception as e:
            error_msg = str(e)
            return None, f"❌ Query execution failed: {error_msg}"

    def cleanup_temp_table(self, table_name: str) -> bool:
        """Clean up temporary table"""
        if not self.is_connected:
            return False

        try:
            cursor = self.connection.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            cursor.close()
            return True
        except Exception as e:
            logger.warning(f"Failed to cleanup temp table {table_name}: {e}")
            return False

    def list_tables(self, schema_name: str = None) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """List available tables in the current database/schema"""
        if not self.is_connected:
            return None, "❌ Not connected to database"

        try:
            cursor = self.connection.cursor(DictCursor)

            if schema_name:
                sql = f"SHOW TABLES IN SCHEMA {schema_name}"
            else:
                sql = "SHOW TABLES"

            cursor.execute(sql)
            rows = cursor.fetchall()
            cursor.close()

            if rows:
                df = pd.DataFrame(rows)
                return df, "✅ Tables retrieved successfully"
            else:
                return pd.DataFrame(), "ℹ️ No tables found"

        except Exception as e:
            return None, f"❌ Failed to list tables: {str(e)}"


def render_snowflake_connection_ui() -> Optional[SnowflakeConnectionManager]:
    """Render Snowflake connection UI and return connection manager if successful"""

    if not SNOWFLAKE_AVAILABLE:
        st.error("""
        ❌ **Snowflake Connector Not Available**

        To use database connectivity features, please install the Snowflake connector:
        ```bash
        pip install snowflake-connector-python
        ```
        """)
        return None

    st.markdown("""
    <div style="background: linear-gradient(145deg, #e3f2fd, #f8f9fa); padding: 1.5rem; border-radius: 10px; border: 1px solid #90caf9; margin-bottom: 1rem;">
        <h4 style="color: #1976d2; margin-bottom: 1rem;">🔗 Snowflake Database Connection</h4>
        <p style="margin-bottom: 0.5rem;">Connect to your Snowflake database to:</p>
        <ul style="margin-bottom: 0;">
            <li>📊 Execute SQL directly on your data</li>
            <li>🏔️ Use your existing stored procedures</li>
            <li>💾 Store and query JSON data in temporary tables</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)

    # Connection form
    with st.form("snowflake_connection_form", clear_on_submit=False):
        col1, col2 = st.columns(2)

        with col1:
            account = st.text_input(
                "Account Identifier*",
                placeholder="your-account.region.cloud",
                help="Your Snowflake account identifier (e.g., abc123.us-east-1.aws)"
            )
            user = st.text_input(
                "Username*",
                placeholder="your_username",
                help="Your Snowflake username"
            )
            password = st.text_input(
                "Password*",
                type="password",
                placeholder="your_password",
                help="Your Snowflake password"
            )

        with col2:
            warehouse = st.text_input(
                "Warehouse*",
                placeholder="COMPUTE_WH",
                help="Warehouse to use for computations"
            )
            database = st.text_input(
                "Database*",
                placeholder="your_database",
                help="Database name"
            )
            schema = st.text_input(
                "Schema*",
                placeholder="PUBLIC",
                value="PUBLIC",
                help="Schema name"
            )

        # Connection options
        st.markdown("**Advanced Options (Optional):**")
        col3, col4 = st.columns(2)

        with col3:
            role = st.text_input(
                "Role",
                placeholder="your_role",
                help="Role to assume (optional)"
            )

        with col4:
            timeout = st.number_input(
                "Timeout (seconds)",
                min_value=30,
                max_value=300,
                value=60,
                help="Connection timeout"
            )

        # Form buttons
        col5, col6, col7 = st.columns([1, 1, 2])

        with col5:
            test_connection = st.form_submit_button("🧪 Test Connection", type="secondary")

        with col6:
            connect_button = st.form_submit_button("🔗 Connect", type="primary")

    # Validate required fields
    required_fields = {
        'Account': account,
        'Username': user,
        'Password': password,
        'Warehouse': warehouse,
        'Database': database,
        'Schema': schema
    }

    missing_fields = [name for name, value in required_fields.items() if not value or not value.strip()]

    if test_connection or connect_button:
        if missing_fields:
            st.error(f"❌ Please fill in required fields: {', '.join(missing_fields)}")
            return None

    # Build connection parameters
    connection_params = {
        'account': account,
        'user': user,
        'password': password,
        'warehouse': warehouse,
        'database': database,
        'schema': schema,
        'login_timeout': timeout
    }

    if role and role.strip():
        connection_params['role'] = role

    # Initialize connection manager
    conn_manager = SnowflakeConnectionManager()

    # Handle test connection
    if test_connection and not missing_fields:
        with st.spinner("🔄 Testing connection..."):
            success, message = conn_manager.test_connection(connection_params)

            if success:
                st.success(message)
                st.balloons()
            else:
                st.error(message)

        return None  # Don't connect, just test

    # Handle actual connection
    if connect_button and not missing_fields:
        with st.spinner("🔄 Connecting to Snowflake..."):
            if conn_manager.connect(connection_params):
                st.success("✅ **Successfully connected to Snowflake!**")
                st.session_state.snowflake_connection = conn_manager
                st.balloons()
                return conn_manager
            else:
                return None

    # Check if already connected
    if 'snowflake_connection' in st.session_state:
        existing_conn = st.session_state.snowflake_connection
        if existing_conn.is_connected:
            st.success("✅ **Already connected to Snowflake!**")
            return existing_conn

    return None


def render_snowflake_operations_ui(conn_manager: SnowflakeConnectionManager, json_data: Any = None):
    """Render UI for Snowflake database operations with database-driven analysis"""

    st.markdown("""
    <div style="background: linear-gradient(145deg, #e8f5e8, #f8f9fa); padding: 1.5rem; border-radius: 10px; border: 1px solid #81c784; margin-bottom: 1rem;">
        <h4 style="color: #388e3c; margin-bottom: 1rem;">🏔️ Snowflake Database Operations</h4>
        <p style="margin-bottom: 0;">Analyze JSON data directly from your Snowflake database tables and generate optimized queries.</p>
    </div>
    """, unsafe_allow_html=True)

    # Operation tabs
    db_tab1, db_tab2, db_tab3, db_tab4 = st.tabs([
        "🧪 **Smart JSON Analysis**", 
        "📊 **Custom Queries**", 
        "🔍 **Schema Discovery**",
        "🔧 **Database Info**"
    ])

    with db_tab1:
        st.subheader("🧪 Database-Driven JSON Analysis")
        
        st.markdown("""
        <div style="background: #e3f2fd; padding: 1rem; border-radius: 6px; margin: 1rem 0;">
            <h5 style="color: #1976d2;">🎯 How it works:</h5>
            <ol>
                <li><strong>Samples</strong> JSON data directly from your database table</li>
                <li><strong>Analyzes</strong> the JSON structure automatically</li>
                <li><strong>Generates</strong> optimized SQL based on discovered schema</li>
                <li><strong>Executes</strong> the query and shows results</li>
            </ol>
            <p><em>No need to upload JSON files - we analyze your actual database content!</em></p>
        </div>
        """, unsafe_allow_html=True)

        # Parameters for database-driven analysis
        col1, col2 = st.columns(2)

        with col1:
            table_name_db = st.text_input(
                "Table Name* 🏗️",
                placeholder="SCHEMA_NAME.TABLE_NAME",
                key="db_table_name",
                help="Full table name including schema (e.g., MY_SCHEMA.JSON_DATA_TABLE)"
            )
            
            sample_size = st.selectbox(
                "Schema Analysis Sample Size 📊",
                [5, 10, 20, 50],
                index=1,
                key="sample_size",
                help="Number of records to sample for JSON schema discovery"
            )

        with col2:
            json_column_db = st.text_input(
                "JSON Column Name* 📄",
                placeholder="json_data",
                key="db_json_column",
                help="Name of the column containing JSON data"
            )
            
            preview_schema = st.checkbox(
                "Preview Discovered Schema 👀",
                value=True,
                key="preview_schema",
                help="Show the JSON schema discovered from your database"
            )

        # Schema discovery section
        if st.button("🔍 Discover JSON Schema", type="secondary"):
            if not all([table_name_db, json_column_db]):
                st.warning("⚠️ Please provide table name and JSON column name.")
            else:
                try:
                    from db_json_analyzer import analyze_database_json_schema, render_database_json_preview
                    
                    schema, error_msg, metadata = analyze_database_json_schema(
                        conn_manager, table_name_db, json_column_db
                    )
                    
                    if schema:
                        st.success(f"✅ **Schema Discovery Complete!** Found {len(schema)} JSON fields.")
                        
                        if preview_schema:
                            render_database_json_preview(schema, metadata)
                        
                        # Store schema in session state for use in field conditions
                        st.session_state.discovered_schema = schema
                        st.session_state.schema_metadata = metadata
                        
                    else:
                        st.error(error_msg)
                        
                except Exception as e:
                    st.error(f"❌ Schema discovery failed: {str(e)}")

        # Field conditions with smart suggestions
        st.markdown("---")
        field_conditions_db = st.text_area(
            "Field Conditions: 🎯",
            height=80,
            placeholder="e.g., name, age[>:18], status[=:active]",
            key="db_field_conditions",
            help="Specify which JSON fields to query and their conditions"
        )

        # Smart suggestions based on discovered schema
        if 'discovered_schema' in st.session_state:
            with st.expander("💡 Smart Field Suggestions (Click to expand)"):
                try:
                    from db_json_analyzer import render_suggested_field_conditions
                    suggestions = render_suggested_field_conditions(st.session_state.discovered_schema)
                    
                    if suggestions:
                        st.markdown("**Suggested field conditions based on your data:**")
                        for i, suggestion in enumerate(suggestions[:5]):
                            col_a, col_b = st.columns([3, 1])
                            with col_a:
                                st.code(suggestion, language="text")
                            with col_b:
                                if st.button("Use This", key=f"use_suggestion_{i}"):
                                    st.session_state.db_field_conditions = suggestion
                                    st.rerun()
                    else:
                        st.info("No specific suggestions available for this schema.")
                except Exception as e:
                    st.warning(f"Could not generate suggestions: {e}")

        # Generate and execute query
        col3, col4 = st.columns(2)
        
        with col3:
            generate_btn = st.button("🚀 Generate Smart SQL", type="primary")
        
        with col4:
            execute_btn = st.button("⚡ Generate & Execute", type="primary")

        if generate_btn or execute_btn:
            if not all([table_name_db, json_column_db, field_conditions_db]):
                st.warning("⚠️ Please fill in all required fields.")
            else:
                try:
                    from db_json_analyzer import generate_database_driven_sql
                    
                    # Generate SQL using database-driven analysis
                    generated_sql, sql_error = generate_database_driven_sql(
                        conn_manager, table_name_db, json_column_db, field_conditions_db
                    )

                    if generated_sql:
                        st.success("✅ **Smart SQL Generated Successfully!**")
                        
                        st.subheader("📄 Generated SQL Query")
                        st.code(generated_sql, language="sql")
                        
                        # Copy to clipboard functionality
                        st.markdown(f"""
                        <div style="margin: 1rem 0;">
                            <small>💡 <strong>Tip:</strong> This query was generated by analyzing your actual database content!</small>
                        </div>
                        """, unsafe_allow_html=True)

                        # Execute if requested
                        if execute_btn:
                            with st.spinner("🔄 Executing generated SQL on Snowflake..."):
                                result_df, exec_error = conn_manager.execute_query(generated_sql)

                                if result_df is not None:
                                    st.subheader("📊 Query Results")
                                    
                                    # Results summary
                                    col_summary1, col_summary2, col_summary3 = st.columns(3)
                                    with col_summary1:
                                        st.metric("Rows Returned", len(result_df))
                                    with col_summary2:
                                        st.metric("Columns", len(result_df.columns))
                                    with col_summary3:
                                        st.metric("Data Size", f"{result_df.memory_usage(deep=True).sum() / 1024:.1f} KB")
                                    
                                    # Display results
                                    st.dataframe(result_df, use_container_width=True)
                                    
                                    # Download option
                                    if not result_df.empty:
                                        csv_data = result_df.to_csv(index=False).encode('utf-8')
                                        st.download_button(
                                            "📥 Download Results as CSV",
                                            data=csv_data,
                                            file_name=f"smart_analysis_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                            mime="text/csv"
                                        )
                                else:
                                    st.error(exec_error)
                    else:
                        st.error(f"❌ {sql_error}")

                except Exception as e:
                    st.error(f"❌ Analysis failed: {str(e)}")

    with db_tab2:
        st.subheader("📊 Execute Custom SQL")

        custom_sql = st.text_area(
            "Enter your SQL query:",
            height=200,
            placeholder="""SELECT json_data:name::VARCHAR as name,
       json_data:age::NUMBER as age
FROM your_table
WHERE json_data:status::VARCHAR = 'active'
LIMIT 10;""",
            help="Write any SQL query to execute on your Snowflake database"
        )

        col1, col2 = st.columns([1, 3])

        with col1:
            if st.button("▶️ Execute SQL", type="primary"):
                if not custom_sql.strip():
                    st.warning("⚠️ Please enter a SQL query")
                else:
                    with st.spinner("🔄 Executing query..."):
                        result_df, error_msg = conn_manager.execute_query(custom_sql)

                        if result_df is not None:
                            st.success("✅ Query executed successfully")
                            st.dataframe(result_df, use_container_width=True)

                            # Download option
                            if not result_df.empty:
                                csv_data = result_df.to_csv(index=False)
                                st.download_button(
                                    "📥 Download Results",
                                    data=csv_data,
                                    file_name=f"query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                    mime="text/csv"
                                )
                        else:
                            st.error(error_msg)

    with db_tab3:
        st.subheader("🔍 JSON Schema Discovery Tools")
        
        st.markdown("""
        <div style="background: #fff3e0; padding: 1rem; border-radius: 6px; margin: 1rem 0;">
            <h5 style="color: #f57c00;">🔍 Advanced Schema Analysis</h5>
            <p>Explore and understand the JSON structure in your database tables.</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Table and column selection
        schema_table = st.text_input("Table Name:", placeholder="SCHEMA.TABLE", key="schema_table")
        schema_column = st.text_input("JSON Column:", placeholder="json_column", key="schema_column")
        
        col1, col2 = st.columns(2)
        with col1:
            sample_count = st.number_input("Sample Size:", min_value=1, max_value=100, value=10, key="schema_sample")
        with col2:
            show_samples = st.checkbox("Show Sample Values", value=True, key="show_samples")
        
        if st.button("🔍 Analyze Schema", type="secondary"):
            if schema_table and schema_column:
                try:
                    from db_json_analyzer import analyze_database_json_schema, render_database_json_preview
                    
                    with st.spinner("Analyzing JSON schema..."):
                        schema, error, metadata = analyze_database_json_schema(
                            conn_manager, schema_table, schema_column
                        )
                        
                        if schema:
                            st.success("✅ Schema analysis complete!")
                            render_database_json_preview(schema, metadata)
                            
                            # Export schema option
                            import pandas as pd
                            schema_export = []
                            for path, details in schema.items():
                                schema_export.append({
                                    'Field Path': path,
                                    'Type': details.get('snowflake_type', 'VARIANT'),
                                    'Frequency': f"{details.get('frequency', 0):.1%}",
                                    'Queryable': details.get('is_queryable', False),
                                    'Sample Values': ', '.join(details.get('sample_values', ['N/A'])[:3])
                                })
                            
                            schema_df = pd.DataFrame(schema_export)
                            csv_data = schema_df.to_csv(index=False)
                            st.download_button(
                                "📥 Export Schema Analysis",
                                data=csv_data,
                                file_name=f"json_schema_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv"
                            )
                        else:
                            st.error(error)
                            
                except Exception as e:
                    st.error(f"Schema analysis failed: {e}")
            else:
                st.warning("Please provide table and column names.")

    with db_tab4:
        st.subheader("🔧 Database Information")

        col1, col2 = st.columns(2)

        with col1:
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

        with col2:
            if st.button("🔌 Disconnect", type="secondary"):
                conn_manager.disconnect()
                if 'snowflake_connection' in st.session_state:
                    del st.session_state.snowflake_connection
                # Clear discovered schema as well
                if 'discovered_schema' in st.session_state:
                    del st.session_state.discovered_schema
                st.success("✅ Disconnected from Snowflake")
                st.rerun()

        # Connection info
        if conn_manager.is_connected:
            st.markdown("**Current Connection:**")
            conn_info = {
                'Account': conn_manager.connection_params.get('account', 'N/A'),
                'Database': conn_manager.connection_params.get('database', 'N/A'),
                'Schema': conn_manager.connection_params.get('schema', 'N/A'),
                'Warehouse': conn_manager.connection_params.get('warehouse', 'N/A')
            }

            for key, value in conn_info.items():
                st.text(f"{key}: {value}")
