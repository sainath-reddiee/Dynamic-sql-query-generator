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


def render_snowflake_operations_ui(conn_manager: SnowflakeConnectionManager, json_data: Any):
    """Render UI for Snowflake database operations"""

    st.markdown("""
    <div style="background: linear-gradient(145deg, #e8f5e8, #f8f9fa); padding: 1.5rem; border-radius: 10px; border: 1px solid #81c784; margin-bottom: 1rem;">
        <h4 style="color: #388e3c; margin-bottom: 1rem;">🏔️ Snowflake Database Operations</h4>
        <p style="margin-bottom: 0;">Execute SQL queries directly on your Snowflake database using your JSON data.</p>
    </div>
    """, unsafe_allow_html=True)

    # Operation tabs
    db_tab1, db_tab2, db_tab3 = st.tabs(["🧪 Quick Analysis", "📊 Custom Queries", "🔧 Database Info"])

    with db_tab1:
        st.subheader("🧪 Quick JSON Analysis")

        # Parameters for quick analysis
        col1, col2 = st.columns(2)

        with col1:
            table_name_db = st.text_input(
                "Table Name*",
                placeholder="your_schema.your_table",
                key="db_table_name"
            )
        with col2:
            json_column_db = st.text_input(
                "JSON Column Name*",
                placeholder="json_data",
                key="db_json_column"
            )

        field_conditions_db = st.text_area(
            "Field Conditions:",
            height=80,
            placeholder="e.g., name, age[>:18], status[=:active]",
            key="db_field_conditions"
        )

        if st.button("🚀 Run Quick Analysis", type="primary"):
            if not all([table_name_db, json_column_db, field_conditions_db]):
                st.warning("⚠️ Please fill in all required fields.")
            else:
                try:
                    with st.spinner("🔄 Generating SQL from JSON data..."):
                        # NOTE: We still need json_data here if the user wants to analyze a file against a DB schema
                        # If json_data is None, this implies direct-to-DB analysis which requires a different logic path.
                        # For now, we assume the user provides a sample JSON for schema inference.
                        if json_data is None:
                            st.warning("⚠️ For Quick Analysis, please provide a sample JSON file in the 'Pure Python' tab for schema inference.")
                            return

                        generated_sql = generate_sql_from_json_data(
                            json_data, table_name_db, json_column_db, field_conditions_db
                        )

                    if generated_sql and not generated_sql.strip().startswith("-- Error"):
                        st.success("✅ SQL Generated Successfully")
                        st.subheader("📄 Generated SQL")
                        st.code(generated_sql, language="sql")

                        with st.spinner("🔄 Executing generated SQL on Snowflake..."):
                            result_df, exec_msg = conn_manager.execute_query(generated_sql)

                            if result_df is not None:
                                st.subheader("📊 Query Results")
                                st.dataframe(result_df, use_container_width=True)
                                if not result_df.empty:
                                    csv_data = result_df.to_csv(index=False).encode('utf-8')
                                    st.download_button(
                                        "📥 Download Results as CSV",
                                        data=csv_data,
                                        file_name=f"snowflake_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                        mime="text/csv"
                                    )
                            else:
                                st.error(exec_msg)
                    else:
                        st.error(f"❌ Failed to generate SQL. Please check your field conditions. Details: {generated_sql}")

                except Exception as e:
                    st.error(f"❌ An unexpected error occurred during analysis: {str(e)}")

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
