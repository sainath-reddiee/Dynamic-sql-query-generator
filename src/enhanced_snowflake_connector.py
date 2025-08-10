"""
Enhanced Snowflake Database Connector with proper session initialization
Fixes the "This session does not have a current database" error
"""
import streamlit as st
import pandas as pd
import json
from typing import Dict, Any, Optional, Tuple
import logging
from datetime import datetime

# Try to import snowflake connector, handle gracefully if not available
try:
    import snowflake.connector
    from snowflake.connector import DictCursor
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False

logger = logging.getLogger(__name__)


class EnhancedSnowflakeConnectionManager:
    """Enhanced Snowflake connection manager with proper session initialization"""

    def __init__(self):
        self.connection = None
        self.connection_params = {}
        self.is_connected = False

    def test_connection(self, connection_params: Dict[str, str]) -> Tuple[bool, str]:
        """Test Snowflake connection parameters with proper session setup"""
        if not SNOWFLAKE_AVAILABLE:
            return False, "❌ Snowflake connector not available. Install with: pip install snowflake-connector-python"

        try:
            # Test connection with enhanced session setup
            test_conn = snowflake.connector.connect(**connection_params)
            test_cursor = test_conn.cursor()
            
            # CRITICAL: Explicitly set the database and schema context
            database = connection_params.get('database')
            schema = connection_params.get('schema', 'PUBLIC')
            
            if database:
                test_cursor.execute(f"USE DATABASE {database}")
                test_cursor.execute(f"USE SCHEMA {schema}")
            
            # Verify the connection and context
            test_cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_VERSION()")
            result = test_cursor.fetchone()
            test_cursor.close()
            test_conn.close()

            if result and result[0]:  # Ensure database is set
                return True, f"✅ Connected successfully! Database: {result[0]}, Schema: {result[1]}, Version: {result[2]}"
            else:
                return False, "❌ Connected but no active database. Check your permissions."

        except Exception as e:
            error_msg = str(e)
            if "Authentication" in error_msg:
                return False, "❌ Authentication failed. Please check your username and password."
            elif "Account" in error_msg:
                return False, "❌ Account identifier is invalid. Please check your account name."
            elif "Database" in error_msg or "does not exist" in error_msg:
                return False, "❌ Database or schema not found. Please verify they exist and you have access."
            elif "Network" in error_msg or "timeout" in error_msg.lower():
                return False, "❌ Network connection failed. Check your internet connection."
            elif "permission" in error_msg.lower() or "access" in error_msg.lower():
                return False, "❌ Access denied. Check your role and permissions."
            else:
                return False, f"❌ Connection failed: {error_msg}"

    def connect(self, connection_params: Dict[str, str]) -> bool:
        """Establish persistent connection with proper session initialization"""
        if not SNOWFLAKE_AVAILABLE:
            st.error("❌ Snowflake connector not available. Please install snowflake-connector-python")
            return False

        try:
            # Create connection
            self.connection = snowflake.connector.connect(**connection_params)
            cursor = self.connection.cursor()
            
            # CRITICAL: Set database and schema context immediately after connection
            database = connection_params.get('database')
            schema = connection_params.get('schema', 'PUBLIC')
            
            if database:
                cursor.execute(f"USE DATABASE {database}")
                cursor.execute(f"USE SCHEMA {schema}")
                
                # Verify the context is set
                cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
                result = cursor.fetchone()
                
                if not result or not result[0]:
                    cursor.close()
                    self.connection.close()
                    return False
            
            cursor.close()
            self.connection_params = connection_params.copy()
            self.is_connected = True
            return True
            
        except Exception as e:
            st.error(f"❌ Failed to establish connection: {str(e)}")
            if self.connection:
                try:
                    self.connection.close()
                except:
                    pass
            return False

    def ensure_session_context(self) -> bool:
        """Ensure the session has proper database/schema context"""
        if not self.is_connected or not self.connection:
            return False
        
        try:
            cursor = self.connection.cursor()
            
            # Check current context
            cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
            result = cursor.fetchone()
            
            # If no database context, re-establish it
            if not result or not result[0]:
                database = self.connection_params.get('database')
                schema = self.connection_params.get('schema', 'PUBLIC')
                
                if database:
                    cursor.execute(f"USE DATABASE {database}")
                    cursor.execute(f"USE SCHEMA {schema}")
            
            cursor.close()
            return True
            
        except Exception as e:
            logger.warning(f"Failed to ensure session context: {e}")
            return False

    def execute_query(self, sql: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """Execute SQL query with automatic session context management"""
        if not self.is_connected:
            return None, "❌ Not connected to database"

        try:
            # Ensure session context before executing query
            if not self.ensure_session_context():
                return None, "❌ Failed to establish database session context"
            
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
            
            # Handle specific session context errors
            if "does not have a current database" in error_msg:
                # Try to re-establish context and retry once
                try:
                    database = self.connection_params.get('database')
                    schema = self.connection_params.get('schema', 'PUBLIC')
                    
                    if database:
                        cursor = self.connection.cursor()
                        cursor.execute(f"USE DATABASE {database}")
                        cursor.execute(f"USE SCHEMA {schema}")
                        cursor.close()
                        
                        # Retry the original query
                        cursor = self.connection.cursor(DictCursor)
                        cursor.execute(sql)
                        columns = [desc[0] for desc in cursor.description] if cursor.description else []
                        rows = cursor.fetchall()
                        cursor.close()
                        
                        if rows and columns:
                            df = pd.DataFrame(rows, columns=columns)
                            return df, None
                        else:
                            return pd.DataFrame(), None
                            
                except Exception as retry_e:
                    return None, f"❌ Database context error (retry failed): {str(retry_e)}"
            
            return None, f"❌ Query execution failed: {error_msg}"

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


def sample_json_from_database_fixed(conn_manager, table_name: str, json_column: str, 
                                  sample_size: int = 5) -> Tuple[Optional[list], Optional[str]]:
    """
    Enhanced JSON sampling with proper table name handling and context management
    """
    if not conn_manager.is_connected:
        return None, "❌ Not connected to database"
    
    try:
        # Ensure database context
        if not conn_manager.ensure_session_context():
            return None, "❌ Failed to establish database session context"
        
        # Handle table name - if it doesn't contain a dot, prepend current database.schema
        if '.' not in table_name:
            database = conn_manager.connection_params.get('database')
            schema = conn_manager.connection_params.get('schema', 'PUBLIC')
            table_name = f"{database}.{schema}.{table_name}"
        elif table_name.count('.') == 1:
            # Only schema.table provided, add database
            database = conn_manager.connection_params.get('database')
            table_name = f"{database}.{table_name}"
        
        # Query to sample JSON data with explicit table qualification
        sample_query = f"""
        SELECT {json_column}
        FROM {table_name}
        WHERE {json_column} IS NOT NULL
        LIMIT {sample_size}
        """
        
        result_df, error_msg = conn_manager.execute_query(sample_query)
        
        if result_df is None:
            return None, f"❌ Failed to sample data: {error_msg}"
        
        if result_df.empty:
            return None, f"❌ No data found in {table_name}.{json_column}"
        
        # Extract JSON data from the results
        json_samples = []
        for _, row in result_df.iterrows():
            json_value = row[json_column]
            
            if json_value is not None:
                try:
                    # Handle different JSON storage formats
                    if isinstance(json_value, str):
                        parsed_json = json.loads(json_value)
                    elif isinstance(json_value, dict):
                        parsed_json = json_value
                    else:
                        # Convert to string and try to parse
                        parsed_json = json.loads(str(json_value))
                    
                    json_samples.append(parsed_json)
                    
                except (json.JSONDecodeError, TypeError) as e:
                    logger.warning(f"Failed to parse JSON from row: {e}")
                    continue
        
        if not json_samples:
            return None, f"❌ No valid JSON found in sampled records from {table_name}.{json_column}"
        
        return json_samples, None
        
    except Exception as e:
        error_msg = str(e)
        
        # Provide more specific error messages
        if "does not exist" in error_msg:
            return None, f"❌ Table {table_name} does not exist or you don't have access to it"
        elif "Invalid identifier" in error_msg:
            return None, f"❌ Invalid column name '{json_column}' in table {table_name}"
        elif "permission" in error_msg.lower():
            return None, f"❌ Permission denied accessing {table_name}"
        else:
            return None, f"❌ Database sampling failed: {error_msg}"


# Enhanced connection UI function
def render_enhanced_snowflake_connection_ui() -> Optional[EnhancedSnowflakeConnectionManager]:
    """Render enhanced Snowflake connection UI with better error handling"""
    
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
    <div style="background: linear-gradient(145deg, #e8f5e8, #f1f8e9); padding: 1.5rem; border-radius: 10px; border: 2px solid #81c784; margin-bottom: 1rem;">
        <h4 style="color: #2e7d32; margin-bottom: 1rem;">🔗 Enhanced Snowflake Connection</h4>
        <p style="margin-bottom: 0.5rem;"><strong>✅ Enhanced Features:</strong></p>
        <ul style="margin-bottom: 0; color: #1b5e20;">
            <li>🎯 <strong>Automatic session context management</strong></li>
            <li>🛡️ <strong>Fixed database context issues</strong></li>
            <li>🔧 <strong>Better error handling and diagnostics</strong></li>
            <li>📊 <strong>Smart table name resolution</strong></li>
        </ul>
    </div>
    """, unsafe_allow_html=True)

    # Connection form
    with st.form("enhanced_snowflake_connection", clear_on_submit=False):
        st.subheader("🔐 Connection Parameters")
        
        col1, col2 = st.columns(2)

        with col1:
            account = st.text_input(
                "Account Identifier*",
                placeholder="your-account.region.cloud",
                help="Your Snowflake account identifier (e.g., abc123.us-east-1.aws)",
                key="enh_account"
            )
            user = st.text_input(
                "Username*",
                placeholder="your_username",
                help="Your Snowflake username",
                key="enh_user"
            )
            password = st.text_input(
                "Password*",
                type="password",
                placeholder="your_password",
                help="Your Snowflake password",
                key="enh_password"
            )

        with col2:
            warehouse = st.text_input(
                "Warehouse*",
                placeholder="COMPUTE_WH",
                help="Warehouse to use for computations",
                key="enh_warehouse"
            )
            database = st.text_input(
                "Database*",
                placeholder="your_database",
                help="Database name (case-sensitive)",
                key="enh_database"
            )
            schema = st.text_input(
                "Schema*",
                placeholder="PUBLIC",
                value="PUBLIC",
                help="Schema name (case-sensitive)",
                key="enh_schema"
            )

        # Advanced options
        with st.expander("🔧 Advanced Options"):
            col3, col4 = st.columns(2)
            
            with col3:
                role = st.text_input(
                    "Role",
                    placeholder="your_role",
                    help="Role to assume (leave empty for default)",
                    key="enh_role"
                )
            
            with col4:
                timeout = st.number_input(
                    "Timeout (seconds)",
                    min_value=30,
                    max_value=300,
                    value=60,
                    help="Connection timeout",
                    key="enh_timeout"
                )

        # Form buttons
        col5, col6 = st.columns(2)

        with col5:
            test_connection = st.form_submit_button("🧪 Test Enhanced Connection", type="secondary")

        with col6:
            connect_button = st.form_submit_button("⚡ Connect with Enhanced Mode", type="primary")

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

    # Initialize enhanced connection manager
    conn_manager = EnhancedSnowflakeConnectionManager()

    # Handle test connection
    if test_connection and not missing_fields:
        with st.spinner("🔄 Testing enhanced connection..."):
            success, message = conn_manager.test_connection(connection_params)

            if success:
                st.success(message)
                st.balloons()
            else:
                st.error(message)
                
                # Provide troubleshooting tips
                st.markdown("""
                **🔧 Troubleshooting Tips:**
                - Ensure your account identifier is correct (format: account.region.cloud)
                - Verify database and schema names are case-sensitive and exist
                - Check that your user has appropriate permissions
                - Consider specifying a role if using SSO or complex permission setup
                """)

        return None  # Don't connect, just test

    # Handle actual connection
    if connect_button and not missing_fields:
        with st.spinner("⚡ Connecting with enhanced session management..."):
            if conn_manager.connect(connection_params):
                st.success("✅ **Enhanced Snowflake connection established successfully!**")
                st.session_state.enhanced_snowflake_connection = conn_manager
                st.balloons()
                return conn_manager
            else:
                st.error("❌ Failed to establish enhanced connection")
                return None

    # Check if already connected
    if 'enhanced_snowflake_connection' in st.session_state:
        existing_conn = st.session_state.enhanced_snowflake_connection
        if existing_conn.is_connected:
            st.success("✅ **Enhanced Snowflake connection is active!**")
            return existing_conn

    return None
