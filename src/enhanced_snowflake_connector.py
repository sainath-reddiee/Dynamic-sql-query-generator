You've run into a common issue in Streamlit when you reuse UI components. The error "There are multiple identical forms with key='enhanced_snowflake_connection_form'" happens because both the "Standard" and "Enhanced" tabs are creating a form with the exact same identifier.

To fix this, we need to make the key for each form and its widgets unique. We can do this by passing a unique prefix to the function for each tab.

Here are the corrected versions of the two files you need to update: src/main.py and src/enhanced_snowflake_connector.py.

1. Corrected src/enhanced_snowflake_connector.py
This file is updated to accept a key_prefix, which will be added to all widget keys to ensure they are unique.

Python

"""
Enhanced Snowflake Database Connector with Modin support and proper session initialization
Fixes the "This session does not have a current database" error
Includes performance acceleration with Modin pandas
"""
import streamlit as st
import pandas as pd
import json
import time
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

# Try to import Modin for performance acceleration
try:
    import modin.pandas as mpd
    MODIN_AVAILABLE = True
except ImportError:
    MODIN_AVAILABLE = False

logger = logging.getLogger(__name__)


def render_enhanced_performance_info():
    """Display performance information about available accelerations"""
    col1, col2, col3 = st.columns(3)
    
    with col1:
        snowflake_status = "✅ Available" if SNOWFLAKE_AVAILABLE else "❌ Not Available"
        st.info(f"**Snowflake:** {snowflake_status}")
    
    with col2:
        modin_status = "🚀 Available" if MODIN_AVAILABLE else "📊 Standard Pandas"
        st.info(f"**Performance:** {modin_status}")
    
    with col3:
        mode = "⚡ Enhanced Mode" if (SNOWFLAKE_AVAILABLE and MODIN_AVAILABLE) else "🏔️ Standard Mode"
        st.info(f"**Mode:** {mode}")

    if MODIN_AVAILABLE:
        st.success("🚀 **Modin Acceleration Enabled** - Large datasets will be processed up to 4x faster!")
    else:
        st.warning("📊 Using standard Pandas - Install Modin for better performance: `pip install modin[ray]`")


def render_performance_metrics(perf_stats: Dict):
    """Render performance metrics in a nice format"""
    st.markdown("### ⚡ Performance Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "⏱️ Total Time", 
            f"{perf_stats.get('total_time', 0):.2f}s"
        )
    
    with col2:
        st.metric(
            "📊 Rows Processed", 
            f"{perf_stats.get('row_count', 0):,}"
        )
    
    with col3:
        engine = "🚀 Modin" if perf_stats.get('modin_used', False) else "📊 Pandas"
        st.metric("🔧 Engine", engine)
    
    with col4:
        memory_mb = perf_stats.get('memory_usage_mb', 0)
        st.metric("💾 Memory", f"{memory_mb:.1f}MB")
    
    # Performance bar chart
    if perf_stats.get('total_time', 0) > 0:
        query_time = perf_stats.get('query_time', 0)
        processing_time = perf_stats.get('processing_time', 0)
        
        perf_df = pd.DataFrame({
            'Stage': ['Database Query', 'Data Processing'],
            'Time (seconds)': [query_time, processing_time]
        })
        
        st.bar_chart(perf_df.set_index('Stage'))


class EnhancedSnowflakeConnectionManager:
    """Enhanced Snowflake connection manager with Modin support and proper session initialization"""

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
                modin_info = " (🚀 Modin acceleration available)" if MODIN_AVAILABLE else " (📊 Standard pandas)"
                return True, f"✅ Connected successfully! Database: {result[0]}, Schema: {result[1]}, Version: {result[2]}{modin_info}"
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

    def execute_query_with_performance(self, sql: str) -> Tuple[Optional[pd.DataFrame], Optional[str], Dict]:
        """Execute SQL query with Modin performance tracking"""
        perf_stats = {
            'total_time': 0,
            'query_time': 0,
            'processing_time': 0,
            'row_count': 0,
            'memory_usage_mb': 0,
            'modin_used': False
        }
        
        if not self.is_connected:
            return None, "❌ Not connected to database", perf_stats

        total_start_time = time.time()

        try:
            # Ensure session context before executing query
            if not self.ensure_session_context():
                return None, "❌ Failed to establish database session context", perf_stats
            
            # Execute query with timing
            query_start_time = time.time()
            cursor = self.connection.cursor(DictCursor)
            cursor.execute(sql)

            # Get column names
            columns = [desc[0] for desc in cursor.description] if cursor.description else []

            # Fetch results
            rows = cursor.fetchall()
            cursor.close()
            
            query_end_time = time.time()
            perf_stats['query_time'] = query_end_time - query_start_time

            if rows and columns:
                # Processing phase with optional Modin acceleration
                processing_start_time = time.time()
                
                # Use Modin for large datasets if available
                if MODIN_AVAILABLE and len(rows) > 1000:
                    try:
                        df = mpd.DataFrame(rows, columns=columns)
                        perf_stats['modin_used'] = True
                        
                        # Convert back to pandas if needed for compatibility
                        if hasattr(df, '_to_pandas'):
                            df = df._to_pandas()
                        else:
                            df = pd.DataFrame(df)
                            
                    except Exception as modin_error:
                        logger.warning(f"Modin processing failed, falling back to pandas: {modin_error}")
                        df = pd.DataFrame(rows, columns=columns)
                        perf_stats['modin_used'] = False
                else:
                    df = pd.DataFrame(rows, columns=columns)
                    perf_stats['modin_used'] = False
                
                processing_end_time = time.time()
                perf_stats['processing_time'] = processing_end_time - processing_start_time
                perf_stats['row_count'] = len(df)
                
                # Estimate memory usage
                memory_usage = df.memory_usage(deep=True).sum()
                perf_stats['memory_usage_mb'] = memory_usage / (1024 * 1024)
                
                total_end_time = time.time()
                perf_stats['total_time'] = total_end_time - total_start_time
                
                return df, None, perf_stats
            else:
                total_end_time = time.time()
                perf_stats['total_time'] = total_end_time - total_start_time
                return pd.DataFrame(), None, perf_stats

        except Exception as e:
            error_msg = str(e)
            total_end_time = time.time()
            perf_stats['total_time'] = total_end_time - total_start_time
            
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
                        
                        # Retry the original query with performance tracking
                        return self.execute_query_with_performance(sql)
                            
                except Exception as retry_e:
                    return None, f"❌ Database context error (retry failed): {str(retry_e)}", perf_stats
            
            return None, f"❌ Query execution failed: {error_msg}", perf_stats

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

# Enhanced connection UI function
# FIXED: Added key_prefix to make all widget keys unique
def render_enhanced_snowflake_connection_ui(key_prefix: str = "") -> Optional[EnhancedSnowflakeConnectionManager]:
    """Render enhanced Snowflake connection UI with better error handling and Modin info"""
    
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
            <li>🚀 <strong>Modin performance acceleration</strong></li>
        </ul>
    </div>
    """, unsafe_allow_html=True)

    # Show Modin status
    if MODIN_AVAILABLE:
        st.success("🚀 **Modin Acceleration Active** - Large query results will be processed up to 4x faster!")
    else:
        st.info("📊 **Standard Mode** - Install Modin for better performance: `pip install modin[ray]`")

    # Connection form with a unique key
    with st.form(f"{key_prefix}_snowflake_connection_form", clear_on_submit=False):
        st.subheader("🔐 Connection Parameters")
        
        col1, col2 = st.columns(2)

        with col1:
            account = st.text_input(
                "Account Identifier*",
                placeholder="your-account.region.cloud",
                help="Your Snowflake account identifier (e.g., abc123.us-east-1.aws)",
                key=f"{key_prefix}_account"
            )
            user = st.text_input(
                "Username*",
                placeholder="your_username",
                help="Your Snowflake username",
                key=f"{key_prefix}_user"
            )
            password = st.text_input(
                "Password*",
                type="password",
                placeholder="your_password",
                help="Your Snowflake password",
                key=f"{key_prefix}_password"
            )

        with col2:
            warehouse = st.text_input(
                "Warehouse*",
                placeholder="COMPUTE_WH",
                help="Warehouse to use for computations",
                key=f"{key_prefix}_warehouse"
            )
            database = st.text_input(
                "Database*",
                placeholder="your_database",
                help="Database name (case-sensitive)",
                key=f"{key_prefix}_database"
            )
            schema = st.text_input(
                "Schema*",
                placeholder="PUBLIC",
                value="PUBLIC",
                help="Schema name (case-sensitive)",
                key=f"{key_prefix}_schema"
            )

        # Advanced options
        with st.expander("🔧 Advanced Options"):
            col3, col4 = st.columns(2)
            
            with col3:
                role = st.text_input(
                    "Role",
                    placeholder="your_role",
                    help="Role to assume (leave empty for default)",
                    key=f"{key_prefix}_role"
                )
            
            with col4:
                timeout = st.number_input(
                    "Timeout (seconds)",
                    min_value=30,
                    max_value=300,
                    value=60,
                    help="Connection timeout",
                    key=f"{key_prefix}_timeout"
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
                st.session_state[f'{key_prefix}_snowflake_connection'] = conn_manager
                st.balloons()
                return conn_manager
            else:
                st.error("❌ Failed to establish enhanced connection")
                return None

    # Check if already connected
    if f'{key_prefix}_snowflake_connection' in st.session_state:
        existing_conn = st.session_state[f'{key_prefix}_snowflake_connection']
        if existing_conn and existing_conn.is_connected:
            st.success("✅ **Enhanced Snowflake connection is active!**")
            return existing_conn

    return None
