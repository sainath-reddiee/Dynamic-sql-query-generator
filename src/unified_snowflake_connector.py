"""
UNIFIED Snowflake Database Connector
Replaces both snowflake_connector.py and enhanced_snowflake_connector.py
Combines all features into a single, maintainable solution
"""
import streamlit as st
import pandas as pd
import json
import time
from typing import Dict, Any, Optional, Tuple, List
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


def render_performance_info():
    """Display performance information about available features"""
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


def render_performance_metrics(perf_stats: Dict):
    """Render performance metrics in a nice format"""
    if not perf_stats:
        return
        
    st.markdown("### ⚡ Performance Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("⏱️ Total Time", f"{perf_stats.get('total_time', 0):.2f}s")
    
    with col2:
        st.metric("📊 Rows Processed", f"{perf_stats.get('row_count', 0):,}")
    
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
        
        if query_time > 0 or processing_time > 0:
            perf_df = pd.DataFrame({
                'Stage': ['Database Query', 'Data Processing'],
                'Time (seconds)': [query_time, processing_time]
            })
            st.bar_chart(perf_df.set_index('Stage'))


class UnifiedSnowflakeConnector:
    """
    Unified Snowflake connector that combines all features:
    - Basic connection management
    - Enhanced session context handling
    - Modin performance acceleration
    - Comprehensive error handling
    """

    def __init__(self, enhanced_mode: bool = True):
        self.connection = None
        self.connection_params = {}
        self.is_connected = False
        self.enhanced_mode = enhanced_mode  # Controls advanced features

    def test_connection(self, connection_params: Dict[str, str]) -> Tuple[bool, str]:
        """Test Snowflake connection parameters with optional enhanced features"""
        if not SNOWFLAKE_AVAILABLE:
            return False, "❌ Snowflake connector not available. Install with: pip install snowflake-connector-python"

        try:
            test_conn = snowflake.connector.connect(**connection_params)
            test_cursor = test_conn.cursor()
            
            if self.enhanced_mode:
                # Enhanced mode: Set database/schema context
                database = connection_params.get('database')
                schema = connection_params.get('schema', 'PUBLIC')
                
                if database:
                    test_cursor.execute(f"USE DATABASE {database}")
                    test_cursor.execute(f"USE SCHEMA {schema}")
            
            # Verify connection
            test_cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_VERSION()")
            result = test_cursor.fetchone()
            test_cursor.close()
            test_conn.close()

            if result and (not self.enhanced_mode or result[0]):
                features = []
                if self.enhanced_mode:
                    features.append("🛡️ Enhanced session management")
                if MODIN_AVAILABLE:
                    features.append("🚀 Modin acceleration")
                
                feature_text = f" ({', '.join(features)})" if features else ""
                
                return True, f"✅ Connected successfully! Database: {result[0]}, Schema: {result[1]}, Version: {result[2]}{feature_text}"
            else:
                if self.enhanced_mode and not result[0]:
                    return False, "❌ Connected but no active database. Check your permissions."
                return True, f"✅ Basic connection successful! Version: {result[2] if result else 'Unknown'}"

        except Exception as e:
            return self._handle_connection_error(str(e))

    def _handle_connection_error(self, error_msg: str) -> Tuple[bool, str]:
        """Enhanced error handling for connection issues"""
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
        """Establish persistent connection with optional enhanced features"""
        if not SNOWFLAKE_AVAILABLE:
            st.error("❌ Snowflake connector not available. Please install snowflake-connector-python")
            return False

        try:
            self.connection = snowflake.connector.connect(**connection_params)
            
            if self.enhanced_mode:
                # Enhanced mode: Set database/schema context immediately
                cursor = self.connection.cursor()
                database = connection_params.get('database')
                schema = connection_params.get('schema', 'PUBLIC')
                
                if database:
                    cursor.execute(f"USE DATABASE {database}")
                    cursor.execute(f"USE SCHEMA {schema}")
                    
                    # Verify context is set
                    cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
                    result = cursor.fetchone()
                    
                    if not result or not result[0]:
                        cursor.close()
                        self.connection.close()
                        st.error("❌ Failed to set database context")
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
        """Ensure the session has proper database/schema context (enhanced mode only)"""
        if not self.enhanced_mode or not self.is_connected or not self.connection:
            return True  # Skip in basic mode
        
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
        """Execute SQL query with basic functionality"""
        if not self.is_connected:
            return None, "❌ Not connected to database"

        try:
            # Enhanced mode: Ensure session context
            if self.enhanced_mode and not self.ensure_session_context():
                return None, "❌ Failed to establish database session context"
            
            cursor = self.connection.cursor(DictCursor)
            cursor.execute(sql)

            # Get column names and fetch results
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            cursor.close()

            if rows and columns:
                df = pd.DataFrame(rows, columns=columns)
                return df, None
            else:
                return pd.DataFrame(), None

        except Exception as e:
            error_msg = str(e)
            
            # Enhanced error handling for session context issues
            if self.enhanced_mode and "does not have a current database" in error_msg:
                try:
                    # Try to re-establish context and retry
                    if self.ensure_session_context():
                        return self.execute_query(sql)  # Retry once
                except:
                    pass
                return None, "❌ Database context error. Try reconnecting."
            
            return None, f"❌ Query execution failed: {error_msg}"

    def execute_query_with_performance(self, sql: str) -> Tuple[Optional[pd.DataFrame], Optional[str], Dict]:
        """Execute SQL query with performance tracking (enhanced mode)"""
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
            # Enhanced mode: Ensure session context
            if self.enhanced_mode and not self.ensure_session_context():
                return None, "❌ Failed to establish database session context", perf_stats
            
            # Execute query with timing
            query_start_time = time.time()
            cursor = self.connection.cursor(DictCursor)
            cursor.execute(sql)

            columns = [desc[0] for desc in cursor.description] if cursor.description else []
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
                        
                        # Convert back to pandas for compatibility
                        if hasattr(df, '_to_pandas'):
                            df = df._to_pandas()
                        else:
                            df = pd.DataFrame(df)
                            
                    except Exception as modin_error:
                        logger.warning(f"Modin processing failed, using pandas: {modin_error}")
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
            
            # Enhanced error handling
            if self.enhanced_mode and "does not have a current database" in error_msg:
                try:
                    if self.ensure_session_context():
                        return self.execute_query_with_performance(sql)  # Retry once
                except:
                    pass
                return None, "❌ Database context error. Try reconnecting.", perf_stats
            
            return None, f"❌ Query execution failed: {error_msg}", perf_stats

    def list_tables(self, schema_name: str = None) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """List available tables"""
        if not self.is_connected:
            return None, "❌ Not connected to database"

        try:
            if self.enhanced_mode and not self.ensure_session_context():
                return None, "❌ Failed to establish database session context"
                
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


def render_unified_connection_ui(enhanced_mode: bool = True, key_prefix: str = "") -> Optional[UnifiedSnowflakeConnector]:
    """
    Render unified connection UI that works for both basic and enhanced modes
    """
    if not SNOWFLAKE_AVAILABLE:
        st.error("""
        ❌ **Snowflake Connector Not Available**
        
        To use database connectivity features, please install the Snowflake connector:
        ```bash
        pip install snowflake-connector-python
        ```
        """)
        return None

    # Display mode information
    mode_description = "Enhanced Mode (🛡️ Session management + 🚀 Performance)" if enhanced_mode else "Standard Mode (Basic connectivity)"
    
    st.markdown(f"""
<div style="background: linear-gradient(145deg, {'#e8f5e8' if enhanced_mode else '#e3f2fd'}, #f8f9fa); padding: 1.5rem; border-radius: 10px; border: 2px solid {'#81c784' if enhanced_mode else '#90caf9'}; margin-bottom: 1rem;">
    <h4 style="color: {'#2e7d32' if enhanced_mode else '#1976d2'}; margin-bottom: 1rem;">🔗 Snowflake Connection - {mode_description}</h4>
    <p style="margin-bottom: 0.5rem;"><strong>Features:</strong></p>
    <ul style="margin-bottom: 0;">
        <li>📊 Execute SQL directly on your data</li>
        <li>🏔️ Use your existing stored procedures</li>
        <li>💾 Store and query JSON data</li>
        {f'<li>🛡️ <strong>Enhanced session context management</strong></li>' if enhanced_mode else ''}
        {f'<li>🚀 <strong>Modin performance acceleration</strong></li>' if enhanced_mode and MODIN_AVAILABLE else ''}
    </ul>
</div>
""", unsafe_allow_html=True)

    # Connection form
    with st.form(f"{key_prefix}_unified_connection_form", clear_on_submit=False):
        col1, col2 = st.columns(2)

        with col1:
            account = st.text_input(
                "Account Identifier*",
                placeholder="your-account.region.cloud",
                help="Your Snowflake account identifier",
                key=f"{key_prefix}_account"
            )
            user = st.text_input(
                "Username*",
                placeholder="your_username",
                key=f"{key_prefix}_user"
            )
            password = st.text_input(
                "Password*",
                type="password",
                placeholder="your_password",
                key=f"{key_prefix}_password"
            )

        with col2:
            warehouse = st.text_input(
                "Warehouse*",
                placeholder="COMPUTE_WH",
                key=f"{key_prefix}_warehouse"
            )
            database = st.text_input(
                "Database*",
                placeholder="your_database",
                key=f"{key_prefix}_database"
            )
            schema = st.text_input(
                "Schema*",
                placeholder="PUBLIC",
                value="PUBLIC",
                key=f"{key_prefix}_schema"
            )

        # Advanced options
        with st.expander("🔧 Advanced Options"):
            col3, col4 = st.columns(2)
            
            with col3:
                role = st.text_input(
                    "Role",
                    placeholder="your_role",
                    key=f"{key_prefix}_role"
                )
            
            with col4:
                timeout = st.number_input(
                    "Timeout (seconds)",
                    min_value=30,
                    max_value=300,
                    value=60,
                    key=f"{key_prefix}_timeout"
                )

        # Form buttons
        col5, col6 = st.columns(2)

        with col5:
            test_connection = st.form_submit_button("🧪 Test Connection", type="secondary")

        with col6:
            connect_button = st.form_submit_button(
                f"⚡ Connect ({mode_description.split(' (')[0]})", 
                type="primary"
            )

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

    # Initialize unified connector
    conn_manager = UnifiedSnowflakeConnector(enhanced_mode=enhanced_mode)

    # Handle test connection
    if test_connection and not missing_fields:
        with st.spinner("🔄 Testing connection..."):
            success, message = conn_manager.test_connection(connection_params)
            if success:
                st.success(message)
                st.balloons()
            else:
                st.error(message)
        return None

    # Handle actual connection
    if connect_button and not missing_fields:
        mode_text = "enhanced" if enhanced_mode else "standard"
        with st.spinner(f"🔄 Connecting in {mode_text} mode..."):
            if conn_manager.connect(connection_params):
                st.success(f"✅ **Successfully connected in {mode_text} mode!**")
                st.session_state[f'{key_prefix}_unified_connection'] = conn_manager
                st.balloons()
                return conn_manager
            else:
                return None

    # Check if already connected
    if f'{key_prefix}_unified_connection' in st.session_state:
        existing_conn = st.session_state[f'{key_prefix}_unified_connection']
        if existing_conn and existing_conn.is_connected:
            mode_text = "Enhanced" if existing_conn.enhanced_mode else "Standard"
            st.success(f"✅ **Already connected in {mode_text} mode!**")
            return existing_conn

    return None
