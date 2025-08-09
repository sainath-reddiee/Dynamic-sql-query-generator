"""
Enhanced Snowflake Database Connector with Modin performance optimization
Includes the new Snowflake Modin plugin for better pandas performance
"""
import streamlit as st
import json
from typing import Dict, Any, Optional, Tuple
import logging
from datetime import datetime

# Try to import enhanced pandas with Snowflake Modin plugin
try:
    import modin.pandas as pd
    import snowflake.snowpark.modin.plugin
    MODIN_AVAILABLE = True
    st.info("🚀 **Modin Performance Mode Enabled** - Enhanced pandas operations for better performance!")
except ImportError:
    try:
        import pandas as pd
        MODIN_AVAILABLE = False
        st.warning("⚠️ Modin not available. Using regular pandas. Install with: `pip install modin[all]`")
    except ImportError:
        st.error("❌ Neither Modin nor pandas available!")
        pd = None

# Try to import snowflake connector
try:
    import snowflake.connector
    from snowflake.connector import DictCursor
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False

logger = logging.getLogger(__name__)


class EnhancedSnowflakeConnectionManager:
    """Enhanced Snowflake connection manager with Modin performance optimization"""
    
    def __init__(self):
        self.connection = None
        self.connection_params = {}
        self.is_connected = False
        self.performance_mode = MODIN_AVAILABLE
        
    def get_performance_info(self) -> Dict[str, Any]:
        """Get performance configuration information"""
        return {
            'modin_available': MODIN_AVAILABLE,
            'pandas_backend': 'Modin' if MODIN_AVAILABLE else 'Standard Pandas',
            'performance_mode': self.performance_mode,
            'recommended_for': 'Large datasets (>1M rows)' if MODIN_AVAILABLE else 'Small to medium datasets'
        }
    
    def test_connection(self, connection_params: Dict[str, str]) -> Tuple[bool, str]:
        """Test Snowflake connection with performance info"""
        if not SNOWFLAKE_AVAILABLE:
            return False, "❌ Snowflake connector not available. Install with: pip install snowflake-connector-python"
        
        try:
            # Test connection
            test_conn = snowflake.connector.connect(**connection_params)
            test_cursor = test_conn.cursor()
            test_cursor.execute("SELECT CURRENT_VERSION(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
            result = test_cursor.fetchone()
            test_cursor.close()
            test_conn.close()
            
            perf_info = self.get_performance_info()
            success_msg = f"""✅ **Connection Successful!**
            
**Environment Info:**
- Snowflake Version: {result[0]}
- Warehouse: {result[1]}
- Database: {result[2]}
- Schema: {result[3]}

**Performance Mode:**
- Backend: {perf_info['pandas_backend']}
- Optimized for: {perf_info['recommended_for']}
            """
            
            return True, success_msg
            
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
            
            # Display performance mode
            perf_info = self.get_performance_info()
            if MODIN_AVAILABLE:
                st.success(f"✅ Connected with **{perf_info['pandas_backend']}** performance optimization!")
            else:
                st.success("✅ Connected successfully!")
                st.info("💡 **Tip:** Install Modin for better performance with large datasets: `pip install modin[all]`")
            
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
    
    def execute_query_with_performance(self, sql: str) -> Tuple[Optional[pd.DataFrame], Optional[str], Dict[str, Any]]:
        """Execute SQL query with performance monitoring"""
        if not SNOWFLAKE_AVAILABLE or not pd:
            return None, "❌ Required libraries not available", {}
        
        if not self.is_connected:
            return None, "❌ Not connected to database", {}
        
        start_time = datetime.now()
        performance_stats = {
            'backend': 'Modin' if MODIN_AVAILABLE else 'Pandas',
            'start_time': start_time,
            'query_size': len(sql),
            'success': False
        }
        
        try:
            cursor = self.connection.cursor(DictCursor)
            cursor.execute(sql)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Fetch results
            rows = cursor.fetchall()
            cursor.close()
            
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            performance_stats.update({
                'end_time': end_time,
                'execution_time': execution_time,
                'rows_returned': len(rows),
                'columns_returned': len(columns),
                'success': True
            })
            
            if rows and columns:
                # Create DataFrame with performance-optimized backend
                if MODIN_AVAILABLE and len(rows) > 10000:  # Use Modin for large datasets
                    df = pd.DataFrame(rows, columns=columns)
                    performance_stats['dataframe_backend'] = 'Modin (optimized)'
                else:
                    # Use regular pandas for small datasets
                    import pandas as regular_pd
                    df = regular_pd.DataFrame(rows, columns=columns)
                    performance_stats['dataframe_backend'] = 'Pandas (standard)'
                
                performance_stats['dataframe_memory'] = df.memory_usage(deep=True).sum()
                return df, None, performance_stats
            else:
                return pd.DataFrame() if pd else None, None, performance_stats
                
        except Exception as e:
            end_time = datetime.now()
            performance_stats.update({
                'end_time': end_time,
                'execution_time': (end_time - start_time).total_seconds(),
                'error': str(e),
                'success': False
            })
            return None, f"❌ Query execution failed: {str(e)}", performance_stats
    
    def create_temp_table_with_json_optimized(self, json_data: Any, temp_table_name: str = None) -> Tuple[Optional[str], Optional[str]]:
        """Create temporary table with JSON data - optimized version"""
        if not self.is_connected:
            return None, "❌ Not connected to database"
        
        if not temp_table_name:
            temp_table_name = f"TEMP_JSON_ANALYSIS_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')[:19]}"
        
        try:
            cursor = self.connection.cursor()
            
            # Create optimized temporary table with better column structure
            create_sql = f"""
            CREATE OR REPLACE TEMPORARY TABLE {temp_table_name} (
                id INTEGER AUTOINCREMENT START 1 INCREMENT 1,
                json_data VARIANT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                data_size NUMBER,
                data_hash STRING
            ) 
            CLUSTER BY (created_at)
            """
            cursor.execute(create_sql)
            
            # Prepare JSON data with optimization
            json_str = json.dumps(json_data, separators=(',', ':')) if not isinstance(json_data, str) else json_data
            json_str_escaped = json_str.replace("'", "''")
            data_size = len(json_str)
            data_hash = str(hash(json_str))
            
            # Insert with additional metadata
            insert_sql = f"""
            INSERT INTO {temp_table_name} (json_data, data_size, data_hash) 
            SELECT 
                PARSE_JSON('{json_str_escaped}') as json_data,
                {data_size} as data_size,
                '{data_hash}' as data_hash
            """
            cursor.execute(insert_sql)
            
            # Verify insertion and get stats
            cursor.execute(f"""
                SELECT 
                    COUNT(*) as record_count,
                    AVG(data_size) as avg_size,
                    MAX(created_at) as created_time
                FROM {temp_table_name}
            """)
            stats = cursor.fetchone()
            cursor.close()
            
            success_msg = f"""✅ **Optimized Temp Table Created**
            
**Table:** {temp_table_name}
**Records:** {stats[0]}
**Data Size:** {stats[1]:,.0f} bytes
**Created:** {stats[2]}
**Backend:** {'Modin-optimized' if MODIN_AVAILABLE else 'Standard'}
            """
            
            return temp_table_name, success_msg
            
        except Exception as e:
            return None, f"❌ Failed to create optimized temp table: {str(e)}"
    
    def call_dynamic_sql_procedure_enhanced(self, table_name: str, json_column: str, 
                                          field_conditions: str) -> Tuple[Optional[str], Optional[str]]:
        """Call the dynamic SQL procedure with enhanced error handling"""
        if not self.is_connected:
            return None, "❌ Not connected to database"
        
        try:
            cursor = self.connection.cursor()
            
            # Enhanced procedure call with timeout and error handling
            procedure_call = f"""
            CALL SAINATH.SNOW.DYNAMIC_SQL_LARGE_IMPROVED(
                '{table_name}',
                '{json_column}',
                '{field_conditions}'
            )
            """
            
            # Set query timeout for large operations
            cursor.execute("ALTER SESSION SET QUERY_TIMEOUT = 300")  # 5 minutes
            cursor.execute(procedure_call)
            
            result = cursor.fetchone()
            cursor.close()
            
            if result and result[0]:
                success_msg = f"""✅ **Procedure Executed Successfully**
                
**Generated SQL Length:** {len(result[0]):,} characters
**Performance Mode:** {'Modin-optimized' if MODIN_AVAILABLE else 'Standard'}
**Execution:** Enhanced error handling enabled
                """
                return result[0], success_msg
            else:
                return None, "❌ Procedure returned no result"
                
        except Exception as e:
            error_details = str(e)
            if "timeout" in error_details.lower():
                return None, "❌ Query timeout - try with simpler field conditions or smaller dataset"
            elif "procedure" in error_details.lower():
                return None, f"❌ Procedure error: {error_details}\n💡 Ensure SAINATH.SNOW.DYNAMIC_SQL_LARGE_IMPROVED exists"
            else:
                return None, f"❌ Execution failed: {error_details}"


def render_enhanced_performance_info():
    """Render performance information panel"""
    perf_info = {
        'modin_available': MODIN_AVAILABLE,
        'snowflake_available': SNOWFLAKE_AVAILABLE,
        'pandas_backend': 'Modin' if MODIN_AVAILABLE else 'Standard Pandas'
    }
    
    st.markdown("""
    <div style="background: linear-gradient(145deg, #fff8e1, #f8f9fa); padding: 1rem; border-radius: 8px; border: 1px solid #ffcc02; margin-bottom: 1rem;">
        <h4 style="color: #f57c00;">⚡ Performance Configuration</h4>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if MODIN_AVAILABLE:
            st.success("🚀 **Modin Enabled**")
            st.caption("Optimized for large datasets")
        else:
            st.warning("📊 **Standard Pandas**")
            st.caption("Good for small-medium datasets")
    
    with col2:
        if SNOWFLAKE_AVAILABLE:
            st.success("🏔️ **Snowflake Ready**")
            st.caption("Database connectivity available")
        else:
            st.error("❌ **Snowflake Missing**")
            st.caption("Install snowflake-connector-python")
    
    with col3:
        backend = perf_info['pandas_backend']
        if backend == 'Modin':
            st.info("⚡ **High Performance**")
            st.caption("Multi-core processing enabled")
        else:
            st.info("🔧 **Standard Mode**")
            st.caption("Single-core processing")
    
    # Performance recommendations
    if not MODIN_AVAILABLE:
        st.markdown("""
        <div style="background: #e3f2fd; padding: 1rem; border-radius: 6px; margin-top: 1rem;">
            <h5 style="color: #1976d2;">💡 Performance Upgrade Available</h5>
            <p>Install Modin for better performance with large datasets:</p>
            <code>pip install modin[all] snowflake-snowpark-python</code>
            <p><small>Modin provides up to 4x faster pandas operations on multi-core systems.</small></p>
        </div>
        """, unsafe_allow_html=True)


def render_performance_metrics(performance_stats: Dict[str, Any]):
    """Render query performance metrics"""
    if not performance_stats.get('success'):
        return
    
    st.markdown("### 📈 Performance Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        exec_time = performance_stats.get('execution_time', 0)
        st.metric("Execution Time", f"{exec_time:.2f}s")
    
    with col2:
        rows = performance_stats.get('rows_returned', 0)
        st.metric("Rows Returned", f"{rows:,}")
    
    with col3:
        cols = performance_stats.get('columns_returned', 0)
        st.metric("Columns", cols)
    
    with col4:
        backend = performance_stats.get('dataframe_backend', 'Unknown')
        st.metric("Backend", backend.split(' ')[0])
    
    # Memory usage if available
    if 'dataframe_memory' in performance_stats:
        memory_mb = performance_stats['dataframe_memory'] / 1024 / 1024
        st.caption(f"Memory Usage: {memory_mb:.2f} MB")
    
    # Performance tips
    if rows > 100000 and not MODIN_AVAILABLE:
        st.info("💡 **Tip:** Large result set detected. Consider installing Modin for better performance with datasets > 100K rows.")
