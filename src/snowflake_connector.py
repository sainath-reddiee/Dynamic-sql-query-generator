"""
Snowflake Database Connector for direct database operations
"""
import streamlit as st
import pandas as pd
import json
from typing import Dict, Any, Optional, Tuple, List
import logging
from datetime import datetime
from python_sql_generator import generate_sql_from_json_data
from json_analyzer import analyze_json_structure

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
        if not SNOWFLAKE_AVAILABLE:
            return False, "❌ Snowflake connector not available."
        try:
            with snowflake.connector.connect(**connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT CURRENT_VERSION()")
                    version = cursor.fetchone()
                    return True, f"✅ Connected successfully! Snowflake version: {version[0]}"
        except Exception as e:
            return False, f"❌ Connection failed: {str(e)}"

    def connect(self, connection_params: Dict[str, str]) -> bool:
        if not SNOWFLAKE_AVAILABLE:
            st.error("❌ Snowflake connector not available.")
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
        if self.connection:
            self.connection.close()
            self.connection = None
            self.is_connected = False

    def execute_query(self, sql: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        if not self.is_connected:
            return None, "❌ Not connected to database"
        try:
            with self.connection.cursor(DictCursor) as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                if rows and columns:
                    return pd.DataFrame(rows, columns=columns), None
                return pd.DataFrame(), None
        except Exception as e:
            return None, f"❌ Query execution failed: {str(e)}"

    def get_json_columns(self, table_name: str) -> Tuple[Optional[List[str]], Optional[str]]:
        """Fetches VARIANT or OBJECT columns from a specified table."""
        if not self.is_connected:
            return None, "❌ Not connected to database"
        try:
            sql = f"DESCRIBE TABLE {table_name};"
            df, error = self.execute_query(sql)
            if error:
                return None, error
            if df is not None and not df.empty:
                json_cols = df[df['type'].isin(['VARIANT', 'OBJECT', 'ARRAY'])]['name'].tolist()
                return json_cols, None
            return [], "No columns found."
        except Exception as e:
            return None, f"❌ Could not describe table: {e}"

    def get_json_sample_and_analyze(self, table_name: str, json_column: str) -> Tuple[Any, Optional[str]]:
        """
        Fetches a sample of JSON records, merges their schemas, and returns a composite schema.
        """
        if not self.is_connected:
            return None, "❌ Not connected to database"
        try:
            # Fetch up to 100 random records for a more robust analysis
            sql = f"SELECT {json_column} FROM {table_name} WHERE {json_column} IS NOT NULL SAMPLE (100 ROWS);"
            df, error = self.execute_query(sql)
            if error:
                return None, error
            if df is None or df.empty:
                return None, "❌ No non-null JSON data found in the specified column."

            # Merge schemas from all sampled records
            composite_schema = {}
            for index, row in df.iterrows():
                try:
                    record_json = json.loads(row[json_column])
                    record_schema = analyze_json_structure(record_json)
                    # Merge the new schema into the composite one
                    composite_schema.update(record_schema)
                except (json.JSONDecodeError, TypeError):
                    # Skip rows that are not valid JSON
                    continue
            
            if not composite_schema:
                 return None, "❌ Could not find any valid JSON records in the sample."

            return composite_schema, None
        except Exception as e:
            return None, f"❌ Failed to fetch or analyze JSON sample: {e}"


def render_snowflake_connection_ui() -> Optional[SnowflakeConnectionManager]:
    if not SNOWFLAKE_AVAILABLE:
        st.error("❌ **Snowflake Connector Not Available**")
        return None

    with st.form("snowflake_connection_form"):
        account = st.text_input("Account Identifier*")
        user = st.text_input("Username*")
        password = st.text_input("Password*", type="password")
        warehouse = st.text_input("Warehouse*")
        database = st.text_input("Database*")
        schema_name = st.text_input("Schema*", value="PUBLIC")
        
        submitted = st.form_submit_button("🔗 Connect")
        if submitted:
            params = {"account": account, "user": user, "password": password, "warehouse": warehouse, "database": database, "schema": schema_name}
            if not all(params.values()):
                st.error("Please fill all required fields.")
                return None
            
            conn_manager = SnowflakeConnectionManager()
            if conn_manager.connect(params):
                st.session_state.snowflake_connection = conn_manager
                st.success("✅ Connected to Snowflake!")
                st.rerun()
            else:
                st.error("Connection failed.")
    
    if 'snowflake_connection' in st.session_state:
        return st.session_state.snowflake_connection
    
    return None


def render_snowflake_operations_ui(conn_manager: SnowflakeConnectionManager):
    """Render UI for Snowflake database operations with the new workflow."""
    
    table_name = st.text_input("Enter Snowflake Table Name:", key="db_table_name_input")

    if table_name:
        json_columns, error = conn_manager.get_json_columns(table_name)
        if error:
            st.error(error)
            return
        if not json_columns:
            st.warning(f"No VARIANT, OBJECT, or ARRAY columns found in table `{table_name}`.")
            return

        json_column = st.selectbox("Select the JSON column to analyze:", json_columns)

        if json_column:
            # The analysis now happens inside this block
            if st.button("Analyze Table Column", key="analyze_button"):
                with st.spinner(f"Fetching and analyzing up to 100 sample records from `{json_column}`..."):
                    composite_schema, error = conn_manager.get_json_sample_and_analyze(table_name, json_column)
                
                if error:
                    st.error(error)
                    st.session_state.composite_schema = None
                elif composite_schema:
                    st.success("✅ Successfully analyzed the data structure.")
                    st.session_state.composite_schema = composite_schema
                else:
                    st.warning("Could not generate a schema from the sampled data.")
            
            # The rest of the UI depends on the schema being in the session state
            if 'composite_schema' in st.session_state and st.session_state.composite_schema:
                schema = st.session_state.composite_schema
                with st.expander("View Analyzed Schema (Composite)"):
                    st.json(schema)

                field_conditions = st.text_area(
                    "Field Conditions:",
                    height=100,
                    placeholder="e.g., name, rating[>:4.0]",
                    key="db_field_conditions"
                )

                if st.button("🚀 Generate and Execute SQL", type="primary"):
                    if not field_conditions:
                        st.warning("⚠️ Please provide field conditions.")
                        return

                    with st.spinner("🔄 Generating SQL based on database schema..."):
                        # Note: We pass a dummy JSON object here because the schema is what matters
                        generated_sql = generate_sql_from_json_data(
                            {}, table_name, json_column, field_conditions, schema
                        )
                    
                    if generated_sql and not generated_sql.strip().startswith("-- Error"):
                        st.subheader("📄 Generated SQL")
                        st.code(generated_sql, language="sql")

                        with st.spinner("🔄 Executing query on Snowflake..."):
                            result_df, exec_error = conn_manager.execute_query(generated_sql)
                        
                        if exec_error:
                            st.error(exec_error)
                        elif result_df is not None:
                            st.subheader("📊 Query Results")
                            st.dataframe(result_df)
                            if not result_df.empty:
                                st.download_button(
                                    "📥 Download Results",
                                    result_df.to_csv(index=False).encode('utf-8'),
                                    "results.csv"
                                )
                    else:
                        st.error("❌ Failed to generate SQL from the database sample.")
