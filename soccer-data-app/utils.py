import streamlit as st
from databricks import sql
import pandas as pd

@st.cache_data(ttl=3600)
def get_data(table_name, schema="analyze"):
    """
    Fetches data from Databricks SQL Warehouse.
    Cached for 1 hour.
    """
    try:
        # Streamlit Cloud stores secrets in st.secrets dict
        host = st.secrets["DATABRICKS_HOST"]
        token = st.secrets["DATABRICKS_TOKEN"]
        http_path = st.secrets["SQL_HTTP_PATH"]

        if not host or not token:
            st.error("Missing secrets.")
            return pd.DataFrame()

        # Clean Hostname
        clean_host = host.replace("https://", "").replace("http://", "").strip("/")
        
        with sql.connect(
            server_hostname=clean_host,
            http_path=http_path,
            access_token=token
        ) as connection:
            
            query = f"SELECT * FROM soccer_data.{schema}.{table_name}"
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                return pd.DataFrame(result, columns=columns)

    except Exception as e:
        st.error(f"❌ Connection Error: {e}")
        return pd.DataFrame()


def get_headline_stats(table_name="headline_stats", schema="analyze", force_refresh=False):
    """
    Fetches headline_stats from session state if available, otherwise from Databricks.
    This prevents redundant warehouse queries when navigating between pages.
    
    Args:
        table_name: Name of the table to fetch (default: "headline_stats")
        schema: Database schema (default: "analyze")
        force_refresh: If True, bypasses session state and fetches fresh data
        
    Returns:
        pd.DataFrame: The headline stats data
    """
    session_key = f"{schema}_{table_name}"
    
    # Check if data exists in session state and force_refresh is not requested
    if not force_refresh and session_key in st.session_state:
        return st.session_state[session_key]
    
    # Fetch data from Databricks using the existing cached function
    df = get_data(table_name, schema)
    
    # Store in session state for future use
    st.session_state[session_key] = df
    
    return df
