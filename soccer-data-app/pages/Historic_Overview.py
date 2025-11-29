import streamlit as st
from databricks import sql
import pandas as pd
import plotly.express as px
import os

# ============================================================
# 1. App Configuration
# ============================================================
# Page config is now handled in app.py

st.title("⚽ Big 5 Leagues Club Historic Overview")

# ============================================================
# 2. Database Connection
# ============================================================
SQL_HTTP_PATH = "/sql/1.0/warehouses/ced38d4ec14f78c6"
CATALOG = "soccer_data"
SCHEMA = "analyze"
TABLE_HEADLINE = "headline_stats"

@st.cache_data(ttl=3600)
def get_data(table_name):
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
            
            query = f"SELECT * FROM soccer_data.analyze.{table_name}"
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                return pd.DataFrame(result, columns=columns)

    except Exception as e:
        st.error(f"❌ Connection Error: {e}")
        return pd.DataFrame()

# ============================================================
# 3. Load & Process Data
# ============================================================
with st.spinner("Loading stats..."):
    df_headline = get_data(TABLE_HEADLINE)

# --- Transformation: Replace Underscores with Spaces ---
if not df_headline.empty:
    df_headline.columns = [c.replace("_", " ") for c in df_headline.columns]
    # Rename team code understat to Team_Abbreviation if it exists
    if "team code understat" in df_headline.columns:
        df_headline = df_headline.rename(columns={"team code understat": "Team_Abbreviation"})

# ============================================================
# 4. Filters
# ============================================================
st.sidebar.title("Filters")

# Initializing filtered DataFrame
df_filtered = df_headline.copy() if not df_headline.empty else pd.DataFrame()

# --- Filter 1: League ---
selected_league = "All Leagues"
if not df_headline.empty and "League" in df_headline.columns:
    all_leagues = sorted(df_headline["League"].unique().tolist())
    # Add "All Leagues" option
    league_options = ["All Leagues"] + all_leagues
    selected_league = st.sidebar.selectbox("Select League", league_options)
    
    if selected_league != "All Leagues":
        df_filtered = df_filtered[df_filtered["League"] == selected_league]

# ============================================================
# 5. Main UI
# ============================================================

# Define shared column configuration
shared_column_config = {
    "Team-Season": None, # Hide ID
    "Team_Abbreviation": None, # Hide Abbreviation
    "Possession": st.column_config.ProgressColumn(
        "Possession %", 
        format="%.1f", 
        min_value=0, 
        max_value=100
    ),
    "xGD W": st.column_config.NumberColumn(
        "xGD W", help="Games with greater than +0.5 xGD"
    ),
    "xGD D": st.column_config.NumberColumn(
        "xGD D", help="Games with xGD between -0.5 and +0.5"
    ),
    "xGD L": st.column_config.NumberColumn(
        "xGD L", help="Games with less than -0.5 xGD"
    )
}

# Desired column order: Position, Team, League, Season, MP, ...
base_stats_order = [
    "MP", "W", "D", "L", "Points", "PPG",
    "xGD W", "xGD D", "xGD L", "xGD Points", "xGD PPG",
    "G", "GA", "GD", "xG", "xGA", "xGD",
    "G PG", "GA PG", "GD PG", "xG PG", "xGA PG", "xGD PG",
    "Possession"
]
column_order = ["Position", "Team", "League", "Season"] + base_stats_order

tab1, = st.tabs(["League Table"])

with tab1:
    if not df_filtered.empty:
        # --- Sort Controls ---
        c_sort1, c_sort2 = st.columns([1, 3])
        with c_sort1:
            # Exclude Position from sort options
            # Available columns for sorting (intersection of desired order and actual columns)
            available_cols = [c for c in column_order if c in df_filtered.columns and c != "Position"]
            
            # Default to PPG if available
            default_idx = available_cols.index("PPG") if "PPG" in available_cols else 0
            sort_col = st.selectbox("Sort Table By", available_cols, index=default_idx, key="sort_hist")
        with c_sort2:
            sort_asc = st.radio("Order", ["Descending", "Ascending"], index=0, horizontal=True, key="order_hist")
        
        ascending = sort_asc == "Ascending"
        
        # Sort
        if sort_col in df_filtered.columns:
             df_filtered = df_filtered.sort_values(by=[sort_col], ascending=ascending)
        
        # Add Position (Rank based on current sort)
        df_filtered["Position"] = range(1, len(df_filtered) + 1)
        
        # Filter columns for display
        cols = [c for c in column_order if c in df_filtered.columns]
             
        st.dataframe(
            df_filtered[cols], 
            width="stretch", 
            hide_index=True,
            column_config=shared_column_config
        )
        st.caption("Source: Opta")

        # --- Scatter Plot ---
        st.divider()
        st.subheader("Historic Performance Analysis")
        
        # Prepare Plot Data
        # Create Short Season (e.g. 2023/2024 -> 23/24)
        if "Season" in df_filtered.columns:
            # Assuming format is YYYY/YYYY or similar. We take last 2 chars of first part and last 2 chars of second part?
            # Or simpler: if it's "2023/2024", we want "23/24".
            # Let's try a generic approach: split by '/', take last 2 chars of each part.
            def shorten_season(s):
                try:
                    parts = str(s).split('/')
                    return "/".join([p[-2:] for p in parts])
                except:
                    return str(s)
            
            df_filtered["Short_Season"] = df_filtered["Season"].apply(shorten_season)
        else:
            df_filtered["Short_Season"] = ""

        # Create Plot Label: Team Abbreviation + Short Season
        if "Team_Abbreviation" in df_filtered.columns:
            df_filtered["Plot_Label"] = df_filtered["Team_Abbreviation"] + " " + df_filtered["Short_Season"]
        else:
            df_filtered["Plot_Label"] = df_filtered["Team"] + " " + df_filtered["Short_Season"]

        # Columns available for plotting
        exclude_cols = ["Position", "Team", "MP", "Team-Season", "Team Abbreviation", "Team_Abbreviation", "Season", "League", "Short_Season", "Plot_Label"]
        plot_cols = [c for c in df_filtered.columns if c not in exclude_cols]
        
        reverse_cols = ["L", "xGD L", "GA", "xGA", "GA PG", "xGA PG"]

        c1, c2, c3 = st.columns(3)
        with c1:
            x_axis = st.selectbox("X-Axis", plot_cols, index=plot_cols.index("xG") if "xG" in plot_cols else 0, key="h_x")
        with c2:
            y_axis = st.selectbox("Y-Axis", plot_cols, index=plot_cols.index("G") if "G" in plot_cols else 0, key="h_y")
        with c3:
            color_col = st.selectbox("Color by", plot_cols, index=plot_cols.index("Points") if "Points" in plot_cols else 0, key="h_c")

        if x_axis and y_axis and color_col:
            try:
                is_reverse_color = color_col in reverse_cols
                labels = ["Q1 (Low)", "Q2", "Q3", "Q4 (High)"]
                if is_reverse_color:
                    labels = ["Q4 (High)", "Q3", "Q2", "Q1 (Low)"]

                df_filtered["Quartile"] = pd.qcut(
                    df_filtered[color_col], 
                    q=4, 
                    labels=labels,
                    duplicates='drop'
                )
            except ValueError:
                df_filtered["Quartile"] = "Q1 (Low)"

            color_map = {
                "Q1 (Low)": "#C8E6C9",
                "Q2": "#81C784",
                "Q3": "#43A047",
                "Q4 (High)": "#1B5E20"
            }

            fig = px.scatter(
                df_filtered,
                x=x_axis,
                y=y_axis,
                color="Quartile",
                text="Plot_Label", # Use the new label
                color_discrete_map=color_map,
                category_orders={"Quartile": ["Q1 (Low)", "Q2", "Q3", "Q4 (High)"]},
                hover_data=["Team", "Season", "Position", color_col]
            )
            
            fig.update_traces(
                textposition='bottom center',
                textfont=dict(color='black', size=10),
                marker=dict(size=12, line=dict(width=1, color='DarkSlateGrey'))
            )
            
            fig.update_layout(
                xaxis_title=x_axis,
                yaxis_title=y_axis,
                font=dict(color='black'),
                plot_bgcolor='white'
            )
            
            fig.update_xaxes(showgrid=False, zeroline=False)
            fig.update_yaxes(showgrid=False, zeroline=False)

            if x_axis in reverse_cols:
                fig.update_xaxes(autorange="reversed")
            if y_axis in reverse_cols:
                fig.update_yaxes(autorange="reversed")

            x_median = df_filtered[x_axis].median()
            y_median = df_filtered[y_axis].median()

            fig.add_vline(x=x_median, line_width=1, line_dash="dash", line_color="black")
            fig.add_hline(y=y_median, line_width=1, line_dash="dash", line_color="black")

            st.plotly_chart(fig, width='stretch')

    else:
        st.info("No data found for the selected filters.")
