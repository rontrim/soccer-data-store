import streamlit as st
from databricks import sql
import pandas as pd
import plotly.express as px
import os

# ============================================================
# 1. App Configuration
# ============================================================
st.set_page_config(
    page_title="Big 5 Leagues Club Overview",
    page_icon="⚽", 
    layout="wide"
)
st.title("⚽ Big 5 Leagues Club Overview")

# ============================================================
# 2. Database Connection
# ============================================================
SQL_HTTP_PATH = "/sql/1.0/warehouses/ced38d4ec14f78c6"
CATALOG = "soccer_data"
SCHEMA = "analyze"
TABLE_HEADLINE = "headline_stats"
TABLE_FORM = "form_stats"

@st.cache_data(ttl=3600)
# Updated connection function for Streamlit Cloud
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
    df_form = get_data(TABLE_FORM)

# --- Transformation: Replace Underscores with Spaces ---
if not df_headline.empty:
    df_headline.columns = [c.replace("_", " ") for c in df_headline.columns]
    # Rename team code understat to Team_Abbreviation if it exists
    if "team code understat" in df_headline.columns:
        df_headline = df_headline.rename(columns={"team code understat": "Team_Abbreviation"})

if not df_form.empty:
    df_form.columns = [c.replace("_", " ") for c in df_form.columns]
    # Rename team code understat to Team_Abbreviation if it exists
    if "team code understat" in df_form.columns:
        df_form = df_form.rename(columns={"team code understat": "Team_Abbreviation"})

# ============================================================
# 4. Filters
# ============================================================
st.sidebar.title("Filters")

# Initializing filtered DataFrames
df_headline_filtered = df_headline.copy() if not df_headline.empty else pd.DataFrame()
df_form_filtered = df_form.copy() if not df_form.empty else pd.DataFrame()

# --- Filter 1: League (Applies to BOTH tables) ---
selected_league = None
if not df_headline.empty and "League" in df_headline.columns:
    all_leagues = sorted(df_headline["League"].unique().tolist())
    selected_league = st.sidebar.selectbox("Select League", all_leagues)
    
    # Apply to both
    df_headline_filtered = df_headline_filtered[df_headline_filtered["League"] == selected_league]
    if not df_form_filtered.empty and "League" in df_form_filtered.columns:
        df_form_filtered = df_form_filtered[df_form_filtered["League"] == selected_league]

# --- Filter 2: Season (Applies to LEAGUE TABLE only) ---
selected_season = None
if not df_headline.empty and "Season" in df_headline.columns:
    # Sort seasons descending so the newest is first
    all_seasons = sorted(df_headline["Season"].unique().tolist(), reverse=True)
    selected_season = st.sidebar.selectbox("Select Season", all_seasons)
    
    # Apply ONLY to headline stats
    df_headline_filtered = df_headline_filtered[df_headline_filtered["Season"] == selected_season]

# ============================================================
# 5. Main UI Tabs
# ============================================================
tab1, tab2 = st.tabs(["League Table", "Form Table (Last 8 Games)"])

# Define shared column configuration for tooltips and formatting
shared_column_config = {
    "Team-Season": None, # Hide ID
    "Team_Abbreviation": None, # Hide Abbreviation
    "Possession": st.column_config.ProgressColumn(
        "Possession %", 
        format="%.1f", 
        min_value=0, 
        max_value=100
    ),
    # --- Tooltips added here ---
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

# Desired column order
column_order = [
    "Position", "Team", "MP", "W", "D", "L", "Points", "PPG",
    "xGD W", "xGD D", "xGD L", "xGD Points", "xGD PPG",
    "G", "GA", "GD", "xG", "xGA", "xGD",
    "G PG", "GA PG", "GD PG", "xG PG", "xGA PG", "xGD PG",
    "Possession"
]

with tab1:
    if not df_headline_filtered.empty:
        # Sort by Points
        if "Points" in df_headline_filtered.columns:
             df_headline_filtered = df_headline_filtered.sort_values(by=["Points"], ascending=False)
        
        # Add Position
        df_headline_filtered["Position"] = range(1, len(df_headline_filtered) + 1)
        
        # Filter columns (intersection with available columns to avoid errors)
        cols = [c for c in column_order if c in df_headline_filtered.columns]
             
        st.dataframe(
            df_headline_filtered[cols], 
            use_container_width=True, 
            hide_index=True,
            column_config=shared_column_config
        )
        # Footnote
        st.caption("Source: Opta")

        # --- Scatter Plot ---
        st.divider()
        st.subheader("League Performance Analysis")
        
        # Columns available for plotting (exclude non-numeric/ID columns)
        exclude_cols = ["Position", "Team", "MP", "Team-Season", "Team_Abbreviation", "Season", "League"]
        plot_cols = [c for c in df_headline_filtered.columns if c not in exclude_cols]
        
        # Columns that should be reversed (Lower is Better)
        reverse_cols = ["L", "xGD L", "GA", "xGA", "GA PG", "xGA PG"]

        c1, c2, c3 = st.columns(3)
        with c1:
            x_axis = st.selectbox("X-Axis", plot_cols, index=plot_cols.index("xG") if "xG" in plot_cols else 0, key="l_x")
        with c2:
            y_axis = st.selectbox("Y-Axis", plot_cols, index=plot_cols.index("G") if "G" in plot_cols else 0, key="l_y")
        with c3:
            color_col = st.selectbox("Color Gradient", plot_cols, index=plot_cols.index("Points") if "Points" in plot_cols else 0, key="l_c")

        if x_axis and y_axis and color_col:
            # Create Quartile Bins for Color
            try:
                # Determine if color gradient should be reversed (Low = Good = Dark Green)
                is_reverse_color = color_col in reverse_cols
                
                # If reversed, we want Low values to be Q4 (High Rank/Dark Green)
                labels = ["Q1 (Low)", "Q2", "Q3", "Q4 (High)"]
                if is_reverse_color:
                    labels = ["Q4 (High)", "Q3", "Q2", "Q1 (Low)"]

                df_headline_filtered["Quartile"] = pd.qcut(
                    df_headline_filtered[color_col], 
                    q=4, 
                    labels=labels,
                    duplicates='drop' # Handle cases with many duplicate values
                )
            except ValueError:
                # Fallback if not enough unique values for 4 bins
                df_headline_filtered["Quartile"] = "Q1 (Low)"

            # Define discrete green colors
            color_map = {
                "Q1 (Low)": "#C8E6C9", # Lightest
                "Q2": "#81C784",
                "Q3": "#43A047",
                "Q4 (High)": "#1B5E20" # Darkest
            }

            fig = px.scatter(
                df_headline_filtered,
                x=x_axis,
                y=y_axis,
                color="Quartile", # Use the discrete bin column
                text="Team Abbreviation" if "Team Abbreviation" in df_headline_filtered.columns else "Team",
                color_discrete_map=color_map, # Use discrete map instead of continuous scale
                category_orders={"Quartile": ["Q1 (Low)", "Q2", "Q3", "Q4 (High)"]}, # Ensure legend order
                hover_data=["Team", "Position", color_col]
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
            
            # Remove gridlines
            fig.update_xaxes(showgrid=False)
            fig.update_yaxes(showgrid=False)

            # Reverse Axes if needed
            if x_axis in reverse_cols:
                fig.update_xaxes(autorange="reversed")
            if y_axis in reverse_cols:
                fig.update_yaxes(autorange="reversed")

            # Add Median Lines
            x_median = df_headline_filtered[x_axis].median()
            y_median = df_headline_filtered[y_axis].median()

            fig.add_vline(x=x_median, line_width=1, line_dash="dash", line_color="black")
            fig.add_hline(y=y_median, line_width=1, line_dash="dash", line_color="black")

            st.plotly_chart(fig, use_container_width=True)

    else:
        st.info("No data found for the selected filters.")

with tab2:
    if not df_form_filtered.empty:
        # Sort by Points
        if "Points" in df_form_filtered.columns:
            df_form_filtered = df_form_filtered.sort_values(by="Points", ascending=False)
            
        # Add Position
        df_form_filtered["Position"] = range(1, len(df_form_filtered) + 1)

        # Filter columns
        cols = [c for c in column_order if c in df_form_filtered.columns]
            
        st.dataframe(
            df_form_filtered[cols], 
            use_container_width=True, 
            hide_index=True,
            column_config=shared_column_config
        )
        # Footnote
        st.caption("Source: Opta")

        # --- Scatter Plot ---
        st.divider()
        st.subheader("Form Analysis (Last 8 Games)")
        
        # Columns available for plotting
        exclude_cols = ["Position", "Team", "MP", "Team-Season", "Team_Abbreviation", "Season", "League"]
        plot_cols_form = [c for c in df_form_filtered.columns if c not in exclude_cols]
        
        # Columns that should be reversed (Lower is Better)
        reverse_cols = ["L", "xGD L", "GA", "xGA", "GA PG", "xGA PG"]

        c1, c2, c3 = st.columns(3)
        with c1:
            x_axis_f = st.selectbox("X-Axis", plot_cols_form, index=plot_cols_form.index("xG") if "xG" in plot_cols_form else 0, key="f_x")
        with c2:
            y_axis_f = st.selectbox("Y-Axis", plot_cols_form, index=plot_cols_form.index("G") if "G" in plot_cols_form else 0, key="f_y")
        with c3:
            color_col_f = st.selectbox("Color Gradient", plot_cols_form, index=plot_cols_form.index("Points") if "Points" in plot_cols_form else 0, key="f_c")

        if x_axis_f and y_axis_f and color_col_f:
            # Create Quartile Bins for Color
            try:
                # Determine if color gradient should be reversed (Low = Good = Dark Green)
                is_reverse_color = color_col_f in reverse_cols
                
                # If reversed, we want Low values to be Q4 (High Rank/Dark Green)
                labels = ["Q1 (Low)", "Q2", "Q3", "Q4 (High)"]
                if is_reverse_color:
                    labels = ["Q4 (High)", "Q3", "Q2", "Q1 (Low)"]

                df_form_filtered["Quartile"] = pd.qcut(
                    df_form_filtered[color_col_f], 
                    q=4, 
                    labels=labels,
                    duplicates='drop'
                )
            except ValueError:
                df_form_filtered["Quartile"] = "Q1 (Low)"

            # Define discrete green colors
            color_map = {
                "Q1 (Low)": "#C8E6C9", # Lightest
                "Q2": "#81C784",
                "Q3": "#43A047",
                "Q4 (High)": "#1B5E20" # Darkest
            }

            fig_f = px.scatter(
                df_form_filtered,
                x=x_axis_f,
                y=y_axis_f,
                color="Quartile",
                text="Team Abbreviation" if "Team Abbreviation" in df_form_filtered.columns else "Team",
                color_discrete_map=color_map,
                category_orders={"Quartile": ["Q1 (Low)", "Q2", "Q3", "Q4 (High)"]},
                hover_data=["Team", "Position", color_col_f]
            )
            
            fig_f.update_traces(
                textposition='bottom center',
                textfont=dict(color='black', size=10),
                marker=dict(size=12, line=dict(width=1, color='DarkSlateGrey'))
            )
            
            fig_f.update_layout(
                xaxis_title=x_axis_f,
                yaxis_title=y_axis_f,
                font=dict(color='black'),
                plot_bgcolor='white'
            )
            
            # Remove gridlines
            fig_f.update_xaxes(showgrid=False)
            fig_f.update_yaxes(showgrid=False)

            # Reverse Axes if needed
            if x_axis_f in reverse_cols:
                fig_f.update_xaxes(autorange="reversed")
            if y_axis_f in reverse_cols:
                fig_f.update_yaxes(autorange="reversed")

            # Add Median Lines
            x_median = df_form_filtered[x_axis_f].median()
            y_median = df_form_filtered[y_axis_f].median()

            fig_f.add_vline(x=x_median, line_width=1, line_dash="dash", line_color="black")
            fig_f.add_hline(y=y_median, line_width=1, line_dash="dash", line_color="black")

            st.plotly_chart(fig_f, use_container_width=True)

    else:
        st.info("No form data found.")