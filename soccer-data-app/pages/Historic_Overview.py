import streamlit as st
from databricks import sql
import pandas as pd
import plotly.express as px
import os
import sys

# Add parent directory to path to allow importing utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_data

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

# --- Filter 2: Team ---
selected_team = "All Teams"
if not df_filtered.empty and "Team" in df_filtered.columns:
    all_teams = sorted(df_filtered["Team"].unique().tolist())
    team_options = ["All Teams"] + all_teams
    selected_team = st.sidebar.selectbox("Select Team", team_options)
    
    if selected_team != "All Teams":
        df_filtered = df_filtered[df_filtered["Team"] == selected_team]

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
    ),
    # --- Per Game Formatting (Display 1 decimal, keep precision for plots) ---
    "PPG": st.column_config.NumberColumn("PPG", format="%.1f"),
    "xGD PPG": st.column_config.NumberColumn("xGD PPG", format="%.1f"),
    "G PG": st.column_config.NumberColumn("G PG", format="%.1f"),
    "GA PG": st.column_config.NumberColumn("GA PG", format="%.1f"),
    "GD PG": st.column_config.NumberColumn("GD PG", format="%.1f"),
    "xG PG": st.column_config.NumberColumn("xG PG", format="%.1f"),
    "xGA PG": st.column_config.NumberColumn("xGA PG", format="%.1f"),
    "xGD PG": st.column_config.NumberColumn("xGD PG", format="%.1f")
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
            # Exclude Position and non-metric columns from sort options
            exclude_sort = ["Position", "Team", "League", "Season", "MP"]
            # Available columns for sorting (intersection of desired order and actual columns)
            available_cols = [c for c in column_order if c in df_filtered.columns and c not in exclude_sort]
            
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

# ============================================================
# 6. Rolling Average Chart
# ============================================================
st.markdown("---")
st.subheader("📈 Team Form (8-Game Rolling Average)")

# 1. Load Match Results Data (Silver Layer)
with st.spinner("Loading match results..."):
    df_results = get_data("results", schema="processed")

# Pre-processing
if not df_results.empty:
    if "date" in df_results.columns:
        df_results["date"] = pd.to_datetime(df_results["date"])
    
    # Capitalize columns
    df_results.columns = [c.capitalize() for c in df_results.columns]

    # Layout
    col_filters, col_graph = st.columns([1, 3])

    with col_filters:
        st.markdown("### Filters")

        numeric_cols = df_results.select_dtypes(include=['float', 'int']).columns.tolist()
        numeric_cols = [c for c in numeric_cols if "id" not in c.lower()]

        # --- Team Filters ---
        if "Team" in df_results.columns:
            team_options = sorted(df_results["Team"].unique())
            team_1 = st.selectbox("Team 1", team_options, index=0, key="hist_roll_team_1")
            team_2 = st.selectbox("Team 2", ["None"] + team_options, index=0, key="hist_roll_team_2")
        else:
            st.error("Team column not found.")
            team_1, team_2 = None, None

        # --- Season Filters (Historic Page Only) ---
        if "Season" in df_results.columns:
            season_options = sorted(df_results["Season"].unique(), reverse=True)
            season_1 = st.selectbox("Season 1", season_options, key="hist_season_1")
            season_2 = st.selectbox("Season 2", ["None"] + season_options, key="hist_season_2")
        else:
            season_1, season_2 = None, None

        # --- Metric Filters ---
        if numeric_cols:
            default_idx = numeric_cols.index("Xg") if "Xg" in numeric_cols else 0
            metric_1 = st.selectbox("Metric 1", numeric_cols, index=default_idx, key="hist_roll_metric_1")
            metric_2 = st.selectbox("Metric 2", ["None"] + numeric_cols, index=0, key="hist_roll_metric_2")
        else:
            metric_1, metric_2 = None, None

    with col_graph:
        if team_1 and metric_1 and season_1:
            # Logic to build the plot data
            pairs = []
            pairs.append({"team": team_1, "season": season_1})
            
            if team_2 != "None" and season_2 != "None":
                pairs.append({"team": team_2, "season": season_2})
            elif team_2 == "None" and season_2 != "None":
                 # Compare same team across seasons
                 pairs.append({"team": team_1, "season": season_2})
            elif team_2 != "None" and season_2 == "None":
                 # Compare two teams in same season (season 1)
                 pairs.append({"team": team_2, "season": season_1})

            metrics_to_plot = [m for m in [metric_1, metric_2] if m != "None"]

            plot_data = pd.DataFrame()

            for pair in pairs:
                p_team = pair["team"]
                p_season = pair["season"]
                
                # Get team data for specific season
                team_df = df_results[(df_results["Team"] == p_team) & (df_results["Season"] == p_season)].copy()
                team_df = team_df.sort_values("Date")
                
                # Reset index to have a sequential game number (1 to N) for x-axis comparison across seasons
                team_df["Game Number"] = range(1, len(team_df) + 1)

                # Calculate Rolling 8 Average
                for metric in metrics_to_plot:
                    col_name = f"{p_team} ({p_season}) - {metric}"
                    team_df[col_name] = team_df[metric].rolling(window=8, min_periods=1).mean()

                    # Keep Game Number and rolling metric
                    temp_df = team_df[["Game Number", col_name]].set_index("Game Number")

                    # Merge
                    plot_data = pd.merge(plot_data, temp_df, left_index=True, right_index=True, how='outer') if not plot_data.empty else temp_df

            if not plot_data.empty:
                fig = px.line(plot_data, x=plot_data.index, y=plot_data.columns,
                            title=f"Rolling 8-Game Average Comparison",
                            labels={"value": "Average Value", "Game Number": "Match Number (in Season)", "variable": "Legend"})
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No data found for the selected combination.")
        else:
            st.info("Please select at least one team, season, and metric.")

else:
    st.error("Could not load results data.")
