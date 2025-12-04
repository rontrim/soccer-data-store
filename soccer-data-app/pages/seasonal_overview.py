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

st.title("⚽ Big 5 Leagues Club Overview")

# ============================================================
# 2. Database Connection
# ============================================================
SQL_HTTP_PATH = "/sql/1.0/warehouses/ced38d4ec14f78c6"
CATALOG = "soccer_data"
SCHEMA = "analyze"
TABLE_HEADLINE = "headline_stats"
TABLE_FORM = "form_stats"


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

# Create Combined DataFrames (All Leagues, Selected Season)
df_headline_combined = df_headline.copy() if not df_headline.empty else pd.DataFrame()
df_form_combined = df_form.copy() if not df_form.empty else pd.DataFrame()

if selected_season:
    if not df_headline_combined.empty and "Season" in df_headline_combined.columns:
        df_headline_combined = df_headline_combined[df_headline_combined["Season"] == selected_season]
    if not df_form_combined.empty and "Season" in df_form_combined.columns:
        df_form_combined = df_form_combined[df_form_combined["Season"] == selected_season]

# ============================================================
# 5. Main UI Tabs
# ============================================================
tab1, tab2, tab3, tab4 = st.tabs(["League Table", "Form Table (Last 8 Games)", "Combined League Table", "Combined Form Table (Last 8 Games)"])

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

# Desired column order
column_order = [
    "Position", "Team", "MP", "W", "D", "L", "Points", "PPG",
    "xGD W", "xGD D", "xGD L", "xGD Points", "xGD PPG",
    "G", "GA", "GD", "xG", "xGA", "xGD",
    "G PG", "GA PG", "GD PG", "xG PG", "xGA PG", "xGD PG",
    "Possession"
]

column_order_combined = ["Position", "Team", "League"] + [c for c in column_order if c not in ["Position", "Team"]]

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
            width="stretch", 
            hide_index=True,
            column_config=shared_column_config
        )
        # Footnote
        st.caption("Source: Opta")

        # --- Scatter Plot ---
        st.divider()
        st.subheader("League Performance Analysis")
        
        # Columns available for plotting (exclude non-numeric/ID columns)
        exclude_cols = ["Position", "Team", "MP", "Team-Season", "Team Abbreviation", "Season", "League"]
        plot_cols = [c for c in df_headline_filtered.columns if c not in exclude_cols]
        
        # Columns that should be reversed (Lower is Better)
        reverse_cols = ["L", "xGD L", "GA", "xGA", "GA PG", "xGA PG"]

        c1, c2, c3 = st.columns(3)
        with c1:
            x_axis = st.selectbox("X-Axis", plot_cols, index=plot_cols.index("xG") if "xG" in plot_cols else 0, key="l_x")
        with c2:
            y_axis = st.selectbox("Y-Axis", plot_cols, index=plot_cols.index("G") if "G" in plot_cols else 0, key="l_y")
        with c3:
            color_col = st.selectbox("Color by", plot_cols, index=plot_cols.index("Points") if "Points" in plot_cols else 0, key="l_c")

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
            
            # Remove gridlines and zerolines
            fig.update_xaxes(showgrid=False, zeroline=False)
            fig.update_yaxes(showgrid=False, zeroline=False)

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

            st.plotly_chart(fig, width='stretch')

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
            width="stretch", 
            hide_index=True,
            column_config=shared_column_config
        )
        # Footnote
        st.caption("Source: Opta")

        # --- Scatter Plot ---
        st.divider()
        st.subheader("Form Analysis (Last 8 Games)")
        
        # Columns available for plotting
        exclude_cols = ["Position", "Team", "MP", "Team-Season", "Team Abbreviation", "Season", "League"]
        plot_cols_form = [c for c in df_form_filtered.columns if c not in exclude_cols]
        
        # Columns that should be reversed (Lower is Better)
        reverse_cols = ["L", "xGD L", "GA", "xGA", "GA PG", "xGA PG"]

        c1, c2, c3 = st.columns(3)
        with c1:
            x_axis_f = st.selectbox("X-Axis", plot_cols_form, index=plot_cols_form.index("xG") if "xG" in plot_cols_form else 0, key="f_x")
        with c2:
            y_axis_f = st.selectbox("Y-Axis", plot_cols_form, index=plot_cols_form.index("G") if "G" in plot_cols_form else 0, key="f_y")
        with c3:
            color_col_f = st.selectbox("Color by", plot_cols_form, index=plot_cols_form.index("Points") if "Points" in plot_cols_form else 0, key="f_c")

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
            
            # Remove gridlines and zerolines
            fig_f.update_xaxes(showgrid=False, zeroline=False)
            fig_f.update_yaxes(showgrid=False, zeroline=False)

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

            st.plotly_chart(fig_f, width='stretch')

    else:
        st.info("No form data found.")

with tab3:
    if not df_headline_combined.empty:
        # --- Sort Controls ---
        c_sort1, c_sort2 = st.columns([1, 3])
        with c_sort1:
            # Exclude Position and non-metric columns from sort options
            exclude_sort = ["Position", "Team", "League", "Season", "MP"]
            sort_options = [c for c in column_order_combined if c not in exclude_sort]
            # Default to PPG
            default_idx = sort_options.index("PPG") if "PPG" in sort_options else 0
            sort_col = st.selectbox("Sort Table By", sort_options, index=default_idx, key="sort_c_l")
        with c_sort2:
            sort_asc = st.radio("Order", ["Descending", "Ascending"], index=0, horizontal=True, key="order_c_l")
        
        ascending = sort_asc == "Ascending"
        
        # Sort
        if sort_col in df_headline_combined.columns:
             df_headline_combined = df_headline_combined.sort_values(by=[sort_col], ascending=ascending)
        
        # Add Position
        df_headline_combined["Position"] = range(1, len(df_headline_combined) + 1)
        
        # Filter columns
        cols = [c for c in column_order_combined if c in df_headline_combined.columns]
             
        st.dataframe(
            df_headline_combined[cols], 
            width="stretch", 
            hide_index=True,
            column_config=shared_column_config
        )
        st.caption("Source: Opta | Includes all Big 5 League teams for the selected season")

        # --- Scatter Plot ---
        st.divider()
        st.subheader("Combined League Performance Analysis")
        
        # Columns available for plotting
        exclude_cols = ["Position", "Team", "MP", "Team-Season", "Team Abbreviation", "Season", "League"]
        plot_cols_combined = [c for c in df_headline_combined.columns if c not in exclude_cols]
        
        reverse_cols = ["L", "xGD L", "GA", "xGA", "GA PG", "xGA PG"]

        c1, c2, c3 = st.columns(3)
        with c1:
            x_axis_c = st.selectbox("X-Axis", plot_cols_combined, index=plot_cols_combined.index("xG") if "xG" in plot_cols_combined else 0, key="c_l_x")
        with c2:
            y_axis_c = st.selectbox("Y-Axis", plot_cols_combined, index=plot_cols_combined.index("G") if "G" in plot_cols_combined else 0, key="c_l_y")
        with c3:
            color_col_c = st.selectbox("Color by", plot_cols_combined, index=plot_cols_combined.index("Points") if "Points" in plot_cols_combined else 0, key="c_l_c")

        if x_axis_c and y_axis_c and color_col_c:
            try:
                is_reverse_color = color_col_c in reverse_cols
                labels = ["Q1 (Low)", "Q2", "Q3", "Q4 (High)"]
                if is_reverse_color:
                    labels = ["Q4 (High)", "Q3", "Q2", "Q1 (Low)"]

                df_headline_combined["Quartile"] = pd.qcut(
                    df_headline_combined[color_col_c], 
                    q=4, 
                    labels=labels,
                    duplicates='drop'
                )
            except ValueError:
                df_headline_combined["Quartile"] = "Q1 (Low)"

            color_map = {
                "Q1 (Low)": "#C8E6C9",
                "Q2": "#81C784",
                "Q3": "#43A047",
                "Q4 (High)": "#1B5E20"
            }

            fig_c = px.scatter(
                df_headline_combined,
                x=x_axis_c,
                y=y_axis_c,
                color="Quartile",
                text="Team Abbreviation" if "Team Abbreviation" in df_headline_combined.columns else "Team",
                color_discrete_map=color_map,
                category_orders={"Quartile": ["Q1 (Low)", "Q2", "Q3", "Q4 (High)"]},
                hover_data=["Team", "Position", color_col_c]
            )
            
            fig_c.update_traces(
                textposition='bottom center',
                textfont=dict(color='black', size=10),
                marker=dict(size=12, line=dict(width=1, color='DarkSlateGrey'))
            )
            
            fig_c.update_layout(
                xaxis_title=x_axis_c,
                yaxis_title=y_axis_c,
                font=dict(color='black'),
                plot_bgcolor='white'
            )
            
            fig_c.update_xaxes(showgrid=False, zeroline=False)
            fig_c.update_yaxes(showgrid=False, zeroline=False)

            if x_axis_c in reverse_cols:
                fig_c.update_xaxes(autorange="reversed")
            if y_axis_c in reverse_cols:
                fig_c.update_yaxes(autorange="reversed")

            x_median = df_headline_combined[x_axis_c].median()
            y_median = df_headline_combined[y_axis_c].median()

            fig_c.add_vline(x=x_median, line_width=1, line_dash="dash", line_color="black")
            fig_c.add_hline(y=y_median, line_width=1, line_dash="dash", line_color="black")

            st.plotly_chart(fig_c, width='stretch')

    else:
        st.info("No combined data found.")

with tab4:
    if not df_form_combined.empty:
        # --- Sort Controls ---
        c_sort1, c_sort2 = st.columns([1, 3])
        with c_sort1:
            # Exclude Position and non-metric columns from sort options
            exclude_sort = ["Position", "Team", "League", "Season", "MP"]
            sort_options_f = [c for c in column_order_combined if c not in exclude_sort]
            # Default to PPG
            default_idx_f = sort_options_f.index("PPG") if "PPG" in sort_options_f else 0
            sort_col_f = st.selectbox("Sort Table By", sort_options_f, index=default_idx_f, key="sort_c_f")
        with c_sort2:
            sort_asc_f = st.radio("Order", ["Descending", "Ascending"], index=0, horizontal=True, key="order_c_f")
        
        ascending_f = sort_asc_f == "Ascending"
        
        # Sort
        if sort_col_f in df_form_combined.columns:
            df_form_combined = df_form_combined.sort_values(by=[sort_col_f], ascending=ascending_f)
        
        # Add Position
        df_form_combined["Position"] = range(1, len(df_form_combined) + 1)

        # Filter columns
        cols = [c for c in column_order_combined if c in df_form_combined.columns]
            
        st.dataframe(
            df_form_combined[cols], 
            width="stretch", 
            hide_index=True,
            column_config=shared_column_config
        )
        st.caption("Source: Opta | Includes all Big 5 League teams for the selected season")

        # --- Scatter Plot ---
        st.divider()
        st.subheader("Combined Form Analysis (Last 8 Games)")
        
        # Columns available for plotting
        exclude_cols = ["Position", "Team", "MP", "Team-Season", "Team Abbreviation", "Season", "League"]
        plot_cols_form_c = [c for c in df_form_combined.columns if c not in exclude_cols]
        
        reverse_cols = ["L", "xGD L", "GA", "xGA", "GA PG", "xGA PG"]

        c1, c2, c3 = st.columns(3)
        with c1:
            x_axis_fc = st.selectbox("X-Axis", plot_cols_form_c, index=plot_cols_form_c.index("xG") if "xG" in plot_cols_form_c else 0, key="c_f_x")
        with c2:
            y_axis_fc = st.selectbox("Y-Axis", plot_cols_form_c, index=plot_cols_form_c.index("G") if "G" in plot_cols_form_c else 0, key="c_f_y")
        with c3:
            color_col_fc = st.selectbox("Color by", plot_cols_form_c, index=plot_cols_form_c.index("Points") if "Points" in plot_cols_form_c else 0, key="c_f_c")

        if x_axis_fc and y_axis_fc and color_col_fc:
            try:
                is_reverse_color = color_col_fc in reverse_cols
                labels = ["Q1 (Low)", "Q2", "Q3", "Q4 (High)"]
                if is_reverse_color:
                    labels = ["Q4 (High)", "Q3", "Q2", "Q1 (Low)"]

                df_form_combined["Quartile"] = pd.qcut(
                    df_form_combined[color_col_fc], 
                    q=4, 
                    labels=labels,
                    duplicates='drop'
                )
            except ValueError:
                df_form_combined["Quartile"] = "Q1 (Low)"

            color_map = {
                "Q1 (Low)": "#C8E6C9",
                "Q2": "#81C784",
                "Q3": "#43A047",
                "Q4 (High)": "#1B5E20"
            }

            fig_fc = px.scatter(
                df_form_combined,
                x=x_axis_fc,
                y=y_axis_fc,
                color="Quartile",
                text="Team Abbreviation" if "Team Abbreviation" in df_form_combined.columns else "Team",
                color_discrete_map=color_map,
                category_orders={"Quartile": ["Q1 (Low)", "Q2", "Q3", "Q4 (High)"]},
                hover_data=["Team", "Position", color_col_fc]
            )
            
            fig_fc.update_traces(
                textposition='bottom center',
                textfont=dict(color='black', size=10),
                marker=dict(size=12, line=dict(width=1, color='DarkSlateGrey'))
            )
            
            fig_fc.update_layout(
                xaxis_title=x_axis_fc,
                yaxis_title=y_axis_fc,
                font=dict(color='black'),
                plot_bgcolor='white'
            )
            
            fig_fc.update_xaxes(showgrid=False, zeroline=False)
            fig_fc.update_yaxes(showgrid=False, zeroline=False)

            if x_axis_fc in reverse_cols:
                fig_fc.update_xaxes(autorange="reversed")
            if y_axis_fc in reverse_cols:
                fig_fc.update_yaxes(autorange="reversed")

            x_median = df_form_combined[x_axis_fc].median()
            y_median = df_form_combined[y_axis_fc].median()

            fig_fc.add_vline(x=x_median, line_width=1, line_dash="dash", line_color="black")
            fig_fc.add_hline(y=y_median, line_width=1, line_dash="dash", line_color="black")

            st.plotly_chart(fig_fc, width='stretch')

    else:
        st.info("No combined form data found.")

# ============================================================
# 6. Rolling Average Chart
# ============================================================
st.markdown("---")
st.subheader("📈 Team Form (8-Game Rolling Average)")

# 1. Load Match Results Data (Silver Layer)
# We fetch the whole table once to save on DB calls
with st.spinner("Loading match results..."):
    df_results = get_data("results", schema="processed")

# Pre-processing (Ensure dates are datetime and sort)
if not df_results.empty:
    if "date" in df_results.columns:
        df_results["date"] = pd.to_datetime(df_results["date"])
    
    # Capitalize columns for display
    df_results.columns = [c.capitalize() for c in df_results.columns]

    # Layout: Filters on Left (1 part), Graph on Right (3 parts)
    col_filters, col_graph = st.columns([1, 3])

    with col_filters:
        st.markdown("### Filters")

        # Dictionary mapping the UI Label (PG) to the underlying Raw Column in 'results'
        # These raw columns must exist in your df_results (from the silver layer)
        metric_map = {
            "PPG": "Points",            # Points Per Game
            "xGD_PPG": "Expected_result_points", # xPoints Per Game
            "G_PG": "Goals",            # Goals Per Game
            "GA_PG": "Goals_allowed",   # Goals Allowed Per Game
            "GD_PG": "Goal_difference", # Goal Difference Per Game
            "xG_PG": "Expected_goals",  # xG Per Game
            "xGA_PG": "Expected_goals_allowed", # xGA Per Game
            "xGD_PG": "Expected_goal_difference" # xGD Per Game
        }

        # --- Team Filters ---
        if "Team" in df_results.columns:
            team_options = sorted(df_results["Team"].unique())
            team_1 = st.selectbox("Team 1", team_options, index=0, key="roll_team_1")
            team_2 = st.selectbox("Team 2", ["None"] + team_options, index=0, key="roll_team_2")
        else:
            st.error("Team column not found.")
            team_1, team_2 = None, None

        # --- Metric Filters ---
        pg_metrics = list(metric_map.keys())
        # Default to xG_PG or first metric
        default_idx = pg_metrics.index("xG_PG") if "xG_PG" in pg_metrics else 0
        metric_1 = st.selectbox("Metric 1", pg_metrics, index=default_idx, key="roll_metric_1")
        metric_2 = st.selectbox("Metric 2", ["None"] + pg_metrics, index=0, key="roll_metric_2")

    with col_graph:
        if team_1 and metric_1:
            # Get the raw column names for the selected metrics
            raw_metric_1 = metric_map.get(metric_1)
            raw_metric_2 = metric_map.get(metric_2) if metric_2 != "None" else None

            # Validate that raw columns exist in df_results
            raw_metrics_to_calc = [m for m in [raw_metric_1, raw_metric_2] if m]
            missing_cols = [col for col in raw_metrics_to_calc if col not in df_results.columns]
            
            if missing_cols:
                st.error(f"Missing required columns in data: {', '.join(missing_cols)}")
            else:
                plot_data = pd.DataFrame()

                for team_name in [team_1, team_2]:
                    if team_name == "None": 
                        continue
                    
                    # Filter for Team
                    team_df = df_results[df_results["Team"] == team_name].copy()
                    team_df = team_df.sort_values("Date")
                    
                    # --- Key Calculation Step ---
                    # We use a rolling window of 8
                    
                    for raw_col in raw_metrics_to_calc:
                        # 1. Calculate Rolling Sum of the stat (e.g., Total Goals in last 8 games)
                        rolling_sum = team_df[raw_col].rolling(window=8, min_periods=1).sum()
                        
                        # 2. Calculate Rolling Count of matches (Matches Played in window)
                        # usually 8, but allows for partial windows at start of season
                        rolling_count = team_df[raw_col].rolling(window=8, min_periods=1).count()
                        
                        # 3. Calculate the PG metric
                        # This matches the logic: Sum(Stat) / Count(Games)
                        # Handle division by zero by replacing 0 count with NaN
                        pg_metric_name = next((k for k, v in metric_map.items() if v == raw_col), None)
                        if pg_metric_name is None:
                            continue
                            
                        legend_name = f"{team_name} - {pg_metric_name}"
                        
                        team_df[legend_name] = rolling_sum / rolling_count.replace(0, pd.NA)
                        
                        # Merge for plotting
                        temp_df = team_df[["Date", legend_name]].set_index("Date")
                        plot_data = pd.merge(plot_data, temp_df, left_index=True, right_index=True, how='outer') if not plot_data.empty else temp_df

                if not plot_data.empty:
                    fig = px.line(plot_data, x=plot_data.index, y=plot_data.columns,
                                title=f"Rolling 8-Game Average (Per Game)",
                                labels={"value": "Per Game Average", "Date": "Match Date", "variable": "Legend"})
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Select a team and metric to visualize trends.")
        else:
            st.info("Please select at least one team and one metric.")

else:
    st.error("Could not load results data.")
