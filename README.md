# ⚽ Soccer Data Store

A comprehensive data pipeline and analytics application for Big 5 European soccer leagues (Premier League, La Liga, Serie A, Bundesliga, Ligue 1). This project combines data engineering best practices with interactive visualization to provide deep insights into club performance metrics.

## 🎯 What This Project Does

**Soccer Data Store** automates the collection and analysis of soccer statistics. It fetches raw match data, cleans it, and calculates advanced metrics like Expected Goals (xG) and recent form. The results are presented in an interactive dashboard that allows users to explore league standings and team performance.

## ✨ Key Features

- **Multi-League Analytics**: Compare teams across the Big 5 leagues, with options to view individual leagues or a combined view of all teams.
- **Historical Analysis**: Explore team performance trends across multiple seasons.
- **Interactive Dashboard**: Multipage Streamlit app for real-time data exploration with dynamic filtering by league, season, and team.
- **Advanced Metrics**: Includes expected goals differential (xGD), possession stats, and performance indicators (xG, xGA, PPG).
- **Customizable Performance Plots**: Interactive scatter plots with user-selectable axes for deep performance analysis.
- **Quartile Analysis**: Visual color-coding of teams based on performance quartiles (Q1-Q4) for selected metrics.
- **Form Tracking**: Monitor team performance over the last 8 games with dedicated tables and visual analysis.
- **Cloud Integration**: Powered by Databricks for scalable data processing.

## 📊 Dashboard Overview

The application is structured into two main pages, each equipped with data tables and analytical visualizations:

### 1. Seasonal Overview
Focuses on the current season's performance with four distinct views:
-   **League Table**: Live season standings for a specific league.
-   **Form Table**: Performance over the last 8 games for a specific league.
-   **Combined Tables**: Cross-league comparison tables (League & Form) allowing you to rank all teams from the Big 5 leagues against each other.

**Visual Analysis Features:**
-   **Performance Scatter Plots**: Customizable plots on every tab allowing correlation of any two metrics (e.g., xG vs. Goals).
-   **Quartile Coloring**: Teams are color-coded (Q1-Q4) based on a selected third metric (e.g., Points).
-   **Median Benchmarks**: Dashed lines indicate the median performance for the selected group.

### 2. Historic Overview
Allows users to explore data from previous seasons.
-   **Deep Dive**: Analyze how team stats have evolved over time.
-   **Team Filtering**: Filter by specific teams to isolate their historical performance.

## 🏗️ How It Works (The Data Journey)

The data flows through a "Medallion Architecture" to ensure quality:

1.  **Ingestion**: Scripts automatically fetch the latest match data.
2.  **Bronze Layer**: Raw data is stored securely.
3.  **Silver Layer**: Data is cleaned, standardized, and checked for errors.
4.  **Gold Layer**: Final stats and tables are calculated for the dashboard.

## 🛠️ Tech Stack

- **Frontend**: Streamlit
- **Data Engineering**: Databricks (Delta Live Tables, PySpark)
- **Data Source**: `soccerdata` API

---

## 👨‍💻 Developer Guide

### Project Structure

```text
soccer-data-store/
├── club-soccer-data/       # Databricks transformation logic (Bronze/Silver/Gold)
├── ingestion/              # Local/Standalone ingestion scripts
├── resources/              # Databricks Asset Bundle (DAB) definitions
├── soccer-data-app/        # Streamlit frontend application
└── databricks.yml          # DAB configuration
```

### Getting Started

**1. Data Pipeline (Databricks)**
```bash
databricks bundle deploy
databricks bundle run Club_Soccer_Data_Processing
```

**2. Streamlit App**
```bash
cd soccer-data-app
pip install -r requirements.txt
streamlit run app.py
```
