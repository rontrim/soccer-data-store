# ⚽ Soccer Data Store

A comprehensive data pipeline and analytics application for Big 5 European soccer leagues (Premier League, La Liga, Serie A, Bundesliga, Ligue 1). This project combines data engineering best practices with interactive visualization to provide deep insights into club performance metrics.

## 🎯 What This Project Does

**Soccer Data Store** automates the collection and analysis of soccer statistics. It fetches raw match data, cleans it, and calculates advanced metrics like Expected Goals (xG) and recent form. The results are presented in an interactive dashboard that allows users to explore league standings and team performance.

## ✨ Key Features

- **Multi-League Analytics**: Compare teams across the Big 5 leagues.
- **Interactive Dashboard**: Built with Streamlit for real-time data exploration with dynamic filtering by league and season.
- **Advanced Metrics**: Includes expected goals differential (xGD), possession stats, and performance indicators (xG, xGA, PPG).
- **Customizable Performance Plots**: Interactive scatter plots with user-selectable axes for deep performance analysis.
- **Quartile Analysis**: Visual color-coding of teams based on performance quartiles (Q1-Q4) for selected metrics.
- **Form Tracking**: Monitor team performance over the last 8 games with dedicated tables and visual analysis.
- **Cloud Integration**: Powered by Databricks for scalable data processing.

## 📊 Dashboard Overview

The application provides two main views, each equipped with data tables and analytical visualizations:

1.  **League Table**:
    -   Live season standings sorted by points.
    -   Visual indicators for **Possession %** and **xGD breakdown** (Wins/Draws/Losses).
    -   **League Performance Analysis**: A customizable scatter plot allowing users to correlate any two metrics (e.g., xG vs. Goals). It features:
        -   **Quartile Coloring**: Teams are color-coded from light to dark green based on a third metric (e.g., Points).
        -   **Median Lines**: Dashed lines indicating the league median for both axes to benchmark performance.

2.  **Form Table (Last 8 Games)**:
    -   A focused view on recent performance.
    -   **Form Analysis**: Similar to the league view, this includes a customizable scatter plot to visualize current form trends using the same quartile and median benchmarking tools.

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
