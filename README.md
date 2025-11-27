# ⚽ Soccer Data Store

A comprehensive data pipeline and analytics application for Big 5 European soccer leagues (Premier League, La Liga, Serie A, Bundesliga, Ligue 1). This project combines data engineering best practices with interactive visualization to provide deep insights into club performance metrics.

## 🎯 What This Project Does

**Soccer Data Store** automates the collection and analysis of soccer statistics. It fetches raw match data, cleans it, and calculates advanced metrics like Expected Goals (xG) and recent form. The results are presented in an interactive dashboard that allows users to explore league standings and team performance.

## ✨ Key Features

- **Multi-League Analytics**: Compare teams across the Big 5 leagues.
- **Interactive Dashboard**: Built with Streamlit for real-time data exploration.
- **Advanced Metrics**: Includes expected goals differential (xGD), possession stats, and performance indicators.
- **Dynamic Filtering**: Filter by league and season to analyze specific competitions.
- **Form Tracking**: Monitor team performance over the last 8 games.
- **Cloud Integration**: Powered by Databricks for scalable data processing.

## 📊 Dashboard Overview

The application provides two main views:

1.  **League Table**: Live season standings sorted by points, featuring:
    - Possession percentage bars.
    - xGD (Expected Goals Difference) analysis.
2.  **Form Table**: A deep dive into the last 8 games to see who is hot and who is not.

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
