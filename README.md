# soccer-data-store

Let me look at the main Python file and requirements to understand what this project does:Based on the files in your repository, here's a polished README description for your soccer-data-store project:

---

## ⚽ Soccer Data Store

A comprehensive data pipeline and analytics application for Big 5 European soccer leagues (Premier League, La Liga, Serie A, Bundesliga, Ligue 1). This project combines data engineering best practices with interactive visualization to provide deep insights into club performance metrics.

### What This Project Does

**Soccer Data Store** processes and analyzes club-level soccer statistics using a medallion architecture (Bronze → Silver → Gold layers) and provides an interactive dashboard for exploring league standings and recent form data.

### Key Features

- **Multi-League Analytics**: Compare teams across the Big 5 leagues
- **Interactive Dashboard**: Built with Streamlit for real-time data exploration
- **Advanced Metrics**: Includes expected goals differential (xGD), possession stats, and performance indicators
- **Dynamic Filtering**: Filter by league and season to analyze specific competitions
- **Form Tracking**: Monitor team performance over the last 8 games
- **Cloud Integration**: Connects to Databricks for scalable data warehousing

### Tech Stack

- **Frontend**: Streamlit (interactive web dashboard)
- **Data Processing**: Pandas
- **Backend**: Databricks SQL Connector & Databricks SDK
- **Data Architecture**: Medallion Pattern (Bronze/Silver/Gold layers)

### Data Structure

- **Bronze Layer**: Raw data ingestion
- **Silver Layer**: Cleaned and standardized data
- **Gold Layer**: Analysis-ready aggregated metrics

### Dashboard Features

- **League Table Tab**: Season standings sorted by points with possession percentage and xGD analysis
- **Form Table Tab**: Last 8 games performance with current season records
- **Smart Tooltips**: Context-aware help text explaining statistical metrics

---

Feel free to customize this further based on your specific goals or any additional features you plan to add!