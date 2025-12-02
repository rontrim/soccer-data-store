import os
import pandas as pd
import soccerdata as sd
from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential
import traceback
import adlfs

# --- Setup for ADLS Connection ---
load_dotenv()
storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME")
container_name = os.getenv("CONTAINER_NAME")
credential = DefaultAzureCredential()

# --- Data Fetching ---
print("Fetching team match stats from Understat...")
understat = sd.Understat(
    leagues=['ENG-Premier League', 'ESP-La Liga', 'FRA-Ligue 1', 'GER-Bundesliga', 'ITA-Serie A'], 
    seasons=['2526']
)

understat_team_match_stats = understat.read_team_match_stats()
print("Successfully fetched data.")

# ---
# 💡 FIX APPLIED HERE
# Convert the 'date' column from a datetime object to an ISO 8601 string.
# This format is universally compatible and avoids Parquet timestamp issues.
# ---
print("Converting 'date' column to string format to ensure compatibility...")
understat_team_match_stats['date'] = understat_team_match_stats['date'].dt.strftime('%Y-%m-%dT%H:%M:%S')

# --- Save to ADLS ---
try:
    # 1. Define the destination folder and file path in ADLS
    folder_name = "match_team_stats_understat"
    ingestion_date_str = pd.Timestamp.now().strftime('%Y-%m-%d')
    understat_team_match_stats = understat_team_match_stats.assign(ingestion_date=ingestion_date_str)
    file_name = f"understat_team_match_stats_{ingestion_date_str}.parquet"
    
    adls_full_path = f"abfs://{container_name}/{folder_name}/{file_name}"

    # 2. Write the DataFrame directly to the ADLS path
    print(f"Writing data to ADLS at: {adls_full_path}...")
    understat_team_match_stats.to_parquet(
        adls_full_path,
        storage_options={
            "account_name": storage_account_name,
            "credential": credential
        }
    )
    
    print("✅ Successfully saved data to ADLS.")

except Exception:
    print(f"--- FAILED: Could not save data to ADLS. Error details below ---")
    traceback.print_exc()
    print("--------------------------------------------------\n")   


# --- Setup for SoccerData ---
stat_types = [
    'schedule', 'keeper', 'passing', 'passing_types', 'defense', 
    'possession', 'misc', 'shooting', 'goal_shot_creation'
]

seasons = ['2526']  # 2025/2026 season
fbref = sd.FBref(
    leagues=['ENG-Premier League', 'ESP-La Liga', 'FRA-Ligue 1', 'GER-Bundesliga', 'ITA-Serie A'],
    seasons=seasons
)

ingestion_date_str = pd.Timestamp.now().strftime('%Y-%m-%d')

# --- Main Loop: Process each stat type and upload to ADLS ---
for stat in stat_types:
    print(f"Processing match data for stat_type: '{stat}'...")

    try:
        df = fbref.read_team_match_stats(stat_type=stat)
        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.strftime('%Y-%m-%dT%H:%M:%S')
        df_flat = df.reset_index()
        new_columns = ['_'.join(col).strip() if isinstance(col, tuple) and col[1] else col[0] if isinstance(col, tuple) else col for col in df_flat.columns]
        df_flat.columns = new_columns
        df_final = df_flat.assign(ingestion_date=ingestion_date_str)

        # ---
        # 💡 NEW: Systematic Cleaning Section
        # This loop automatically finds and fixes columns with mixed data types.
        # ---
        print("Searching for object columns to clean...")
        for col in df_final.select_dtypes(include=['object']).columns:
            # Attempt to convert the column to numeric, turning errors into NaNs
            numeric_col = pd.to_numeric(df_final[col], errors='coerce')
            
            # Check if the conversion was at all successful (i.e., not all values are NaN)
            if numeric_col.notna().any():
                print(f"  -> Cleaning and converting column '{col}' to a numeric type.")
                # Replace the original column with the cleaned version
                # Fill any remaining NaNs with 0 and cast to a whole number
                df_final[col] = numeric_col.fillna(0).astype(int)

        folder_name = f"match_team_{stat}"
        file_name = f"match_data_{ingestion_date_str}.parquet"
        adls_full_path = f"abfs://{container_name}/{folder_name}/{file_name}"

        print(f"Writing all seasons to ADLS: {adls_full_path}")
        df_final.to_parquet(
            adls_full_path,
            storage_options={
                "account_name": storage_account_name,
                "credential": credential
            }
        )
        print(f"✅ Successfully saved all seasons for '{stat}' to a single file.\n")

    except Exception:
        print(f"--- FAILED: Could not process '{stat}'. Error details below ---")
        traceback.print_exc()
        print("--------------------------------------------------\n")

print("All stat types processed and uploaded to ADLS.")