import os
import pandas as pd
import soccerdata as sd
from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential
import traceback

# --- Setup for ADLS Connection ---
load_dotenv()
storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME")
container_name = os.getenv("CONTAINER_NAME")
credential = DefaultAzureCredential()

# --- Setup for SoccerData ---
stat_types = [
    'keeper', 'keeper_adv', 'shooting', 'passing',
    'passing_types', 'goal_shot_creation', 'defense', 'possession',
    'playing_time', 'misc'
]

fbref = sd.FBref(
    leagues=['ENG-Premier League', 'ESP-La Liga', 'FRA-Ligue 1', 'GER-Bundesliga', 'ITA-Serie A'],
    seasons='2526'  # 2025/2026 season
)

ingestion_date_str = pd.Timestamp.now().strftime('%Y-%m-%d')


# --- Main Loop: Process each stat type and UPLOAD to ADLS ---
for stat in stat_types:
    print(f"Processing data for stat_type: '{stat}'...")

    try:
        df = fbref.read_team_season_stats(stat_type=stat)
        df_flat = df.reset_index()
        new_columns = ['_'.join(col).strip() if col[1] else col[0] for col in df_flat.columns]
        df_flat.columns = new_columns
        df_final = df_flat.assign(ingestion_date=ingestion_date_str)

        # --- UPDATED ADLS WRITE LOGIC ---
        
        # 1. Define the destination folder and file path
        # NOTE: We construct the full "abfs://" URI for pandas
        adls_full_path = f"abfs://{container_name}/season_team_{stat}/ingestion_date_{ingestion_date_str}.parquet"
        
        # 2. Write the DataFrame directly to the ADLS path, overwriting if it exists
        print(f"Writing/overwriting data to ADLS: {adls_full_path}...")
        df_final.to_parquet(
            adls_full_path,
            storage_options={
                "account_name": storage_account_name,
                "credential": credential
            }
        )
            
        print(f"Successfully saved data to ADLS.\n")

    except Exception:
        print(f"--- Could not process '{stat}'. Error details below ---")
        traceback.print_exc()
        print("--------------------------------------------------\n")


print("All stat types processed and uploaded to ADLS.")