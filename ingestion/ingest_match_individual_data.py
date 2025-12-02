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

# --- Data Fetching Configuration ---
leagues = ['ENG-Premier League', 'ESP-La Liga', 'FRA-Ligue 1', 'GER-Bundesliga', 'ITA-Serie A']
seasons = ['2526']
ingestion_date_str = pd.Timestamp.now().strftime('%Y-%m-%d')

# --- Main Loop: Process each season individually ---
for season in seasons:
    print(f"--- Processing season: {season} ---")
    
    try:
        # 1. Fetch data for only the current season
        print(f"Fetching shot events for season {season}...")
        fbref_single_season = sd.FBref(leagues=leagues, seasons=season, no_cache=True)
        df_shots = fbref_single_season.read_shot_events()
        
        if df_shots.empty:
            print(f"No data found for season {season}. Skipping.")
            continue
            
        print(f"Successfully fetched {len(df_shots)} shot events for season {season}.")

        # 2. Flatten the DataFrame's index and columns for compatibility
        df_flat = df_shots.reset_index()
        new_columns = ['_'.join(col).strip('_') if isinstance(col, tuple) else col for col in df_flat.columns]
        df_flat.columns = new_columns
        df_final = df_flat.assign(ingestion_date=ingestion_date_str)

        # 3. Convert the 'date' column to a standard string format
        if 'date' in df_final.columns:
            print("  -> Converting 'date' column to string format...")
            df_final['date'] = df_final['date'].dt.strftime('%Y-%m-%dT%H:%M:%S')

        # ---
        # 💡 FIX APPLIED HERE
        # Clean the 'minute' column to ensure it's a consistent numeric type.
        # ---
        if 'minute' in df_final.columns:
            print("  -> Cleaning 'minute' column to ensure numeric type...")
            df_final['minute'] = pd.to_numeric(df_final['minute'], errors='coerce').fillna(0).astype(int)

        # 4. Save this season's data to a partitioned folder in ADLS
        folder_name = "match_individual_shots"
        file_name = f"shots_data_{ingestion_date_str}.parquet"
        adls_full_path = f"abfs://{container_name}/{folder_name}/season={season}/{file_name}"
        
        print(f"  -> Writing data to ADLS at: {adls_full_path}...")
        df_final.to_parquet(
            adls_full_path,
            storage_options={
                "account_name": storage_account_name,
                "credential": credential
            }
        )
        print(f"✅ Successfully saved data for season {season}.\n")

    except Exception:
        print(f"--- FAILED: Could not process season '{season}'. Error details below ---")
        traceback.print_exc()
        print("--------------------------------------------------\n")

print("All seasons processed.")