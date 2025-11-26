import subprocess
import sys
import os

# ==============================================================================
# SCRIPT CONFIGURATION
# ==============================================================================

# Get the directory where this script (run_ingestion_scripts.py) is located
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Define scripts relative to this folder
# This works on Windows, Mac, and Linux automatically
scripts_to_run = [
    os.path.join(BASE_DIR, "ingest_match_team_data.py"),
    os.path.join(BASE_DIR, "ingest_match_individual_data.py"),
    os.path.join(BASE_DIR, "ingest_season_team_data.py"),
]

# ==============================================================================
# SCRIPT EXECUTION LOGIC
# ==============================================================================

def run_all_scripts():
    """
    Executes a list of Python scripts sequentially.
    Stops execution if any script fails.
    """
    print("🚀 Starting the data pipeline execution...")
    
    python_executable = sys.executable

    for script_path in scripts_to_run:
        print("-" * 60)
        print(f"▶️  Running script: {os.path.basename(script_path)}") # Print just the filename
        print("-" * 60)
        
        try:
            result = subprocess.run(
                [python_executable, script_path], 
                check=True, 
                capture_output=True, 
                text=True
            )
            
            print("✅ Script finished successfully.")
            if result.stdout:
                print("\n--- Script Output ---\n" + result.stdout)

        except FileNotFoundError:
            print(f"❌ ERROR: The file was not found: {script_path}")
            break 
        except subprocess.CalledProcessError as e:
            print(f"❌ ERROR: An error occurred in script: {script_path}")
            print(f"Return Code: {e.returncode}")
            if e.stderr:
                print("\n--- Error Details ---\n" + e.stderr)
            break
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            break

if __name__ == "__main__":
    run_all_scripts()
    print("\n🎉 Pipeline execution finished.")