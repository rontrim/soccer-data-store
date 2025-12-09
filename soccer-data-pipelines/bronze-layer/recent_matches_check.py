import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import sys

# Initialize Spark Session and DBUtils for Python Script Task
spark = SparkSession.builder.getOrCreate()

try:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
except ImportError:
    # Fallback for local testing where DBUtils might not be available
    print("Warning: DBUtils not available.")
    dbutils = None

# 1. Define the path to your schedule data
# Note: Confirm this is the correct folder for your "schedule" data.
# Get the base path from the Spark configuration set in the job task
try:
    base_path = spark.conf.get("soccer.datastore.extract")
except:
    # Fallback to the direct path if config is missing (e.g. interactive run)
    base_path = "abfss://extract@soccerdatastore.dfs.core.windows.net/"

# Ensure no trailing slash to avoid double slashes
base_path = base_path.rstrip("/")

# 1. Define the path to your schedule data
data_path = f"{base_path}/match_team_schedule"
print(f"Checking for files in: {data_path}")

# 2. Find the most recent file in the directory
try:
  files = dbutils.fs.ls(data_path)
  latest_file = max(files, key=lambda f: f.modificationTime)
  latest_file_path = latest_file.path
  print(f"Found latest file: {latest_file_path}")
except Exception as e:
  print(f"Could not list files or folder is empty in {data_path}. Exiting.")
  if dbutils:
      dbutils.jobs.taskValues.set(key="row_count", value=0)
  sys.exit(0) # Exit successfully so the job continues (condition task will handle the 0)

# 3. Read the Parquet file and apply your validation logic
df = spark.read.parquet(latest_file_path)

# Apply your filters
# IMPORTANT: Casting the 'date' string to a date type for correct comparison
df_filtered = df.filter(
    (F.to_date(F.col("date")) >= F.date_sub(F.current_date(), 7)) &
    (F.col("match_report").isNotNull())
)

# 4. Count the resulting rows
row_count = df_filtered.count()
print(f"Validation query returned {row_count} row(s).")

# 5. Set the row count as a task value to be used by the next task
# This is how this notebook communicates the result to the workflow.
dbutils.jobs.taskValues.set(key="row_count", value=row_count)