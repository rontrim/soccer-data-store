import dlt
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Configuration
SOURCE_TABLE = "soccer_data.processed.results"

@dlt.table(
    name="rolling_stats",
    comment="Gold layer: 8-game rolling averages for trendline analysis.",
    table_properties={"quality": "gold"}
)
def rolling_stats():
    df = dlt.read(SOURCE_TABLE)

    # Define the window: Partition by Team-Season (team_id), Order by Date, Last 8 rows
    # team_id is unique per team-season (e.g. Arsenal-2023), ensuring we don't cross seasons.
    w = Window.partitionBy("team_id").orderBy("date").rowsBetween(-7, 0)
    
    # Window for Match Number (1 to N) per season
    w_rank = Window.partitionBy("team_id").orderBy("date")

    return (
        df
        # Calculate Rolling Averages
        .withColumn("PPG", F.avg("points").over(w))
        .withColumn("xGD_PPG", F.avg("expected_result_points").over(w))
        .withColumn("G_PG", F.avg("goals").over(w))
        .withColumn("GA_PG", F.avg("goals_allowed").over(w))
        .withColumn("GD_PG", F.avg("goal_difference").over(w))
        .withColumn("xG_PG", F.avg("expected_goals").over(w))
        .withColumn("xGA_PG", F.avg("expected_goals_allowed").over(w))
        .withColumn("xGD_PG", F.avg("expected_goal_difference").over(w))
        
        # Add Match Number for x-axis in historic comparisons
        .withColumn("Match_No", F.row_number().over(w_rank))
        
        # Clean up League Name (remove prefix like 'ENG-')
        .withColumn("League", F.regexp_replace(F.col("league"), r"^.*-", ""))
        
        # Format Season (e.g., '2526' -> '2025/26')
        .withColumn(
            "Season",
            F.concat(
                F.lit("20"), 
                F.substring(F.col("season"), 1, 2), 
                F.lit("/"), 
                F.substring(F.col("season"), 3, 2)
            )
        )
        
        .select(
            F.col("team").alias("Team"),
            "Season",
            "League",
            F.col("date").alias("Date"),
            "Match_No",
            "PPG",
            "xGD_PPG",
            "G_PG",
            "GA_PG",
            "GD_PG",
            "xG_PG",
            "xGA_PG",
            "xGD_PG"
        )
    )
