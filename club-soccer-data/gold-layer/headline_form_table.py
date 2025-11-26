import dlt
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Configuration
SOURCE_TABLE = "soccer_data.processed.results"

# ============================================================
# Helper Functions
# ============================================================

def get_common_aggregations():
    """Defines the core aggregation logic shared by both tables."""
    # Names use underscores to avoid Delta invalid character errors
    return [
        F.sum("points").cast("integer").alias("Points"),
        F.count(F.when(F.col("result") == "W", 1)).cast("integer").alias("W"),
        F.count(F.when(F.col("result") == "D", 1)).cast("integer").alias("D"),
        F.count(F.when(F.col("result") == "L", 1)).cast("integer").alias("L"),
        F.count("result_id").cast("integer").alias("MP"),
        F.count(F.when(F.col("expected_result") == "W", 1)).cast("integer").alias("xGD_W"),
        F.count(F.when(F.col("expected_result") == "D", 1)).cast("integer").alias("xGD_D"),
        F.count(F.when(F.col("expected_result") == "L", 1)).cast("integer").alias("xGD_L"),
        F.sum("expected_result_points").cast("integer").alias("xGD_Points"),
        F.sum("goals").cast("integer").alias("G"),
        F.sum("goals_allowed").cast("integer").alias("GA"),
        F.sum("goal_difference").cast("integer").alias("GD"),
        F.sum("expected_goals").cast("float").alias("xG"),
        F.sum("expected_goals_allowed").cast("float").alias("xGA"),
        F.sum("expected_goal_difference").cast("float").alias("xGD"),
        F.round(F.avg("possession"), 1).alias("Possession")
    ]

def apply_transformations_and_ratios(df):
    """Applies formatting and per-game (PG) calculations, then renames keys."""
    return (
        df
        # --- Transformations (In Place) ---
        # Update 'season' column in place: '2526' -> '2025/26'
        .withColumn(
            "season",
            F.concat(
                F.lit("20"), 
                F.substring(F.col("season"), 1, 2), 
                F.lit("/"), 
                F.substring(F.col("season"), 3, 2)
            )
        )
        # Update 'league' column in place: remove prefix
        .withColumn(
            "league", 
            F.regexp_replace(F.col("league"), r"^.*-", "")
        )
        
        # --- Per Game Calculations (Rounded to 1 decimal) ---
        .withColumn("PPG", F.round(F.col("Points") / F.col("MP"), 1))
        .withColumn("xGD_PPG", F.round(F.col("xGD_Points") / F.col("MP"), 1))
        .withColumn("G_PG", F.round(F.col("G") / F.col("MP"), 1))
        .withColumn("GA_PG", F.round(F.col("GA") / F.col("MP"), 1))
        .withColumn("GD_PG", F.round(F.col("GD") / F.col("MP"), 1))
        .withColumn("xG_PG", F.round(F.col("xG") / F.col("MP"), 1))
        .withColumn("xGA_PG", F.round(F.col("xGA") / F.col("MP"), 1))
        .withColumn("xGD_PG", F.round(F.col("xGD") / F.col("MP"), 1))
        
        # --- Final Rename ---
        # Rename keys to Title Case (safe operation now that transforms are done)
        .withColumnRenamed("season", "Season")
        .withColumnRenamed("league", "League")
        .withColumnRenamed("team_id", "Team-Season")
        .withColumnRenamed("team", "Team")
    )

# ============================================================
# Table 1: Headline Table
# ============================================================

@dlt.table(
    name="headline_stats",
    comment="Gold layer aggregated stats per team, season, and league.",
    table_properties={"quality": "gold"}
)
@dlt.expect_or_fail("headline_points_not_null", "Points IS NOT NULL")
@dlt.expect_or_fail("headline_league_format", "League NOT LIKE '%-%'")
@dlt.expect_or_fail(
    "headline_mp_consistency", 
    "(MP = W + D + L) AND (MP = xGD_W + xGD_D + xGD_L)"
)
def headline_stats():
    df = dlt.read(SOURCE_TABLE)
    aggregated_df = df.groupBy("team_id", "team", "season", "league").agg(*get_common_aggregations())
    return apply_transformations_and_ratios(aggregated_df)

# ============================================================
# Table 2: Form Table (Last 8 Games)
# ============================================================

@dlt.table(
    name="form_stats",
    comment="Gold layer stats for the last 8 matches of the current season.",
    table_properties={"quality": "gold"}
)
@dlt.expect_or_drop("form_points_not_null", "Points IS NOT NULL")
@dlt.expect_or_fail("form_league_format", "League NOT LIKE '%-%'")
@dlt.expect_or_fail(
    "form_mp_check", 
    "MP <= 8 AND (MP = W + D + L) AND (MP = xGD_W + xGD_D + xGD_L)"
)
def form_stats():
    df = dlt.read(SOURCE_TABLE)
    
    max_season_df = df.select(F.max("season").alias("max_season"))
    current_season_df = df.join(max_season_df, F.col("season") == F.col("max_season"))
    
    window_spec = Window.partitionBy("team_id").orderBy(F.col("date").desc())
    
    last_8_df = (
        current_season_df
        .withColumn("match_rank", F.row_number().over(window_spec))
        .filter(F.col("match_rank") <= 8)
    )
    
    aggregated_df = last_8_df.groupBy("team_id", "team", "season", "league").agg(*get_common_aggregations())
    return apply_transformations_and_ratios(aggregated_df)