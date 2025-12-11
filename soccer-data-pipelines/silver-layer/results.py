import dlt
import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException
from pyspark.sql import Window, SparkSession

spark = SparkSession.builder.getOrCreate()

# ============================================================
# Configuration
# ============================================================
BRONZE_STANDARD = "soccer_data.extract.team_match_results"
BRONZE_ADV = "soccer_data.extract.team_match_results_advanced"
MAPPING_TABLE = "soccer_data.processed.team_name_mappings"
FINAL_TABLE = "results"
# TEST_SEASON = "1819" # Keep this commented out for production

# ============================================================
# Helper: sanitize column names
# ============================================================
def sanitize_columns(df):
    """Sanitizes all column names in a DataFrame."""
    for col_name in df.columns:
        safe = (
            col_name.lower()
            .replace(" ", "_")
            .replace("(", "")
            .replace(")", "")
            .replace("/", "_")
            .replace("-", "_")
        )
        if safe != col_name:
            df = df.withColumnRenamed(col_name, safe)
    return df

# ============================================================
# 1) STANDARD: read bronze stream, sanitize, add IDs
# ============================================================
@dlt.view(
    name="bronze_standard_cleaned",
    comment="Cleaned streaming view of standard match results (sanitized names + ids)."
)
def bronze_standard_cleaned():
    df = spark.readStream.table(BRONZE_STANDARD)
    df = sanitize_columns(df)

    # Recover missing data from _rescued_data if available
    # Example rescued data: {"season":2526,"GF":1,"GA":1,"_file_path":"..."}
    df = df.withColumn("season", F.coalesce(F.col("season"), F.get_json_object(F.col("_rescued_data"), "$.season")))
    df = df.withColumn("gf", F.coalesce(F.col("gf"), F.get_json_object(F.col("_rescued_data"), "$.GF")))
    df = df.withColumn("ga", F.coalesce(F.col("ga"), F.get_json_object(F.col("_rescued_data"), "$.GA")))

    # # limit to test season
    # df = df.filter(F.col("season") == TEST_SEASON)

    # add key ids used downstream
    df = (
        df
        .withColumn("result_id", F.regexp_extract(F.col("match_report"), r"/en/matches/([^/]+)/", 1))
        .withColumn("team_id", F.concat(F.col("team"), F.lit("-"), F.col("season")))
        .withColumn("result_team_id", F.concat(F.col("result_id"), F.lit("-"), F.col("team"), F.lit("-"), F.col("season")))
    )

    # inline league inference to keep fewer views
    df = df.withColumn(
        "league",
        F.coalesce(
            F.col("league"),
            F.when(F.col("match_report").contains("Bundesliga"), "GER-Bundesliga")
             .when(F.col("match_report").contains("Premier-League"), "ENG-Premier League")
             .when(F.col("match_report").contains("Ligue-1"), "FRA-Ligue 1")
             .when(F.col("match_report").contains("Serie-A"), "ITA-Serie A")
             .when(F.col("match_report").contains("La-Liga"), "ESP-La Liga")
        )
    )

    return df

# ============================================================
# 2) STANDARD: deduplicate streaming results (SCD1)
# ============================================================
@dlt.view(
    name="results_standard_filtered",
    comment="Standard results stream filtered to only valid records (non-null league, season, IDs)."
)
def results_standard_filtered():
    return (
        dlt.read_stream("bronze_standard_cleaned")
        .filter(F.col("league").isNotNull())
        .filter(F.col("season").isNotNull())
        .filter(F.col("result_team_id").isNotNull())
    )

dlt.create_streaming_table(
    name="results_standard",
    comment="Deduplicated standard results (streaming SCD1, valid league only)."
)

dlt.apply_changes(
    target="results_standard",
    source="results_standard_filtered",
    keys=["result_team_id"],
    sequence_by=F.col("ingestion_date"),
    stored_as_scd_type="1"
)

# ============================================================
# 3) ADVANCED: read bronze advanced stream and unify home/away
# ============================================================
@dlt.view(
    name="adv_bronze_with_ids_unified",
    comment="Advanced bronze unified stream: expands home/away into one side-per-row with consistent columns."
)
def adv_bronze_with_ids_unified():
    adv = sanitize_columns(spark.readStream.table(BRONZE_ADV))

    # # limit to test season
    # adv = adv.filter(F.col("season") == TEST_SEASON)

    # home side
    home = (
        adv
        .withColumn("game_team_id", F.concat(F.col("game_id"), F.lit("-"), F.col("home_team_id")))
        .withColumn("team_id_understat", F.col("home_team_id"))
        .withColumn("team_understat", F.col("home_team"))
        .withColumn("team_code_understat", F.col("home_team_code"))
        .withColumn("opponent_understat", F.col("away_team"))
        .withColumn("points", F.col("home_points"))
        .withColumn("ppda_raw", F.col("home_ppda"))
        .withColumn("deep_completions", F.col("home_deep_completions"))
        .withColumn("venue", F.lit("Home"))
        .select(
            "league", "season", "date", "game_id", "season_id",
            "game_team_id", "team_id_understat", "team_understat", "team_code_understat",
            "opponent_understat", "points", "ppda_raw", "deep_completions", "venue", "ingestion_date"
        )
    )

    # away side
    away = (
        adv
        .withColumn("game_team_id", F.concat(F.col("game_id"), F.lit("-"), F.col("away_team_id")))
        .withColumn("team_id_understat", F.col("away_team_id"))
        .withColumn("team_understat", F.col("away_team"))
        .withColumn("team_code_understat", F.col("away_team_code"))
        .withColumn("opponent_understat", F.col("home_team"))
        .withColumn("points", F.col("away_points"))
        .withColumn("ppda_raw", F.col("away_ppda"))
        .withColumn("deep_completions", F.col("away_deep_completions"))
        .withColumn("venue", F.lit("Away"))
        .select(
            "league", "season", "date", "game_id", "season_id",
            "game_team_id", "team_id_understat", "team_understat", "team_code_understat",
            "opponent_understat", "points", "ppda_raw", "deep_completions", "venue", "ingestion_date"
        )
    )

    unioned = home.unionByName(away, allowMissingColumns=True)

    return unioned.withColumn("ingestion_date", F.col("ingestion_date").cast("timestamp"))

# ============================================================
# 4) ADVANCED: deduplicate unified advanced data (SCD1)
# ============================================================
dlt.create_streaming_table(
    name="adv_results_dedup",
    comment="Deduplicated advanced side rows (one row per game_team_id, streaming SCD1)."
)

dlt.apply_changes(
    target="adv_results_dedup",
    source="adv_bronze_with_ids_unified",
    keys=["game_team_id"],
    sequence_by=F.col("ingestion_date"),
    stored_as_scd_type="1"
)

# ============================================================
# 5) ADVANCED: median ppda per season (batch)
# ============================================================
@dlt.table(
    name="median_ppda_stats",
    comment="Median PPDA per season (batch) used to fill null ppda in advanced rows.",
    table_properties={"quality": "silver"}
)
def median_ppda_stats():
    # Reads from the streaming table 'adv_results_dedup' as a batch
    df = dlt.read("adv_results_dedup") 
    ppdas = df.select(
        F.col("season"),
        F.col("ppda_raw").cast("float").alias("ppda_value")
    ).filter(F.col("ppda_value").isNotNull())

    return ppdas.groupBy("season").agg(
        F.expr("percentile_approx(ppda_value, 0.5)").alias("median_ppda")
    )

# ============================================================
# 6) ADVANCED: map understat names to master names (batch)
# ============================================================
@dlt.table(
    name="results_advanced",
    comment="Final advanced match stats (batch) — mapped to master club names and deduped for safe join.",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("league_not_null", "league IS NOT NULL")
@dlt.expect_or_drop("season_not_null", "season_understat IS NOT NULL")
@dlt.expect_or_drop("game_id_not_null", "game_id IS NOT NULL")
@dlt.expect_or_drop("date_not_null", "date IS NOT NULL")
def results_advanced():
    # Reads from the streaming table 'adv_results_dedup' as a batch
    adv = (dlt.read("adv_results_dedup") 
        #    .filter(F.col("season") == TEST_SEASON)
    )
    median = dlt.read("median_ppda_stats")
    mapping = spark.read.table(MAPPING_TABLE)

    adv = (
        adv.join(median, "season", "left")
           .withColumn("ppda", F.coalesce(F.col("ppda_raw"), F.col("median_ppda")).cast("float"))
    )

    # attach master names for team and opponent
    joined_team = adv.join(mapping.alias("mteam"), adv["team_understat"] == F.col("mteam.understat_club_name"), "left") \
                     .withColumn("master_team_name", F.col("mteam.master_club_name")).drop("mteam.*")
    joined_both = joined_team.join(mapping.alias("mopp"), joined_team["opponent_understat"] == F.col("mopp.understat_club_name"), "left") \
                             .withColumn("master_opp_name", F.col("mopp.master_club_name")).drop("mopp.*")

    return joined_both.select(
        "game_team_id",
        F.col("game_id").cast("integer"),
        F.col("season_id").cast("integer"),
        "team_id_understat",
        "league",
        F.col("season").alias("season_understat"),
        F.to_date(F.col("date")).alias("date"),
        "master_team_name",
        "master_opp_name",
        "venue",
        "points",
        "ppda",
        "deep_completions",
        "team_understat",
        "opponent_understat",
        "team_code_understat",
        "ingestion_date",
        F.current_timestamp().alias("update_date")
    )

# ============================================================
# 7) FINAL: stream-static join → Liquid Clustered Silver table
# ============================================================
@dlt.table(
    name=FINAL_TABLE,
    comment="Final Silver table joining standard match results (batch) with advanced stats (batch).",
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.targetFileSize": "128MB",
        # "delta.feature.liquidClustering": "enabled"
        # "delta.liquidClustering.columns": "team_id,team,league,season"
        # "delta.clusteredBy": "team_id,team,league,season"
    }
)
@dlt.expect_or_fail("result_team_id_not_null", "result_team_id IS NOT NULL")
@dlt.expect_or_fail("result_id_not_null", "result_id IS NOT NULL")
@dlt.expect_or_fail("team_id_not_null", "team_id IS NOT NULL")
@dlt.expect_or_fail("league_not_null", "league IS NOT NULL")
@dlt.expect_or_fail("season_not_null", "season IS NOT NULL")
@dlt.expect_or_fail("date_not_null", "date IS NOT NULL")
@dlt.expect_or_fail("team_not_null", "team IS NOT NULL")
def final_results():
    """
    Creates the final silver-layer results table by joining standard and advanced match data.
    
    Uses COALESCE fallbacks for advanced-derived columns (team_understat, team_code_understat, 
    opponent_understat) to handle cases where advanced data lags behind standard results.
    Fallback values prevent NULL grouping in downstream gold-layer aggregations.
    """
    # Reading 'results_standard' as a BATCH table
    std = (
        dlt.read("results_standard")
        .withColumn("ingestion_date", F.col("ingestion_date").cast("timestamp"))
        # .filter(F.col("season") == TEST_SEASON)
    )

    # Reading 'results_advanced' as a BATCH table
    adv = (
        dlt.read("results_advanced")
        # .filter(F.col("season_understat") == TEST_SEASON)
        .withColumnRenamed("master_team_name", "adv_master_team_name")
        .withColumnRenamed("master_opp_name", "adv_master_opp_name")
        .withColumnRenamed("season_understat", "adv_season")
        .withColumnRenamed("ppda", "adv_ppda")
        .withColumnRenamed("deep_completions", "adv_deep_completions")
        .withColumnRenamed("team_understat", "adv_team_understat")
        .withColumnRenamed("opponent_understat", "adv_opponent_understat")
        .withColumnRenamed("team_code_understat", "adv_team_code_understat")
        .withColumnRenamed("league", "adv_league")
        .withColumnRenamed("date", "adv_date")
        .withColumnRenamed("venue", "adv_venue")
        .withColumnRenamed("ingestion_date", "adv_ingestion_date") # <-- FIX 1
    )

    joined = std.join(
        adv,
        on=[
            std["team"] == adv["adv_master_team_name"],
            std["opponent"] == adv["adv_master_opp_name"],
            std["season"] == adv["adv_season"]
        ],
        how="left"
    )

    with_calcs = (
        joined
        .withColumn("goals", F.col("gf").cast("integer"))
        .withColumn("goals_allowed", F.col("ga").cast("integer"))
        .withColumn("expected_goals", F.col("xg").cast("float"))
        .withColumn("expected_goals_allowed", F.col("xga").cast("float"))
        .withColumn("possession", F.col("poss").cast("integer"))
    )

    points_expr = (
        F.when(F.col("goals") == F.col("goals_allowed"), F.lit(1))
         .when(F.col("goals") > F.col("goals_allowed"), F.lit(3))
         .otherwise(F.lit(0))
    )
    result_expr = (
        F.when(points_expr == 1, F.lit("D"))
         .when(points_expr == 3, F.lit("W"))
         .otherwise(F.lit("L"))
    )
    expected_goal_difference_expr = F.col("expected_goals") - F.col("expected_goals_allowed")
    expected_result_expr = (
        F.when(expected_goal_difference_expr > 0.5, F.lit("W"))
         .when(expected_goal_difference_expr < -0.5, F.lit("L"))
         .otherwise(F.lit("D"))
    )
    expected_result_points_expr = (
        F.when(expected_result_expr == "W", F.lit(3))
         .when(expected_result_expr == "D", F.lit(1))
         .otherwise(F.lit(0))
    )
    goal_difference_expr = F.col("goals") - F.col("goals_allowed")

    final = (
        with_calcs
        .withColumn("points", points_expr)
        .withColumn("result", result_expr)
        .withColumn("expected_goal_difference", expected_goal_difference_expr)
        .withColumn("expected_result", expected_result_expr)
        .withColumn("goal_difference", goal_difference_expr)
        .withColumn("expected_result_points", expected_result_points_expr)
        .withColumn("update_date", F.current_timestamp())
    )

    final_dedup = final.dropDuplicates(["result_team_id"])

    return final_dedup.select(
        F.col("result_team_id"),
        F.col("game_team_id").alias("result_team_id_understat"),
        F.col("result_id"),
        F.col("game_id").alias("result_id_understat"),
        F.col("team_id"),
        F.col("team_id_understat"),
        F.col("league"),
        F.col("season"),
        F.to_date(F.col("date")).alias("date"),
        F.col("team"),
        F.coalesce(F.col("adv_team_understat"), F.col("team")).alias("team_understat"),
        # Fallback to first 3 uppercase chars when advanced data is missing
        F.coalesce(F.col("adv_team_code_understat"), F.upper(F.substring(F.col("team"), 1, 3))).alias("team_code_understat"),
        F.col("opponent"),
        F.coalesce(F.col("adv_opponent_understat"), F.col("opponent")).alias("opponent_understat"),
        F.col("formation"),
        F.col("opp_formation").alias("opponent_formation"),
        F.col("venue"),
        F.col("result"),
        F.col("expected_result"),
        F.col("points"),
        F.col("expected_result_points"),
        F.col("goals"),
        F.col("goals_allowed"),
        F.col("goal_difference"),
        F.col("expected_goals"),
        F.col("expected_goals_allowed"),
        F.col("expected_goal_difference"),
        F.col("possession"),
        F.col("adv_ppda").alias("ppda"),
        F.col("adv_deep_completions").alias("deep_completions"),
        F.col("update_date")
    )