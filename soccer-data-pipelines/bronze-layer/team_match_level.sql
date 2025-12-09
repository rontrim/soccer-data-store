-- ======================================================
-- Bronze Layer: Match Level Team Data
-- ======================================================

-- 1. Advanced Results (Understat)
CREATE OR REFRESH STREAMING LIVE TABLE team_match_results_advanced
COMMENT "Raw advanced team match results data from Understat. Includes bespoke metrics such as PPDA and Deep Completions."
TBLPROPERTIES (
  'quality' = 'bronze',
  delta.dataSkippingStatsColumns = 'ingestion_date,season,league'
)
CLUSTER BY (ingestion_date, season, league)
AS
SELECT *
FROM (
  SELECT 
    *,
    _metadata.file_path AS source_file,
    _metadata.file_modification_time AS file_modification_time
  FROM cloud_files(
      '${soccer.datastore.extract}/match_team_stats_understat',
      'parquet',
      map(
        'cloudFiles.schemaLocation','${soccer.datastore.extract}/schemas/match_team_stats_understat',
        'cloudFiles.schemaEvolutionMode', 'addNewColumns',
        'cloudFiles.includeExistingFiles','true'
      )
    )
);

-- 2. Standard Results (FBref)
CREATE OR REFRESH STREAMING LIVE TABLE team_match_results
COMMENT "Raw standard team match results data from FBref. Exclusively includes headline match stats such as W/D/L,  attendance, formations, and referee."
TBLPROPERTIES (
  delta.columnMapping.mode = 'name',
  delta.minReaderVersion   = '2',
  delta.minWriterVersion   = '5',
  delta.dataSkippingStatsColumns = 'ingestion_date,team,season,league',
  'quality' = 'bronze'
)
CLUSTER BY (ingestion_date, team, season, league)
AS
SELECT *
FROM (
  SELECT 
    *,
    _metadata.file_path AS source_file,
    _metadata.file_modification_time AS file_modification_time
FROM cloud_files(
    '${soccer.datastore.extract}/match_team_schedule',
    'parquet',
    map(
      'cloudFiles.schemaLocation','${soccer.datastore.extract}/schemas/match_team_schedule',
      'cloudFiles.schemaEvolutionMode', 'addNewColumns',
      'cloudFiles.includeExistingFiles','true'
    )
  )
);

-- 3. Goal and Shot Creating Actions
CREATE OR REFRESH STREAMING LIVE TABLE team_match_gca_sca
COMMENT "Raw team goal and shot creating actions data from Fbref."
TBLPROPERTIES (
  delta.columnMapping.mode = 'name',
  delta.minReaderVersion   = '2',
  delta.minWriterVersion   = '5',
  delta.dataSkippingStatsColumns = 'ingestion_date,team,season,league',  
  'quality' = 'bronze'
)
CLUSTER BY (ingestion_date, team, season, league)
AS
SELECT *
FROM (
  SELECT 
    *,
    _metadata.file_path AS source_file,
    _metadata.file_modification_time AS file_modification_time
FROM cloud_files(
    '${soccer.datastore.extract}/match_team_goal_shot_creation',
    'parquet',
    map(
      'cloudFiles.schemaLocation','${soccer.datastore.extract}/schemas/match_team_goal_shot_creation',
      'cloudFiles.schemaEvolutionMode', 'addNewColumns',
      'cloudFiles.includeExistingFiles','true'
    )
  )
);

-- 4. Shooting
CREATE OR REFRESH STREAMING LIVE TABLE team_match_shooting
COMMENT "Raw team match shooting data from Fbref."
TBLPROPERTIES (
  'quality' = 'bronze',
  delta.dataSkippingStatsColumns = 'ingestion_date,team,season,league'
)
CLUSTER BY (ingestion_date, team, season, league)
AS
SELECT *
FROM (
  SELECT 
    *,
    _metadata.file_path AS source_file,
    _metadata.file_modification_time AS file_modification_time
FROM cloud_files(
    '${soccer.datastore.extract}/match_team_shooting',
    'parquet',
    map(
      'cloudFiles.schemaLocation','${soccer.datastore.extract}/schemas/match_team_shooting',
      'cloudFiles.schemaEvolutionMode', 'addNewColumns',
      'cloudFiles.includeExistingFiles','true'
    )
  )
);

-- 5. Miscellaneous
CREATE OR REFRESH STREAMING LIVE TABLE team_match_miscellaneous
COMMENT "Raw team match miscellaneous data from Fbref."
TBLPROPERTIES (
  delta.columnMapping.mode = 'name',
  delta.minReaderVersion   = '2',
  delta.minWriterVersion   = '5',
  delta.dataSkippingStatsColumns = 'ingestion_date,team,season,league',  
  'quality' = 'bronze'
)
CLUSTER BY (ingestion_date, team, season, league)
AS
SELECT *
FROM (
  SELECT 
    *,
    _metadata.file_path AS source_file,
    _metadata.file_modification_time AS file_modification_time
FROM cloud_files(
    '${soccer.datastore.extract}/match_team_misc',
    'parquet',
    map(
      'cloudFiles.schemaLocation','${soccer.datastore.extract}/schemas/match_team_misc',
      'cloudFiles.schemaEvolutionMode', 'addNewColumns',
      'cloudFiles.includeExistingFiles','true'
    )
  )
);

-- 6. Possession
CREATE OR REFRESH STREAMING LIVE TABLE team_match_possession
COMMENT "Raw team match possession data from Fbref."
TBLPROPERTIES (
  delta.columnMapping.mode = 'name',
  delta.minReaderVersion   = '2',
  delta.minWriterVersion   = '5',
  delta.dataSkippingStatsColumns = 'ingestion_date,team,season,league',  
  'quality' = 'bronze'
)
CLUSTER BY (ingestion_date, team, season, league)
AS
SELECT *
FROM (
  SELECT 
    *,
    _metadata.file_path AS source_file,
    _metadata.file_modification_time AS file_modification_time
FROM cloud_files(
    '${soccer.datastore.extract}/match_team_possession',
    'parquet',
    map(
      'cloudFiles.schemaLocation','${soccer.datastore.extract}/schemas/match_team_possession',
      'cloudFiles.schemaEvolutionMode', 'addNewColumns',
      'cloudFiles.includeExistingFiles','true'
    )
  )
);

-- 7. Defense
CREATE OR REFRESH STREAMING LIVE TABLE team_match_defense
COMMENT "Raw team match defense data from Fbref."
TBLPROPERTIES (
  delta.columnMapping.mode = 'name',
  delta.minReaderVersion   = '2',
  delta.minWriterVersion   = '5',
  delta.dataSkippingStatsColumns = 'ingestion_date,team,season,league',
  'quality' = 'bronze'
)
CLUSTER BY (ingestion_date, team, season, league)
AS
SELECT *
FROM (
  SELECT 
    *,
    _metadata.file_path AS source_file,
    _metadata.file_modification_time AS file_modification_time
FROM cloud_files(
    '${soccer.datastore.extract}/match_team_defense',
    'parquet',
    map(
      'cloudFiles.schemaLocation','${soccer.datastore.extract}/schemas/match_team_defense',
      'cloudFiles.schemaEvolutionMode', 'addNewColumns',
      'cloudFiles.includeExistingFiles','true'
    )
  )
);

-- 8. Passing Types
CREATE OR REFRESH STREAMING LIVE TABLE team_match_passing_types
COMMENT "Raw team match passing type data from Fbref."
TBLPROPERTIES (
  delta.columnMapping.mode = 'name',
  delta.minReaderVersion   = '2',
  delta.minWriterVersion   = '5',
  delta.dataSkippingStatsColumns = 'ingestion_date,team,season,league',
  'quality' = 'bronze'
)
CLUSTER BY (ingestion_date, team, season, league)
AS
SELECT *
FROM (
  SELECT 
    *,
    _metadata.file_path AS source_file,
    _metadata.file_modification_time AS file_modification_time
FROM cloud_files(
    '${soccer.datastore.extract}/match_team_passing_types',
    'parquet',
    map(
      'cloudFiles.schemaLocation','${soccer.datastore.extract}/schemas/match_team_passing_types',
      'cloudFiles.schemaEvolutionMode', 'addNewColumns',
      'cloudFiles.includeExistingFiles','true'
    )
  )
);

-- 9. Passing
CREATE OR REFRESH STREAMING LIVE TABLE team_match_passing
COMMENT "Raw team match passing data from Fbref."
TBLPROPERTIES (
  'quality' = 'bronze',
  delta.dataSkippingStatsColumns = 'ingestion_date,team,season,league'
)
CLUSTER BY (ingestion_date, team, season, league)
AS
SELECT *
FROM (
  SELECT 
    *,
    _metadata.file_path AS source_file,
    _metadata.file_modification_time AS file_modification_time
FROM cloud_files(
    '${soccer.datastore.extract}/match_team_passing',
    'parquet',
    map(
      'cloudFiles.schemaLocation','${soccer.datastore.extract}/schemas/match_team_passing',
      'cloudFiles.schemaEvolutionMode', 'addNewColumns',
      'cloudFiles.includeExistingFiles','true'
    )
  )
);

-- 10. Goalkeeping
CREATE OR REFRESH STREAMING LIVE TABLE team_match_keeping
COMMENT "Raw team match goalkeeping data from Fbref."
TBLPROPERTIES (
  delta.columnMapping.mode = 'name',
  delta.minReaderVersion   = '2',
  delta.minWriterVersion   = '5',
  delta.dataSkippingStatsColumns = 'ingestion_date,team,season,league',
  'quality' = 'bronze'
)
CLUSTER BY (ingestion_date, team, season, league)
AS
SELECT *
FROM (
  SELECT 
    *,
    _metadata.file_path AS source_file,
    _metadata.file_modification_time AS file_modification_time
FROM cloud_files(
    '${soccer.datastore.extract}/match_team_keeper',
    'parquet',
    map(
      'cloudFiles.schemaLocation','${soccer.datastore.extract}/schemas/match_team_keeper',
      'cloudFiles.schemaEvolutionMode', 'addNewColumns',
      'cloudFiles.includeExistingFiles','true'
    )
  )
);