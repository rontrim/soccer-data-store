CREATE OR REFRESH STREAMING LIVE TABLE player_match_shots
COMMENT "Raw player shots data from FBref. Includes xG, distance, shot creating actions."
TBLPROPERTIES (
  delta.columnMapping.mode = 'name',
  delta.minReaderVersion   = '2',
  delta.minWriterVersion   = '5',
  'quality' = 'bronze'
)
CLUSTER BY (ingestion_date, team, season, league)
AS
SELECT *, 
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS file_modification_time
FROM cloud_files(
    '${soccer.datastore.extract}/match_individual_shots',
    'parquet',
    map(
      'cloudFiles.schemaLocation','${soccer.datastore.extract}/schemas/match_individual_shots',
      'cloudFiles.schemaEvolutionMode', 'addNewColumns',
      'cloudFiles.includeExistingFiles','true'
    )
  );