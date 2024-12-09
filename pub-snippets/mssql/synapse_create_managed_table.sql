
%%sql
CREATE TABLE IF NOT EXISTS db.bronze_tblActivity
USING PARQUET
PARTITIONED BY (zPartKey)
LOCATION '/db/bronze/tblActivity/'
