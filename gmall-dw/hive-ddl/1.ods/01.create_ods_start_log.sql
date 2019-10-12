drop table if exists ods_start_log;
CREATE EXTERNAL TABLE ods_start_log (`line` string)
PARTITIONED BY (`dt` string)
LOCATION '/warehouse/gmall/ods/ods_start_log';