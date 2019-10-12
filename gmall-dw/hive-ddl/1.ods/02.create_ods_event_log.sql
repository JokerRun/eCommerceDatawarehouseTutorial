drop table if exists ods_event_log;
CREATE EXTERNAL TABLE ods_event_log(`line` string)
PARTITIONED BY (`dt` string)
LOCATION '/warehouse/gmall/ods/ods_event_log';