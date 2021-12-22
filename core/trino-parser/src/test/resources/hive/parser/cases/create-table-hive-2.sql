CREATE TABLE `tmp.rec_like_event_orc`(
  `uid` bigint)
PARTITIONED BY ( 
  `day` string, 
  `hour` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  '/tmp.db/rec_like_event_orc'
TBLPROPERTIES (
  'last_modified_by'='tangyun', 
  'last_modified_time'='1544684612', 
  'orc.compress'='SNAPPY', 
  'transient_lastDdlTime'='1554739798')
