create external table tmp.rec_like_event_orc_1
(
 uid int,
 sex boolean,
 age string,
 video_id bigint
)
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
  '/tmp.db/rec_like_event_orc_1'