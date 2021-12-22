create table tmp.rec_like_event_orc_1
PARTITIONED BY (
  `day` string,
  `hour` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
as
select * from tmp.rec_like_event_orc
where day='2019-08-03'
and hour='00'
limit 10000