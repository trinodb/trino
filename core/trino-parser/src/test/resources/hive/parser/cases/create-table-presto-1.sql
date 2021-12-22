create table tmp.rec_like_event_orc_1
(
 uid int ,
 sex boolean,
 age string ,
 video_id bigint,
 day string,
 hour string
)
with
(
partitioned_by=ARRAY['day','hour'],
format = 'ORC',
external_location='/tmp.db/rec_like_event_orc_1'
)