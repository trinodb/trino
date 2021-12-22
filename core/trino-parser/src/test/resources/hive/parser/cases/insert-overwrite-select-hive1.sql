insert overwrite table tmp.rec_like_event_orc_1
partition(day,hour)
select
uid,
day,
hour
from tmp.rec_like_event_orc
where day='2019-08-03'
and hour='00'
limit 10000