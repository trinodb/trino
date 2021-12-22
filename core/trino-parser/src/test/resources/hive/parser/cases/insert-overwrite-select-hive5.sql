insert overwrite table tmp.rec_like_event_orc_1
partition(day='2012-12-02',hour, event_id='1234')
select
uid,
hour
from tmp.rec_like_event_orc
where day='2019-08-03'
and hour='00'
limit 10000
