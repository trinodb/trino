insert into tmp.rec_like_event_orc_1
select
uid,
'2012-12-02' as day,
'00' as hour,
'123' as event_id
from tmp.rec_like_event_orc
where day='2019-08-03'
and hour='00'
limit 10000
