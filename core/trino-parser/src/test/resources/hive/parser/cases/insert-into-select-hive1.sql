insert into tmp.rec_like_event_orc_1
(((select uid from tmp.rec_like_event_orc
where day='2019-08-03'
and hour='00'
limit 10000)))