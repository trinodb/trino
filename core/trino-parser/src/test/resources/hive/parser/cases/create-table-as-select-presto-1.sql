create table tmp.rec_like_event_orc_1
with
(
partitioned_by=ARRAY['day','hour'],
format = 'ORC'
)
as
select * from tmp.rec_like_event_orc
where day='2019-08-03'
and hour='00'
limit 10000