with t as
(select * from algo.rec_like_event_orc where day='2019-08-08' and hour='00' limit 100)
SELECT * from t limit 1
