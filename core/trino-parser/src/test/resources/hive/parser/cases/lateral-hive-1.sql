SELECT t.a1
FROM bigolive.live_sdk_video_stats_event_simplification
lateral view explode(k11)  t AS a1,a2,a3
WHERE day ='2019-08-01' and t.a1=1818776012
limit 100