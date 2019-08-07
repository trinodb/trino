SELECT '2019-07-29' AS sday,
       country,
       CASE
           WHEN event.event_info['height']>=1000 THEN '720P'
           WHEN event.event_info['height']>=864
                AND event.event_info['height']<1000 THEN '540p'
           WHEN event.event_info['height']>=801
                AND event.event_info['height']<864 THEN '480p'
           WHEN event.event_info['height']>=640
                AND event.event_info['height']<801 THEN '360p'
           WHEN event.event_info['height']>0
                AND event.event_info['height']<640 THEN 'less_360p'
           WHEN event.event_info['height']='-1' THEN 'no_play'
           ELSE 'other'
       END AS TYPE,---下发清晰度分辨率
 avg(event.event_info['bitrate']) AS avgbitrate,
 count(1) AS cnt,
 '2019-07-29' AS DAY
FROM vlog.like_user_event_hour_orc
WHERE event_id = '0103002'
  AND DAY ='2019-07-29'
  AND event.event_info['url'] IS NOT NULL
  AND event.event_info['reslevel'] IN (0, 2, 8, 16, 32, 64)
  AND country IN ('RU', 'US', 'IN', 'ID')
GROUP BY country,
         CASE
             WHEN event.event_info['height']>=1000 THEN '720P'
             WHEN event.event_info['height']>=864
                  AND event.event_info['height']<1000 THEN '540p'
             WHEN event.event_info['height']>=801
                  AND event.event_info['height']<864 THEN '480p'
             WHEN event.event_info['height']>=640
                  AND event.event_info['height']<801 THEN '360p'
             WHEN event.event_info['height']>0
                  AND event.event_info['height']<640 THEN 'less_360p'
             WHEN event.event_info['height']='-1' THEN 'no_play'
             ELSE 'other'
         END;

SELECT '2019-07-29' AS sday,
       country,
       CASE
           WHEN event.event_info['height']>=1000 THEN '720P'
           WHEN event.event_info['height']>=864
                AND event.event_info['height']<1000 THEN '540p'
           WHEN event.event_info['height']>=801
                AND event.event_info['height']<864 THEN '480p'
           WHEN event.event_info['height']>=640
                AND event.event_info['height']<801 THEN '360p'
           WHEN event.event_info['height']>0
                AND event.event_info['height']<640 THEN 'less_360p'
           WHEN event.event_info['height']='-1' THEN 'no_play'
           ELSE 'other'
       END AS TYPE,---下发清晰度分辨率
 avg(event.event_info['bitrate']) AS avgbitrate,
 count(1) AS cnt,
 '2019-07-29' AS DAY
FROM vlog.like_user_event_hour_orc
WHERE event_id = '0103002'
  AND DAY ='2019-07-29'
  AND event.event_info['url'] IS NOT NULL
  AND event.event_info['reslevel'] IN (0, 2, 8, 16, 32, 64)
  AND country IN ('RU', 'US', 'IN', 'ID')
GROUP BY country,
         CASE
             WHEN event.event_info['height']>=1000 THEN '720P'
             WHEN event.event_info['height']>=864
                  AND event.event_info['height']<1000 THEN '540p'
             WHEN event.event_info['height']>=801
                  AND event.event_info['height']<864 THEN '480p'
             WHEN event.event_info['height']>=640
                  AND event.event_info['height']<801 THEN '360p'
             WHEN event.event_info['height']>0
                  AND event.event_info['height']<640 THEN 'less_360p'
             WHEN event.event_info['height']='-1' THEN 'no_play'
             ELSE 'other'
         END