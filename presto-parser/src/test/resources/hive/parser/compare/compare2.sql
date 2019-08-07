
SELECT '2019-07-30',
       '2019-07-30 07:30:00',
       uri,
       upper(trim(countryCode)) AS countrycode,
       platform,
       sum(rescount) AS ressum,
       sum(reqcount) AS reqsum,
       sum(rescount)/sum(reqcount) AS avg_ratio_success,
       IF (sum(rescount) >0,
           SUM (rescount * avg_time) / sum(rescount),
               0) AS avg_time
FROM
  (SELECT protodata.uri,
          countryCode,
          platform,
          protodata.counttotal AS reqcount,
          (protodata.countdista +protodata.countdistb + protodata.countdistc + protodata.countdistd) AS rescount,
          (protodata.countdista * protodata.avgtimedista + protodata.countdistb * protodata.avgtimedistb + protodata.countdistc * protodata.avgtimedistc +protodata.countdistd * protodata.avgtimedistd) / (protodata.countdista +protodata.countdistb + protodata.countdistc + protodata.countdistd) AS avg_time,
          appid
   FROM
     (SELECT DAY,
             substr(rtime,12,2) AS hour,
             unix_timestamp(rtime) AS reportTime,
             platform,
             countrycode AS countryCode,
             t1.protodata AS protodata,
             appid
      FROM
        (SELECT *
         FROM instatus.instatus_proto_stat_report_v2
         WHERE DAY='2019-07-30'
           AND substr(rtime,12,2)='07' ) t_tmp LATERAL VIEW explode(protodata) t1 AS protodata) tt
   WHERE reportTime>=unix_timestamp('2019-07-30 07:30:00')
     AND reportTime<(unix_timestamp('2019-07-30 07:30:00') + 900) ) tttt
WHERE rescount<=reqcount
  AND rescount<256
  AND reqcount<256
  AND countryCode IS NOT NULL
  AND platform IS NOT NULL
  AND countryCode!=''
GROUP BY uri,
         upper(trim(countryCode)),
         platform,
         appid;


SELECT '2019-07-30',
       '2019-07-30 07:30:00',
       uri,
       upper(trim(countryCode)) AS countrycode,
       platform,
       sum(rescount) AS ressum,
       sum(reqcount) AS reqsum,
       sum(rescount)/sum(reqcount) AS avg_ratio_success,
       IF (sum(rescount) >0,
           SUM (rescount * avg_time) / sum(rescount),
               0) AS avg_time
FROM
  (SELECT protodata.uri,
          countryCode,
          platform,
          protodata.counttotal AS reqcount,
          (protodata.countdista +protodata.countdistb + protodata.countdistc + protodata.countdistd) AS rescount,
          (protodata.countdista * protodata.avgtimedista + protodata.countdistb * protodata.avgtimedistb + protodata.countdistc * protodata.avgtimedistc +protodata.countdistd * protodata.avgtimedistd) / (protodata.countdista +protodata.countdistb + protodata.countdistc + protodata.countdistd) AS avg_time,
          appid
   FROM
     (SELECT DAY,
             substr(rtime,12,2) AS hour,
             unix_timestamp(rtime) AS reportTime,
             platform,
             countrycode AS countryCode,
             t1.protodata AS protodata,
             appid
      FROM
        (SELECT *
         FROM instatus.instatus_proto_stat_report_v2
         WHERE DAY='2019-07-30'
           AND substr(rtime,12,2)='07' ) t_tmp
         CROSS JOIN UNNEST(protodata) AS t1  (protodata)
        ) tt
   WHERE reportTime>=unix_timestamp('2019-07-30 07:30:00')
     AND reportTime<(unix_timestamp('2019-07-30 07:30:00') + 900) ) tttt
WHERE rescount<=reqcount
  AND rescount<256
  AND reqcount<256
  AND countryCode IS NOT NULL
  AND platform IS NOT NULL
  AND countryCode!=''
GROUP BY uri,
         upper(trim(countryCode)),
         platform,
         appid