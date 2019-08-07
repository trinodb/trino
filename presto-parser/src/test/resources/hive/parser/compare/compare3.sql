SELECT start_time,
       t1.uri,
       t1.countryCode,
       t1.platform,
       t1.resSum AS resSum_now,
       t1.reqSum AS reqSum_now,
       t2.resSum AS resSum_his,
       t2.reqSum AS reqSum_his,
       avg_ratio_success_now,
       stddev_ratio_success_now,
       avg_ratio_success_his,
       stddev_ratio_success_his,
       t1.appid
FROM vlog.uri_req_success_alarm_realtime t1
JOIN mediate_tb.like_uri_req_success_alarm_base t2 ON substr(t1.start_time,12,2)=substr(t2.his_end_day_hour,12,2)
AND t1.uri = t2.uri
AND t1.countryCode = t2.countryCode
AND t1.platform = t2.platform
AND t1.appid = t2.appid;
SELECT start_time,
       t1.uri,
       t1.countryCode,
       t1.platform,
       t1.resSum AS resSum_now,
       t1.reqSum AS reqSum_now,
       t2.resSum AS resSum_his,
       t2.reqSum AS reqSum_his,
       avg_ratio_success_now,
       stddev_ratio_success_now,
       avg_ratio_success_his,
       stddev_ratio_success_his,
       t1.appid
FROM vlog.uri_req_success_alarm_realtime t1
JOIN mediate_tb.like_uri_req_success_alarm_base t2 ON substr(t1.start_time,12,2)=substr(t2.his_end_day_hour,12,2)
AND t1.uri = t2.uri
AND t1.countryCode = t2.countryCode
AND t1.platform = t2.platform
AND t1.appid = t2.appid