SELECT DOMAIN,
       coalesce(LAYER,'all') AS LAYER,
       coalesce(exp,'all') AS exp,
       coalesce(abflag,'all') AS abflag,
       '15' AS hour,
       coalesce(count(DISTINCT UID),0) AS dau,
       coalesce(count(DISTINCT if(LEVEL = 1,UID,NULL))/count(DISTINCT UID),0) AS low_rate,
       coalesce(count(DISTINCT if(LEVEL = 2,UID,NULL))/count(DISTINCT UID),0) AS middle_rate,
       coalesce(count(DISTINCT if(LEVEL = 3,UID,NULL))/count(DISTINCT UID),0) AS high_rate,
       coalesce(sum(all_timewatch)/count(DISTINCT UID),0) AS avg_timewatch,
       coalesce(sum(all_original_play_times)/count(DISTINCT UID),0) AS avg_all_original_play_times,
       coalesce(sum(all_play_times)/count(DISTINCT UID),0) AS avg_all_play_times,
       coalesce(sum(all_finish_times)/count(DISTINCT UID),0) AS avg_all_finish_times,
       coalesce(sum(all_like_times)/count(DISTINCT UID),0) AS avg_all_like_times,
       coalesce(sum(all_follow_times)/count(DISTINCT UID),0) AS avg_all_follow_times,
       coalesce(sum(all_share_times)/count(DISTINCT UID),0) AS avg_all_share_times,
       coalesce(sum(all_comment_times)/count(DISTINCT UID),0) AS avg_all_comment_times,
       coalesce(sum(all_like_times)/sum(all_play_times),0) AS like_rate,
       coalesce(sum(all_follow_times)/sum(all_play_times),0) AS follow_rate,
       coalesce(sum(all_share_times)/sum(all_play_times),0) AS share_rate,
       coalesce(sum(all_comment_times)/sum(all_play_times),0) AS comment_rate,
       coalesce(sum(all_finish_times)/sum(all_play_times),0) AS finish_rate,
       coalesce(sum(hot_fromlist_timewatch)/count(DISTINCT UID),0) AS avg_hot_fromlist_timewatch,
       coalesce(sum(hot_fromlist_play_times)/count(DISTINCT UID),0) AS avg_hot_fromlist_play_times,
       coalesce(sum(hot_fromlist_like_times)/count(DISTINCT UID),0) AS avg_hot_fromlist_like_times,
       coalesce(sum(hot_fromlist_follow_times)/count(DISTINCT UID),0) AS avg_hot_fromlist_follow_times,
       coalesce(sum(hot_fromlist_share_times)/count(DISTINCT UID),0) AS avg_hot_fromlist_share_times,
       coalesce(sum(hot_fromlist_comment_times)/count(DISTINCT UID),0) AS avg_hot_fromlist_comment_times,
       coalesce(sum(hot_fromlist_finish_times)/count(DISTINCT UID),0) AS avg_hot_fromlist_finish_times,
       coalesce(sum(hot_original_play_times)/count(DISTINCT UID),0) AS avg_hot_original_play_times,
       coalesce(sum(hot_unique_click_times)/sum(disposure),0) AS hot_list_ctr,
       coalesce(sum(hot_fromlist_like_times)/sum(hot_fromlist_play_times),0) AS hot_like_rate,
       coalesce(sum(hot_fromlist_follow_times)/sum(hot_fromlist_play_times),0) AS hot_follow_rate,
       coalesce(sum(hot_fromlist_share_times)/sum(hot_fromlist_play_times),0) AS hot_share_rate,
       coalesce(sum(hot_fromlist_comment_times)/sum(hot_fromlist_play_times),0) AS hot_comment_rate,
       coalesce(sum(hot_fromlist_finish_times)/sum(hot_fromlist_play_times),0) AS hot_finish_rate,
       sum(all_play_times) AS all_play_times,
       sum(hot_unique_click_times) AS hot_click,
       '2019-07-30' AS stat_day,
       coalesce(sum(follow_fromlist_timewatch)/count(DISTINCT UID),0) AS follow_fromlist_timewatch,
       coalesce(sum(follow_fromlist_vv)/count(DISTINCT UID),0) AS follow_fromlist_vv,
       coalesce(sum(personal_fromlist_timewatch)/count(DISTINCT UID),0) AS personal_fromlist_timewatch,
       coalesce(sum(personal_fromlist_vv)/count(DISTINCT UID),0) AS personal_fromlist_vv
FROM
  (SELECT t0.uid,
          DOMAIN,
          LAYER,
          exp,
          abflag,
          CASE
              WHEN coalesce(t1.all_play_times,0) <= 5 THEN '1'
              WHEN coalesce(t1.all_play_times,0) <= 55 THEN '2'
              ELSE '3'
          END AS LEVEL,
          coalesce(t1.all_timewatch,0) AS all_timewatch,
          coalesce(t1.all_play_times,0) AS all_play_times,
          coalesce(t1.all_like_times,0) AS all_like_times,
          coalesce(t1.all_play_follow_times,0) AS all_follow_times,
          coalesce(t1.all_share_times,0) AS all_share_times,
          coalesce(t1.all_comment_times,0) AS all_comment_times,
          coalesce(t1.all_finish_times,0) AS all_finish_times,
          coalesce(t1.all_original_play_times,0) AS all_original_play_times,
          coalesce(t1.hot_fromlist_timewatch,0) AS hot_fromlist_timewatch,
          coalesce(t1.hot_fromlist_play_times,0) AS hot_fromlist_play_times,
          coalesce(t1.hot_fromlist_like_times,0) AS hot_fromlist_like_times,
          coalesce(t1.hot_fromlist_play_follow_times,0) AS hot_fromlist_follow_times,
          coalesce(t1.hot_fromlist_share_times,0) AS hot_fromlist_share_times,
          coalesce(t1.hot_fromlist_comment_times,0) AS hot_fromlist_comment_times,
          coalesce(t1.hot_fromlist_finish_times,0) AS hot_fromlist_finish_times,
          coalesce(t1.hot_original_play_times,0) AS hot_original_play_times,
          coalesce(t1.hot_unique_click_times,0) AS hot_unique_click_times,
          coalesce(disposure,0) AS disposure,
          coalesce(follow_fromlist_timewatch,0) AS follow_fromlist_timewatch,
          coalesce(follow_fromlist_vv,0) AS follow_fromlist_vv,
          coalesce(personal_fromlist_timewatch,0) AS personal_fromlist_timewatch,
          coalesce(personal_fromlist_vv,0) AS personal_fromlist_vv
   FROM
     (SELECT UID,
             DOMAIN,
             LAYER,
             experiment AS exp,
             abflag
      FROM like_dw_ded.dwd_like_ded_video_dispatch
      WHERE DAY = '2019-07-30'
        AND hour = '15'
        AND refer_list = 'WELOG_POPULAR'
      GROUP BY UID,
               DOMAIN,
               LAYER,
               experiment,
               abflag)t0
   LEFT OUTER JOIN
     (SELECT UID,
             sum(coalesce(timewatch,0)) AS all_timewatch,
             sum(coalesce(unique_play_times,0)) AS all_play_times,
             sum(coalesce(unique_like_times,0)) AS all_like_times,
             sum(coalesce(unique_follow_times,0)) AS all_play_follow_times,
             sum(coalesce(unique_share_times,0)) AS all_share_times,
             sum(coalesce(unique_comment_times,0)) AS all_comment_times,
             sum(coalesce(unique_finish_times,0)) AS all_finish_times,
             sum(coalesce(play_times,0)) AS all_original_play_times,
             sum(if(fromlist = 'HOT',timewatch,0)) AS hot_fromlist_timewatch,
             sum(if(fromlist = 'HOT',unique_play_times,0)) AS hot_fromlist_play_times,
             sum(if(fromlist = 'HOT',unique_like_times,0)) AS hot_fromlist_like_times,
             sum(if(fromlist = 'HOT',unique_follow_times,0)) AS hot_fromlist_play_follow_times,
             sum(if(fromlist = 'HOT',unique_share_times,0)) AS hot_fromlist_share_times,
             sum(if(fromlist = 'HOT',unique_comment_times,0)) AS hot_fromlist_comment_times,
             sum(if(fromlist = 'HOT',unique_finish_times,0)) AS hot_fromlist_finish_times,
             sum(if(fromlist = 'HOT',play_times,0)) AS hot_original_play_times,
             sum(if(fromlist = 'HOT',unique_click_times,0)) AS hot_unique_click_times,
             sum(if(fromlist = 'FOLLOW',timewatch,0)) AS follow_fromlist_timewatch,
             sum(if(fromlist = 'FOLLOW',unique_play_times,0)) AS follow_fromlist_vv,
             sum(if(fromlist = 'personal',timewatch,0)) AS personal_fromlist_timewatch,
             sum(if(fromlist = 'personal',unique_play_times,0)) AS personal_fromlist_vv
      FROM algo.dw_user_hot_list_view_hour_v2
      WHERE DAY = '2019-07-30'
        AND hour = '15'
      GROUP BY UID)t1 ON t0.uid = t1.uid
   LEFT OUTER JOIN
     (SELECT UID,
             sum(video_exposure_count_01) AS disposure
      FROM like_dw_ded.dwd_like_ded_video_exposure
      WHERE DAY = '2019-07-30'
        AND hour = '15'
        AND refer_list IN ('hot_list')
      GROUP BY UID)t2 ON t0.uid = t2.uid)t0
GROUP BY DOMAIN,
         LAYER,
         exp,
         abflag