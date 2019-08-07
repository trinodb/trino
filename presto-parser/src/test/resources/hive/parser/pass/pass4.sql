SELECT DAY AS day1,
              game_name,
              collect_set(nvl((active_users),0))[0] AS active_users,
                         collect_set(nvl(clicktab_times,0))[0] AS clicktab_times,
                                    collect_set(nvl(clicktab_users,0))[0] AS clicktab_users,
                                               collect_set(nvl(clickgame_times,0))[0] AS clickgame_times,
                                                          collect_set(nvl(clickgame_users,0))[0] AS clickgame_users,
                                                                     collect_set(nvl(strcholeave_times,0))[0] AS strcholeave_times,
                                                                                collect_set(nvl(strcholeave_users,0))[0] AS strcholeave_users,
                                                                                           collect_set(nvl(strchoedit_times,0))[0] AS strchoedit_times,
                                                                                                      collect_set(nvl(strchoedit_users,0))[0] AS strchoedit_users,
                                                                                                                 collect_set(nvl(sofcholeave_times,0))[0] AS sofcholeave_times,
                                                                                                                            collect_set(nvl(sofcholeave_users,0))[0] AS sofcholeave_users,
                                                                                                                                       collect_set(nvl(sofchoedit_times,0))[0] AS sofchoedit_times,
                                                                                                                                                  collect_set(nvl(sofchoedit_users,0))[0] AS sofchoedit_users,
                                                                                                                                                             collect_set(nvl(sendmes_times,0))[0] AS sendmes_times,
                                                                                                                                                                        collect_set(nvl(sendmes_users,0))[0] AS sendmes_users,
                                                                                                                                                                                   collect_set(nvl(binding_times,0))[0] AS binding_times,
                                                                                                                                                                                              collect_set(nvl(binding_users,0))[0] AS binding_users,
                                                                                                                                                                                                         0,
                                                                                                                                                                                                         0,
                                                                                                                                                                                                         0,
                                                                                                                                                                                                         0,
                                                                                                                                                                                                         0,
                                                                                                                                                                                                         0,
                                                                                                                                                                                                         '2019-07-29' AS DAY
FROM
  (SELECT *
   FROM
     (SELECT DAY,
             count(DISTINCT UID) AS active_users
      FROM report_tb.cd_hello_daily_report_no_partition_hzx
      WHERE DAY ='2019-07-29'
      GROUP BY DAY)a
   JOIN
     (SELECT DAY AS dayb,
                    count(UID) AS clicktab_times,
                    count(DISTINCT UID) AS clicktab_users
      FROM hello.hello_common_stat_orc
      WHERE event_id='0102036'
        AND event.event_info['action']='36'
        AND DAY ='2019-07-29'
      GROUP BY DAY)b ON a.day = dayb)c
JOIN
  (SELECT dayd,
          game_name,
          collect_set(clickgame_times)[0] AS clickgame_times,
          collect_set(clickgame_users)[0] AS clickgame_users,
          collect_set(strcholeave_times)[0] AS strcholeave_times,
          collect_set(strcholeave_users)[0] AS strcholeave_users,
          collect_set(strchoedit_times)[0] AS strchoedit_times,
          collect_set(strchoedit_users)[0] AS strchoedit_users,
          collect_set(sofcholeave_times)[0] AS sofcholeave_times,
          collect_set(sofcholeave_users)[0] AS sofcholeave_users,
          collect_set(sofchoedit_times)[0] AS sofchoedit_times,
          collect_set(sofchoedit_users)[0] AS sofchoedit_users,
          collect_set(sendmes_times)[0] AS sendmes_times,
          collect_set(sendmes_users)[0] AS sendmes_users,
          collect_set(binding_times)[0] AS binding_times,
          collect_set(binding_users)[0] AS binding_users
   FROM
     (SELECT dayd,
             game_name,
             collect_set(clickgame_times)[0] AS clickgame_times,
             collect_set(clickgame_users)[0] AS clickgame_users,
             collect_set(strcholeave_times)[0] AS strcholeave_times,
             collect_set(strcholeave_users)[0] AS strcholeave_users,
             collect_set(strchoedit_times)[0] AS strchoedit_times,
             collect_set(strchoedit_users)[0] AS strchoedit_users,
             collect_set(sofcholeave_times)[0] AS sofcholeave_times,
             collect_set(sofcholeave_users)[0] AS sofcholeave_users,
             collect_set(sofchoedit_times)[0] AS sofchoedit_times,
             collect_set(sofchoedit_users)[0] AS sofchoedit_users
      FROM
        (SELECT DAY AS dayd,
                       if(event.event_info['game_name']=''
                          OR event.event_info['game_name'] IS NULL,'未识别',event.event_info['game_name']) AS game_name,
                       count(UID) AS clickgame_times,
                       count(DISTINCT UID) AS clickgame_users
         FROM hello.hello_common_stat_orc
         WHERE event_id='0102036'
           AND event.event_info['action']='37'
           AND DAY ='2019-07-29'
         GROUP BY DAY,
                  if(event.event_info['game_name']=''
                     OR event.event_info['game_name'] IS NULL,'未识别',event.event_info['game_name']))d
      LEFT JOIN
        (SELECT DAY AS daym,
                       if(event.event_info['game_name']=''
                          OR event.event_info['game_name'] IS NULL,'未识别',event.event_info['game_name']) AS game_namem,
                       sum(CASE
                               WHEN event.event_info['window_action']=0
                                    AND event.event_info['action'] =38 THEN 1
                               ELSE 0
                           END) AS strcholeave_times,
                       count(distinct(CASE
                                          WHEN event.event_info['window_action']=0
                                               AND event.event_info['action'] =38 THEN UID
                                      END)) AS strcholeave_users,
                       sum(CASE
                               WHEN event.event_info['window_action']=1
                                    AND event.event_info['action'] =38 THEN 1
                               ELSE 0
                           END) AS strchoedit_times,
                       count(distinct(CASE
                                          WHEN event.event_info['window_action']=1
                                               AND event.event_info['action'] =38 THEN UID
                                      END)) AS strchoedit_users,
                       sum(CASE
                               WHEN event.event_info['window_action']=0
                                    AND event.event_info['action'] =39 THEN 1
                               ELSE 0
                           END) AS sofcholeave_times,
                       count(distinct(CASE
                                          WHEN event.event_info['window_action']=0
                                               AND event.event_info['action'] =39 THEN UID
                                      END)) AS sofcholeave_users,
                       sum(CASE
                               WHEN event.event_info['window_action']=1
                                    AND event.event_info['action'] =39 THEN 1
                               ELSE 0
                           END) AS sofchoedit_times,
                       count(distinct(CASE
                                          WHEN event.event_info['window_action']=1
                                               AND event.event_info['action'] =39 THEN UID
                                      END)) AS sofchoedit_users
         FROM hello.hello_common_stat_orc
         WHERE event_id='0102036'
           AND DAY ='2019-07-29'
         GROUP BY DAY,
                  if(event.event_info['game_name']=''
                     OR event.event_info['game_name'] IS NULL,'未识别',event.event_info['game_name']))m ON dayd=daym
      AND game_name=game_namem
      GROUP BY dayd,
               game_name)e
   LEFT JOIN
     (SELECT dayh,
             game_nameh,
             collect_set(sendmes_times)[0] AS sendmes_times,
             collect_set(sendmes_users)[0] AS sendmes_users,
             collect_set(binding_times)[0] AS binding_times,
             collect_set(binding_users)[0] AS binding_users
      FROM
        (SELECT DAY AS dayh,
                       CASE
                           WHEN game_id=1 THEN 'QQ飞车'
                           WHEN game_id=2 THEN '王者荣耀'
                           WHEN game_id=3 THEN '刺激战场国际服'
                           WHEN game_id=4 THEN '球球大作战'
                           WHEN game_id=5 THEN 'CF手游'
                           WHEN game_id=6 THEN '第五人格'
                           WHEN game_id=7 THEN '和平精英'
                           WHEN game_id=8 THEN '和平精英'
                           WHEN game_id=9 THEN 'DNF'
                           WHEN game_id=10 THEN '绝地求生端游'
                           WHEN game_id=11 THEN 'LOL'
                           WHEN game_id=12 THEN '跑跑卡丁车'
                           ELSE '未识别'
                       END AS game_nameh,
                       count(UID) AS binding_times,
                       count(DISTINCT UID) AS binding_users
         FROM report_tb.hello_daily_user_game_role_info_yangchuman
         WHERE DAY='2019-07-29'
         GROUP BY DAY,
                  CASE
                      WHEN game_id=1 THEN 'QQ飞车'
                      WHEN game_id=2 THEN '王者荣耀'
                      WHEN game_id=3 THEN '刺激战场国际服'
                      WHEN game_id=4 THEN '球球大作战'
                      WHEN game_id=5 THEN 'CF手游'
                      WHEN game_id=6 THEN '第五人格'
                      WHEN game_id=7 THEN '和平精英'
                      WHEN game_id=8 THEN '和平精英'
                      WHEN game_id=9 THEN 'DNF'
                      WHEN game_id=10 THEN '绝地求生端游'
                      WHEN game_id=11 THEN 'LOL'
                      WHEN game_id=12 THEN '跑跑卡丁车'
                      ELSE '未识别'
                  END)j
      LEFT JOIN
        (SELECT DAY AS dayf,
                       if(event.event_info['game_name']=''
                          OR event.event_info['game_name'] IS NULL,'未识别',event.event_info['game_name']) AS game_namef,
                       count(UID) AS sendmes_times,
                       count(DISTINCT UID) AS sendmes_users
         FROM hello.hello_common_stat_orc
         WHERE event_id='0102036'
           AND event.event_info['action']='40'
           AND DAY ='2019-07-29'
         GROUP BY DAY,
                  if(event.event_info['game_name']=''
                     OR event.event_info['game_name'] IS NULL,'未识别',event.event_info['game_name']))f ON dayf=dayh
      AND game_namef=game_nameh
      GROUP BY dayh,
               game_nameh)n ON dayd = dayh
   AND game_name=game_nameh
   GROUP BY dayd,
            game_name)k ON c.day = dayd
GROUP BY DAY,
         game_name