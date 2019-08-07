SELECT '2019-07-29' AS stats_day,
       tab_1.cc,
       tab_1.version,
       tab_1.os,
       tab_1.forum_id,
       tab_1.short_id,
       tab_1.forum_name,
       coalesce(tab_1.forum_uv,0) forum_uv,
       coalesce(tab_1.post_uv,0) post_uv,
       coalesce(tab_1.forum_pv,0) forum_pv,
       coalesce(tab_1.post_pv,0) post_pv,
       coalesce(tab_2.total_share_pv,0) total_share_pv,
       coalesce(tab_2.total_share_uv,0) total_share_uv,
       coalesce(tab_2.total_forum_share_pv,0) total_forum_share_pv,
       coalesce(tab_2.total_forum_share_uv,0) total_forum_share_uv,
       coalesce(tab_2.total_post_share_pv,0) total_post_share_pv,
       coalesce(tab_2.total_post_share_uv,0) total_post_share_uv,
       coalesce(tab_2.share_pv,0) share_pv,
       coalesce(tab_2.share_uv,0) share_uv,
       coalesce(tab_2.forum_share_pv,0) forum_share_pv,
       coalesce(tab_2.forum_share_uv,0) forum_share_uv,
       coalesce(tab_2.post_share_pv,0) post_share_pv,
       coalesce(tab_2.post_share_uv,0) post_share_uv,
       coalesce(tab_2.force_share_pv,0) force_share_pv,
       coalesce(tab_2.force_share_uv,0) force_share_uv,
       coalesce(tab_2.click_force_share_pv,0) click_force_share_pv,
       coalesce(tab_2.click_force_share_uv,0) click_force_share_uv,
       coalesce(tab_2.forum_force_share_pv,0) forum_force_share_pv,
       coalesce(tab_2.forum_force_share_uv,0) forum_force_share_uv,
       coalesce(tab_2.post_force_share_pv,0) post_force_share_pv,
       coalesce(tab_2.post_force_share_uv,0) post_force_share_uv,
       coalesce(tab_3.total_share_inner_tab_uv,0) total_share_inner_tab_uv,
       coalesce(tab_3.total_share_inner_tab_pv,0) total_share_inner_tab_pv,
       coalesce(tab_3.force_share_inner_tab_uv,0) force_share_inner_tab_uv,
       coalesce(tab_3.share_inner_tab_uv,0) share_inner_tab_uv,
       coalesce(tab_3.force_share_inner_tab_pv,0) force_share_inner_tab_pv,
       coalesce(tab_3.share_inner_tab_pv,0) share_inner_tab_pv,
       coalesce(tab_3.inside_share_uv,0) inside_share_uv,
       coalesce(tab_3.outside_share_uv,0) outside_share_uv,
       coalesce(tab_3.inside_share_pv,0) inside_share_pv,
       coalesce(tab_3.outside_share_pv,0) outside_share_pv,
       coalesce(tab_3.forum_share_inner_tab_uv,0) forum_share_inner_tab_uv,
       coalesce(tab_3.post_share_inner_tab_uv,0) post_share_inner_tab_uv,
       coalesce(tab_3.forum_share_inner_tab_pv,0) forum_share_inner_tab_pv,
       coalesce(tab_3.post_share_inner_tab_pv,0) post_share_inner_tab_pv,
       coalesce(tab_3.force_share_rule_1_uv,0) force_share_rule_1_uv,
       coalesce(tab_3.force_share_rule_2_uv,0) force_share_rule_2_uv,
       coalesce(tab_3.force_share_rule_1_pv,0) force_share_rule_1_pv,
       coalesce(tab_3.force_share_rule_2_pv,0) force_share_rule_2_pv,
       coalesce(tab_4.outside_press_uv,0) outside_press_uv,
       coalesce(tab_4.outside_press_pv,0) outside_press_pv,
       coalesce(tab_4.post_outside_press_uv,0) post_outside_press_uv,
       coalesce(tab_4.post_outside_press_pv,0) post_outside_press_pv,
       coalesce(tab_4.forum_outside_press_uv,0) forum_outside_press_uv,
       coalesce(tab_4.forum_outside_press_pv,0) forum_outside_press_pv,
       coalesce(tab_5.ads_down_uv,0) ads_down_uv,
       coalesce(tab_5.force_share_down_uv,0) force_share_down_uv,
       coalesce(tab_5.share_down_uv,0) share_down_uv,
       coalesce(tab_5.ads_down_pv,0) ads_down_pv,
       coalesce(tab_5.force_share_down_pv,0) force_share_down_pv,
       coalesce(tab_5.share_down_pv,0) share_down_pv
FROM
  (SELECT cc,
          VERSION,
          os,
          a.forum_id,
          b.short_id,
          b.forum_name,
          forum_uv,
          post_uv,
          forum_pv,
          post_pv
   FROM
     (SELECT coalesce(cc, 'all') AS cc,
             coalesce(`user-agent`, 'all') AS VERSION,
             coalesce(os, 'all') AS os,
             coalesce(forum_id, 'all') AS forum_id,
             count(DISTINCT UID) AS forum_uv,
             count(DISTINCT if(post_id IS NOT NULL, UID, NULL)) AS post_uv,
             count(UID) AS forum_pv,
             count(if(post_id IS NOT NULL, UID, NULL)) AS post_pv
      FROM
        (SELECT UID,
                coalesce(upper(sim_iso), 'other') AS cc,
                `user-agent`,
                CASE
                    WHEN instr(`user-agent`, 'Android 2019.') != 0
                         AND substr(split(`user-agent`,'\n')[0],-1,1) = 1 THEN 'stable'
                    WHEN instr(`user-agent`, 'AndroidBeta 2019.') != 0
                         AND substr(split(`user-agent`,'\n')[0],-1,1) = 2 THEN 'beta'
                    WHEN instr(split(`user-agent`,'\n')[0], 'iPhone') != 0 THEN 'ios'
                    ELSE 'other'
                END AS os,
                SOURCE,
                forum_id,
                post_id
         FROM indigo_us.monitormanager_forum_view
         WHERE TYPE IN ('post_list',
                        'post',
                        'forum_list',
                        'profile',
                        'msg_list')
           AND forum_id IS NOT NULL
           AND DAY = '2019-07-29'
           AND instr(split(`user-agent`,'\n')[0], 'iPhone') = 0 ) aa
      GROUP BY cc,
               `user-agent`,
               os,
               forum_id WITH CUBE) a
   LEFT JOIN
     (SELECT get_json_object(detail, '$.forum_id') AS forum_id,
             get_json_object(detail, '$.short_id') AS short_id,
             get_json_object(detail, '$.forum_name') AS forum_name,
             get_json_object(detail, '$.tags') AS tags,
             get_json_object(detail, '$.description') AS description,
             get_json_object(detail, '$.share_after_x_post') AS share_after_x_post,
             get_json_object(detail, '$.minutes_between_post') AS minutes_between_post,
             get_json_object(detail, '$.share_type') AS share_type,
             get_json_object(detail, '$.share_type_num') AS share_type_num
      FROM mysql_tb.indigo_tbl_wo_forum_stat
      WHERE stat_date = '2019-07-29'
        AND TYPE = 'set_pref' ) b ON a.forum_id = b.forum_id) tab_1
LEFT JOIN
  (SELECT coalesce(cc, 'all') AS cc,
          coalesce(VERSION, 'all') AS VERSION,
          coalesce(os, 'all') AS os,
          coalesce(forum_id, 'all') AS forum_id,
          count(if(`type` IN ('share','force_share') , UID,NULL)) AS total_share_pv,
          count(DISTINCT if(`type` IN ('share','force_share') , UID,NULL)) AS total_share_uv,
          count(if(`type` IN ('share','force_share')
                   AND share_source = 'post_list', UID,NULL)) AS total_forum_share_pv,
          count(DISTINCT if(`type` IN ('share','force_share')
                            AND share_source = 'post_list', UID,NULL)) AS total_forum_share_uv,
          count(if(`type` IN ('share','force_share')
                   AND share_source = 'post', UID,NULL)) AS total_post_share_pv,
          count(DISTINCT if(`type` IN ('share','force_share')
                            AND share_source = 'post', UID,NULL)) AS total_post_share_uv,
          count(if(`type` = 'share', UID,NULL)) AS share_pv,
          count(DISTINCT if(`type` = 'share', UID,NULL)) AS share_uv,
          count(if(`type` = 'share'
                   AND share_source = 'post_list', UID,NULL)) AS forum_share_pv,
          count(DISTINCT if(`type` = 'share'
                            AND share_source = 'post_list', UID,NULL)) AS forum_share_uv,
          count(if(`type` = 'share'
                   AND share_source = 'post', UID,NULL)) AS post_share_pv,
          count(DISTINCT if(`type` = 'share'
                            AND share_source = 'post', UID,NULL)) AS post_share_uv,
          count(if(`type` = 'force_share', UID,NULL)) AS force_share_pv,
          count(DISTINCT if(`type` = 'force_share', UID,NULL)) AS force_share_uv,
          count(if(`type` = 'force_share'
                   AND force_share_press = 1, UID,NULL)) AS click_force_share_pv,
          count(DISTINCT if(`type` = 'force_share'
                            AND force_share_press = 1, UID,NULL)) AS click_force_share_uv,
          count(if(`type` = 'force_share'
                   AND share_source = 'post_list', UID,NULL)) AS forum_force_share_pv,
          count(DISTINCT if(`type` = 'force_share'
                            AND share_source = 'post_list', UID,NULL)) AS forum_force_share_uv,
          count(if(`type` = 'force_share'
                   AND share_source = 'post', UID,NULL)) AS post_force_share_pv,
          count(DISTINCT if(`type` = 'force_share'
                            AND share_source = 'post', UID,NULL)) AS post_force_share_uv
   FROM
     (SELECT UID,
             coalesce(upper(sim_iso), 'other') AS cc,
             `user-agent` AS VERSION,
             CASE
                 WHEN instr(`user-agent`, 'Android 2019.') != 0
                      AND substr(split(`user-agent`,'\n')[0],-1,1) = 1 THEN 'stable'
                 WHEN instr(`user-agent`, 'AndroidBeta 2019.') != 0
                      AND substr(split(`user-agent`,'\n')[0],-1,1) = 2 THEN 'beta'
                 WHEN instr(split(`user-agent`,'\n')[0], 'iPhone') != 0 THEN 'ios'
                 ELSE 'other'
             END AS os,
             `type` AS `type`,
             forum_id,
             share_source,
             force_share_press
      FROM indigo_us.monitormanager_forum_view
      WHERE `day` = '2019-07-29'
        AND instr(split(`user-agent`,'\n')[0], 'iPhone') = 0 )a
   GROUP BY cc,
            VERSION,
            os,
            forum_id WITH CUBE) tab_2 ----分享按钮弹窗点击
 ON tab_1.version= tab_2.version
AND tab_1.cc = tab_2.cc
AND tab_1.os = tab_2.os
AND tab_1.forum_id = tab_2.forum_id
LEFT JOIN
  (SELECT coalesce(cc, 'all') AS cc,
          coalesce(VERSION, 'all') AS VERSION,
          coalesce(os, 'all') AS os,
          coalesce(forum_id, 'all') AS forum_id,
          count(DISTINCT UID) AS total_share_inner_tab_uv,
          count(UID) AS total_share_inner_tab_pv,
          count(DISTINCT CASE
                             WHEN flag = 'force_share' THEN UID
                         END) AS force_share_inner_tab_uv,
          count(DISTINCT CASE
                             WHEN flag = 'share' THEN UID
                         END) AS share_inner_tab_uv,
          count(CASE
                    WHEN flag = 'force_share' THEN UID
                END) AS force_share_inner_tab_pv,
          count(CASE
                    WHEN flag = 'share' THEN UID
                END) AS share_inner_tab_pv,
          count(DISTINCT CASE
                             WHEN SHARE IN ('friend','story','copylink') THEN UID
                         END) AS inside_share_uv--分享到站内
 ,
          count(DISTINCT CASE
                             WHEN SHARE NOT IN ('friend','story','copylink') THEN UID
                         END) AS outside_share_uv--分享到站外
 ,
          count(CASE
                    WHEN SHARE IN ('friend','story','copylink') THEN UID
                END) AS inside_share_pv,
          count(CASE
                    WHEN SHARE NOT IN ('friend','story','copylink') THEN UID
                END) AS outside_share_pv,
          count(DISTINCT CASE
                             WHEN types = 'forum' THEN UID
                         END) AS forum_share_inner_tab_uv,
          count(DISTINCT CASE
                             WHEN types = 'post' THEN UID
                         END) AS post_share_inner_tab_uv,
          count(CASE
                    WHEN types = 'forum' THEN UID
                END) AS forum_share_inner_tab_pv,
          count(CASE
                    WHEN types = 'post' THEN UID
                END) AS post_share_inner_tab_pv,
          count(DISTINCT CASE
                             WHEN b.share_type_num = 0
                                  AND flag = 'force_share' THEN UID
                         END) AS force_share_rule_1_uv,
          count(DISTINCT CASE
                             WHEN b.share_type_num <> 0
                                  AND flag = 'force_share' THEN UID
                         END) AS force_share_rule_2_uv,
          count(CASE
                    WHEN b.share_type_num = 0
                         AND flag = 'force_share' THEN UID
                END) AS force_share_rule_1_pv,
          count(CASE
                    WHEN b.share_type_num <> 0
                         AND flag = 'force_share' THEN UID
                END) AS force_share_rule_2_pv
   FROM
     (SELECT coalesce(upper(sim_iso), 'other') AS cc,
             `user-agent` AS VERSION,
             CASE
                 WHEN instr(`user-agent`, 'Android 2019.') != 0
                      AND substr(split(`user-agent`,'\n')[0],-1,1) = 1 THEN 'stable'
                 WHEN instr(`user-agent`, 'AndroidBeta 2019.') != 0
                      AND substr(split(`user-agent`,'\n')[0],-1,1) = 2 THEN 'beta'
                 WHEN instr(split(`user-agent`,'\n')[0], 'iPhone') != 0 THEN 'ios'
                 ELSE 'other'
             END AS os,
             CASE
                 WHEN substr(url,-2) = '01' THEN 'force_share'
                 WHEN substr(url,-2) = '02' THEN 'share'
             END AS flag,
             SHARE,
             types,
             substr(url,21,8) AS short_id,
             UID
      FROM indigo_us.monitormanager_client_share_stable
      WHERE DAY = '2019-07-29'
        AND modual = 'forum'
        AND url IS NOT NULL
        AND instr(split(`user-agent`,'\n')[0], 'iPhone') = 0
      UNION ALL SELECT coalesce(upper(sim_iso), 'other') AS cc,
                       `user-agent` AS VERSION,
                       CASE
                           WHEN instr(`user-agent`, 'Android 2019.') != 0
                                AND substr(split(`user-agent`,'\n')[0],-1,1) = 1 THEN 'stable'
                           WHEN instr(`user-agent`, 'AndroidBeta 2019.') != 0
                                AND substr(split(`user-agent`,'\n')[0],-1,1) = 2 THEN 'beta'
                           WHEN instr(split(`user-agent`,'\n')[0], 'iPhone') != 0 THEN 'ios'
                           ELSE 'other'
                       END AS os,
                       CASE
                           WHEN substr(url,-2) = '01' THEN 'force_share'
                           WHEN substr(url,-2) = '02' THEN 'share'
                       END AS flag,
                       SHARE,
                       types,
                       substr(url,21,8) AS short_id,
                       UID
      FROM indigo_us.monitormanager_client_share_beta
      WHERE DAY = '2019-07-29'
        AND modual = 'forum'
        AND url IS NOT NULL
        AND instr(split(`user-agent`,'\n')[0], 'iPhone') = 0 ) a
   LEFT JOIN
     (SELECT get_json_object(detail, '$.forum_id') AS forum_id,
             get_json_object(detail, '$.short_id') AS short_id,
             get_json_object(detail, '$.forum_name') AS forum_name,
             get_json_object(detail, '$.tags') AS tags,
             get_json_object(detail, '$.description') AS description,
             get_json_object(detail, '$.share_after_x_post') AS share_after_x_post,
             get_json_object(detail, '$.minutes_between_post') AS minutes_between_post,
             get_json_object(detail, '$.share_type') AS share_type,
             get_json_object(detail, '$.share_type_num') AS share_type_num
      FROM mysql_tb.indigo_tbl_wo_forum_stat
      WHERE stat_date = '2019-07-29'
        AND TYPE = 'set_pref' ) b ON a.short_id = b.short_id
   GROUP BY cc,
            VERSION,
            os,
            forum_id WITH CUBE)tab_3 ---内部面板点击
 ON tab_1.version= tab_3.version
AND tab_1.cc = tab_3.cc
AND tab_1.os = tab_3.os
AND tab_1.forum_id = tab_3.forum_id
LEFT JOIN
  (SELECT coalesce(VERSION, 'all') AS VERSION,
          coalesce(cc, 'all') AS cc,
          coalesce(os, 'all') AS os,
          coalesce(forum_id, 'all') AS forum_id,
          count(DISTINCT remote_ip) AS outside_press_uv,
          count(remote_ip) AS outside_press_pv,
          count(DISTINCT CASE
                             WHEN TYPE = 'post' THEN remote_ip
                         END) AS post_outside_press_uv,
          count(CASE
                    WHEN TYPE = 'post' THEN remote_ip
                END) AS post_outside_press_pv,
          count(DISTINCT CASE
                             WHEN TYPE = 'forum' THEN remote_ip
                         END) AS forum_outside_press_uv,
          count(CASE
                    WHEN TYPE = 'forum' THEN remote_ip
                END) AS forum_outside_press_pv
   FROM
     (SELECT 'all' AS VERSION,
             'all' AS cc,
             'all' AS os,
             'post' AS TYPE,
             short_id,
             remote_ip
      FROM indigo_us.warpy_share_forum_detail_clicks_success
      WHERE DAY = '2019-07-29'
      UNION ALL SELECT 'all' AS VERSION,
                       'all' AS cc,
                       'all' AS os,
                       'forum' AS TYPE,
                       short_id,
                       remote_ip
      FROM indigo_us.warpy_share_forum_clicks_success
      WHERE DAY = '2019-07-29' ) a
   LEFT JOIN
     (SELECT get_json_object(detail, '$.forum_id') AS forum_id,
             get_json_object(detail, '$.short_id') AS short_id,
             get_json_object(detail, '$.forum_name') AS forum_name,
             get_json_object(detail, '$.tags') AS tags,
             get_json_object(detail, '$.description') AS description,
             get_json_object(detail, '$.share_after_x_post') AS share_after_x_post,
             get_json_object(detail, '$.minutes_between_post') AS minutes_between_post,
             get_json_object(detail, '$.share_type') AS share_type,
             get_json_object(detail, '$.share_type_num') AS share_type_num
      FROM mysql_tb.indigo_tbl_wo_forum_stat
      WHERE stat_date = '2019-07-29'
        AND TYPE = 'set_pref' ) b ON a.short_id = b.short_id
   GROUP BY VERSION,
            cc,
            os,
            forum_id WITH CUBE)tab_4----贴吧外链接点击次数，用户数
 ON tab_1.version= tab_4.version
AND tab_1.cc = tab_4.cc
AND tab_1.os = tab_4.os
AND tab_1.forum_id = tab_4.forum_id
LEFT JOIN
  (SELECT coalesce(cc, 'all') AS cc,
          coalesce(VERSION, 'all') AS VERSION,
          coalesce(os, 'all') AS os,
          coalesce(forum_id, 'all') AS forum_id,
          count(DISTINCT CASE
                             WHEN install_flag = 'ads' THEN udid
                         END) AS ads_down_uv,
          count(DISTINCT CASE
                             WHEN install_flag = 'force_share' THEN udid
                         END) AS force_share_down_uv,
          count(DISTINCT CASE
                             WHEN install_flag = 'share' THEN udid
                         END) AS share_down_uv,
          count(CASE
                    WHEN install_flag = 'ads' THEN udid
                END) AS ads_down_pv,
          count(CASE
                    WHEN install_flag = 'force_share' THEN udid
                END) AS force_share_down_pv,
          count(CASE
                    WHEN install_flag = 'share' THEN udid
                END) AS share_down_pv
   FROM
     (SELECT cc,
             VERSION,
             os,
             CASE
                 WHEN instr(referrer,'?aid=')!=0 THEN 'ads'
                 WHEN instr(referrer,'?s_id=')!=0 THEN 'force_share'
                 ELSE 'share'
             END AS install_flag,
             split(regexp_extract(referrer, 'zone.imo.im\/[0-9]{0,8}', 0), '/')[1] AS short_id,
             udid
      FROM
        (SELECT coalesce(upper(sim_iso), 'other') AS cc,
                `user-agent` AS VERSION,
                CASE
                    WHEN instr(`user-agent`, 'Android 2019.') != 0
                         AND substr(split(`user-agent`,'\n')[0],-1,1) = 1 THEN 'stable'
                    WHEN instr(`user-agent`, 'AndroidBeta 2019.') != 0
                         AND substr(split(`user-agent`,'\n')[0],-1,1) = 2 THEN 'beta'
                    WHEN instr(split(`user-agent`,'\n')[0], 'iPhone') != 0 THEN 'ios'
                    ELSE 'other'
                END AS os,
                udid,
                reflect('java.net.URLDecoder', 'decode', coalesce(reflect('java.net.URLDecoder', 'decode', referrer, 'UTF-8'), 'other'), 'UTF-8') AS referrer
         FROM indigo_us.monitormanager_referrer
         WHERE DAY = '2019-07-29'
           AND instr(split(`user-agent`,'\n')[0], 'iPhone') = 0 )aa
      WHERE instr(referrer,'zone.imo.im') != 0 )a
   LEFT JOIN
     (SELECT get_json_object(detail, '$.forum_id') AS forum_id,
             get_json_object(detail, '$.short_id') AS short_id,
             get_json_object(detail, '$.forum_name') AS forum_name,
             get_json_object(detail, '$.tags') AS tags,
             get_json_object(detail, '$.description') AS description,
             get_json_object(detail, '$.share_after_x_post') AS share_after_x_post,
             get_json_object(detail, '$.minutes_between_post') AS minutes_between_post,
             get_json_object(detail, '$.share_type') AS share_type,
             get_json_object(detail, '$.share_type_num') AS share_type_num
      FROM mysql_tb.indigo_tbl_wo_forum_stat
      WHERE stat_date = '2019-07-29'
        AND TYPE = 'set_pref' ) b ON a.short_id = b.short_id
   GROUP BY cc,
            VERSION,
            os,
            forum_id WITH CUBE)tab_5 ----站外分享安装转化
 ON tab_1.version= tab_5.version
AND tab_1.cc = tab_5.cc
AND tab_1.os = tab_5.os
AND tab_1.forum_id = tab_5.forum_id;



SELECT '2019-07-29' AS stats_day,
       tab_1.cc,
       tab_1.version,
       tab_1.os,
       tab_1.forum_id,
       tab_1.short_id,
       tab_1.forum_name,
       coalesce(tab_1.forum_uv,0) forum_uv,
       coalesce(tab_1.post_uv,0) post_uv,
       coalesce(tab_1.forum_pv,0) forum_pv,
       coalesce(tab_1.post_pv,0) post_pv,
       coalesce(tab_2.total_share_pv,0) total_share_pv,
       coalesce(tab_2.total_share_uv,0) total_share_uv,
       coalesce(tab_2.total_forum_share_pv,0) total_forum_share_pv,
       coalesce(tab_2.total_forum_share_uv,0) total_forum_share_uv,
       coalesce(tab_2.total_post_share_pv,0) total_post_share_pv,
       coalesce(tab_2.total_post_share_uv,0) total_post_share_uv,
       coalesce(tab_2.share_pv,0) share_pv,
       coalesce(tab_2.share_uv,0) share_uv,
       coalesce(tab_2.forum_share_pv,0) forum_share_pv,
       coalesce(tab_2.forum_share_uv,0) forum_share_uv,
       coalesce(tab_2.post_share_pv,0) post_share_pv,
       coalesce(tab_2.post_share_uv,0) post_share_uv,
       coalesce(tab_2.force_share_pv,0) force_share_pv,
       coalesce(tab_2.force_share_uv,0) force_share_uv,
       coalesce(tab_2.click_force_share_pv,0) click_force_share_pv,
       coalesce(tab_2.click_force_share_uv,0) click_force_share_uv,
       coalesce(tab_2.forum_force_share_pv,0) forum_force_share_pv,
       coalesce(tab_2.forum_force_share_uv,0) forum_force_share_uv,
       coalesce(tab_2.post_force_share_pv,0) post_force_share_pv,
       coalesce(tab_2.post_force_share_uv,0) post_force_share_uv,
       coalesce(tab_3.total_share_inner_tab_uv,0) total_share_inner_tab_uv,
       coalesce(tab_3.total_share_inner_tab_pv,0) total_share_inner_tab_pv,
       coalesce(tab_3.force_share_inner_tab_uv,0) force_share_inner_tab_uv,
       coalesce(tab_3.share_inner_tab_uv,0) share_inner_tab_uv,
       coalesce(tab_3.force_share_inner_tab_pv,0) force_share_inner_tab_pv,
       coalesce(tab_3.share_inner_tab_pv,0) share_inner_tab_pv,
       coalesce(tab_3.inside_share_uv,0) inside_share_uv,
       coalesce(tab_3.outside_share_uv,0) outside_share_uv,
       coalesce(tab_3.inside_share_pv,0) inside_share_pv,
       coalesce(tab_3.outside_share_pv,0) outside_share_pv,
       coalesce(tab_3.forum_share_inner_tab_uv,0) forum_share_inner_tab_uv,
       coalesce(tab_3.post_share_inner_tab_uv,0) post_share_inner_tab_uv,
       coalesce(tab_3.forum_share_inner_tab_pv,0) forum_share_inner_tab_pv,
       coalesce(tab_3.post_share_inner_tab_pv,0) post_share_inner_tab_pv,
       coalesce(tab_3.force_share_rule_1_uv,0) force_share_rule_1_uv,
       coalesce(tab_3.force_share_rule_2_uv,0) force_share_rule_2_uv,
       coalesce(tab_3.force_share_rule_1_pv,0) force_share_rule_1_pv,
       coalesce(tab_3.force_share_rule_2_pv,0) force_share_rule_2_pv,
       coalesce(tab_4.outside_press_uv,0) outside_press_uv,
       coalesce(tab_4.outside_press_pv,0) outside_press_pv,
       coalesce(tab_4.post_outside_press_uv,0) post_outside_press_uv,
       coalesce(tab_4.post_outside_press_pv,0) post_outside_press_pv,
       coalesce(tab_4.forum_outside_press_uv,0) forum_outside_press_uv,
       coalesce(tab_4.forum_outside_press_pv,0) forum_outside_press_pv,
       coalesce(tab_5.ads_down_uv,0) ads_down_uv,
       coalesce(tab_5.force_share_down_uv,0) force_share_down_uv,
       coalesce(tab_5.share_down_uv,0) share_down_uv,
       coalesce(tab_5.ads_down_pv,0) ads_down_pv,
       coalesce(tab_5.force_share_down_pv,0) force_share_down_pv,
       coalesce(tab_5.share_down_pv,0) share_down_pv
FROM
  (SELECT cc,
          VERSION,
          os,
          a.forum_id,
          b.short_id,
          b.forum_name,
          forum_uv,
          post_uv,
          forum_pv,
          post_pv
   FROM
     (SELECT coalesce(cc, 'all') AS cc,
             coalesce("user-agent", 'all') AS VERSION,
             coalesce(os, 'all') AS os,
             coalesce(forum_id, 'all') AS forum_id,
             count(DISTINCT UID) AS forum_uv,
             count(DISTINCT if(post_id IS NOT NULL, UID, NULL)) AS post_uv,
             count(UID) AS forum_pv,
             count(if(post_id IS NOT NULL, UID, NULL)) AS post_pv
      FROM
        (SELECT UID,
                coalesce(upper(sim_iso), 'other') AS cc,
                "user-agent",
                CASE
                    WHEN instr("user-agent", 'Android 2019.') != 0
                         AND substr(split("user-agent",'\n')[0],-1,1) = 1 THEN 'stable'
                    WHEN instr("user-agent", 'AndroidBeta 2019.') != 0
                         AND substr(split("user-agent",'\n')[0],-1,1) = 2 THEN 'beta'
                    WHEN instr(split("user-agent",'\n')[0], 'iPhone') != 0 THEN 'ios'
                    ELSE 'other'
                END AS os,
                SOURCE,
                forum_id,
                post_id
         FROM indigo_us.monitormanager_forum_view
         WHERE TYPE IN ('post_list',
                        'post',
                        'forum_list',
                        'profile',
                        'msg_list')
           AND forum_id IS NOT NULL
           AND DAY = '2019-07-29'
           AND instr(split("user-agent",'\n')[0], 'iPhone') = 0 ) aa
      GROUP BY CUBE(cc,
               "user-agent",
               os,
               forum_id)) a
   LEFT JOIN
     (SELECT get_json_object(detail, '$.forum_id') AS forum_id,
             get_json_object(detail, '$.short_id') AS short_id,
             get_json_object(detail, '$.forum_name') AS forum_name,
             get_json_object(detail, '$.tags') AS tags,
             get_json_object(detail, '$.description') AS description,
             get_json_object(detail, '$.share_after_x_post') AS share_after_x_post,
             get_json_object(detail, '$.minutes_between_post') AS minutes_between_post,
             get_json_object(detail, '$.share_type') AS share_type,
             get_json_object(detail, '$.share_type_num') AS share_type_num
      FROM mysql_tb.indigo_tbl_wo_forum_stat
      WHERE stat_date = '2019-07-29'
        AND TYPE = 'set_pref' ) b ON a.forum_id = b.forum_id) tab_1
LEFT JOIN
  (SELECT coalesce(cc, 'all') AS cc,
          coalesce(VERSION, 'all') AS VERSION,
          coalesce(os, 'all') AS os,
          coalesce(forum_id, 'all') AS forum_id,
          count(if("type" IN ('share','force_share') , UID,NULL)) AS total_share_pv,
          count(DISTINCT if("type" IN ('share','force_share') , UID,NULL)) AS total_share_uv,
          count(if("type" IN ('share','force_share')
                   AND share_source = 'post_list', UID,NULL)) AS total_forum_share_pv,
          count(DISTINCT if("type" IN ('share','force_share')
                            AND share_source = 'post_list', UID,NULL)) AS total_forum_share_uv,
          count(if("type" IN ('share','force_share')
                   AND share_source = 'post', UID,NULL)) AS total_post_share_pv,
          count(DISTINCT if("type" IN ('share','force_share')
                            AND share_source = 'post', UID,NULL)) AS total_post_share_uv,
          count(if("type" = 'share', UID,NULL)) AS share_pv,
          count(DISTINCT if("type" = 'share', UID,NULL)) AS share_uv,
          count(if("type" = 'share'
                   AND share_source = 'post_list', UID,NULL)) AS forum_share_pv,
          count(DISTINCT if("type" = 'share'
                            AND share_source = 'post_list', UID,NULL)) AS forum_share_uv,
          count(if("type" = 'share'
                   AND share_source = 'post', UID,NULL)) AS post_share_pv,
          count(DISTINCT if("type" = 'share'
                            AND share_source = 'post', UID,NULL)) AS post_share_uv,
          count(if("type" = 'force_share', UID,NULL)) AS force_share_pv,
          count(DISTINCT if("type" = 'force_share', UID,NULL)) AS force_share_uv,
          count(if("type" = 'force_share'
                   AND force_share_press = 1, UID,NULL)) AS click_force_share_pv,
          count(DISTINCT if("type" = 'force_share'
                            AND force_share_press = 1, UID,NULL)) AS click_force_share_uv,
          count(if("type" = 'force_share'
                   AND share_source = 'post_list', UID,NULL)) AS forum_force_share_pv,
          count(DISTINCT if("type" = 'force_share'
                            AND share_source = 'post_list', UID,NULL)) AS forum_force_share_uv,
          count(if("type" = 'force_share'
                   AND share_source = 'post', UID,NULL)) AS post_force_share_pv,
          count(DISTINCT if("type" = 'force_share'
                            AND share_source = 'post', UID,NULL)) AS post_force_share_uv
   FROM
     (SELECT UID,
             coalesce(upper(sim_iso), 'other') AS cc,
             "user-agent" AS VERSION,
             CASE
                 WHEN instr("user-agent", 'Android 2019.') != 0
                      AND substr(split("user-agent",'\n')[0],-1,1) = 1 THEN 'stable'
                 WHEN instr("user-agent", 'AndroidBeta 2019.') != 0
                      AND substr(split("user-agent",'\n')[0],-1,1) = 2 THEN 'beta'
                 WHEN instr(split("user-agent",'\n')[0], 'iPhone') != 0 THEN 'ios'
                 ELSE 'other'
             END AS os,
             "type" AS "type",
             forum_id,
             share_source,
             force_share_press
      FROM indigo_us.monitormanager_forum_view
      WHERE "day" = '2019-07-29'
        AND instr(split("user-agent",'\n')[0], 'iPhone') = 0 )a
   GROUP BY CUBE(cc,
            VERSION,
            os,
            forum_id)) tab_2 ----分享按钮弹窗点击
 ON tab_1.version= tab_2.version
AND tab_1.cc = tab_2.cc
AND tab_1.os = tab_2.os
AND tab_1.forum_id = tab_2.forum_id
LEFT JOIN
  (SELECT coalesce(cc, 'all') AS cc,
          coalesce(VERSION, 'all') AS VERSION,
          coalesce(os, 'all') AS os,
          coalesce(forum_id, 'all') AS forum_id,
          count(DISTINCT UID) AS total_share_inner_tab_uv,
          count(UID) AS total_share_inner_tab_pv,
          count(DISTINCT CASE
                             WHEN flag = 'force_share' THEN UID
                         END) AS force_share_inner_tab_uv,
          count(DISTINCT CASE
                             WHEN flag = 'share' THEN UID
                         END) AS share_inner_tab_uv,
          count(CASE
                    WHEN flag = 'force_share' THEN UID
                END) AS force_share_inner_tab_pv,
          count(CASE
                    WHEN flag = 'share' THEN UID
                END) AS share_inner_tab_pv,
          count(DISTINCT CASE
                             WHEN SHARE IN ('friend','story','copylink') THEN UID
                         END) AS inside_share_uv--分享到站内
 ,
          count(DISTINCT CASE
                             WHEN SHARE NOT IN ('friend','story','copylink') THEN UID
                         END) AS outside_share_uv--分享到站外
 ,
          count(CASE
                    WHEN SHARE IN ('friend','story','copylink') THEN UID
                END) AS inside_share_pv,
          count(CASE
                    WHEN SHARE NOT IN ('friend','story','copylink') THEN UID
                END) AS outside_share_pv,
          count(DISTINCT CASE
                             WHEN types = 'forum' THEN UID
                         END) AS forum_share_inner_tab_uv,
          count(DISTINCT CASE
                             WHEN types = 'post' THEN UID
                         END) AS post_share_inner_tab_uv,
          count(CASE
                    WHEN types = 'forum' THEN UID
                END) AS forum_share_inner_tab_pv,
          count(CASE
                    WHEN types = 'post' THEN UID
                END) AS post_share_inner_tab_pv,
          count(DISTINCT CASE
                             WHEN b.share_type_num = 0
                                  AND flag = 'force_share' THEN UID
                         END) AS force_share_rule_1_uv,
          count(DISTINCT CASE
                             WHEN b.share_type_num <> 0
                                  AND flag = 'force_share' THEN UID
                         END) AS force_share_rule_2_uv,
          count(CASE
                    WHEN b.share_type_num = 0
                         AND flag = 'force_share' THEN UID
                END) AS force_share_rule_1_pv,
          count(CASE
                    WHEN b.share_type_num <> 0
                         AND flag = 'force_share' THEN UID
                END) AS force_share_rule_2_pv
   FROM
     (SELECT coalesce(upper(sim_iso), 'other') AS cc,
             "user-agent" AS VERSION,
             CASE
                 WHEN instr("user-agent", 'Android 2019.') != 0
                      AND substr(split("user-agent",'\n')[0],-1,1) = 1 THEN 'stable'
                 WHEN instr("user-agent", 'AndroidBeta 2019.') != 0
                      AND substr(split("user-agent",'\n')[0],-1,1) = 2 THEN 'beta'
                 WHEN instr(split("user-agent",'\n')[0], 'iPhone') != 0 THEN 'ios'
                 ELSE 'other'
             END AS os,
             CASE
                 WHEN substr(url,-2) = '01' THEN 'force_share'
                 WHEN substr(url,-2) = '02' THEN 'share'
             END AS flag,
             SHARE,
             types,
             substr(url,21,8) AS short_id,
             UID
      FROM indigo_us.monitormanager_client_share_stable
      WHERE DAY = '2019-07-29'
        AND modual = 'forum'
        AND url IS NOT NULL
        AND instr(split("user-agent",'\n')[0], 'iPhone') = 0
      UNION ALL SELECT coalesce(upper(sim_iso), 'other') AS cc,
                       "user-agent" AS VERSION,
                       CASE
                           WHEN instr("user-agent", 'Android 2019.') != 0
                                AND substr(split("user-agent",'\n')[0],-1,1) = 1 THEN 'stable'
                           WHEN instr("user-agent", 'AndroidBeta 2019.') != 0
                                AND substr(split("user-agent",'\n')[0],-1,1) = 2 THEN 'beta'
                           WHEN instr(split("user-agent",'\n')[0], 'iPhone') != 0 THEN 'ios'
                           ELSE 'other'
                       END AS os,
                       CASE
                           WHEN substr(url,-2) = '01' THEN 'force_share'
                           WHEN substr(url,-2) = '02' THEN 'share'
                       END AS flag,
                       SHARE,
                       types,
                       substr(url,21,8) AS short_id,
                       UID
      FROM indigo_us.monitormanager_client_share_beta
      WHERE DAY = '2019-07-29'
        AND modual = 'forum'
        AND url IS NOT NULL
        AND instr(split("user-agent",'\n')[0], 'iPhone') = 0 ) a
   LEFT JOIN
     (SELECT get_json_object(detail, '$.forum_id') AS forum_id,
             get_json_object(detail, '$.short_id') AS short_id,
             get_json_object(detail, '$.forum_name') AS forum_name,
             get_json_object(detail, '$.tags') AS tags,
             get_json_object(detail, '$.description') AS description,
             get_json_object(detail, '$.share_after_x_post') AS share_after_x_post,
             get_json_object(detail, '$.minutes_between_post') AS minutes_between_post,
             get_json_object(detail, '$.share_type') AS share_type,
             get_json_object(detail, '$.share_type_num') AS share_type_num
      FROM mysql_tb.indigo_tbl_wo_forum_stat
      WHERE stat_date = '2019-07-29'
        AND TYPE = 'set_pref' ) b ON a.short_id = b.short_id
   GROUP BY CUBE(cc,
            VERSION,
            os,
            forum_id))tab_3 ---内部面板点击
 ON tab_1.version= tab_3.version
AND tab_1.cc = tab_3.cc
AND tab_1.os = tab_3.os
AND tab_1.forum_id = tab_3.forum_id
LEFT JOIN
  (SELECT coalesce(VERSION, 'all') AS VERSION,
          coalesce(cc, 'all') AS cc,
          coalesce(os, 'all') AS os,
          coalesce(forum_id, 'all') AS forum_id,
          count(DISTINCT remote_ip) AS outside_press_uv,
          count(remote_ip) AS outside_press_pv,
          count(DISTINCT CASE
                             WHEN TYPE = 'post' THEN remote_ip
                         END) AS post_outside_press_uv,
          count(CASE
                    WHEN TYPE = 'post' THEN remote_ip
                END) AS post_outside_press_pv,
          count(DISTINCT CASE
                             WHEN TYPE = 'forum' THEN remote_ip
                         END) AS forum_outside_press_uv,
          count(CASE
                    WHEN TYPE = 'forum' THEN remote_ip
                END) AS forum_outside_press_pv
   FROM
     (SELECT 'all' AS VERSION,
             'all' AS cc,
             'all' AS os,
             'post' AS TYPE,
             short_id,
             remote_ip
      FROM indigo_us.warpy_share_forum_detail_clicks_success
      WHERE DAY = '2019-07-29'
      UNION ALL SELECT 'all' AS VERSION,
                       'all' AS cc,
                       'all' AS os,
                       'forum' AS TYPE,
                       short_id,
                       remote_ip
      FROM indigo_us.warpy_share_forum_clicks_success
      WHERE DAY = '2019-07-29' ) a
   LEFT JOIN
     (SELECT get_json_object(detail, '$.forum_id') AS forum_id,
             get_json_object(detail, '$.short_id') AS short_id,
             get_json_object(detail, '$.forum_name') AS forum_name,
             get_json_object(detail, '$.tags') AS tags,
             get_json_object(detail, '$.description') AS description,
             get_json_object(detail, '$.share_after_x_post') AS share_after_x_post,
             get_json_object(detail, '$.minutes_between_post') AS minutes_between_post,
             get_json_object(detail, '$.share_type') AS share_type,
             get_json_object(detail, '$.share_type_num') AS share_type_num
      FROM mysql_tb.indigo_tbl_wo_forum_stat
      WHERE stat_date = '2019-07-29'
        AND TYPE = 'set_pref' ) b ON a.short_id = b.short_id
   GROUP BY CUBE(VERSION,
            cc,
            os,
            forum_id))tab_4----贴吧外链接点击次数，用户数
 ON tab_1.version= tab_4.version
AND tab_1.cc = tab_4.cc
AND tab_1.os = tab_4.os
AND tab_1.forum_id = tab_4.forum_id
LEFT JOIN
  (SELECT coalesce(cc, 'all') AS cc,
          coalesce(VERSION, 'all') AS VERSION,
          coalesce(os, 'all') AS os,
          coalesce(forum_id, 'all') AS forum_id,
          count(DISTINCT CASE
                             WHEN install_flag = 'ads' THEN udid
                         END) AS ads_down_uv,
          count(DISTINCT CASE
                             WHEN install_flag = 'force_share' THEN udid
                         END) AS force_share_down_uv,
          count(DISTINCT CASE
                             WHEN install_flag = 'share' THEN udid
                         END) AS share_down_uv,
          count(CASE
                    WHEN install_flag = 'ads' THEN udid
                END) AS ads_down_pv,
          count(CASE
                    WHEN install_flag = 'force_share' THEN udid
                END) AS force_share_down_pv,
          count(CASE
                    WHEN install_flag = 'share' THEN udid
                END) AS share_down_pv
   FROM
     (SELECT cc,
             VERSION,
             os,
             CASE
                 WHEN instr(referrer,'?aid=')!=0 THEN 'ads'
                 WHEN instr(referrer,'?s_id=')!=0 THEN 'force_share'
                 ELSE 'share'
             END AS install_flag,
             split(regexp_extract(referrer, 'zone.imo.im\/[0-9]{0,8}', 0), '/')[1] AS short_id,
             udid
      FROM
        (SELECT coalesce(upper(sim_iso), 'other') AS cc,
                "user-agent" AS VERSION,
                CASE
                    WHEN instr("user-agent", 'Android 2019.') != 0
                         AND substr(split("user-agent",'\n')[0],-1,1) = 1 THEN 'stable'
                    WHEN instr("user-agent", 'AndroidBeta 2019.') != 0
                         AND substr(split("user-agent",'\n')[0],-1,1) = 2 THEN 'beta'
                    WHEN instr(split("user-agent",'\n')[0], 'iPhone') != 0 THEN 'ios'
                    ELSE 'other'
                END AS os,
                udid,
                reflect('java.net.URLDecoder', 'decode', coalesce(reflect('java.net.URLDecoder', 'decode', referrer, 'UTF-8'), 'other'), 'UTF-8') AS referrer
         FROM indigo_us.monitormanager_referrer
         WHERE DAY = '2019-07-29'
           AND instr(split("user-agent",'\n')[0], 'iPhone') = 0 )aa
      WHERE instr(referrer,'zone.imo.im') != 0 )a
   LEFT JOIN
     (SELECT get_json_object(detail, '$.forum_id') AS forum_id,
             get_json_object(detail, '$.short_id') AS short_id,
             get_json_object(detail, '$.forum_name') AS forum_name,
             get_json_object(detail, '$.tags') AS tags,
             get_json_object(detail, '$.description') AS description,
             get_json_object(detail, '$.share_after_x_post') AS share_after_x_post,
             get_json_object(detail, '$.minutes_between_post') AS minutes_between_post,
             get_json_object(detail, '$.share_type') AS share_type,
             get_json_object(detail, '$.share_type_num') AS share_type_num
      FROM mysql_tb.indigo_tbl_wo_forum_stat
      WHERE stat_date = '2019-07-29'
        AND TYPE = 'set_pref' ) b ON a.short_id = b.short_id
   GROUP BY CUBE(cc,
            VERSION,
            os,
            forum_id))tab_5 ----站外分享安装转化
 ON tab_1.version= tab_5.version
AND tab_1.cc = tab_5.cc
AND tab_1.os = tab_5.os
AND tab_1.forum_id = tab_5.forum_id;
