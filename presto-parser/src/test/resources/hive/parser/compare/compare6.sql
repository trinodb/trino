SELECT t1.*,
       nvl(t2.total_diamond,0) total_diamond,
       nvl(t2.recharge_diamond,0) AS recharge_diamond2,
       nvl(t2.exchange_diamond,0) exchange_diamond,
       nvl(t2.black_diamond,0) black_diamond,
       nvl(t3.use_diamond,0) AS use_diamond2,
       nvl(t3.lottery_diamond,0) lottery_diamond,
       nvl(t3.noble_diamond,0) noble_diamond,
       nvl(t3.car_diamond,0) car_diamond,
       nvl(t3.star_gift,0) star_gift,
       nvl(t3.lucky_gift,0) lucky_gift,
       nvl(t3.bag_gift,0) bag_gift,
       nvl(t3.ordinary_gift,0) ordinary_gift,
       0 AS total_post_diamonds,
       0 AS post_diamonds2,
       0 AS black_diamonds,
       '2019-07-29' AS DAY
FROM
  (SELECT a.cday,
          a.appid,
          nvl(b.recharge_diamond,0) AS recharge_diamond,
          nvl(b.recharge_uid,0) recharge_uid,
          nvl(b.total_use_diamond,0) total_use_diamond,
          nvl(b.total_use_diamond_uid,0) total_use_diamond_uid,
          nvl(b.use_diamond,0) use_diamond,
          nvl(b.use_diamond_uid,0) use_diamond_uid,
          nvl(b.use_black_diamond,0) use_black_diamond,
          nvl(b.use_black_diamond_uid,0) use_black_diamond_uid,
          0 AS post_diamonds,
          nvl(a.dau,0) total_uid,
          nvl(b.recharge_uid/a.dau,0) r_recharge
   FROM
     (SELECT DAY cday,
                 appkey appid,
                 count(DISTINCT UID) DAU
      FROM hello.hello_daily_report
      WHERE DAY='2019-07-29'
        AND appkey IS NOT NULL
      GROUP BY DAY,
               appkey) a
   LEFT JOIN
     (SELECT DAY cday,
                 appid,
                 sum(if(TYPE=1,vm_change,0)) recharge_diamond,
                 count(DISTINCT if(TYPE IN (1,60),UID,NULL)) recharge_uid,
                 sum(if(vm_change<0,vm_change,0)) total_use_diamond,
                 count(DISTINCT if(vm_change<0,UID,NULL)) total_use_diamond_uid,
                 sum(if(vm_change<0
                        AND vm_typeid=2,vm_change,0)) use_diamond,
                 count(DISTINCT if(vm_change<0
                                   AND vm_typeid=2,UID,NULL)) use_diamond_uid,
                 sum(if(vm_change<0
                        AND vm_typeid=5,vm_change,0)) use_black_diamond,
                 count(DISTINCT if(vm_change<0
                                   AND vm_typeid=5,UID,NULL)) use_black_diamond_uid
      FROM hello.user_vmoney_changelog
      WHERE DAY='2019-07-29'
        AND vm_typeid IN(2,
                         5)
        AND TYPE!=3
      GROUP BY DAY,
               appid) b ON a.cday=b.cday
   AND a.appid=b.appid) t1
LEFT JOIN
  (SELECT DAY,
          appid,
          sum(if(TYPE=1
                 AND vm_typeid=2,vm_change,0))+sum(if(TYPE=60
                                                      AND vm_typeid=2,vm_change,0))+sum(if(TYPE=62
                                                                                           AND vm_typeid=5,vm_change,0)) total_diamond,
          sum(if(TYPE=1
                 AND vm_typeid=2,vm_change,0)) recharge_diamond,
          sum(if(TYPE=60
                 AND vm_typeid=2,vm_change,0)) exchange_diamond,
          sum(if(TYPE=62
                 AND vm_typeid=5,vm_change,0)) black_diamond
   FROM hello.user_vmoney_changelog
   WHERE DAY='2019-07-29'
     AND vm_typeid IN (2,
                       5)
     AND TYPE IN (1,
                  60,
                  62)
   GROUP BY DAY,
            appid) t2 ON t1.cday=t2.day
AND t1.appid=t2.appid
LEFT JOIN
  (SELECT a1.*,
          b1.star_gift,
          b1.lucky_gift,
          b1.bag_gift,
          b1.ordinary_gift
   FROM
     (SELECT DAY,
             appid,
             sum(if(vm_change>0,vm_change,0)) use_diamond,
             sum(if(TYPE=53,vm_change,0)) lottery_diamond,
             sum(if(TYPE=61,vm_change,0)) noble_diamond,
             sum(if(TYPE=50,vm_change,0)) car_diamond
      FROM hello.user_vmoney_changelog
      WHERE DAY='2019-07-29'
        AND vm_typeid IN (2,
                          5)
        AND TYPE NOT IN (3,
                         1,
                         60,
                         62)
      GROUP BY DAY,
               appid) a1
   LEFT JOIN
     (SELECT DAY AS cday,
                    appid,
                    sum(if(type_name='周星礼物',money,0)) star_gift,
                    sum(if(type_name='幸运礼物',money,0)) lucky_gift,
                    sum(if(type_name='福袋礼物',money,0)) bag_gift,
                    sum(if(type_name='普通礼物',money,0)) ordinary_gift
      FROM
        (SELECT t_a.day,
                t_a.appid,
                CASE
                    WHEN t_b.groupid=1 THEN '普通金币礼物'
                    WHEN t_b.groupid=2 THEN '普通钻石礼物'
                    WHEN t_b.groupid=3 THEN '幸运礼物'
                    WHEN t_b.groupid=4 THEN '极品礼物'
                    WHEN t_b.groupid=5 THEN '福袋礼物'
                    ELSE t_b.groupid
                END AS type_name,
                t_a.vgift_typeid,
                vm_count money
         FROM
           (SELECT from_uid,
                   DAY,
                   vgift_typeid,
                   vm_count,
                   appid
            FROM report_tb.hello_user_send_gift_zpj
            WHERE DAY='2019-07-29'
              AND status=1
              AND vm_typeid=2) t_a
         LEFT JOIN hello.virtual_gift_type t_b ON t_a.vgift_typeid=t_b.typeid
         WHERE t_b.groupid NOT IN (1,
                                   2)
         UNION ALL SELECT DAY,
                          appid,
                          CASE
                              WHEN superscript_id=1 THEN '周星礼物'
                              WHEN superscript_id=2 THEN '活动礼物'
                              WHEN superscript_id=3 THEN '年度礼物'
                              ELSE superscript_id
                          END AS type_name,
                          vgift_typeid,
                          money
         FROM
           (SELECT t_a.day,
                   t_a.appid,
                   if(cast(substr(t_a.ts,1,10) AS bigint) BETWEEN t_b.superscript_start_time AND t_b.superscript_end_time,t_b.superscript_id,0) superscript_id,
                   t_a.vgift_typeid,
                   t_a.from_uid,
                   vm_count money
            FROM
              (SELECT from_uid,
                      DAY,
                      vgift_typeid,
                      vm_count,
                      ts,
                      appid
               FROM report_tb.hello_user_send_gift_zpj
               WHERE DAY='2019-07-29'
                 AND status=1
                 AND vm_typeid=2) t_a
            LEFT JOIN hello.virtual_gift_type t_b ON t_a.vgift_typeid=t_b.typeid) ee
         WHERE superscript_id!=0
         UNION ALL SELECT DAY,
                          appid,
                          '普通礼物' AS type_name,
                          vgift_typeid,
                          money
         FROM
           (SELECT t_a.day,
                   t_a.appid,
                   if(cast(substr(t_a.ts,1,10) AS bigint) BETWEEN t_b.superscript_start_time AND t_b.superscript_end_time,t_b.superscript_id,0) superscript_id,
                   t_a.vgift_typeid,
                   t_b.name,
                   t_a.from_uid,
                   vm_count money
            FROM
              (SELECT from_uid,
                      DAY,
                      vgift_typeid,
                      vm_count,
                      ts,
                      appid
               FROM report_tb.hello_user_send_gift_zpj
               WHERE DAY='2019-07-29'
                 AND status=1
                 AND vm_typeid=2) t_a
            LEFT JOIN hello.virtual_gift_type t_b ON t_a.vgift_typeid=t_b.typeid
            WHERE t_b.groupid IN (1,
                                  2)) ee
         WHERE superscript_id=0) ttt
      GROUP BY DAY,
               appid) b1 ON a1.day=b1.cday
   AND a1.appid=b1.appid) t3 ON t1.cday=t3.day
AND t1.appid=t3.appid
UNION ALL
SELECT t1.*,
       t2.total_diamond,
       t2.recharge_diamond AS recharge_diamond2,
       t2.exchange_diamond,
       t2.black_diamond,
       t3.use_diamond AS use_diamond2,
       t3.lottery_diamond,
       t3.noble_diamond,
       t3.car_diamond,
       t3.star_gift,
       t3.lucky_gift,
       t3.bag_gift,
       t3.ordinary_gift,
       t4.total_post_diamonds,
       t4.post_diamonds2,
       t4.black_diamonds,
       '2019-07-29' AS DAY
FROM
  (SELECT a.*,
          c.post_diamonds,
          b.dau total_uid,
          a.recharge_uid/b.dau r_recharge
   FROM
     (SELECT DAY cday,
                 'all' AS appid,
                 sum(if(TYPE=1,vm_change,0)) recharge_diamond,
                 count(DISTINCT if(TYPE IN (1,60),UID,NULL)) recharge_uid,
                 sum(if(vm_change<0,vm_change,0)) total_use_diamond,
                 count(DISTINCT if(vm_change<0,UID,NULL)) total_use_diamond_uid,
                 sum(if(vm_change<0
                        AND vm_typeid=2,vm_change,0)) use_diamond,
                 count(DISTINCT if(vm_change<0
                                   AND vm_typeid=2,UID,NULL)) use_diamond_uid,
                 sum(if(vm_change<0
                        AND vm_typeid=5,vm_change,0)) use_black_diamond,
                 count(DISTINCT if(vm_change<0
                                   AND vm_typeid=5,UID,NULL)) use_black_diamond_uid
      FROM hello.user_vmoney_changelog
      WHERE DAY='2019-07-29'
        AND vm_typeid IN(2,
                         5)
        AND TYPE!=3
      GROUP BY DAY) a
   LEFT JOIN
     (SELECT DAY,
             count(DISTINCT UID) DAU
      FROM hello.hello_daily_report
      WHERE DAY='2019-07-29'
      GROUP BY DAY) b ON a.cday=b.day
   LEFT JOIN
     (SELECT '2019-07-29' AS DAY,
             sum(b.vm_postcount) post_diamonds
      FROM
        (SELECT a.*,
                ROW_NUMBER() OVER (partition BY a.uid
                                   ORDER BY a.time DESC,a.vm_postcount DESC) rank
         FROM
           (SELECT UID,
                   vm_postcount,
                   vm_change,
                   ts,
                   cast(substr(ts,1,13) AS bigint) TIME
            FROM report_tb.hello_vmoney_no_partition_hzx
            WHERE DAY<='2019-07-29'
              AND vm_typeid=2) a) b
      WHERE b.rank=1
        AND b.vm_postcount>0) c ON a.cday=c.day) t1
LEFT JOIN
  (SELECT DAY,
          sum(if(TYPE=1
                 AND vm_typeid=2,vm_change,0))+sum(if(TYPE=60
                                                      AND vm_typeid=2,vm_change,0))+sum(if(TYPE=62
                                                                                           AND vm_typeid=5,vm_change,0)) total_diamond,
          sum(if(TYPE=1
                 AND vm_typeid=2,vm_change,0)) recharge_diamond,
          sum(if(TYPE=60
                 AND vm_typeid=2,vm_change,0)) exchange_diamond,
          sum(if(TYPE=62
                 AND vm_typeid=5,vm_change,0)) black_diamond
   FROM hello.user_vmoney_changelog
   WHERE DAY='2019-07-29'
     AND vm_typeid IN (2,
                       5)
     AND TYPE IN (1,
                  60,
                  62)
   GROUP BY DAY) t2 ON t1.cday=t2.day
LEFT JOIN
  (SELECT a1.*,
          b1.star_gift,
          b1.lucky_gift,
          b1.bag_gift,
          b1.ordinary_gift
   FROM
     (SELECT DAY,
             sum(if(vm_change>0,vm_change,0)) use_diamond,
             sum(if(TYPE=53,vm_change,0)) lottery_diamond,
             sum(if(TYPE=61,vm_change,0)) noble_diamond,
             sum(if(TYPE=50,vm_change,0)) car_diamond
      FROM hello.user_vmoney_changelog
      WHERE DAY='2019-07-29'
        AND vm_typeid IN (2,
                          5)
        AND TYPE NOT IN (3,
                         1,
                         60,
                         62)
      GROUP BY DAY) a1
   LEFT JOIN
     (SELECT DAY AS cday,
                    sum(if(type_name='周星礼物',money,0)) star_gift,
                    sum(if(type_name='幸运礼物',money,0)) lucky_gift,
                    sum(if(type_name='福袋礼物',money,0)) bag_gift,
                    sum(if(type_name='普通礼物',money,0)) ordinary_gift
      FROM
        (SELECT t_a.day,
                CASE
                    WHEN t_b.groupid=1 THEN '普通金币礼物'
                    WHEN t_b.groupid=2 THEN '普通钻石礼物'
                    WHEN t_b.groupid=3 THEN '幸运礼物'
                    WHEN t_b.groupid=4 THEN '极品礼物'
                    WHEN t_b.groupid=5 THEN '福袋礼物'
                    ELSE t_b.groupid
                END AS type_name,
                t_a.vgift_typeid,
                vm_count money
         FROM
           (SELECT from_uid,
                   DAY,
                   vgift_typeid,
                   vm_count
            FROM report_tb.hello_user_send_gift_zpj
            WHERE DAY='2019-07-29'
              AND status=1
              AND vm_typeid=2) t_a
         LEFT JOIN hello.virtual_gift_type t_b ON t_a.vgift_typeid=t_b.typeid
         WHERE t_b.groupid NOT IN (1,
                                   2)
         UNION ALL SELECT DAY,
                          CASE
                              WHEN superscript_id=1 THEN '周星礼物'
                              WHEN superscript_id=2 THEN '活动礼物'
                              WHEN superscript_id=3 THEN '年度礼物'
                              ELSE superscript_id
                          END AS type_name,
                          vgift_typeid,
                          money
         FROM
           (SELECT t_a.day,
                   if(cast(substr(t_a.ts,1,10) AS bigint) BETWEEN t_b.superscript_start_time AND t_b.superscript_end_time,t_b.superscript_id,0) superscript_id,
                   t_a.vgift_typeid,
                   t_a.from_uid,
                   vm_count money
            FROM
              (SELECT from_uid,
                      DAY,
                      vgift_typeid,
                      vm_count,
                      ts
               FROM report_tb.hello_user_send_gift_zpj
               WHERE DAY='2019-07-29'
                 AND status=1
                 AND vm_typeid=2) t_a
            LEFT JOIN hello.virtual_gift_type t_b ON t_a.vgift_typeid=t_b.typeid) ee
         WHERE superscript_id!=0
         UNION ALL SELECT DAY,
                          '普通礼物' AS type_name,
                          vgift_typeid,
                          money
         FROM
           (SELECT t_a.day,
                   if(cast(substr(t_a.ts,1,10) AS bigint) BETWEEN t_b.superscript_start_time AND t_b.superscript_end_time,t_b.superscript_id,0) superscript_id,
                   t_a.vgift_typeid,
                   t_b.name,
                   t_a.from_uid,
                   vm_count money
            FROM
              (SELECT from_uid,
                      DAY,
                      vgift_typeid,
                      vm_count,
                      ts
               FROM report_tb.hello_user_send_gift_zpj
               WHERE DAY='2019-07-29'
                 AND status=1
                 AND vm_typeid=2) t_a
            LEFT JOIN hello.virtual_gift_type t_b ON t_a.vgift_typeid=t_b.typeid
            WHERE t_b.groupid IN (1,
                                  2)) ee
         WHERE superscript_id=0) ttt
      GROUP BY DAY) b1 ON a1.day=b1.cday) t3 ON t1.cday=t3.day
LEFT JOIN
  (SELECT '2019-07-29' AS DAY,
          sum(ttt.vm_postcount) total_post_diamonds,
          sum(if(ttt.vm_typeid=2,ttt.vm_postcount,0)) post_diamonds2,
          sum(if(ttt.vm_typeid=5,ttt.vm_postcount,0)) black_diamonds
   FROM
     (SELECT *
      FROM
        (SELECT a.*,
                ROW_NUMBER() OVER (partition BY a.uid
                                   ORDER BY a.time DESC,a.vm_postcount DESC) rank
         FROM
           (SELECT vm_typeid,
                   UID,
                   vm_postcount,
                   vm_change,
                   ts,
                   cast(substr(ts,1,13) AS bigint) TIME
            FROM report_tb.hello_vmoney_no_partition_hzx
            WHERE DAY<='2019-07-29'
              AND vm_typeid=2) a) b
      WHERE b.rank=1
        AND b.vm_postcount>0
      UNION ALL SELECT *
      FROM
        (SELECT a.*,
                ROW_NUMBER() OVER (partition BY a.uid
                                   ORDER BY a.time DESC,a.vm_postcount DESC) rank
         FROM
           (SELECT vm_typeid,
                   UID,
                   vm_postcount,
                   vm_change,
                   ts,
                   cast(substr(ts,1,13) AS bigint) TIME
            FROM report_tb.hello_vmoney_no_partition_hzx
            WHERE DAY<='2019-07-29'
              AND vm_typeid=5) a) b
      WHERE b.rank=1
        AND b.vm_postcount>0) ttt) t4 ON t1.cday=t4.day;


SELECT t1.*,
       nvl(t2.total_diamond,0) total_diamond,
       nvl(t2.recharge_diamond,0) AS recharge_diamond2,
       nvl(t2.exchange_diamond,0) exchange_diamond,
       nvl(t2.black_diamond,0) black_diamond,
       nvl(t3.use_diamond,0) AS use_diamond2,
       nvl(t3.lottery_diamond,0) lottery_diamond,
       nvl(t3.noble_diamond,0) noble_diamond,
       nvl(t3.car_diamond,0) car_diamond,
       nvl(t3.star_gift,0) star_gift,
       nvl(t3.lucky_gift,0) lucky_gift,
       nvl(t3.bag_gift,0) bag_gift,
       nvl(t3.ordinary_gift,0) ordinary_gift,
       0 AS total_post_diamonds,
       0 AS post_diamonds2,
       0 AS black_diamonds,
       '2019-07-29' AS DAY
FROM
  (SELECT a.cday,
          a.appid,
          nvl(b.recharge_diamond,0) AS recharge_diamond,
          nvl(b.recharge_uid,0) recharge_uid,
          nvl(b.total_use_diamond,0) total_use_diamond,
          nvl(b.total_use_diamond_uid,0) total_use_diamond_uid,
          nvl(b.use_diamond,0) use_diamond,
          nvl(b.use_diamond_uid,0) use_diamond_uid,
          nvl(b.use_black_diamond,0) use_black_diamond,
          nvl(b.use_black_diamond_uid,0) use_black_diamond_uid,
          0 AS post_diamonds,
          nvl(a.dau,0) total_uid,
          nvl(b.recharge_uid/a.dau,0) r_recharge
   FROM
     (SELECT DAY cday,
                 appkey appid,
                 count(DISTINCT UID) DAU
      FROM hello.hello_daily_report
      WHERE DAY='2019-07-29'
        AND appkey IS NOT NULL
      GROUP BY DAY,
               appkey) a
   LEFT JOIN
     (SELECT DAY cday,
                 appid,
                 sum(if(TYPE=1,vm_change,0)) recharge_diamond,
                 count(DISTINCT if(TYPE IN (1,60),UID,NULL)) recharge_uid,
                 sum(if(vm_change<0,vm_change,0)) total_use_diamond,
                 count(DISTINCT if(vm_change<0,UID,NULL)) total_use_diamond_uid,
                 sum(if(vm_change<0
                        AND vm_typeid=2,vm_change,0)) use_diamond,
                 count(DISTINCT if(vm_change<0
                                   AND vm_typeid=2,UID,NULL)) use_diamond_uid,
                 sum(if(vm_change<0
                        AND vm_typeid=5,vm_change,0)) use_black_diamond,
                 count(DISTINCT if(vm_change<0
                                   AND vm_typeid=5,UID,NULL)) use_black_diamond_uid
      FROM hello.user_vmoney_changelog
      WHERE DAY='2019-07-29'
        AND vm_typeid IN(2,
                         5)
        AND TYPE!=3
      GROUP BY DAY,
               appid) b ON a.cday=b.cday
   AND a.appid=b.appid) t1
LEFT JOIN
  (SELECT DAY,
          appid,
          sum(if(TYPE=1
                 AND vm_typeid=2,vm_change,0))+sum(if(TYPE=60
                                                      AND vm_typeid=2,vm_change,0))+sum(if(TYPE=62
                                                                                           AND vm_typeid=5,vm_change,0)) total_diamond,
          sum(if(TYPE=1
                 AND vm_typeid=2,vm_change,0)) recharge_diamond,
          sum(if(TYPE=60
                 AND vm_typeid=2,vm_change,0)) exchange_diamond,
          sum(if(TYPE=62
                 AND vm_typeid=5,vm_change,0)) black_diamond
   FROM hello.user_vmoney_changelog
   WHERE DAY='2019-07-29'
     AND vm_typeid IN (2,
                       5)
     AND TYPE IN (1,
                  60,
                  62)
   GROUP BY DAY,
            appid) t2 ON t1.cday=t2.day
AND t1.appid=t2.appid
LEFT JOIN
  (SELECT a1.*,
          b1.star_gift,
          b1.lucky_gift,
          b1.bag_gift,
          b1.ordinary_gift
   FROM
     (SELECT DAY,
             appid,
             sum(if(vm_change>0,vm_change,0)) use_diamond,
             sum(if(TYPE=53,vm_change,0)) lottery_diamond,
             sum(if(TYPE=61,vm_change,0)) noble_diamond,
             sum(if(TYPE=50,vm_change,0)) car_diamond
      FROM hello.user_vmoney_changelog
      WHERE DAY='2019-07-29'
        AND vm_typeid IN (2,
                          5)
        AND TYPE NOT IN (3,
                         1,
                         60,
                         62)
      GROUP BY DAY,
               appid) a1
   LEFT JOIN
     (SELECT DAY AS cday,
                    appid,
                    sum(if(type_name='周星礼物',money,0)) star_gift,
                    sum(if(type_name='幸运礼物',money,0)) lucky_gift,
                    sum(if(type_name='福袋礼物',money,0)) bag_gift,
                    sum(if(type_name='普通礼物',money,0)) ordinary_gift
      FROM
        (SELECT t_a.day,
                t_a.appid,
                CASE
                    WHEN t_b.groupid=1 THEN '普通金币礼物'
                    WHEN t_b.groupid=2 THEN '普通钻石礼物'
                    WHEN t_b.groupid=3 THEN '幸运礼物'
                    WHEN t_b.groupid=4 THEN '极品礼物'
                    WHEN t_b.groupid=5 THEN '福袋礼物'
                    ELSE t_b.groupid
                END AS type_name,
                t_a.vgift_typeid,
                vm_count money
         FROM
           (SELECT from_uid,
                   DAY,
                   vgift_typeid,
                   vm_count,
                   appid
            FROM report_tb.hello_user_send_gift_zpj
            WHERE DAY='2019-07-29'
              AND status=1
              AND vm_typeid=2) t_a
         LEFT JOIN hello.virtual_gift_type t_b ON t_a.vgift_typeid=t_b.typeid
         WHERE t_b.groupid NOT IN (1,
                                   2)
         UNION ALL SELECT DAY,
                          appid,
                          CASE
                              WHEN superscript_id=1 THEN '周星礼物'
                              WHEN superscript_id=2 THEN '活动礼物'
                              WHEN superscript_id=3 THEN '年度礼物'
                              ELSE superscript_id
                          END AS type_name,
                          vgift_typeid,
                          money
         FROM
           (SELECT t_a.day,
                   t_a.appid,
                   if(cast(substr(t_a.ts,1,10) AS bigint) BETWEEN t_b.superscript_start_time AND t_b.superscript_end_time,t_b.superscript_id,0) superscript_id,
                   t_a.vgift_typeid,
                   t_a.from_uid,
                   vm_count money
            FROM
              (SELECT from_uid,
                      DAY,
                      vgift_typeid,
                      vm_count,
                      ts,
                      appid
               FROM report_tb.hello_user_send_gift_zpj
               WHERE DAY='2019-07-29'
                 AND status=1
                 AND vm_typeid=2) t_a
            LEFT JOIN hello.virtual_gift_type t_b ON t_a.vgift_typeid=t_b.typeid) ee
         WHERE superscript_id!=0
         UNION ALL SELECT DAY,
                          appid,
                          '普通礼物' AS type_name,
                          vgift_typeid,
                          money
         FROM
           (SELECT t_a.day,
                   t_a.appid,
                   if(cast(substr(t_a.ts,1,10) AS bigint) BETWEEN t_b.superscript_start_time AND t_b.superscript_end_time,t_b.superscript_id,0) superscript_id,
                   t_a.vgift_typeid,
                   t_b.name,
                   t_a.from_uid,
                   vm_count money
            FROM
              (SELECT from_uid,
                      DAY,
                      vgift_typeid,
                      vm_count,
                      ts,
                      appid
               FROM report_tb.hello_user_send_gift_zpj
               WHERE DAY='2019-07-29'
                 AND status=1
                 AND vm_typeid=2) t_a
            LEFT JOIN hello.virtual_gift_type t_b ON t_a.vgift_typeid=t_b.typeid
            WHERE t_b.groupid IN (1,
                                  2)) ee
         WHERE superscript_id=0) ttt
      GROUP BY DAY,
               appid) b1 ON a1.day=b1.cday
   AND a1.appid=b1.appid) t3 ON t1.cday=t3.day
AND t1.appid=t3.appid
UNION ALL
SELECT t1.*,
       t2.total_diamond,
       t2.recharge_diamond AS recharge_diamond2,
       t2.exchange_diamond,
       t2.black_diamond,
       t3.use_diamond AS use_diamond2,
       t3.lottery_diamond,
       t3.noble_diamond,
       t3.car_diamond,
       t3.star_gift,
       t3.lucky_gift,
       t3.bag_gift,
       t3.ordinary_gift,
       t4.total_post_diamonds,
       t4.post_diamonds2,
       t4.black_diamonds,
       '2019-07-29' AS DAY
FROM
  (SELECT a.*,
          c.post_diamonds,
          b.dau total_uid,
          a.recharge_uid/b.dau r_recharge
   FROM
     (SELECT DAY cday,
                 'all' AS appid,
                 sum(if(TYPE=1,vm_change,0)) recharge_diamond,
                 count(DISTINCT if(TYPE IN (1,60),UID,NULL)) recharge_uid,
                 sum(if(vm_change<0,vm_change,0)) total_use_diamond,
                 count(DISTINCT if(vm_change<0,UID,NULL)) total_use_diamond_uid,
                 sum(if(vm_change<0
                        AND vm_typeid=2,vm_change,0)) use_diamond,
                 count(DISTINCT if(vm_change<0
                                   AND vm_typeid=2,UID,NULL)) use_diamond_uid,
                 sum(if(vm_change<0
                        AND vm_typeid=5,vm_change,0)) use_black_diamond,
                 count(DISTINCT if(vm_change<0
                                   AND vm_typeid=5,UID,NULL)) use_black_diamond_uid
      FROM hello.user_vmoney_changelog
      WHERE DAY='2019-07-29'
        AND vm_typeid IN(2,
                         5)
        AND TYPE!=3
      GROUP BY DAY) a
   LEFT JOIN
     (SELECT DAY,
             count(DISTINCT UID) DAU
      FROM hello.hello_daily_report
      WHERE DAY='2019-07-29'
      GROUP BY DAY) b ON a.cday=b.day
   LEFT JOIN
     (SELECT '2019-07-29' AS DAY,
             sum(b.vm_postcount) post_diamonds
      FROM
        (SELECT a.*,
                ROW_NUMBER() OVER (partition BY a.uid
                                   ORDER BY a.time DESC,a.vm_postcount DESC) rank
         FROM
           (SELECT UID,
                   vm_postcount,
                   vm_change,
                   ts,
                   cast(substr(ts,1,13) AS bigint) TIME
            FROM report_tb.hello_vmoney_no_partition_hzx
            WHERE DAY<='2019-07-29'
              AND vm_typeid=2) a) b
      WHERE b.rank=1
        AND b.vm_postcount>0) c ON a.cday=c.day) t1
LEFT JOIN
  (SELECT DAY,
          sum(if(TYPE=1
                 AND vm_typeid=2,vm_change,0))+sum(if(TYPE=60
                                                      AND vm_typeid=2,vm_change,0))+sum(if(TYPE=62
                                                                                           AND vm_typeid=5,vm_change,0)) total_diamond,
          sum(if(TYPE=1
                 AND vm_typeid=2,vm_change,0)) recharge_diamond,
          sum(if(TYPE=60
                 AND vm_typeid=2,vm_change,0)) exchange_diamond,
          sum(if(TYPE=62
                 AND vm_typeid=5,vm_change,0)) black_diamond
   FROM hello.user_vmoney_changelog
   WHERE DAY='2019-07-29'
     AND vm_typeid IN (2,
                       5)
     AND TYPE IN (1,
                  60,
                  62)
   GROUP BY DAY) t2 ON t1.cday=t2.day
LEFT JOIN
  (SELECT a1.*,
          b1.star_gift,
          b1.lucky_gift,
          b1.bag_gift,
          b1.ordinary_gift
   FROM
     (SELECT DAY,
             sum(if(vm_change>0,vm_change,0)) use_diamond,
             sum(if(TYPE=53,vm_change,0)) lottery_diamond,
             sum(if(TYPE=61,vm_change,0)) noble_diamond,
             sum(if(TYPE=50,vm_change,0)) car_diamond
      FROM hello.user_vmoney_changelog
      WHERE DAY='2019-07-29'
        AND vm_typeid IN (2,
                          5)
        AND TYPE NOT IN (3,
                         1,
                         60,
                         62)
      GROUP BY DAY) a1
   LEFT JOIN
     (SELECT DAY AS cday,
                    sum(if(type_name='周星礼物',money,0)) star_gift,
                    sum(if(type_name='幸运礼物',money,0)) lucky_gift,
                    sum(if(type_name='福袋礼物',money,0)) bag_gift,
                    sum(if(type_name='普通礼物',money,0)) ordinary_gift
      FROM
        (SELECT t_a.day,
                CASE
                    WHEN t_b.groupid=1 THEN '普通金币礼物'
                    WHEN t_b.groupid=2 THEN '普通钻石礼物'
                    WHEN t_b.groupid=3 THEN '幸运礼物'
                    WHEN t_b.groupid=4 THEN '极品礼物'
                    WHEN t_b.groupid=5 THEN '福袋礼物'
                    ELSE t_b.groupid
                END AS type_name,
                t_a.vgift_typeid,
                vm_count money
         FROM
           (SELECT from_uid,
                   DAY,
                   vgift_typeid,
                   vm_count
            FROM report_tb.hello_user_send_gift_zpj
            WHERE DAY='2019-07-29'
              AND status=1
              AND vm_typeid=2) t_a
         LEFT JOIN hello.virtual_gift_type t_b ON t_a.vgift_typeid=t_b.typeid
         WHERE t_b.groupid NOT IN (1,
                                   2)
         UNION ALL SELECT DAY,
                          CASE
                              WHEN superscript_id=1 THEN '周星礼物'
                              WHEN superscript_id=2 THEN '活动礼物'
                              WHEN superscript_id=3 THEN '年度礼物'
                              ELSE superscript_id
                          END AS type_name,
                          vgift_typeid,
                          money
         FROM
           (SELECT t_a.day,
                   if(cast(substr(t_a.ts,1,10) AS bigint) BETWEEN t_b.superscript_start_time AND t_b.superscript_end_time,t_b.superscript_id,0) superscript_id,
                   t_a.vgift_typeid,
                   t_a.from_uid,
                   vm_count money
            FROM
              (SELECT from_uid,
                      DAY,
                      vgift_typeid,
                      vm_count,
                      ts
               FROM report_tb.hello_user_send_gift_zpj
               WHERE DAY='2019-07-29'
                 AND status=1
                 AND vm_typeid=2) t_a
            LEFT JOIN hello.virtual_gift_type t_b ON t_a.vgift_typeid=t_b.typeid) ee
         WHERE superscript_id!=0
         UNION ALL SELECT DAY,
                          '普通礼物' AS type_name,
                          vgift_typeid,
                          money
         FROM
           (SELECT t_a.day,
                   if(cast(substr(t_a.ts,1,10) AS bigint) BETWEEN t_b.superscript_start_time AND t_b.superscript_end_time,t_b.superscript_id,0) superscript_id,
                   t_a.vgift_typeid,
                   t_b.name,
                   t_a.from_uid,
                   vm_count money
            FROM
              (SELECT from_uid,
                      DAY,
                      vgift_typeid,
                      vm_count,
                      ts
               FROM report_tb.hello_user_send_gift_zpj
               WHERE DAY='2019-07-29'
                 AND status=1
                 AND vm_typeid=2) t_a
            LEFT JOIN hello.virtual_gift_type t_b ON t_a.vgift_typeid=t_b.typeid
            WHERE t_b.groupid IN (1,
                                  2)) ee
         WHERE superscript_id=0) ttt
      GROUP BY DAY) b1 ON a1.day=b1.cday) t3 ON t1.cday=t3.day
LEFT JOIN
  (SELECT '2019-07-29' AS DAY,
          sum(ttt.vm_postcount) total_post_diamonds,
          sum(if(ttt.vm_typeid=2,ttt.vm_postcount,0)) post_diamonds2,
          sum(if(ttt.vm_typeid=5,ttt.vm_postcount,0)) black_diamonds
   FROM
     (SELECT *
      FROM
        (SELECT a.*,
                ROW_NUMBER() OVER (partition BY a.uid
                                   ORDER BY a.time DESC,a.vm_postcount DESC) rank
         FROM
           (SELECT vm_typeid,
                   UID,
                   vm_postcount,
                   vm_change,
                   ts,
                   cast(substr(ts,1,13) AS bigint) TIME
            FROM report_tb.hello_vmoney_no_partition_hzx
            WHERE DAY<='2019-07-29'
              AND vm_typeid=2) a) b
      WHERE b.rank=1
        AND b.vm_postcount>0
      UNION ALL SELECT *
      FROM
        (SELECT a.*,
                ROW_NUMBER() OVER (partition BY a.uid
                                   ORDER BY a.time DESC,a.vm_postcount DESC) rank
         FROM
           (SELECT vm_typeid,
                   UID,
                   vm_postcount,
                   vm_change,
                   ts,
                   cast(substr(ts,1,13) AS bigint) TIME
            FROM report_tb.hello_vmoney_no_partition_hzx
            WHERE DAY<='2019-07-29'
              AND vm_typeid=5) a) b
      WHERE b.rank=1
        AND b.vm_postcount>0) ttt) t4 ON t1.cday=t4.day;
