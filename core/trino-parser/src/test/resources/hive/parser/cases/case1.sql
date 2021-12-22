SELECT countrycode,
       business_type,
       item_type,
       if(times IS NULL,
                   0,
                   times)AS times,
       if(total_num IS NULL,
                       0,
                       total_num)AS total_num,
       '2019-07-31'AS stat_day
FROM
  (SELECT if(countrycode IS NULL,'ALL',countrycode)AS countrycode,
          if(business_type IS NULL,'ALL',business_type)AS business_type,
          item_type,
          sum(times)AS times,
          sum(total_num)AS total_num
   FROM
     (SELECT if(t2.countrycode like '%%a%%',t2.countrycode,'UNKNOWN')AS countrycode,
             business_type,
             item_type,
             sum(times)AS times,
             sum(total_num)AS total_num
      FROM
        (SELECT to_uid,
                business_type,
                item_type,
                count(to_uid)AS times,
                sum(item_count)AS total_num
         FROM mysql_tb.bigo_tbl_prize_distribute_v2_m
         WHERE DAY='2019-07-31'
           AND order_status=2
         GROUP BY to_uid,
                  business_type,
                  item_type)t1
      LEFT JOIN
        (SELECT UID,
                countrycode
         FROM bigolive.user_countrycode)t2 ON t1.to_uid=t2.uid
      GROUP BY countrycode,
               business_type,
               item_type)A
   GROUP BY countrycode,
            business_type,
            item_type WITH CUBE)B