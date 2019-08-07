SELECT DAY,
       countrycode,
       os,
       client_version,
       reason,
       SOURCE,
       live_type
FROM
  (SELECT DAY,
          countrycode1 AS countrycode,
          os1 AS os,
          client_version1 AS client_version,
          reason1 AS reason,
          source1 AS SOURCE,
          live_type1 AS live_type
   FROM tmp.bigolive_lx_purchasae_funnel_step1
   UNION ALL SELECT DAY,
                    countrycode2 AS countrycode,
                    os2 AS os,
                    client_version2 AS client_version,
                    reason2 AS reason,
                    source2 AS SOURCE,
                    live_type2 AS live_type
   FROM tmp.bigolive_lx_purchasae_funnel_step3
   UNION ALL SELECT DAY,
                    countrycode3 AS countrycode,
                    os3 AS os,
                    client_version3 AS client_version,
                    reason3 AS reason,
                    source3 AS SOURCE,
                    live_type3 AS live_type
   FROM tmp.bigolive_lx_purchasae_funnel_step4
   UNION ALL SELECT DAY,
                    countrycode4 AS countrycode,
                    os4 AS os,
                    client_version4 AS client_version,
                    reason4 AS reason,
                    source4 AS SOURCE,
                    live_type4 AS live_type
   FROM tmp.bigolive_lx_purchasae_funnel_step5
   UNION ALL SELECT DAY,
                    countrycode5 AS countrycode,
                    os5 AS os,
                    client_version5 AS client_version,
                    reason5 AS reason,
                    source5 AS SOURCE,
                    live_type5 AS live_type
   FROM tmp.bigolive_lx_purchasae_funnel_step7
   UNION ALL SELECT DAY,
                    countrycode6 AS countrycode,
                    os6 AS os,
                    client_version6 AS client_version,
                    reason6 AS reason,
                    source6 AS SOURCE,
                    live_type6 AS live_type
   FROM tmp.bigolive_lx_purchasae_funnel_step8)t
GROUP BY DAY,
         countrycode,
         os,
         client_version,
         reason,
         SOURCE,
         live_type;


SELECT DAY,
       countrycode,
       os,
       client_version,
       reason,
       SOURCE,
       live_type
FROM
  (SELECT DAY,
          countrycode1 AS countrycode,
          os1 AS os,
          client_version1 AS client_version,
          reason1 AS reason,
          source1 AS SOURCE,
          live_type1 AS live_type
   FROM tmp.bigolive_lx_purchasae_funnel_step1
   UNION ALL SELECT DAY,
                    countrycode2 AS countrycode,
                    os2 AS os,
                    client_version2 AS client_version,
                    reason2 AS reason,
                    source2 AS SOURCE,
                    live_type2 AS live_type
   FROM tmp.bigolive_lx_purchasae_funnel_step3
   UNION ALL SELECT DAY,
                    countrycode3 AS countrycode,
                    os3 AS os,
                    client_version3 AS client_version,
                    reason3 AS reason,
                    source3 AS SOURCE,
                    live_type3 AS live_type
   FROM tmp.bigolive_lx_purchasae_funnel_step4
   UNION ALL SELECT DAY,
                    countrycode4 AS countrycode,
                    os4 AS os,
                    client_version4 AS client_version,
                    reason4 AS reason,
                    source4 AS SOURCE,
                    live_type4 AS live_type
   FROM tmp.bigolive_lx_purchasae_funnel_step5
   UNION ALL SELECT DAY,
                    countrycode5 AS countrycode,
                    os5 AS os,
                    client_version5 AS client_version,
                    reason5 AS reason,
                    source5 AS SOURCE,
                    live_type5 AS live_type
   FROM tmp.bigolive_lx_purchasae_funnel_step7
   UNION ALL SELECT DAY,
                    countrycode6 AS countrycode,
                    os6 AS os,
                    client_version6 AS client_version,
                    reason6 AS reason,
                    source6 AS SOURCE,
                    live_type6 AS live_type
   FROM tmp.bigolive_lx_purchasae_funnel_step8)t
GROUP BY DAY,
         countrycode,
         os,
         client_version,
         reason,
         SOURCE,
         live_type