SELECT t4.post_id,
       t4.detail_id,
       t4.type,
       coalesce(t2.like_count,
                0) AS like_count,
       coalesce(t2.play_count,
                0) AS play_count
FROM
  (SELECT post_id,
          like_count,
          play_count
   FROM mysql_tb.welog_tbl_video_counter)t2
JOIN
  (SELECT post_id,
          detail_id,
          TYPE
   FROM
     (SELECT post_id,
             d3_magic AS detail_id,
             'd3_magic' AS TYPE
      FROM mysql_tb.welog_tbl_video_option_data_4rec
      WHERE (d3_magic IS NOT NULL
             AND d3_magic!='0'
             AND d3_magic!='')
        OR (get_json_object(other_value,'$.d4_bg') IS NOT NULL
            AND get_json_object(other_value,'$.d4_bg')!='0'
            AND get_json_object(other_value,'$.d4_bg')!='')
      UNION ALL SELECT post_id,
                       get_json_object(other_value,'$.d4_bg') AS detail_id,
                       'd4_bg' AS TYPE
      FROM mysql_tb.welog_tbl_video_option_data_4rec
      WHERE (get_json_object(other_value,'$.d4_bg') IS NOT NULL
             AND get_json_object(other_value,'$.d4_bg')!='0'
             AND get_json_object(other_value,'$.d4_bg')!='')
      UNION ALL SELECT post_id,
                       d3_magic AS detail_id,
                       'no_d4_bg' AS TYPE
      FROM mysql_tb.welog_tbl_video_option_data_4rec
      WHERE (d3_magic IS NOT NULL
             AND d3_magic!='0'
             AND d3_magic!='')
        AND (get_json_object(other_value,'$.d4_bg') IS NULL
             OR get_json_object(other_value,'$.d4_bg')='0'
             OR get_json_object(other_value,'$.d4_bg')='')
      UNION ALL SELECT post_id,
                       magic AS detail_id,
                       'magic' AS TYPE
      FROM mysql_tb.welog_tbl_video_option_data_4rec
      WHERE magic IS NOT NULL
        AND magic!='0'
        AND magic!=''
      UNION ALL SELECT post_id,
                       effect AS detail_id,
                       'effect' AS TYPE
      FROM mysql_tb.welog_tbl_video_option_data_4rec
      WHERE effect IS NOT NULL
        AND effect!='0'
        AND effect<>''
      UNION ALL SELECT post_id,
                       boom AS detail_id,
                       'boom' AS TYPE
      FROM mysql_tb.welog_tbl_video_option_data_4rec
      WHERE boom IS NOT NULL
        AND boom!='0'
        AND boom!=''
      UNION ALL SELECT post_id,
                       get_json_object(other_value,'$.sticker') AS detail_id,
                       'sticker' AS TYPE
      FROM mysql_tb.welog_tbl_video_option_data_4rec
      WHERE get_json_object(other_value,'$.sticker') IS NOT NULL
        AND get_json_object(other_value,'$.sticker')!='-1'
        AND get_json_object(other_value,'$.sticker')!=''
      UNION ALL SELECT post_id,
                       get_json_object(other_value,'$.music_magic') AS detail_id,
                       'music_magic' AS TYPE
      FROM mysql_tb.welog_tbl_video_option_data_4rec
      WHERE get_json_object(other_value,'$.music_magic') IS NOT NULL
        AND get_json_object(other_value,'$.music_magic')!='-1'
        AND get_json_object(other_value,'$.music_magic')!='0'
        AND get_json_object(other_value,'$.music_magic')!=''
      UNION ALL SELECT video_id AS post_id,
                       coalesce(cast(music_id AS STRING),0) AS detail_id,
                       'dialogue' AS TYPE
      FROM algo.welog_video_comdedy_list
      WHERE DAY='2019-07-29'
      UNION ALL SELECT post_id,
                       get_json_object(other_value,'$.body_magic') AS detail_id,
                       'body_magic' AS TYPE
      FROM mysql_tb.welog_tbl_video_option_data_4rec
      WHERE get_json_object(other_value,'$.body_magic') IS NOT NULL
        AND get_json_object(other_value,'$.body_magic')!='-1'
        AND get_json_object(other_value,'$.body_magic')!=''
        AND get_json_object(other_value,'$.body_magic')!='0'
      UNION ALL SELECT post_id,
                       get_json_object(other_value,'$.slim') AS detail_id,
                       'slim' AS TYPE
      FROM mysql_tb.welog_tbl_video_option_data_4rec
      WHERE get_json_object(other_value,'$.slim') IS NOT NULL
        AND get_json_object(other_value,'$.slim')='1'
      UNION ALL SELECT post_id,
                       get_json_object(other_value,'$.slimvs') AS detail_id,
                       'slimv_duet' AS TYPE
      FROM mysql_tb.welog_tbl_video_option_data_4rec
      WHERE get_json_object(other_value,'$.slimvs') IS NOT NULL
        AND get_json_object(other_value,'$.slimvs')='1'
      UNION ALL SELECT post_id,
                       get_json_object(other_value,'$.duet_video_id') AS detail_id,
                       'duet' AS TYPE
      FROM mysql_tb.welog_tbl_video_option_data_4rec
      WHERE get_json_object(other_value,'$.duet_video_id') IS NOT NULL
        AND get_json_object(other_value,'$.duet_video_id')<>'0'
      UNION ALL SELECT cast(get_json_object(other_value,'$.duet_video_id') AS bigint) AS post_id,
                       cast(post_id AS string) AS detail_id,
                       'by_duet' AS TYPE
      FROM mysql_tb.welog_tbl_video_option_data_4rec
      WHERE get_json_object(other_value,'$.duet_video_id') IS NOT NULL
        AND get_json_object(other_value,'$.duet_video_id')<>'0'
      UNION ALL SELECT post_id,
                       get_json_object(other_value,'$.filter') AS detail_id,
                       'filter' AS TYPE
      FROM mysql_tb.welog_tbl_video_option_data_4rec
      WHERE get_json_object(other_value,'$.filter') IS NOT NULL
        AND get_json_object(other_value,'$.filter') != ''
        AND get_json_object(other_value,'$.filter') != '-1'
      UNION ALL SELECT post_id,
                       get_json_object(other_value,'$.cut_me_id') AS cutme_id,
                       'cutme' AS TYPE
      FROM mysql_tb.welog_tbl_video_option_data_4rec
      WHERE get_json_object(other_value,'$.cut_me_id') IS NOT NULL
        AND get_json_object(other_value,'$.cut_me_id')<>0 )t3
   GROUP BY post_id,
            detail_id,
            TYPE)t4 ON t4.post_id = t2.post_id