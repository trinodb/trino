create table  test.tbl (
    broadcaster_id bigint,
    eff_day string,
    br_time bigint,
    day_rank bigint,
    bean_uv bigint,
    bean_cnt bigint,
    follow_uv bigint,
    enter_uv bigint,
    stay_time bigint,
    effective_stay_time bigint,
    mai_uv bigint,
    mai_time bigint
)
partitioned by (day string)
stored as orc tblproperties ('orc.compress'='SNAPPY')
