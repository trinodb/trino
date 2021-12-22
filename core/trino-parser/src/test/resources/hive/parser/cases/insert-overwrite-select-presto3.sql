INSERT overwrite test2
SELECT start_time,
       uri,
       '20191112074000' as stats_time
FROM test1
