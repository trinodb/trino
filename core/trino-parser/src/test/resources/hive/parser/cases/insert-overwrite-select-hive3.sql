INSERT overwrite TABLE test2
partition(stats_time='20191112074000')
SELECT start_time,
       uri
FROM test1