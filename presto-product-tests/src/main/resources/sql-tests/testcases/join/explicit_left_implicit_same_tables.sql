-- database: presto; groups: join; tables: nation, part
SELECT p_partkey,
       n_name
FROM   part
       LEFT JOIN nation
              ON n_nationkey = p_partkey
WHERE  n_name < p_name 

