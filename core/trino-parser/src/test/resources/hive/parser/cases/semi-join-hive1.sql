INSERT overwrite TABLE tmp.client partition (DAY='2019-11-24')
SELECT UID,
       client_ip
FROM tmp.record
WHERE DAY='2019-11-24'
AND UID NOT IN
(
  SELECT DISTINCT UID
  FROM
  (
    SELECT DISTINCT UID
    FROM tmp.record
    WHERE DAY='2019-11-24'
  )a 
  LEFT semi
  JOIN
  (
    SELECT DISTINCT UID
    FROM tmp.report
    WHERE DAY='2019-11-24'
  )b 
  on(a.UID=b.UID)
)