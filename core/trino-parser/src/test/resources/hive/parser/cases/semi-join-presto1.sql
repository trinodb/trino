INSERT overwrite tmp.client
SELECT UID,
       client_ip,
       '2019-11-24' DAY
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
  where a.UID IN
  (
    select b.UID FROM
    (
      SELECT DISTINCT UID
      FROM tmp.report
      WHERE DAY='2019-11-24'
    )b
  )
)