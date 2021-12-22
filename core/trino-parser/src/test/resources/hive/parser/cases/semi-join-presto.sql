SELECT a.key, a.value
FROM a
WHERE a.key
in
(
  SELECT b.key
  FROM b
)
and a.day='1'