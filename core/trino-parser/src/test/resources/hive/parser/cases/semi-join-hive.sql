SELECT a.key, a.value
FROM a
LEFT SEMI
JOIN b
on (a.key = b.key)
where a.day='1'