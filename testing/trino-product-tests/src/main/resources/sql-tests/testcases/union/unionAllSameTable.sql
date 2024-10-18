-- database: trino; tables: nation; groups: union;
SELECT *
FROM nation
UNION ALL
SELECT *
FROM nation
