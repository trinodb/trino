-- database: trino; tables: nation; groups: union;
SELECT *
FROM nation
UNION DISTINCT
SELECT *
FROM nation
