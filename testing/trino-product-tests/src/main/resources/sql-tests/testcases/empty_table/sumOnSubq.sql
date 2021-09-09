-- database: presto; tables: empty_table; groups: empty;
SELECT SUM(cnt) FROM (SELECT COUNT(*) AS cnt FROM empty_table) foo
