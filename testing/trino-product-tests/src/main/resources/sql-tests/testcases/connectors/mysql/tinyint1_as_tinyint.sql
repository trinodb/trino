-- database: trino; groups: mysql,profile_specific_tests; tables: mysql.test.workers_mysql
--!
select * from mysql.test.workers_mysql where department = 2
--!
-- delimiter: |; trimValues: true; ignoreOrder: true;
2|Ann|Turner|2000-05-28|2|2|R&D|5000
3|Martin|Smith|2000-05-28|2|2|R&D|5000
