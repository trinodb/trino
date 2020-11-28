-- database: presto; groups: mysql,profile_specific_tests; queryType: SELECT;
--!
show schemas from mysql
--!
-- delimiter: |; trimValues: true; ignoreOrder: true; ignoreExcessRows: true;
test|
information_schema|
