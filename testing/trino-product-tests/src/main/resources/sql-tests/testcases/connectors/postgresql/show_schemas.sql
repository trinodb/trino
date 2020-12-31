-- database: presto; groups: postgresql,profile_specific_tests; queryType: SELECT;
--!
show schemas from postgresql
--!
-- delimiter: |; trimValues: true; ignoreOrder: true;
information_schema|
pg_catalog|
public|
