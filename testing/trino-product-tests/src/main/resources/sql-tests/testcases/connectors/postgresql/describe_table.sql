-- database: trino; groups: postgresql,profile_specific_tests; queryType: SELECT; tables: postgres.public.workers_psql
--!
describe postgresql.public.workers_psql
--!
-- delimiter: |; trimValues: true; ignoreOrder: true;
id_employee        | integer     | | |
first_name         | varchar(32) | | |
last_name          | varchar(32) | | |
date_of_employment | date        | | |
department         | integer     | | |
id_department      | integer     | | |
name               | varchar(32) | | |
salary             | integer     | | |
