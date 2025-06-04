-- database: trino; groups: postgresql,profile_specific_tests; tables: postgres.public.workers_psql
--!
select t1.last_name, t2.first_name from postgresql.public.workers_psql t1, postgresql.public.workers_psql t2 where t1.id_department = t2.id_employee
--!
-- delimiter: |; trimValues: true; ignoreOrder: true;
null      | Mary|
Turner    | Ann|
Smith     | Ann|
null      | Martin|
Donne     | Joana|
Grant     | Kate|
Johnson   | Kate|
null      | Christopher|
Cage      | George|
Brown     | Jacob|
Black     | John|
null      | Charlie|
