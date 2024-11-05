-- database: trino; groups: aggregate; tables: datatype
select avg(c_bigint), avg(c_double) from datatype
