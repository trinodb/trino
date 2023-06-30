Data generated using Databricks 10.4 LTS:

```sql
CREATE TABLE nested_fields (id INT, parent STRUCT<child1: BIGINT, child2: STRING>, grandparent STRUCT<parent1 STRUCT<child1: DOUBLE, child2: BOOLEAN>, parent2 STRUCT<child1: DATE>>) USING DELTA LOCATION 's3://starburst-test/nested_fields';

INSERT INTO nested_fields VALUES
    (1, struct('100', 'INDIA'), struct(struct(10.99, true), struct('2023-01-01'))),
    (2, struct('200', 'POLAND'), struct(struct(20.99, false), struct('2023-02-01'))),
    (3, struct('300', 'USA'), struct(struct(30.99, true), struct('2023-03-01'))),
    (4, struct('400', 'AUSTRIA'), struct(struct(40.99, false), struct('2023-04-01'))),
    (5, struct('500', 'JAPAN'), struct(struct(50.99, true), struct('2023-05-01'))),
    (6, struct('600', 'UK'), struct(struct(60.99, false), struct('2023-06-01'))),
    (7, struct('700', 'FRANCE'), struct(struct(70.99, true), struct('2023-07-01'))),
    (8, struct('800', 'BRAZIL'), struct(struct(80.99, false), struct('2023-08-01'))),
    (9, struct('900', 'NAMIBIA'), struct(struct(90.99, true), struct('2023-09-01'))),
    (10, struct('1000', 'RSA'), struct(struct(100.99, false), struct('2023-10-01')));
```