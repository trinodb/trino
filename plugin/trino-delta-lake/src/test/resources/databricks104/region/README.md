Data generated using Databricks 10.4:

```sql
CREATE TABLE default.region (regionkey bigint, name string, comment string)
USING DELTA LOCATION 's3://trino-ci-test/default/region';

INSERT INTO default.region VALUES 
(0, 'AFRICA', 'lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to '),
(1, 'AMERICA', 'hs use ironic, even requests. s'),
(2, 'ASIA', 'ges. thinly even pinto beans ca'),
(3, 'EUROPE', 'ly final courts cajole furiously final excuse'),
(4, 'MIDDLE EAST', 'uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl');
```
