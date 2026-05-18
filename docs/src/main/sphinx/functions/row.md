# Row functions and operators

A ROW is a complex data type that groups multiple named fields into a single 
value, similar to:

* A struct in C or Hive
* An object in JSON
* A record in many databases

You can think of it as "a mini table row inside one column".
```sql
ROW(name VARCHAR, age INTEGER)
```

A structured object with named fields can be created with a cast:
```sql
SELECT CAST(ROW('Alice', 30) AS ROW(name VARCHAR, age INTEGER));
```

ROWs can contain other ROWs:
```sql
ROW(
  name VARCHAR,
  address ROW(
    city VARCHAR,
    zip INTEGER
  )
)
```

## Row functions

:::{function} row.fields() -> array(varchar)
Returns all the field names of the current row instance.
```sql
SELECT CAST(NULL AS ROW(greeting VARCHAR, planet ROW(color varchar, size bigint))).fields();
-- ['greeting', 'planet']
```
:::
