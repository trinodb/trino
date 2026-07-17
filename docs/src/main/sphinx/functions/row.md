# Row functions
Functions for the [ROW](/language/types) data type.


:::{function} ROW::fields(data) -> array(varchar)
Returns all the field names of the `data` ROW.
```sql
SELECT ROW::fields(row('hello' as greeting, 'world' as planet));
-- ['GREETING', 'PLANET']
```
:::
