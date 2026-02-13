# Row functions

Row functions use the [ROW type](row-type).
Create a row by explicitly casting the field names and types:

```sql
SELECT CAST(ROW('hello', 'world') AS ROW(greeting varchar, planet varchar))
-- ROW('hello', 'world')
```

Fields can be accessed via the dot fieldname:
```sql
SELECT CAST(ROW('hello', 'world') AS ROW(greeting varchar, planet varchar)).greeting
-- 'hello'
```

## Row functions

:::{function} transform(T, varchar, V, function(V, V)) -> T
With this function, a field in the row can be updated with the lambda function. 
The returned value is the original value with the updated field. The second 
argument is the name of the field to update. The third argument, `V` is a dummy
so the type of the function can be resolved. It can be any value, as long as the
type of the value is equal to the type of the argument and return type of the
lambda function.

```sql
SELECT transform(
    CAST(ROW('hello', 'world') AS ROW(greeting varchar, planet varchar))",
    'greeting', '', greeting -> concat(greeting, ' or goodbye'));
-- ROW('hello or goodbye', 'world')
```

The transform can be used to reach fields in nested rows or fields in rows 
in arrays:

```sql
SELECT transform(
    CAST(ROW(ROW('hello'), 'world') as ROW(greeting ROW(text varchar), planet varchar)),
    'greeting',
    CAST(ROW('') as ROW(text varchar)),
    greeting -> transform(
        greeting, 'text', '', text -> concat(text, ' or goodbye')));
-- ROW(ROW('hello or goodbye'), 'world')
```


```sql
SELECT transform(ARRAY[
    CAST(ROW('hello', 'world') AS ROW(greeting varchar, planet varchar)),
    CAST(ROW('hi', 'mars') AS ROW(greeting varchar, planet varchar))],
    data -> transform(data, 'greeting', '', greeting -> concat(greeting, ' or goodbye')));
-- ARRAY[ROW('hello or goodbye', 'world'), ROW('hi or goodbye', 'mars')]
```
:::
