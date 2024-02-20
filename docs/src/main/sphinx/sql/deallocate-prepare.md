# DEALLOCATE PREPARE

## Synopsis

```text
DEALLOCATE PREPARE statement_name
```

## Description

Removes a statement with the name `statement_name` from the list of prepared
statements in a session.

## Examples

Deallocate a statement with the name `my_query`:

```
DEALLOCATE PREPARE my_query;
```

## See also

{doc}`prepare`, {doc}`execute`, {doc}`execute-immediate`
