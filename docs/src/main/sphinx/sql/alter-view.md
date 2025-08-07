# ALTER VIEW

## Synopsis

```text
ALTER VIEW name RENAME TO new_name
ALTER VIEW name REFRESH
ALTER VIEW name SET AUTHORIZATION ( user | USER user | ROLE role )
```

## Description

Change the definition of an existing view.

## Examples

Rename view `people` to `users`:

```
ALTER VIEW people RENAME TO users
```

Refresh view `people`:

```
ALTER VIEW people REFRESH
```

Change owner of VIEW `people` to user `alice`:

```
ALTER VIEW people SET AUTHORIZATION alice
```

## See also

{doc}`create-view`
