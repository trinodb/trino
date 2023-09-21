# SHOW SESSION

## Synopsis

```text
SHOW SESSION [ LIKE pattern ]
```

## Description

List the current {ref}`session properties <session-properties-definition>`.

{ref}`Specify a pattern <like-operator>` in the optional `LIKE` clause to
filter the results to the desired subset. For example, the following query
allows you to find session properties that begin with `query`:

```
SHOW SESSION LIKE 'query%'
```

## See also

{doc}`reset-session`, {doc}`set-session`
