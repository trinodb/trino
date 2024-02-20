# Session information

Functions providing information about the query execution environment.

:::{data} current_user
Returns the current user running the query.
:::

:::{function} current_groups
Returns the list of groups for the current user running the query.
:::

:::{data} current_catalog
Returns a character string that represents the current catalog name.
:::

::::{data} current_schema
Returns a character string that represents the current unqualified schema name.

:::{note}
This is part of the SQL standard and does not use parenthesis.
:::
::::
