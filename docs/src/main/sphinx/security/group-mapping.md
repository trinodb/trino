# Group mapping

This section describes the group providers available in Trino to map usernames
onto groups for easier access control and resource group management.

Configure a group provider by creating an `etc/group-provider.properties` file on the coordinator:

```text
group-provider.name=provider-name
group-provider.group-case=keep
```
_The configuration of the chosen group provider should be appended to this file_

:::{list-table} Group provider configuration
:widths: 35, 50, 15
:header-rows: 1

* - Property Name
  - Description
  - Default
 
* - `group-provider.name`
  - Name of the group provider to use.
  -
* - `group-provider.group-case`
  - Optional transformation of the case of the group name.
    Supported values are:

    * `keep`: default, no conversion
    * `upper`: convert group name to _UPPERCASE_
    * `lower`: converts the group name to _lowercase_
  - `keep`
:::

The available group providers are:
```{toctree}
:maxdepth: 1

File group provider        <group-mapping/file>
```
