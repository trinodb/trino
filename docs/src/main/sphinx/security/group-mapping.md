# Group mapping

Group providers in Trino to map usernames onto groups for easier access control
and resource group management.

Configure a group provider by creating an `etc/group-provider.properties` file
on the coordinator:

```properties
group-provider.name=file
```
_The configuration of the chosen group provider should be appended to this
file_

:::{list-table} Group provider configuration
:widths: 40, 60
:header-rows: 1

* - Property name
  - Description

* - `group-provider.name`
  - Name of the group provider to use.
    Supported values are:

      * `file`: [See configuration](file-group-provider)
      * `ldap`
* - `group-provider.group-case`
  - Optional transformation of the case of the group name.
    Supported values are:

    * `keep`: default, no conversion
    * `upper`: convert group name to _UPPERCASE_
    * `lower`: converts the group name to _lowercase_

    Defaults to `keep`.
:::

(file-group-provider)=
## File group provider

Group file resolves group membership using a file on the coordinator.

### Group file configuration

Enable group file by creating an `etc/group-provider.properties`
file on the coordinator:

```properties
group-provider.name=file
file.group-file=/path/to/group.txt
```

The following configuration properties are available:

:::{list-table} File group provider configuration
:widths: 40, 60
:header-rows: 1

* - Property name
  - Description

* - `file.group-file`
  - Path of the group file
* - `file.refresh-period`
  - [Duration](prop-type-duration) between refreshing the group mapping
    configuration from the file.
    Defaults to `5s`.
  :::

### Group file format

The group file contains a list of groups and members, one per line,
separated by a colon. Users are separated by a comma.

```text
group_name:user_1,user_2,user_3
```

