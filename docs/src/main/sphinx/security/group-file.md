# File group provider

Trino can map user names onto groups for easier access control and
resource group management. Group file resolves group membership using
a file on the coordinator.

## Group file configuration

Enable group file by creating an `etc/group-provider.properties`
file on the coordinator:

```text
group-provider.name=file
file.group-file=/path/to/group.txt
```

The following configuration properties are available:

| Property              | Description                                           |
| --------------------- | ----------------------------------------------------- |
| `file.group-file`     | Path of the group file.                               |
| `file.refresh-period` | How often to reload the group file. Defaults to `5s`. |

## Group files

### File format

The group file contains a list of groups and members, one per line,
separated by a colon. Users are separated by a comma.

```text
group_name:user_1,user_2,user_3
```
