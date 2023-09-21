# System access control

A system access control enforces authorization at a global level,
before any connector level authorization. You can use one of the built-in
implementations in Trino, or provide your own by following the guidelines in
{doc}`/develop/system-access-control`.

To use a system access control, add an `etc/access-control.properties` file
with the following content and the desired system access control name on all
cluster nodes:

```text
access-control.name=allow-all
```

Multiple system access control implementations may be configured at once
using the `access-control.config-files` configuration property. It should
contain a comma separated list of the access control property files to use
(rather than the default `etc/access-control.properties`).

Trino offers the following built-in system access control implementations:

```{eval-rst}
.. list-table::
  :widths: 20, 80
  :header-rows: 1

  * - Name
    - Description
  * - ``default``
    - All operations are permitted, except for user impersonation and triggering
      :doc:`/admin/graceful-shutdown`.

      This is the default access control if none are configured.
  * - ``allow-all``
    - All operations are permitted.
  * - ``read-only``
    - Operations that read data or metadata are permitted, but none of the
      operations that write data or metadata are allowed.
  * - ``file``
    - Authorization rules are specified in a config file. See
      :doc:`file-system-access-control`.
```

If you want to limit access on a system level in any other way than the ones
listed above, you must implement a custom {doc}`/develop/system-access-control`.

Access control must be configured on the coordinator. Authorization for
operations on specific worker nodes, such a triggering
{doc}`/admin/graceful-shutdown`, must also be configured on all workers.

## Read only system access control

This access control allows any operation that reads data or
metadata, such as `SELECT` or `SHOW`. Setting system level or catalog level
session properties is also permitted. However, any operation that writes data or
metadata, such as `CREATE`, `INSERT` or `DELETE`, is prohibited.
To use this access control, add an `etc/access-control.properties`
file with the following contents:

```text
access-control.name=read-only
```
