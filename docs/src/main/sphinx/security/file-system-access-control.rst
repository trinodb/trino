==========================
File system access control
==========================

This access control plugin allows you to specify authorization rules in a JSON file.

Configuration
-------------

To use this plugin, add an ``etc/access-control.properties`` file containing two
required properties: ``access-control.name``, which must be equal to ``file``, and
``security.config-file``, which must be equal to the location of the config file.
For example, if a config file named ``rules.json``
resides in ``etc``, add an ``etc/access-control.properties`` with the following
contents:

.. code-block:: text

   access-control.name=file
   security.config-file=etc/rules.json

The config file is specified in JSON format. It contains rules that define
which users have access to which resources. The rules are read from top to bottom
and the first matching rule is applied. If no rule matches, access is denied.

Refresh
--------

By default, when a change is made to the JSON rules file, Trino must be restarted
to load the changes. There is an optional property to refresh the properties without requiring a
Trino restart. The refresh period is specified in the ``etc/access-control.properties``:

.. code-block:: text

   security.refresh-period=1s

Catalog, schema, and table access
---------------------------------

Access to catalogs, schemas, tables, and views is controlled by the catalog, schema, and table
rules.  The catalog rules are course grained rules used to restrict all access or write
access to catalogs. They do not explicitly grant any specific schema or table permissions.
The table and schema rules are used to specify who can create, drop, alter, select, insert,
delete, etc. for schemas and tables.

.. note::

    These rules do not apply to system defined table in the ``information_schema`` schema.

For each rule set, permission is based on the first matching rule read from top to bottom.  If
no rule matches, access is denied. If no rules are provided at all, then access is granted.

The following table summarizes the permissions required for each SQL command:

==================================== ========== ======= ==================== ===================================================
SQL Command                          Catalog    Schema  Table                Note
==================================== ========== ======= ==================== ===================================================
SHOW CATALOGS                                                                Always allowed
SHOW SCHEMAS                         read-only  any*    any*                 Allowed if catalog is :ref:`visible<visibility>`
SHOW TABLES                          read-only  any*    any*                 Allowed if schema :ref:`visible<visibility>`
CREATE SCHEMA                        read-only  owner
DROP SCHEMA                          all        owner
SHOW CREATE SCHEMA                   all        owner
ALTER SCHEMA ... RENAME TO           all        owner*                       Ownership is required on both old and new schemas
ALTER SCHEMA ... SET AUTHORIZATION   all        owner
CREATE TABLE                         all                owner
DROP TABLE                           all                owner
ALTER TABLE ... RENAME TO            all                owner*               Ownership is required on both old and new tables
ALTER TABLE ... SET PROPERTIES       all                owner
CREATE VIEW                          all                owner
DROP VIEW                            all                owner
ALTER VIEW ... RENAME TO             all                owner*               Ownership is required on both old and new views
COMMENT ON TABLE                     all                owner
COMMENT ON COLUMN                    all                owner
ALTER TABLE ... ADD COLUMN           all                owner
ALTER TABLE ... DROP COLUMN          all                owner
ALTER TABLE ... RENAME COLUMN        all                owner
SHOW COLUMNS                         all                any
SELECT FROM table                    read-only          select
SELECT FROM view                     read-only          select, grant_select
INSERT INTO                          all                insert
DELETE FROM                          all                delete
==================================== ========== ======= ==================== ===================================================

.. _visibility:

Visibility
^^^^^^^^^^

For a catalog, schema, or table to be visible in a ``SHOW`` command, the user must have
at least one permission on the item or any nested item. The nested items do not
need to already exist as any potential permission makes the item visible. Specifically:

* catalog: Visible if user is the owner of any nested schema, has permissions on any nested
  table, or has permissions to set session properties in the catalog.
* schema: Visible if the user is the owner of the schema, or has permissions on any nested table.
* table: Visible if the user has any permissions on the table.

Catalog rules
^^^^^^^^^^^^^

Each catalog rule is composed of the following fields:

* ``user`` (optional): regex to match against user name. Defaults to ``.*``.
* ``role`` (optional): regex to match against role names. Defaults to ``.*``.
* ``group`` (optional): regex to match against group names. Defaults to ``.*``.
* ``catalog`` (optional): regex to match against catalog name. Defaults to ``.*``.
* ``allow`` (required): string indicating whether a user has access to the catalog.
  This value can be ``all``, ``read-only`` or ``none``, and defaults to ``none``.
  Setting this value to ``read-only`` has the same behavior as the ``read-only``
  system access control plugin.

In order for a rule to apply the user name must match the regular expression
specified in ``user`` attribute.

For role names, a rule can be applied if at least one of the currently enabled roles
matches the ``role`` regular expression.

For group names, a rule can be applied if at least one group name of this user
matches the ``group`` regular expression.

The ``all`` value for ``allow`` means these rules do not restrict access in any way,
but the schema and table rules can restrict access.

.. note::

    By default, all users have access to the ``system`` catalog. You can
    override this behavior by adding a rule.

    Boolean ``true`` and ``false`` are also supported as legacy values for ``allow``,
    to support backwards compatibility.  ``true`` maps to ``all``, and ``false`` maps to ``none``.

For example, if you want to allow only the role ``admin`` to access the
``mysql`` and the ``system`` catalog, allow users from the ``finance``
and ``human_resources`` groups access to ``postgres`` catalog, allow all users to
access the ``hive`` catalog, and deny all other access, you can use the
following rules:

.. code-block:: json

    {
      "catalogs": [
        {
          "role": "admin",
          "catalog": "(mysql|system)",
          "allow": "all"
        },
        {
          "group": "finance|human_resources",
          "catalog": "postgres",
          "allow": true
        },
        {
          "catalog": "hive",
          "allow": "all"
        },
        {
          "user": "alice",
          "catalog": "postgresql",
          "allow": "read-only"
        },
        {
          "catalog": "system",
          "allow": "none"
        }
      ]
    }

For group-based rules to match, users need to be assigned to groups by a
:doc:`/develop/group-provider`.

Schema rules
^^^^^^^^^^^^

Each schema rule is composed of the following fields:

* ``user`` (optional): regex to match against user name. Defaults to ``.*``.
* ``role`` (optional): regex to match against role names. Defaults to ``.*``.
* ``group`` (optional): regex to match against group names. Defaults to ``.*``.
* ``catalog`` (optional): regex to match against catalog name. Defaults to ``.*``.
* ``schema`` (optional): regex to match against schema name. Defaults to ``.*``.
* ``owner`` (required): boolean indicating whether the user is to be considered
  an owner of the schema. Defaults to ``false``.

For example, to provide ownership of all schemas to role ``admin``, treat all
users as owners of the ``default.default`` schema and prevent user ``guest`` from
ownership of any schema, you can use the following rules:

.. code-block:: json

    {
      "schemas": [
        {
          "role": "admin",
          "schema": ".*",
          "owner": true
        },
        {
          "user": "guest",
          "owner": false
        },
        {
          "catalog": "default",
          "schema": "default",
          "owner": true
        }
      ]
    }

Table rules
^^^^^^^^^^^

Each table rule is composed of the following fields:

* ``user`` (optional): regex to match against user name. Defaults to ``.*``.
* ``role`` (optional): regex to match against role names. Defaults to ``.*``.
* ``group`` (optional): regex to match against group names. Defaults to ``.*``.
* ``catalog`` (optional): regex to match against catalog name. Defaults to ``.*``.
* ``schema`` (optional): regex to match against schema name. Defaults to ``.*``.
* ``table`` (optional): regex to match against table names. Defaults to ``.*``.
* ``privileges`` (required): zero or more of ``SELECT``, ``INSERT``,
  ``DELETE``, ``OWNERSHIP``, ``GRANT_SELECT``
* ``columns`` (optional): list of column constraints.
* ``filter`` (optional): boolean filter expression for the table.
* ``filter_environment`` (optional): environment use during filter evaluation.

Column constraint
^^^^^^^^^^^^^^^^^

These constraints can be used to restrict access to column data.

* ``name``: name of the column.
* ``allow`` (optional): if false, column can not be accessed.
* ``mask`` (optional): mask expression applied to column.
* ``mask_environment`` (optional): environment use during mask evaluation.

Filter and mask environment
^^^^^^^^^^^^^^^^^^^^^^^^^^^

* ``user`` (optional): username for checking permission of subqueries in mask.

.. note::

    These rules do not apply to ``information_schema``.

The example below defines the following table access policy:

* Role ``admin`` has all privileges across all tables and schemas
* User ``banned_user`` has no privileges
* All users have ``SELECT`` privileges on ``default.hr.employees``, but the
  table is filtered to only the row for the current user.
* All users have ``SELECT`` privileges on all tables in the ``default.default``
  schema, except for the ``address`` column which is blocked, and ``ssn`` which is masked.

.. code-block:: json

    {
      "tables": [
        {
          "role": "admin",
          "privileges": ["SELECT", "INSERT", "DELETE", "OWNERSHIP"]
        },
        {
          "user": "banned_user",
          "privileges": []
        },
        {
          "catalog": "default",
          "schema": "hr",
          "table": "employee",
          "privileges": ["SELECT"],
          "filter": "user = current_user",
          "filter_environment": {
            "user": "system_user"
          }
        },
        {
          "catalog": "default",
          "schema": "default",
          "table": ".*",
          "privileges": ["SELECT"],
          "columns" : [
             {
                "name": "address",
                "allow": false
             },
             {
                "name": "SSN",
                "mask": "'XXX-XX-' + substring(credit_card, -4)",
                "mask_environment": {
                  "user": "system_user"
                }
             }
          ]
        }
      ]
    }

.. _session_property_rules:

Session property rules
----------------------

These rules control the ability of a user to set system and catalog session properties. The
user is granted or denied access, based on the first matching rule, read from top to bottom.
If no rules are specified, all users are allowed set any session property. If no rule matches,
setting the session property is denied. System session property rules are composed of the
following fields:

* ``user`` (optional): regex to match against user name. Defaults to ``.*``.
* ``role`` (optional): regex to match against role names. Defaults to ``.*``.
* ``group`` (optional): regex to match against group names. Defaults to ``.*``.
* ``property`` (optional): regex to match against the property name. Defaults to ``.*``.
* ``allow`` (required): boolean indicating if the setting the session property should be allowed.

The catalog session property rules have the additional field:

* ``catalog`` (optional): regex to match against catalog name. Defaults to ``.*``.

The example below defines the following table access policy:

* Role ``admin`` can set all session property
* User ``banned_user`` can not set any session properties
* All users can set the ``resource_overcommit`` system session property, and the
  ``bucket_execution_enabled`` session property in the ``hive`` catalog.

.. literalinclude:: session-property-access.json
    :language: json

.. _query_rules:

Query rules
-----------

These rules control the ability of a user to execute, view, or kill a query. The user is
granted or denied access, based on the first matching rule read from top to bottom. If no
rules are specified, all users are allowed to execute queries, and to view or kill queries
owned by any user. If no rule matches, query management is denied. Each rule is composed
of the following fields:

* ``user`` (optional): regex to match against user name. Defaults to ``.*``.
* ``role`` (optional): regex to match against role names. Defaults to ``.*``.
* ``group`` (optional): regex to match against group names. Defaults to ``.*``.
* ``queryOwner`` (optional): regex to match against the query owner name. Defaults to ``.*``.
* ``allow`` (required): set of query permissions granted to user. Values: ``execute``, ``view``, ``kill``

.. note::

    Users always have permission to view or kill their own queries.

    A rule that includes ``owner`` may not include the ``execute`` access mode. Queries are only owned
    by a user once their execution has begun.

For example, if you want to allow the role ``admin`` full query access, allow the user ``alice``
to execute and kill queries, allow members of the group ``contractors`` to view queries owned by
users ``alice`` or ``dave``, allow any user to execute queries, and deny all other access, you can
use the following rules:

.. literalinclude:: query-access.json
    :language: json

.. _impersonation_rules:

Impersonation rules
-------------------

These rules control the ability of a user to impersonate another user. In
some environments it is desirable for an administrator (or managed system) to
run queries on behalf of other users. In these cases, the administrator
authenticates using their credentials, and then submits a query as a different
user. When the user context is changed, Trino will verify the administrator
is authorized to run queries as the target user.

When these rules are present, the authorization is based on the first matching rule,
processed from top to bottom. If no rules match, the authorization is denied.
If impersonation rules are not present but the legacy principal rules are specified,
it is assumed impersonation access control is being handled by the principal rules,
so impersonation is allowed. If neither impersonation nor principal rules are
defined, impersonation is not allowed.

Each impersonation rule is composed of the following fields:

* ``original_user`` (optional): regex to match against the user requesting the impersonation. Defaults to ``.*``.
* ``original_role`` (optional): regex to match against role names of the requesting impersonation. Defaults to ``.*``.
* ``new_user`` (optional): regex to match against the user that will be impersonated. Defaults to ``.*``.
* ``allow`` (optional): boolean indicating if the authentication should be allowed. Defaults to ``true``.

The following example allows the ``admin`` role, to impersonate any user, except
for ``bob``. It also allows any user to impersonate the ``test`` user:

.. literalinclude:: user-impersonation.json
    :language: json

.. _principal_rules:

Principal rules
---------------

.. warning::

    Principal rules are deprecated and will be removed in a future release.
    These rules have been replaced with :doc:`/security/user-mapping`, which
    specifies how a complex authentication user name is mapped to a simple
    user name for Trino, and impersonation rules defined above.

These rules serve to enforce a specific matching between a principal and a
specified user name. The principal is granted authorization as a user, based
on the first matching rule read from top to bottom. If no rules are specified,
no checks are performed. If no rule matches, user authorization is denied.
Each rule is composed of the following fields:

* ``principal`` (required): regex to match and group against principal.
* ``user`` (optional): regex to match against user name. If matched, it
  will grant or deny the authorization based on the value of ``allow``.
* ``principal_to_user`` (optional): replacement string to substitute against
  principal. If the result of the substitution is same as the user name, it will
  grant or deny the authorization based on the value of ``allow``.
* ``allow`` (required): boolean indicating whether a principal can be authorized
  as a user.

.. note::

    You would at least specify one criterion in a principal rule. If you specify
    both criteria in a principal rule, it returns the desired conclusion when
    either of criteria is satisfied.

The following implements an exact matching of the full principal name for LDAP
and Kerberos authentication:

.. code-block:: json

    {
      "principals": [
        {
          "principal": "(.*)",
          "principal_to_user": "$1",
          "allow": true
        },
        {
          "principal": "([^/]+)(/.*)?@.*",
          "principal_to_user": "$1",
          "allow": true
        }
      ]
    }

If you want to allow users to use the exact same name as their Kerberos principal
name, and allow ``alice`` and ``bob`` to use a group principal named as
``group@example.net``, you can use the following rules.

.. code-block:: json

    {
      "principals": [
        {
          "principal": "([^/]+)/?.*@example.net",
          "principal_to_user": "$1",
          "allow": true
        },
        {
          "principal": "group@example.net",
          "user": "alice|bob",
          "allow": true
        }
      ]
    }

.. _system_information_rules:

System information rules
------------------------

These rules specify which users can access the system information management interface.
The user is granted or denied access, based on the first matching rule read from top to
bottom. If no rules are specified, all access to system information is denied. If
no rule matches, system access is denied. Each rule is composed of the following fields:

* ``user`` (optional): regex to match against user name. If matched, it
  will grant or deny the authorization based on the value of ``allow``.
* ``allow`` (required): set of access permissions granted to user. Values: ``read``, ``write``

For example, if you want to allow only the role ``admin`` to read and write
system information, allow ``alice`` to read system information, and deny all other access, you
can use the following rules:

.. literalinclude:: system-information-access.json
    :language: json

A fixed user can be set for management interfaces using the ``management.user``
configuration property.  When this is configured, system information rules must still be set
to authorize this user to read or write to management information. The fixed management user
only applies to HTTP by default. To enable the fixed user over HTTPS, set the
``management.user.https-enabled`` configuration property.
