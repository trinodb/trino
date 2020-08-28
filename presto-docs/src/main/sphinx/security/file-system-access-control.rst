================================
File Based System Access Control
================================

This access control plugin allows you to specify authorization rules in a JSON file.

Configuration
-------------

To use this plugin, add an ``etc/access-control.properties`` file containing two
required properties: ``access-control.name``, which must be equal to ``file``, and
``security.config-file``, which must be equal to the location of the config file.
For example, if a config file named ``rules.json``
resides in ``etc``, add an ``etc/access-control.properties`` with the following
contents:

.. code-block:: none

   access-control.name=file
   security.config-file=etc/rules.json

The config file is specified in JSON format. It contains rules that define
which users have access to which resources. The rules are read from top to bottom
and the first matching rule is applied. If no rule matches, access is denied.

Refresh
--------

By default, when a change is made to the JSON rules file, Presto must be restarted
to load the changes. There is an optional property to refresh the properties without requiring a
Presto restart. The refresh period is specified in the ``etc/access-control.properties``:

.. code-block:: none

   security.refresh-period=1s

Catalog Rules
-------------

These rules govern the catalogs particular users can access. The user is
granted access to a catalog, based on the first matching rule read from top to
bottom. If no rule matches, access is denied. Each rule is composed of the
following fields:

* ``user`` (optional): regex to match against user name. Defaults to ``.*``.
* ``group`` (optional): regex to match against group names. Defaults to ``.*``.
* ``catalog`` (optional): regex to match against catalog name. Defaults to ``.*``.
* ``allow`` (required): string indicating whether a user has access to the catalog.
  This value can be ``all``, ``read-only`` or ``none``, and defaults to ``none``.
  Setting this value to ``read-only`` has the same behavior as the ``read-only``
  system access control plugin.

In order for a rule to apply the user name must match the regular expression
specified in ``user`` attribute.

For group names, a rule can be applied if at least one group name of this user
matches the ``group`` regular expression.

.. note::

    By default, all users have access to the ``system`` catalog. You can
    override this behavior by adding a rule.

    Boolean ``true`` and ``false`` are also supported as legacy values for ``allow``,
    to support backwards compatibility.  ``true`` maps to ``all``, and ``false`` maps to ``none``.

For example, if you want to allow only the user ``admin`` to access the
``mysql`` and the ``system`` catalog, allow users from the ``finance``
and ``admin`` groups access to ``postgres`` catalog, allow all users to
access the ``hive`` catalog, and deny all other access, you can use the
following rules:

.. code-block:: json

    {
      "catalogs": [
        {
          "user": "admin",
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

Schema Rules
------------

These rules allow you to grant ownership of a schema. Having ownership of an
schema allows users to execute ``DROP SCHEMA``, ``ALTER SCHEMA`` (both renaming
and setting authorization) and ``SHOW CREATE SCHEMA``. The user is granted
ownership of a schema, based on the first matching rule read from top to
bottom. If no rule matches, ownership is not granted. Each rule is composed of
the following fields:

* ``user`` (optional): regex to match against user name. Defaults to ``.*``.
* ``group`` (optional): regex to match against group names. Defaults to ``.*``.
* ``schema`` (optional): regex to match against schema name. Defaults to ``.*``.
* ``owner`` (required): boolean indicating whether the user is to be considered
  an owner of the schema. Defaults to ``false``.

For example, to provide ownership of all schemas to user ``admin``, treat all
users as owners of ``default`` schema and prevent user ``guest`` from ownership
of any schema, you can use the following rules:

.. code-block:: json

    {
      "catalogs": [
        {
          "allow": true
        }
      ],
      "schemas": [
        {
          "user": "admin",
          "schema": ".*",
          "owner": true
        },
        {
          "user": "guest",
          "owner": false
        },
        {
          "schema": "default",
          "owner": true
        }
      ]
    }

Table Rules
-----------

These rules define the privileges for table access for users. If no table rules
are specified, all users are treated as having all privileges by default. The
user is granted privileges based on the first matching rule read from top to
bottom. Each rule is composed of the following fields:

* ``user`` (optional): regex to match against user name. Defaults to ``.*``.
* ``group`` (optional): regex to match against group names. Defaults to ``.*``.
* ``schema`` (optional): regex to match against schema name. Defaults to ``.*``.
* ``table`` (optional): regex to match against table names. Defaults to ``.*``.
* ``privileges`` (required): zero or more of ``SELECT``, ``INSERT``,
  ``DELETE``, ``OWNERSHIP``, ``GRANT_SELECT``

.. note::

    These rules do not apply to ``information_schema``.

The example below defines the following table access policy:

* User ``admin`` has all privileges across all tables and schemas
* User ``banned_user`` has no privileges
* All users have ``SELECT`` privileges on all tables in ``default`` schema

.. code-block:: json

    {
      "catalogs": [
        {
          "allow": true
        }
      ],
      "tables": [
        {
          "user": "admin",
          "privileges": ["SELECT", "INSERT", "DELETE", "OWNERSHIP"]
        },
        {
          "user": "banned_user",
          "privileges": []
        },
        {
          "schema": "default",
          "table": ".*",
          "privileges": ["SELECT"]
        }
      ]
    }

.. _query_rules:

Query Rules
-----------

These rules control the ability of a user to execute, view, or kill a query. The user is
granted or denied access, based on the first matching rule read from top to bottom. If no
rules are specified, all users are allowed to execute queries, and to view or kill queries
owned by any user. If no rule matches, query management is denied. Each rule is composed
of the following fields:

* ``user`` (optional): regex to match against user name. Defaults to ``.*``.
* ``owner`` (optional): regex to match against the query owner name. Defaults to ``.*``.
* ``allow`` (required): set of query permissions granted to user. Values: ``execute``, ``view``, ``kill``

.. note::

    Users always have permission to view or kill their own queries.

For example, if you want to allow the user ``admin`` full query access, allow the user ``alice``
to execute and kill queries, any user to execute queries, and deny all other access, you can use
the following rules:

.. literalinclude:: query-access.json
    :language: json

.. _impersonation_rules:

Impersonation Rules
-------------------

These rules control the ability of a user to impersonate another user.  In
some environments it is desirable for an administrator (or managed system) to
run queries on behalf of other users.  In these cases, the administrator
authenticates using their credentials, and then submits a query as a different
user.  When the user context is changed, Presto will verify the administrator
is authorized to run queries as the target user.

When these rules are present, the authorization is based on the first matching rule,
processed from top to bottom. If no rules match, the authorization is denied.
If impersonation rules are not present but the legacy principal rules are specified,
it is assumed impersonation access control is being handled by the principal rules,
so impersonation is allowed.  If neither impersonation nor principal rules are
defined, impersonation is not allowed.

Each impersonation rule is composed of the following fields:

* ``originalUser`` (required): regex to match against the user requesting the impersonation.
* ``newUser`` (required): regex to match against the user that will be impersonated.
* ``allow`` (optional): boolean indicating if the authentication should be allowed.

The following example allows the two admins, ``alice`` and ``bob``, to impersonate
any user, except they may not impersonate each other.  It also allows any user to
impersonate the ``test`` user:

.. literalinclude:: user-impersonation.json
    :language: json

.. _principal_rules:

Principal Rules
---------------

.. warning::

    Principal rules are deprecated and will be removed in a future release.
    These rules have been replaced with :doc:`/security/user-mapping`, which
    specifies how a complex authentication user name is mapped to a simple
    user name for Presto, and impersonation rules defined above.

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
      "catalogs": [
        {
          "allow": true
        }
      ],
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
      "catalogs": [
        {
          "allow": true
        }
      ],
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

System Information Rules
------------------------

These rules specify which users can access the system information management interface.
The user is granted or denied access, based on the first matching rule read from top to
bottom. If no rules are specified, all access to system information is denied. If
no rule matches, system access is denied. Each rule is composed of the following fields:

* ``user`` (optional): regex to match against user name. If matched, it
  will grant or deny the authorization based on the value of ``allow``.
* ``allow`` (required): set of access permissions granted to user. Values: ``read``, ``write``

For example, if you want to allow only the user ``admin`` to read and write
system information, allow ``alice`` to read system information, and deny all other access, you
can use the following rules:

.. literalinclude:: system-information-access.json
    :language: json

A fixed user can be set for management interfaces using the ``management.user``
configuration property.  When this is configured, system information rules must still be set
to authorize this user to read or write to management information. The fixed management user
only applies to HTTP by default. To enable the fixed user over HTTPS, set the
``management.user.https-enabled`` configuration property.
