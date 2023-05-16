===================
LDAP group provider
===================

Trino can lookup user and groups to help manage access control.
The LDAP group provider resolves group membership from an LDAP server.

LDAP Group Provider Configuration
---------------------------------

Enable the LDAP group provider by creating an ``etc/group-provider.properties``
file on the coordinator. Example:

.. code-block:: text

    group-provider.name=ldap
    ldap.user-bind-pattern=uid=${USER},ou=org,dc=trino,dc=testldap,dc=com:uid=${USER},ou=alt
    ldap.url=ldap://ldap-server:389
    ldap.bind-dn=cn=admin,dc=trino,dc=testldap,dc=com
    ldap.bind-password=<bind password>
    ldap.allow-insecure=true
    ldap.user-bind-pattern=cn=${USER}
    ldap.user-base-dn=ou=America,dc=trino,dc=testldap,dc=com
    ldap.group-base-dn=ou=admins,dc=trino,dc=testldap,dc=com
    ldap.group-membership-attribute=member
    ldap.group-user-membership-attribute=memberOf
    ldap.group-name-attribute=ou
    ldap.cache-ttl=1h

The following configuration properties are available:

========================================= ========================================================
Property                                  Description
========================================= ========================================================
``ldap.url``                              The URL to the LDAP server. The URL scheme must be
                                          ``ldap://`` or ``ldaps://``. Connecting to the LDAP
                                          server without TLS enabled requires
                                          ``ldap.allow-insecure=true``.
``ldap.allow-insecure``                   Allow using an LDAP connection that is not secured with
                                          TLS.
``ldap.ssl.keystore.path``                The path to the :doc:`PEM </security/inspect-pem>`
                                          or :doc:`JKS </security/inspect-jks>` keystore file.
``ldap.ssl.keystore.password``            Password for the key store.
``ldap.ssl.truststore.path``              The path to the :doc:`PEM </security/inspect-pem>`
                                          or :doc:`JKS </security/inspect-jks>` truststore file.
``ldap.ssl.truststore.password``          Password for the truststore.
``ldap.user-bind-pattern``                This property can be used to specify the LDAP user
                                          bind string for password authentication. This property
                                          must contain the pattern ``${USER}``, which is
                                          replaced by the actual username.
``ldap.user-base-dn``                     The starting place to look for users in LDAP.
                                          Specified as a distinguished name (DN).
``ldap.group-base-dn``                    (Optional) The starting place to look for groups in
                                          LDAP. Specified as a distinguished name (DN).
``ldap.group-membership-attribute``       (Optional) The attribute name on a Group that stores
                                          member users.
``ldap.group-user-membership-attribute``  (Optional) The attribute name on a User that stores
                                          group membership.
``ldap.group-name-attribute``             (Optional) The attribute Type that is used to get the
                                          name of the group that will be used in Trino.
``ldap.cache-ttl``                        LDAP group cache duration. Defaults to ``1h``.
``ldap.allow-user-not-exist``             Do not error for User's that do not exist.
                                          Defaults to ``false``.
========================================= ========================================================

.. note::

    Both the ``ldap.group-membership-attribute`` and ``ldap.group-user-membership-attribute`` properties
    cannot be empty. When setting ``ldap.group-membership-attribute`` then ``ldap.group-name-attribute``
    cannot be empty. This will result in a configuration error.

Based on the LDAP server implementation type, the property
``ldap.user-bind-pattern`` can be used as described below.

Active Directory
****************

.. code-block:: text

    ldap.user-bind-pattern=${USER}@<domain_name_of_the_server>

Example:

.. code-block:: text

    ldap.user-bind-pattern=${USER}@corp.example.com

OpenLDAP
********

.. code-block:: text

    ldap.user-bind-pattern=uid=${USER},<distinguished_name_of_the_user>

Example:

.. code-block:: text

    ldap.user-bind-pattern=uid=${USER},OU=America,DC=corp,DC=example,DC=com

LDAP Group Membership Search Algorithm
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Different LDAP server configurations allow for groups to be stored in different ways. When
storing groups, the membership information can be stored in the User record, in a
Group record, one of these or a combination of both. If the group membership information is
available on the User record, no further group query is attempted.

There are three primary configurations supported.

1. Groups are a first class entry in LDAP with members of the group listed using an
identifying reference as attributes on the group. Example:

.. code-block:: text

    dn: ou=users,dc=trino,dc=testldap,dc=com
    objectClass: organizationalUnit
    ou: users

    dn: cn=Jane,ou=users,dc=trino,dc=testldap,dc=com
    objectClass: person
    objectClass: organizationalPerson
    objectClass: inetOrgPerson
    cn: Jane
    sn: Doe
    displayName: Jane Doe
    mail: jane.doe@testldap.com

    dn: ou=groups,dc=trino,dc=testldap,dc=com
    objectClass: organizationalUnit
    ou: groups

    dn: cn=admins,ou=groups,dc=trino,dc=testldap,dc=com
    objectClass: groupOfNames
    cn: admins
    owner: cn=admin,dc=example,dc=com
    description: System Administrators
    member: cn=Jane,ou=users,dc=trino,dc=testldap,dc=com

This would correspond to the following ``etc/group-provider.properties`` properties.
Example:

.. code-block:: text

    group-provider.name=ldap
    ldap.user-bind-pattern=uid=${USER},ou=org,dc=test,dc=com:uid=${USER},ou=alt
    ldap.url=ldap://ldap-server:389
    ldap.bind-dn=cn=admin,dc=trino,dc=testldap,dc=com
    ldap.bind-password=<bind password>
    ldap.allow-insecure=true
    ldap.user-bind-pattern=cn=${USER}
    ldap.user-base-dn=ou=users,dc=trino,dc=testldap,dc=com
    ldap.group-base-dn=ou=groups,dc=trino,dc=testldap,dc=com
    ldap.group-membership-attribute=member
    ldap.group-name-attribute=cn
    ldap.cache-ttl=1h

In this case group membership information is not available on the User record, hence
the ``ldap.group-membership-attribute`` property is set and not the
``ldap.group-user-membership-attribute`` property. The User query
would be followed by a group search query. The name of the group added to Trino
would be ``admins`` which is identified using the ``ldap.group-name-attribute=cn``
property.


2. Group membership's are listed as attributes on the User record with the group
being a first-class entry containing a linkage back to the User record. Example:

.. code-block:: text

    dn: ou=users,dc=trino,dc=testldap,dc=com
    objectClass: organizationalUnit
    ou: users

    dn: cn=Jane,ou=users,dc=trino,dc=testldap,dc=com
    objectClass: person
    objectClass: organizationalPerson
    objectClass: inetOrgPerson
    cn: Jane
    sn: Doe
    displayName: Jane Doe
    mail: jane.doe@testldap.com
    memberOf: cn=admins,ou=groups,dc=trino,dc=testldap,dc=com

    dn: ou=groups,dc=trino,dc=testldap,dc=com
    objectClass: organizationalUnit
    ou: groups

    dn: cn=admins,ou=groups,dc=trino,dc=testldap,dc=com
    objectClass: groupOfNames
    cn: admins
    owner: cn=admin,dc=example,dc=com
    description: System Administrators
    member: cn=Jane,ou=users,dc=trino,dc=testldap,dc=com

This would correspond to the following ``etc/group-provider.properties`` properties.
Example:

.. code-block:: text

    group-provider.name=ldap
    ldap.user-bind-pattern=uid=${USER},ou=org,dc=test,dc=com:uid=${USER},ou=alt
    ldap.url=ldap://ldap-server:389
    ldap.bind-dn=cn=admin,dc=trino,dc=testldap,dc=com
    ldap.bind-password=<bind password>
    ldap.allow-insecure=true
    ldap.user-bind-pattern=cn=${USER}
    ldap.user-base-dn=ou=users,dc=trino,dc=testldap,dc=com
    ldap.group-base-dn=ou=groups,dc=trino,dc=testldap,dc=com
    ldap.group-membership-attribute=member
    ldap.group-user-membership-attribute=memberOf
    ldap.group-name-attribute=cn
    ldap.cache-ttl=1h

In this case group membership information is available on the User record, hence
the ``ldap.group-user-membership-attribute`` property is set. The User query
is all that is needed to return membership Groups. The name of the group added to Trino
would be ``admins`` which is identified using the ``ldap.group-name-attribute=cn``
property.


3. A User’s group memberships are listed as attributes on the User record. The
Group does not exist as an entry on the LDAP server. The Group name attribute is not a
literal attribute. Example:

.. code-block:: text

    dn: ou=users,dc=trino,dc=testldap,dc=com
    objectClass: organizationalUnit
    ou: users

    dn: cn=Jane,ou=users,dc=trino,dc=testldap,dc=com
    objectClass: person
    objectClass: organizationalPerson
    objectClass: inetOrgPerson
    cn: Jane
    sn: Doe
    displayName: Jane Doe
    mail: jane.doe@testldap.com
    memberOf: admins

This would correspond to the following ``etc/group-provider.properties`` properties.
Example:

.. code-block:: text

    group-provider.name=ldap
    ldap.user-bind-pattern=uid=${USER},ou=org,dc=test,dc=com:uid=${USER},ou=alt
    ldap.url=ldap://ldap-server:389
    ldap.bind-dn=cn=admin,dc=trino,dc=testldap,dc=com
    ldap.bind-password=<bind password>
    ldap.allow-insecure=true
    ldap.user-bind-pattern=cn=${USER}
    ldap.user-base-dn=ou=users,dc=trino,dc=testldap,dc=com
    ldap.group-user-membership-attribute=memberOf
    ldap.cache-ttl=1h

In this case there is no Group record, so the Optional properties ``ldap.group-base-dn``,
and ``ldap.group-membership-attribute`` are not set. The attribute on the User specifying
group membership ``ldap.group-user-membership-attribute=memberOf`` is not a literal value
as it contains only the text ``admins``. Hence ``ldap.group-name-attribute`` is also not set.
The name of the group added to Trino would be ``admins``. In some LDAP servers, it is
created during search and returned to the client, but not committed to the LDAP database.

Querying Group Memberships
~~~~~~~~~~~~~~~~~~~~~~~~~~

Depending on the LDAP server configuration, when multiple ``memberOf`` or ``member`` records
exist for a User's group membership, these will be returned as Trino groups.

You can use the :doc:`session information </functions/session>` to query for a User's groups. Example:

.. code-block:: text

    select current_groups();

Log Levels
~~~~~~~~~~

To increase the verbosity of the LDAP Group Provider log level add the following to ``etc/log.properties``:

.. code-block:: text

    io.trino.plugin.password.ldap=DEBUG
