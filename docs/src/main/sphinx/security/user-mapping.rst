============
User mapping
============

User mapping defines rules for mapping from users in the authentication method to Trino users. This
mapping is particularly important for :doc:`Kerberos </security/kerberos>` or
certificate authentication where the user names
are complex, such as ``alice@example`` or ``CN=Alice Smith,OU=Finance,O=Acme,C=US``.

There are two ways to map the username format of a given authentication
provider into the simple username format of Trino users:

* With a single regular expression (regex) :ref:`pattern mapping rule <pattern-rule>`
* With a :ref:`file of regex mapping rules <pattern-file>` in JSON format

.. _pattern-rule:

Pattern mapping rule
--------------------

If you can map all of your authentication method’s usernames with a single
reqular expression, consider using a **Pattern mapping rule**.

For example, your authentication method uses all usernames in the form
``alice@example.com``, with no exceptions. In this case, choose a regex that
breaks incoming usernames into at least two regex capture groups, such that the
first capture group includes only the name before the ``@`` sign. You can use
the simple regex ``(.*)(@.*)`` for this case.

Trino automatically uses the first capture group – the $1 group – as the
username to emit after the regex substitution. If the regular expression does
not match the incoming username, authentication is denied.

Specify your regex pattern in the appropriate property in your coordinator’s
``config.properties`` file, using one of the ``*user-mapping.pattern``
properties from the table below that matches the authentication type of your
configured authentication provider. For example, for an :doc:`LDAP
</security/ldap>` authentication provider:

.. code-block:: text

    http-server.authentication.password.user-mapping.pattern=(.*)(@.*)

Remember that an :doc:`authentication type </security/authentication-types>`
represents a category, such as ``PASSWORD``, ``OAUTH2``, ``KERBEROS``. More than
one authentication method can have the same authentication type. For example,
the Password file, LDAP, and Salesforce authentication methods all share the
``PASSWORD`` authentication type.

You can specify different user mapping patterns for different authentication
types when multiple authentication methods are enabled:

===================================== ===============================================================
Authentication type                   Property
===================================== ===============================================================
Password (file, LDAP, Salesforce)     ``http-server.authentication.password.user-mapping.pattern``
OAuth2                                ``http-server.authentication.oauth2.user-mapping.pattern``
Certificate                           ``http-server.authentication.certificate.user-mapping.pattern``
Header                                ``http-server.authentication.header.user-mapping.pattern``
JSON Web Token                        ``http-server.authentication.jwt.user-mapping.pattern``
Kerberos                              ``http-server.authentication.krb5.user-mapping.pattern``
Insecure                              ``http-server.authentication.insecure.user-mapping.pattern``
===================================== ===============================================================

.. _pattern-file:

File mapping rules
------------------

Use the **File mapping rules** method if your authentication provider expresses
usernames in a way that cannot be reduced to a single rule, or if you want to
exclude a set of users from accessing the cluster.

The rules are loaded from a JSON file identified in a configuration property.
The mapping is based on the first matching rule, processed from top to bottom.
If no rules match, authentication is denied.  Each rule is composed of the
following fields:

* ``pattern`` (required): regex to match against the authentication method's
  username.
* ``user`` (optional): replacement string to substitute against *pattern*.
  The default value is ``$1``.
* ``allow`` (optional): boolean indicating whether authentication is to be
  allowed for the current match.
* ``case`` (optional): one of:

  * ``keep`` - keep the matched username as is (default behavior)
  * ``lower`` - lowercase the matched username; thus both ``Admin`` and ``ADMIN`` become ``admin``
  * ``upper`` - uppercase the matched username; thus both ``admin`` and ``Admin`` become ``ADMIN``

The following example maps all usernames in the form ``alice@example.com`` to
just ``alice``, except for the ``test`` user, which is denied authentication. It
also maps users in the form ``bob@uk.example.com`` to ``bob_uk``:

.. literalinclude:: user-mapping.json
    :language: json

Set up the preceding example to use the :doc:`LDAP </security/ldap>`
authentication method with the :doc:`PASSWORD </security/authentication-types>`
authentication type by adding the following line to your coordinator's
``config.properties`` file:

.. code-block:: text

    http-server.authentication.password.user-mapping.file=etc/user-mapping.json

You can place your user mapping JSON file in any local file system location on
the coordinator, but placement in the ``etc`` directory is typical. There is no
naming standard for the file or its extension, although using ``.json`` as the
extension is traditional. Specify an absolute path or a path relative to the
Trino installation root.

You can specify different user mapping files for different authentication
types when multiple authentication methods are enabled:

===================================== ===============================================================
Authentication type                   Property
===================================== ===============================================================
Password (file, LDAP, Salesforce)     ``http-server.authentication.password.user-mapping.file``
OAuth2                                ``http-server.authentication.oauth2.user-mapping.file``
Certificate                           ``http-server.authentication.certificate.user-mapping.file``
Header                                ``http-server.authentication.header.user-mapping.pattern``
JSON Web Token                        ``http-server.authentication.jwt.user-mapping.file``
Kerberos                              ``http-server.authentication.krb5.user-mapping.file``
Insecure                              ``http-server.authentication.insecure.user-mapping.file``
===================================== ===============================================================

