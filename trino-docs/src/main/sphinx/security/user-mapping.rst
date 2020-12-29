============
User Mapping
============

User mapping defines rules for mapping from users in the authentication system to Presto users.  This
mapping is particularly important for Kerberos or certificate authentication where the user names
are complex like ``alice@example`` or ``CN=Alice Smith, OU=Finance, O=Acme, C=US``.

User mapping can be configured with a simple regex extraction pattern, or more complex rules in a
separate configuration file.

Pattern Mapping Rule
--------------------

The pattern mapping rule maps the authentication user to the first matching group in the regular
expression.  If the regular expression does not match the authentication user, authentication is
denied.

Each authentication system has a separate property for the user mapping pattern to allow different mapping
when multiple authentication systems are enabled:

===================================== ===============================================================
Authentication                        Property
===================================== ===============================================================
Username and Password (file or LDAP)  ``http-server.authentication.password.user-mapping.pattern``
Kerberos                              ``http-server.authentication.krb5.user-mapping.pattern``
Certificate                           ``http-server.authentication.certificate.user-mapping.pattern``
Json Web Token                        ``http-server.authentication.jwt.user-mapping.pattern``
Insecure                              ``http-server.authentication.insecure.user-mapping.pattern``
===================================== ===============================================================

File Mapping Rules
------------------

The file mapping rules allow for more complex mappings from the authentication user.  These rules are
loaded from a JSON file defined in a configuration property.  The mapping is based on the first matching
rule, processed from top to bottom. If no rules match, authentication is denied.  Each rule is composed of the
following fields:

* ``pattern`` (required): regex to match against authentication user.
* ``user`` (optional): replacement string to substitute against pattern.
  The default value is ``$1``.
* ``allow`` (optional): boolean indicating if the authentication should be allowed.
* ``case`` (optional): one of:

  * ``keep`` - keep matched user name as is (default behavior)
  * ``lower`` - lowercase matched user name, e.g., ``Admin`` or ``ADMIN`` will become ``admin``
  * ``upper`` - uppercase matched user name, e.g., ``admin`` or ``Admin`` will become ``ADMIN``

The following example maps all users like ``alice@example.com`` to just ``alice``, except for the ``test``
user which is denied authentication, and it maps users like ``bob@uk.example.com`` to ``bob_uk``:

.. literalinclude:: user-mapping.json
    :language: json

Each authentication system has a separate property for the user mapping file to allow different mapping
when multiple authentication systems are enabled:

===================================== ===============================================================
Authentication                        Property
===================================== ===============================================================
Username and password (file or LDAP)  ``http-server.authentication.password.user-mapping.file``
Kerberos                              ``http-server.authentication.krb5.user-mapping.file``
Certificate                           ``http-server.authentication.certificate.user-mapping.file``
Json Web Token                        ``http-server.authentication.jwt.user-mapping.file``
Insecure                              ``http-server.authentication.insecure.user-mapping.file``
===================================== ===============================================================

