=========================
Salesforce authentication
=========================

Trino can be configured to enable frontend password authentication over
HTTPS for clients, such as the CLI, or the JDBC and ODBC drivers. The
username and password (or password and `security token <#security-token>`__ concatenation)
are validated by having the Trino coordinator perform a login to Salesforce.

This allows you to enable users to authenticate to Trino via their Salesforce
basic credentials.  This can also be used to secure the :ref:`Web UI <web-ui-authentication>`.

.. note::

    This is *not* a Salesforce connector, and does not allow users to query
    Salesforce data. Salesforce authentication is simply a means by which users
    can authenticate to Trino, similar to :doc:`ldap` or :doc:`password-file`.

Salesforce authenticator configuration
--------------------------------------

To enable Salesfore authentication, set the :doc:`password authentication
type <authentication-types>` in ``etc/config.properties``:

.. code-block:: properties

    http-server.authentication.type=PASSWORD

In addition, create a ``etc/password-authenticator.properties`` file on the
coordinator with the ``salesforce`` authenticator name:

.. code-block:: properties

    password-authenticator.name=salesforce
    salesforce.allowed-organizations=<allowed-org-ids or all>

The following configuration properties are available:

====================================   ============================================================
Property                               Description
====================================   ============================================================
``salesforce.allowed-organizations``   Comma separated list of 18 character Salesforce.com
                                       Organization IDs for a second, simple layer of security.
                                       This option can be explicitly ignored using ``all``, which
                                       bypasses any check of the authenticated user's
                                       Salesforce.com Organization ID.

``salesforce.cache-size``              Maximum number of cached authenticated users.
                                       Defaults to ``4096``.

``salesforce.cache-expire-duration``   How long a cached authentication should be considered valid.
                                       Defaults to ``2m``.
====================================   ============================================================

Salesforce concepts
-------------------

There are two Salesforce specific aspects to this authenticator.  They are the use of the
Salesforce security token, and configuration of one or more Salesforce.com Organization IDs.


Security token
^^^^^^^^^^^^^^

Credentials are a user's Salesforce username and password if Trino is connecting from a whitelisted
IP, or username and password/`security token <https://help.salesforce.com/articleView?id=user_security_token.htm&type=5>`_
concatenation otherwise.  For example, if Trino is *not* whitelisted, and your password is ``password``
and security token is ``token``, use ``passwordtoken`` to authenticate.

You can configure a public IP for Trino as a trusted IP by `whitelisting an IP range
<https://help.salesforce.com/articleView?id=security_networkaccess.htm&type=5>`_.

Salesforce.com organization IDs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can configure one or more Salesforce Organization IDs for additional security.  When the user authenticates,
the Salesforce API returns the *18 character* Salesforce.com Organization ID for the user.  The Trino Salesforce
authenticator ensures that the ID matches one of the IDs configured in ``salesforce.allowed-organizations``.

Optionally, you can configure ``all`` to explicitly ignore this layer of security.

Admins can find their Salesforce.com Organization ID using the `Salesforce Setup UI
<https://help.salesforce.com/articleView?id=000325251&type=1&mode=1>`_.  This will be the 15 character
ID, which can be `converted to the 18 character ID <https://sf1518.click/>`_.

