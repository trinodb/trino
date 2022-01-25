=================
Security overview
=================

After the initial :doc:`installation </installation>` of your cluster, security
is the next major concern for successfully operating Trino. This overview
provides an introduction to different aspects of configuring security for your
Trino cluster.

Aspects of configuring security
-------------------------------

The default installation of Trino has no security features enabled. Security
can be enabled for different parts of the Trino architecture:

* :ref:`security-client`
* :ref:`security-inside-cluster`
* :ref:`security-data-sources`

Suggested configuration workflow
--------------------------------

To configure security for a new Trino cluster, follow this best practice
order of steps. Do not skip or combine steps.

#. **Enable** :doc:`HTTPS/TLS </security/tls>`

   * Work with your security team.
   * Use a :ref:`load balancer or proxy <https-load-balancer>` to terminate
     HTTPS, if possible.
   * Use a globally trusted TLS certificate.

#. **Enable authentication**

   * Start with :doc:`password file authentication <password-file>` to get up
     and running.
   * Then configure your preferred authentication provider, such as :doc:`LDAP
     </security/ldap>`.
   * Avoid the complexity of Kerberos for client authentication, if possible.

#. **Enable authorization and access control**

   * Start with :doc:`file-based rules <file-system-access-control>`.
   * Then configure another access control method as required.

Configure one step at a time. Always restart the Trino server after each
change, and verify the results before proceeding.

.. _security-client:

Securing client access to the cluster
-------------------------------------

Trino :doc:`clients </client>` include the Trino :doc:`CLI </installation/cli>`,
the :doc:`Web UI </admin/web-interface>`, the :doc:`JDBC driver
</installation/jdbc>`, `Python, Go, or other clients
<https://trino.io/resources.html>`_, and any applications using these tools.

All access to the Trino cluster is managed by the coordinator. Thus, securing
access to the cluster means securing access to the coordinator.

There are three aspects to consider:

* :ref:`cl-access-encrypt`: protecting the integrity of client to server
  communication in transit.
* :ref:`cl-access-auth`: identifying users and user name management.
* :ref:`cl-access-control`: validating each user's access rights.

.. _cl-access-encrypt:

Encryption
^^^^^^^^^^

The Trino server uses the standard :doc:`HTTPS protocol and TLS encryption
<tls>`, formerly known as SSL.

.. _cl-access-auth:

Authentication
^^^^^^^^^^^^^^

Trino supports several authentication providers. When setting up a new cluster,
start with simple password file authentication before configuring another
provider.

* :doc:`Password file authentication <password-file>`
* :doc:`LDAP authentication <ldap>`
* :doc:`Salesforce authentication <salesforce>`
* :doc:`OAuth 2.0 authentication <oauth2>`
* :doc:`Certificate authentication <certificate>`
* :doc:`JSON Web Token (JWT) authentication <jwt>`
* :doc:`Kerberos authentication <kerberos>`

.. _user-name-management:

User name management
""""""""""""""""""""

Trino provides ways to map the user and group names from authentication
providers to Trino user names.

* :doc:`User mapping <user-mapping>` applies to all authentication systems,
  and allows for regular expression rules to be specified that map complex user
  names from other systems (``alice@example.com``) to simple user names
  (``alice``).
* :doc:`File group provider <group-file>` provides a way to assign a set
  of user names to a group name to ease access control.

.. _cl-access-control:

Authorization and access control
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Trino's :doc:`default method of access control <built-in-system-access-control>`
allows all operations for all authenticated users.

To implement access control, use:

* :doc:`File-based system access control <file-system-access-control>`, where
  you configure JSON files that specify fine-grained user access restrictions at
  the catalog, schema, or table level.

In addition, Trino :doc:`provides an API </develop/system-access-control>` that
allows you to create a custom access control method, or to extend an existing
one.

Access control can limit access to columns of a table. The default behavior
of a query to all columns with a ``SELECT *`` statement is to show an error
denying access to any inaccessible columns.

You can change this behavior to silently hide inaccessible columns with the
global property ``hide-inaccessible-columns`` configured in
:ref:`config_properties`:

.. code-block:: properties

    hide-inaccessible-columns = true

.. _security-inside-cluster:

Securing inside the cluster
---------------------------

You can :doc:`secure the internal communication <internal-communication>`
between coordinator and workers inside the clusters.

Secrets in properties files, such as passwords in catalog files, can be secured
with :doc:`secrets management <secrets>`.

.. _security-data-sources:

Securing cluster access to data sources
---------------------------------------

Communication between the Trino cluster and data sources is configured for each
catalog. Each catalog uses a connector, which supports a variety of
security-related configurations.

More information is available with the documentation for individual
:doc:`connectors </connector>`.

:doc:`Secrets management <secrets>` can be used for the catalog properties files
content.

