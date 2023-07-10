======
Web UI
======

Trino provides a web-based user interface (UI) for monitoring a Trino cluster
and managing queries. The Web UI is accessible on the coordinator via
HTTP or HTTPS, using the corresponding port number specified in the coordinator
:ref:`config-properties`. It can be configured with :doc:`/admin/properties-web-interface`.

The Web UI can be disabled entirely with the ``web-ui.enabled`` property.

.. _web-ui-authentication:

Authentication
--------------

The Web UI requires users to authenticate. If Trino is not configured to require
authentication, then any username can be used, and no password is required or
allowed. Typically, users login with the same username that they use for
running queries.

If no system access control is installed, then all users are able to view and kill
any query. This can be restricted by using :ref:`query rules <query-rules>` with the
:doc:`/security/built-in-system-access-control`. Users always have permission to view
or kill their own queries.

Password authentication
^^^^^^^^^^^^^^^^^^^^^^^

Typically, a password-based authentication method
such as :doc:`LDAP </security/ldap>` or :doc:`password file </security/password-file>`
is used to secure both the Trino server and the Web UI. When the Trino server
is configured to use a password authenticator, the Web UI authentication type
is automatically set to ``FORM``. In this case, the Web UI displays a login form
that accepts a username and password.

Fixed user authentication
^^^^^^^^^^^^^^^^^^^^^^^^^

If you require the Web UI to be accessible without authentication, you can set a fixed
username that will be used for all Web UI access by setting the authentication type to
``FIXED`` and setting the username with the ``web-ui.user`` configuration property.
If there is a system access control installed, this user must have permission to view
(and possibly to kill) queries.

Other authentication types
^^^^^^^^^^^^^^^^^^^^^^^^^^

The following Web UI authentication types are also supported:

* ``CERTIFICATE``, see details in :doc:`/security/certificate`
* ``KERBEROS``, see details in :doc:`/security/kerberos`
* ``JWT``, see details in :doc:`/security/jwt`
* ``OAUTH2``, see details in :doc:`/security/oauth2`

For these authentication types, the username is defined by :doc:`/security/user-mapping`.

.. _web-ui-overview:

User interface overview
-----------------------

The main page has a list of queries along with information like unique query ID, query text,
query state, percentage completed, username and source from which this query originated.
The currently running queries are at the top of the page, followed by the most recently
completed or failed queries.

The possible query states are as follows:

* ``QUEUED`` -- Query has been accepted and is awaiting execution.
* ``PLANNING`` -- Query is being planned.
* ``STARTING`` -- Query execution is being started.
* ``RUNNING`` -- Query has at least one running task.
* ``BLOCKED`` -- Query is blocked and is waiting for resources (buffer space, memory, splits, etc.).
* ``FINISHING`` -- Query is finishing (e.g. commit for autocommit queries).
* ``FINISHED`` -- Query has finished executing and all output has been consumed.
* ``FAILED`` -- Query execution failed.

The ``BLOCKED`` state is normal, but if it is persistent, it should be investigated.
It has many potential causes: insufficient memory or splits, disk or network I/O bottlenecks, data skew
(all the data goes to a few workers), a lack of parallelism (only a few workers available), or computationally
expensive stages of the query following a given stage.  Additionally, a query can be in
the ``BLOCKED`` state if a client is not processing the data fast enough (common with "SELECT \*" queries).

For more detailed information about a query, simply click the query ID link.
The query detail page has a summary section, graphical representation of various stages of the
query and a list of tasks. Each task ID can be clicked to get more information about that task.

The summary section has a button to kill the currently running query. There are two visualizations
available in the summary section: task execution and timeline. The full JSON document containing
information and statistics about the query is available by clicking the *JSON* link. These visualizations
and other statistics can be used to analyze where time is being spent for a query.

Configuring query history
-------------------------

The following configuration properties affect :doc:`how query history
is collected </admin/properties-query-management>` for display in the Web UI:

* ``query.min-expire-age``
* ``query.max-history``
