======
Web UI
======

Presto provides a web-based user interface (UI) for monitoring a Presto cluster
and managing queries. The Web UI is accessible on the coordinator via
HTTP/HTTPS, using the corresponding port number specified in the coordinator
:ref:`config_properties`. It can be configured with :doc:`/admin/properties-web-interface`.

The Web UI can be disabled entirely with the ``web-ui.enabled`` property.

.. _web-ui-authentication:

Authentication
--------------

The Web UI requires users to authenticate. If Presto is not configured to require
authentication, then any username can be used, and no password is required or
allowed. Typically, users should login with the same username that they use for
running queries.

Accessing the Web UI over HTTPS requires configuring an authentication type for
the Web UI or the Presto server. If no authentication type is configured for the
Web UI, then it will chosen based on the Presto server authentication type.

If no system access control is installed, then all users will be able to view and kill
any query. This can be restricted by using :ref:`query rules <query_rules>` with the
:doc:`/security/built-in-system-access-control`. Users always have permission to view
or kill their own queries.

Password Authentication
^^^^^^^^^^^^^^^^^^^^^^^

Typically, a :doc:`password authenticator </develop/password-authenticator>`
such as :doc:`LDAP </security/ldap>` or :doc:`password file </security/password-file>`
is used to secure both the Presto server and the Web UI. When the Presto server
is configured to use a password authenticator, the Web UI authentication type
is automatically set to ``form``. The Web UI will display a login form that accepts
a username and password.

Fixed User Authentication
^^^^^^^^^^^^^^^^^^^^^^^^^

If you require the Web UI to be accessible without authentication, you can set a fixed
username that will be used for all Web UI access by setting the authentication type to
``fixed`` and setting the username with the ``web-ui.user`` configuration property.
If there is a system access control installed, this user must have permission to view
(and possibly to kill) queries.

Other Authentication Types
^^^^^^^^^^^^^^^^^^^^^^^^^^

The following Web UI authentication types are also supported:

* ``certificate``
* ``kerberos``
* ``jwt``

For these authentication types, the username is defined by :doc:`/security/user-mapping`.

.. _web-ui-overview:

User Interface Overview
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
