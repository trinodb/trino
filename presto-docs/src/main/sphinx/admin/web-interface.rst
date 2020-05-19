======
Web UI
======

Presto provides a web-based user interface (UI) for monitoring a Presto cluster
and managing queries. The Web UI is accessible on the coordinator via
HTTP/HTTPS, using the corresponding port number specified in the coordinator
:ref:`config_properties`. It can be configured with :ref:`specific related
properties <web-ui-properties>`.

You can disable the Web UI with the ``web-ui.enabled`` property if you do not
want to expose the information from the Web UI to your users.

.. _web-ui-authentication:

Authentication
--------------

The Web UI requires users to log in. If Presto is not configured to require
authentication, then any username can be used, and no password is required or
allowed. Typically, users should login with the same username that they use for
running queries.

When the server is configured to use HTTPS, a :doc:`password authenticator
</develop/password-authenticator>` such as :doc:`LDAP </security/ldap>` or
:doc:`password file </security/password-file>` must be configured in order to
use the Web UI. In these cases, the authentication type defaults to the required
``form`` value. It triggers the Web UI to display a login form, that accepts the
relevant username and password, and verifies against the authentication backend.

If you require the Web UI to be accessible to anyone, you can set the
authentication type to ``fixed`` and configure the ``web-ui.user``. As a result
any interaction in the Web UI, such as killing a running query, is logged with
the configured user.

Alternatively the authentication type can be set to use certificate
authentication (``certificate``), :doc:`Kerberos authentication
</security/server>` (``kerberos``), or JWT authentication (``jwt``).

After successful login you have access to view and kill queries submitted to
Presto under the same username. Queries of other users may be visible, if you
have permissions to view other users queries. You can use :ref:`query rules
<query_rules>` with the built-in access control to configure access.

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
