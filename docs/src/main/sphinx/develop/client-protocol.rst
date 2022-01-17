======================
Trino client REST API
======================

The REST API allows clients to submit SQL queries to Trino and receive the
results. Clients include the CLI, the JDBC driver, and others provided by
the community. The preferred method to interact with Trino is using these
existing clients. This document provides details about the API for reference.
It can also be used to implement your own client, if necessary.

HTTP methods
------------

* A ``POST`` to ``/v1/statement`` runs the query string in the ``POST`` body,
  and returns a JSON document containing the query results.  If there are more
  results, the JSON document contains a ``nextUri`` URL attribute.
* A ``GET`` to the ``nextUri`` attribute returns the next batch of query results.
* A ``DELETE`` to ``nextUri`` terminates a running query.

Overview of query processing
----------------------------

A Trino client request is initiated by an HTTP ``POST`` to the endpoint
``/v1/statement``, with a ``POST`` body consisting of the SQL query string.
The caller may set various :ref:`client-request-headers`. The headers are
only required on the initial ``POST`` request, and not when following the
``nextUri`` links.

If the client request returns an HTTP 503, that means the server was busy,
and the client should try again in 50-100 milliseconds.  Any HTTP status other
than 503 or 200 means that query processing has failed.

The ``/v1/statement`` ``POST`` request returns a JSON document of type
``QueryResults``, as well as a collection of response headers.  The
``QueryResults`` document contains an ``error`` field of type
``QueryError`` if the query has failed, and if that object is not present,
the query succeeded.  Important members of ``QueryResults`` are documented
in the following sections.

If the ``data`` field of the JSON document is set, it contains a list of the
rows of data.  The ``columns`` field is set to a list of the
names and types of the columns returned by the query.  Most of the response
headers are treated like browser cookies by the client, and echoed back
as request headers in subsequent client requests, as documented below.

If the JSON document returned by the ``POST`` to ``/v1/statement`` does not
contain a ``nextUri`` link, the query has completed, either successfully or
unsuccessfully, and no additional requests need to be made.  If the
``nextUri`` link is present in the document, there are more query results
to be fetched.  The client should loop executing a ``GET`` request
to the ``nextUri`` returned in the ``QueryResults`` response object until
``nextUri`` is absent from the response.

The ``status`` field of the JSON document is for human consumption only, and
provides a hint about the query state.  It can not be used to tell if the
query is finished.

Important ``QueryResults`` attributes
-------------------------------------

The most important attributes of the ``QueryResults`` JSON document returned by the REST API
endpoints are listed in this table.  Refer to the class ``io.trino.client.QueryResults`` in
module ``trino-client`` for more details.

.. list-table:: ``QueryResults attributes``
  :widths: 25, 55
  :header-rows: 1

  * - Attribute
    - Description
  * - ``id``
    - The ID of the query.
  * - ``nextUri``
    - If present, the URL to use for subsequent ``GET`` or
      ``DELETE`` requests.  If not present, the query is complete or
      ended in error.
  * - ``columns``
    - A list of the names and types of the columns returned by the query.
  * - ``data``
    - The ``data`` attribute contains a list of the rows returned by the
      query request.  Each row is itself a list that holds values of the
      columns in the row, in the order specified by the ``columns``
      attribute.
  * - ``updateType``
    - A human-readable string representing the operation.  For a
      ``CREATE TABLE`` request, the ``updateType`` is
      "CREATE TABLE"; for ``SET SESSION`` it is "SET SESSION"; etc.
  * - ``error``
    - If query failed, the ``error`` attribute contains a
      ``QueryError`` object.  That object contains a ``message``, an
      ``errorCode`` and other information about the error.  See the
      ``io.trino.client.QueryError`` class in module ``trino-client``
      for more details.


``QueryResults`` diagnostic attributes
--------------------------------------

These ``QueryResults`` data members may be useful in tracking down problems:

.. list-table:: ``QueryResults diagnostic attributes``
  :widths: 20, 20, 40
  :header-rows: 1

  * - Attribute
    - Type
    - Description
  * - ``queryError``
    - ``QueryError``
    - Non-null only if the query resulted in an error.
  * - ``failureInfo``
    - ``FailureInfo``
    - ``failureInfo`` has detail on the reason for the failure, including
      a stack trace, and ``FailureInfo.errorLocation``, providing the
      query line number and column number where the failure was detected.
  * - ``warnings``
    - ``List<TrinoWarning>``
    - A usually-empty list of warnings.
  * - ``statementStats``
    - ``StatementStats``
    - A class containing statistics about the query execution.  Of
      particular interest is ``StatementStats.rootStage``, of type
      ``StageStats``, providing statistics on the execution of each of
      the stages of query processing.

.. _client-request-headers:

Client request headers
----------------------

This table lists all supported client request headers.  Many of the
headers can be updated in the client as response headers, and supplied
in subsequent requests, just like browser cookies.

.. list-table:: Client request headers
  :widths: 30, 50
  :header-rows: 1

  * - Header name
    - Description
  * - ``X-Trino-User``
    - Specifies the session user. If not supplied, the session user is
      automatically determined via :doc:`/security/user-mapping`.
  * - ``X-Trino-Source``
    - For reporting purposes, this supplies the name of the software
      that submitted the query.
  * - ``X-Trino-Catalog``
    - The catalog context for query processing.  Set by response
      header ``X-Trino-Set-Catalog``.
  * - ``X-Trino-Schema``
    - The schema context for query processing.  Set by response
      header ``X-Trino-Set-Schema``.
  * - ``X-Trino-Time-Zone``
    - The timezone for query processing. Defaults to the timezone
      of the Trino cluster, and not the timezone of the client.
  * - ``X-Trino-Language``
    - The language to use when processing the query and formatting
      results, formatted as a Java ``Locale`` string, e.g., ``en-US``
      for US English.  The language of the
      session can be set on a per-query basis using the
      ``X-Trino-Language`` HTTP header.
  * - ``X-Trino-Trace-Token``
    - Supplies a trace token to the Trino engine to help identify
      log lines that originate with this query request.
  * - ``X-Trino-Session``
    - Supplies a comma-separated list of name=value pairs as session
      properties.  When the Trino client run a
      ``SET SESSION name=value`` query, the name=value pair
      is returned in the ``X-Set-Trino-Session`` response header,
      and added to the client's list of session properties.
      If the response header ``X-Trino-Clear-Session`` is returned,
      its value is the name of a session property that is
      removed from the client's accumulated list.
  * - ``X-Trino-Role``
    - Sets the "role" for query processing.  A "role" is represents
      a collection of permissions.  Set by response header
      ``X-Trino-Set-Role``.  See doc:/sql/create-role to
      understand roles.
  * - ``X-Trino-Prepared-Statement``
    - A comma-separated list of the name=value pairs, where the
      names are names of previously prepared SQL statements, and
      the values are keys that identify the executable form of the
      named prepared statements.
  * - ``X-Trino-Transaction-Id``
    - The transaction ID to use for query processing.  Set
      by response header ``X-Trino-Started-Transaction-Id`` and
      cleared by ``X-Trino-Clear-Transaction-Id``.
  * - ``X-Trino-Client-Info``
    - Contains arbitrary information about the client program
      submitting the query.
  * - ``X-Trino-Client-Tags``
    - A comma-separated list of "tag" strings, used to identify
      Trino resource groups.
  * - ``X-Trino-Resource-Estimate``
    - A comma-separated list of ``resource=value`` type
      assigments.  The possible choices of ``resource`` are
      ``EXECUTION_TIME``, ``CPU_TIME``,  ``PEAK_MEMORY`` and
      ``PEAK_TASK_MEMORY``.  ``EXECUTION_TIME`` and ``CPU_TIME``
      have values specified as airlift ``Duration`` strings
      The format is a double precision number followed by
      a ``TimeUnit`` string, e.g., of ``s`` for seconds,
      ``m`` for minutes, ``h`` for hours, etc.  "PEAK_MEMORY" and
      "PEAK_TASK_MEMORY" are specified as as airlift ``DataSize`` strings,
      whose format is an integer followed by ``B`` for bytes; ``kB`` for
      kilobytes; ``mB`` for megabytes, ``gB`` for gigabytes, etc.
  * - ``X-Trino-Extra-Credential``
    - Provides extra credentials to the connector.  The header is
      a name=value string that is saved in the session ``Identity``
      object.  The name and value are only meaningful to the connector.

Client response headers
-----------------------

This table lists the supported client response headers.  After receiving a
response, a client must update the request headers used in
subsequent requests to be consistent with the response headers received.

.. list-table:: Client response headers
  :widths: 30, 50
  :header-rows: 1

  * - Header name
    - Description
  * - ``X-Trino-Set-Catalog``
    - Instructs the client to set the catalog in the
      ``X-Trino-Catalog`` request header in subsequent client requests.
  * - ``X-Trino-Set-Schema``
    - Instructs the client to set the schema in the
      ``X-Trino-Schema`` request header in subsequent client requests.
  * - ``X-Trino-Set-Session``
    - The value of the ``X-Trino-Set-Session`` response header is a
      string of the form *property* = *value*.  It
      instructs the client include session property *property* with value
      *value* in the ``X-Trino-Session`` header of subsequent
      client requests.
  * - ``X-Trino-Clear-Session``
    - Instructs the client to remove the session property with the
      whose name is the value of the ``X-Trino-Clear-Session`` header
      from the list of session properties
      in the ``X-Trino-Session`` header in subsequent client requests.
  * - ``X-Trino-Set-Role``
    - Instructs the client to set ``X-Trino-Role`` request header to the
      catalog role supplied by the ``X-Trino-Set-Role`` header
      in subsequent client requests.
  * - ``X-Trino-Added-Prepare``
    - Instructs the client to add the name=value pair to the set of
      prepared statements in the ``X-Trino-Prepared-Statements``
      request header in subsequent client requests.
  * - ``X-Trino-Deallocated-Prepare``
    - Instructs the client to remove the prepared statement whose name
      is the value of the ``X-Trino-Deallocated-Prepare`` header from
      the client's list of prepared statements sent in the
      ``X-Trino-Prepared-Statements`` request header in subsequent client
      requests.
  * - ``X-Trino-Started-Transaction-Id``
    - Provides the transaction ID that the client should pass back in the
      ``X-Trino-Transaction-Id`` request header in subsequent requests.
  * - ``X-Trino-Clear-Transaction-Id``
    - Instructs the client to clear the ``X-Trino-Transaction-Id`` request
      header in subsequent requests.

``ProtocolHeaders``
-------------------

Class ``io.trino.client.ProtocolHeaders``, in module ``trino-client``,
enumerates all the HTTP request and response headers allowed by the
Trino client REST API.
