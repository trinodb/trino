=======================
Elasticsearch connector
=======================

.. raw:: html

  <img src="../_static/img/elasticsearch.png" class="connector-logo">

The Elasticsearch Connector allows access to `Elasticsearch <https://www.elastic.co/products/elasticsearch>`_ data from Trino.
This document describes how to setup the Elasticsearch Connector to run SQL queries against Elasticsearch.

.. note::

    Elasticsearch (6.6.0 or later) or OpenSearch (1.1.0 or later) is required.

Configuration
-------------

To configure the Elasticsearch connector, create a catalog properties file
``etc/catalog/example.properties`` with the following contents, replacing the
properties as appropriate for your setup:

.. code-block:: text

    connector.name=elasticsearch
    elasticsearch.host=localhost
    elasticsearch.port=9200
    elasticsearch.default-schema-name=default

Configuration properties
^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table:: Elasticsearch configuration properties
    :widths: 35, 55, 10
    :header-rows: 1

    * - Property name
      - Description
      - Default
    * - ``elasticsearch.host``
      - The comma-separated list of host names for the Elasticsearch node to
        connect to. This property is required.
      -
    * - ``elasticsearch.port``
      - Port of the Elasticsearch node to connect to.
      - ``9200``
    * - ``elasticsearch.default-schema-name``
      - The schema that contains all tables defined without a qualifying schema
        name.
      - ``default``
    * - ``elasticsearch.scroll-size``
      - Sets the maximum number of hits that can be returned with each
        Elasticsearch scroll request.
      - ``1000``
    * - ``elasticsearch.scroll-timeout``
      - Amount of time Elasticsearch keeps the
        `search context <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html#scroll-search-context>`_
        alive for scroll requests.
      - ``1m``
    * - ``elasticsearch.request-timeout``
      - Timeout value for all Elasticsearch requests.
      - ``10s``
    * - ``elasticsearch.connect-timeout``
      - Timeout value for all Elasticsearch connection attempts.
      - ``1s``
    * - ``elasticsearch.backoff-init-delay``
      - The minimum duration between backpressure retry attempts for a single
        request to Elasticsearch. Setting it too low might overwhelm an already
        struggling ES cluster.
      - ``500ms``
    * - ``elasticsearch.backoff-max-delay``
      - The maximum duration between backpressure retry attempts for a single
        request to Elasticsearch.
      - ``20s``
    * - ``elasticsearch.max-retry-time``
      - The maximum duration across all retry attempts for a single request to
        Elasticsearch.
      - ``20s``
    * - ``elasticsearch.node-refresh-interval``
      - How often the list of available Elasticsearch nodes is refreshed.
      - ``1m``
    * - ``elasticsearch.ignore-publish-address``
      - Disables using the address published by Elasticsearch to connect for
        queries.
      -

TLS security
------------

The Elasticsearch connector provides additional security options to support
Elasticsearch clusters that have been configured to use TLS.

If your cluster has globally-trusted certificates, you should only need to
enable TLS. If you require custom configuration for certificates, the connector
supports key stores and trust stores in PEM or Java Key Store (JKS) format.

The allowed configuration values are:

.. list-table:: TLS Security Properties
    :widths: 40, 60
    :header-rows: 1

    * - Property name
      - Description
    * - ``elasticsearch.tls.enabled``
      - Enables TLS security.
    * - ``elasticsearch.tls.keystore-path``
      - The path to the :doc:`PEM </security/inspect-pem>` or
        :doc:`JKS </security/inspect-jks>` key store.
    * - ``elasticsearch.tls.truststore-path``
      - The path to :doc:`PEM </security/inspect-pem>` or
        :doc:`JKS </security/inspect-jks>` trust store.
    * - ``elasticsearch.tls.keystore-password``
      - The key password for the key store specified by
        ``elasticsearch.tls.keystore-path``.
    * - ``elasticsearch.tls.truststore-password``
      - The key password for the trust store specified by
        ``elasticsearch.tls.truststore-path``.

.. _elasticesearch-type-mapping:

Type mapping
------------

Because Trino and Elasticsearch each support types that the other does not, this
connector :ref:`maps some types <type-mapping-overview>` when reading data.

Elasticsearch type to Trino type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The connector maps Elasticsearch types to the corresponding Trino types
according to the following table:

.. list-table:: Elasticsearch type to Trino type mapping
  :widths: 30, 30, 50
  :header-rows: 1

  * - Elasticsearch type
    - Trino type
    - Notes
  * - ``BOOLEAN``
    - ``BOOLEAN``
    -
  * - ``DOUBLE``
    - ``DOUBLE``
    -
  * - ``FLOAT``
    - ``REAL``
    -
  * - ``BYTE``
    - ``TINYINT``
    -
  * - ``SHORT``
    - ``SMALLINT``
    -
  * - ``INTEGER``
    - ``INTEGER``
    -
  * - ``LONG``
    - ``BIGINT``
    -
  * - ``KEYWORD``
    - ``VARCHAR``
    -
  * - ``TEXT``
    - ``VARCHAR``
    -
  * - ``DATE``
    - ``TIMESTAMP``
    - For more information, see :ref:`elasticsearch-date-types`.
  * - ``IPADDRESS``
    - ``IP``
    -

No other types are supported.

.. _elasticsearch-array-types:

Array types
^^^^^^^^^^^

Fields in Elasticsearch can contain `zero or more values <https://www.elastic.co/guide/en/elasticsearch/reference/current/array.html>`_
, but there is no dedicated array type. To indicate a field contains an array, it can be annotated in a Trino-specific structure in
the `_meta <https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-meta-field.html>`_ section of the index mapping.

For example, you can have an Elasticsearch index that contains documents with the following structure:

.. code-block:: json

    {
        "array_string_field": ["trino","the","lean","machine-ohs"],
        "long_field": 314159265359,
        "id_field": "564e6982-88ee-4498-aa98-df9e3f6b6109",
        "timestamp_field": "1987-09-17T06:22:48.000Z",
        "object_field": {
            "array_int_field": [86,75,309],
            "int_field": 2
        }
    }

The array fields of this structure can be defined by using the following command to add the field
property definition to the ``_meta.trino`` property of the target index mapping.

.. code-block:: shell

    curl --request PUT \
        --url localhost:9200/doc/_mapping \
        --header 'content-type: application/json' \
        --data '
    {
        "_meta": {
            "trino":{
                "array_string_field":{
                    "isArray":true
                },
                "object_field":{
                    "array_int_field":{
                        "isArray":true
                    }
                },
            }
        }
    }'

.. note::

    It is not allowed to use ``asRawJson`` and ``isArray`` flags simultaneously for the same column.

.. _elasticsearch-date-types:

Date types
^^^^^^^^^^

Elasticsearch supports a wide array of `date`_ formats including
`built-in date formats`_ and also `custom date formats`_.
The Elasticsearch connector supports only the default ``date`` type. All other
date formats including `built-in date formats`_ and `custom date formats`_ are
not supported. Dates with the `format`_ property are ignored.

.. _date: https://www.elastic.co/guide/en/elasticsearch/reference/current/date.html
.. _built-in date formats: https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html#built-in-date-formats
.. _custom date formats: https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html#custom-date-formats
.. _format: https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html#mapping-date-format


Raw JSON transform
^^^^^^^^^^^^^^^^^^

There are many occurrences where documents in Elasticsearch have more complex
structures that are not represented in the mapping. For example, a single
``keyword`` field can have widely different content including a single
``keyword`` value, an array, or a multidimensional ``keyword`` array with any
level of nesting.

.. code-block:: shell

    curl --request PUT \
        --url localhost:9200/doc/_mapping \
        --header 'content-type: application/json' \
        --data '
    {
        "properties": {
            "array_string_field":{
                "type": "keyword"
            }
        }
    }'

Notice for the ``array_string_field`` that all the following documents are legal
for Elasticsearch. See the `Elasticsearch array documentation
<https://www.elastic.co/guide/en/elasticsearch/reference/current/array.html>`_
for more details.

.. code-block:: json

    [
        {
            "array_string_field": "trino"
        },
        {
            "array_string_field": ["trino","is","the","besto"]
        },
        {
            "array_string_field": ["trino",["is","the","besto"]]
        },
        {
            "array_string_field": ["trino",["is",["the","besto"]]]
        }
    ]

Further, Elasticsearch supports types, such as
`dense_vector
<https://www.elastic.co/guide/en/elasticsearch/reference/current/dense-vector.html>`_,
that are not supported in Trino. New types are constantly emerging which can
cause parsing exceptions for users that use of these types in Elasticsearch. To
manage all of these scenarios, you can transform fields to raw JSON by
annotating it in a Trino-specific structure in the `_meta
<https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-meta-field.html>`_
section of the index mapping. This indicates to Trino that the field, and all
nested fields beneath, need to be cast to a ``VARCHAR`` field that contains
the raw JSON content. These fields can be defined by using the following command
to add the field property definition to the ``_meta.presto`` property of the
target index mapping.

.. code-block:: shell

    curl --request PUT \
        --url localhost:9200/doc/_mapping \
        --header 'content-type: application/json' \
        --data '
    {
        "_meta": {
            "presto":{
                "array_string_field":{
                    "asRawJson":true
                }
            }
        }
    }'

This preceding configurations causes Trino to return the ``array_string_field``
field as a ``VARCHAR`` containing raw JSON. You can parse these fields with the
:doc:`built-in JSON functions </functions/json>`.

.. note::

    It is not allowed to use ``asRawJson`` and ``isArray`` flags simultaneously for the same column.

Special columns
---------------

The following hidden columns are available:

======= =======================================================
Column  Description
======= =======================================================
_id     The Elasticsearch document ID
_score  The document score returned by the Elasticsearch query
_source The source of the original document
======= =======================================================

.. _elasticsearch-full-text-queries:

Full text queries
-----------------

Trino SQL queries can be combined with Elasticsearch queries by providing the `full text query`_
as part of the table name, separated by a colon. For example:

.. code-block:: sql

    SELECT * FROM "tweets: +trino SQL^2"

.. _full text query: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#query-string-syntax

Predicate push down
-------------------

The connector supports predicate push down of below data types:

============= ============= =============
Elasticsearch Trino         Supports
============= ============= =============
``binary``    ``VARBINARY`` ``NO``
``boolean``   ``BOOLEAN``   ``YES``
``double``    ``DOUBLE``    ``YES``
``float``     ``REAL``      ``YES``
``byte``      ``TINYINT``   ``YES``
``short``     ``SMALLINT``  ``YES``
``integer``   ``INTEGER``   ``YES``
``long``      ``BIGINT``    ``YES``
``keyword``   ``VARCHAR``   ``YES``
``text``      ``VARCHAR``   ``NO``
``date``      ``TIMESTAMP`` ``YES``
``ip``        ``IPADDRESS`` ``NO``
(all others)  (unsupported) (unsupported)
============= ============= =============

AWS authorization
-----------------

To enable AWS authorization using IAM policies, the ``elasticsearch.security`` option needs to be set to ``AWS``.
Additionally, the following options need to be configured appropriately:

================================================ ==================================================================
Property name                                    Description
================================================ ==================================================================
``elasticsearch.aws.region``                     AWS region or the Elasticsearch endpoint. This option is required.

``elasticsearch.aws.access-key``                 AWS access key to use to connect to the Elasticsearch domain.
                                                 If not set, the Default AWS Credentials Provider chain will be used.

``elasticsearch.aws.secret-key``                 AWS secret key to use to connect to the Elasticsearch domain.
                                                 If not set, the Default AWS Credentials Provider chain will be used.

``elasticsearch.aws.iam-role``                   Optional ARN of an IAM Role to assume to connect to the Elasticsearch domain.
                                                 Note: the configured IAM user has to be able to assume this role.

``elasticsearch.aws.external-id``                Optional external ID to pass while assuming an AWS IAM Role.
================================================ ==================================================================

Password authentication
-----------------------

To enable password authentication, the ``elasticsearch.security`` option needs to be set to ``PASSWORD``.
Additionally the following options need to be configured appropriately:

================================================ ==================================================================
Property name                                    Description
================================================ ==================================================================
``elasticsearch.auth.user``                      User name to use to connect to Elasticsearch.
``elasticsearch.auth.password``                  Password to use to connect to Elasticsearch.
================================================ ==================================================================

.. _elasticsearch-sql-support:

SQL support
-----------

The connector provides :ref:`globally available <sql-globally-available>` and
:ref:`read operation <sql-read-operations>` statements to access data and
metadata in the Elasticsearch catalog.

Table functions
---------------

The connector provides specific :doc:`table functions </functions/table>` to
access Elasticsearch.

.. _elasticsearch-raw-query-function:

``raw_query(varchar) -> table``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``raw_query`` function allows you to query the underlying database directly.
This function requires `Elastic Query DSL
<https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html>`_
syntax, because the full query is pushed down and processed in Elasticsearch.
This can be useful for accessing native features which are not available in
Trino or for improving query performance in situations where running a query
natively may be faster.

.. include:: query-passthrough-warning.fragment

The ``raw_query`` function requires three parameters:

* ``schema``: The schema in the catalog that the query is to be executed on.
* ``index``: The index in Elasticsearch to be searched.
* ``query``: The query to be executed, written in Elastic Query DSL.

Once executed, the query returns a single row containing the resulting JSON
payload returned by Elasticsearch.

For example, query the ``example`` catalog and use the ``raw_query`` table
function to search for documents in the ``orders`` index where the country name
is ``ALGERIA``::

    SELECT
      *
    FROM
      TABLE(
        example.system.raw_query(
          schema => 'sales',
          index => 'orders',
          query => '{
            "query": {
              "match": {
                "name": "ALGERIA"
              }
            }
          }'
        )
      );

.. include:: query-table-function-ordering.fragment
