=======================
Elasticsearch connector
=======================

Overview
--------

The Elasticsearch Connector allows access to `Elasticsearch <https://www.elastic.co/products/elasticsearch>`_ data from Trino.
This document describes how to setup the Elasticsearch Connector to run SQL queries against Elasticsearch.

.. note::

    Elasticsearch 6.0.0 or later is required.

Configuration
-------------

To configure the Elasticsearch connector, create a catalog properties file
``etc/catalog/elasticsearch.properties`` with the following contents,
replacing the properties as appropriate:

.. code-block:: text

    connector.name=elasticsearch
    elasticsearch.host=localhost
    elasticsearch.port=9200
    elasticsearch.default-schema-name=default

Configuration properties
------------------------

The following configuration properties are available:

``elasticsearch.host``
^^^^^^^^^^^^^^^^^^^^^^

Hostname of the Elasticsearch node to connect to.

This property is required.

``elasticsearch.port``
^^^^^^^^^^^^^^^^^^^^^^

Specifies the port of the Elasticsearch node to connect to.

This property is optional; the default is ``9200``.

``elasticsearch.default-schema-name``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Defines the schema that contains all tables defined without
a qualifying schema name.

This property is optional; the default is ``default``.

``elasticsearch.scroll-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This property defines the maximum number of hits that can be returned with each
Elasticsearch scroll request.

This property is optional; the default is ``1000``.

``elasticsearch.scroll-timeout``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This property defines the amount of time (ms) Elasticsearch keeps the `search context alive`_ for scroll requests

This property is optional; the default is ``1m``.

.. _search context alive: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html#scroll-search-context

``elasticsearch.request-timeout``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This property defines the timeout value for all Elasticsearch requests.

This property is optional; the default is ``10s``.

``elasticsearch.connect-timeout``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This property defines the timeout value for all Elasticsearch connection attempts.

This property is optional; the default is ``1s``.

``elasticsearch.backoff-init-delay``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This property defines the minimum duration between backpressure retry attempts for a single request to Elasticsearch.
Setting it too low might overwhelm an already struggling ES cluster.

This property is optional; the default is ``500ms``.

``elasticsearch.backoff-max-delay``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This property defines the maximum duration between backpressure retry attempts for a single request to Elasticsearch.

This property is optional; the default is ``20s``.

``elasticsearch.max-retry-time``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This property defines the maximum duration across all retry attempts for a single request to Elasticsearch.

This property is optional; the default is ``20s``.

``elasticsearch.node-refresh-interval``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This property controls how often the list of available Elasticsearch nodes is refreshed.

This property is optional; the default is ``1m``.

``elasticsearch.ignore-publish-address``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Enable or disable using the address published by Elasticsearch to connect for
queries.

TLS security
------------

The Elasticsearch connector provides additional security options to support Elasticsearch clusters that have been configured to use TLS.

The connector supports key stores and trust stores in PEM or Java Key Store (JKS) format. The allowed configuration values are:

``elasticsearch.tls.keystore-path``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The path to the PEM or JKS key store. This file must be readable by the operating system user running Trino.

This property is optional.

``elasticsearch.tls.truststore-path``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The path to PEM or JKS trust store. This file must be readable by the operating system user running Trino.

This property is optional.

``elasticsearch.tls.keystore-password``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The key password for the key store specified by ``elasticsearch.tls.keystore-path``.

This property is optional.

``elasticsearch.tls.truststore-password``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The key password for the trust store specified by ``elasticsearch.tls.truststore-path``.

This property is optional.

Data types
----------

The data type mappings are as follows:

============= =============
Elasticsearch Trino
============= =============
``binary``    ``VARBINARY``
``boolean``   ``BOOLEAN``
``double``    ``DOUBLE``
``float``     ``REAL``
``byte``      ``TINYINT``
``short``     ``SMALLINT``
``integer``   ``INTEGER``
``long``      ``BIGINT``
``keyword``   ``VARCHAR``
``text``      ``VARCHAR``
``date``      ``TIMESTAMP``
``ip``        ``IPADDRESS``
(all others)  (unsupported)
============= =============

.. _elasticsearch-array-types:

Array types
^^^^^^^^^^^

Fields in Elasticsearch can contain `zero or more values <https://www.elastic.co/guide/en/elasticsearch/reference/current/array.html>`_
, but there is no dedicated array type. To indicate a field contains an array, it can be annotated in a Trino-specific structure in
the `_meta <https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-meta-field.html>`_ section of the index mapping.

For example, you can have an Elasticsearch index that contains documents with the following structure:

.. code-block:: json

    {
        "array_string_field": ["trino","is","the","besto"],
        "long_field": 314159265359,
        "id_field": "564e6982-88ee-4498-aa98-df9e3f6b6109",
        "timestamp_field": "1987-09-17T06:22:48.000Z",
        "object_field": {
            "array_int_field": [86,75,309],
            "int_field": 2
        }
    }

The array fields of this structure can be defined by using the following command to add the field
property definition to the ``_meta.presto`` property of the target index mapping.

.. code-block:: shell

    curl --request PUT \
        --url localhost:9200/doc/_mapping \
        --header 'content-type: application/json' \
        --data '
    {
        "_meta": {
            "presto":{
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


Pass-through queries
--------------------

The Elasticsearch connector allows you to embed any valid Elasticsearch query,
that uses the `Elasticsearch Query DSL
<https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html>`_
in your SQL query.

The results can then be used in any SQL statement, wrapping the Elasticsearch
query. The syntax extends the syntax of the enhanced Elasticsearch table names
with the following::

    SELECT * FROM es.default."<index>$query:<es-query>"

The Elasticsearch query string ``es-query`` is base32-encoded to avoid having to
deal with escaping quotes and case sensitivity issues in table identifiers.

The result of these query tables is a table with a single row and a single
column named ``result`` of type VARCHAR. It contains the JSON payload returned
by Elasticsearch, and can be processed with the :doc:`built-in JSON functions
</functions/json>`.

AWS authorization
-----------------

To enable AWS authorization using IAM policies, the ``elasticsearch.security`` option needs to be set to ``AWS``.
Additionally, the following options need to be configured appropriately:

================================================ ==================================================================
Property Name                                    Description
================================================ ==================================================================
``elasticsearch.aws.region``                     AWS region or the Elasticsearch endpoint. This option is required.
``elasticsearch.aws.access-key``                 AWS access key to use to connect to the Elasticsearch domain.
``elasticsearch.aws.secret-key``                 AWS secret key to use to connect to the Elasticsearch domain.
================================================ ==================================================================

Password authentication
-----------------------

To enable password authentication, the ``elasticsearch.security`` option needs to be set to ``PASSWORD``.
Additionally the following options need to be configured appropriately:

================================================ ==================================================================
Property Name                                    Description
================================================ ==================================================================
``elasticsearch.auth.user``                      User name to use to connect to Elasticsearch.
``elasticsearch.auth.password``                  Password to use to connect to Elasticsearch.
================================================ ==================================================================
