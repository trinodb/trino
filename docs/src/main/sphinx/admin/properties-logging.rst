==================
Logging properties
==================

``log.annotation-file``
^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-string`

An optional properties file that contains annotations to be included with
each log message. This can be used to include machine-specific or
environment-specific information into logs which are centrally aggregated.
The annotation values can contain references to environment variables.

.. code-block:: properties

    environment=production
    host=${ENV:HOSTNAME}

``log.format``
^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-string`
* **Default value:** ``TEXT``

The file format for log records. Can be set to either ``TEXT`` or ``JSON``. When
set to ``JSON``, the log record is formatted as a JSON object, one record per
line. Any newlines in the field values, such as exception stack traces, are
escaped as normal in the JSON object. This allows for capturing and indexing
exceptions as singular fields in a logging search system.

``log.path``
^^^^^^^^^^^^

* **Type:** :ref:`prop-type-string`

The path to the log file used by Trino. The path is relative to the data
directory, configured to ``var/log/server.log`` by the launcher script as
detailed in :ref:`running-trino`. Alternatively, you can write logs to separate
the process (typically running next to Trino as a sidecar process) via the TCP
protocol by using a log path of the format ``tcp://host:port``.

``log.max-size``
^^^^^^^^^^^^^^^^
* **Type:** :ref:`prop-type-data-size`
* **Default value:** ``100MB``

The maximum file size for the general application log file.

``log.max-total-size``
^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-data-size`
* **Default value:** ``1GB``

The maximum file size for all general application log files combined.

``log.compression``
^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-string`
* **Default value:** ``GZIP``

The compression format for rotated log files. Can be set to either ``GZIP`` or ``NONE``. When
set to ``NONE``, compression is disabled.

``http-server.log.enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-boolean`
* **Default value:** ``true``

Flag to enable or disable logging for the HTTP server.

``http-server.log.compression.enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-boolean`
* **Default value:** ``true``

Flag to enable or disable compression of the log files of the HTTP server.

``http-server.log.path``
^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-string`
* **Default value:** ``var/log/http-request.log``

The path to the log file used by the HTTP server. The path is relative to
the data directory, configured by the launcher script as detailed in
:ref:`running-trino`.

``http-server.log.max-history``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-integer`
* **Default value:** ``15``

The maximum number of log files for the HTTP server to use, before
log rotation replaces old content.

``http-server.log.max-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-data-size`
* **Default value:** ``unlimited``

The maximum file size for the log file of the HTTP server. Defaults to
``unlimited``, setting a :ref:`prop-type-data-size` value limits the file size
to that value.
