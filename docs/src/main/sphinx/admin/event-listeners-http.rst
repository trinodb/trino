===================
HTTP event listener
===================

The HTTP event listener plugin allows streaming of query events, encoded in
JSON format, to an external service for further processing, by POSTing them
to a specified URI.

Rationale
---------

This event listener is a simple first step into better understanding the usage
of a datalake using query events provided by Trino. These can provide CPU and memory
usage metrics, what data is being accessed with resolution down to specific columns,
and metadata about the query processing.

Running the capture system separate from Trino reduces the performance impact and
avoids downtime for non-client-facing changes.

Requirements
------------

You need to perform the following steps:

* Provide an HTTP/S service that accepts POST events with a JSON body.
* Configure ``http-event-listener.ingest-uri`` in the event listener properties file
  with the URI of the service.
* Detail the events to send in the :ref:`http_event_listener_configuration` section.

.. _http_event_listener_configuration:

Configuration
-------------

To configure the HTTP event listener plugin, create an event listener properties
file in ``etc`` named ``http-event-listener.properties`` with the following contents
as an example:

.. code-block:: properties

    event-listener.name=http
    http-event-listener.log-created=true
    http-event-listener.connect-ingest-uri=<your ingest URI>

And set add ``etc/http-event-listener.properties`` to ``event-listener.config-files``
in :ref:`config_properties`:

.. code-block:: properties

    event-listener.config-files=etc/http-event-listener.properties,...

Configuration properties
^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 40, 40, 20
  :header-rows: 1

  * - Property name
    - Description
    - Default

  * - http-event-listener.log-created
    - Enable the plugin to log ``QueryCreatedEvent`` events
    - ``false``

  * - http-event-listener.log-completed
    - Enable the plugin to log ``QueryCcompletedEvent`` events
    - ``false``

  * - http-event-listener.log-split
    - Enable the plugin to log ``SplitCompletedEvent`` events
    - ``false``

  * - http-event-listener.connect-ingest-uri
    - The URI that the plugin will POST events to
    - None. See the `requirements <#requirements>`_ section.

  * - http-event-listener.connect-http-headers
    - List of custom HTTP headers to be sent along with the events. See
      :ref:`http_event_listener_custom_headers` for more details
    - Empty

  * - http-event-listener.connect-retry-count
    - The number of retries on server error. A server is considered to be
      in an error state when the response code is 500 or higher
    - ``0``

  * - http-event-listener.connect-retry-delay
    - Duration for which to delay between attempts to send a request
    - ``1s``

  * - http-event-listener.connect-backoff-base
    - The base used for exponential backoff when retrying on server error.
      The formula used to calculate the delay is
      :math:`attemptDelay = retryDelay * backoffBase^{attemptCount}`.
      Attempt count starts from 0. Leave this empty or set to 1 to disable
      exponential backoff and keep constant delays
    - ``2``

  * - http-event-listener.connect-max-delay
    - The upper bound of a delay between 2 retries. This should be
      used with exponential backoff.
    - ``1m``

  * - http-event-listener.*
    - Pass configuration onto the HTTP client
    -

.. _http_event_listener_custom_headers:

Custom HTTP headers
^^^^^^^^^^^^^^^^^^^

Providing custom HTTP headers is a useful mechanism for sending metadata along with
event messages.

Providing headers follows the pattern of ``key:value`` pairs separated by commas:

.. code-block:: text

    http-event-listener.connect-http-headers="Header-Name-1:header value 1,Header-Value-2:header value 2,..."

If you need to use a comma(``,``) or colon(``:``) in a header name or value,
escape it using a backslash (``\``).

Keep in mind that these are static, so they can not carry information
taken from the event itself.
