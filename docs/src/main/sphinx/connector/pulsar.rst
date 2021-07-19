=================
Pulsar connector
=================
`Apache Pulsar <https://pulsar.apache.org/en/>`_ is a cloud-native, distributed messaging and streaming platform.

This connector allows the use of Pulsar topics as tables in Trino, such that each Pulsar message
in a Pulsar topic is presented as a row in Trino. And Pulsar topic schema is mapped to Trino
table schema.

Topics in Pulsar are stored as segments in `Apache BookKeeper <https://bookkeeper.apache.org/>`_. Each topic segment is replicated to some BookKeeper nodes,
which enables concurrent reads and high read throughput. You can configure the number of BookKeeper nodes,
and the default number is 3. In Trino Pulsar connector, data is read directly from BookKeeper,
so Trino workers can read concurrently from horizontally scalable number BookKeeper nodes.

This connector is **read-only** for now. It can only read data from Pulsar topics,
but cannot create topic or write to exiting topic yet.

To configure the Pulsar connector, create a catalog properties file ``etc/catalog/pulsar.properties``
A sample is provided:

.. code-block:: text

    connector.name=pulsar
    pulsar.broker-service-url=http://localhost:8080
    pulsar.zookeeper-uri=127.0.0.1:2181
    pulsar.max-entry-read-batch-size=100
    pulsar.target-num-splits=2
    pulsar.max-split-message-queue-size=10000
    pulsar.max-split-entry-queue-size=1000
    pulsar.max-split-queue-cache-size=-1
    pulsar.namespace-delimiter-rewrite-enable=false
    pulsar.rewrite-namespace-delimiter=/

    pulsar.managed-ledger-offload-driver = aws-s3
    pulsar.offloaders-directory = /pulsar/offloaders
    pulsar.managed-ledger-offload-max-threads = 2

    pulsar.offloader-properties = \
      {"s3ManagedLedgerOffloadBucket": "offload-bucket", \
      "s3ManagedLedgerOffloadRegion": "us-west-2", \
      "s3ManagedLedgerOffloadServiceEndpoint": "http://s3.amazonaws.com"}

    pulsar.auth-plugin =
    pulsar.auth-params =
    pulsar.tls-allow-insecure-connection =
    pulsar.tls-hostname-verification-enable =

    pulsar.tls-trust-cert-file-path =

    pulsar.bookkeeper-throttle-value = 0
    pulsar.bookkeeper-num-io-threads =
    pulsar.bookkeeper-num-worker-threads =
    pulsar.bookkeeper-use-v2-protocol=true
    pulsar.bookkeeper-explicit-interval=0

    pulsar.managed-ledger-cache-size-MB = 0
    pulsar.managed-ledger-num-worker-threads =
    pulsar.managed-ledger-num-scheduler-threads =

    pulsar.stats-provider-configs={"httpServerEnabled":"false", "prometheusStatsHttpPort":"9092", "prometheusStatsHttpEnable":"true"}

The following configuration properties are available:

================================================      =======================================================================
Property Name                                         Description
================================================      =======================================================================
``pulsar.broker-service-url``                         URL for connecting to Pulsar broker
``pulsar.zookeeper-uri``                              URI for Bookkeeper cluster's metadata service
``pulsar.max-message-size``                           Maximum allowed message size in byte
``pulsar.max-entry-read-batch-size``                  Max number of entries read in a single read request to Bookkeeper
``pulsar.target-num-splits``                          Desired number of split to use at each query
``pulsar.max-split-message-queue-size``               Size of message buffer for a split
``pulsar.max-split-entry-queue-size``                 Size of entry buffer for a split
``pulsar.max-split-queue-cache-size``                 Limit of buffered message and entry size in byte
``pulsar.stats-provider``                             StatsProvider to collect Pulsar Trino connector stats
``pulsar.stats-provider-configs``                     Configuration for StatsProvider in key-value format
``pulsar.namespace-delimiter-rewrite-enable``         If enable rewrite Pulsar namespace delimiter
``pulsar.rewrite-namespace-delimiter``                Delimiter used to rewrite Pulsar's default delimiter '/'
``pulsar.managed-ledger-offload-max-threads``         Maximum number of thread pool size for ledger offloading
``pulsar.managed-ledger-offload-driver``              Driver to use to offload/read old data to/from long term storage
``pulsar.offloaders-directory``                       The directory to locate offloaders nar file
``pulsar.offloader-properties``                       Properties and configurations related to specific offloader implementation
``pulsar.nar-extraction-directory``                   Directory to use for extraction Nar file
``pulsar.auth-plugin``                                Authentication plugin used to authenticate to Pulsar cluster
``pulsar.auth-params``                                Authentication parameter to be used to authenticate to Pulsar cluster
``pulsar.tls-allow-insecure-connection``              Whether the Pulsar client accept untrusted TLS certificate from broker
``pulsar.tls-hostname-verification-enable``           Whether to allow hostname verification when client connects to broker over TLS
``pulsar.tls-trust-cert-file-path``                   Path for the trusted TLS certificate file of Pulsar broker
``pulsar.bookkeeper-throttle-value``                  Set Bookkeeper request throttle threshold
``pulsar.bookkeeper-num-io-threads``                  Set number of IO thread
``pulsar.bookkeeper-num-worker-threads``              Set number of worker thread
``pulsar.bookkeeper-use-v2-protocol``                 Whether to use Bookkeeper V2 wire protocol
``pulsar.bookkeeper-explicit-interval``               The interval to check the need for sending an explicit LAC
``pulsar.managed-ledger-cache-size-MB``               Size for managed ledger entry cache in MB
``pulsar.managed-ledger-num-worker-threads``          Number of threads to be used for managed ledger tasks dispatching
``pulsar.managed-ledger-num-scheduler-threads``       Number of threads to be used for managed ledger scheduled tasks
================================================      =======================================================================

``pulsar.broker-service-url``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Url for connecting to Pulsar broker, can be url of either broker or Pulsar proxy if enabled.

``pulsar.zookeeper-uri``
^^^^^^^^^^^^^^^^^^^^^^^^

URI for Bookkeeper cluster's metadata service, should be RUI of a Zookeeper cluster.

``pulsar.max-message-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Maximum allowed message size in byte. Default is 5 MB.

``pulsar.max-entry-read-batch-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Max number of entries read in a single read request to Bookkeeper. Default is 100.

``pulsar.target-num-splits``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Desired number of split to use at each query, it might not be the actual split number used.
Actual split number used in query also depends on number of the partition a topic has.
Number of partition a topic has will be used when issuing query against table backing by this topic if no value is specified.
Default is max of 2 and number of partition of a topic.

``pulsar.max-split-message-queue-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Size of message buffer for a split, message(s) is deserialized from Bookkeepr entry and put into a buffer then Trino will take
message from that buffer. This parameter control the maximum size for the buffer.
Default is 10000.

``pulsar.max-split-entry-queue-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Size of Bookkeeper entry buffer for a split, entry is read from Bookkeepr and put into a buffer and then be deserialized into Pulsar message(s).
This parameter control the maximum size for the buffer.
Default is 1000.

``pulsar.max-split-queue-cache-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``pulsar.max-split-entry-queue-size`` and ``pulsar.max-split-queue-cache-size`` and only control the buffer by entry or message size.
This parameter can controller these 2 buffers by storage size in byte.
Half of cache size is used as max entry queue size bytes and the other half is used as max message queue size bytes, the queue size bytes shouldn't exceed this value, but it's not a hard limit.
Default value -1 indicate no limit.

``pulsar.stats-provider``
^^^^^^^^^^^^^^^^^^^^^^^^^

Provider to collect Pulsar Trino connector stats. Default is 'NullStatsProvider'. See available `metrics providers <https://github.com/apache/pulsar/tree/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/stats>`_.

``pulsar.stats-provider-configs``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Configuration for StatsProvider in key-value format. e.g. {"httpServerEnabled":"false", "prometheusStatsHttpPort":"9092", "prometheusStatsHttpEnable":"true"}

``pulsar.namespace-delimiter-rewrite-enable``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If enable rewrite Pulsar namespace delimiter. Default is false.


``pulsar.rewrite-namespace-delimiter``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Delimiter used to rewrite Pulsar's default delimiter '/'. e.g. If set to -, then a Pulsar topic "my_namespace/my_tenant/my_topic"
will become table "my_namespace-my_tenant-my_topic" in Trino. This allow Trino Pulsar table to be read into other system like `Apache SuperSet <https://superset.apache.org/>`_.
Warn: avoid using symbols allowed by Namespace (a-zA-Z_0-9 -=:%) to prevent erroneous rewriting.

``pulsar.managed-ledger-offload-max-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Maximum number of thread pool threads for ledger offloader. Default is 2.

``pulsar.managed-ledger-offload-driver``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Driver to use to offload/read old data to/from long term storage. See Pulsar `tiered storage <https://pulsar.apache.org/docs/en/tiered-storage-overview/>`_ for more information about offloader.

``pulsar.offloaders-directory``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The directory to locate offloaders nar file

``pulsar.offloader-properties``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Properties and configurations related to specific offloader implementation. e.g.
    {"s3ManagedLedgerOffloadBucket": "offload-bucket", "s3ManagedLedgerOffloadRegion": "us-west-2", "s3ManagedLedgerOffloadServiceEndpoint": "http://s3.amazonaws.com"}

``pulsar.nar-extraction-directory``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Directory to use for extraction offloader Nar file.

``pulsar.auth-plugin``
^^^^^^^^^^^^^^^^^^^^^^

Authentication plugin used to authenticate to Pulsar cluster. Check Pulsar `doc <https://pulsar.apache.org/docs/en/security-overview/#authentication-providers>`_ for available auth plugins.

``pulsar.auth-params``
^^^^^^^^^^^^^^^^^^^^^^

Authentication parameter to be used to authenticate to Pulsar cluster

``pulsar.tls-allow-insecure-connection``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Whether the Pulsar client accept untrusted TLS certificate from broker. Default is false.


``pulsar.tls-hostname-verification-enable``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Whether to allow hostname verification when client connects to broker over TLS. It validates incoming x509 certificate
and matches provided hostname(CN/SAN) with expected broker's host name. It follows RFC 2818, 3.1. Server Identity hostname verification.

``pulsar.tls-trust-cert-file-path``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Path for the trusted TLS certificate file of Pulsar broker.

``pulsar.bookkeeper-throttle-value``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Set Bookkeeper request throttle threshold per second. Since BookKeeper process requests in asynchronous way, it will holds
those pending requests in queue. You may easily run it out of memory if producing too many requests than the capability
of bookie servers can handle. To prevent that from happening, you can set a throttle value here.
Setting the throttle value to 0, will disable any throttling. Default is 0.

``pulsar.bookkeeper-num-io-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Set number of IO thread. This is the number of threads used by Netty to handle TCP connections. Default is 2 * Runtime.getRuntime().availableProcessors().

``pulsar.bookkeeper-num-worker-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Set number of worker thread. This is the number of worker threads used by bookkeeper client to submit operations. Default is Runtime.getRuntime().availableProcessors().

``pulsar.bookkeeper-use-v2-protocol``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Whether the bookkeeper client use v2 wire protocol or v3 wire protocol. V2 protocol use piggy back lac and v3 protocol
and use explicit lac. Default is true.

``pulsar.bookkeeper-explicit-interval``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The interval to check the need for sending an explicit LAC. Has no effect for reading.

``pulsar.managed-ledger-cache-size-MB``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Size for Bookkeeper entry cache in MB. It's used for caching data payload in managed ledger. This memory is allocated
from JVM direct memory and it's shared across all the managed ledgers running in same Trino worker.
0 is represents disable the cache, default is 0.

``pulsar.managed-ledger-num-worker-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Number of threads to be used for managed ledger tasks dispatching. Default is Runtime.getRuntime().availableProcessors().

``pulsar.managed-ledger-num-scheduler-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Number of threads to be used for managed ledger scheduled tasks. Default is Runtime.getRuntime().availableProcessors().
