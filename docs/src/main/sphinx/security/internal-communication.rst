=============================
Secure internal communication
=============================

The Trino cluster can be configured to use secured communication with internal
authentication of the nodes in the cluster, and optionally added security with
:ref:`TLS <glossTLS>`.

Internal authentication
-----------------------

Requests between Trino nodes are authenticated using a shared secret. For secure
internal communication, the shared secret must be set to the same value on all
nodes in the cluster:

.. code-block:: text

    internal-communication.shared-secret=<secret>

A large random key is recommended, and can be generated with the following Linux
command:

.. code-block:: text

    openssl rand 512 | base64

Internal TLS configuration
--------------------------

You can configure the coordinator and all workers to encrypt all communication
with each other using TLS. Every node in the cluster must be configured. Nodes
that have not been configured, or are configured incorrectly, are not able to
communicate with other nodes in the cluster.

In typical deployments, you should enable :ref:`TLS directly on the coordinator
<https-secure-directly>` for fully encrypted access to the cluster by client
tools.

Now you can enable TLS for internal communication with the following
configuration identical on all cluster nodes.

1. Configure a shared secret for internal communication as described in
   the preceding section.

2. Enable automatic certificate creation and trust setup in
   ``etc/config.properties``:

   .. code-block:: properties

     internal-communication.https.required=true

3. Change the URI for the discovery service to use HTTPS and point to the IP
   address of the coordinator in ``etc/config.properties``:

   .. code-block:: properties

     discovery.uri=https://<coordinator ip address>:<https port>

   Note that using hostnames or fully qualified domain names for the URI is
   not supported. The automatic certificate creation for internal TLS only
   supports IP addresses. Java 17 is known to be incompatible with this feature
   and can not be used as a runtime for Trino with this feature enabled.

4. Enable the HTTPS endpoint on all workers.

   .. code-block:: properties

     http-server.https.enabled=true
     http-server.https.port=<https port>

5. Restart all nodes.

Certificates are automatically created and used to ensure all communication
inside the cluster is secured with TLS.

.. warning::

    Older versions of Trino required you to manually manage all the certificates
    on the nodes. If you upgrade from this setup, you must remove the following
    configuration properties:

    * ``internal-communication.https.keystore.path``
    * ``internal-communication.https.truststore.path``
    * ``node.internal-address-source``

Performance with SSL/TLS enabled
--------------------------------

Enabling encryption impacts performance. The performance degradation can vary
based on the environment, queries, and concurrency.

For queries that do not require transferring too much data between the Trino
nodes e.g. ``SELECT count(*) FROM table``, the performance impact is negligible.

However, for CPU intensive queries which require a considerable amount of data
to be transferred between the nodes (for example, distributed joins, aggregations and
window functions, which require repartitioning), the performance impact can be
considerable. The slowdown may vary from 10% to even 100%+, depending on the network
traffic and the CPU utilization.

Advanced performance tuning
---------------------------

In some cases, changing the source of random numbers improves performance
significantly.

By default, TLS encryption uses the ``/dev/urandom`` system device as a source of entropy.
This device has limited throughput, so on environments with high network bandwidth
(e.g. InfiniBand), it may become a bottleneck. In such situations, it is recommended to try
to switch the random number generator algorithm to ``SHA1PRNG``, by setting it via
``http-server.https.secure-random-algorithm`` property in ``config.properties`` on the coordinator
and all of the workers:

.. code-block:: text

    http-server.https.secure-random-algorithm=SHA1PRNG

Be aware that this algorithm takes the initial seed from
the blocking ``/dev/random`` device. For environments that do not have enough entropy to seed
the ``SHAPRNG`` algorithm, the source can be changed to ``/dev/urandom``
by adding the ``java.security.egd`` property to ``jvm.config``:

.. code-block:: text

    -Djava.security.egd=file:/dev/urandom
