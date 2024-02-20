# Secure internal communication

The Trino cluster can be configured to use secured communication with internal
authentication of the nodes in the cluster, and to optionally use added security
with {ref}`TLS <glossTLS>`.

## Configure shared secret

Configure a shared secret to authenticate all communication between nodes of the
cluster. Use this configuration under the following conditions:

- When opting to configure [internal TLS encryption](internal-tls)
  between nodes of the cluster
- When using any {doc}`external authentication <authentication-types>` method
  between clients and the coordinator

Set the shared secret to the same value in {ref}`config.properties
<config-properties>` on all nodes of the cluster:

```text
internal-communication.shared-secret=<secret>
```

A large random key is recommended, and can be generated with the following Linux
command:

```text
openssl rand 512 | base64
```

(verify-secrets)=

### Verify configuration

To verify shared secret configuration:

1. Start your Trino cluster with two or more nodes configured with a shared
   secret.
2. Connect to the {doc}`Web UI </admin/web-interface>`.
3. Confirm the number of `ACTIVE WORKERS` equals the number of nodes
   configured with your shared secret.
4. Change the value of the shared secret on one worker, and restart the worker.
5. Log in to the Web UI and confirm the number of `ACTIVE WORKERS` is one
   less. The worker with the invalid secret is not authenticated, and therefore
   not registered with the coordinator.
6. Stop your Trino cluster, revert the value change on the worker, and restart
   your cluster.
7. Confirm the number of `ACTIVE WORKERS` equals the number of nodes
   configured with your shared secret.

(internal-tls)=

## Configure internal TLS

You can optionally add an extra layer of security by configuring the cluster to
encrypt communication between nodes with {ref}`TLS <glossTLS>`.

You can configure the coordinator and all workers to encrypt all communication
with each other using TLS. Every node in the cluster must be configured. Nodes
that have not been configured, or are configured incorrectly, are not able to
communicate with other nodes in the cluster.

In typical deployments, you should enable {ref}`TLS directly on the coordinator
<https-secure-directly>` for fully encrypted access to the cluster by client
tools.

Enable TLS for internal communication with the following
configuration identical on all cluster nodes.

1. Configure a shared secret for internal communication as described in
   the preceding section.

2. Enable automatic certificate creation and trust setup in
   `etc/config.properties`:

   ```properties
   internal-communication.https.required=true
   ```

3. Change the URI for the discovery service to use HTTPS and point to the IP
   address of the coordinator in `etc/config.properties`:

   ```properties
   discovery.uri=https://<coordinator ip address>:<https port>
   ```

   Note that using hostnames or fully qualified domain names for the URI is
   not supported. The automatic certificate creation for internal TLS only
   supports IP addresses.

4. Enable the HTTPS endpoint on all workers.

   ```properties
   http-server.https.enabled=true
   http-server.https.port=<https port>
   ```

5. Restart all nodes.

Certificates are automatically created and used to ensure all communication
inside the cluster is secured with TLS.

:::{warning}
Older versions of Trino required you to manually manage all the certificates
on the nodes. If you upgrade from this setup, you must remove the following
configuration properties:

- `internal-communication.https.keystore.path`
- `internal-communication.https.truststore.path`
- `node.internal-address-source`
:::

### Performance with SSL/TLS enabled

Enabling encryption impacts performance. The performance degradation can vary
based on the environment, queries, and concurrency.

For queries that do not require transferring too much data between the Trino
nodes e.g. `SELECT count(*) FROM table`, the performance impact is negligible.

However, for CPU intensive queries which require a considerable amount of data
to be transferred between the nodes (for example, distributed joins, aggregations and
window functions, which require repartitioning), the performance impact can be
considerable. The slowdown may vary from 10% to even 100%+, depending on the network
traffic and the CPU utilization.

### Advanced performance tuning

In some cases, changing the source of random numbers improves performance
significantly.

By default, TLS encryption uses the `/dev/urandom` system device as a source of entropy.
This device has limited throughput, so on environments with high network bandwidth
(e.g. InfiniBand), it may become a bottleneck. In such situations, it is recommended to try
to switch the random number generator algorithm to `SHA1PRNG`, by setting it via
`http-server.https.secure-random-algorithm` property in `config.properties` on the coordinator
and all of the workers:

```text
http-server.https.secure-random-algorithm=SHA1PRNG
```

Be aware that this algorithm takes the initial seed from
the blocking `/dev/random` device. For environments that do not have enough entropy to seed
the `SHAPRNG` algorithm, the source can be changed to `/dev/urandom`
by adding the `java.security.egd` property to `jvm.config`:

```text
-Djava.security.egd=file:/dev/urandom
```
