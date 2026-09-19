/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.redis.util;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import org.testcontainers.containers.GenericContainer;
import redis.clients.jedis.Connection;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.SslOptions;
import redis.clients.jedis.util.SafeEncoder;

import java.io.Closeable;
import java.io.File;

import static org.testcontainers.utility.MountableFile.forClasspathResource;

public class RedisServer
        implements Closeable
{
    public static final String DEFAULT_VERSION = "5.0.14";
    public static final String LATEST_VERSION = "7.0.0";
    private static final int PORT = 6379;

    public static final String USER = "test";
    public static final String PASSWORD = "password";

    public static final String TLS_STORE_PASSWORD = "changeit";
    private static final String CONTAINER_CERTS_DIR = "/etc/redis/certs/";

    private final GenericContainer<?> container;
    private final RedisClient redisClient;

    public RedisServer()
    {
        this(DEFAULT_VERSION, false);
    }

    public RedisServer(String version, boolean setAccessControl)
    {
        this(version, setAccessControl, false, false);
    }

    public static RedisServer createTlsServer()
    {
        return new RedisServer(LATEST_VERSION, false, true, false);
    }

    public static RedisServer createClusterServer()
    {
        return new RedisServer(LATEST_VERSION, false, false, true);
    }

    private RedisServer(String version, boolean setAccessControl, boolean tls, boolean cluster)
    {
        container = new GenericContainer<>("redis:" + version)
                .withExposedPorts(PORT);
        if (setAccessControl) {
            container.withCommand("redis-server", "--requirepass", PASSWORD);
        }
        if (tls) {
            configureTls(container);
        }
        if (cluster) {
            container.withCommand("redis-server", "--cluster-enabled", "yes", "--cluster-config-file", "nodes.conf", "--cluster-node-timeout", "5000");
            // Publish the Redis port on the same host port. A runtime CONFIG SET of
            // cluster-announce-port is not reliably reflected in the CLUSTER NODES "myself" entry
            // on Redis 7.0, so the connector would otherwise discover the internal port 6379 and
            // fail to connect. Binding host 6379 -> container 6379 makes the advertised address
            // (127.0.0.1:6379) reachable from the test JVM.
            container.setPortBindings(ImmutableList.of(PORT + ":" + PORT));
        }
        container.start();

        DefaultJedisClientConfig.Builder clientConfig = DefaultJedisClientConfig.builder();
        if (setAccessControl) {
            clientConfig.password(PASSWORD);
        }
        if (tls) {
            clientConfig.sslOptions(buildClientSslOptions());
        }
        redisClient = RedisClient.builder()
                .hostAndPort(container.getHost(), container.getMappedPort(PORT))
                .clientConfig(clientConfig.build())
                .build();
        if (setAccessControl) {
            aclSetUser(USER, "on", ">" + PASSWORD, "~*:*", "+@all");
        }
        if (cluster) {
            initializeCluster();
        }
    }

    public RedisClient getClient()
    {
        return redisClient;
    }

    public void closeClient()
    {
        redisClient.close();
    }

    public HostAndPort getHostAndPort()
    {
        return HostAndPort.fromParts(container.getHost(), container.getMappedPort(PORT));
    }

    public static String getKeystorePath()
    {
        return forClasspathResource("tls/keystore.p12").getResolvedPath();
    }

    public static String getTruststorePath()
    {
        return forClasspathResource("tls/truststore.p12").getResolvedPath();
    }

    @Override
    public void close()
    {
        redisClient.close();
        container.close();
    }

    private void aclSetUser(String user, String... rules)
    {
        String[] args = new String[2 + rules.length];
        args[0] = "SETUSER";
        args[1] = user;
        System.arraycopy(rules, 0, args, 2, rules.length);

        try (Connection connection = redisClient.getPool().getResource()) {
            connection.sendCommand(Protocol.Command.ACL, args);
            connection.getStatusCodeReply();
        }
    }

    // Turns the single container into a one-node Redis Cluster that owns all 16384 slots.
    // The Redis port is published on the same host port (see setPortBindings above), so the address
    // returned by CLUSTER NODES (127.0.0.1:6379) is reachable from the test JVM. This exercises the
    // full cluster code path (node discovery, per-node scan, pipelined single-key fetch) in CI
    // without the node-address advertisement problems of a multi-container cluster.
    private void initializeCluster()
    {
        String announceIp = container.getHost();
        if (announceIp.equals("localhost")) {
            announceIp = "127.0.0.1";
        }
        int announcePort = container.getMappedPort(PORT);
        try (Connection connection = redisClient.getPool().getResource()) {
            connection.sendCommand(Protocol.Command.CONFIG, "SET", "cluster-announce-ip", announceIp);
            connection.getStatusCodeReply();
            connection.sendCommand(Protocol.Command.CONFIG, "SET", "cluster-announce-port", Integer.toString(announcePort));
            connection.getStatusCodeReply();
            connection.sendCommand(Protocol.Command.CLUSTER, "ADDSLOTSRANGE", "0", "16383");
            connection.getStatusCodeReply();
            waitForClusterReady(connection);
        }
    }

    private static void waitForClusterReady(Connection connection)
    {
        long deadlineMillis = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadlineMillis) {
            connection.sendCommand(Protocol.Command.CLUSTER, "INFO");
            String info = SafeEncoder.encode((byte[]) connection.getOne());
            if (info != null && info.contains("cluster_state:ok")) {
                return;
            }
            try {
                Thread.sleep(200);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for Redis cluster to be ready", e);
            }
        }
        throw new IllegalStateException("Redis cluster did not become ready within 30 seconds");
    }

    private static void configureTls(GenericContainer<?> container)
    {
        container
                .withCopyFileToContainer(forClasspathResource("tls/ca.crt", 0644), CONTAINER_CERTS_DIR + "ca.crt")
                .withCopyFileToContainer(forClasspathResource("tls/redis.crt", 0644), CONTAINER_CERTS_DIR + "redis.crt")
                .withCopyFileToContainer(forClasspathResource("tls/redis.key", 0644), CONTAINER_CERTS_DIR + "redis.key")
                .withCommand(
                        "redis-server",
                        // serve TLS on the exposed port and disable the plaintext port
                        "--tls-port",
                        Integer.toString(PORT),
                        "--port",
                        "0",
                        "--tls-cert-file",
                        CONTAINER_CERTS_DIR + "redis.crt",
                        "--tls-key-file",
                        CONTAINER_CERTS_DIR + "redis.key",
                        "--tls-ca-cert-file",
                        CONTAINER_CERTS_DIR + "ca.crt");
    }

    private static SslOptions buildClientSslOptions()
    {
        return SslOptions.builder()
                .keystore(new File(getKeystorePath()), TLS_STORE_PASSWORD.toCharArray())
                .truststore(new File(getTruststorePath()), TLS_STORE_PASSWORD.toCharArray())
                .build();
    }
}
