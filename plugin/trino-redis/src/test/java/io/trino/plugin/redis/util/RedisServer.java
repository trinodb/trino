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

import com.google.common.net.HostAndPort;
import org.testcontainers.containers.GenericContainer;
import redis.clients.jedis.Connection;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.SslOptions;

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
        this(version, setAccessControl, false);
    }

    public static RedisServer createTlsServer()
    {
        return new RedisServer(LATEST_VERSION, false, true);
    }

    private RedisServer(String version, boolean setAccessControl, boolean tls)
    {
        container = new GenericContainer<>("redis:" + version)
                .withExposedPorts(PORT);
        if (setAccessControl) {
            container.withCommand("redis-server", "--requirepass", PASSWORD);
        }
        if (tls) {
            configureTls(container);
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
