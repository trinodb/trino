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

import java.io.Closeable;

public class RedisServer
        implements Closeable
{
    public static final String DEFAULT_VERSION = "5.0.14";
    public static final String LATEST_VERSION = "7.0.0";
    private static final int PORT = 6379;

    public static final String USER = "test";
    public static final String PASSWORD = "password";

    private final GenericContainer<?> container;
    private final RedisClient redisClient;

    public RedisServer()
    {
        this(DEFAULT_VERSION, false);
    }

    public RedisServer(String version, boolean setAccessControl)
    {
        container = new GenericContainer<>("redis:" + version)
                .withExposedPorts(PORT);
        if (setAccessControl) {
            container.withCommand("redis-server", "--requirepass", PASSWORD);
        }
        container.start();

        DefaultJedisClientConfig.Builder clientConfig = DefaultJedisClientConfig.builder();
        if (setAccessControl) {
            clientConfig.password(PASSWORD);
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
}
