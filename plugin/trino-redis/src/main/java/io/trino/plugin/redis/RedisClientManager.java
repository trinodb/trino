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
package io.trino.plugin.redis;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.spi.HostAddress;
import jakarta.annotation.PreDestroy;
import redis.clients.jedis.CommandArguments;
import redis.clients.jedis.Connection;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.util.SafeEncoder;

import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Manages connections to the Redis nodes
 */
public class RedisClientManager
{
    private static final Logger log = Logger.get(RedisClientManager.class);

    private final ConcurrentMap<HostAddress, RedisClient> clientCache = new ConcurrentHashMap<>();

    private final String redisUser;
    private final String redisPassword;
    private final Duration redisConnectTimeout;
    private final int redisDataBaseIndex;
    private final int redisMaxKeysPerFetch;
    private final char redisKeyDelimiter;
    private final boolean keyPrefixSchemaTable;
    private final int redisScanCount;
    private final Set<RedisClientConfigurator> clientConfigurators;
    private final Set<HostAddress> seedNodes;
    private final boolean clusterEnabled;

    @Inject
    RedisClientManager(RedisConnectorConfig redisConnectorConfig, Set<RedisClientConfigurator> clientConfigurators)
    {
        requireNonNull(redisConnectorConfig, "redisConnectorConfig is null");
        this.redisUser = redisConnectorConfig.getRedisUser();
        this.redisPassword = redisConnectorConfig.getRedisPassword();
        this.redisConnectTimeout = redisConnectorConfig.getRedisConnectTimeout();
        this.redisDataBaseIndex = redisConnectorConfig.getRedisDataBaseIndex();
        this.redisMaxKeysPerFetch = redisConnectorConfig.getRedisMaxKeysPerFetch();
        this.redisKeyDelimiter = redisConnectorConfig.getRedisKeyDelimiter();
        this.keyPrefixSchemaTable = redisConnectorConfig.isKeyPrefixSchemaTable();
        this.redisScanCount = redisConnectorConfig.getRedisScanCount();
        this.clientConfigurators = ImmutableSet.copyOf(clientConfigurators);
        this.seedNodes = redisConnectorConfig.getNodes();
        this.clusterEnabled = redisConnectorConfig.isClusterEnabled();
        if (this.clusterEnabled && this.redisDataBaseIndex != 0) {
            throw new IllegalArgumentException(
                    "redis.database-index must be 0 when redis.cluster.enabled=true. "
                            + "Redis Cluster only supports database index 0.");
        }
    }

    @PreDestroy
    public void tearDown()
    {
        for (Entry<HostAddress, RedisClient> entry : clientCache.entrySet()) {
            try {
                entry.getValue().close();
            }
            catch (Exception e) {
                log.warn(e, "While closing RedisClient %s:", entry.getKey());
            }
        }
    }

    public char getRedisKeyDelimiter()
    {
        return redisKeyDelimiter;
    }

    public int getRedisMaxKeysPerFetch()
    {
        return redisMaxKeysPerFetch;
    }

    public boolean isKeyPrefixSchemaTable()
    {
        return keyPrefixSchemaTable;
    }

    public int getRedisScanCount()
    {
        return redisScanCount;
    }

    public boolean isClusterEnabled()
    {
        return clusterEnabled;
    }

    public RedisClient getClient(HostAddress host)
    {
        requireNonNull(host, "host is null");
        return clientCache.computeIfAbsent(host, this::createClient);
    }

    /**
     * Discovers all master nodes in a Redis Cluster via the CLUSTER NODES command.
     * Uses a low-level Connection to send the raw command, since clusterNodes() was
     * removed from high-level Jedis 7.x clients.
     * Only applicable when redis.cluster.enabled=true.
     */
    public Set<HostAddress> getClusterMasterNodes()
    {
        HostAddress seed = seedNodes.iterator().next();
        DefaultJedisClientConfig.Builder configBuilder = DefaultJedisClientConfig.builder()
                .connectionTimeoutMillis(toIntExact(redisConnectTimeout.toMillis()))
                .socketTimeoutMillis(toIntExact(redisConnectTimeout.toMillis()));
        if (redisUser != null && !redisUser.isEmpty()) {
            configBuilder.user(redisUser);
        }
        if (redisPassword != null && !redisPassword.isEmpty()) {
            configBuilder.password(redisPassword);
        }
        clientConfigurators.forEach(configurator -> configurator.configure(configBuilder));
        try (Connection connection = new Connection(
                new HostAndPort(seed.getHostText(), seed.getPort()),
                configBuilder.build())) {
            connection.sendCommand(new CommandArguments(Protocol.Command.CLUSTER).add("NODES"));
            return parseClusterMasterNodes(SafeEncoder.encode((byte[]) connection.getOne()));
        }
    }

    private static Set<HostAddress> parseClusterMasterNodes(String clusterNodes)
    {
        ImmutableSet.Builder<HostAddress> masters = ImmutableSet.builder();
        for (String line : clusterNodes.split("\n")) {
            String trimmed = line.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            String[] parts = trimmed.split("\\s+");
            // CLUSTER NODES format: <id> <ip:port@bus-port> <flags> ...
            // flags field contains "master" for master nodes and "slave" for replicas
            if (parts.length >= 3 && parts[2].contains("master") && !parts[2].contains("fail")) {
                String hostPort = parts[1].split("@")[0];
                masters.add(HostAddress.fromString(hostPort));
            }
        }
        return masters.build();
    }

    private RedisClient createClient(HostAddress host)
    {
        log.info("Creating new RedisClient for %s", host);

        DefaultJedisClientConfig.Builder clientConfigBuilder = DefaultJedisClientConfig.builder()
                .connectionTimeoutMillis(toIntExact(redisConnectTimeout.toMillis()))
                .socketTimeoutMillis(toIntExact(redisConnectTimeout.toMillis()))
                .database(redisDataBaseIndex);

        if (redisUser != null && !redisUser.isEmpty()) {
            clientConfigBuilder.user(redisUser);
        }
        if (redisPassword != null && !redisPassword.isEmpty()) {
            clientConfigBuilder.password(redisPassword);
        }

        clientConfigurators.forEach(configurator -> configurator.configure(clientConfigBuilder));

        return RedisClient.builder()
                .hostAndPort(host.getHostText(), host.getPort())
                .clientConfig(clientConfigBuilder.build())
                .build();
    }
}
