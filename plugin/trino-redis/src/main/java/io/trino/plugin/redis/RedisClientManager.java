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
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.util.JedisClusterCRC16;
import redis.clients.jedis.util.SafeEncoder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
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

    private final AtomicReference<RedisClusterTopology> clusterTopology = new AtomicReference<>();

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
        checkArgument(
                !clusterEnabled || redisDataBaseIndex == 0,
                "redis.database-index must be 0 when redis.cluster.enabled is true because Redis Cluster only supports database index 0");
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
        clusterTopology.set(null);
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
     * Returns the current cluster topology, discovering it from the seed nodes
     * on first access.  Only applicable when {@code redis.cluster.enabled=true}.
     */
    public RedisClusterTopology getClusterTopology()
    {
        RedisClusterTopology topology = clusterTopology.get();
        if (topology == null) {
            topology = RedisClusterTopology.discover(seedNodes, baseClientConfigBuilder().build());
            if (!clusterTopology.compareAndSet(null, topology)) {
                topology = clusterTopology.get();
            }
        }
        return topology;
    }

    /**
     * Forces a re-discovery of the cluster topology from the seed nodes.
     * Called after a MOVED redirect indicates the cached topology is stale.
     * Clients for primaries that are no longer in the new topology are closed
     * and removed from the cache to avoid leaking connections to departed nodes.
     */
    public RedisClusterTopology refreshTopology()
    {
        RedisClusterTopology oldTopology = clusterTopology.get();
        RedisClusterTopology newTopology = RedisClusterTopology.discover(seedNodes, baseClientConfigBuilder().build());
        clusterTopology.set(newTopology);

        // Close clients for primaries that are no longer in the topology
        if (oldTopology != null) {
            Set<HostAddress> stalePrimaries = new HashSet<>(oldTopology.getPrimaries());
            stalePrimaries.removeAll(newTopology.getPrimaries());
            for (HostAddress stale : stalePrimaries) {
                RedisClient staleClient = clientCache.remove(stale);
                if (staleClient != null) {
                    try {
                        staleClient.close();
                        log.info("Closed stale RedisClient for departed primary %s", stale);
                    }
                    catch (Exception e) {
                        log.warn(e, "While closing stale RedisClient for %s:", stale);
                    }
                }
            }
        }
        return newTopology;
    }

    /**
     * Returns the primary node that owns the slot for the given key.
     */
    public HostAddress getPrimaryForKey(String key)
    {
        return getClusterTopology().getPrimaryForKey(key);
    }

    /**
     * Returns a client for the primary that owns the slot for the given key.
     */
    public RedisClient getClientForKey(String key)
    {
        return getClient(getPrimaryForKey(key));
    }

    /**
     * Returns the Redis hash slot for the given key.
     */
    public static int getSlot(String key)
    {
        return JedisClusterCRC16.getSlot(key);
    }

    /**
     * Parses a MOVED or ASK redirection error and returns the target host.
     *
     * @return the target HostAddress, or null if the exception is not a redirection
     */
    public static HostAddress parseRedirectionTarget(JedisDataException exception)
    {
        String message = exception.getMessage();
        if (message == null) {
            return null;
        }
        // Format: "MOVED <slot> <ip:port>" or "ASK <slot> <ip:port>"
        if (!message.startsWith("MOVED") && !message.startsWith("ASK")) {
            return null;
        }
        String[] parts = message.split("\\s+", -1);
        if (parts.length < 3) {
            return null;
        }
        return HostAddress.fromString(parts[2]).withDefaultPort(6379);
    }

    /**
     * Returns true if the given exception is a MOVED or ASK redirection.
     */
    public static boolean isRedirectionError(JedisDataException exception)
    {
        String message = exception.getMessage();
        return message != null && (message.startsWith("MOVED") || message.startsWith("ASK"));
    }

    /**
     * Returns true if the given exception is a MOVED redirection (topology change).
     */
    public static boolean isMovedRedirection(JedisDataException exception)
    {
        String message = exception.getMessage();
        return message != null && message.startsWith("MOVED");
    }

    /**
     * Returns true if the given exception is an ASK redirection (temporary).
     */
    public static boolean isAskRedirection(JedisDataException exception)
    {
        String message = exception.getMessage();
        return message != null && message.startsWith("ASK");
    }

    /**
     * Sends ASKING followed by GET on a fresh connection to the target node.
     * Required for ASK redirects where the slot is in migrating state.
     *
     * @return the string value, or null if the key does not exist
     */
    public String askAndGet(HostAddress target, String key)
    {
        DefaultJedisClientConfig clientConfig = baseClientConfigBuilder().build();
        try (Connection connection = new Connection(
                new HostAndPort(target.getHostText(), target.getPort()),
                clientConfig)) {
            connection.sendCommand(new CommandArguments(Protocol.Command.ASKING));
            connection.getStatusCodeReply();
            connection.sendCommand(new CommandArguments(Protocol.Command.GET).add(key));
            Object reply = connection.getOne();
            return reply == null ? null : SafeEncoder.encode((byte[]) reply);
        }
    }

    /**
     * Sends ASKING followed by HGETALL on a fresh connection to the target node.
     * Required for ASK redirects where the slot is in migrating state.
     *
     * @return a map of field names to values, or an empty map if the key does not exist
     */
    public Map<String, String> askAndGetAll(HostAddress target, String key)
    {
        DefaultJedisClientConfig clientConfig = baseClientConfigBuilder().build();
        try (Connection connection = new Connection(
                new HostAndPort(target.getHostText(), target.getPort()),
                clientConfig)) {
            connection.sendCommand(new CommandArguments(Protocol.Command.ASKING));
            connection.getStatusCodeReply();
            connection.sendCommand(new CommandArguments(Protocol.Command.HGETALL).add(key));
            Object reply = connection.getOne();
            if (reply == null) {
                return Map.of();
            }
            @SuppressWarnings("unchecked")
            List<Object> entries = (List<Object>) reply;
            Map<String, String> result = new HashMap<>();
            for (int i = 0; i + 1 < entries.size(); i += 2) {
                String field = SafeEncoder.encode((byte[]) entries.get(i));
                String value = SafeEncoder.encode((byte[]) entries.get(i + 1));
                result.put(field, value);
            }
            return result;
        }
    }

    private RedisClient createClient(HostAddress host)
    {
        log.info("Creating new RedisClient for %s", host);

        DefaultJedisClientConfig clientConfig = baseClientConfigBuilder()
                .database(redisDataBaseIndex)
                .build();

        return RedisClient.builder()
                .hostAndPort(host.getHostText(), host.getPort())
                .clientConfig(clientConfig)
                .build();
    }

    private DefaultJedisClientConfig.Builder baseClientConfigBuilder()
    {
        DefaultJedisClientConfig.Builder builder = DefaultJedisClientConfig.builder()
                .connectionTimeoutMillis(toIntExact(redisConnectTimeout.toMillis()))
                .socketTimeoutMillis(toIntExact(redisConnectTimeout.toMillis()));
        if (redisUser != null && !redisUser.isEmpty()) {
            builder.user(redisUser);
        }
        if (redisPassword != null && !redisPassword.isEmpty()) {
            builder.password(redisPassword);
        }
        clientConfigurators.forEach(configurator -> configurator.configure(builder));
        return builder;
    }
}
