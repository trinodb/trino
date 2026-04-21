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

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.spi.HostAddress;
import jakarta.annotation.PreDestroy;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.RedisClient;

import java.util.Map.Entry;
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

    @Inject
    RedisClientManager(RedisConnectorConfig redisConnectorConfig)
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

    public RedisClient getClient(HostAddress host)
    {
        requireNonNull(host, "host is null");
        return clientCache.computeIfAbsent(host, this::createClient);
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

        return RedisClient.builder()
                .hostAndPort(host.getHostText(), host.getPort())
                .clientConfig(clientConfigBuilder.build())
                .build();
    }
}
