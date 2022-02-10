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

import io.airlift.log.Logger;
import io.trino.spi.HostAddress;
import io.trino.spi.NodeManager;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Manages connections to the Redis nodes
 */
public class RedisJedisManager
{
    private static final Logger log = Logger.get(RedisJedisManager.class);

    private final ConcurrentMap<HostAddress, JedisPool> jedisPoolCache = new ConcurrentHashMap<>();

    private final RedisConnectorConfig redisConnectorConfig;
    private final JedisPoolConfig jedisPoolConfig;

    @Inject
    RedisJedisManager(
            RedisConnectorConfig redisConnectorConfig,
            NodeManager nodeManager)
    {
        this.redisConnectorConfig = requireNonNull(redisConnectorConfig, "redisConnectorConfig is null");
        this.jedisPoolConfig = new JedisPoolConfig();
    }

    @PreDestroy
    public void tearDown()
    {
        for (Map.Entry<HostAddress, JedisPool> entry : jedisPoolCache.entrySet()) {
            try {
                entry.getValue().destroy();
            }
            catch (Exception e) {
                log.warn(e, "While destroying JedisPool %s:", entry.getKey());
            }
        }
    }

    public RedisConnectorConfig getRedisConnectorConfig()
    {
        return redisConnectorConfig;
    }

    public JedisPool getJedisPool(HostAddress host)
    {
        requireNonNull(host, "host is null");
        return jedisPoolCache.computeIfAbsent(host, this::createConsumer);
    }

    private JedisPool createConsumer(HostAddress host)
    {
        log.info("Creating new JedisPool for %s", host);
        return new JedisPool(jedisPoolConfig,
                host.getHostText(),
                host.getPort(),
                toIntExact(redisConnectorConfig.getRedisConnectTimeout().toMillis()),
                redisConnectorConfig.getRedisPassword(),
                redisConnectorConfig.getRedisDataBaseIndex());
    }
}
