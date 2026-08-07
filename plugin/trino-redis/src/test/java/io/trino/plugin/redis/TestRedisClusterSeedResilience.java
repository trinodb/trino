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
import io.trino.plugin.redis.util.RedisCluster;
import io.trino.spi.HostAddress;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.DefaultJedisClientConfig;

import java.util.LinkedHashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

final class TestRedisClusterSeedResilience
{
    @Test
    void testDiscoveryWithFirstDeadSeed()
    {
        try (RedisCluster redisCluster = new RedisCluster()) {
            Set<HostAddress> seeds = new LinkedHashSet<>();
            // First seed is unavailable
            seeds.add(HostAddress.fromParts("127.0.0.1", 1));
            // Remaining seeds are the real cluster primaries
            Set<HostAddress> expected = ImmutableSet.copyOf(
                    redisCluster.getSeedAddresses().stream()
                            .map(seed -> HostAddress.fromParts(seed.getHost(), seed.getPort()))
                            .toList());
            seeds.addAll(expected);

            RedisClusterTopology topology = RedisClusterTopology.discover(
                    seeds,
                    DefaultJedisClientConfig.builder().build());

            assertThat(topology.isComplete()).isTrue();
            assertThat(topology.getPrimaries()).containsExactlyInAnyOrderElementsOf(expected);
        }
    }
}
