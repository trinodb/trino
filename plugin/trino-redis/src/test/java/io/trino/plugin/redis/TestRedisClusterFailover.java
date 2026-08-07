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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.redis.util.RedisCluster;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.tpch.TpchTable.NATION;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests Redis Cluster failover by killing a primary and verifying the connector
 * discovers the promoted replica and still returns the correct data.
 */
final class TestRedisClusterFailover
        extends AbstractTestQueryFramework
{
    private RedisCluster redisCluster;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // 3 primaries with 1 replica each
        redisCluster = closeAfterClass(new RedisCluster(3, 7000, 3));
        return RedisQueryRunner.builder(redisCluster)
                .addConnectorProperties(ImmutableMap.of("redis.cluster.enabled", "true"))
                .setDataFormat("string")
                .setClusterMode(true)
                .setInitialTables(ImmutableList.of(NATION))
                .build();
    }

    @Test
    void testFailover()
    {
        String key = "tpch:nation:0";
        String query = "SELECT name FROM nation WHERE nationkey = 0";

        // Baseline query to populate the connector's topology cache
        assertThat(query(query)).matches("VALUES CAST('ALGERIA' AS VARCHAR(25))");

        int sourceIndex = redisCluster.getPrimaryIndexForSlot(redisCluster.getKeySlot(key));

        // Kill the primary that owns the key and wait for the replica to take over
        redisCluster.killPrimaryAndWaitForFailover(sourceIndex);

        // The connector must refresh topology and still return the value
        assertThat(query(query)).matches("VALUES CAST('ALGERIA' AS VARCHAR(25))");
    }
}
