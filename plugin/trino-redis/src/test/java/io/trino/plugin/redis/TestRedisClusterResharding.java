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
 * Tests that Trino's Redis connector returns complete results after a slot is
 * migrated from one primary to another (resharding).
 */
final class TestRedisClusterResharding
        extends AbstractTestQueryFramework
{
    private RedisCluster redisCluster;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        redisCluster = closeAfterClass(new RedisCluster());
        return RedisQueryRunner.builder(redisCluster)
                .addConnectorProperties(ImmutableMap.of("redis.cluster.enabled", "true"))
                .setDataFormat("string")
                .setClusterMode(true)
                .setInitialTables(ImmutableList.of(NATION))
                .build();
    }

    @Test
    void testFullScanAfterResharding()
    {
        String key = "tpch:nation:0";
        String query = "SELECT count(*) FROM nation";

        // Baseline count
        assertThat(query(query)).matches("VALUES BIGINT '25'");

        int sourceIndex = redisCluster.getPrimaryIndexForSlot(redisCluster.getKeySlot(key));
        int targetIndex = (sourceIndex + 1) % 3;

        // Migrate the whole slot (and every key in it) to another primary
        redisCluster.migrateSlot(redisCluster.getKeySlot(key), targetIndex);

        // Full scan must still return all 25 rows without data loss
        assertThat(query(query)).matches("VALUES BIGINT '25'");
    }
}
