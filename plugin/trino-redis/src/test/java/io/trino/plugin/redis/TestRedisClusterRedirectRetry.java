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
 * Tests that Trino's Redis connector correctly retries on MOVED and ASK redirects
 * while a slot is migrating between primaries.
 */
final class TestRedisClusterRedirectRetry
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
    void testMovedRetry()
    {
        String key = "tpch:nation:0";
        String query = "SELECT name FROM nation WHERE nationkey = 0";

        // Baseline query to populate the connector's topology cache
        assertThat(query(query)).matches("VALUES CAST('ALGERIA' AS VARCHAR(25))");

        int sourceIndex = redisCluster.getPrimaryIndexForSlot(redisCluster.getKeySlot(key));
        int targetIndex = (sourceIndex + 1) % 3;

        // Move the slot and the key to another primary; cluster will return MOVED
        redisCluster.migrateSlotAndKey(key, sourceIndex, targetIndex);

        // The connector must follow the MOVED redirect and still return the value
        assertThat(query(query)).matches("VALUES CAST('ALGERIA' AS VARCHAR(25))");
    }

    @Test
    void testAskRetry()
    {
        String key = "tpch:nation:0";
        String query = "SELECT name FROM nation WHERE nationkey = 0";

        // Baseline query to populate the connector's topology cache
        assertThat(query(query)).matches("VALUES CAST('ALGERIA' AS VARCHAR(25))");

        int sourceIndex = redisCluster.getPrimaryIndexForSlot(redisCluster.getKeySlot(key));
        int targetIndex = (sourceIndex + 2) % 3;

        // Put the slot into MIGRATING/IMPORTING state and move the key without
        // finalizing ownership. The source primary returns ASK to the target.
        redisCluster.prepareAskingSlot(key, sourceIndex, targetIndex);

        // The connector must send ASKING to the target and still return the value
        assertThat(query(query)).matches("VALUES CAST('ALGERIA' AS VARCHAR(25))");

        // Restore a stable slot owner so the shared cluster is not left mid-migration
        redisCluster.finalizeSlotMigration(key, targetIndex);
    }
}
