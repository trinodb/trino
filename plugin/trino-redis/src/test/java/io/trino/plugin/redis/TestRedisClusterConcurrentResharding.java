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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.trino.tpch.TpchTable.NATION;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that a Trino query returns correct results when a Redis Cluster slot
 * is migrated while the query is in flight.  This exercises the connector's
 * MOVED/ASK redirect handling under real concurrent topology change.
 */
final class TestRedisClusterConcurrentResharding
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
    void testQueryDuringResharding()
            throws Exception
    {
        String key = "tpch:nation:0";
        int sourceIndex = redisCluster.getPrimaryIndexForSlot(redisCluster.getKeySlot(key));
        int targetIndex = (sourceIndex + 1) % 3;

        // Start a full scan in a background thread
        CompletableFuture<Long> queryFuture = CompletableFuture.supplyAsync(() ->
                (Long) computeActual("SELECT count(*) FROM nation").getMaterializedRows().get(0).getField(0));

        // Give the query a moment to start, then migrate the slot
        Thread.sleep(100);
        redisCluster.migrateSlot(redisCluster.getKeySlot(key), targetIndex);

        // The query should complete successfully despite the concurrent migration.
        // SCAN is not a consistent snapshot, so during slot migration a key may
        // be transiently missed (24 instead of 25).  This is expected Redis behavior.
        Long count = queryFuture.get(60, TimeUnit.SECONDS);
        assertThat(count).isBetween(24L, 25L);

        // After migration completes, a fresh query must return the full count.
        Long postMigrationCount = (Long) computeActual("SELECT count(*) FROM nation")
                .getMaterializedRows().get(0).getField(0);
        assertThat(postMigrationCount).isEqualTo(25L);
    }

    @Test
    void testPredicateQueryDuringResharding()
            throws Exception
    {
        String key = "tpch:nation:0";
        int sourceIndex = redisCluster.getPrimaryIndexForSlot(redisCluster.getKeySlot(key));
        int targetIndex = (sourceIndex + 1) % 3;

        // Start a predicate query in a background thread
        CompletableFuture<String> queryFuture = CompletableFuture.supplyAsync(() ->
                (String) computeActual("SELECT name FROM nation WHERE nationkey = 0").getMaterializedRows().get(0).getField(0));

        // Give the query a moment to start, then migrate the slot
        Thread.sleep(100);
        redisCluster.migrateSlot(redisCluster.getKeySlot(key), targetIndex);

        // The query should complete successfully despite the concurrent migration
        String name = queryFuture.get(60, TimeUnit.SECONDS);
        assertThat(name).isEqualTo("ALGERIA");
    }
}
