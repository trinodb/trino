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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.redis.util.RedisCluster;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.tpch.TpchTable.getTables;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests Redis Cluster routing with a real multi-primary cluster (3 primaries).
 * Verifies cross-shard scans, predicate routing, and data completeness.
 */
final class TestRedisClusterRouting
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        RedisCluster redisCluster = closeAfterClass(new RedisCluster());
        return RedisQueryRunner.builder(redisCluster)
                .addConnectorProperties(ImmutableMap.of("redis.cluster.enabled", "true"))
                .setDataFormat("string")
                .setClusterMode(true)
                .setInitialTables(getTables())
                .build();
    }

    /**
     * Verifies that a full scan across all shards returns the complete expected row count.
     * With 3 primaries and slots divided evenly, keys are distributed across all shards.
     */
    @Test
    void testFullScanAcrossShards()
    {
        // nation has 25 rows, region has 5 rows — all should be returned regardless of shard distribution
        assertThat(query("SELECT count(*) FROM nation")).matches("VALUES BIGINT '25'");
        assertThat(query("SELECT count(*) FROM region")).matches("VALUES BIGINT '5'");
    }

    /**
     * Verifies that predicate pushdown with equality (=) routes to the correct shard.
     */
    @Test
    void testEqualityPredicateRouting()
    {
        // Query a specific nation by key — should route to the owning primary
        assertThat(query("SELECT nationkey FROM nation WHERE nationkey = 0")).matches("VALUES BIGINT '0'");
        assertThat(query("SELECT nationkey FROM nation WHERE nationkey = 15")).matches("VALUES BIGINT '15'");
        assertThat(query("SELECT nationkey FROM nation WHERE nationkey = 24")).matches("VALUES BIGINT '24'");
    }

    /**
     * Verifies that IN predicates with keys on different shards are routed correctly.
     */
    @Test
    void testInPredicateRoutingAcrossShards()
    {
        // Keys 0, 10, 20 are likely on different shards — all should be returned
        assertThat(query("SELECT count(*) FROM nation WHERE nationkey IN (0, 5, 10, 15, 20)"))
                .matches("VALUES BIGINT '5'");
    }

    /**
     * Verifies that data values are correct across shards (not just counts).
     */
    @Test
    void testValuesAcrossShards()
    {
        assertThat(query("SELECT name FROM nation WHERE nationkey = 0")).matches("VALUES CAST('ALGERIA' AS VARCHAR(25))");
        assertThat(query("SELECT name FROM nation WHERE nationkey = 1")).matches("VALUES CAST('ARGENTINA' AS VARCHAR(25))");
        assertThat(query("SELECT name FROM nation WHERE nationkey = 24")).matches("VALUES CAST('UNITED STATES' AS VARCHAR(25))");
    }

    /**
     * Verifies that a join across cluster-scanned tables works correctly.
     */
    @Test
    void testJoinAcrossShards()
    {
        assertThat(query(
                "SELECT n.name, r.name FROM nation n JOIN region r ON n.regionkey = r.regionkey WHERE n.nationkey = 0"))
                .matches("VALUES (CAST('ALGERIA' AS VARCHAR(25)), CAST('AFRICA' AS VARCHAR(25)))");
    }
}
