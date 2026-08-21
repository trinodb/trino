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
import io.trino.plugin.redis.util.RedisServer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.tpch.TpchTable.NATION;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests Redis Cluster mode with password authentication.  Verifies that
 * topology discovery, slot routing, and query execution all work when
 * the Redis cluster requires a password.
 */
final class TestRedisClusterAuth
        extends AbstractTestQueryFramework
{
    private RedisCluster redisCluster;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        redisCluster = closeAfterClass(new RedisCluster(3, 7000, 0, false, true));
        return RedisQueryRunner.builder(redisCluster)
                .addConnectorProperties(ImmutableMap.of(
                        "redis.cluster.enabled", "true",
                        "redis.password", RedisServer.PASSWORD))
                .setDataFormat("string")
                .setClusterMode(true)
                .setInitialTables(ImmutableList.of(NATION))
                .build();
    }

    @Test
    void testAuthClusterFullScan()
    {
        assertThat(query("SELECT count(*) FROM nation")).matches("VALUES BIGINT '25'");
    }

    @Test
    void testAuthClusterPredicateRouting()
    {
        assertThat(query("SELECT name FROM nation WHERE nationkey = 0")).matches("VALUES CAST('ALGERIA' AS VARCHAR(25))");
        assertThat(query("SELECT name FROM nation WHERE nationkey = 24")).matches("VALUES CAST('UNITED STATES' AS VARCHAR(25))");
    }
}
