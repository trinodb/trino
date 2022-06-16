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
import io.trino.plugin.redis.util.RedisServer;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.testing.AbstractTestQueries;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.trino.plugin.redis.RedisQueryRunner.createRedisQueryRunner;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRedisDistributedHash
        extends AbstractTestQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        RedisServer redisServer = closeAfterClass(new RedisServer());
        return createRedisQueryRunner(redisServer, ImmutableMap.of(), ImmutableMap.of(), "hash", REQUIRED_TPCH_TABLES);
    }

    @Test
    public void testPredicatePushdown()
    {
        // redis key equality
        assertThat(query("SELECT regionkey, nationkey, name FROM tpch.nation WHERE redis_key = 'tpch:nation:19'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // redis key equality
        assertThat(query("SELECT regionkey, nationkey, name FROM tpch.nation WHERE redis_key = 'tpch:nation:19' AND regionkey = 3"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isNotFullyPushedDown(FilterNode.class);

        // redis key equality
        assertThat(query("SELECT regionkey, nationkey, name FROM tpch.nation WHERE redis_key = 'tpch:nation:19' AND regionkey = 4"))
                .returnsEmptyResult()
                .isNotFullyPushedDown(FilterNode.class);

        // redis key different case
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE redis_key = 'tpch:nation:100'"))
                .returnsEmptyResult()
                .isFullyPushedDown();

        // redis key IN case
        assertThat(query("SELECT regionkey, nationkey, name FROM tpch.nation WHERE redis_key IN ('tpch:nation:19', 'tpch:nation:2', 'tpch:nation:24')"))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25))), " +
                        "(BIGINT '1', BIGINT '2', CAST('BRAZIL' AS varchar(25))), " +
                        "(BIGINT '1', BIGINT '24', CAST('UNITED STATES' AS varchar(25)))")
                .isFullyPushedDown();

        // redis key range
        assertThat(query("SELECT regionkey, nationkey, name FROM tpch.nation WHERE redis_key BETWEEN 'tpch:nation:23' AND 'tpch:nation:24'"))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '23', CAST('UNITED KINGDOM' AS varchar(25))), " +
                        "(BIGINT '1', BIGINT '24', CAST('UNITED STATES' AS varchar(25)))")
                .isNotFullyPushedDown(FilterNode.class);
    }
}
