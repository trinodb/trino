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
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;

final class TestRedisClusterConnectorSmokeTest
        extends BaseConnectorSmokeTest
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
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE,
                 SUPPORTS_CREATE_VIEW,
                 SUPPORTS_DELETE,
                 SUPPORTS_INSERT,
                 SUPPORTS_MERGE,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_UPDATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }
}
