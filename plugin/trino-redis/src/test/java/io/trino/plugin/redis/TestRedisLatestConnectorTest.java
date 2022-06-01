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
import io.trino.testing.QueryRunner;

import static io.trino.plugin.redis.RedisQueryRunner.createRedisQueryRunner;
import static io.trino.plugin.redis.util.RedisServer.LATEST_VERSION;
import static io.trino.plugin.redis.util.RedisServer.PASSWORD;
import static io.trino.plugin.redis.util.RedisServer.USER;

public class TestRedisLatestConnectorTest
        extends BaseRedisConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        RedisServer redisServer = closeAfterClass(new RedisServer(LATEST_VERSION, true));
        return createRedisQueryRunner(
                redisServer,
                ImmutableMap.of(),
                ImmutableMap.of("redis.user", USER, "redis.password", PASSWORD),
                "string",
                REQUIRED_TPCH_TABLES);
    }
}
