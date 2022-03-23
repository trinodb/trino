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
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.testng.annotations.AfterClass;

import static io.trino.plugin.redis.RedisQueryRunner.createRedisQueryRunner;

public class TestRedisConnectorTest
        extends BaseConnectorTest
{
    private RedisServer redisServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        redisServer = new RedisServer();
        return createRedisQueryRunner(redisServer, ImmutableMap.of(), "string", REQUIRED_TPCH_TABLES);
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        redisServer.close();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_TOPN_PUSHDOWN:
                return false;

            case SUPPORTS_CREATE_SCHEMA:
                return false;

            case SUPPORTS_CREATE_TABLE:
                return false;

            case SUPPORTS_ADD_COLUMN:
                return false;

            case SUPPORTS_RENAME_TABLE:
                return false;

            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
                return false;

            case SUPPORTS_INSERT:
                return false;

            case SUPPORTS_DELETE:
                return false;

            case SUPPORTS_ARRAY:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }
}
