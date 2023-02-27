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
import io.trino.Session;
import io.trino.plugin.redis.util.JsonEncoder;
import io.trino.plugin.redis.util.RedisServer;
import io.trino.spi.connector.SchemaTableName;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.StandaloneQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;

import java.util.Map;
import java.util.UUID;

import static io.trino.plugin.redis.util.RedisTestUtils.createTableDescription;
import static io.trino.plugin.redis.util.RedisTestUtils.installRedisPlugin;
import static io.trino.plugin.redis.util.RedisTestUtils.loadSimpleTableDescription;
import static io.trino.testing.TestingSession.testSessionBuilder;

@Test(singleThreaded = true)
public abstract class AbstractTestMinimalFunctionality
{
    protected static final Session SESSION = testSessionBuilder()
            .setCatalog("redis")
            .setSchema("default")
            .build();

    protected RedisServer redisServer;
    protected String tableName;
    protected String stringValueTableName;
    protected String hashValueTableName;
    protected StandaloneQueryRunner queryRunner;
    protected QueryAssertions assertions;

    protected abstract Map<String, String> connectorProperties();

    @BeforeClass
    public void startRedis()
            throws Exception
    {
        redisServer = new RedisServer();

        this.queryRunner = new StandaloneQueryRunner(SESSION);
        assertions = new QueryAssertions(queryRunner);

        this.tableName = "test_" + UUID.randomUUID().toString().replaceAll("-", "_");
        RedisTableDescription stringValueTableDescription = loadSimpleTableDescription(queryRunner, "string");
        RedisTableDescription hashValueTableDescription = loadSimpleTableDescription(queryRunner, "hash");
        this.stringValueTableName = stringValueTableDescription.getTableName();
        this.hashValueTableName = hashValueTableDescription.getTableName();

        installRedisPlugin(redisServer, queryRunner,
                ImmutableMap.<SchemaTableName, RedisTableDescription>builder()
                        .put(createTableDescription(new RedisTableDescription(tableName, "default", null, null)))
                        .put(createTableDescription(stringValueTableDescription))
                        .put(createTableDescription(hashValueTableDescription))
                        .buildOrThrow(),
                connectorProperties());

        populateData(1000);
    }

    @AfterClass(alwaysRun = true)
    public void stopRedis()
    {
        clearData();

        queryRunner.close();
        queryRunner = null;
        assertions = null;

        redisServer.close();
        redisServer = null;
    }

    protected void populateData(int count)
    {
        JsonEncoder jsonEncoder = new JsonEncoder();
        for (long i = 0; i < count; i++) {
            Object value = ImmutableMap.of("id", Long.toString(i), "value", UUID.randomUUID().toString());
            try (Jedis jedis = redisServer.getJedisPool().getResource()) {
                jedis.set(tableName + ":" + i, jsonEncoder.toString(value));
                jedis.set(stringValueTableName + ":" + i, jsonEncoder.toString(value));
                jedis.hmset(hashValueTableName + ":" + i, (Map<String, String>) value);
            }
        }
    }

    protected void clearData()
    {
        try (Jedis jedis = redisServer.getJedisPool().getResource()) {
            jedis.flushAll();
        }
    }
}
