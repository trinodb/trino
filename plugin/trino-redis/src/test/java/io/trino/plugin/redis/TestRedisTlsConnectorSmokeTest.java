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
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;

import static io.trino.plugin.redis.util.RedisServer.TLS_STORE_PASSWORD;

final class TestRedisTlsConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        RedisServer redisServer = closeAfterClass(RedisServer.createTlsServer());
        return RedisQueryRunner.builder(redisServer)
                .addConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("redis.tls.enabled", "true")
                        .put("redis.tls.keystore-path", RedisServer.getKeystorePath())
                        .put("redis.tls.keystore-password", TLS_STORE_PASSWORD)
                        .put("redis.tls.truststore-path", RedisServer.getTruststorePath())
                        .put("redis.tls.truststore-password", TLS_STORE_PASSWORD)
                        .buildOrThrow())
                .setDataFormat("string")
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
