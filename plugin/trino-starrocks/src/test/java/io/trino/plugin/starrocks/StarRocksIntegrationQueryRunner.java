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
package io.trino.plugin.starrocks;

import io.trino.plugin.base.util.Closables;
import io.trino.testing.DistributedQueryRunner;

import java.util.HashMap;
import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;

public final class StarRocksIntegrationQueryRunner
{
    private static final String DEFAULT_SCHEMA = System.getProperty("starrocks.test.schema", "starrocks_test");

    private StarRocksIntegrationQueryRunner() {}

    public static DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog("starrocks")
                        .setSchema(DEFAULT_SCHEMA)
                        .build())
                .build();

        try {
            queryRunner.installPlugin(new StarRocksPlugin());
            queryRunner.createCatalog("starrocks", "starrocks", connectorProperties());
            return queryRunner;
        }
        catch (Throwable t) {
            Closables.closeAllSuppress(t, queryRunner);
            throw t;
        }
    }

    private static Map<String, String> connectorProperties()
    {
        Map<String, String> properties = new HashMap<>();
        properties.put("starrocks.jdbc-url", System.getProperty("starrocks.test.jdbc-url", "jdbc:starrocks://127.0.0.1:9030"));
        properties.put("starrocks.username", System.getProperty("starrocks.test.username", "root"));
        properties.put("starrocks.password", System.getProperty("starrocks.test.password", ""));
        properties.put("starrocks.flight-sql-host", System.getProperty("starrocks.test.flight-sql-host", "127.0.0.1"));
        properties.put("starrocks.flight-sql-port", System.getProperty("starrocks.test.flight-sql-port", "9408"));
        return Map.copyOf(properties);
    }
}
