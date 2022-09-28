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
package io.trino.plugin.oracle;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;

import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.oracle.TestingOracleServer.TEST_PASS;
import static io.trino.plugin.oracle.TestingOracleServer.TEST_SCHEMA;
import static io.trino.plugin.oracle.TestingOracleServer.TEST_USER;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;

public final class OracleQueryRunner
{
    private OracleQueryRunner() {}

    public static DistributedQueryRunner createOracleQueryRunner(
            TestingOracleServer server,
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(createSession())
                    .setExtraProperties(extraProperties)
                    .build();

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            queryRunner.installPlugin(new OraclePlugin());
            queryRunner.createCatalog("oracle", "oracle", connectorProperties);

            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), tables);

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner, server);
            throw e;
        }
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("oracle")
                .setSchema(TEST_SCHEMA)
                .build();
    }

    public static Map<String, String> connectionProperties(TestingOracleServer server)
    {
        return ImmutableMap.<String, String>builder()
                .put("connection-url", server.getJdbcUrl())
                .put("connection-user", TEST_USER)
                .put("connection-password", TEST_PASS)
                .buildOrThrow();
    }

    public static void main(String[] args)
            throws Exception
    {
        TestingOracleServer server = new TestingOracleServer();
        DistributedQueryRunner queryRunner = createOracleQueryRunner(
                server,
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.<String, String>builder()
                        .put("connection-url", server.getJdbcUrl())
                        .put("connection-user", TEST_USER)
                        .put("connection-password", TEST_PASS)
                        .put("oracle.connection-pool.enabled", "false")
                        .put("oracle.remarks-reporting.enabled", "false")
                        .buildOrThrow(),
                TpchTable.getTables());

        Logger log = Logger.get(OracleQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
