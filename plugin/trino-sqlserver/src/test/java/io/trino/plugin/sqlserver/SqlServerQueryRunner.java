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
package io.trino.plugin.sqlserver;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.util.HashMap;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;

public final class SqlServerQueryRunner
{
    private static final Logger log = Logger.get(SqlServerQueryRunner.class);

    private SqlServerQueryRunner() {}

    private static final String CATALOG = "sqlserver";

    private static final String TEST_SCHEMA = "dbo";

    public static QueryRunner createSqlServerQueryRunner(
            TestingSqlServer testingSqlServer,
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(createSession(testingSqlServer.getUsername()))
                .setExtraProperties(extraProperties)
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("connection-url", testingSqlServer.getJdbcUrl());
            connectorProperties.putIfAbsent("connection-user", testingSqlServer.getUsername());
            connectorProperties.putIfAbsent("connection-password", testingSqlServer.getPassword());

            queryRunner.installPlugin(new SqlServerPlugin());
            queryRunner.createCatalog(CATALOG, "sqlserver", connectorProperties);
            log.info("%s catalog properties: %s", CATALOG, connectorProperties);

            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(testingSqlServer.getUsername()), tables);

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static Session createSession(String username)
    {
        return testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(TEST_SCHEMA)
                .setIdentity(Identity.ofUser(username))
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        TestingSqlServer testingSqlServer = new TestingSqlServer();

        // SqlServer is using docker container so in case that shutdown hook is not called, developer can easily clean docker container on their own
        Runtime.getRuntime().addShutdownHook(new Thread(testingSqlServer::close));

        DistributedQueryRunner queryRunner = (DistributedQueryRunner) createSqlServerQueryRunner(
                testingSqlServer,
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of(),
                TpchTable.getTables());

        Logger log = Logger.get(SqlServerQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
