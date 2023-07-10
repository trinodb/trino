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
package io.trino.plugin.postgresql;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.jmx.JmxPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;

public final class PostgreSqlQueryRunner
{
    private PostgreSqlQueryRunner() {}

    private static final String TPCH_SCHEMA = "tpch";

    public static DistributedQueryRunner createPostgreSqlQueryRunner(
            TestingPostgreSqlServer server,
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createPostgreSqlQueryRunner(server, extraProperties, Map.of(), connectorProperties, tables, runner -> {});
    }

    public static DistributedQueryRunner createPostgreSqlQueryRunner(
            TestingPostgreSqlServer server,
            Map<String, String> extraProperties,
            Map<String, String> coordinatorProperties,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables,
            Consumer<QueryRunner> moreSetup)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(createSession())
                    .setExtraProperties(extraProperties)
                    .setCoordinatorProperties(coordinatorProperties)
                    .setAdditionalSetup(moreSetup)
                    .build();

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            // note: additional copy via ImmutableList so that if fails on nulls
            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("connection-url", server.getJdbcUrl());
            connectorProperties.putIfAbsent("connection-user", server.getUser());
            connectorProperties.putIfAbsent("connection-password", server.getPassword());
            connectorProperties.putIfAbsent("postgresql.include-system-tables", "true");
            //connectorProperties.putIfAbsent("postgresql.experimental.enable-string-pushdown-with-collate", "true");

            queryRunner.installPlugin(new PostgreSqlPlugin());
            queryRunner.createCatalog("postgresql", "postgresql", connectorProperties);

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
                .setCatalog("postgresql")
                .setSchema(TPCH_SCHEMA)
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        DistributedQueryRunner queryRunner = createPostgreSqlQueryRunner(
                new TestingPostgreSqlServer(true),
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of(),
                TpchTable.getTables());

        queryRunner.installPlugin(new JmxPlugin());
        queryRunner.createCatalog("jmx", "jmx");

        Logger log = Logger.get(PostgreSqlQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
