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
package io.prestosql.plugin.sqlserver;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import io.airlift.tpch.TpchTable;
import io.prestosql.Session;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.spi.security.Identity;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tests.DistributedQueryRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.tests.QueryAssertions.copyTable;
import static java.util.Locale.ENGLISH;

public final class SqlServerQueryRunner
{
    private SqlServerQueryRunner() {}

    private static final String CATALOG = "sqlserver";

    private static final String TEST_SCHEMA = "dbo";

    public static QueryRunner createSqlServerQueryRunner(TestingSqlServer testingSqlServer, TpchTable<?>... tables)
            throws Exception
    {
        return createSqlServerQueryRunner(testingSqlServer, ImmutableMap.of(), ImmutableList.copyOf(tables));
    }

    public static QueryRunner createSqlServerQueryRunner(TestingSqlServer testingSqlServer, Map<String, String> connectorProperties, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(createSession())
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("connection-url", testingSqlServer.getJdbcUrl());
            connectorProperties.putIfAbsent("connection-user", TestingSqlServer.USER);
            connectorProperties.putIfAbsent("connection-password", TestingSqlServer.PASSWORD);
            connectorProperties.putIfAbsent("allow-drop-table", "true");

            queryRunner.installPlugin(new SqlServerPlugin());
            queryRunner.createCatalog(CATALOG, "sqlserver", connectorProperties);

            provisionTables(createSession(), queryRunner, tables);

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static void provisionTables(Session session, QueryRunner queryRunner, Iterable<TpchTable<?>> tables)
    {
        Set<String> existingTables = queryRunner.listTables(session, CATALOG, TEST_SCHEMA).stream()
                .map(QualifiedObjectName::getObjectName)
                .collect(toImmutableSet());

        Streams.stream(tables)
                .filter(table -> !existingTables.contains(table.getTableName().toLowerCase(ENGLISH)))
                .forEach(table -> copyTable(queryRunner, "tpch", TINY_SCHEMA_NAME, table.getTableName().toLowerCase(ENGLISH), session));
    }

    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(TEST_SCHEMA)
                .setIdentity(new Identity(TestingSqlServer.USER, Optional.empty()))
                .build();
    }
}
