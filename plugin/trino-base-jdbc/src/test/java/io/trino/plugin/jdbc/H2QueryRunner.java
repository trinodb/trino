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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;

public final class H2QueryRunner
{
    private H2QueryRunner() {}

    private static final String TPCH_SCHEMA = "tpch";

    public static DistributedQueryRunner createH2QueryRunner(TpchTable<?>... tables)
            throws Exception
    {
        return createH2QueryRunner(ImmutableList.copyOf(tables));
    }

    public static DistributedQueryRunner createH2QueryRunner(Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createH2QueryRunner(tables, TestingH2JdbcModule.createProperties());
    }

    public static DistributedQueryRunner createH2QueryRunner(Iterable<TpchTable<?>> tables, Map<String, String> properties)
            throws Exception
    {
        return createH2QueryRunner(tables, properties, new TestingH2JdbcModule());
    }

    public static DistributedQueryRunner createH2QueryRunner(Iterable<TpchTable<?>> tables, Map<String, String> properties, Module module)
            throws Exception
    {
        return createH2QueryRunner(tables, properties, ImmutableMap.of(), module);
    }

    public static DistributedQueryRunner createH2QueryRunner(
            Iterable<TpchTable<?>> tables,
            Map<String, String> properties,
            Map<String, String> coordinatorProperties,
            Module module)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(createSession())
                    .setCoordinatorProperties(coordinatorProperties)
                    .build();

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            createSchema(properties, "tpch");

            queryRunner.installPlugin(new JdbcPlugin("base_jdbc", module));
            queryRunner.createCatalog("jdbc", "base_jdbc", properties);

            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), tables);

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static void createSchema(Map<String, String> properties, String schema)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(properties.get("connection-url"));
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
        }
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("jdbc")
                .setSchema(TPCH_SCHEMA)
                .build();
    }
}
