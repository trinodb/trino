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
package io.trino.plugin.ignite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.assertUpdate;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public final class IgniteQueryRunner
{
    private static final Logger log = Logger.get(IgniteQueryRunner.class);

    private static final String IGNITE_SCHEMA = "public";

    public static final String CREATE_CUSTOM = "CREATE TABLE customer (" +
            "custkey bigint NOT NULL," +
            "name varchar(25) NOT NULL," +
            "address varchar(40) NOT NULL," +
            "nationkey bigint NOT NULL," +
            "phone varchar(15) NOT NULL," +
            "acctbal double NOT NULL," +
            "mktsegment varchar(10) NOT NULL," +
            "comment varchar(117) NOT NULL )" +
            "WITH (" +
            "primary_key = ARRAY['custkey'])";

    public static final String CREATE_NATION = "CREATE TABLE nation (" +
            "nationkey bigint NOT NULL," +
            "name varchar(25) NOT NULL," +
            "regionkey bigint NOT NULL," +
            "comment varchar(152) NOT NULL )" +
            "WITH (" +
            "primary_key = ARRAY['nationkey'])";

    public static final String CREATE_ORDERS = " CREATE TABLE orders (" +
            "orderkey bigint NOT NULL," +
            "custkey bigint NOT NULL," +
            "orderstatus varchar(1) NOT NULL," +
            "totalprice double NOT NULL," +
            "orderdate date NOT NULL," +
            "orderpriority varchar(15) NOT NULL," +
            "clerk varchar(15) NOT NULL," +
            "shippriority integer NOT NULL," +
            "comment varchar(79) NOT NULL) " +
            " WITH ( " +
            "primary_key = ARRAY['orderkey'])";

    public static final String CREATE_REGION = " CREATE TABLE region (" +
            "regionkey bigint NOT NULL," +
            "name varchar(25) NOT NULL," +
            "comment varchar(152) NOT NULL)" +
            "WITH ( " +
            "primary_key = ARRAY['regionkey'])";

    private IgniteQueryRunner() {}

    public static QueryRunner createIgniteQueryRunner(TestingIgniteServer server, TpchTable<?>... tables)
            throws Exception
    {
        return createIgniteQueryRunner(server, ImmutableMap.of(), ImmutableMap.of(), ImmutableList.copyOf(tables));
    }

    public static DistributedQueryRunner createIgniteQueryRunner(
            TestingIgniteServer server,
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

            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("connection-url", server.getJdbcUrl());

            queryRunner.installPlugin(new IgniteJdbcPlugin());
            queryRunner.createCatalog("ignite", "ignite", connectorProperties);
            copyFromTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), tables);
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    public static void copyFromTpchTables(
            QueryRunner queryRunner,
            String sourceCatalog,
            String sourceSchema,
            Session session,
            Iterable<TpchTable<?>> tables)
    {
        assertUpdate(queryRunner, session, CREATE_CUSTOM, OptionalLong.empty(), Optional.empty());
        assertUpdate(queryRunner, session, CREATE_NATION, OptionalLong.empty(), Optional.empty());
        assertUpdate(queryRunner, session, CREATE_REGION, OptionalLong.empty(), Optional.empty());
        assertUpdate(queryRunner, session, CREATE_ORDERS, OptionalLong.empty(), Optional.empty());
        for (TpchTable<?> table : tables) {
            copyTable(queryRunner, sourceCatalog + "." + sourceSchema + "." + table.getTableName(), table.getTableName(), session);
        }
    }

    private static void copyTable(QueryRunner queryRunner, String remoteTable, String targetTableName, Session session)
    {
        long start = System.nanoTime();
        log.info("Running import for %s", remoteTable);
        @Language("SQL") String sql = format("INSERT INTO %s SELECT * FROM %s", targetTableName, remoteTable);
        long rows = (Long) queryRunner.execute(session, sql).getMaterializedRows().get(0).getField(0);
        log.info("Imported %s rows for %s in %s", rows, remoteTable, nanosSince(start).convertToMostSuccinctTimeUnit());

        assertThat(queryRunner.execute(session, "SELECT count(*) FROM " + targetTableName).getOnlyValue())
                .as("Table is not loaded properly: %s", targetTableName)
                .isEqualTo(queryRunner.execute(session, "SELECT count(*) FROM " + remoteTable).getOnlyValue());
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("ignite")
                .setSchema(IGNITE_SCHEMA)
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();

        DistributedQueryRunner queryRunner = createIgniteQueryRunner(
                new TestingIgniteServer(),
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of(),
                ImmutableList.of("customer", "orders", "region", "nation").stream()
                        .map(TpchTable::getTable)
                        .collect(toImmutableList()));

        Logger log = Logger.get(IgniteQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
