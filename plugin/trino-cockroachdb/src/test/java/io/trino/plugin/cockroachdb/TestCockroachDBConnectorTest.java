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
package io.trino.plugin.cockroachdb;

import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.plugin.jdbc.RemoteLogTracingEvent;
import io.trino.plugin.postgresql.TestPostgreSqlConnectorTest;
import io.trino.spi.QueryId;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TestView;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CANCELLATION;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE_WITH_DATA;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_SET_COLUMN_TYPE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

public class TestCockroachDBConnectorTest
        extends TestPostgreSqlConnectorTest
{
    private static final Logger log = Logger.get(TestCockroachDBConnectorTest.class);

    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();

    protected TestingCockroachDBServer cockroachDBServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        cockroachDBServer = closeAfterClass(new io.trino.plugin.cockroachdb.TestingCockroachDBServer());
        return CockroachDBQueryRunner.builder(cockroachDBServer)
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .withProtocolSpooling("json")
                .build();
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return cockroachDBServer::execute;
    }

    @Override
    public void setExtensions()
    {
    }

    @Override
    protected void createTableForWrites(String createTable, String tableName, Optional<String> primaryKey, OptionalInt updateCount)
    {
        updateCount.ifPresentOrElse(count -> assertUpdate(format(createTable, tableName), count), () -> assertUpdate(format(createTable, tableName)));
        primaryKey.ifPresent(key -> onRemoteDatabase().execute(format("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL", tableName, key)));
        primaryKey.ifPresent(key -> onRemoteDatabase().execute(format("ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s)", tableName, "pk_" + tableName, key)));
    }

    @Override
    protected TestTable createTestTableForWrites(String namePrefix, String tableDefinition, String primaryKey)
    {
        TestTable testTable = newTrinoTable(namePrefix, tableDefinition);
        String tableName = testTable.getName();
        for (String keyColumn : Arrays.stream(primaryKey.split(",")).toList()) {
            onRemoteDatabase().execute(format("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL", tableName, keyColumn));
        }
        onRemoteDatabase().execute(format("ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s)", tableName, "pk_" + tableName, primaryKey));
        return testTable;
    }

    @Override
    protected TestTable createTestTableForWrites(String namePrefix, String tableDefinition, List<String> rowsToInsert, String primaryKey)
    {
        TestTable testTable = newTrinoTable(namePrefix, tableDefinition, rowsToInsert);
        String tableName = testTable.getName();
        for (String keyColumn : Arrays.stream(primaryKey.split(",")).toList()) {
            onRemoteDatabase().execute(format("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL", tableName, keyColumn));
        }
        onRemoteDatabase().execute(format("ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s)", tableName, "pk_" + tableName, primaryKey));
        return testTable;
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.empty();
    }

    @Override
    protected OptionalInt maxTableRenameLength()
    {
        return OptionalInt.empty();
    }

    @Override
    protected OptionalInt maxColumnNameLength()
    {
        return OptionalInt.empty();
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.empty();
    }

    @Override
    protected MaterializedResult getDescribeOrdersResult()
    {
        return resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "bigint", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();
    }

    @Override
    protected String getColumnType(String tableName, String columnName)
    {
        String type = (String) computeScalar(format("SELECT data_type FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA AND table_name = '%s' AND column_name = '%s'",
                tableName,
                columnName));
        if (type.equals("bigint")) {
            return "integer";
        }
        return type;
    }

    @Override
    protected String getJdbcUrl()
    {
        return cockroachDBServer.getJdbcUrl();
    }

    @Override
    protected Properties getProperties()
    {
        return cockroachDBServer.getProperties();
    }

    @Override
    protected String invalidTimestampError()
    {
        return "This connector only supports reading tables AS OF VERSION, not TIMESTAMP";
    }

    @Override
    protected void verifyAddNotNullColumnToNonEmptyTableFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("ERROR: null value in column \".*\" violates not-null constraint");
    }

    @Override
    protected void verifyVersionedQueryFailurePermissible(Exception e)
    {
        assertThat(e)
                .hasMessageMatching("This connector only supports reading tables AS OF VERSION, not TIMESTAMP|" +
                        "This connector only allows passing in strings as a read version|" +
                       "ERROR: AS OF SYSTEM TIME: value is neither timestamp, decimal, nor interval|" +
                        "ERROR: database \"tpch\" does not exist|" +
                        "(?s)ERROR: inconsistent AS OF SYSTEM TIME timestamp.*");
    }

    @Override
    @Test
    public void testSetColumnOutOfRangeType()
    {
        skipTestUnless(hasBehavior(SUPPORTS_SET_COLUMN_TYPE) && hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA));

        try (TestTable table = newTrinoTable("test_set_column_type_invalid_range_", "AS SELECT CAST(9223372036854775807 AS bigint) AS col")) {
            assertThatThrownBy(() -> assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN col SET DATA TYPE SMALLINT"))
                    .satisfies(this::verifySetColumnTypeFailurePermissible);
        }
    }

    @Override
    @Test
    public void testShowColumns()
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedUnparametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("clerk", "varchar", "", "")
                .row("shippriority", "bigint", "", "")
                .row("comment", "varchar", "", "")
                .build();

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "bigint", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();

        // Until we migrate all connectors to parametrized varchar we check two options
        assertThat(actual.equals(expectedParametrizedVarchar) || actual.equals(expectedUnparametrizedVarchar))
                .describedAs(format("%s does not match neither of %s and %s", actual, expectedParametrizedVarchar, expectedUnparametrizedVarchar))
                .isTrue();
    }

    @Test
    public void testSetColumnType()
    {
        if (!hasBehavior(SUPPORTS_SET_COLUMN_TYPE)) {
            assertQueryFails("ALTER TABLE nation ALTER COLUMN nationkey SET DATA TYPE bigint", "This connector does not support setting column types");
            return;
        }

        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA));

        try (TestTable table = newTrinoTable("test_set_column_type_", "AS SELECT CAST(123 AS integer) AS col")) {
            assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN col SET DATA TYPE bigint");

            assertThat(getColumnType(table.getName(), "col")).isEqualTo("integer");
            assertThat(query("SELECT * FROM " + table.getName()))
                    .skippingTypesCheck()
                    .matches("VALUES bigint '123'");
        }
    }

    @Override
    @Test
    public void testShowCreateTable()
    {
        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        assertThat(computeScalar("SHOW CREATE TABLE orders"))
                // If the connector reports additional column properties, the expected value needs to be adjusted in the test subclass
                .isEqualTo(format(
                        """
                        CREATE TABLE %s.%s.orders (
                           orderkey bigint,
                           custkey bigint,
                           orderstatus varchar(1),
                           totalprice double,
                           orderdate date,
                           orderpriority varchar(15),
                           clerk varchar(15),
                           shippriority bigint,
                           comment varchar(79)
                        )\
                        """,
                        catalog,
                        schema));
    }

    @Override
    protected void startTracingDatabaseEvent(RemoteLogTracingEvent event)
    {
        cockroachDBServer.startTracingDatabaseEvent(event);
    }

    @Override
    @Test
    public void testTimestampColumnAndTimestampWithTimeZoneConstant()
    {
        try (TestTable table = new TestTable(onRemoteDatabase(), "test_timestamptz_unwrap_cast", "(id bigint, ts_col timestamp(6))")) {
            onRemoteDatabase().execute("INSERT INTO " + table.getName() + " (id, ts_col) VALUES " +
                    "(1, timestamp '2020-01-01 01:01:01.000')," +
                    "(2, timestamp '2019-01-01 01:01:01.000')");

            assertThat(query(format("SELECT id FROM %s WHERE ts_col >= TIMESTAMP '2019-01-01 00:00:00 %s'", table.getName(), getSession().getTimeZoneKey().getId())))
                    .matches("VALUES BIGINT '1', BIGINT '2'")
                    .isFullyPushedDown();

            assertThat(query(format("SELECT id FROM %s WHERE ts_col >= TIMESTAMP '2019-01-01 00:00:00 %s'", table.getName(), "UTC")))
                    .matches("VALUES BIGINT '1'")
                    .isFullyPushedDown();
        }
    }

    // CockroachDB adds row id
    @Override
    @Test
    public void testTableWithNoSupportedColumns()
    {
        String unsupportedDataType = "interval";
        String supportedDataType = "varchar(5)";

        try (TestTable noSupportedColumns = new TestTable(onRemoteDatabase(), "no_supported_columns", format("(c %s)", unsupportedDataType));
                TestTable supportedColumns = new TestTable(onRemoteDatabase(), "supported_columns", format("(good %s)", supportedDataType));
                TestTable noColumns = new TestTable(onRemoteDatabase(), "no_columns", "()")) {
            assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet()).contains("orders", noSupportedColumns.getName(), supportedColumns.getName(), noColumns.getName());

            assertQueryFails("SELECT c FROM " + noSupportedColumns.getName(), "\\QTable 'tpch." + noSupportedColumns.getName() + "' has no supported columns (all 2 columns are not supported)");
            assertQueryFails("SELECT * FROM " + noSupportedColumns.getName(), "\\QTable 'tpch." + noSupportedColumns.getName() + "' has no supported columns (all 2 columns are not supported)");
            assertQueryFails("SELECT 'a' FROM " + noSupportedColumns.getName(), "\\QTable 'tpch." + noSupportedColumns.getName() + "' has no supported columns (all 2 columns are not supported)");

            assertQueryFails("SELECT c FROM " + noColumns.getName(), "\\QTable 'tpch." + noColumns.getName() + "' has no supported columns (all 1 columns are not supported)");
            assertQueryFails("SELECT * FROM " + noColumns.getName(), "\\QTable 'tpch." + noColumns.getName() + "' has no supported columns (all 1 columns are not supported)");
            assertQueryFails("SELECT 'a' FROM " + noColumns.getName(), "\\QTable 'tpch." + noColumns.getName() + "' has no supported columns (all 1 columns are not supported)");

            assertQueryFails("SELECT c FROM non_existent", ".* Table .*tpch.non_existent.* does not exist");
            assertQueryFails("SELECT * FROM non_existent", ".* Table .*tpch.non_existent.* does not exist");
            assertQueryFails("SELECT 'a' FROM non_existent", ".* Table .*tpch.non_existent.* does not exist");

            assertQueryFails("SHOW COLUMNS FROM " + noSupportedColumns.getName(), "\\QTable 'tpch." + noSupportedColumns.getName() + "' has no supported columns (all 2 columns are not supported)");
            assertQueryFails("SHOW COLUMNS FROM " + noColumns.getName(), "\\QTable 'tpch." + noColumns.getName() + "' has no supported columns (all 1 columns are not supported)");

            // Other tables should be visible in SHOW TABLES (the no_supported_columns might be included or might be not) and information_schema.tables
            assertThat(computeActual("SHOW TABLES").getOnlyColumn())
                    .contains("orders", noSupportedColumns.getName(), supportedColumns.getName(), noColumns.getName());
            assertThat(computeActual("SELECT table_name FROM information_schema.tables WHERE table_schema = 'tpch'").getOnlyColumn())
                    .contains("orders", noSupportedColumns.getName(), supportedColumns.getName(), noColumns.getName());

            // Other tables should be introspectable with SHOW COLUMNS and information_schema.columns
            assertQuery("SHOW COLUMNS FROM " + supportedColumns.getName(), "VALUES ('good', 'varchar(5)', '', '')");

            // Listing columns in all tables should not fail due to tables with no columns
            computeActual("SELECT column_name FROM information_schema.columns WHERE table_schema = 'tpch'");
        }
    }

    // Unimplemented in CockroachDB
    @Override
    @Test
    public void testForeignTable()
    {
    }

    // Bigint is alias for int in CockroachDB
    @Override
    @Test
    public void testNonLowercaseUserDefinedTypeName()
    {
        String enumType = "TEST_ENUM_" + randomNameSuffix();
        onRemoteDatabase().execute("CREATE TYPE public.\"" + enumType + "\" AS ENUM ('A', 'B')");
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
                "test_case_sensitive_",
                "(id bigint, user_type public.\"" + enumType + "\")",
                List.of("1, 'A'", "2, 'B'"))) {
            assertThat(query("SELECT id FROM " + testTable.getName() + " WHERE user_type = 'A'"))
                    .matches("VALUES BIGINT '1'");
        }
        finally {
            onRemoteDatabase().execute("DROP TYPE public.\"" + enumType + "\"");
        }
    }

    @Override
    @Test
    public void testPartitionedTables()
    {
        try (TestTable testTable = new TestTable(
                cockroachDBServer::execute,
                "test_part_tbl",
                "(id BIGINT NOT NULL, payload VARCHAR, logdate DATE NOT NULL, PRIMARY KEY (logdate, id)) " +
                        "PARTITION BY RANGE (logdate) (" +
                        "PARTITION p202111 VALUES FROM ('2021-11-01') TO ('2021-12-01'), " +
                        "PARTITION p202112 VALUES FROM ('2021-12-01') TO ('2022-01-01')" +
                        ")"
        )) {
            String values202111 = "(CAST(1 AS BIGINT), CAST('A' AS VARCHAR), CAST('2021-11-01' AS DATE)), " +
                    "(CAST(2 AS BIGINT), CAST('B' AS VARCHAR), CAST('2021-11-25' AS DATE))";
            String values202112 = "(CAST(3 AS BIGINT), CAST('C' AS VARCHAR), CAST('2021-12-01' AS DATE))";

            assertUpdate(format("INSERT INTO %s VALUES %s, %s", testTable.getName(), values202111, values202112), 3);

            assertThat(computeActual("SELECT name FROM crdb_internal.partitions").getOnlyColumnAsSet())
                    .contains("p202111", "p202112");

            assertQuery(format("SELECT * FROM %s", testTable.getName()),
                    format("VALUES %s, %s", values202111, values202112));

            assertQuery(format("SELECT * FROM %s WHERE logdate >= DATE '2021-12-01' AND logdate < DATE '2022-01-01'", testTable.getName()),
                    "VALUES " + values202112);
        }

    }

    // Money type not yet implemented
    @Override
    @Test
    public void testReverseFunctionOnSpecialColumn()
    {
    }

    // Bigint instead of int
    @Override
    @Test
    public void testUserDefinedTypeNameContainsDoubleQuotes()
    {
        String enumType = "test_double_\"\"_quotes_" + randomNameSuffix();
        onRemoteDatabase().execute("CREATE TYPE public.\"" + enumType + "\" AS ENUM ('A', 'B')");
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
                "test_case_sensitive_",
                "(id bigint, user_type public.\"" + enumType + "\")",
                List.of("1, 'A'", "2, 'B'"))) {
            assertThat(query("SELECT id FROM " + testTable.getName() + " WHERE user_type = 'A'"))
                    .matches("VALUES BIGINT '1'");
        }
        finally {
            onRemoteDatabase().execute("DROP TYPE public.\"" + enumType + "\"");
        }
    }

    // Different error message
    @Override
    @Test
    public void testInsertWithFailureDoesNotLeaveBehindOrphanedTable()
            throws Exception
    {
        String schemaName = format("tmp_schema_%s", UUID.randomUUID().toString().replaceAll("-", ""));
        try (AutoCloseable schema = withSchema(schemaName);
                TestTable table = new TestTable(onRemoteDatabase(), format("%s.test_cleanup", schemaName), "(x INTEGER)")) {
            assertQuery(format("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'", schemaName), "VALUES '" + table.getName().replace(schemaName + ".", "") + "'");

            onRemoteDatabase().execute("ALTER TABLE " + table.getName() + " ADD CHECK (x > 0)");

            assertQueryFails("INSERT INTO " + table.getName() + " (x) VALUES (0)", "ERROR: failed to satisfy CHECK constraint \\(.* > 0:::INT8\\)");
            assertQuery(format("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'", schemaName), "VALUES '" + table.getName().replace(schemaName + ".", "") + "'");
        }
    }

    @Override
    protected void verifyConcurrentUpdateFailurePermissible(Exception e)
    {
        assertThat(e).hasMessageMatching("WriteTooOldError.*");
    }

    // Casting to integer actually returns a "bigint"
    protected Optional<SetColumnTypeSetup> filterSetColumnTypesDataProvider(SetColumnTypeSetup setup)
    {
        if (setup.newColumnType().contains("integer") || setup.newColumnType().contains("bigint") || setup.newColumnType().contains("tinyint") || setup.newColumnType().contains("time zone") || setup.sourceValueLiteral().contains("char-to-varchar")) {
            return Optional.empty();
        }
        return Optional.of(setup);
    }

    @Override
    @Test
    @Timeout(60)
    public void testCancellation()
            throws Exception
    {
        if (!hasBehavior(SUPPORTS_CANCELLATION)) {
            abort("Cancellation is not supported by given connector");
        }

        try (TestView sleepingView = createSleepingView(new Duration(5, SECONDS))) {
            String query = "SELECT * FROM " + sleepingView.getName();
            Future<?> future = executor.submit(() -> assertQueryFails(query, "Query killed. Message: Killed by test"));
            QueryId queryId = getQueryId(query);
            assertUpdate(format("CALL system.runtime.kill_query(query_id => '%s', message => '%s')", queryId, "Killed by test"));
            future.get();
        }
    }
}
