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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.TestProcedure;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TestView;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.plugin.sqlserver.DataCompression.NONE;
import static io.trino.plugin.sqlserver.DataCompression.PAGE;
import static io.trino.plugin.sqlserver.DataCompression.ROW;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseSqlServerConnectorTest
        extends BaseJdbcConnectorTest
{
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_JOIN_PUSHDOWN,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY,
                 SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN,
                 SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY -> true;
            case SUPPORTS_ADD_COLUMN_WITH_COMMENT,
                 SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION,
                 SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT,
                 SUPPORTS_AGGREGATION_PUSHDOWN_COVARIANCE,
                 SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION,
                 SUPPORTS_ARRAY,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                 SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT,
                 SUPPORTS_DROP_NOT_NULL_CONSTRAINT,
                 SUPPORTS_DROP_SCHEMA_CASCADE,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_NEGATIVE_DATE,
                 SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY,
                 SUPPORTS_RENAME_SCHEMA,
                 SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_SET_COLUMN_TYPE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                onRemoteDatabase(),
                "table",
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT 43," +
                        "col_nonnull_default BIGINT NOT NULL DEFAULT 42," +
                        "col_required2 BIGINT NOT NULL)");
    }

    @Override
    protected TestTable createTableWithUnsupportedColumn()
    {
        return new TestTable(
                onRemoteDatabase(),
                "test_unsupported_column_present",
                "(one bigint, two sql_variant, three varchar(10))");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("timestamp(3) with time zone") ||
                typeName.equals("timestamp(6) with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        return Optional.of(dataMappingTestSetup);
    }

    @Test
    public void testReadFromView()
    {
        try (TestView view = new TestView(onRemoteDatabase(), "test_view", "SELECT * FROM orders")) {
            assertThat(getQueryRunner().tableExists(getSession(), view.getName())).isTrue();
            assertQuery("SELECT orderkey FROM " + view.getName(), "SELECT orderkey FROM orders");
        }
    }

    @Override
    protected void verifyAddNotNullColumnToNonEmptyTableFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching(
                "ALTER TABLE only allows columns to be added that can contain nulls, " +
                        "or have a DEFAULT definition specified, or the column being added is an identity or timestamp column, " +
                        "or alternatively if none of the previous conditions are satisfied the table must be empty to allow addition of this column\\. " +
                        "Column '.*' cannot be added to non-empty table '.*' because it does not satisfy these conditions\\.");
    }

    @Override
    protected void verifyConcurrentAddColumnFailurePermissible(Exception e)
    {
        assertThat(e).hasMessageContaining("was deadlocked on lock resources");
    }

    @Test
    public void testColumnComment()
    {
        try (TestTable testTable = new TestTable(onRemoteDatabase(), "test_column_comment", "(col1 bigint, col2 bigint, col3 bigint)")) {
            onRemoteDatabase().execute("" +
                    "EXEC sp_addextendedproperty " +
                    " 'MS_Description', 'test comment', " +
                    " 'Schema', 'dbo', " +
                    " 'Table', '" + testTable.getName() + "', " +
                    " 'Column', 'col1'");

            // SQL Server JDBC driver doesn't support REMARKS for column comment https://github.com/Microsoft/mssql-jdbc/issues/646
            assertQuery(
                    "SELECT column_name, comment FROM information_schema.columns WHERE table_schema = 'dbo' AND table_name = '" + testTable.getName() + "'",
                    "VALUES ('col1', null), ('col2', null), ('col3', null)");
        }
    }

    @Test
    public void testPredicatePushdown()
    {
        // varchar inequality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name != 'ROMANIA' AND name != 'ALGERIA'"))
                .isFullyPushedDown();

        // varchar equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'ROMANIA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // varchar range
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name BETWEEN 'POLAND' AND 'RPA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                // We are not supporting range predicate pushdown for varchars
                .isNotFullyPushedDown(FilterNode.class);

        // varchar NOT IN
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name NOT IN ('POLAND', 'ROMANIA', 'VIETNAM')"))
                .isFullyPushedDown();

        // varchar NOT IN with small compaction threshold
        assertThat(query(
                Session.builder(getSession())
                        .setCatalogSessionProperty("sqlserver", "domain_compaction_threshold", "1")
                        .build(),
                "SELECT regionkey, nationkey, name FROM nation WHERE name NOT IN ('POLAND', 'ROMANIA', 'VIETNAM')"))
                // no pushdown because it was converted to range predicate
                .isNotFullyPushedDown(
                        node(
                                FilterNode.class,
                                // verify that no constraint is applied by the connector
                                tableScan(
                                        tableHandle -> ((JdbcTableHandle) tableHandle).getConstraint().isAll(),
                                        TupleDomain.all(),
                                        ImmutableMap.of())));

        // varchar IN without domain compaction
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name IN ('POLAND', 'ROMANIA', 'VIETNAM')"))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25))), " +
                        "(BIGINT '2', BIGINT '21', CAST('VIETNAM' AS varchar(25)))")
                .isFullyPushedDown();

        // varchar IN with small compaction threshold
        assertThat(query(
                Session.builder(getSession())
                        .setCatalogSessionProperty("sqlserver", "domain_compaction_threshold", "1")
                        .build(),
                "SELECT regionkey, nationkey, name FROM nation WHERE name IN ('POLAND', 'ROMANIA', 'VIETNAM')"))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25))), " +
                        "(BIGINT '2', BIGINT '21', CAST('VIETNAM' AS varchar(25)))")
                // no pushdown because it was converted to range predicate
                .isNotFullyPushedDown(
                        node(
                                FilterNode.class,
                                // verify that no constraint is applied by the connector
                                tableScan(
                                        tableHandle -> ((JdbcTableHandle) tableHandle).getConstraint().isAll(),
                                        TupleDomain.all(),
                                        ImmutableMap.of())));

        // varchar different case
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'romania'"))
                .returnsEmptyResult()
                .isFullyPushedDown();

        // bigint equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE nationkey = 19"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // bigint equality with small compaction threshold
        assertThat(query(
                Session.builder(getSession())
                        .setCatalogSessionProperty("sqlserver", "domain_compaction_threshold", "1")
                        .build(),
                "SELECT regionkey, nationkey, name FROM nation WHERE nationkey IN (19, 21)"))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25))), " +
                        "(BIGINT '2', BIGINT '21', CAST('VIETNAM' AS varchar(25)))")
                .isNotFullyPushedDown(FilterNode.class);

        // bigint range, with decimal to bigint simplification
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE nationkey BETWEEN 18.5 AND 19.5"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // date equality
        assertThat(query("SELECT orderkey FROM orders WHERE orderdate = DATE '1992-09-29'"))
                .matches("VALUES BIGINT '1250', 34406, 38436, 57570")
                .isFullyPushedDown();

        // predicate over aggregation key (likely to be optimized before being pushed down into the connector)
        assertThat(query("SELECT * FROM (SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey) WHERE regionkey = 3"))
                .matches("VALUES (BIGINT '3', BIGINT '77')")
                .isFullyPushedDown();

        // predicate over aggregation result
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey HAVING sum(nationkey) = 77"))
                .matches("VALUES (BIGINT '3', BIGINT '77')")
                .isFullyPushedDown();

        // decimals
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
                "test_decimal_pushdown",
                "(short_decimal decimal(9, 3), long_decimal decimal(30, 10))",
                List.of("123.321, 123456789.987654321"))) {
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE short_decimal <= 124"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE short_decimal <= 124"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE long_decimal <= 123456790"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE short_decimal <= 123.321"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE long_decimal <= 123456789.987654321"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE short_decimal = 123.321"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE long_decimal = 123456789.987654321"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();

            // varchar predicate over join
            Session joinPushdownEnabled = joinPushdownEnabled(getSession());
            assertThat(query(joinPushdownEnabled, "SELECT c.name, n.name FROM customer c JOIN nation n ON c.custkey = n.nationkey WHERE n.name = 'POLAND'"))
                    .isFullyPushedDown();

            // join on varchar columns
            assertThat(query(joinPushdownEnabled, "SELECT n.name, n2.regionkey FROM nation n JOIN nation n2 ON n.name = n2.name"))
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testNoPushdownOnCaseInsensitiveVarcharColumn()
    {
        // if collation on column is caseinsensitive we should not apply pushdown
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
                "test_collate",
                "(collate_column varchar(25) COLLATE Latin1_General_CI_AS)",
                List.of("'collation'", "'no_collation'"))) {
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE collate_column = 'collation'"))
                    .matches("VALUES " +
                            "(CAST('collation' AS varchar(25)))")
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE collate_column != 'collation'"))
                    .matches("VALUES " +
                            "(CAST('no_collation' AS varchar(25)))")
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE collate_column > 'collation'"))
                    .matches("VALUES " +
                            "(CAST('no_collation' AS varchar(25)))")
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE collate_column < 'no_collation'"))
                    .matches("VALUES " +
                            "(CAST('collation' AS varchar(25)))")
                    .isNotFullyPushedDown(FilterNode.class);
        }
    }

    @Test
    public void testNoJoinPushdownOnCaseInsensitiveVarcharColumn()
    {
        // if collation on column is caseinsensitive we should not apply join pushdown
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
                "test_join_collate",
                "(collate_column_1 varchar(25) COLLATE Latin1_General_CI_AS, collate_column_2 varchar(25) COLLATE Latin1_General_CI_AS)",
                List.of("'Collation', 'Collation'", "'collation', 'collation'"))) {
            assertThat(query(format("SELECT n.collate_column_1, n2.collate_column_2 FROM %1$s n JOIN %1$s n2 ON n.collate_column_1 = n2.collate_column_2", testTable.getName())))
                    .matches("VALUES " +
                            "((CAST('Collation' AS varchar(25))), (CAST('Collation' AS varchar(25)))), " +
                            "((CAST('collation' AS varchar(25))), (CAST('collation' AS varchar(25))))")
                    .joinIsNotFullyPushedDown();
            assertThat(query(format("SELECT n.collate_column_1, n2.collate_column_2 FROM %1$s n JOIN %1$s n2 ON n.collate_column_1 != n2.collate_column_2", testTable.getName())))
                    .matches("VALUES " +
                            "((CAST('collation' AS varchar(25))), (CAST('Collation' AS varchar(25)))), " +
                            "((CAST('Collation' AS varchar(25))), (CAST('collation' AS varchar(25))))")
                    .joinIsNotFullyPushedDown();
            assertThat(query(format("SELECT n.collate_column_1, n2.collate_column_2 FROM %1$s n JOIN %1$s n2 ON n.collate_column_1 = n2.collate_column_2 WHERE n.collate_column_1 = 'Collation'", testTable.getName())))
                    .matches("VALUES " +
                            "((CAST('Collation' AS varchar(25))), (CAST('Collation' AS varchar(25))))")
                    .joinIsNotFullyPushedDown();
            assertThat(query(format("SELECT n.collate_column_1, n2.collate_column_2 FROM %1$s n JOIN %1$s n2 ON n.collate_column_1 != n2.collate_column_2 WHERE n.collate_column_1 != 'collation'", testTable.getName())))
                    .matches("VALUES " +
                            "((CAST('Collation' AS varchar(25))), (CAST('collation' AS varchar(25))))")
                    .joinIsNotFullyPushedDown();
        }
    }

    @Override
    @Test
    public void testDeleteWithVarcharInequalityPredicate()
    {
        // Override this because by enabling this flag SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY,
        // we assume that we also support range pushdowns, but for now we only support 'not equal' pushdown,
        // so cannot enable this flag for now
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_varchar", "(col varchar(1))", ImmutableList.of("'a'", "'A'", "null"))) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE col != 'A'", 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES 'A', null");
        }
    }

    @Test
    public void testTooLargeDomainCompactionThreshold()
    {
        assertQueryFails(
                Session.builder(getSession())
                        .setCatalogSessionProperty("sqlserver", "domain_compaction_threshold", "10000")
                        .build(),
                "SELECT * from nation", "Domain compaction threshold \\(10000\\) cannot exceed 500");
    }

    /**
     * This test helps to tune TupleDomain simplification threshold.
     */
    @Test
    public void testNativeLargeIn()
    {
        // Using IN list of size 10_000 as bigger list (around 40_000) causes error:
        // "com.microsoft.sqlserver.jdbc.SQLServerException: Internal error: An expression services
        //  limit has been reached.Please look for potentially complex expressions in your query,
        //  and try to simplify them."
        //
        // List around 30_000 causes query to be really slow
        onRemoteDatabase().execute("SELECT count(*) FROM dbo.orders WHERE " + getLongInClause(0, 10_000));
    }

    /**
     * This test helps to tune TupleDomain simplification threshold.
     */
    @Test
    public void testNativeMultipleInClauses()
    {
        // using 1_000 for single IN list as 10_000 causes error:
        // "com.microsoft.sqlserver.jdbc.SQLServerException: Internal error: An expression services
        //  limit has been reached.Please look for potentially complex expressions in your query,
        //  and try to simplify them."
        String longInClauses = range(0, 10)
                .mapToObj(value -> getLongInClause(value * 1_000, 1_000))
                .collect(joining(" OR "));
        onRemoteDatabase().execute("SELECT count(*) FROM dbo.orders WHERE " + longInClauses);
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .matches("CREATE TABLE \\w+\\.\\w+\\.orders \\Q(\n" +
                        "   orderkey bigint,\n" +
                        "   custkey bigint,\n" +
                        "   orderstatus varchar(1),\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar(15),\n" +
                        "   clerk varchar(15),\n" +
                        "   shippriority integer,\n" +
                        "   comment varchar(79)\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   data_compression = 'NONE'\n" +
                        ")");
    }

    @Test
    public void testCreateWithDataCompression()
    {
        testCreateWithDataCompression(NONE);
        testCreateWithDataCompression(ROW);
        testCreateWithDataCompression(PAGE);
    }

    private void testCreateWithDataCompression(DataCompression dataCompression)
    {
        String tableName = "test_create_with_compression_" + randomNameSuffix();
        String createQuery = format("CREATE TABLE sqlserver.dbo.%s (\n" +
                        "   a bigint,\n" +
                        "   b bigint\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   data_compression = '%s'\n" +
                        ")",
                tableName,
                dataCompression);
        assertUpdate(createQuery);

        assertThat(getQueryRunner().execute("SHOW CREATE TABLE " + tableName).getOnlyValue())
                .isEqualTo(createQuery);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testShowCreateForPartitionedTablesWithDataCompression()
    {
        String partitionFunction = "pfSales" + randomNameSuffix();
        String partitionScheme = "psSales" + randomNameSuffix();
        String tableName = "partitionedsales" + randomNameSuffix();

        try {
            onRemoteDatabase().execute("CREATE PARTITION FUNCTION " + partitionFunction + " (DATE)\n" +
                    "AS RANGE LEFT FOR VALUES \n" +
                    "('2013-01-01', '2014-01-01', '2015-01-01')");
            onRemoteDatabase().execute("CREATE PARTITION SCHEME " + partitionScheme + "\n" +
                    "AS PARTITION " + partitionFunction + " \n" +
                    "ALL TO ([PRIMARY])");
            onRemoteDatabase().execute("CREATE TABLE " + tableName + " (\n" +
                    "   SalesDate DATE,\n" +
                    "   Quantity INT\n" +
                    ") ON " + partitionScheme + "(SalesDate) WITH (DATA_COMPRESSION = PAGE)");
            assertThat((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue())
                    .matches("CREATE TABLE \\w+\\.\\w+\\." + tableName + " \\Q(\n" +
                            "   salesdate date,\n" +
                            "   quantity integer\n" +
                            ")");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
            onRemoteDatabase().execute("DROP PARTITION SCHEME " + partitionScheme);
            onRemoteDatabase().execute("DROP PARTITION FUNCTION " + partitionFunction);
        }
    }

    @Test
    public void testShowCreateForIndexedAndCompressedTable()
    {
        // SHOW CREATE doesn't expose data compression for Indexed tables
        String tableName = "test_show_indexed_table" + randomNameSuffix();
        onRemoteDatabase().execute("CREATE TABLE " + tableName + " (\n" +
                "   key1 BIGINT NOT NULL,\n" +
                "   key2 BIGINT NOT NULL,\n" +
                "   key3 BIGINT NOT NULL,\n" +
                "   key4 BIGINT NOT NULL,\n" +
                "   key5 BIGINT NOT NULL,\n" +
                "   CONSTRAINT PK_IndexedTable PRIMARY KEY CLUSTERED (key1),\n" +
                "   CONSTRAINT IX_IndexedTable UNIQUE (key2, key3),\n" +
                "   INDEX IX_MyTable4 NONCLUSTERED (key4, key5))\n" +
                "   WITH (DATA_COMPRESSION = PAGE)");
        try {
            assertThat((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue())
                    .isEqualTo("CREATE TABLE sqlserver.dbo." + tableName + " (\n" +
                            "   key1 bigint NOT NULL,\n" +
                            "   key2 bigint NOT NULL,\n" +
                            "   key3 bigint NOT NULL,\n" +
                            "   key4 bigint NOT NULL,\n" +
                            "   key5 bigint NOT NULL\n" +
                            ")");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testShowCreateForUniqueConstraintCompressedTable()
    {
        String tableName = "test_show_unique_constraint_table" + randomNameSuffix();
        onRemoteDatabase().execute("CREATE TABLE " + tableName + " (\n" +
                "   key1 BIGINT NOT NULL,\n" +
                "   key2 BIGINT NOT NULL,\n" +
                "   key3 BIGINT NOT NULL,\n" +
                "   key4 BIGINT NOT NULL,\n" +
                "   key5 BIGINT NOT NULL,\n" +
                "   UNIQUE (key1, key4),\n" +
                "   UNIQUE (key2, key3))\n" +
                "   WITH (DATA_COMPRESSION = PAGE)");
        try {
            assertThat((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue())
                    .isEqualTo("CREATE TABLE sqlserver.dbo." + tableName + " (\n" +
                            "   key1 bigint NOT NULL,\n" +
                            "   key2 bigint NOT NULL,\n" +
                            "   key3 bigint NOT NULL,\n" +
                            "   key4 bigint NOT NULL,\n" +
                            "   key5 bigint NOT NULL\n" +
                            ")\n" +
                            "WITH (\n" +
                            "   data_compression = 'PAGE'\n" +
                            ")");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    @Override
    public void testDateYearOfEraPredicate()
    {
        // SQL Server throws an exception instead of an empty result when the value is out of range
        assertQuery("SELECT orderdate FROM orders WHERE orderdate = DATE '1997-09-14'", "VALUES DATE '1997-09-14'");
        assertQueryFails(
                "SELECT * FROM orders WHERE orderdate = DATE '-1996-09-14'",
                ".*\\QConversion failed when converting date and/or time from character string.\\E");
    }

    @Test
    @Override
    public void testNativeQuerySimple()
    {
        // override because SQL Server provides an empty string as the name for unnamed column
        assertQuery("SELECT * FROM TABLE(system.query(query => 'SELECT 1 a'))", "VALUES 1");
    }

    @Override
    protected String errorMessageForCreateTableAsSelectNegativeDate(String date)
    {
        return "Failed to insert data: Conversion failed when converting date and/or time from character string.";
    }

    @Override
    protected String errorMessageForInsertNegativeDate(String date)
    {
        return "Failed to insert data: Conversion failed when converting date and/or time from character string.";
    }

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return format("Cannot insert the value NULL into column '%s'.*", columnName);
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.of(128);
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("The identifier that starts with '.*' is too long. Maximum length is 128.");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(128);
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("(The identifier that starts with '.*' is too long. Maximum length is 128.|Table name must be shorter than or equal to '128' characters but got '129')");
    }

    @Override
    protected OptionalInt maxColumnNameLength()
    {
        return OptionalInt.of(128);
    }

    @Override
    protected void verifyColumnNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("Column name must be shorter than or equal to '128' characters but got '129': '.*'");
    }

    @Test
    public void testSelectFromProcedureFunction()
    {
        try (TestProcedure testProcedure = createTestingProcedure("SELECT * FROM nation WHERE nationkey = 1")) {
            assertQuery(
                    format("SELECT name FROM TABLE(system.procedure(query => 'EXECUTE %s.%s'))".formatted(getSession().getSchema().orElseThrow(), testProcedure.getName()), getSession().getSchema().orElseThrow()),
                    "VALUES 'ARGENTINA'");
        }
    }

    @Test
    public void testSelectFromProcedureFunctionWithInputParameter()
    {
        try (TestProcedure testProcedure = createTestingProcedure(
                "@nationkey bigint, @name varchar(30)",
                "SELECT * FROM nation WHERE nationkey = @nationkey AND name = @name")) {
            assertQuery(
                    "SELECT nationkey, name FROM TABLE(system.procedure(query => 'EXECUTE %s.%s 0, ''ALGERIA''')) ".formatted(getSession().getSchema().orElseThrow(), testProcedure.getName()),
                    "VALUES (0, 'ALGERIA')");
        }
    }

    @Test
    public void testSelectFromProcedureFunctionWithOutputParameter()
    {
        try (TestProcedure testProcedure = createTestingProcedure("@row_count bigint OUTPUT", "SELECT * FROM nation; SELECT @row_count = @@ROWCOUNT")) {
            assertQueryFails(
                    "SELECT name FROM TABLE(system.procedure(query => 'EXECUTE %s.%s')) ".formatted(getSession().getSchema().orElseThrow(), testProcedure.getName()),
                    "Failed to get table handle for procedure query\\. Procedure or function '.*' expects parameter '@row_count', which was not supplied\\.");
        }
    }

    @Test
    public void testFilterPushdownRestrictedForProcedureFunction()
    {
        try (TestProcedure testProcedure = createTestingProcedure("SELECT * FROM nation")) {
            assertThat(query("SELECT name FROM TABLE(system.procedure(query => 'EXECUTE %s.%s')) WHERE nationkey = 0".formatted(getSession().getSchema().orElseThrow(), testProcedure.getName())))
                    .isNotFullyPushedDown(FilterNode.class)
                    .skippingTypesCheck()
                    .matches("VALUES 'ALGERIA'");
        }
    }

    @Test
    public void testAggregationPushdownRestrictedForProcedureFunction()
    {
        try (TestProcedure testProcedure = createTestingProcedure("SELECT * FROM nation")) {
            assertThat(query(
                    "SELECT COUNT(*) FROM TABLE(system.procedure(query => 'EXECUTE %s.%s'))"
                            .formatted(getSession().getSchema().orElseThrow(), testProcedure.getName())))
                    .isNotFullyPushedDown(AggregationNode.class)
                    .matches("VALUES BIGINT '25'");
        }
    }

    @Test
    public void testJoinPushdownRestrictedForProcedureFunction()
    {
        try (TestProcedure testProcedure = createTestingProcedure("SELECT * FROM nation")) {
            assertThat(query(
                    joinPushdownEnabled(getSession()),
                    "SELECT nationkey FROM TABLE(system.procedure(query => 'EXECUTE %s.%s')) INNER JOIN nation USING (nationkey) ORDER BY 1 LIMIT 1"
                            .formatted(getSession().getSchema().orElseThrow(), testProcedure.getName())))
                    .joinIsNotFullyPushedDown()
                    .matches("VALUES BIGINT '0'");
        }
    }

    @Test
    public void testProcedureWithSingleIfStatement()
    {
        try (TestProcedure testProcedure = createTestingProcedure(
                "@id INTEGER",
                """
                IF @id > 50
                    SELECT 1 as first_column;
                """)) {
            assertQuery(
                    format("SELECT first_column FROM TABLE(system.procedure(query => 'EXECUTE %s.%s 100')) ".formatted(getSession().getSchema().orElseThrow(), testProcedure.getName()), getSession().getSchema().orElseThrow()),
                    "VALUES 1");

            assertQueryFails(
                    "SELECT first_column FROM TABLE(system.procedure(query => 'EXECUTE %s.%s 10')) ".formatted(getSession().getSchema().orElseThrow(), testProcedure.getName()),
                    "The statement did not return a result set.");
        }
    }

    @Test
    public void testProcedureWithIfElseStatement()
    {
        try (TestProcedure testProcedure = createTestingProcedure(
                "@id INTEGER",
                """
                IF @id > 50
                    SELECT 1 as first_column;
                ELSE
                    SELECT '2' as second_column;
                """)) {
            assertQueryFails(
                    "SELECT * FROM TABLE(system.procedure(query => 'EXECUTE %s.%s 100')) "
                            .formatted(getSession().getSchema().orElseThrow(), testProcedure.getName()),
                    "Procedure has multiple ResultSets for query: .*");
        }
    }

    @Test
    public void testProcedureWithMultipleResultSet()
    {
        try (TestProcedure testProcedure = createTestingProcedure("SELECT 1 as first_row; SELECT 2 as second_row")) {
            assertQueryFails(
                    "SELECT * FROM TABLE(system.procedure(query => 'EXECUTE %s.%s')) "
                            .formatted(getSession().getSchema().orElseThrow(), testProcedure.getName()),
                    "Procedure has multiple ResultSets for query: .*");
        }
    }

    @Test
    public void testProcedureWithCreateOperation()
    {
        String tableName = "table_to_create" + randomNameSuffix();
        try (TestProcedure testProcedure = createTestingProcedure("CREATE TABLE %s (id BIGINT)".formatted(tableName))) {
            assertQueryFails(
                    "SELECT * FROM TABLE(system.procedure(query => 'EXECUTE %s.%s'))"
                            .formatted(getSession().getSchema().orElseThrow(), testProcedure.getName()),
                    "Failed to get table handle for procedure query. The statement did not return a result set.");
            assertQueryReturnsEmptyResult("SHOW TABLES LIKE '%s'".formatted(tableName));
        }
    }

    @Test
    public void testProcedureWithDropOperation()
    {
        try (TestTable table = new TestTable(onRemoteDatabase(), "table_to_drop", "(id BIGINT)");
                TestProcedure testProcedure = createTestingProcedure("DROP TABLE " + table.getName())) {
            assertQueryFails(
                    "SELECT * FROM TABLE(system.procedure(query => 'EXECUTE %s.%s'))"
                            .formatted(getSession().getSchema().orElseThrow(), testProcedure.getName()),
                    "Failed to get table handle for procedure query. The statement did not return a result set.");
            assertQuery("SHOW TABLES LIKE '%s'".formatted(table.getName()), "VALUES '%s'".formatted(table.getName()));
        }
    }

    @Test
    public void testProcedureWithInsertOperation()
    {
        try (TestTable table = new TestTable(onRemoteDatabase(), "table_to_insert", "(id BIGINT)");
                TestProcedure testProcedure = createTestingProcedure("INSERT INTO %s VALUES (1)".formatted(table.getName()))) {
            assertQueryFails(
                    "SELECT * FROM TABLE(system.procedure(query => 'EXECUTE %s.%s'))"
                            .formatted(getSession().getSchema().orElseThrow(), testProcedure.getName()),
                    "Failed to get table handle for procedure query. The statement did not return a result set.");
            assertQueryReturnsEmptyResult("SELECT * FROM " + table.getName());
        }
    }

    @Test
    public void testProcedureWithDeleteOperation()
    {
        try (TestTable table = new TestTable(onRemoteDatabase(), "table_to_delete", "(id BIGINT)", ImmutableList.of("1", "2", "3"));
                TestProcedure testProcedure = createTestingProcedure("DELETE %s".formatted(table.getName()))) {
            assertQueryFails(
                    "SELECT * FROM TABLE(system.procedure(query => 'EXECUTE %s.%s'))"
                            .formatted(getSession().getSchema().orElseThrow(), testProcedure.getName()),
                    "Failed to get table handle for procedure query. The statement did not return a result set.");
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (1), (2), (3)");
        }
    }

    @Test
    public void testProcedureWithUpdateOperation()
    {
        try (TestTable table = new TestTable(onRemoteDatabase(), "table_to_update", "(id BIGINT)", ImmutableList.of("1", "2", "3"));
                TestProcedure testProcedure = createTestingProcedure("UPDATE %s SET id = 4".formatted(table.getName()))) {
            assertQueryFails(
                    "SELECT * FROM TABLE(system.procedure(query => 'EXECUTE %s.%s'))"
                            .formatted(getSession().getSchema().orElseThrow(), testProcedure.getName()),
                    "Failed to get table handle for procedure query. The statement did not return a result set.");
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (1), (2), (3)");
        }
    }

    @Test
    public void testProcedureWithMergeOperation()
    {
        try (TestTable sourceTable = new TestTable(onRemoteDatabase(), "source_table", "(id BIGINT)", ImmutableList.of("1", "2", "3"));
                TestTable targetTable = new TestTable(onRemoteDatabase(), "destination_table", "(id BIGINT)", ImmutableList.of("3", "4", "5"))) {
            String mergeQuery =
                    """
                    MERGE %s AS TARGET USING %s AS SOURCE
                    ON (TARGET.id = SOURCE.id)
                    WHEN NOT MATCHED BY TARGET
                        THEN INSERT(id) VALUES(SOURCE.id)
                    WHEN NOT MATCHED BY SOURCE
                        THEN DELETE
                    """.formatted(targetTable.getName(), sourceTable.getName());
            try (TestProcedure testProcedure = createTestingProcedure(mergeQuery + ";")) {
                assertQueryFails(
                        "SELECT * FROM TABLE(system.procedure(query => 'EXECUTE %s.%s'))"
                                .formatted(getSession().getSchema().orElseThrow(), testProcedure.getName()),
                        "Failed to get table handle for procedure query. The statement did not return a result set.");
                assertQuery("SELECT * FROM " + targetTable.getName(), "VALUES (3), (4), (5)");
            }
        }
    }

    @Test
    @Override
    public void testConstantUpdateWithVarcharInequalityPredicates()
    {
        // Sql Server supports push down predicate for not equal operator
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_update_varchar", "(col1 INT, col2 varchar(1))", ImmutableList.of("1, 'a'", "2, 'A'"))) {
            assertUpdate("UPDATE " + table.getName() + " SET col1 = 20 WHERE col2 != 'A'", 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (20, 'a'), (2, 'A')");
        }
    }

    @Test
    void testInvalidTable()
    {
        String tableName = "sqlserver.dbo.bogus";
        assertQueryFails("SELECT * FROM " + tableName, ".* Table '%s' does not exist".formatted(tableName));
    }

    @Test
    void testInvalidSchema()
    {
        assertQueryFails(
                "SELECT * FROM sqlserver.does_not_exist.bogus",
                ".* Schema 'does_not_exist' does not exist");
    }

    @Test
    void testNationJoinRegion()
    {
        assertQuery(
                "SELECT c.name, t.name FROM nation c JOIN " +
                        "tpch.tiny.region t ON c.regionkey = t.regionkey " +
                        "WHERE c.nationkey = 3",
                "VALUES ('CANADA', 'AMERICA')");
    }

    @Test
    void testNationSelfInnerJoin()
    {
        assertQuery(
                "SELECT n1.name, n2.regionkey FROM nation n1 JOIN " +
                        "nation n2 ON n1.nationkey = n2.regionkey " +
                        "WHERE n1.nationkey = 3",
                "VALUES ('CANADA', 3), ('CANADA', 3), ('CANADA', 3), ('CANADA', 3), ('CANADA', 3)");
    }

    @Test
    void testInvalidColumn()
    {
        assertQueryFails("SELECT bogus FROM nation", ".* Column 'bogus' cannot be resolved");
    }

    private TestProcedure createTestingProcedure(String baseQuery)
    {
        return createTestingProcedure("", baseQuery);
    }

    private TestProcedure createTestingProcedure(String inputArguments, String baseQuery)
    {
        String procedureName = "procedure" + randomNameSuffix();
        return new TestProcedure(
                onRemoteDatabase(),
                procedureName,
                """
                    CREATE PROCEDURE %s.%s %s
                    AS BEGIN
                        %s
                    END
                """.formatted(getSession().getSchema().orElseThrow(), procedureName, inputArguments, baseQuery));
    }

    private String getLongInClause(int start, int length)
    {
        String longValues = range(start, start + length)
                .mapToObj(Integer::toString)
                .collect(joining(", "));
        return "orderkey IN (" + longValues + ")";
    }

    @Override
    protected Session joinPushdownEnabled(Session session)
    {
        return Session.builder(super.joinPushdownEnabled(session))
                // strategy is AUTOMATIC by default and would not work for certain test cases (even if statistics are collected)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "join_pushdown_strategy", "EAGER")
                .build();
    }
}
