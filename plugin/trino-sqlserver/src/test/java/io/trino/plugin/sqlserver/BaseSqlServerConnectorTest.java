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
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.sqlserver.DataCompression.NONE;
import static io.trino.plugin.sqlserver.DataCompression.PAGE;
import static io.trino.plugin.sqlserver.DataCompression.ROW;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public abstract class BaseSqlServerConnectorTest
        extends BaseJdbcConnectorTest
{
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY:
            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY:
                return false;

            case SUPPORTS_AGGREGATION_PUSHDOWN_STDDEV:
            case SUPPORTS_AGGREGATION_PUSHDOWN_VARIANCE:
                return true;

            case SUPPORTS_JOIN_PUSHDOWN:
                return true;

            case SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM:
                return false;

            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
                return false;

            case SUPPORTS_ARRAY:
            case SUPPORTS_NEGATIVE_DATE:
                return false;

            case SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS:
                return false;

            case SUPPORTS_RENAME_SCHEMA:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
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
        if (typeName.equals("date")) {
            // SQL Server plus 10 days when the date is the range of 1582 Oct 5 and 14
            if (dataMappingTestSetup.getSampleValueLiteral().equals("DATE '1582-10-05'") || dataMappingTestSetup.getSampleValueLiteral().equals("DATE '1582-10-14'")) {
                return Optional.empty();
            }
        }
        if (typeName.equals("timestamp(3) with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        return Optional.of(dataMappingTestSetup);
    }

    @Test
    public void testReadFromView()
    {
        onRemoteDatabase().execute("CREATE VIEW test_view AS SELECT * FROM orders");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_view"));
        assertQuery("SELECT orderkey FROM test_view", "SELECT orderkey FROM orders");
        onRemoteDatabase().execute("DROP VIEW IF EXISTS test_view");
    }

    @Test
    public void testColumnComment()
            throws Exception
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
            throws Exception
    {
        // varchar equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'ROMANIA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                // SQL Server is case insensitive by default
                .isNotFullyPushedDown(FilterNode.class);

        // varchar range
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name BETWEEN 'POLAND' AND 'RPA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                // SQL Server is case insensitive by default
                .isNotFullyPushedDown(FilterNode.class);

        // varchar IN without domain compaction
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name IN ('POLAND', 'ROMANIA', 'VIETNAM')"))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25))), " +
                        "(BIGINT '2', BIGINT '21', CAST('VIETNAM' AS varchar(25)))")
                // SQL Server is case insensitive by default
                .isNotFullyPushedDown(
                        node(
                                FilterNode.class,
                                // verify that pushed down constraint is applied by the connector
                                tableScan(
                                        tableHandle -> {
                                            TupleDomain<ColumnHandle> constraint = ((JdbcTableHandle) tableHandle).getConstraint();
                                            ColumnHandle nameColumn = constraint.getDomains().orElseThrow()
                                                    .keySet().stream()
                                                    .map(JdbcColumnHandle.class::cast)
                                                    .filter(column -> column.getColumnName().equals("name"))
                                                    .collect(onlyElement());
                                            return constraint.getDomains().get().get(nameColumn)
                                                    .equals(Domain.multipleValues(
                                                            createVarcharType(25),
                                                            ImmutableList.of(
                                                                    utf8Slice("POLAND"),
                                                                    utf8Slice("ROMANIA"),
                                                                    utf8Slice("VIETNAM"))));
                                        },
                                        TupleDomain.all(),
                                        ImmutableMap.of())));

        // varchar IN with small compaction threshold
        assertThat(query(
                Session.builder(getSession())
                        .setCatalogSessionProperty("sqlserver", "domain_compaction_threshold", "1")
                        .build(),
                "SELECT regionkey, nationkey, name FROM nation WHERE name IN ('POLAND', 'ROMANIA', 'VIETNAM')"))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25))), " +
                        "(BIGINT '2', BIGINT '21', CAST('VIETNAM' AS varchar(25)))")
                // SQL Server is case insensitive by default
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
                // SQL Server is case insensitive by default
                .isNotFullyPushedDown(FilterNode.class);

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

    @Test(dataProvider = "dataCompression")
    public void testCreateWithDataCompression(DataCompression dataCompression)
    {
        String tableName = "test_create_with_compression_" + randomTableSuffix();
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

        assertEquals(getQueryRunner().execute("SHOW CREATE TABLE " + tableName).getOnlyValue(), createQuery);

        assertUpdate("DROP TABLE " + tableName);
    }

    @DataProvider
    public Object[][] dataCompression()
    {
        return new Object[][] {
                {NONE},
                {ROW},
                {PAGE}
        };
    }

    @Test
    public void testShowCreateForPartitionedTablesWithDataCompression()
    {
        onRemoteDatabase().execute("CREATE PARTITION FUNCTION pfSales (DATE)\n" +
                "AS RANGE LEFT FOR VALUES \n" +
                "('2013-01-01', '2014-01-01', '2015-01-01')");
        onRemoteDatabase().execute("CREATE PARTITION SCHEME psSales\n" +
                "AS PARTITION pfSales \n" +
                "ALL TO ([PRIMARY])");
        onRemoteDatabase().execute("CREATE TABLE partitionedsales (\n" +
                "   SalesDate DATE,\n" +
                "   Quantity INT\n" +
                ") ON psSales(SalesDate) WITH (DATA_COMPRESSION = PAGE)");
        assertThat((String) computeActual("SHOW CREATE TABLE partitionedsales").getOnlyValue())
                .matches("CREATE TABLE \\w+\\.\\w+\\.partitionedsales \\Q(\n" +
                        "   salesdate date,\n" +
                        "   quantity integer\n" +
                        ")");
        assertUpdate("DROP TABLE partitionedSales");
        onRemoteDatabase().execute("DROP PARTITION SCHEME psSales");
        onRemoteDatabase().execute("DROP PARTITION FUNCTION pfSales");
    }

    @Test
    public void testShowCreateForIndexedAndCompressedTable()
    {
        // SHOW CREATE doesn't expose data compression for Indexed tables
        onRemoteDatabase().execute("CREATE TABLE test_show_indexed_table (\n" +
                "   key1 BIGINT NOT NULL,\n" +
                "   key2 BIGINT NOT NULL,\n" +
                "   key3 BIGINT NOT NULL,\n" +
                "   key4 BIGINT NOT NULL,\n" +
                "   key5 BIGINT NOT NULL,\n" +
                "   CONSTRAINT PK_IndexedTable PRIMARY KEY CLUSTERED (key1),\n" +
                "   CONSTRAINT IX_IndexedTable UNIQUE (key2, key3),\n" +
                "   INDEX IX_MyTable4 NONCLUSTERED (key4, key5))\n" +
                "   WITH (DATA_COMPRESSION = PAGE)");

        assertThat((String) computeActual("SHOW CREATE TABLE test_show_indexed_table").getOnlyValue())
                .isEqualTo("CREATE TABLE sqlserver.dbo.test_show_indexed_table (\n" +
                        "   key1 bigint NOT NULL,\n" +
                        "   key2 bigint NOT NULL,\n" +
                        "   key3 bigint NOT NULL,\n" +
                        "   key4 bigint NOT NULL,\n" +
                        "   key5 bigint NOT NULL\n" +
                        ")");

        assertUpdate("DROP TABLE test_show_indexed_table");
    }

    @Test
    public void testShowCreateForUniqueConstraintCompressedTable()
    {
        onRemoteDatabase().execute("CREATE TABLE test_show_unique_constraint_table (\n" +
                "   key1 BIGINT NOT NULL,\n" +
                "   key2 BIGINT NOT NULL,\n" +
                "   key3 BIGINT NOT NULL,\n" +
                "   key4 BIGINT NOT NULL,\n" +
                "   key5 BIGINT NOT NULL,\n" +
                "   UNIQUE (key1, key4),\n" +
                "   UNIQUE (key2, key3))\n" +
                "   WITH (DATA_COMPRESSION = PAGE)");

        assertThat((String) computeActual("SHOW CREATE TABLE test_show_unique_constraint_table").getOnlyValue())
                .isEqualTo("CREATE TABLE sqlserver.dbo.test_show_unique_constraint_table (\n" +
                        "   key1 bigint NOT NULL,\n" +
                        "   key2 bigint NOT NULL,\n" +
                        "   key3 bigint NOT NULL,\n" +
                        "   key4 bigint NOT NULL,\n" +
                        "   key5 bigint NOT NULL\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   data_compression = 'PAGE'\n" +
                        ")");

        assertUpdate("DROP TABLE test_show_unique_constraint_table");
    }

    @Test
    @Override
    public void testDateYearOfEraPredicate()
    {
        // SQL Server throws an exception instead of an empty result when the value is out of range
        assertQuery("SELECT orderdate FROM orders WHERE orderdate = DATE '1997-09-14'", "VALUES DATE '1997-09-14'");
        assertQueryFails(
                "SELECT * FROM orders WHERE orderdate = DATE '-1996-09-14'",
                "Conversion failed when converting date and/or time from character string\\.");
    }

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return format("Cannot insert the value NULL into column '%s'.*", columnName);
    }

    private String getLongInClause(int start, int length)
    {
        String longValues = range(start, start + length)
                .mapToObj(Integer::toString)
                .collect(joining(", "));
        return "orderkey IN (" + longValues + ")";
    }
}
