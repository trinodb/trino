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
package io.trino.plugin.phoenix5;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.plugin.jdbc.UnsupportedTypeHandling;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.DOMAIN_COMPACTION_THRESHOLD;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.UNSUPPORTED_TYPE_HANDLING;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.plugin.phoenix5.PhoenixQueryRunner.createPhoenixQueryRunner;
import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.limit;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.sort;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.topN;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.trino.sql.planner.plan.TopNNode.Step.FINAL;
import static io.trino.sql.tree.SortItem.NullOrdering.FIRST;
import static io.trino.sql.tree.SortItem.NullOrdering.LAST;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;
import static io.trino.sql.tree.SortItem.Ordering.DESCENDING;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPhoenixConnectorTest
        extends BaseJdbcConnectorTest
{
    private TestingPhoenixServer testingPhoenixServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testingPhoenixServer = closeAfterClass(TestingPhoenixServer.getInstance()).get();
        return createPhoenixQueryRunner(testingPhoenixServer, ImmutableMap.of(), REQUIRED_TPCH_TABLES);
    }

    @SuppressWarnings("DuplicateBranchesInSwitch")
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_LIMIT_PUSHDOWN:
            case SUPPORTS_TOPN_PUSHDOWN:
            case SUPPORTS_AGGREGATION_PUSHDOWN:
                return false;

            case SUPPORTS_RENAME_SCHEMA:
                return false;

            case SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT:
            case SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT:
            case SUPPORTS_RENAME_TABLE:
                return false;

            case SUPPORTS_ADD_COLUMN_WITH_COMMENT:
            case SUPPORTS_SET_COLUMN_TYPE:
                return false;

            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
                return false;

            case SUPPORTS_NOT_NULL_CONSTRAINT:
                return false;

            case SUPPORTS_TRUNCATE:
                return false;

            case SUPPORTS_ROW_TYPE:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        throw new SkipException("Phoenix connector does not support column default values");
    }

    @Override
    protected TestTable createTableWithUnsupportedColumn()
    {
        // Apparently all Phoenix types are supported in the Phoenix connector.
        throw new SkipException("Cannot find an unsupported data type");
    }

    @Override
    public void testRenameColumn()
    {
        assertThatThrownBy(super::testRenameColumn)
                // TODO (https://github.com/trinodb/trino/issues/7205) support column rename in Phoenix
                .hasMessageContaining("Syntax error. Encountered \"RENAME\"");
        throw new SkipException("Rename column is not yet supported by Phoenix connector");
    }

    @Override
    public void testAlterTableRenameColumnToLongName()
    {
        assertThatThrownBy(super::testAlterTableRenameColumnToLongName)
                // TODO (https://github.com/trinodb/trino/issues/7205) support column rename in Phoenix
                .hasMessageContaining("Syntax error. Encountered \"RENAME\"");
        throw new SkipException("Rename column is not yet supported by Phoenix connector");
    }

    @Override
    public void testRenameColumnName(String columnName)
    {
        // The column name is rejected when creating a table
        if (columnName.equals("a\"quote")) {
            super.testRenameColumnName(columnName);
            return;
        }
        assertThatThrownBy(() -> super.testRenameColumnName(columnName))
                // TODO (https://github.com/trinodb/trino/issues/7205) support column rename in Phoenix
                .hasMessageContaining("Syntax error. Encountered \"RENAME\"");
        throw new SkipException("Rename column is not yet supported by Phoenix connector");
    }

    @Override
    public void testAddAndDropColumnName(String columnName)
    {
        // TODO: Investigate why these two case fail
        if (columnName.equals("an'apostrophe")) {
            assertThatThrownBy(() -> super.testAddAndDropColumnName(columnName))
                    .hasMessageContaining("Syntax error. Mismatched input");
            throw new SkipException("TODO");
        }
        if (columnName.equals("a\\backslash`")) {
            assertThatThrownBy(() -> super.testAddAndDropColumnName(columnName))
                    .hasMessageContaining("Undefined column");
            throw new SkipException("TODO");
        }
        super.testAddAndDropColumnName(columnName);
    }

    @Override
    public void testInsertArray()
    {
        assertThatThrownBy(super::testInsertArray)
                // TODO (https://github.com/trinodb/trino/issues/6421) array with double null stored as array with 0
                .hasMessage("Phoenix JDBC driver replaced 'null' with '0.0' at index 1 in [0.0]");
    }

    @Override
    public void testCreateSchema()
    {
        throw new SkipException("test disabled until issue fixed"); // TODO https://github.com/trinodb/trino/issues/2348
    }

    @Override
    protected boolean isColumnNameRejected(Exception exception, String columnName, boolean delimited)
    {
        // TODO This should produce a reasonable exception message like "Invalid column name". Then, we should verify the actual exception message
        return columnName.equals("a\"quote");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("time(6)") ||
                typeName.equals("timestamp") ||
                typeName.equals("timestamp(6)") ||
                typeName.equals("timestamp(3) with time zone") ||
                typeName.equals("timestamp(6) with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        if (typeName.equals("time")) {
            // TODO Enable when adding support reading time column
            return Optional.empty();
        }

        if (typeName.equals("date") && dataMappingTestSetup.getSampleValueLiteral().equals("DATE '1582-10-05'")) {
            // Phoenix connector returns +10 days during julian->gregorian switch. The test case exists in TestPhoenixTypeMapping.testDate().
            return Optional.empty();
        }

        return Optional.of(dataMappingTestSetup);
    }

    @Override
    public void testShowCreateTable()
    {
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo("CREATE TABLE phoenix.tpch.orders (\n" +
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
                        "   data_block_encoding = 'FAST_DIFF',\n" +
                        "   rowkeys = 'ROWKEY',\n" +
                        "   salt_buckets = 10\n" +
                        ")");
    }

    @Override
    public void testCharVarcharComparison()
    {
        // test overridden because super uses all-space char values ('  ') that are null-out by Phoenix

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_char_varchar",
                "(k, v) AS VALUES" +
                        "   (-1, CAST(NULL AS char(3))), " +
                        "   (3, CAST('x  ' AS char(3)))")) {
            assertQuery(
                    "SELECT k, v FROM " + table.getName() + " WHERE v = CAST('x ' AS varchar(2))",
                    // The value is included because both sides of the comparison are coerced to char(3)
                    "VALUES (3, 'x  ')");

            assertQuery(
                    "SELECT k, v FROM " + table.getName() + " WHERE v = CAST('x ' AS varchar(4))",
                    // The value is included because both sides of the comparison are coerced to char(4)
                    "VALUES (3, 'x  ')");
        }
    }

    @Override
    public void testVarcharCharComparison()
    {
        // test overridden because Phoenix nulls-out '' varchar value, impacting results

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_varchar_char",
                "(k, v) AS VALUES" +
                        "   (-1, CAST(NULL AS varchar(3))), " +
                        "   (0, CAST('' AS varchar(3)))," + // '' gets replaced with null in Phoenix
                        "   (1, CAST(' ' AS varchar(3))), " +
                        "   (2, CAST('  ' AS varchar(3))), " +
                        "   (3, CAST('   ' AS varchar(3)))," +
                        "   (4, CAST('x' AS varchar(3)))," +
                        "   (5, CAST('x ' AS varchar(3)))," +
                        "   (6, CAST('x  ' AS varchar(3)))")) {
            assertQuery(
                    "SELECT k, v FROM " + table.getName() + " WHERE v = CAST('  ' AS char(2))",
                    // The 3-spaces value is included because both sides of the comparison are coerced to char(3)
                    "VALUES (1, ' '), (2, '  '), (3, '   ')");

            // value that's not all-spaces
            assertQuery(
                    "SELECT k, v FROM " + table.getName() + " WHERE v = CAST('x ' AS char(2))",
                    // The 3-spaces value is included because both sides of the comparison are coerced to char(3)
                    "VALUES (4, 'x'), (5, 'x '), (6, 'x  ')");
        }
    }

    @Override
    public void testCharTrailingSpace()
    {
        assertThatThrownBy(super::testCharTrailingSpace)
                .hasMessageContaining("The table does not have a primary key. tableName=TPCH.CHAR_TRAILING_SPACE");
        throw new SkipException("Implement test for Phoenix");
    }

    // Overridden because Phoenix requires a ROWID column
    @Override
    public void testCountDistinctWithStringTypes()
    {
        assertThatThrownBy(super::testCountDistinctWithStringTypes).hasStackTraceContaining("Illegal data. CHAR types may only contain single byte characters");
        // Skipping the ą test case because it is not supported
        List<String> rows = Streams.mapWithIndex(Stream.of("a", "b", "A", "B", " a ", "a", "b", " b "), (value, idx) -> String.format("%d, '%2$s', '%2$s'", idx, value))
                .collect(toImmutableList());
        String tableName = "count_distinct_strings" + randomNameSuffix();

        try (TestTable testTable = new TestTable(getQueryRunner()::execute, tableName, "(id int, t_char CHAR(5), t_varchar VARCHAR(5)) WITH (ROWKEYS='id')", rows)) {
            assertQuery("SELECT count(DISTINCT t_varchar) FROM " + testTable.getName(), "VALUES 6");
            assertQuery("SELECT count(DISTINCT t_char) FROM " + testTable.getName(), "VALUES 6");
            assertQuery("SELECT count(DISTINCT t_char), count(DISTINCT t_varchar) FROM " + testTable.getName(), "VALUES (6, 6)");
        }
    }

    @Override
    public void testDeleteWithLike()
    {
        assertThatThrownBy(super::testDeleteWithLike)
                .hasStackTraceContaining("TrinoException: " + MODIFYING_ROWS_MESSAGE);
    }

    @Test
    public void testSchemaOperations()
    {
        assertUpdate("CREATE SCHEMA new_schema");
        assertUpdate("CREATE TABLE new_schema.test (x bigint)");

        assertThatThrownBy(() -> getQueryRunner().execute("DROP SCHEMA new_schema"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Cannot drop non-empty schema 'new_schema'");

        assertUpdate("DROP TABLE new_schema.test");
        assertUpdate("DROP SCHEMA new_schema");
    }

    @Test
    public void testMultipleSomeColumnsRangesPredicate()
    {
        assertQuery("SELECT orderkey, shippriority, clerk, totalprice, custkey FROM orders WHERE orderkey BETWEEN 10 AND 50 OR orderkey BETWEEN 100 AND 150");
    }

    @Test
    public void testUnsupportedType()
    {
        onRemoteDatabase().execute("CREATE TABLE tpch.test_timestamp (pk bigint primary key, val1 timestamp)");
        onRemoteDatabase().execute("UPSERT INTO tpch.test_timestamp (pk, val1) VALUES (1, null)");
        onRemoteDatabase().execute("UPSERT INTO tpch.test_timestamp (pk, val1) VALUES (2, '2002-05-30T09:30:10.5')");
        assertUpdate("INSERT INTO test_timestamp VALUES (3)", 1);
        assertQuery("SELECT * FROM test_timestamp", "VALUES 1, 2, 3");
        assertQuery(
                withUnsupportedType(CONVERT_TO_VARCHAR),
                "SELECT * FROM test_timestamp",
                "VALUES " +
                        "(1, null), " +
                        "(2, '2002-05-30 09:30:10.500'), " +
                        "(3, null)");
        assertQueryFails(
                withUnsupportedType(CONVERT_TO_VARCHAR),
                "INSERT INTO test_timestamp VALUES (4, '2002-05-30 09:30:10.500')",
                "Underlying type that is mapped to VARCHAR is not supported for INSERT: TIMESTAMP");
        assertUpdate("DROP TABLE tpch.test_timestamp");
    }

    @Test
    public void testDefaultDecimalTable()
    {
        onRemoteDatabase().execute("CREATE TABLE tpch.test_null_decimal (pk bigint primary key, val1 decimal)");
        onRemoteDatabase().execute("UPSERT INTO tpch.test_null_decimal (pk, val1) VALUES (1, 2)");
        assertQuery("SELECT * FROM tpch.test_null_decimal", "VALUES (1, 2) ");
    }

    private Session withUnsupportedType(UnsupportedTypeHandling unsupportedTypeHandling)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("phoenix", UNSUPPORTED_TYPE_HANDLING, unsupportedTypeHandling.name())
                .build();
    }

    @Test
    public void testCreateTableWithProperties()
    {
        assertUpdate("CREATE TABLE test_create_table_with_properties (created_date date, a bigint, b double, c varchar(10), d varchar(10)) WITH(rowkeys = 'created_date row_timestamp, a,b,c', salt_buckets=10)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_table_with_properties"));
        assertTableColumnNames("test_create_table_with_properties", "created_date", "a", "b", "c", "d");
        assertThat(computeActual("SHOW CREATE TABLE test_create_table_with_properties").getOnlyValue())
                .isEqualTo("CREATE TABLE phoenix.tpch.test_create_table_with_properties (\n" +
                        "   created_date date,\n" +
                        "   a bigint NOT NULL,\n" +
                        "   b double NOT NULL,\n" +
                        "   c varchar(10) NOT NULL,\n" +
                        "   d varchar(10)\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   data_block_encoding = 'FAST_DIFF',\n" +
                        "   rowkeys = 'A,B,C',\n" +
                        "   salt_buckets = 10\n" +
                        ")");

        assertUpdate("DROP TABLE test_create_table_with_properties");
    }

    @Test
    public void testCreateTableWithPresplits()
    {
        assertUpdate("CREATE TABLE test_create_table_with_presplits (rid varchar(10), val1 varchar(10)) with(rowkeys = 'rid', SPLIT_ON='\"1\",\"2\",\"3\"')");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_table_with_presplits"));
        assertTableColumnNames("test_create_table_with_presplits", "rid", "val1");
        assertUpdate("DROP TABLE test_create_table_with_presplits");
    }

    @Test
    public void testSecondaryIndex()
    {
        assertUpdate("CREATE TABLE test_primary_table (pk bigint, val1 double, val2 double, val3 double) with(rowkeys = 'pk')");
        onRemoteDatabase().execute("CREATE LOCAL INDEX test_local_index ON tpch.test_primary_table (val1)");
        onRemoteDatabase().execute("CREATE INDEX test_global_index ON tpch.test_primary_table (val2)");
        assertUpdate("INSERT INTO test_primary_table VALUES (1, 1.1, 1.2, 1.3)", 1);
        assertQuery("SELECT val1,val3 FROM test_primary_table where val1 < 1.2", "SELECT 1.1,1.3");
        assertQuery("SELECT val2,val3 FROM test_primary_table where val2 < 1.3", "SELECT 1.2,1.3");
        assertUpdate("DROP TABLE test_primary_table");
    }

    @Test
    public void testCaseInsensitiveNameMatching()
    {
        onRemoteDatabase().execute("CREATE TABLE tpch.\"TestCaseInsensitive\" (\"pK\" bigint primary key, \"Val1\" double)");
        assertUpdate("INSERT INTO testcaseinsensitive VALUES (1, 1.1)", 1);
        assertQuery("SELECT Val1 FROM testcaseinsensitive where Val1 < 1.2", "SELECT 1.1");
    }

    @Test
    public void testMissingColumnsOnInsert()
    {
        onRemoteDatabase().execute("CREATE TABLE tpch.test_col_insert(pk VARCHAR NOT NULL PRIMARY KEY, col1 VARCHAR, col2 VARCHAR)");
        assertUpdate("INSERT INTO test_col_insert(pk, col1) VALUES('1', 'val1')", 1);
        assertUpdate("INSERT INTO test_col_insert(pk, col2) VALUES('1', 'val2')", 1);
        assertQuery("SELECT * FROM test_col_insert", "SELECT 1, 'val1', 'val2'");
    }

    @Override
    public void testTopNPushdown()
    {
        throw new SkipException("Phoenix does not support topN push down, but instead replaces partial topN with partial Limit.");
    }

    @Test
    public void testReplacePartialTopNWithLimit()
    {
        List<PlanMatchPattern.Ordering> orderBy = ImmutableList.of(sort("orderkey", ASCENDING, LAST));

        assertThat(query("SELECT orderkey FROM orders ORDER BY orderkey LIMIT 10"))
                .matches(output(
                        topN(10, orderBy, FINAL,
                                exchange(LOCAL, GATHER, ImmutableList.of(),
                                        exchange(REMOTE, GATHER, ImmutableList.of(),
                                                limit(
                                                        10,
                                                        ImmutableList.of(),
                                                        true,
                                                        orderBy.stream()
                                                                .map(PlanMatchPattern.Ordering::getField)
                                                                .collect(toImmutableList()),
                                                        tableScan("orders", ImmutableMap.of("orderkey", "orderkey"))))))));

        orderBy = ImmutableList.of(sort("orderkey", ASCENDING, FIRST));

        assertThat(query("SELECT orderkey FROM orders ORDER BY orderkey NULLS FIRST LIMIT 10"))
                .matches(output(
                        topN(10, orderBy, FINAL,
                                exchange(LOCAL, GATHER, ImmutableList.of(),
                                        exchange(REMOTE, GATHER, ImmutableList.of(),
                                                limit(
                                                        10,
                                                        ImmutableList.of(),
                                                        true,
                                                        orderBy.stream()
                                                                .map(PlanMatchPattern.Ordering::getField)
                                                                .collect(toImmutableList()),
                                                        tableScan("orders", ImmutableMap.of("orderkey", "orderkey"))))))));

        orderBy = ImmutableList.of(sort("orderkey", DESCENDING, LAST));

        assertThat(query("SELECT orderkey FROM orders ORDER BY orderkey DESC LIMIT 10"))
                .matches(output(
                        topN(10, orderBy, FINAL,
                                exchange(LOCAL, GATHER, ImmutableList.of(),
                                        exchange(REMOTE, GATHER, ImmutableList.of(),
                                                limit(
                                                        10,
                                                        ImmutableList.of(),
                                                        true,
                                                        orderBy.stream()
                                                                .map(PlanMatchPattern.Ordering::getField)
                                                                .collect(toImmutableList()),
                                                        tableScan("orders", ImmutableMap.of("orderkey", "orderkey"))))))));

        orderBy = ImmutableList.of(sort("orderkey", ASCENDING, LAST), sort("custkey", ASCENDING, LAST));

        assertThat(query("SELECT orderkey FROM orders ORDER BY orderkey, custkey LIMIT 10"))
                .matches(output(
                        project(
                                topN(10, orderBy, FINAL,
                                        exchange(LOCAL, GATHER, ImmutableList.of(),
                                                exchange(REMOTE, GATHER, ImmutableList.of(),
                                                        limit(
                                                                10,
                                                                ImmutableList.of(),
                                                                true,
                                                                orderBy.stream()
                                                                        .map(PlanMatchPattern.Ordering::getField)
                                                                        .collect(toImmutableList()),
                                                                tableScan("orders", ImmutableMap.of("orderkey", "orderkey", "custkey", "custkey")))))))));

        orderBy = ImmutableList.of(sort("orderkey", ASCENDING, LAST), sort("custkey", DESCENDING, LAST));

        assertThat(query("SELECT orderkey FROM orders ORDER BY orderkey, custkey DESC LIMIT 10"))
                .matches(output(
                        project(
                                topN(10, orderBy, FINAL,
                                        exchange(LOCAL, GATHER, ImmutableList.of(),
                                                exchange(REMOTE, GATHER, ImmutableList.of(),
                                                        limit(
                                                                10,
                                                                ImmutableList.of(),
                                                                true,
                                                                orderBy.stream()
                                                                        .map(PlanMatchPattern.Ordering::getField)
                                                                        .collect(toImmutableList()),
                                                                tableScan("orders", ImmutableMap.of("orderkey", "orderkey", "custkey", "custkey")))))))));
    }

    /*
     * Make sure that partial topN is replaced with a partial limit when the input is presorted.
     */
    @Test
    public void testUseSortedPropertiesForPartialTopNElimination()
    {
        String tableName = "test_propagate_table_scan_sorting_properties";
        // salting ensures multiple splits
        String createTableSql = format("" +
                        "CREATE TABLE %s WITH (salt_buckets = 5) AS " +
                        "SELECT * FROM tpch.tiny.customer",
                tableName);
        assertUpdate(createTableSql, 1500L);

        String expected = "SELECT custkey FROM customer ORDER BY 1 NULLS FIRST LIMIT 100";
        String actual = format("SELECT custkey FROM %s ORDER BY 1 NULLS FIRST LIMIT 100", tableName);
        assertQuery(getSession(), actual, expected, assertPartialLimitWithPreSortedInputsCount(getSession(), 1));
        assertUpdate("DROP TABLE " + tableName);
    }

    @Override
    public void testNativeQuerySimple()
    {
        // not implemented
        assertQueryFails("SELECT * FROM TABLE(system.query(query => 'SELECT 1'))", "line 1:21: Table function system.query not registered");
    }

    @Override
    public void testNativeQueryParameters()
    {
        // not implemented
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query_simple", "SELECT * FROM TABLE(system.query(query => ?))")
                .addPreparedStatement("my_query", "SELECT * FROM TABLE(system.query(query => format('SELECT %s FROM %s', ?, ?)))")
                .build();
        assertQueryFails(session, "EXECUTE my_query_simple USING 'SELECT 1 a'", "line 1:21: Table function system.query not registered");
        assertQueryFails(session, "EXECUTE my_query USING 'a', '(SELECT 2 a) t'", "line 1:21: Table function system.query not registered");
    }

    @Override
    public void testNativeQuerySelectFromNation()
    {
        // not implemented
        assertQueryFails(
                format("SELECT * FROM TABLE(system.query(query => 'SELECT name FROM %s.nation WHERE nationkey = 0'))", getSession().getSchema().orElseThrow()),
                "line 1:21: Table function system.query not registered");
    }

    @Override
    public void testNativeQuerySelectFromTestTable()
    {
        // not implemented
        try (TestTable testTable = simpleTable()) {
            assertQueryFails(
                    format("SELECT * FROM TABLE(system.query(query => 'SELECT * FROM %s'))", testTable.getName()),
                    "line 1:21: Table function system.query not registered");
        }
    }

    @Override
    public void testNativeQueryColumnAlias()
    {
        // not implemented
        assertQueryFails(
                "SELECT * FROM TABLE(system.query(query => 'SELECT name AS region_name FROM tpch.region WHERE regionkey = 0'))",
                ".* Table function system.query not registered");
    }

    @Override
    public void testNativeQueryColumnAliasNotFound()
    {
        // not implemented
        assertQueryFails(
                "SELECT name FROM TABLE(system.query(query => 'SELECT name AS region_name FROM tpch.region'))",
                ".* Table function system.query not registered");
    }

    @Override
    public void testNativeQueryCreateStatement()
    {
        // not implemented
        assertFalse(getQueryRunner().tableExists(getSession(), "numbers"));
        assertThatThrownBy(() -> query("SELECT * FROM TABLE(system.query(query => 'CREATE TABLE numbers(n INTEGER)'))"))
                .hasMessage("line 1:21: Table function system.query not registered");
        assertFalse(getQueryRunner().tableExists(getSession(), "numbers"));
    }

    @Override
    public void testNativeQueryInsertStatementTableDoesNotExist()
    {
        // not implemented
        assertFalse(getQueryRunner().tableExists(getSession(), "non_existent_table"));
        assertThatThrownBy(() -> query("SELECT * FROM TABLE(system.query(query => 'INSERT INTO non_existent_table VALUES (1)'))"))
                .hasMessage("line 1:21: Table function system.query not registered");
    }

    @Override
    public void testNativeQueryInsertStatementTableExists()
    {
        // not implemented
        try (TestTable testTable = simpleTable()) {
            assertThatThrownBy(() -> query(format("SELECT * FROM TABLE(system.query(query => 'INSERT INTO %s VALUES (3)'))", testTable.getName())))
                    .hasMessage("line 1:21: Table function system.query not registered");
            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .matches("VALUES BIGINT '1', BIGINT '2'");
        }
    }

    @Override
    public void testNativeQueryIncorrectSyntax()
    {
        // not implemented
        assertThatThrownBy(() -> query("SELECT * FROM TABLE(system.query(query => 'some wrong syntax'))"))
                .hasMessage("line 1:21: Table function system.query not registered");
    }

    @Override
    protected TestTable simpleTable()
    {
        // override because Phoenix requires primary key specification
        return new PhoenixTestTable(onRemoteDatabase(), "tpch.simple_table", "(col BIGINT PRIMARY KEY)", ImmutableList.of("1", "2"));
    }

    @Override
    protected TestTable createTableWithDoubleAndRealColumns(String name, List<String> rows)
    {
        return new TestTable(onRemoteDatabase(), name, "(t_double double primary key, u_double double, v_real float, w_real float)", rows);
    }

    @Override
    protected void verifyConcurrentAddColumnFailurePermissible(Exception e)
    {
        assertThat(e)
                .hasMessageContaining("Concurrent modification to table");
    }

    @Override
    public void testCreateSchemaWithLongName()
    {
        // TODO: Find the maximum table schema length in Phoenix and enable this test.
        throw new SkipException("TODO");
    }

    @Override
    public void testCreateTableWithLongTableName()
    {
        // TODO: Find the maximum table name length in Phoenix and enable this test.
        // Table name length with 65536 chars throws "startRow's length must be less than or equal to 32767 to meet the criteria for a row key."
        // 32767 chars still causes the same error and shorter names (e.g. 10000) causes timeout.
        throw new SkipException("TODO");
    }

    @Override
    public void testCreateTableWithLongColumnName()
    {
        // TODO: Find the maximum column name length in Phoenix and enable this test.
        throw new SkipException("TODO");
    }

    @Override
    public void testAlterTableAddLongColumnName()
    {
        // TODO: Find the maximum column name length in Phoenix and enable this test.
        throw new SkipException("TODO");
    }

    @Test
    public void testLargeDefaultDomainCompactionThreshold()
    {
        String catalogName = getSession().getCatalog().orElseThrow();
        String propertyName = catalogName + "." + DOMAIN_COMPACTION_THRESHOLD;
        assertQuery(
                "SHOW SESSION LIKE '" + propertyName + "'",
                "VALUES('" + propertyName + "','5000', '5000', 'integer', 'Maximum ranges to allow in a tuple domain without simplifying it')");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(32767);
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return sql -> {
            try {
                try (Connection connection = DriverManager.getConnection(testingPhoenixServer.getJdbcUrl());
                        Statement statement = connection.createStatement()) {
                    statement.execute(sql);
                    connection.commit();
                }
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
    }
}
