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
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.DOMAIN_COMPACTION_THRESHOLD;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.UNSUPPORTED_TYPE_HANDLING;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.plugin.phoenix5.PhoenixQueryRunner.createPhoenixQueryRunner;
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
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_MERGE,
                    SUPPORTS_PREDICATE_ARITHMETIC_EXPRESSION_PUSHDOWN,
                    SUPPORTS_UPDATE -> true;
            case SUPPORTS_ADD_COLUMN_WITH_COMMENT,
                    SUPPORTS_AGGREGATION_PUSHDOWN,
                    SUPPORTS_COMMENT_ON_COLUMN,
                    SUPPORTS_COMMENT_ON_TABLE,
                    SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                    SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT,
                    SUPPORTS_DROP_SCHEMA_CASCADE,
                    SUPPORTS_LIMIT_PUSHDOWN,
                    SUPPORTS_NATIVE_QUERY,
                    SUPPORTS_NOT_NULL_CONSTRAINT,
                    SUPPORTS_RENAME_SCHEMA,
                    SUPPORTS_RENAME_TABLE,
                    SUPPORTS_ROW_TYPE,
                    SUPPORTS_SET_COLUMN_TYPE,
                    SUPPORTS_TOPN_PUSHDOWN,
                    SUPPORTS_TRUNCATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    // TODO: wait https://github.com/trinodb/trino/pull/14939 done and then remove this test
    @Override
    public void testArithmeticPredicatePushdown()
    {
        super.testArithmeticPredicatePushdown();
        // negate
        assertThat(query("SELECT nationkey, name, regionkey FROM nation WHERE -(nationkey) = -7"))
                .isFullyPushedDown();
        assertThat(query("SELECT nationkey, name, regionkey FROM nation WHERE -(nationkey) = 0"))
                .isFullyPushedDown();

        // additive identity
        assertThat(query("SELECT nationkey, name, regionkey FROM nation WHERE nationkey + 0 = nationkey"))
                .isFullyPushedDown();
        assertThat(query("SELECT nationkey, name, regionkey FROM nation WHERE nationkey - 0 = nationkey"))
                .isFullyPushedDown();
        // additive inverse
        assertThat(query("SELECT nationkey, name, regionkey FROM nation WHERE nationkey + (-nationkey) = 0"))
                .isFullyPushedDown();
        assertThat(query("SELECT nationkey, name, regionkey FROM nation WHERE nationkey - nationkey = 0"))
                .isFullyPushedDown();

        // addition and subtraction of constant
        assertThat(query("SELECT nationkey, name, regionkey FROM nation WHERE nationkey + 1 = 7"))
                .isFullyPushedDown();
        assertThat(query("SELECT nationkey, name, regionkey FROM nation WHERE nationkey - 1 = 7"))
                .isFullyPushedDown();

        // addition and subtraction of columns
        assertThat(query("SELECT nationkey, name, regionkey FROM nation WHERE nationkey + regionkey = 26"))
                .isFullyPushedDown();
        assertThat(query("SELECT nationkey, name, regionkey FROM nation WHERE nationkey - regionkey = 23"))
                .isFullyPushedDown();
        // addition and subtraction of columns ANDed with another predicate
        assertThat(query("SELECT nationkey, name, regionkey FROM nation WHERE regionkey = 0 AND nationkey + regionkey = 14"))
                .isFullyPushedDown();
        assertThat(query("SELECT nationkey, name, regionkey FROM nation WHERE regionkey = 0 AND nationkey - regionkey = 16"))
                .isFullyPushedDown();

        // multiplication/division/modulo by zero
        assertThat(query("SELECT nationkey, name, regionkey FROM nation WHERE nationkey * 0 != 0"))
                .isFullyPushedDown();
        assertThatThrownBy(() -> query("SELECT nationkey, name, regionkey FROM nation WHERE nationkey / 0 = 0"))
                .satisfies(this::verifyDivisionByZeroFailure);
        assertThatThrownBy(() -> query("SELECT nationkey, name, regionkey FROM nation WHERE nationkey % 0 = 0"))
                .satisfies(this::verifyDivisionByZeroFailure);
        // Expression that evaluates to 0 for some rows on RHS of modulus
        assertThatThrownBy(() -> query("SELECT nationkey, name, regionkey FROM nation WHERE nationkey > 0 AND (nationkey - regionkey) / (regionkey - 1) = 2"))
                .satisfies(this::verifyDivisionByZeroFailure);
        assertThatThrownBy(() -> query("SELECT nationkey, name, regionkey FROM nation WHERE nationkey > 0 AND (nationkey - regionkey) % (regionkey - 1) = 2"))
                .satisfies(this::verifyDivisionByZeroFailure);

        // multiplicative/divisive identity
        assertThat(query("SELECT nationkey, name, regionkey FROM nation WHERE nationkey * 1 = nationkey"))
                .isFullyPushedDown();
        assertThat(query("SELECT nationkey, name, regionkey FROM nation WHERE nationkey / 1 = nationkey"))
                .isFullyPushedDown();
        // multiplicative/divisive inverse
        assertThat(query("SELECT nationkey, name, regionkey FROM nation WHERE nationkey > 0 AND nationkey / nationkey = 1"))
                .isFullyPushedDown();

        // multiplication/division/modulus with a constant
        assertThat(query("SELECT nationkey, name, regionkey FROM nation WHERE nationkey * 2 = 40"))
                .isFullyPushedDown();
        assertThat(query("SELECT nationkey, name, regionkey FROM nation WHERE nationkey / 2 = 12"))
                .isFullyPushedDown();
        assertThat(query("SELECT nationkey, name, regionkey FROM nation WHERE nationkey % 20 = 7"))
                .isFullyPushedDown();

        // multiplication/division/modulus of columns
        assertThat(query("SELECT nationkey, name, regionkey FROM nation WHERE nationkey * regionkey = 40"))
                .isFullyPushedDown();
        assertThat(query("SELECT orderkey, custkey FROM orders WHERE orderkey / custkey = 243"))
                .isFullyPushedDown();
        assertThat(query("SELECT orderkey, custkey FROM orders WHERE orderkey % custkey = 1470"))
                .isFullyPushedDown();
        // multiplication/division/modulus of columns ANDed with another predicate
        assertThat(query("SELECT nationkey, name, regionkey FROM nation WHERE regionkey = 2 AND nationkey * regionkey = 16"))
                .isFullyPushedDown();
        assertThat(query("SELECT orderkey, custkey FROM orders WHERE custkey = 223 AND orderkey / custkey = 243"))
                .isFullyPushedDown();
        assertThat(query("SELECT orderkey, custkey FROM orders WHERE custkey = 1483 AND orderkey % custkey = 1470"))
                .isFullyPushedDown();

        // some databases calculate remainder instead of modulus when one of the values is negative
        assertThat(query("SELECT nationkey, name, regionkey FROM nation WHERE nationkey > 0 AND (nationkey - regionkey) % -nationkey = 2"))
                .isFullyPushedDown();
    }

    private void verifyDivisionByZeroFailure(Throwable e)
    {
        assertThat(e.getCause().getCause()).hasMessageContainingAll("by zero");
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
                        "   bloomfilter = 'ROW',\n" +
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
        String schema = getSession().getSchema().orElseThrow();
        try (TestTable table = new PhoenixTestTable(onRemoteDatabase(), schema + ".char_trailing_space", "(x char(10) primary key)", List.of("'test'"))) {
            String tableName = table.getName();
            assertQuery("SELECT * FROM " + tableName + " WHERE x = char 'test'", "VALUES 'test      '");
            assertQuery("SELECT * FROM " + tableName + " WHERE x = char 'test  '", "VALUES 'test      '");
            assertQuery("SELECT * FROM " + tableName + " WHERE x = char 'test        '", "VALUES 'test      '");
            assertQueryReturnsEmptyResult("SELECT * FROM " + tableName + " WHERE x = char ' test'");
        }
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
    public void testMergeLarge()
    {
        String tableName = "test_merge_" + randomNameSuffix();

        assertUpdate(createTableForWrites(format("CREATE TABLE %s (orderkey BIGINT, custkey BIGINT, totalprice DOUBLE)", tableName)));

        assertUpdate(
                format("INSERT INTO %s SELECT orderkey, custkey, totalprice FROM tpch.sf1.orders", tableName),
                (long) computeScalar("SELECT count(*) FROM tpch.sf1.orders"));

        @Language("SQL") String mergeSql = "" +
                "MERGE INTO " + tableName + " t USING (SELECT * FROM tpch.sf1.orders) s ON (t.orderkey = s.orderkey)\n" +
                "WHEN MATCHED AND mod(s.orderkey, 3) = 0 THEN UPDATE SET totalprice = t.totalprice + s.totalprice\n" +
                "WHEN MATCHED AND mod(s.orderkey, 3) = 1 THEN DELETE";

        assertUpdate(mergeSql, 1_000_000);

        // verify deleted rows
        assertQuery("SELECT count(*) FROM " + tableName + " WHERE mod(orderkey, 3) = 1", "SELECT 0");

        // verify untouched rows
        assertThat(query("SELECT count(*), cast(sum(totalprice) AS decimal(18,2)) FROM " + tableName + " WHERE mod(orderkey, 3) = 2"))
                .matches("SELECT count(*), cast(sum(totalprice) AS decimal(18,2)) FROM tpch.sf1.orders WHERE mod(orderkey, 3) = 2");

        // TODO investigate why sum(DOUBLE) not correct
        // verify updated rows
        String sql = format("SELECT count(*) FROM %s t JOIN tpch.sf1.orders s ON t.orderkey = s.orderkey WHERE mod(t.orderkey, 3) = 0 AND t.totalprice != s.totalprice * 2", tableName);
        assertQuery(sql, "SELECT 0");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testMergeWithSpecifiedRowkeys()
    {
        testMergeWithSpecifiedRowkeys("customer");
        testMergeWithSpecifiedRowkeys("customer_copy");
        testMergeWithSpecifiedRowkeys("customer,customer_copy");
    }

    // This method is mainly copied from BaseConnectorTest#testMergeMultipleOperations, and appended a 'customer_copy' column which is the copy of the 'customer' column for
    // testing merge with specifying rowkeys explicitly in Phoenix
    private void testMergeWithSpecifiedRowkeys(String rowkeyDefinition)
    {
        int targetCustomerCount = 32;
        String targetTable = "merge_multiple_rowkeys_specified_" + randomNameSuffix();
        // check the upper case table name also works
        targetTable = targetTable.toUpperCase(ENGLISH);
        assertUpdate(createTableForWrites(format("CREATE TABLE %s (customer VARCHAR, purchases INT, zipcode INT, spouse VARCHAR, address VARCHAR, customer_copy VARCHAR) WITH (rowkeys = '%s')", targetTable, rowkeyDefinition)));

        String originalInsertFirstHalf = IntStream.range(1, targetCustomerCount / 2)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jan_%s', '%s Poe Ct', 'joe_%s')", intValue, 1000, 91000, intValue, intValue, intValue))
                .collect(Collectors.joining(", "));
        String originalInsertSecondHalf = IntStream.range(targetCustomerCount / 2, targetCustomerCount)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jan_%s', '%s Poe Ct', 'joe_%s')", intValue, 2000, 92000, intValue, intValue, intValue))
                .collect(Collectors.joining(", "));

        assertUpdate(format("INSERT INTO %s (customer, purchases, zipcode, spouse, address, customer_copy) VALUES %s, %s", targetTable, originalInsertFirstHalf, originalInsertSecondHalf), targetCustomerCount - 1);

        String firstMergeSource = IntStream.range(targetCustomerCount / 2, targetCustomerCount)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jill_%s', '%s Eop Ct', 'joe_%s')", intValue, 3000, 83000, intValue, intValue, intValue))
                .collect(Collectors.joining(", "));

        assertUpdate(format("MERGE INTO %s t USING (VALUES %s) AS s(customer, purchases, zipcode, spouse, address, customer_copy)", targetTable, firstMergeSource) +
                        "    ON t.customer = s.customer" +
                        "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases, zipcode = s.zipcode, spouse = s.spouse, address = s.address",
                targetCustomerCount / 2);

        assertQuery(
                "SELECT customer, purchases, zipcode, spouse, address, customer_copy FROM " + targetTable,
                format("VALUES %s, %s", originalInsertFirstHalf, firstMergeSource));

        String nextInsert = IntStream.range(targetCustomerCount, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('jack_%s', %s, %s, 'jan_%s', '%s Poe Ct', 'jack_%s')", intValue, 4000, 74000, intValue, intValue, intValue))
                .collect(Collectors.joining(", "));

        assertUpdate(format("INSERT INTO %s (customer, purchases, zipcode, spouse, address, customer_copy) VALUES %s", targetTable, nextInsert), targetCustomerCount / 2);

        String secondMergeSource = IntStream.range(1, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jen_%s', '%s Poe Ct', 'joe_%s')", intValue, 5000, 85000, intValue, intValue, intValue))
                .collect(Collectors.joining(", "));

        assertUpdate(format("MERGE INTO %s t USING (VALUES %s) AS s(customer, purchases, zipcode, spouse, address, customer_copy)", targetTable, secondMergeSource) +
                        "    ON t.customer = s.customer" +
                        "    WHEN MATCHED AND t.zipcode = 91000 THEN DELETE" +
                        "    WHEN MATCHED AND s.zipcode = 85000 THEN UPDATE SET zipcode = 60000" +
                        "    WHEN MATCHED THEN UPDATE SET zipcode = s.zipcode, spouse = s.spouse, address = s.address" +
                        "    WHEN NOT MATCHED THEN INSERT (customer, purchases, zipcode, spouse, address, customer_copy) VALUES(s.customer, s.purchases, s.zipcode, s.spouse, s.address, s.customer_copy)",
                targetCustomerCount * 3 / 2 - 1);

        String updatedBeginning = IntStream.range(targetCustomerCount / 2, targetCustomerCount)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jill_%s', '%s Eop Ct', 'joe_%s')", intValue, 3000, 60000, intValue, intValue, intValue))
                .collect(Collectors.joining(", "));
        String updatedMiddle = IntStream.range(targetCustomerCount, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jen_%s', '%s Poe Ct', 'joe_%s')", intValue, 5000, 85000, intValue, intValue, intValue))
                .collect(Collectors.joining(", "));
        String updatedEnd = IntStream.range(targetCustomerCount, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('jack_%s', %s, %s, 'jan_%s', '%s Poe Ct', 'jack_%s')", intValue, 4000, 74000, intValue, intValue, intValue))
                .collect(Collectors.joining(", "));

        assertQuery(
                "SELECT customer, purchases, zipcode, spouse, address, customer_copy FROM " + targetTable,
                format("VALUES %s, %s, %s", updatedBeginning, updatedMiddle, updatedEnd));

        assertUpdate("DROP TABLE " + targetTable);
    }

    @Override
    public void testUpdateRowConcurrently()
    {
        throw new SkipException("Phoenix doesn't support concurrent update of different columns in a row");
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
                        "   bloomfilter = 'ROW',\n" +
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
    protected TestTable simpleTable()
    {
        // override because Phoenix requires primary key specification
        return new PhoenixTestTable(onRemoteDatabase(), "tpch.simple_table", "(col BIGINT PRIMARY KEY)", ImmutableList.of("1", "2"));
    }

    @Override
    protected TestTable createTableWithDoubleAndRealColumns(String name, List<String> rows)
    {
        return new PhoenixTestTable(onRemoteDatabase(), name, "(t_double double primary key, u_double double, v_real float, w_real float)", rows);
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
