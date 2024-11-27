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
package io.trino.plugin.oracle;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TestView;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.plugin.oracle.TestingOracleServer.TEST_USER;
import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseOracleConnectorTest
        extends BaseJdbcConnectorTest
{
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_JOIN_PUSHDOWN -> true;
            case SUPPORTS_ADD_COLUMN_WITH_COMMENT,
                 SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION,
                 SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION,
                 SUPPORTS_ARRAY,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                 SUPPORTS_DROP_NOT_NULL_CONSTRAINT,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_TOPN_PUSHDOWN -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected String dataMappingTableName(String trinoTypeName)
    {
        return "tmp_trino_" + System.nanoTime();
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("date")) {
            // Oracle TO_DATE function returns +10 days during julian and gregorian calendar switch
            if (dataMappingTestSetup.getSampleValueLiteral().equals("DATE '1582-10-05'")) {
                return Optional.empty();
            }
        }
        if (typeName.equals("time") ||
                typeName.equals("time(6)") ||
                typeName.equals("timestamp(6) with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }
        if (typeName.equals("boolean")) {
            // Oracle does not have native support for boolean however usually it is represented as number(1)
            return Optional.empty();
        }

        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                onRemoteDatabase(),
                "test_default_cols",
                "(col_required decimal(20,0) NOT NULL," +
                        "col_nullable decimal(20,0)," +
                        "col_default decimal(20,0) DEFAULT 43," +
                        "col_nonnull_default decimal(20,0) DEFAULT 42 NOT NULL ," +
                        "col_required2 decimal(20,0) NOT NULL)");
    }

    @Override
    protected TestTable createTableWithUnsupportedColumn()
    {
        return new TestTable(
                onRemoteDatabase(),
                "test_unsupported_col",
                "(one NUMBER(19), two NUMBER, three VARCHAR2(10 CHAR))");
    }

    @Test
    @Override
    public void testShowColumns()
    {
        assertThat(query("SHOW COLUMNS FROM orders")).result().matches(getDescribeOrdersResult());
    }

    @Test
    @Override
    public void testInformationSchemaFiltering()
    {
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'orders' LIMIT 1",
                "SELECT 'orders' table_name");
        assertQuery(
                "SELECT table_name FROM information_schema.columns WHERE data_type = 'decimal(19,0)' AND table_name = 'customer' AND column_name = 'custkey' LIMIT 1",
                "SELECT 'customer' table_name");
    }

    @Override
    protected boolean isColumnNameRejected(Exception exception, String columnName, boolean delimited)
    {
        if (columnName.equals("a\"quote") && exception.getMessage().contains("ORA-03001: unimplemented feature")) {
            return true;
        }

        return false;
    }

    @Override
    protected MaterializedResult getDescribeOrdersResult()
    {
        return resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "decimal(19,0)", "", "")
                .row("custkey", "decimal(19,0)", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "timestamp(0)", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "decimal(10,0)", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                // If the connector reports additional column properties, the expected value needs to be adjusted in the test subclass
                .matches("CREATE TABLE \\w+\\.\\w+\\.orders \\Q(\n" +
                        "   orderkey decimal(19, 0),\n" +
                        "   custkey decimal(19, 0),\n" +
                        "   orderstatus varchar(1),\n" +
                        "   totalprice double,\n" +
                        "   orderdate timestamp(0),\n" +
                        "   orderpriority varchar(15),\n" +
                        "   clerk varchar(15),\n" +
                        "   shippriority decimal(10, 0),\n" +
                        "   comment varchar(79)\n" +
                        ")");
    }

    @Test
    public void testTimestampOutOfPrecisionRounded()
    {
        String tableName = "test_timestamp_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (t timestamp(12))");

        assertThat(getColumnType(tableName, "t"))
                .isEqualTo("timestamp(9)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    @Override
    public void testCharVarcharComparison()
    {
        // test overridden because super uses all-space char values ('  ') that are null-out by Oracle

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

    @Test
    @Override
    public void testVarcharCharComparison()
    {
        // test overridden because Oracle nulls-out '' varchar value, impacting results

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_varchar_char",
                "(k, v) AS VALUES" +
                        "   (-1, CAST(NULL AS varchar(3))), " +
                        "   (0, CAST('' AS varchar(3)))," + // '' gets replaced with null in Oracle
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

    @Test
    @Override
    public void testAggregationWithUnsupportedResultType()
    {
        // Overridden because for approx_set(bigint) a ProjectNode is present above table scan because Oracle doesn't support bigint
        // array_agg returns array, which is not supported
        assertThat(query("SELECT array_agg(nationkey) FROM nation"))
                .skipResultsCorrectnessCheckForPushdown() // array_agg doesn't have a deterministic order of elements in result array
                .isNotFullyPushedDown(AggregationNode.class);
        // histogram returns map, which is not supported
        assertThat(query("SELECT histogram(regionkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);
        // multimap_agg returns multimap, which is not supported
        assertThat(query("SELECT multimap_agg(regionkey, nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);
        // approx_set returns HyperLogLog, which is not supported
        assertThat(query("SELECT approx_set(nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class, ProjectNode.class);
    }

    @Override
    protected TestTable createAggregationTestTable(String name, List<String> rows)
    {
        return new TestTable(onRemoteDatabase(), name, "(short_decimal number(9, 3), long_decimal number(30, 10), a_bigint number(19), t_double binary_double)", rows);
    }

    @Test
    @Override
    public void testDeleteWithLike()
    {
        assertThatThrownBy(super::testDeleteWithLike)
                .hasStackTraceContaining("TrinoException: " + MODIFYING_ROWS_MESSAGE);
    }

    @Test
    public void testViews()
    {
        try (TestView view = new TestView(onRemoteDatabase(), getUser() + ".test_view", "SELECT 'O' as status FROM dual")) {
            assertQuery("SELECT status FROM " + view.getName(), "SELECT 'O'");
        }
    }

    @Test
    public void testSynonyms()
    {
        try (TestSynonym synonym = new TestSynonym(onRemoteDatabase(), getUser() + ".test_synonym", "FOR ORDERS")) {
            assertQueryFails("SELECT orderkey FROM " + synonym.getName(), "line 1:22: Table 'oracle.*' does not exist");
        }
    }

    @Test
    public void testPredicatePushdown()
    {
        // varchar equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'ROMANIA'"))
                .matches("VALUES (CAST(3 AS DECIMAL(19,0)), CAST(19 AS DECIMAL(19,0)), CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // varchar range
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name BETWEEN 'POLAND' AND 'RPA'"))
                .matches("VALUES (CAST(3 AS DECIMAL(19,0)), CAST(19 AS DECIMAL(19,0)), CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // varchar different case
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'romania'"))
                .returnsEmptyResult()
                .isFullyPushedDown();

        // date equality
        assertThat(query("SELECT orderkey FROM orders WHERE orderdate = DATE '1992-09-29'"))
                .matches("VALUES CAST(1250 AS DECIMAL(19,0)), 34406, 38436, 57570")
                .isFullyPushedDown();

        // predicate over aggregation key (likely to be optimized before being pushed down into the connector)
        assertThat(query("SELECT * FROM (SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey) WHERE regionkey = 3"))
                .matches("VALUES (CAST(3 AS decimal(19,0)), CAST(77 AS decimal(38,0)))")
                .isFullyPushedDown();

        // predicate over aggregation result
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey HAVING sum(nationkey) = 77"))
                .matches("VALUES (CAST(3 AS decimal(19,0)), CAST(77 AS decimal(38,0)))")
                .isFullyPushedDown();
    }

    @Test
    public void testPredicatePushdownForNumerics()
    {
        predicatePushdownTest("DECIMAL(9, 3)", "123.321", "<=", "124");
        predicatePushdownTest("DECIMAL(9, 3)", "123.321", "<=", "123.321");
        predicatePushdownTest("DECIMAL(9, 3)", "123.321", "=", "123.321");
        predicatePushdownTest("DECIMAL(30, 10)", "123456789.987654321", "<=", "123456790");
        predicatePushdownTest("DECIMAL(30, 10)", "123456789.987654321", "<=", "123456789.987654321");
        predicatePushdownTest("DECIMAL(30, 10)", "123456789.987654321", "=", "123456789.987654321");
        predicatePushdownTest("FLOAT(63)", "123456789.987654321", "<=", "CAST(123456789.99 AS REAL)");
        predicatePushdownTest("FLOAT(63)", "123456789.987654321", "<=", "CAST(123456789.99 AS DOUBLE)");
        predicatePushdownTest("FLOAT(126)", "123456789.987654321", "<=", "CAST(123456789.99 AS REAL)");
        predicatePushdownTest("FLOAT(126)", "123456789.987654321", "<=", "CAST(123456789.99 AS DOUBLE)");
        predicatePushdownTest("BINARY_FLOAT", "5.0f", "=", "CAST(5.0 AS REAL)");
        predicatePushdownTest("BINARY_DOUBLE", "20.233", "=", "CAST(20.233 AS DOUBLE)");
        predicatePushdownTest("NUMBER(5,3)", "5.0", "=", "CAST(5.0 AS DECIMAL(5,3))");
    }

    @Test
    public void testPredicatePushdownForChars()
    {
        predicatePushdownTest("CHAR(1)", "'0'", "=", "'0'");
        predicatePushdownTest("CHAR(1)", "'0'", "<=", "'0'");
        predicatePushdownTest("CHAR(5)", "'0'", "=", "CHAR'0'");
        predicatePushdownTest("CHAR(7)", "'my_char'", "=", "CAST('my_char' AS CHAR(7))");
        predicatePushdownTest("NCHAR(7)", "'my_char'", "=", "CAST('my_char' AS CHAR(7))");
        predicatePushdownTest("VARCHAR2(7)", "'my_char'", "=", "CAST('my_char' AS VARCHAR(7))");
        predicatePushdownTest("NVARCHAR2(7)", "'my_char'", "=", "CAST('my_char' AS VARCHAR(7))");

        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                getUser() + ".test_pdown_",
                "(c_clob CLOB, c_nclob NCLOB)",
                ImmutableList.of("'my_clob', 'my_nclob'"))) {
            assertThat(query(format("SELECT c_clob FROM %s WHERE c_clob = VARCHAR 'my_clob'", table.getName()))).isNotFullyPushedDown(FilterNode.class);
            assertThat(query(format("SELECT c_nclob FROM %s WHERE c_nclob = VARCHAR 'my_nclob'", table.getName()))).isNotFullyPushedDown(FilterNode.class);
        }
    }

    @Test
    public void testTooLargeDomainCompactionThreshold()
    {
        assertQueryFails(
                Session.builder(getSession())
                        .setCatalogSessionProperty("oracle", "domain_compaction_threshold", "10000")
                        .build(),
                "SELECT * from nation", "Domain compaction threshold \\(10000\\) cannot exceed 1000");
    }

    @Test
    @Override
    public void testNativeQuerySimple()
    {
        // override because Oracle requires the FROM clause, and it needs explicit type
        assertQuery("SELECT * FROM TABLE(system.query(query => 'SELECT CAST(1 AS number(2, 1)) FROM DUAL'))", "VALUES 1");
    }

    @Test
    @Override
    public void testNativeQueryParameters()
    {
        // override because Oracle requires the FROM clause, and it needs explicit type
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query_simple", "SELECT * FROM TABLE(system.query(query => ?))")
                .addPreparedStatement("my_query", "SELECT * FROM TABLE(system.query(query => format('SELECT %s FROM %s', ?, ?)))")
                .build();
        assertQuery(session, "EXECUTE my_query_simple USING 'SELECT CAST(1 AS number(2, 1)) a FROM DUAL'", "VALUES 1");
        assertQuery(session, "EXECUTE my_query USING 'a', '(SELECT CAST(2 AS number(2, 1)) a FROM DUAL) t'", "VALUES 2");
    }

    @Test
    @Override
    public void testNativeQueryInsertStatementTableDoesNotExist()
    {
        // override because Oracle succeeds in preparing query, and then fails because of no metadata available
        assertThat(getQueryRunner().tableExists(getSession(), "non_existent_table")).isFalse();
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'INSERT INTO non_existent_table VALUES (1)'))"))
                .failure().hasMessageContaining("Query not supported: ResultSetMetaData not available for query: INSERT INTO non_existent_table VALUES (1)");
    }

    @Test
    @Override
    public void testNativeQueryIncorrectSyntax()
    {
        // override because Oracle succeeds in preparing query, and then fails because of no metadata available
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'some wrong syntax'))"))
                .failure().hasMessageContaining("Query not supported: ResultSetMetaData not available for query: some wrong syntax");
    }

    @Override
    protected TestTable simpleTable()
    {
        // override because Oracle does not support type bigint
        return new TestTable(onRemoteDatabase(), format("%s.simple_table", getSession().getSchema().orElseThrow()), "(col decimal(2, 1))", ImmutableList.of("1", "2"));
    }

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return format("ORA-01400: cannot insert NULL into \\(.*\"%s\"\\)\n\nhttps://docs.oracle.com/error-help/db/ora-01400/", columnName.toUpperCase(ENGLISH));
    }

    @Override
    protected void verifyAddNotNullColumnToNonEmptyTableFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("ORA-01758: table must be empty to add mandatory (NOT NULL) column");
    }

    @Override
    protected void verifyConcurrentAddColumnFailurePermissible(Exception e)
    {
        assertThat(e)
                .hasMessageContaining("ORA-14411: The DDL cannot be run concurrently with other DDLs");
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.of(30);
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("ORA-00972: identifier is too long");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(30);
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("ORA-00972: identifier is too long");
    }

    @Override
    protected OptionalInt maxColumnNameLength()
    {
        return OptionalInt.of(30);
    }

    @Override
    protected void verifyColumnNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("ORA-00972: identifier is too long");
    }

    @Override
    protected String sumDistinctAggregationPushdownExpectedResult()
    {
        return "VALUES (BIGINT '4', DECIMAL '8')";
    }

    private void predicatePushdownTest(String oracleType, String oracleLiteral, String operator, String filterLiteral)
    {
        String tableName = ("test_pdown_" + oracleType.replaceAll("[^a-zA-Z0-9]", ""))
                .replaceFirst("^(.{18}).*", "$1__");
        try (TestTable table = new TestTable(onRemoteDatabase(), getUser() + "." + tableName, format("(c %s)", oracleType))) {
            onRemoteDatabase().execute(format("INSERT INTO %s VALUES (%s)", table.getName(), oracleLiteral));

            assertThat(query(format("SELECT * FROM %s WHERE c %s %s", table.getName(), operator, filterLiteral)))
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testJoinPushdownWithImplicitCast()
    {
        try (TestTable leftTable = new TestTable(getQueryRunner()::execute, "left_table", "(id int, varchar_50 varchar(50))", ImmutableList.of("(1, 'India')", "(2, 'Poland')"));
                TestTable rightTable = new TestTable(getQueryRunner()::execute, "right_table_", "(varchar_100 varchar(100), varchar_unbounded varchar)", ImmutableList.of("('India', 'Japan')", "('France', 'Poland')"))) {
            String leftTableName = leftTable.getName();
            String rightTableName = rightTable.getName();
            Session session = joinPushdownEnabled(getSession());

            // Implicit cast between bounded varchar
            String joinWithBoundedVarchar = "SELECT id FROM %s l %s %s r ON l.varchar_50 = r.varchar_100".formatted(leftTableName, "%s", rightTableName);
            assertThat(query(session, joinWithBoundedVarchar.formatted("LEFT JOIN")))
                    .isFullyPushedDown();
            assertThat(query(session, joinWithBoundedVarchar.formatted("RIGHT JOIN")))
                    .isFullyPushedDown();
            assertThat(query(session, joinWithBoundedVarchar.formatted("INNER JOIN")))
                    .isFullyPushedDown();
            assertThat(query(session, joinWithBoundedVarchar.formatted("FULL JOIN")))
                    .isFullyPushedDown();

            // Implicit cast between bounded and unbounded varchar
            String joinWithUnboundedVarchar = "SELECT id FROM %s l %s %s r ON l.varchar_50 = r.varchar_unbounded".formatted(leftTableName, "%s", rightTableName);
            assertThat(query(session, joinWithUnboundedVarchar.formatted("LEFT JOIN")))
                    .joinIsNotFullyPushedDown();
            assertThat(query(session, joinWithUnboundedVarchar.formatted("RIGHT JOIN")))
                    .joinIsNotFullyPushedDown();
            assertThat(query(session, joinWithUnboundedVarchar.formatted("INNER JOIN")))
                    .joinIsNotFullyPushedDown();
            assertThat(query(session, joinWithUnboundedVarchar.formatted("FULL JOIN")))
                    .joinIsNotFullyPushedDown();
        }
    }

    protected String getUser()
    {
        return TEST_USER;
    }
}
