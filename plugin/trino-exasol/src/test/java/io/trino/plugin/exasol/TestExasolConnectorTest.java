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
package io.trino.plugin.exasol;

import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TestView;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.exasol.TestingExasolServer.TEST_SCHEMA;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Isolated
final class TestExasolConnectorTest
        extends BaseJdbcConnectorTest
{
    private TestingExasolServer exasolServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        exasolServer = closeAfterClass(new TestingExasolServer());
        return ExasolQueryRunner.builder(exasolServer)
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            // Tests requires write access which is not implemented
            case SUPPORTS_AGGREGATION_PUSHDOWN,
                 SUPPORTS_JOIN_PUSHDOWN,
                 SUPPORTS_LIMIT_PUSHDOWN,
                 SUPPORTS_TOPN_PUSHDOWN -> false;

            // Parallel writing is not supported due to restrictions of the Exasol JDBC driver.
            case SUPPORTS_ADD_COLUMN,
                 SUPPORTS_ARRAY,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE,
                 SUPPORTS_DELETE,
                 SUPPORTS_INSERT,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_NEGATIVE_DATE, // min date is 0001-01-01
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_UPDATE -> false;

            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup setup)
    {
        String type = setup.getTrinoTypeName();
        if (type.equals("time") || type.startsWith("time(")) {
            // Exasol does not have a TIME type
            return Optional.of(setup.asUnsupported());
        }
        if (type.startsWith("array") || type.startsWith("row")) {
            // Exasol does not support composite types
            return Optional.of(setup.asUnsupported());
        }
        if (setup.getTrinoTypeName().equals("char(3)") && setup.getSampleValueLiteral().equals("'ab'")) {
            // Exasol fills char(3) with spaces, causing test to fail
            return Optional.of(new DataMappingTestSetup("char(3)", "'abc'", "'zzz'"));
        }
        return Optional.of(setup);
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                onRemoteDatabase(),
                TestingExasolServer.TEST_SCHEMA + ".test_default_cols",
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
                TEST_SCHEMA + ".test_unsupported_col",
                "(one NUMBER(19), two GEOMETRY, three VARCHAR(10 CHAR))");
    }

    @Test
    @Override
    public void testShowColumns()
    {
        // Test expects
        // - bigint instead of decimal(19,0) for columns orderkey, custkey
        // - varchar instead of varchar(n) for columns orderstatus, orderpriority and comment
        // - double instead of decimal(10,2) for column totalprice
        // - integer instead of decimal(10,0) for column shippriority
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");
        MaterializedResult expected = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "decimal(19,0)", "", "")
                .row("custkey", "decimal(19,0)", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "decimal(10,2)", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "decimal(10,0)", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    @Override
    public void testDescribeTable()
    {
        // Exasol reports bigint columns as decimal(19,0)
        MaterializedResult expectedColumns = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "decimal(19,0)", "", "")
                .row("custkey", "decimal(19,0)", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "decimal(10,2)", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "decimal(10,0)", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        assertThat(actualColumns).isEqualTo(expectedColumns);
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        // Exasol reports bigint columns as decimal(19, 0) and integer as decimal(10, 0)
        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        assertThat(computeScalar("SHOW CREATE TABLE orders"))
                // If the connector reports additional column properties, the expected value needs to be adjusted in the test subclass
                .isEqualTo(format(
                        """
                        CREATE TABLE %s.%s.orders (
                           orderkey decimal(19, 0),
                           custkey decimal(19, 0),
                           orderstatus varchar(1),
                           totalprice decimal(10, 2),
                           orderdate date,
                           orderpriority varchar(15),
                           clerk varchar(15),
                           shippriority decimal(10, 0),
                           comment varchar(79)
                        )\
                        """,
                        catalog,
                        schema));
    }

    @Test
    @Override
    public void testAggregationWithUnsupportedResultType()
    {
        assertThat(query("SELECT array_agg(nationkey) FROM nation"))
                .skipResultsCorrectnessCheckForPushdown() // array_agg doesn't have a deterministic order of elements in result array
                .isNotFullyPushedDown(AggregationNode.class);
        assertThat(query("SELECT histogram(regionkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);
        assertThat(query("SELECT multimap_agg(regionkey, nationkey) FROM nation"))
                .skipResultsCorrectnessCheckForPushdown() // multimap_agg doesn't have a deterministic order of values for a key
                .isNotFullyPushedDown(AggregationNode.class);
        // Overridden because for approx_set(bigint) a ProjectNode is present above table scan because Exasol doesn't support bigint
        assertThat(query("SELECT approx_set(nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class, ProjectNode.class);
    }

    @Test
    void testViews()
    {
        try (TestView view = new TestView(onRemoteDatabase(), TEST_SCHEMA + ".test_view", "SELECT 'O' as status")) {
            assertQuery("SELECT status FROM " + view.getName(), "SELECT 'O'");
        }
    }

    @Test
    void testPredicatePushdownForNumerics()
    {
        predicatePushdownTest("DECIMAL(9, 3)", "123.321", "<=", "124");
        predicatePushdownTest("DECIMAL(9, 3)", "123.321", "<=", "123.321");
        predicatePushdownTest("DECIMAL(9, 3)", "123.321", "=", "123.321");
        predicatePushdownTest("DECIMAL(30, 10)", "123456789.987654321", "<=", "123456790");
        predicatePushdownTest("DECIMAL(30, 10)", "123456789.987654321", "<=", "123456789.987654321");
        predicatePushdownTest("DECIMAL(30, 10)", "123456789.987654321", "=", "123456789.987654321");
        predicatePushdownTest("FLOAT", "123456789.987654321", "<=", "CAST(123456789.99 AS REAL)");
        predicatePushdownTest("DOUBLE PRECISION", "123456789.987654321", "<=", "CAST(123456789.99 AS DOUBLE)");
        predicatePushdownTest("NUMBER(5,3)", "5.0", "=", "CAST(5.0 AS DECIMAL(5,3))");
    }

    @Test
    void testPredicatePushdownForChars()
    {
        predicatePushdownTest("CHAR(1)", "'0'", "=", "'0'");
        predicatePushdownTest("CHAR(1)", "'0'", "<=", "'0'");
        predicatePushdownTest("CHAR(7)", "'my_char'", "=", "CAST('my_char' AS CHAR(7))");
    }

    private void predicatePushdownTest(String exasolType, String exasolLiteral, String operator, String filterLiteral)
    {
        String tableName = "test_pdown_" + exasolType.replaceAll("[^a-zA-Z0-9]", "");
        try (TestTable table = new TestTable(onRemoteDatabase(), TEST_SCHEMA + "." + tableName, format("(c %s)", exasolType))) {
            onRemoteDatabase().execute(format("INSERT INTO %s VALUES (%s)", table.getName(), exasolLiteral));

            assertThat(query(format("SELECT * FROM %s WHERE c %s %s", table.getName(), operator, filterLiteral)))
                    .isFullyPushedDown();
        }
    }

    @Test
    @Override // Override because Exasol requires named columns
    public void testNativeQuerySimple()
    {
        assertQuery("SELECT * FROM TABLE(system.query(query => 'SELECT 1 AS res'))", "VALUES 1");
    }

    @Test
    @Override // Override to specify test schema
    public void testNativeQueryCreateStatement()
    {
        assertThat(getQueryRunner().tableExists(getSession(), "numbers")).isFalse();
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'CREATE TABLE " + TEST_SCHEMA + ".numbers(n INTEGER)'))"))
                .failure().hasMessageContaining("Query not supported: ResultSetMetaData not available for query: CREATE TABLE ");
        assertThat(getQueryRunner().tableExists(getSession(), "numbers")).isFalse();
    }

    @Test
    @Override
    public void testDateYearOfEraPredicate()
    {
        // Override because Exasol does not support negative dates
        assertQuery("SELECT orderdate FROM orders WHERE orderdate = DATE '1997-09-14'", "VALUES DATE '1997-09-14'");
    }

    @Test
    @Override
    public void testInformationSchemaFiltering()
    {
        // Exasol reports bigint columns as decimal(19,0)
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'orders' LIMIT 1",
                "SELECT 'orders' table_name");
        assertQuery(
                "SELECT table_name FROM information_schema.columns WHERE data_type = 'decimal(19,0)' AND table_name = 'customer' AND column_name = 'custkey' LIMIT 1",
                "SELECT 'customer' table_name");
    }

    @Test
    @Override
    public void testCharTrailingSpace()
    {
        // Exasol pads CHAR(n) with spaces
        String schema = getSession().getSchema().orElseThrow();
        try (TestTable table = new TestTable(onRemoteDatabase(), schema + ".char_trailing_space", "(x char(10))", List.of("'test'"))) {
            String tableName = table.getName();
            assertQueryReturnsEmptyResult("SELECT * FROM " + tableName + " WHERE x = char 'test'");
            assertQueryReturnsEmptyResult("SELECT * FROM " + tableName + " WHERE x = char 'test  '");
            assertQueryReturnsEmptyResult("SELECT * FROM " + tableName + " WHERE x = char 'test      '");
            assertQueryReturnsEmptyResult("SELECT * FROM " + tableName + " WHERE x = char 'test        '");
            assertQueryReturnsEmptyResult("SELECT * FROM " + tableName + " WHERE x = char ' test'");
        }
    }

    @Test
    @Override // Override test because INSERT is not supported
    public void testNativeQuerySelectUnsupportedType()
    {
        try (TestTable testTable = createTableWithUnsupportedColumn()) {
            String unqualifiedTableName = testTable.getName().replaceAll("^\\w+\\.", "");
            // Check that column 'two' is not supported.
            assertQuery("SELECT column_name FROM information_schema.columns WHERE table_name = '" + unqualifiedTableName + "'", "VALUES 'one', 'three'");
        }
    }

    @Test
    void testNativeMultipleInClauses()
    {
        String longInClauses = range(0, 10)
                .mapToObj(value -> getLongInClause(value * 1_000, 1_000))
                .collect(joining(" OR "));
        onRemoteDatabase().execute(format("SELECT count(*) FROM %s.orders WHERE %s", TEST_SCHEMA, longInClauses));
    }

    private static String getLongInClause(int start, int length)
    {
        String longValues = range(start, start + length)
                .mapToObj(Integer::toString)
                .collect(joining(", "));
        return "orderkey IN (" + longValues + ")";
    }

    @Test
    @Override
    public void testInsertInPresenceOfNotSupportedColumn()
    {
        assertThatThrownBy(super::testInsertInPresenceOfNotSupportedColumn)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("This connector does not support inserts");
    }

    @Test
    @Override
    public void testExecuteProcedure()
    {
        String tableName = "test_execute" + randomNameSuffix();
        String schemaTableName = getSession().getSchema().orElseThrow() + "." + tableName;

        assertUpdate("CALL system.execute('CREATE TABLE " + schemaTableName + "(a int)')");
        try {
            assertUpdate("CALL system.execute('INSERT INTO " + schemaTableName + " VALUES (1)')");
            assertQuery("SELECT * FROM " + schemaTableName, "VALUES 1");

            assertUpdate("CALL system.execute('UPDATE " + schemaTableName + " SET a = 2')");
            assertQuery("SELECT * FROM " + schemaTableName, "VALUES 2");

            assertUpdate("CALL system.execute('DELETE FROM " + schemaTableName + "')");
            assertQueryReturnsEmptyResult("SELECT * FROM " + schemaTableName);

            assertUpdate("CALL system.execute('DROP TABLE " + schemaTableName + "')");
            assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        }
        finally {
            assertUpdate("CALL system.execute('DROP TABLE IF EXISTS " + schemaTableName + "')");
        }
    }

    @Test
    @Override
    public void testExecuteProcedureWithNamedArgument()
    {
        String tableName = "test_execute" + randomNameSuffix();
        String schemaTableName = getSession().getSchema().orElseThrow() + "." + tableName;

        assertUpdate("CALL system.execute('CREATE TABLE " + schemaTableName + "(a int)')");
        try {
            assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();
            assertUpdate("CALL system.execute(query => 'DROP TABLE " + schemaTableName + "')");
            assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        }
        finally {
            assertUpdate("CALL system.execute('DROP TABLE IF EXISTS " + schemaTableName + "')");
        }
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return exasolServer::execute;
    }
}
