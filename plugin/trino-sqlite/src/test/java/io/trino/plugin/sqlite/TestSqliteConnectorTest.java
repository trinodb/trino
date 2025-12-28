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
package io.trino.plugin.sqlite;

import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TestView;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.List;
import java.util.Optional;

import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_AGGREGATION_PUSHDOWN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_NATIVE_QUERY;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_TOPN_PUSHDOWN;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

@Isolated
final class TestSqliteConnectorTest
        extends BaseJdbcConnectorTest
{
    private TestingSqliteServer sqliteServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        sqliteServer = closeAfterClass(new TestingSqliteServer());
        return SqliteQueryRunner.builder(sqliteServer)
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_ADD_COLUMN,
                 SUPPORTS_ARRAY,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE,
                 SUPPORTS_DELETE,
                 SUPPORTS_INSERT,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_NEGATIVE_DATE, // min date is 0001-01-01
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_AGGREGATION_PUSHDOWN,
                 SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN,
                 SUPPORTS_UPDATE,
                 SUPPORTS_MERGE -> false;

            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected TestTable newTrinoTable(String namePrefix, @Language("SQL") String tableDefinition, List<String> rowsToInsert)
    {
        // Use Sqlite executor because the connector does not support creating tables
        return new TestTable(sqliteServer.getSqlExecutor(), namePrefix, tableDefinition, rowsToInsert);
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup setup)
    {
        String type = setup.getTrinoTypeName();
        if (type.equals("time") || type.startsWith("time(")) {
            // Sqlite does not have a TIME type
            return Optional.of(setup.asUnsupported());
        }
        if (type.startsWith("array") || type.startsWith("row")) {
            // Sqlite does not support composite types
            return Optional.of(setup.asUnsupported());
        }
        if (setup.getTrinoTypeName().equals("char(3)") && setup.getSampleValueLiteral().equals("'ab'")) {
            // Sqlite fills char(3) with spaces, causing test to fail
            return Optional.of(new DataMappingTestSetup("char(3)", "'abc'", "'zzz'"));
        }
        return Optional.of(setup);
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
                        "col_nonnull_default decimal(20,0) DEFAULT 42 NOT NULL," +
                        "col_required2 decimal(20,0) NOT NULL)");
    }

    @Override
    protected TestTable createTableWithUnsupportedColumn()
    {
        return new TestTable(
                onRemoteDatabase(),
                "test_unsupported_col",
                "(one BIGINT, two GEOMETRY, three VARCHAR(10))");
    }

    @Test
    @Override // For Sqlite column orderdate is varchar(10)
    public void testTopNPushdown()
    {
        if (!hasBehavior(SUPPORTS_TOPN_PUSHDOWN)) {
            assertThat(query("SELECT orderkey FROM orders ORDER BY orderkey LIMIT 10"))
                    .ordered()
                    .isNotFullyPushedDown(TopNNode.class);
            return;
        }

        assertThat(query("SELECT orderkey FROM orders ORDER BY orderkey LIMIT 10"))
                .ordered()
                .isFullyPushedDown();

        assertThat(query("SELECT orderkey FROM orders ORDER BY orderkey DESC LIMIT 10"))
                .ordered()
                .isFullyPushedDown();

        // multiple sort columns with different orders
        assertThat(query("SELECT * FROM orders ORDER BY shippriority DESC, totalprice ASC LIMIT 10"))
                .ordered()
                .isFullyPushedDown();

        // TopN over aggregation column
        if (hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN)) {
            assertThat(query("SELECT sum(totalprice) AS total FROM orders GROUP BY custkey ORDER BY total DESC LIMIT 10"))
                    .ordered()
                    .isFullyPushedDown();
        }

        // TopN over TopN
        assertThat(query("SELECT orderkey, totalprice FROM (SELECT orderkey, totalprice FROM orders ORDER BY 1, 2 LIMIT 10) ORDER BY 2, 1 LIMIT 5"))
                .ordered()
                .isFullyPushedDown();

        assertThat(query("" +
                "SELECT orderkey, totalprice " +
                "FROM (SELECT orderkey, totalprice FROM (SELECT orderkey, totalprice FROM orders ORDER BY 1, 2 LIMIT 10) " +
                "ORDER BY 2, 1 LIMIT 5) ORDER BY 1, 2 LIMIT 3"))
                .ordered()
                .isFullyPushedDown();

        // TopN over limit - use high limit for deterministic result
        assertThat(query("SELECT orderkey, totalprice FROM (SELECT orderkey, totalprice FROM orders LIMIT 15000) ORDER BY totalprice ASC LIMIT 5"))
                .ordered()
                .isFullyPushedDown();

        // TopN over limit with filter
        assertThat(query("" +
                "SELECT orderkey, totalprice " +
                "FROM (SELECT orderkey, totalprice FROM orders WHERE orderdate = DATE '1995-09-16' LIMIT 20) " +
                "ORDER BY totalprice ASC LIMIT 5"))
                .ordered()
                .isFullyPushedDown();

        // TopN over aggregation with filter
        if (hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN)) {
            assertThat(query("" +
                    "SELECT * " +
                    "FROM (SELECT SUM(totalprice) as sum, custkey AS total FROM orders GROUP BY custkey HAVING COUNT(*) > 3) " +
                    "ORDER BY sum DESC LIMIT 10"))
                    .ordered()
                    .isFullyPushedDown();
        }
    }

    @Test
    @Override // For Sqlite column orderkey and custkey are integer, orderdate is varchar(10).
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
                           shippriority integer,
                           comment varchar(79)
                        )\
                        """,
                        catalog,
                        schema));
    }

    @Test
    @Override
    public void testInsertNegativeDate()
    {
        abort("Sqlite connector does not map 'orderdate' column to date but varchar(10)");
    }

    @Test
    @Override
    public void testCharTrailingSpace()
    {
        String schema = getSession().getSchema().orElseThrow();
        try (TestTable table = new TestTable(onRemoteDatabase(), schema + ".char_trailing_space", "(x char(10))", List.of("'test'"))) {
            String tableName = table.getName();
            assertQuery("SELECT * FROM " + tableName + " WHERE x = char 'test'", "VALUES 'test'");
            assertQuery("SELECT * FROM " + tableName + " WHERE x = char 'test  '", "VALUES 'test'");
            assertQuery("SELECT * FROM " + tableName + " WHERE x = char 'test        '", "VALUES 'test'");
            assertQueryReturnsEmptyResult("SELECT * FROM " + tableName + " WHERE x = char ' test'");
        }
    }

    @Test
    void testViews()
    {
        try (TestView view = new TestView(onRemoteDatabase(), "test_view", "SELECT 'O' as status")) {
            assertQuery("SELECT status FROM " + view.getName(), "SELECT 'O'");
        }
    }

    @Test
    @Override // Override because Sqlite allows SELECT query in execute procedure
    public void testExecuteProcedureWithInvalidQuery()
    {
        assertUpdate("CALL system.execute('SELECT 1')");
        assertQueryFails("CALL system.execute('invalid')", "(?s)Failed to execute query.*");
    }

    @Test
    void testPredicatePushdownForNumerics()
    {
        predicatePushdownTest("DECIMAL(9, 3)", "123.321", "<=", "124");
        predicatePushdownTest("DECIMAL(9, 3)", "123.321", "<=", "123.321");
        predicatePushdownTest("DECIMAL(9, 3)", "123.321", "=", "123.321");
        predicatePushdownTest("DECIMAL(30, 10)", "123456789.987654321", "<=", "123456790");
        predicatePushdownTest("DECIMAL(30, 10)", "123456789.987654321", "<=", "123456789.987654321");
        predicatePushdownTest("DECIMAL(30, 10)", "123456789.654321", "=", "123456789.654321"); // max precision for Sqlite
        predicatePushdownTest("FLOAT", "123456789.987654321", "<=", "CAST(123456789.99 AS REAL)");
        predicatePushdownTest("DOUBLE", "123456789.987654321", "<=", "CAST(123456789.99 AS DOUBLE)");
        predicatePushdownTest("NUMERIC(5,3)", "5.0", "=", "CAST(5.0 AS DECIMAL(5,3))");
    }

    @Test
    void testPredicatePushdownForChars()
    {
        predicatePushdownTest("CHAR(1)", "'0'", "=", "'0'");
        predicatePushdownTest("CHAR(1)", "'0'", "<=", "'0'");
        predicatePushdownTest("CHAR(7)", "'my_char'", "=", "CAST('my_char' AS CHAR(7))");
    }

    private void predicatePushdownTest(String columnType, String columnLiteral, String operator, String filterLiteral)
    {
        String tableName = "test_pdown_" + columnType.replaceAll("[^a-zA-Z0-9]", "");
        try (TestTable table = new TestTable(onRemoteDatabase(), tableName, format("(c %s)", columnType))) {
            onRemoteDatabase().execute(format("INSERT INTO %s VALUES (%s)", table.getName(), columnLiteral));

            assertThat(query(format("SELECT * FROM %s WHERE c %s %s", table.getName(), operator, filterLiteral)))
                    .isFullyPushedDown();
        }
    }

    @Test
    void testNativeMultipleInClauses()
    {
        String longInClauses = range(0, 10)
                .mapToObj(value -> getLongInClause(value * 1_000, 1_000))
                .collect(joining(" OR "));
        onRemoteDatabase().execute(format("SELECT count(*) FROM orders WHERE %s", longInClauses));
    }

    private static String getLongInClause(int start, int length)
    {
        String longValues = range(start, start + length)
                .mapToObj(Integer::toString)
                .collect(joining(", "));
        return "orderkey IN (" + longValues + ")";
    }

    @Test
    @Override // Override because Sqlite does not yet support INSERT
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
    @Override // Override because Sqlite does not yet support INSERT
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

    @Test
    @Override // Override because the expected error message is different
    public void testNativeQueryInsertStatementTableExists()
    {
        skipTestUnless(hasBehavior(SUPPORTS_NATIVE_QUERY));
        try (TestTable testTable = simpleTable()) {
            assertThat(query("SELECT * FROM TABLE(system.query(query => 'INSERT INTO %s VALUES (3)'))".formatted(testTable.getName())))
                    .failure().hasMessageContaining("Failed to get table handle for prepared query. column 1 out of bounds [1,0]");
            assertQuery("SELECT * FROM " + testTable.getName(), "VALUES 1, 2");
        }
    }

    @Test
    @Override // Override because Sqlite does not yet support INSERT
    public void testInsertInPresenceOfNotSupportedColumn()
    {
        assertThatThrownBy(super::testInsertInPresenceOfNotSupportedColumn)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("This connector does not support inserts");
    }

    @Test
    @Override // Override because Sqlite does not yet support INSERT
    public void testNativeQuerySelectUnsupportedType()
    {
        try (TestTable testTable = createTableWithUnsupportedColumn()) {
            String unqualifiedTableName = testTable.getName().replaceAll("^\\w+\\.", "");
            // Check that column 'two' is not supported.
            assertQuery("SELECT column_name FROM information_schema.columns WHERE table_name = '" + unqualifiedTableName + "'", "VALUES 'one', 'three'");
        }
    }

    @Test
    @Override // Override because the expected error message is different
    public void testNativeQueryCreateStatement()
    {
        skipTestUnless(hasBehavior(SUPPORTS_NATIVE_QUERY));
        assertThat(getQueryRunner().tableExists(getSession(), "numbers")).isFalse();
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'CREATE TABLE numbers(n INTEGER)'))"))
                .failure().hasMessageContaining("Failed to get table handle for prepared query. column 1 out of bounds [1,0]");
        assertThat(getQueryRunner().tableExists(getSession(), "numbers")).isFalse();
    }

    @Override // Override because Sqlite does not support multi rows insertion...
    protected SqlExecutor onRemoteDatabase()
    {
        return sqliteServer.getSqlExecutor();
    }
}
