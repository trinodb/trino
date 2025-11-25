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
package io.trino.plugin.firebird;

import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TestView;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.List;
import java.util.Optional;

import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_NATIVE_QUERY;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Isolated
final class TestFirebirdConnectorTest
        extends BaseJdbcConnectorTest
{
    private TestingFirebirdServer firebirdServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        firebirdServer = closeAfterClass(new TestingFirebirdServer());
        return FirebirdQueryRunner.builder(firebirdServer)
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
                 SUPPORTS_MERGE,
                 SUPPORTS_SCHEMA -> false;

            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected TestTable newTrinoTable(String namePrefix, @Language("SQL") String tableDefinition, List<String> rowsToInsert)
    {
        // Use Firebird executor because the connector does not support creating tables
        return new TestTable(firebirdServer.getSqlExecutor(), namePrefix, tableDefinition, rowsToInsert);
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup setup)
    {
        String type = setup.getTrinoTypeName();
        if (type.equals("time") || type.startsWith("time(")) {
            // Firebird does not have a TIME type
            return Optional.of(setup.asUnsupported());
        }
        if (type.startsWith("array") || type.startsWith("row")) {
            // Firebird does not support composite types
            return Optional.of(setup.asUnsupported());
        }
        if (setup.getTrinoTypeName().equals("char(3)") && setup.getSampleValueLiteral().equals("'ab'")) {
            // Firebird fills char(3) with spaces, causing test to fail
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
    void testViews()
    {
        // Firebird requires the FROM clause on the RDB$DATABASE
        try (TestView view = new TestView(onRemoteDatabase(), "test_view", "SELECT 'O' as status FROM RDB$DATABASE")) {
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
    @Override // Override because Firebird requires named columns and FROM clause in SELECT
    public void testNativeQuerySimple()
    {
        assertQuery("SELECT * FROM TABLE(system.query(query => 'SELECT 1 res FROM RDB$DATABASE'))", "VALUES 1");
    }

    @Test
    @Override // Override test because for Firebird the predicate of '-1996-09-14' match '1997-09-14'
    public void testDateYearOfEraPredicate()
    {
        assertQuery("SELECT orderdate FROM orders WHERE orderdate = DATE '1997-09-14'", "VALUES DATE '1997-09-14'");
        assertQuery("SELECT orderdate FROM orders WHERE orderdate = DATE '-1996-09-14'", "VALUES DATE '1997-09-14'");
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
    @Override // Override because Firebird does not support DROP IF EXIST
    public void testExecuteProcedure()
    {
        String tableName = "test_execute" + randomNameSuffix();
        String schemaTableName = getSchemaTableName(tableName);

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
            if (getQueryRunner().tableExists(getSession(), tableName)) {
                assertUpdate("CALL system.execute('DROP TABLE " + schemaTableName + "')");
            }
        }
    }

    @Test
    @Override // Override because Firebird does not support DROP IF EXIST
    public void testExecuteProcedureWithNamedArgument()
    {
        String tableName = "test_execute" + randomNameSuffix();
        String schemaTableName = getSchemaTableName(tableName);

        assertUpdate("CALL system.execute('CREATE TABLE " + schemaTableName + "(a int)')");
        try {
            assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();
            assertUpdate("CALL system.execute(query => 'DROP TABLE " + schemaTableName + "')");
            assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        }
        finally {
            if (getQueryRunner().tableExists(getSession(), tableName)) {
                assertUpdate("CALL system.execute('DROP TABLE" + schemaTableName + "')");
            }
        }
    }

    @Override // Override because Firebird does not support multi rows insertion...

    protected SqlExecutor onRemoteDatabase()
    {
        return firebirdServer.getSqlExecutor();
    }

    @Test
    @Override
    public void testNativeQueryInsertStatementTableExists()
    {
        // Firebird returns a ResultSet metadata with no columns for INSERT statement.
        // This is unusual, because other connectors don't produce a ResultSet metadata for INSERT at all.
        // The query fails because there are no columns, but even if columns were not required, the query would fail
        // to execute in Firebird because the connector wraps it in additional syntax, which causes syntax error.
        try (TestTable testTable = simpleTable()) {
            assertThat(query(format("SELECT * FROM TABLE(system.query(query => 'INSERT INTO %s VALUES (3)'))", testTable.getName())))
                    .nonTrinoExceptionFailure().hasMessageContaining("descriptor has no fields");
            assertQuery("SELECT * FROM " + testTable.getName(), "VALUES 1, 2");
        }
    }

    @Test
    @Override // Override because Firebird requires the FROM clause on the RDB$DATABASE
    public void testNativeQueryParameters()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query_simple", "SELECT * FROM TABLE(system.query(query => ?))")
                .addPreparedStatement("my_query", "SELECT * FROM TABLE(system.query(query => format('SELECT %s FROM %s', ?, ?)))")
                .build();
        assertQuery(session, "EXECUTE my_query_simple USING 'SELECT 1 a FROM RDB$DATABASE'", "VALUES 1");
        assertQuery(session, "EXECUTE my_query USING 'a', '(SELECT 2 a FROM RDB$DATABASE) t'", "VALUES 2");
    }

    @Test
    @Override // Override because Firebird returns a ResultSet metadata with no columns for CREATE statement.
    public void testNativeQueryCreateStatement()
    {
        skipTestUnless(hasBehavior(SUPPORTS_NATIVE_QUERY));
        assertThat(getQueryRunner().tableExists(getSession(), "numbers")).isFalse();
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'CREATE TABLE numbers(n INTEGER)'))"))
                .nonTrinoExceptionFailure().hasMessageContaining("descriptor has no fields");
        assertThat(getQueryRunner().tableExists(getSession(), "numbers")).isFalse();
    }

    @Test
    @Override
    @Disabled // I can't get this test to work because I get a JDBC driver error immediately when trying to create the table.
    public void testInsertInPresenceOfNotSupportedColumn()
    {
        assertThatThrownBy(super::testInsertInPresenceOfNotSupportedColumn)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("This connector does not support inserts");
    }

    @Test
    @Override // Override test because INSERT is not supported
    @Disabled // I can't get this test to work because I get a JDBC driver error immediately when trying to create the table.
    public void testNativeQuerySelectUnsupportedType()
    {
        try (TestTable testTable = createTableWithUnsupportedColumn()) {
            String unqualifiedTableName = testTable.getName().replaceAll("^\\w+\\.", "");
            // Check that column 'two' is not supported.
            assertQuery("SELECT column_name FROM information_schema.columns WHERE table_name = '" + unqualifiedTableName + "'", "VALUES 'one', 'three'");
        }
    }
}
