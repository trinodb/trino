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

import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TestView;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.jdbc.JdbcWriteSessionProperties.NON_TRANSACTIONAL_MERGE;
import static io.trino.plugin.jdbc.JdbcWriteSessionProperties.WRITE_BATCH_SIZE;
import static io.trino.plugin.jdbc.JdbcWriteSessionProperties.WRITE_PARALLELISM;
import static io.trino.plugin.sqlite.SqliteTableProperties.PRIMARY_KEY_PROPERTY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.QueryAssertions.assertContains;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_VIEW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_NATIVE_QUERY;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
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
    protected Session getSession()
    {
        Session session = super.getSession();
        return Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), NON_TRANSACTIONAL_MERGE, "true")
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), WRITE_BATCH_SIZE, "500")
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), WRITE_PARALLELISM, "1")
                .build();
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return sqliteServer::execute;
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_VIEW,
                 SUPPORTS_DEFAULT_COLUMN_VALUE -> true;

            case SUPPORTS_ADD_COLUMN_WITH_COMMENT,
                 SUPPORTS_ADD_COLUMN_WITH_POSITION,
                 SUPPORTS_AGGREGATION_PUSHDOWN,
                 SUPPORTS_ARRAY,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                 SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT,
                 SUPPORTS_DROP_DEFAULT_COLUMN_VALUE,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN,
                 SUPPORTS_REFRESH_VIEW,
                 SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_SET_DEFAULT_COLUMN_VALUE,
                 SUPPORTS_TOPN_PUSHDOWN -> false;

            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected String canonicalize(String value)
    {
        return value.toLowerCase(ENGLISH);
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        return switch (dataMappingTestSetup.getTrinoTypeName()) {
            case "time",
                 "time(6)",
                 "timestamp",
                 "timestamp(6)",
                 "timestamp(3) with time zone",
                 "timestamp(6) with time zone" -> Optional.of(dataMappingTestSetup.asUnsupported());
            default -> Optional.of(dataMappingTestSetup);
        };
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                onRemoteDatabase(),
                "main.test_default_cols",
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
                "test_unsupported_col",
                "(one BIGINT, two GEOMETRY, three VARCHAR(10))");
    }

    @Test
    @Override // Override because SQLite map char to varchar
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
    @Override // Override because SQLite allows SELECT query in execute procedure
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
        predicatePushdownTest("DECIMAL(30, 10)", "123456789.654321", "<=", "123456789.654321");
        predicatePushdownTest("DECIMAL(30, 10)", "123456789.654321", "=", "123456789.654321"); // max precision for SQLite
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
    @Override // Override because the expected error message is different
    public void testNativeQueryCreateStatement()
    {
        skipTestUnless(hasBehavior(SUPPORTS_NATIVE_QUERY));
        assertThat(getQueryRunner().tableExists(getSession(), "numbers")).isFalse();
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'CREATE TABLE numbers(n INTEGER)'))"))
                .failure().hasMessageContaining("Failed to get table handle for prepared query. column 1 out of bounds [1,0]");
        assertThat(getQueryRunner().tableExists(getSession(), "numbers")).isFalse();
    }

    @Override
    protected void verifyAddNotNullColumnToNonEmptyTableFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("[SQLITE_ERROR] SQL error or missing database (Cannot add a NOT NULL column with default value NULL)");
    }

    @Override
    protected TestTable createTestTableForWrites(String namePrefix, String tableDefinition, String primaryKey)
    {
        return super.createTestTableForWrites(namePrefix, getTableDefinitionWithPrimaryKey(tableDefinition, primaryKey), primaryKey);
    }

    @Override
    protected TestTable createTestTableForWrites(String namePrefix, String tableDefinition, List<String> rowsToInsert, String primaryKey)
    {
        return super.createTestTableForWrites(namePrefix, getTableDefinitionWithPrimaryKey(tableDefinition, primaryKey), rowsToInsert, primaryKey);
    }

    @Override
    protected void createTableForWrites(String createTable, String tableName, Optional<String> primaryKey, OptionalInt updateCount)
    {
        super.createTableForWrites(createTableWithPrimaryKey(createTable, primaryKey), tableName, primaryKey, updateCount);
    }

    private String createTableWithPrimaryKey(String createTable, Optional<String> primaryKey)
    {
        return primaryKey.map(key -> getTableDefinitionWithPrimaryKey(createTable, key)).orElse(createTable);
    }

    private String getTableDefinitionWithPrimaryKey(String createTable, String primaryKeys)
    {
        String[] keys = primaryKeys.split(",\\s*");
        String constraint = format("WITH (%s = ARRAY['%s'])", PRIMARY_KEY_PROPERTY, String.join("','", keys));
        String tableDefinition;
        if (createTable.contains("AS")) {
            tableDefinition = createTable.replaceFirst("AS", constraint + " AS");
        }
        else {
            tableDefinition = createTable + " " + constraint;
        }
        return tableDefinition;
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(42768);
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("[SQLITE_ERROR] SQL error or missing database (LIKE or GLOB pattern too complex)");
    }

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return format("Failed to insert data: \\[SQLITE_CONSTRAINT_NOTNULL] A NOT NULL constraint failed \\(NOT NULL constraint failed: .*\\.%s\\)", columnName);
    }

    @Test
    @Override
    public void testCharVarcharComparison()
    {
        abort("SQLite map char to varchar, skip test");
    }

    @Test
    @Override
    public void testCreateTableWithLongTableName()
    {
        abort("Long names cause SQLite timeouts");
    }

    @Test
    @Override
    public void testRenameTableToLongTableName()
    {
        abort("Long names cause SQLite timeouts");
    }

    @Test
    @Override
    public void testMergeLarge()
    {
        abort("MergeLarge cause SQLite timeouts");
    }

    @Test
    @Override // Overridden because the expected message is different. I think the NOT NULL constraint is not fully propagated and does not trigger the appropriate Trino exception.
    public void testInsertDefaultNullIntoNotNullColumn()
    {
        assertThatThrownBy(super::testInsertDefaultNullIntoNotNullColumn)
                .hasStackTraceContaining("Failed to insert data: [SQLITE_CONSTRAINT_NOTNULL] A NOT NULL constraint failed (NOT NULL constraint failed: ");
    }

    @Test
    @Override // Overridden because testView() create view with comment
    public void testView()
    {
        assertThatThrownBy(super::testView)
                .hasStackTraceContaining("This connector does not support creating views with comment");
    }

    @Test
    @Override // Overridden because this test does not trigger the expected exception.
    public void testSetDefaultColumn()
    {
        assertThatThrownBy(super::testSetDefaultColumn)
                .hasStackTraceContaining("This connector does not support setting default values");
    }

    @Test
    @Override // Overridden because this test does not trigger the expected exception.
    public void testDropDefaultColumn()
    {
        assertThatThrownBy(super::testDropDefaultColumn)
                .hasStackTraceContaining("This connector does not support dropping default values");
    }

    @Test
    @Override // Override because Sqlite does not support row expression
    public void testShowCreateView()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_VIEW));
        checkState(getSession().getCatalog().isPresent(), "catalog is not set");
        checkState(getSession().getSchema().isPresent(), "schema is not set");

        String viewName = "test_show_create_view" + randomNameSuffix();
        assertUpdate("DROP VIEW IF EXISTS " + viewName);
        String ddl = format(
                """
                CREATE VIEW %s.%s.%s SECURITY INVOKER AS
                SELECT
                  nation.nationkey
                , nation.name
                , nation.regionkey
                , nation.comment
                FROM
                  nation\
                """,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                viewName);
        assertUpdate(ddl);

        assertThat(computeScalar("SHOW CREATE VIEW " + viewName)).isEqualTo(ddl);

        assertUpdate("DROP VIEW " + viewName);
    }

    @Test
    @Override
    public void testViewCaseSensitivity()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_VIEW));

        String upperCaseView = "test_view_uppercase_" + randomNameSuffix();
        String mixedCaseView = "test_view_mixedcase_" + randomNameSuffix();

        computeActual("CREATE VIEW " + upperCaseView + " AS SELECT X FROM (SELECT 'upperCaseView' X)");
        computeActual("CREATE VIEW " + mixedCaseView + " AS SELECT XyZ FROM (SELECT 'mixedCaseView' XyZ)");
        assertQuery("SELECT * FROM " + upperCaseView, "SELECT X FROM (SELECT 'upperCaseView' X)");
        assertQuery("SELECT * FROM " + mixedCaseView, "SELECT XyZ FROM (SELECT 'mixedCaseView' XyZ)");

        assertUpdate("DROP VIEW " + upperCaseView);
        assertUpdate("DROP VIEW " + mixedCaseView);
    }

    @Test
    @Override
    public void testViewMetadata()
    {
        testViewMetadata("", "INVOKER");
        testViewMetadata(" SECURITY DEFINER", "INVOKER");
        testViewMetadata(" SECURITY INVOKER", "INVOKER");
    }

    private void testViewMetadata(String securityClauseInCreate, String securityClauseInShowCreate)
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_VIEW));

        String viewName = "meta_test_view_" + randomNameSuffix();

        @Language("SQL") String query = "SELECT x, y FROM (SELECT 'bar' x, 'foo' y)";
        assertUpdate("CREATE VIEW " + viewName + securityClauseInCreate + " AS " + query);

        // test INFORMATION_SCHEMA.TABLES
        MaterializedResult actual = computeActual(format(
                "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = '%s'",
                getSession().getSchema().get()));

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("customer", "BASE TABLE")
                .row(viewName, "VIEW")
                .row("nation", "BASE TABLE")
                .row("orders", "BASE TABLE")
                .row("region", "BASE TABLE")
                .build();

        assertContains(actual, expected);

        // test SHOW TABLES
        actual = computeActual("SHOW TABLES");

        MaterializedResult.Builder builder = resultBuilder(getSession(), actual.getTypes());
        for (MaterializedRow row : expected.getMaterializedRows()) {
            builder.row(row.getField(0));
        }
        expected = builder.build();

        assertContains(actual, expected);

        // test INFORMATION_SCHEMA.VIEWS
        actual = computeActual(format(
                "SELECT table_name, view_definition FROM information_schema.views WHERE table_schema = '%s' AND table_name = '%s'",
                getSession().getSchema().get(),
                viewName));

        expected = resultBuilder(getSession(), actual.getTypes())
                .row(viewName, formatSqlText(query))
                .build();

        assertThat(getComparableQuery(actual.toString(), "[\\s\"]")).isEqualTo(getComparableQuery(expected.toString(), "\\s"));

        // test SHOW COLUMNS
        assertThat(query("SHOW COLUMNS FROM " + viewName))
                .result().matches(resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                        .row("x", "varchar", "", "")
                        .row("y", "varchar", "", "")
                        .build());

        // test SHOW CREATE VIEW
        String expectedSql = formatSqlText(format(
                "CREATE VIEW %s.%s.%s SECURITY %s AS %s",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                viewName,
                securityClauseInShowCreate,
                query)).trim();

        actual = computeActual("SHOW CREATE VIEW " + viewName);

        assertThat(getOnlyElement(actual.getOnlyColumnAsSet())).isEqualTo(expectedSql);

        actual = computeActual(format("SHOW CREATE VIEW %s.%s.%s", getSession().getCatalog().get(), getSession().getSchema().get(), viewName));

        assertThat(getOnlyElement(actual.getOnlyColumnAsSet())).isEqualTo(expectedSql);

        assertUpdate("DROP VIEW " + viewName);
    }

    private String getComparableQuery(String query, String pattern)
    {
        return query.replaceAll(pattern, "");
    }

    @Test
    @Override // Overridden because test view does not take into account how SQLite handles views
    public void testCompatibleTypeChangeForView()
    {
        abort("aborted because testCompatibleTypeChangeForView does not take into account how SQLite handles views");
    }

    @Test
    @Override // Overridden because test view does not take into account how SQLite handles views
    public void testCompatibleTypeChangeForView2()
    {
        abort("aborted because testCompatibleTypeChangeForView2 does not take into account how SQLite handles views");
    }
}
