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

import static io.trino.plugin.jdbc.JdbcWriteSessionProperties.NON_TRANSACTIONAL_MERGE;
import static io.trino.plugin.jdbc.JdbcWriteSessionProperties.WRITE_BATCH_SIZE;
import static io.trino.plugin.jdbc.JdbcWriteSessionProperties.WRITE_PARALLELISM;
import static io.trino.plugin.sqlite.SqliteTableProperties.PRIMARY_KEY_PROPERTY;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_DEFAULT_COLUMN_VALUE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_DROP_DEFAULT_COLUMN_VALUE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_INSERT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_MERGE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_NATIVE_QUERY;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_SET_DEFAULT_COLUMN_VALUE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.Math.min;
import static java.lang.String.format;
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
            case SUPPORTS_ADD_COLUMN,
                 SUPPORTS_CREATE_TABLE,
                 SUPPORTS_CREATE_VIEW,
                 SUPPORTS_DEFAULT_COLUMN_VALUE,
                 SUPPORTS_DELETE,
                 SUPPORTS_INSERT,
                 SUPPORTS_LIMIT_PUSHDOWN,
                 SUPPORTS_MERGE,
                 SUPPORTS_NEGATIVE_DATE,
                 SUPPORTS_PREDICATE_PUSHDOWN,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_UPDATE -> true;

            case SUPPORTS_ADD_COLUMN_WITH_COMMENT,
                 SUPPORTS_ADD_COLUMN_WITH_POSITION,
                 SUPPORTS_ADD_FIELD,
                 SUPPORTS_AGGREGATION_PUSHDOWN,
                 SUPPORTS_ARRAY,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                 SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT,
                 SUPPORTS_DEREFERENCE_PUSHDOWN,
                 SUPPORTS_DROP_DEFAULT_COLUMN_VALUE,
                 SUPPORTS_DROP_FIELD,
                 SUPPORTS_DROP_NOT_NULL_CONSTRAINT,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN,
                 SUPPORTS_RENAME_FIELD,
                 SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_SET_DEFAULT_COLUMN_VALUE,
                 SUPPORTS_SET_FIELD_TYPE,
                 SUPPORTS_TOPN_PUSHDOWN -> false;

            default -> super.hasBehavior(connectorBehavior);
        };
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
        predicatePushdownTest("DECIMAL(30, 10)", "123456789.987654321", "<=", "123456789.987654321");
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
        skipTestUnless(hasBehavior(SUPPORTS_MERGE) && hasBehavior(SUPPORTS_INSERT));
        mergeLarge(TINY_SCHEMA_NAME, 15_000);
        mergeLarge("sf1", 15_000);
        mergeLarge("sf1", 100_000);
        // FIXME: SQLite does not support the full insert FROM tpch.sf1.orders
        assertThatThrownBy(() -> mergeLarge("sf1", 1_500_000))
                .hasMessageContaining("[SQLITE_BUSY] The database file is locked (database is locked)");
    }

    private void mergeLarge(String sourceSchema, long limit)
    {
        String sourceTable = format("tpch.%s.orders", sourceSchema);
        String tableName = "test_merge_" + randomNameSuffix();

        createTableForWrites("CREATE TABLE %s (orderkey BIGINT, custkey BIGINT, totalprice DOUBLE)", tableName, Optional.of("orderkey"));

        assertUpdate(
                format("INSERT INTO %s SELECT orderkey, custkey, totalprice FROM %s ORDER BY orderkey LIMIT %d", tableName, sourceTable, limit),
                min((long) computeScalar("SELECT count(*) FROM " + sourceTable), limit));

        @Language("SQL") String mergeSql = "" +
                "MERGE INTO " + tableName + " t USING (SELECT * FROM " + sourceTable + ") s ON (t.orderkey = s.orderkey)\n" +
                "WHEN MATCHED AND mod(t.orderkey, 3) = 0 THEN UPDATE SET totalprice = t.totalprice + s.totalprice\n" +
                "WHEN MATCHED AND mod(t.orderkey, 3) = 1 THEN DELETE";

        assertUpdate(mergeSql, (long) computeScalar("SELECT count(*) FROM (SELECT orderkey FROM " + sourceTable + " ORDER BY orderkey LIMIT " + limit + ") s WHERE mod(s.orderkey, 3) IN (0, 1)"));

        // verify deleted rows
        assertQuery("SELECT count(*) FROM " + tableName + " WHERE mod(orderkey, 3) = 1", "SELECT 0");

        // verify untouched rows
        assertThat(query("SELECT count(*), sum(cast(totalprice AS decimal(18,2))) FROM " + tableName + " WHERE mod(orderkey, 3) = 2"))
                .matches("SELECT count(*), sum(cast(s.totalprice AS decimal(18,2))) FROM (SELECT orderkey, totalprice FROM " + sourceTable + " ORDER BY orderkey LIMIT " + limit + ") s WHERE mod(s.orderkey, 3) = 2");

        // verify updated rows
        assertThat(query("SELECT count(*), sum(cast(totalprice AS decimal(18,2))) FROM " + tableName + " WHERE mod(orderkey, 3) = 0"))
                .matches("SELECT count(*), sum(cast(s.totalprice AS decimal(18,2)) * 2) FROM (SELECT orderkey, totalprice FROM " + sourceTable + " ORDER BY orderkey LIMIT " + limit + ") s WHERE mod(s.orderkey, 3) = 0");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    @Override // FIXME: This test cant work as is...
    public void testDropDefaultColumn()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE) && hasBehavior(SUPPORTS_DEFAULT_COLUMN_VALUE));

        try (TestTable table = newTrinoTable("test_set_default", "(col int DEFAULT 123)")) {
            if (!hasBehavior(SUPPORTS_DROP_DEFAULT_COLUMN_VALUE)) {
                assertQueryFails("ALTER TABLE " + table.getName() + " ALTER COLUMN col DROP DEFAULT", "This connector does not support dropping default values");
                return;
            }

            assertThat(getColumnDefault(table.getName(), "col")).isEqualTo("123");

            assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN col DROP DEFAULT");
            assertThat(getColumnDefault(table.getName(), "col")).isNull();
        }
    }

    @Test
    @Override // Overridden because the expected message is different
    public void testSetDefaultColumn()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE) && hasBehavior(SUPPORTS_DEFAULT_COLUMN_VALUE));

        try (TestTable table = newTrinoTable("test_set_default", "(col int)")) {
            if (!hasBehavior(SUPPORTS_SET_DEFAULT_COLUMN_VALUE)) {
                assertQueryFails("ALTER TABLE " + table.getName() + " ALTER COLUMN col SET DEFAULT NULL", "This connector does not support setting default values");
                return;
            }

            assertThat(getColumnDefault(table.getName(), "col")).isNull();

            assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN col SET DEFAULT 123");
            assertThat(getColumnDefault(table.getName(), "col")).isEqualTo("123");
        }
    }

    @Test
    @Override // Overridden because the expected message is different
    public void testInsertDefaultNullIntoNotNullColumn()
    {
        assertThatThrownBy(super::testInsertDefaultNullIntoNotNullColumn)
                .hasStackTraceContaining("Failed to insert data: [SQLITE_CONSTRAINT_NOTNULL] A NOT NULL constraint failed (NOT NULL constraint failed: ");
    }

    @Test
    @Override // Overridden because SQLite does not support row field
    public void testSetFieldType()
    {
        assertThatThrownBy(super::testSetFieldType)
                .hasStackTraceContaining("Unsupported column type: row(\"field\" integer)");
    }

    @Test
    @Override // Overridden because SQLite does not support row field
    public void testAddRowField()
    {
        assertThatThrownBy(super::testAddRowField)
                .hasStackTraceContaining("Unsupported column type: row(\"x\" integer)");
    }

    @Test
    @Override // Overridden because SQLite does not support row field
    public void testDropRowField()
    {
        assertThatThrownBy(super::testDropRowField)
                .hasStackTraceContaining("Unsupported column type: row(\"x\" integer, \"y\" integer)");
    }

    @Test
    @Override // Overridden because SQLite does not support row field
    public void testAddRowFieldInArray()
    {
        assertThatThrownBy(super::testAddRowFieldInArray)
                .hasStackTraceContaining("Unsupported column type: array(row(\"x\" integer))");
    }

    @Test
    @Override // Overridden because SQLite does not support row field
    public void testDropRowFieldInArray()
    {
        assertThatThrownBy(super::testDropRowFieldInArray)
                .hasStackTraceContaining("Unsupported column type: array(row(\"x\" integer, \"y\" integer))");
    }

    @Test
    @Override // Overridden because SQLite does not support row field
    public void testUpdateRowType()
    {
        assertThatThrownBy(super::testUpdateRowType)
                .hasStackTraceContaining("Unsupported column type: row(\"f1\" integer, \"f2\" integer)");
    }

    @Test
    @Override // Overridden because SQLite does not support row field
    public void testRenameRowField()
    {
        assertThatThrownBy(super::testRenameRowField)
                .hasStackTraceContaining("Unsupported column type: row(\"x\" integer)");
    }

    @Test
    @Override // Overridden because SQLite does not support row field
    public void testPredicateOnRowTypeField()
    {
        assertThatThrownBy(super::testPredicateOnRowTypeField)
                .hasStackTraceContaining("Unsupported column type: row(\"varchar_t\" varchar, \"int_t\" integer)");
    }

    @Test
    @Override // Overridden because SQLite does not support row field
    public void testPotentialDuplicateDereferencePushdown()
    {
        assertThatThrownBy(super::testPotentialDuplicateDereferencePushdown)
                .hasStackTraceContaining("Unsupported column type: row(\"a\" varchar, \"b\" bigint)");
    }

    @Test
    @Override // Overridden because SQLite does not support row field
    public void testProjectionPushdown()
    {
        assertThatThrownBy(super::testProjectionPushdown)
                .hasStackTraceContaining("Unsupported column type: row(\"f1\" bigint, \"f2\" bigint)");
    }

    @Test
    @Override // Overridden because SQLite only supports literal as default column value
    public void testMergeDefaultNullIntoNotNullColumn()
    {
        assertThatThrownBy(super::testMergeDefaultNullIntoNotNullColumn)
                .hasStackTraceContaining("Default column value supports only literals");
    }

    @Test
    @Override // Overridden because test view does not take into account how SQLite handles views
    public void testView()
    {
        abort("aborted because testView does not take into account how SQLite handles views");
    }

    @Test
    @Override // Overridden because test view does not take into account how SQLite handles views
    public void testShowCreateView()
    {
        abort("aborted because testShowCreateView does not take into account how SQLite handles views");
    }

    @Test
    @Override // Overridden because test view does not take into account how SQLite handles views
    public void testViewCaseSensitivity()
    {
        abort("aborted because testViewCaseSensitivity does not take into account how SQLite handles views");
    }

    @Test
    @Override // Overridden because test view does not take into account how SQLite handles views
    public void testViewMetadata()
    {
        abort("aborted because testViewMetadata does not take into account how SQLite handles views");
    }

    @Test
    @Override // Overridden because test view does not take into account how SQLite handles views
    public void testRefreshView()
    {
        abort("aborted because testRefreshView does not take into account how SQLite handles views");
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
