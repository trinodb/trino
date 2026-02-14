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
package io.trino.plugin.hsqldb;

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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.hsqldb.HsqlDbHelper.toRemoteIdentifier;
import static io.trino.plugin.hsqldb.HsqlDbTableProperties.PRIMARY_KEY_PROPERTY;
import static io.trino.plugin.hsqldb.TestingHsqlDbServer.TEST_SCHEMA;
import static io.trino.plugin.jdbc.JdbcWriteSessionProperties.NON_TRANSACTIONAL_MERGE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.QueryAssertions.assertContains;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_COMMENT_ON_VIEW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_COMMENT_ON_VIEW_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_MATERIALIZED_VIEW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_SCHEMA;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_VIEW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_DROP_SCHEMA_CASCADE;
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
final class TestHsqlDbConnectorTest
        extends BaseJdbcConnectorTest
{
    private TestingHsqlDbServer hsqldbServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        hsqldbServer = closeAfterClass(new TestingHsqlDbServer());
        return HsqlDbQueryRunner.builder(hsqldbServer)
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return hsqldbServer::execute;
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_ADD_COLUMN,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_CREATE_SCHEMA,
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
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_UPDATE -> true;

            case SUPPORTS_ADD_COLUMN_WITH_COMMENT,
                 SUPPORTS_ADD_FIELD,
                 SUPPORTS_AGGREGATION_PUSHDOWN,
                 SUPPORTS_ARRAY,
                 SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                 SUPPORTS_DEREFERENCE_PUSHDOWN,
                 SUPPORTS_DROP_FIELD,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN,
                 SUPPORTS_RENAME_FIELD,
                 SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS,
                 SUPPORTS_SET_FIELD_TYPE,
                 SUPPORTS_TOPN_PUSHDOWN -> false;

            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected Session getSession()
    {
        Session session = super.getSession();
        return Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), NON_TRANSACTIONAL_MERGE, "true")
                .build();
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
        List<String> keys = Arrays.asList(primaryKeys.split(",\\s*"));
        String constraint = format("WITH (%s = ARRAY['%s'])", PRIMARY_KEY_PROPERTY, String.join("','", keys.stream().map(String::toUpperCase).toList()));
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
    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.of(128);
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(128);
    }

    @Override
    protected OptionalInt maxColumnNameLength()
    {
        return OptionalInt.of(128);
    }

    @Override
    protected void verifyColumnNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("user lacks privilege or object not found: .*");
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("user lacks privilege or object not found: .*");
    }

    @Override
    protected Optional<SetColumnTypeSetup> filterSetColumnTypesDataProvider(SetColumnTypeSetup setup)
    {
        if (setup.sourceColumnType().equals("time(6)") && setup.newColumnType().equals("time(3)")) {
            // The connector trunk value
            return Optional.of(setup.withNewValueLiteral("TIME '15:03:00.123'"));
        }
        if (setup.sourceColumnType().equals("char(20)") && setup.newColumnType().equals("varchar")) {
            // The connector handle unbundled varchar as varchar(32768)
            return Optional.of(setup.withNewColumnType("varchar(32768)"));
        }

        return Optional.of(setup);
    }

    @Test
    @Override
    public void testInsertNegativeDate()
    {
        assertThatThrownBy(super::testInsertNegativeDate)
                .hasStackTraceContaining("java.lang.AssertionError: For query ");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup setup)
    {
        String type = setup.getTrinoTypeName();
        if (type.startsWith("timestamp") || type.startsWith("array") || type.startsWith("row")) {
            // HsqlDB does not support timestamp nor composite types
            return Optional.of(setup.asUnsupported());
        }
        String valueLiteral = setup.getSampleValueLiteral();
        if (type.equals("char(3)") && valueLiteral.equals("'ab'")) {
            // HsqlDB fills char(3) with spaces, causing test to fail
            return Optional.of(new DataMappingTestSetup("char(3)", "'abc'", "'zzz'"));
        }
        if (type.equals("date")) {
            if (valueLiteral.equals("DATE '0001-01-01'") || valueLiteral.equals("DATE '1582-10-05'")) {
                // HsqlDB does not support DATE 0001-01-01 nor DATE 1582-10-05, causing test to fail
                return Optional.empty();
            }
        }
        return Optional.of(setup);
    }

    @Test
    @Override
    public void testInsertInPresenceOfNotSupportedColumn()
    {
        assertThatThrownBy(super::testInsertInPresenceOfNotSupportedColumn)
                .hasMessageMatching("java.sql.SQLSyntaxErrorException: type not found or user lacks privilege: .*");
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        // HsqlDB requires declaring the DEFAULT value before the NOT NULL constraint
        return new TestTable(
                onRemoteDatabase(),
                TEST_SCHEMA + ".test_default_cols",
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT 43," +
                        "col_nonnull_default BIGINT DEFAULT 42 NOT NULL," +
                        "col_required2 BIGINT NOT NULL)");
    }

    @Override
    protected TestTable createTableWithUnsupportedColumn()
    {
        return new TestTable(
                onRemoteDatabase(),
                TEST_SCHEMA + ".test_unsupported_col",
                "(one BIGINT, two GEOMETRY, three VARCHAR(10))");
    }

    @Test
    void testViews()
    {
        // HsqlDB requires a FROM clause in SELECT queries. We use the nation table with LIMIT as dual table.
        try (TestView view = new TestView(onRemoteDatabase(), TEST_SCHEMA + ".test_view", "SELECT 'O' as status FROM nation LIMIT 1")) {
            assertQuery("SELECT status FROM " + view.getName(), "SELECT 'O'");
        }
    }

    @Test
    @Override // Override because HsqlDB requires a FROM clause in SELECT queries
    public void testCreateViewSchemaNotFound()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_VIEW));

        String schemaName = "test_schema_" + randomNameSuffix();
        String viewName = "test_view_create_no_schema_" + randomNameSuffix();
        try {
            assertQueryFails(
                    format("CREATE VIEW %s.%s AS SELECT 1 AS c1", schemaName, viewName),
                    format("Schema %s not found", schemaName));
            assertQueryFails(
                    format("CREATE OR REPLACE VIEW %s.%s AS SELECT 1 AS c1", schemaName, viewName),
                    format("Schema %s not found", schemaName));
        }
        finally {
            assertUpdate(format("DROP VIEW IF EXISTS %s.%s", schemaName, viewName));
        }
    }

    @Test
    @Override // Override because HsqlDB requires a FROM clause in SELECT queries
    public void testDropSchemaCascade()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_SCHEMA));

        if (!hasBehavior(SUPPORTS_DROP_SCHEMA_CASCADE)) {
            String schemaName = "test_drop_schema_cascade_" + randomNameSuffix();
            assertUpdate(createSchemaSql(schemaName));
            assertQueryFails(
                    "DROP SCHEMA " + schemaName + " CASCADE",
                    "This connector does not support dropping schemas with CASCADE option");
            assertUpdate("DROP SCHEMA " + schemaName);
            return;
        }

        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE) || hasBehavior(SUPPORTS_CREATE_VIEW) || hasBehavior(SUPPORTS_CREATE_MATERIALIZED_VIEW));

        String schemaName = "test_drop_schema_cascade_" + randomNameSuffix();
        String tableName = "test_table" + randomNameSuffix();
        String viewName = "test_view" + randomNameSuffix();
        String materializedViewName = "test_materialized_view" + randomNameSuffix();
        try {
            assertUpdate(createSchemaSql(schemaName));
            if (hasBehavior(SUPPORTS_CREATE_TABLE)) {
                assertUpdate("CREATE TABLE " + schemaName + "." + tableName + "(a INT)");
            }
            if (hasBehavior(SUPPORTS_CREATE_VIEW)) {
                assertUpdate("CREATE VIEW " + schemaName + "." + viewName + " AS SELECT 1 a FROM nation LIMIT 1");
            }
            if (hasBehavior(SUPPORTS_CREATE_MATERIALIZED_VIEW)) {
                assertUpdate("CREATE MATERIALIZED VIEW " + schemaName + "." + materializedViewName + " AS SELECT 1 a");
            }

            assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains(schemaName);

            assertUpdate("DROP SCHEMA " + schemaName + " CASCADE");
            assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).doesNotContain(schemaName);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + schemaName + "." + tableName);
            assertUpdate("DROP VIEW IF EXISTS " + schemaName + "." + viewName);
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + schemaName + "." + materializedViewName);
            assertUpdate("DROP SCHEMA IF EXISTS " + schemaName);
        }
    }

    @Test
    @Override // Override because HsqlDB requires a FROM clause in SELECT queries
    public void testViewCaseSensitivity()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_VIEW));

        String upperCaseView = "test_view_uppercase_" + randomNameSuffix();
        String mixedCaseView = "test_view_mixedcase_" + randomNameSuffix();
        // FIXME: With HsqlDB you can take the INFORMATION_SCHEMA.SYSTEM_USERS as a DUAL table
        // FIXME: but Trino cant retrieve this table and as workaround we use nation table here...
        computeActual("CREATE VIEW " + upperCaseView + " AS SELECT X FROM (SELECT 123 X FROM nation LIMIT 1)");
        computeActual("CREATE VIEW " + mixedCaseView + " AS SELECT XyZ FROM (SELECT 456 XyZ FROM nation LIMIT 1)");
        assertQuery("SELECT * FROM " + upperCaseView, "SELECT X FROM (SELECT 123 X)");
        assertQuery("SELECT * FROM " + mixedCaseView, "SELECT XyZ FROM (SELECT 456 XyZ)");

        assertUpdate("DROP VIEW " + upperCaseView);
        assertUpdate("DROP VIEW " + mixedCaseView);
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
        try (TestTable table = new TestTable(onRemoteDatabase(), TEST_SCHEMA + "." + tableName, format("(c %s)", columnType))) {
            onRemoteDatabase().execute(format("INSERT INTO %s VALUES (%s)", table.getName(), columnLiteral));

            assertThat(query(format("SELECT * FROM %s WHERE c %s %s", table.getName(), operator, filterLiteral)))
                    .isFullyPushedDown();
        }
    }

    @Test
    @Override // Override because HsqlDB requires named columns and FROM clause in SELECT queries
    public void testNativeQuerySimple()
    {
        assertQuery("SELECT * FROM TABLE(system.query(query => 'SELECT 1 res FROM INFORMATION_SCHEMA.SYSTEM_USERS'))", "VALUES 1");
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

    @Test
    @Override
    public void testNativeQuerySelectUnsupportedType()
    {
        assertThatThrownBy(super::testNativeQuerySelectUnsupportedType)
                .hasStackTraceContaining("type not found or user lacks privilege");
    }

    @Test
    @Override // Overridden because HsqlDB does not support row field
    public void testInsertDefaultNullIntoNotNullColumn()
    {
        assertThatThrownBy(super::testInsertDefaultNullIntoNotNullColumn)
                .hasStackTraceContaining("integrity constraint violation: NOT NULL check constraint ;");
    }

    @Test
    @Override // Overridden because HsqlDB does not support row field
    public void testSetFieldType()
    {
        assertThatThrownBy(super::testSetFieldType)
                .hasStackTraceContaining("Unsupported column type: row(\"field\" integer)");
    }

    @Test
    @Override // Overridden because HsqlDB does not support row field
    public void testAddRowField()
    {
        assertThatThrownBy(super::testAddRowField)
                .hasStackTraceContaining("Unsupported column type: row(\"x\" integer)");
    }

    @Test
    @Override // Overridden because HsqlDB does not support row field
    public void testDropRowField()
    {
        assertThatThrownBy(super::testDropRowField)
                .hasStackTraceContaining("Unsupported column type: row(\"x\" integer, \"y\" integer)");
    }

    @Test
    @Override // Overridden because HsqlDB does not support row field
    public void testAddRowFieldInArray()
    {
        assertThatThrownBy(super::testAddRowFieldInArray)
                .hasStackTraceContaining("Unsupported column type: array(row(\"x\" integer))");
    }

    @Test
    @Override // Overridden because HsqlDB does not support row field
    public void testDropRowFieldInArray()
    {
        assertThatThrownBy(super::testDropRowFieldInArray)
                .hasStackTraceContaining("Unsupported column type: array(row(\"x\" integer, \"y\" integer))");
    }

    @Test
    @Override // Overridden because HsqlDB does not support row field
    public void testUpdateRowType()
    {
        assertThatThrownBy(super::testUpdateRowType)
                .hasStackTraceContaining("Unsupported column type: row(\"f1\" integer, \"f2\" integer)");
    }

    @Test
    @Override // Overridden because HsqlDB does not support row field
    public void testRenameRowField()
    {
        assertThatThrownBy(super::testRenameRowField)
                .hasStackTraceContaining("Unsupported column type: row(\"x\" integer)");
    }

    @Test
    @Override // Overridden because HsqlDB does not support row field
    public void testPredicateOnRowTypeField()
    {
        assertThatThrownBy(super::testPredicateOnRowTypeField)
                .hasStackTraceContaining("Unsupported column type: row(\"varchar_t\" varchar, \"int_t\" integer)");
    }

    @Test
    @Override // Overridden because HsqlDB does not support row field
    public void testPotentialDuplicateDereferencePushdown()
    {
        assertThatThrownBy(super::testPotentialDuplicateDereferencePushdown)
                .hasStackTraceContaining("Unsupported column type: row(\"a\" varchar, \"b\" bigint)");
    }

    @Test
    @Override // Overridden because HsqlDB does not support row field
    public void testProjectionPushdown()
    {
        assertThatThrownBy(super::testProjectionPushdown)
                .hasStackTraceContaining("Unsupported column type: row(\"f1\" bigint, \"f2\" bigint)");
    }

    @Test
    @Override // Overridden because HsqlDB only supports literal as default column value
    public void testMergeDefaultNullIntoNotNullColumn()
    {
        assertThatThrownBy(super::testMergeDefaultNullIntoNotNullColumn)
                .hasStackTraceContaining("Default column value supports only literals");
    }

    @Test
    @Override
    public void testCreateTableAsSelectNegativeDate()
    {
        assertThatThrownBy(super::testCreateTableAsSelectNegativeDate)
                .hasStackTraceContaining("java.lang.AssertionError: ");
    }

    @Test
    @Override // Override because HsqlDB requires the FROM clause on the INFORMATION_SCHEMA.SYSTEM_USERS
    public void testNativeQueryParameters()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query_simple", "SELECT * FROM TABLE(system.query(query => ?))")
                .addPreparedStatement("my_query", "SELECT * FROM TABLE(system.query(query => format('SELECT %s FROM %s', ?, ?)))")
                .build();
        assertQuery(session, "EXECUTE my_query_simple USING 'SELECT 1 a FROM INFORMATION_SCHEMA.SYSTEM_USERS'", "VALUES 1");
        assertQuery(session, "EXECUTE my_query USING 'a', '(SELECT 2 a FROM INFORMATION_SCHEMA.SYSTEM_USERS) t'", "VALUES 2");
    }

    @Override
    protected void verifyAddNotNullColumnToNonEmptyTableFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("default expression needed in statement \\[.*]");
    }

    @Override
    @Language("RegExp")
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return format("integrity constraint violation: NOT NULL check constraint ; .* table: \".*\" column: \"%s\"", columnName.toUpperCase(ENGLISH));
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("user lacks privilege or object not found:");
    }

    @Override
    @Language("RegExp")
    protected String errorMessageForCreateTableAsSelectNegativeDate(String date)
    {
        return format("Failed to insert data: \\(conn=.*\\) Incorrect date value: '%s'.*", date);
    }

    @Override
    protected void verifySetColumnTypeFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("data exception: .*");
    }

    @Test
    @Override // overridden because a table to which a view is attached cannot be deleted.
    public void testCompatibleTypeChangeForView2()
    {
        abort("aborted because a table to which a view is attached cannot be deleted");
    }

    @Test
    @Override // Overridden because HsqlDB requires schema in table name for view select
    public void testView()
    {
        if (!hasBehavior(SUPPORTS_CREATE_VIEW)) {
            assertQueryFails("CREATE VIEW nation_v AS SELECT * FROM nation", "This connector does not support creating views");
            return;
        }

        @Language("SQL") String query = "SELECT orderkey, orderstatus, (totalprice / 2) half FROM public.orders";
        @Language("SQL") String viewQuery = " AS SELECT 123 x FROM nation LIMIT 1";

        String catalogName = getSession().getCatalog().orElseThrow();
        String schemaName = getSession().getSchema().orElseThrow();
        String testView = "test_view_" + randomNameSuffix();
        String testViewWithComment = "test_view_with_comment_" + randomNameSuffix();
        assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet()) // prime the cache, if any
                .doesNotContain(testView);
        assertUpdate("CREATE VIEW " + testView + viewQuery);
        assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet())
                .contains(testView);
        assertUpdate("CREATE OR REPLACE VIEW " + testView + " AS " + query);

        assertUpdate("CREATE VIEW " + testViewWithComment + " COMMENT 'orders'" + viewQuery);
        assertUpdate("CREATE OR REPLACE VIEW " + testViewWithComment + " COMMENT 'orders' AS " + query);

        // verify comment
        assertThat((String) computeScalar("SHOW CREATE VIEW " + testViewWithComment)).contains("COMMENT 'orders'");
        assertThat(query(
                "SELECT table_name, comment FROM system.metadata.table_comments " +
                        "WHERE catalog_name = '" + catalogName + "' AND " +
                        "schema_name = '" + schemaName + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES ('" + testView + "', null), ('" + testViewWithComment + "', 'orders')");

        // reading
        assertQuery("SELECT * FROM " + testView, query);
        assertQuery("SELECT * FROM " + testViewWithComment, query);

        assertQuery(
                "SELECT * FROM " + testView + " a JOIN " + testView + " b on a.orderkey = b.orderkey",
                format("SELECT * FROM (%s) a JOIN (%s) b ON a.orderkey = b.orderkey", query, query));

        assertQuery("WITH orders AS (SELECT * FROM orders LIMIT 0) SELECT * FROM " + testView, query);

        String name = format("%s.%s.%s", catalogName, schemaName, testView);
        assertQuery("SELECT * FROM " + name, query);

        assertUpdate("DROP VIEW " + testViewWithComment);

        // information_schema.views without table_name filter
        assertThat(query(
                "SELECT table_name, regexp_replace(view_definition, '[\\s\"]', '') FROM information_schema.views " +
                        "WHERE table_schema = '" + schemaName + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES ('" + testView + "', '" + getComparableQuery(query) + "')");
        // information_schema.views with table_name filter
        assertQuery(
                "SELECT table_name, regexp_replace(view_definition, '[\\s\"]', '') FROM information_schema.views " +
                        "WHERE table_schema = '" + schemaName + "' and table_name = '" + testView + "'",
                "VALUES ('" + testView + "', '" + getComparableQuery(query) + "')");

        // table listing
        assertThat(query("SHOW TABLES"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + testView + "'");
        // information_schema.tables without table_name filter
        assertThat(query(
                "SELECT table_name, table_type FROM information_schema.tables " +
                        "WHERE table_schema = '" + schemaName + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES ('" + testView + "', 'VIEW')");
        // information_schema.tables with table_name filter
        assertQuery(
                "SELECT table_name, table_type FROM information_schema.tables " +
                        "WHERE table_schema = '" + schemaName + "' and table_name = '" + testView + "'",
                "VALUES ('" + testView + "', 'VIEW')");

        // system.jdbc.tables without filter
        assertThat(query("SELECT table_schem, table_name, table_type FROM system.jdbc.tables"))
                .skippingTypesCheck()
                .containsAll("VALUES ('" + schemaName + "', '" + testView + "', 'VIEW')");

        // system.jdbc.tables with table prefix filter
        assertQuery(
                "SELECT table_schem, table_name, table_type " +
                        "FROM system.jdbc.tables " +
                        "WHERE table_cat = '" + catalogName + "' AND " +
                        "table_schem = '" + schemaName + "' AND " +
                        "table_name = '" + testView + "'",
                "VALUES ('" + schemaName + "', '" + testView + "', 'VIEW')");

        // column listing
        assertThat(query("SHOW COLUMNS FROM " + testView))
                .result()
                .projected("Column") // column types can very between connectors
                .skippingTypesCheck()
                .matches("VALUES 'orderkey', 'orderstatus', 'half'");

        assertThat(query("DESCRIBE " + testView))
                .result()
                .projected("Column") // column types can very between connectors
                .skippingTypesCheck()
                .matches("VALUES 'orderkey', 'orderstatus', 'half'");

        // information_schema.columns without table_name filter
        assertThat(query(
                "SELECT table_name, column_name " +
                        "FROM information_schema.columns " +
                        "WHERE table_schema = '" + schemaName + "'"))
                .skippingTypesCheck()
                .containsAll(
                        "SELECT * FROM (VALUES '" + testView + "') " +
                                "CROSS JOIN UNNEST(ARRAY['orderkey', 'orderstatus', 'half'])");

        // information_schema.columns with table_name filter
        assertThat(query(
                "SELECT table_name, column_name " +
                        "FROM information_schema.columns " +
                        "WHERE table_schema = '" + schemaName + "' and table_name = '" + testView + "'"))
                .skippingTypesCheck()
                .containsAll(
                        "SELECT * FROM (VALUES '" + testView + "') " +
                                "CROSS JOIN UNNEST(ARRAY['orderkey', 'orderstatus', 'half'])");

        // view-specific listings
        assertThat(query("SELECT table_name FROM information_schema.views WHERE table_schema = '" + schemaName + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + testView + "'");

        // system.jdbc.columns without filter
        assertThat(query("SELECT table_schem, table_name, column_name FROM system.jdbc.columns"))
                .skippingTypesCheck()
                .containsAll(
                        "SELECT * FROM (VALUES ('" + schemaName + "', '" + testView + "')) " +
                                "CROSS JOIN UNNEST(ARRAY['orderkey', 'orderstatus', 'half'])");

        // system.jdbc.columns with schema filter
        assertThat(query(
                "SELECT table_schem, table_name, column_name " +
                        "FROM system.jdbc.columns " +
                        "WHERE table_schem LIKE '%" + schemaName + "%'"))
                .skippingTypesCheck()
                .containsAll(
                        "SELECT * FROM (VALUES ('" + schemaName + "', '" + testView + "')) " +
                                "CROSS JOIN UNNEST(ARRAY['orderkey', 'orderstatus', 'half'])");

        // system.jdbc.columns with table filter
        assertThat(query(
                "SELECT table_schem, table_name, column_name " +
                        "FROM system.jdbc.columns " +
                        "WHERE table_name LIKE '%" + testView + "%'"))
                .skippingTypesCheck()
                .containsAll(
                        "SELECT * FROM (VALUES ('" + schemaName + "', '" + testView + "')) " +
                                "CROSS JOIN UNNEST(ARRAY['orderkey', 'orderstatus', 'half'])");

        assertUpdate("DROP VIEW " + testView);
        assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet())
                .doesNotContain(testView);
    }

    @Test
    @Override // Override because HsqlDB does not support row expression
    public void testShowCreateView()
    {
        assertThatThrownBy(super::testShowCreateView)
                .hasStackTraceContaining("row expression not allowed");
    }

    @Test
    @Override // Override because HsqlDB does not support row expression
    public void testRefreshView()
    {
        assertThatThrownBy(super::testShowCreateView)
                .hasStackTraceContaining("row expression not allowed");
    }

    @Test
    @Override // Override because for HsqlDB an empty or null comment are same
    public void testCommentViewColumn()
    {
        if (!hasBehavior(SUPPORTS_COMMENT_ON_VIEW_COLUMN)) {
            if (hasBehavior(SUPPORTS_CREATE_VIEW)) {
                try (TestView view = new TestView(getQueryRunner()::execute, "test_comment_view_column", "SELECT * FROM region")) {
                    assertQueryFails("COMMENT ON COLUMN " + view.getName() + ".regionkey IS 'new region key comment'", "This connector does not support setting view column comments");
                }
                return;
            }
            abort("Skipping as connector does not support CREATE VIEW");
        }

        String viewColumnName = "regionkey";
        try (TestView view = new TestView(getQueryRunner()::execute, "test_comment_view_column", "SELECT * FROM region")) {
            // comment set
            assertUpdate("COMMENT ON COLUMN " + view.getName() + "." + viewColumnName + " IS 'new region key comment'");
            assertThat(getColumnComment(view.getName(), viewColumnName)).isEqualTo("new region key comment");

            // comment deleted
            assertUpdate("COMMENT ON COLUMN " + view.getName() + "." + viewColumnName + " IS NULL");
            assertThat(getColumnComment(view.getName(), viewColumnName)).isEqualTo(null);

            // comment set to non-empty value before verifying setting empty comment
            assertUpdate("COMMENT ON COLUMN " + view.getName() + "." + viewColumnName + " IS 'updated region key comment'");
            assertThat(getColumnComment(view.getName(), viewColumnName)).isEqualTo("updated region key comment");

            // comment set to empty
            assertUpdate("COMMENT ON COLUMN " + view.getName() + "." + viewColumnName + " IS ''");
            assertThat(getColumnComment(view.getName(), viewColumnName)).isEqualTo(null);
        }
    }

    @Test
    @Override // Override because HsqlDB requires FROM clause in SELECT
    public void testDropNonEmptySchemaWithView()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_VIEW));

        // A connector either supports CREATE SCHEMA and DROP SCHEMA or none of them.
        if (!hasBehavior(SUPPORTS_CREATE_SCHEMA)) {
            return;
        }

        String schemaName = "test_drop_non_empty_schema_view_" + randomNameSuffix();

        try {
            assertUpdate(createSchemaSql(schemaName));
            assertUpdate("CREATE VIEW " + schemaName + ".v_t  AS SELECT 123 x FROM nation LIMIT 1");

            assertQueryFails("DROP SCHEMA " + schemaName, ".*Cannot drop non-empty schema '\\Q" + schemaName + "\\E'");
        }
        finally {
            assertUpdate("DROP VIEW IF EXISTS " + schemaName + ".v_t");
            assertUpdate("DROP SCHEMA IF EXISTS " + schemaName);
        }
    }

    @Test
    @Override // Overridden because for HsqlDB empty comment and NULL comment are same
    public void testCommentView()
    {
        if (!hasBehavior(SUPPORTS_COMMENT_ON_VIEW)) {
            if (hasBehavior(SUPPORTS_CREATE_VIEW)) {
                try (TestView view = new TestView(getQueryRunner()::execute, "test_comment_view", "SELECT * FROM region")) {
                    assertQueryFails("COMMENT ON VIEW " + view.getName() + " IS 'new comment'", "This connector does not support setting view comments");
                }
                return;
            }
            abort("Skipping as connector does not support CREATE VIEW");
        }

        try (TestView view = new TestView(getQueryRunner()::execute, "test_comment_view", "SELECT * FROM region")) {
            // comment set
            assertUpdate("COMMENT ON VIEW " + view.getName() + " IS 'new comment'");
            assertThat((String) computeScalar("SHOW CREATE VIEW " + view.getName())).contains("COMMENT 'new comment'");
            assertThat(getTableComment(view.getName())).isEqualTo("new comment");

            // comment deleted
            assertUpdate("COMMENT ON VIEW " + view.getName() + " IS NULL");
            assertThat(getTableComment(view.getName())).isEqualTo(null);

            // comment set to non-empty value before verifying setting empty comment
            assertUpdate("COMMENT ON VIEW " + view.getName() + " IS 'updated comment'");
            assertThat(getTableComment(view.getName())).isEqualTo("updated comment");

            // comment set to empty
            assertUpdate("COMMENT ON VIEW " + view.getName() + " IS ''");
            assertThat(getTableComment(view.getName())).isEqualTo(null);
        }

        String viewName = "test_comment_view" + randomNameSuffix();
        try {
            // comment set when creating a table
            assertUpdate("CREATE VIEW " + viewName + " COMMENT 'new view comment' AS SELECT * FROM region");
            assertThat(getTableComment(viewName)).isEqualTo("new view comment");
        }
        finally {
            assertUpdate("DROP VIEW IF EXISTS " + viewName);
        }
    }

    @Test
    @Override
    public void testCompatibleTypeChangeForView()
    {
        abort("Skipping as HsqlDB does not support changing type on table column held by view");
    }

    @Test
    @Override // Overridden because for views, all comparisons must be made without regard to case or double cote.
    public void testViewMetadata()
    {
        testViewMetadata("", "DEFINER");
        testViewMetadata(" SECURITY DEFINER", "DEFINER");
        testViewMetadata(" SECURITY INVOKER", "INVOKER");
    }

    private void testViewMetadata(String securityClauseInCreate, String securityClauseInShowCreate)
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_VIEW));

        String viewName = "meta_test_view_" + randomNameSuffix();

        @Language("SQL") String query = "SELECT CAST('123' AS BIGINT) x, 'foo' y FROM public.nation LIMIT 1";
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
                "SELECT table_name, view_definition FROM information_schema.views WHERE table_schema = '%s'",
                getSession().getSchema().get()));

        expected = resultBuilder(getSession(), actual.getTypes())
                .row(viewName, formatSqlText(query))
                .build();

        assertThat(getComparableQuery(actual.toString(), "[\\s\"]")).isEqualTo(getComparableQuery(expected.toString(), "\\s"));

        // test SHOW COLUMNS
        assertThat(query("SHOW COLUMNS FROM " + viewName))
                .result().matches(resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                        .row("x", "bigint", "", "")
                        .row("y", "char(3)", "", "")
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

        assertThat(getComparableQuery(getOnlyElement(actual.getOnlyColumnAsSet()), "\"")).isEqualTo(getComparableQuery(expectedSql, "\""));

        actual = computeActual(format("SHOW CREATE VIEW %s.%s.%s", getSession().getCatalog().get(), getSession().getSchema().get(), viewName));

        assertThat(getComparableQuery(getOnlyElement(actual.getOnlyColumnAsSet()), "\"")).isEqualTo(getComparableQuery(expectedSql, "\""));

        assertUpdate("DROP VIEW " + viewName);
    }

    @Test
    @Override
    public void testAddColumnConcurrently()
    {
        // TODO: Difficult to determine whether the exception is concurrent issue or not from the error message
        abort("TODO: Enable this test after finding the failure cause");
    }

    private String getComparableQuery(String query)
    {
        return getComparableQuery(query, "\\s");
    }

    private String getComparableQuery(Object query, String pattern)
    {
        return getComparableQuery((String) query, pattern);
    }

    private String getComparableQuery(String query, String pattern)
    {
        return toRemoteIdentifier(query).replaceAll(pattern, "");
    }
}
