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
package io.trino.plugin.duckdb;

import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.nio.file.Path;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;

import static io.trino.plugin.duckdb.DuckDbClientModule.SECURITY_CONNECTION_PROPERTIES;
import static io.trino.plugin.duckdb.TestingDuckDb.TPCH_SCHEMA;
import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_NATIVE_QUERY;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Isolated
final class TestDuckDbConnectorTest
        extends BaseJdbcConnectorTest
{
    private TestingDuckDb duckDb;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        duckDb = closeAfterClass(new TestingDuckDb());
        return DuckDbQueryRunner.builder(duckDb)
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_ADD_COLUMN_NOT_NULL_CONSTRAINT,
                 SUPPORTS_ADD_COLUMN_WITH_COMMENT,
                 SUPPORTS_ADD_COLUMN_WITH_POSITION,
                 SUPPORTS_AGGREGATION_PUSHDOWN,
                 SUPPORTS_PREDICATE_ARITHMETIC_EXPRESSION_PUSHDOWN,
                 SUPPORTS_ARRAY,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                 SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT,
                 SUPPORTS_JOIN_PUSHDOWN,
                 SUPPORTS_LIMIT_PUSHDOWN,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_RENAME_SCHEMA,
                 SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_MERGE,
                 SUPPORTS_ROW_LEVEL_UPDATE -> false;

            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup setup)
    {
        String type = setup.getTrinoTypeName();

        if (type.equals("time") ||
                type.startsWith("time(") ||
                type.startsWith("timestamp") ||
                type.equals("varbinary") ||
                type.startsWith("array") ||
                type.startsWith("row")) {
            return Optional.of(setup.asUnsupported());
        }

        if (setup.getTrinoTypeName().equals("char(3)") && setup.getSampleValueLiteral().equals("'ab'")) {
            return Optional.of(new DataMappingTestSetup("char(3)", "'abc'", "'zzz'"));
        }
        return Optional.of(setup);
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                onRemoteDatabase(),
                TPCH_SCHEMA + ".test_default_cols",
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
                TPCH_SCHEMA + ".test_unsupported_col",
                "(one bigint, two union(num integer, str varchar), three varchar)");
    }

    @Override // Override because DuckDB ignores column size of varchar type
    protected MaterializedResult getDescribeOrdersResult()
    {
        return resultBuilder(VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("clerk", "varchar", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar", "", "")
                .build();
    }

    @Test
    @Override // Override because DuckDB ignores column size of varchar type
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE orders"))
                .isEqualTo(
                        """
                        CREATE TABLE duckdb.tpch.orders (
                           orderkey bigint,
                           custkey bigint,
                           orderstatus varchar,
                           totalprice double,
                           orderdate date,
                           orderpriority varchar,
                           clerk varchar,
                           shippriority integer,
                           comment varchar
                        )""");
    }

    @Test
    @Override // Override because the connector doesn't support row level delete
    public void testDeleteWithLike()
    {
        assertThatThrownBy(super::testDeleteWithLike)
                .hasStackTraceContaining("TrinoException: " + MODIFYING_ROWS_MESSAGE);
    }

    @Override
    @Language("RegExp")
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return "Constraint Error: NOT NULL constraint failed: .*." + columnName;
    }

    @Override
    protected void verifyAddNotNullColumnToNonEmptyTableFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("Adding columns with constraints not yet supported");
    }

    @Override
    protected void verifyConcurrentAddColumnFailurePermissible(Exception e)
    {
        assertThat(e).hasMessageContaining("Catalog write-write conflict");
    }

    @Override
    protected void verifySetColumnTypeFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("Conversion Error");
    }

    @Override
    protected Optional<SetColumnTypeSetup> filterSetColumnTypesDataProvider(SetColumnTypeSetup setup)
    {
        return switch ("%s -> %s".formatted(setup.sourceColumnType(), setup.newColumnType())) {
            case "varchar(100) -> varchar(50)" -> Optional.of(setup.withNewColumnType("varchar"));
            case "char(25) -> char(20)",
                 "char(20) -> varchar",
                 "varchar -> char(20)" -> Optional.of(setup.withNewColumnType("varchar").withNewValueLiteral("rtrim(%s)".formatted(setup.newValueLiteral())));
            default -> Optional.of(setup);
        };
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("maximum length of identifier exceeded");
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageStartingWith("maximum length of identifier exceeded");
    }

    @Override
    protected void verifyColumnNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("maximum length of identifier exceeded");
    }

    @Test
    @Override // Override because the expected error message is different
    public void testNativeQueryCreateStatement()
    {
        assertThat(getQueryRunner().tableExists(getSession(), "numbers")).isFalse();
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'CREATE TABLE " + TPCH_SCHEMA + ".numbers(n INTEGER)'))"))
                .failure().hasMessageContaining("Query not supported: only a single SELECT statement is allowed");
        assertThat(getQueryRunner().tableExists(getSession(), "numbers")).isFalse();
    }

    @Test
    @Override // Override because the expected error message is different
    public void testNativeQueryInsertStatementTableDoesNotExist()
    {
        assertThat(getQueryRunner().tableExists(getSession(), "non_existent_table")).isFalse();
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'INSERT INTO non_existent_table VALUES (1)'))"))
                .failure().hasMessageContaining("Query not supported: only a single SELECT statement is allowed");
    }

    @Test
    @Override // Override because the expected error message is different
    public void testNativeQueryInsertStatementTableExists()
    {
        skipTestUnless(hasBehavior(SUPPORTS_NATIVE_QUERY));
        try (TestTable testTable = simpleTable()) {
            assertThat(query("SELECT * FROM TABLE(system.query(query => 'INSERT INTO %s VALUES (3)'))".formatted(testTable.getName())))
                    .failure().hasMessageContaining("Query not supported: only a single SELECT statement is allowed");
            assertQuery("SELECT * FROM " + testTable.getName(), "VALUES 1, 2");
        }
    }

    @Test
    @Override // Override because the expected error message is different
    public void testNativeQueryIncorrectSyntax()
    {
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'some wrong syntax'))"))
                .failure().hasMessageContaining("Query not supported: only a single SELECT statement is allowed");
    }

    @Test
    void testNativeQueryMultipleStatements()
    {
        String tableName = "test_native_query_multiple_statements" + randomNameSuffix();
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'CREATE TABLE %s.%s AS SELECT 1 x; SELECT 1'))".formatted(TPCH_SCHEMA, tableName)))
                .failure().hasMessageContaining("Query not supported: only a single SELECT statement is allowed");
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'SELECT 1; SELECT 2'))"))
                .failure().hasMessageContaining("Query not supported: only a single SELECT statement is allowed");
        assertThat(query("SELECT * FROM TABLE(system.query(query => ''))"))
                .failure().hasMessageContaining("Query not supported: only a single SELECT statement is allowed");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
    }

    @Test
    void testNativeQueryCannotAccessHost()
    {
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'SELECT * FROM read_text(''/etc/passwd'')'))"))
                .failure().hasMessageContaining("file system operations are disabled by configuration");
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'SELECT * FROM read_csv(''/etc/passwd'')'))"))
                .failure().hasMessageContaining("file system operations are disabled by configuration");
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'SELECT * FROM read_csv(''https://example.com/data.csv'')'))"))
                .failure().hasMessageContaining("file system operations are disabled by configuration");
        for (Entry<String, String> setting : SECURITY_CONNECTION_PROPERTIES.entrySet()) {
            assertThat(query("SELECT * FROM TABLE(system.query(query => 'SELECT current_setting(''%s'')'))".formatted(setting.getKey())))
                    .matches("VALUES " + setting.getValue());
        }
    }

    @Test
    void testExecuteProcedureCannotAccessHost(@TempDir Path directory)
    {
        String tableName = "test_execute_read_file" + randomNameSuffix();
        assertQueryFails("CALL system.execute('CREATE TABLE %s.%s AS SELECT * FROM read_text(''/etc/passwd'')')".formatted(TPCH_SCHEMA, tableName), "(?s).*file system operations are disabled by configuration.*");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertQueryFails("CALL system.execute('CREATE TABLE %s.%s AS SELECT * FROM read_csv(''https://example.com/data.csv'')')".formatted(TPCH_SCHEMA, tableName), "(?s).*file system operations are disabled by configuration.*");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();

        Path exportFile = directory.resolve("export.csv");
        assertQueryFails("CALL system.execute('COPY (SELECT 1) TO ''%s''')".formatted(exportFile), "(?s).*file system operations are disabled by configuration.*");
        assertThat(exportFile).doesNotExist();

        Path attachFile = directory.resolve("attached.db");
        assertQueryFails("CALL system.execute('ATTACH ''%s'' AS attached')".formatted(attachFile), "(?s).*file system operations are disabled by configuration.*");
        assertThat(attachFile).doesNotExist();

        assertQueryFails("CALL system.execute('INSTALL httpfs')", "(?s).*disabled.*");
        assertQueryFails("CALL system.execute('LOAD httpfs')", "(?s).*Loading external extensions is disabled through configuration.*");
        assertQueryFails("CALL system.execute('SET enable_external_access = true')", "(?s).*the configuration has been locked.*");
        assertQueryFails("CALL system.execute('RESET enable_external_access')", "(?s).*the configuration has been locked.*");
        assertQueryFails("CALL system.execute('SET lock_configuration = false')", "(?s).*the configuration has been locked.*");
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
    @Override // Override because char type is an alias of varchar in DuckDB
    public void testCharVarcharComparison()
    {
        // with char->varchar coercion on table creation, this is essentially varchar/varchar comparison
        try (TestTable table = newTrinoTable(
                "test_char_varchar",
                "(k, v) AS VALUES" +
                        "   (-1, CAST(NULL AS CHAR(3))), " +
                        "   (3, CAST('   ' AS CHAR(3)))," +
                        "   (6, CAST('x  ' AS CHAR(3)))")) {
            // varchar of length shorter than column length
            assertThat(query("SELECT k, v FROM " + table.getName() + " WHERE v = CAST('  ' AS varchar(2))")).returnsEmptyResult();
            // varchar of length longer than column length
            assertThat(query("SELECT k, v FROM " + table.getName() + " WHERE v = CAST('    ' AS varchar(4))")).returnsEmptyResult();
            // value that's not all-spaces
            assertThat(query("SELECT k, v FROM " + table.getName() + " WHERE v = CAST('x ' AS varchar(2))")).returnsEmptyResult();
            // exact match
            assertQuery("SELECT k, v FROM " + table.getName() + " WHERE v = CAST('' AS varchar(3))", "VALUES (3, '')");
        }
    }

    @Test
    void testCustomUserAgent()
    {
        assertThat(query("SELECT filter(split(user_agent, ' '), value -> value LIKE 'trino%') FROM TABLE(system.query(query => 'SELECT user_agent FROM pragma_user_agent()'))"))
                .skippingTypesCheck()
                .matches("VALUES (ARRAY['trino/testversion'])");
    }

    @Test
    void testTemporaryTable()
    {
        String tableName = "test_temporary_table" + randomNameSuffix();
        onRemoteDatabase().execute("CREATE TEMP TABLE " + tableName + "(x int)");

        assertQueryFails("SELECT * FROM " + tableName, ".* Table 'duckdb.tpch.%s' does not exist".formatted(tableName));
        assertThatThrownBy(() -> onRemoteDatabase().execute("SELECT * FROM " + tableName))
                .hasStackTraceContaining("Table with name %s does not exist", tableName);
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return duckDb::execute;
    }
}
