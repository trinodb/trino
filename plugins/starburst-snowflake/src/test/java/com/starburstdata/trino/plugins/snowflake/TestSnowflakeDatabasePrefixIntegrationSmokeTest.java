/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.testng.services.ManageTestResources;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.jdbcBuilder;
import static com.starburstdata.trino.plugins.snowflake.jdbc.SnowflakeClient.DATABASE_SEPARATOR;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.QueryAssertions.assertContains;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSnowflakeDatabasePrefixIntegrationSmokeTest
        extends AbstractTestQueryFramework
{
    @ManageTestResources.Suppress(because = "Mock to remote server")
    private SnowflakeServer server;
    private TestDatabase testDatabase;
    private String normalizedDatabaseName;
    private final SqlExecutor snowflakeExecutor = (sql) -> server.safeExecuteOnDatabase(testDatabase.getName(), sql);

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = new SnowflakeServer();
        testDatabase = closeAfterClass(server.createTestDatabase());
        normalizedDatabaseName = testDatabase.getName().toLowerCase(ENGLISH);

        return createBuilder()
                .withServer(server)
                .withConnectorProperties(ImmutableMap.of(
                        "snowflake.database-prefix-for-schema.enabled", "true",
                        "snowflake.role", "test_role"))
                .build();
    }

    protected SnowflakeQueryRunner.Builder createBuilder()
    {
        return jdbcBuilder();
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
    {
        server = null;
        testDatabase = null;
    }

    @Test
    public void testCreateSchema()
    {
        String schemaName = format("%s.test_schema_create_%s", normalizedDatabaseName, randomNameSuffix());
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).doesNotContain(schemaName);
        assertUpdate(format("CREATE SCHEMA \"%s\"", schemaName));
        assertUpdate(format("CREATE SCHEMA IF NOT EXISTS \"%s\"", schemaName));

        // verify listing of new schema
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains(schemaName);

        // verify SHOW CREATE SCHEMA works
        assertThat((String) computeScalar("SHOW CREATE SCHEMA \"" + schemaName + "\""))
                .startsWith(format("CREATE SCHEMA %s.\"%s\"", getSession().getCatalog().orElseThrow(), schemaName));

        // try to create duplicate schema
        assertQueryFails(format("CREATE SCHEMA \"%s\"", schemaName), format("line 1:1: Schema '.*\\.%s' already exists", schemaName));

        // cleanup
        assertUpdate(format("DROP SCHEMA \"%s\"", schemaName));

        // verify DROP SCHEMA for non-existing schema
        assertQueryFails(format("DROP SCHEMA \"%s\"", schemaName), format("line 1:1: Schema '.*\\.%s' does not exist", schemaName));
        assertUpdate(format("DROP SCHEMA IF EXISTS \"%s\"", schemaName));
    }

    @Test
    public void testShowSchemas()
    {
        MaterializedResult actualSchemas = computeActual(format("SHOW SCHEMAS LIKE '%s%%'", normalizedDatabaseName)).toTestTypes();

        // Expect at least one schema public
        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(getSession(), VARCHAR)
                .row(normalizedDatabaseName + ".public");

        assertContains(actualSchemas, resultBuilder.build());
    }

    @Test
    public void testInformationSchemataTable()
    {
        String availableSchema = normalizedDatabaseName + ".public";
        assertQuery(
                format("SELECT schema_name FROM information_schema.schemata WHERE schema_name = '%s'", availableSchema),
                format("VALUES '%s'", availableSchema));
    }

    @Test
    public void testSystemTablesJdbc()
    {
        String availableSchema = normalizedDatabaseName + ".public";
        assertQuery(
                format("SELECT table_schem FROM system.jdbc.schemas WHERE table_schem = '%s'", availableSchema),
                format("VALUES '%s'", availableSchema));
    }

    @Test
    public void testOnSchemaWithSingleIdentifier()
    {
        assertThatThrownBy(() -> query("CREATE SCHEMA test_schema_create_"))
                .hasMessageMatching("The expected format is '<database name>.<schema name>': .*");

        assertThatThrownBy(() -> query("CREATE TABLE test_schema_create_.unknown_table (columna BIGINT)"))
                .hasMessageMatching(".*The expected format is '<database name>.<schema name>': .*");

        assertThatThrownBy(() -> query("SELECT * FROM test_schema_create_.unknown_table"))
                .hasMessageMatching(".*The expected format is '<database name>.<schema name>': .*");
    }

    @Test
    public void testOnSchemaWithMultipleIdentifier()
    {
        assertThatThrownBy(() -> query("CREATE SCHEMA \"test_schema_create.part_1.part_2\""))
                .hasMessage("Too many identifier parts found");

        assertThatThrownBy(() -> query("CREATE TABLE \"test_schema_create.part_1.part_2\".unknown_table (columna BIGINT)"))
                .hasMessageMatching(".*Too many identifier parts found");

        assertThatThrownBy(() -> query("SELECT * FROM \"test_schema_create.part_1.part_2\".unknown_table"))
                .hasMessageMatching(".*Too many identifier parts found");
    }

    @Test
    public void testCreateTable()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, databaseSchemaTableName(normalizedDatabaseName, "public", "test_table_for_create"), "(a VARCHAR)", ImmutableList.of("'test-table'"))) {
            String tableName = table.getName().split("\\.")[2];
            assertThat(computeActual(format("SHOW TABLES FROM \"%s.public\"", normalizedDatabaseName)).getOnlyColumnAsSet())
                    .contains(tableName);
            // try to create duplicate table
            assertQueryFails(format("CREATE TABLE %s (columnB BIGINT)", table.getName()), format("line 1:1: Table 'snowflake.%s.public.%s' already exists", normalizedDatabaseName, tableName));
        }
    }

    @Test
    public void testAddColumn()
    {
        String tableName;
        try (TestTable table = new TestTable(getQueryRunner()::execute, databaseSchemaTableName(normalizedDatabaseName, "public", "test_table_for_add"), "(x VARCHAR)")) {
            tableName = table.getName();
            assertUpdate("INSERT INTO " + tableName + " SELECT 'first'", 1);
            assertQueryFails("ALTER TABLE " + tableName + " ADD COLUMN x bigint", ".* Column 'x' already exists");
            assertQueryFails("ALTER TABLE " + tableName + " ADD COLUMN X bigint", ".* Column 'X' already exists");
            assertQueryFails("ALTER TABLE " + tableName + " ADD COLUMN q bad_type", ".* Unknown type 'bad_type' for column 'q'");

            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN a varchar(50)");
            // Verify table state after adding a column, but before inserting anything to it
            assertQuery(
                    "SELECT * FROM " + tableName,
                    "VALUES ('first', NULL)");
            assertUpdate("INSERT INTO " + tableName + " SELECT 'second', 'xxx'", 1);
            assertQuery(
                    "SELECT x, a FROM " + tableName,
                    "VALUES ('first', NULL), ('second', 'xxx')");

            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN b double");
            assertUpdate("INSERT INTO " + tableName + " SELECT 'third', 'yyy', 33.3E0", 1);
            assertQuery(
                    "SELECT x, a, b FROM " + tableName,
                    "VALUES ('first', NULL, NULL), ('second', 'xxx', NULL), ('third', 'yyy', 33.3)");

            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN IF NOT EXISTS c varchar(50)");
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN IF NOT EXISTS c varchar(50)");
            assertUpdate("INSERT INTO " + tableName + " SELECT 'fourth', 'zzz', 55.3E0, 'newColumn'", 1);
            assertQuery(
                    "SELECT x, a, b, c FROM " + tableName,
                    "VALUES ('first', NULL, NULL, NULL), ('second', 'xxx', NULL, NULL), ('third', 'yyy', 33.3, NULL), ('fourth', 'zzz', 55.3, 'newColumn')");
        }
    }

    @Test
    public void testCreateAsSelect()
    {
        try (TestTable table = new TestTable(snowflakeExecutor, "public.base_table", "(a STRING)", ImmutableList.of("'value-1'"))) {
            String tableName = table.getName().split("\\.")[1];
            String newTableName = "create_as_select" + randomNameSuffix();
            assertUpdate(
                    format(
                            "CREATE TABLE %s AS SELECT * FROM %s",
                            databaseSchemaTableName(normalizedDatabaseName, "public", newTableName),
                            databaseSchemaTableName(normalizedDatabaseName, "public", tableName)),
                    1);
            assertQuery("SELECT * FROM " + databaseSchemaTableName(normalizedDatabaseName, "public", newTableName), "VALUES 'value-1'");
        }
    }

    @Test
    public void testCreateWithDotAsSelect()
    {
        String baseTable = databaseSchemaTableName(normalizedDatabaseName, "public", "\"base_table.with_dot_" + randomNameSuffix() + "\"");
        String newTableName = databaseSchemaTableName(normalizedDatabaseName, "public", "\"create_as_select.with_dot_" + randomNameSuffix() + "\"");

        try {
            assertUpdate(format("CREATE TABLE %s (column1 BIGINT)", baseTable));
            assertUpdate(format("CREATE TABLE %s AS SELECT * FROM %s", newTableName, baseTable), 0);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + newTableName);
            assertUpdate("DROP TABLE IF EXISTS " + baseTable);
        }
    }

    @Test
    public void testSelectTable()
    {
        try (TestTable table = new TestTable(snowflakeExecutor, "public.test_table_for_select", "(a STRING)", ImmutableList.of("'test-table'"))) {
            String tableName = table.getName().split("\\.")[1];
            assertQuery("SELECT * FROM " + databaseSchemaTableName(normalizedDatabaseName, "public", tableName), "VALUES 'test-table'");
        }
    }

    @Test
    public void testInsertTable()
    {
        try (TestTable table = new TestTable(snowflakeExecutor, "public.test_table_for_insert", "(a STRING)", ImmutableList.of("'value-1'"))) {
            String tableName = table.getName().split("\\.")[1];
            assertUpdate(format("INSERT INTO %s (a) VALUES ('value-2')", databaseSchemaTableName(normalizedDatabaseName, "public", tableName)), 1L);
            assertQuery("SELECT * FROM " + databaseSchemaTableName(normalizedDatabaseName, "public", tableName), "VALUES 'value-1', 'value-2'");
        }
    }

    @Test
    public void testShowCreateTable()
    {
        try (TestTable table = new TestTable(snowflakeExecutor, "public.test_table_for_show_create", "(a VARCHAR(3))")) {
            String tableName = table.getName().split("\\.")[1];
            assertThat((String) computeActual("SHOW CREATE TABLE " + databaseSchemaTableName(normalizedDatabaseName, "public", tableName)).getOnlyValue())
                    .isEqualTo(format("CREATE TABLE snowflake.\"%s.public\".%s (\n" +
                            "   a varchar(3)\n" +
                            ")",
                            normalizedDatabaseName,
                            tableName));
        }
    }

    private String databaseSchemaTableName(String databaseName, String schemaName, String tableName)
    {
        return format("\"%s\".%s", Joiner.on(DATABASE_SEPARATOR).join(databaseName, schemaName), tableName);
    }
}
