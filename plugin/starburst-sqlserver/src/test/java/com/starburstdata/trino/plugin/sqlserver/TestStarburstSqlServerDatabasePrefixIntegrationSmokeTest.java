/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.sqlserver;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.sqlserver.TestingSqlServer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import static com.starburstdata.trino.plugin.sqlserver.StarburstSqlServerMultiDatabaseClient.DATABASE_SEPARATOR;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.QueryAssertions.assertContains;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

public class TestStarburstSqlServerDatabasePrefixIntegrationSmokeTest
        extends AbstractTestQueryFramework
{
    protected TestingSqlServer sqlServer;
    protected String sqlServerDatabaseName;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.sqlServer = closeAfterClass(new TestingSqlServer());
        sqlServerDatabaseName = sqlServer.getDatabaseName().toLowerCase(ENGLISH);
        return StarburstSqlServerQueryRunner.builder(sqlServer)
                .withConnectorProperties(ImmutableMap.of(
                        "sqlserver.database-prefix-for-schema.enabled", "true"))
                .build();
    }

    @AfterAll
    public void cleanup()
    {
        sqlServer = null;
    }

    @Test
    public void testDropSchema()
    {
        String schemaName = "test_schema_drop_" + randomNameSuffix();
        String databaseSchemaName = sqlServerDatabaseName + "." + schemaName;
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).doesNotContain(databaseSchemaName);
        assertUpdate(format("CREATE SCHEMA \"%s\"", databaseSchemaName));

        assertUpdate(format("DROP SCHEMA \"%s\"", databaseSchemaName));

        // verify listing of new schema
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).doesNotContain(databaseSchemaName);
    }

    @Test
    public void testDropSchemaCascade()
    {
        String schemaName = "test_schema_drop_" + randomNameSuffix();
        String databaseSchemaName = sqlServerDatabaseName + "." + schemaName;
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).doesNotContain(databaseSchemaName);
        assertUpdate(format("CREATE SCHEMA \"%s\"", databaseSchemaName));

        try (TestTable table = new TestTable(getQueryRunner()::execute, databaseSchemaTableName(sqlServerDatabaseName, schemaName, "test_table_for_create"), "(a VARCHAR(25))", ImmutableList.of("'test-table'"))) {
            String tableName = table.getName().split("\\.")[2];
            assertThat(computeActual(format("SHOW TABLES FROM \"%s\"", databaseSchemaName)).getOnlyColumnAsSet())
                    .contains(tableName);

            assertQueryFails(format("DROP SCHEMA \"%s\" CASCADE", databaseSchemaName),
                    "This connector does not support dropping schemas with CASCADE option");

            // verify listing of new schema
            assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains(databaseSchemaName);
        }
        finally {
            assertUpdate(format("DROP SCHEMA \"%s\"", databaseSchemaName));
        }
    }

    @Test
    public void testCreateSchema()
    {
        String schemaName = format("%s.test_schema_create_%s", sqlServerDatabaseName, randomNameSuffix());
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
        // try to rename schema
        assertQueryFails(
                format("ALTER SCHEMA \"%s\" RENAME TO renamed_schema", schemaName),
                "This connector does not support renaming schemas");

        // cleanup
        assertUpdate(format("DROP SCHEMA \"%s\"", schemaName));

        // verify DROP SCHEMA for non-existing schema
        assertQueryFails(format("DROP SCHEMA \"%s\"", schemaName), format("line 1:1: Schema '.*\\.%s' does not exist", schemaName));
        assertUpdate(format("DROP SCHEMA IF EXISTS \"%s\"", schemaName));
    }

    @Test
    public void testShowSchemas()
    {
        MaterializedResult actualSchemas = computeActual(format("SHOW SCHEMAS LIKE '%s%%'", sqlServerDatabaseName)).toTestTypes();

        // Expect at least one schema dbo
        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(getSession(), VARCHAR)
                .row(sqlServerDatabaseName + ".dbo");

        assertContains(actualSchemas, resultBuilder.build());
    }

    @Test
    public void testInformationSchemataTable()
    {
        String availableSchema = sqlServerDatabaseName + ".dbo";
        assertQuery(
                format("SELECT schema_name FROM information_schema.schemata WHERE schema_name = '%s'", availableSchema),
                format("VALUES '%s'", availableSchema));
    }

    @Test
    public void testSystemTablesJdbc()
    {
        String availableSchema = sqlServerDatabaseName + ".dbo";
        assertQuery(
                format("SELECT table_schem FROM system.jdbc.schemas WHERE table_schem = '%s'", availableSchema),
                format("VALUES '%s'", availableSchema));
    }

    @Test
    public void testOnSchemaWithSingleIdentifier()
    {
        assertThat(query("CREATE SCHEMA test_schema_create_"))
                .failure().hasMessageMatching("The expected format is '<database name>.<schema name>': .*");

        assertThat(query("CREATE TABLE test_schema_create_.unknown_table (columna BIGINT)"))
                .failure().hasMessageMatching(".*The expected format is '<database name>.<schema name>': .*");

        assertThat(query("SELECT * FROM test_schema_create_.unknown_table"))
                .failure().hasMessageMatching(".*The expected format is '<database name>.<schema name>': .*");
    }

    @Test
    public void testOnSchemaWithMultipleIdentifier()
    {
        assertThat(query("CREATE SCHEMA \"test_schema_create.part_1.part_2\""))
                .failure().hasMessage("Too many identifier parts found");

        assertThat(query("CREATE TABLE \"test_schema_create.part_1.part_2\".unknown_table (columna BIGINT)"))
                .failure().hasMessageMatching(".*Too many identifier parts found");

        assertThat(query("SELECT * FROM \"test_schema_create.part_1.part_2\".unknown_table"))
                .failure().hasMessageMatching(".*Too many identifier parts found");
    }

    @Test
    public void testCreateTable()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, databaseSchemaTableName(sqlServerDatabaseName, "dbo", "test_table_for_create"), "(a VARCHAR(25))", ImmutableList.of("'test-table'"))) {
            String tableName = table.getName().split("\\.")[2];
            assertThat(computeActual(format("SHOW TABLES FROM \"%s.dbo\"", sqlServerDatabaseName)).getOnlyColumnAsSet())
                    .contains(tableName);
            // try to create duplicate table
            assertQueryFails(format("CREATE TABLE %s (columnB BIGINT)", table.getName()), format("line 1:1: Table 'sqlserver.\"%s.dbo\".%s' already exists", sqlServerDatabaseName, tableName));
        }
    }

    @Test
    public void testCreateAsSelect()
    {
        try (TestTable table = new TestTable(sqlServer::execute, "dbo.base_table", "(a VARCHAR(25))", ImmutableList.of("'value-1'"))) {
            String tableName = table.getName().split("\\.")[1];
            String newTableName = "create_as_select" + randomNameSuffix();
            assertUpdate(
                    format(
                            "CREATE TABLE %s AS SELECT * FROM %s",
                            databaseSchemaTableName(sqlServerDatabaseName, "dbo", newTableName),
                            databaseSchemaTableName(sqlServerDatabaseName, "dbo", tableName)),
                    1);
            assertQuery("SELECT * FROM " + databaseSchemaTableName(sqlServerDatabaseName, "dbo", newTableName), "VALUES 'value-1'");
        }
    }

    @Test
    public void testSelectTable()
    {
        try (TestTable table = new TestTable(sqlServer::execute, "dbo.test_table_for_select", "(a VARCHAR(25))", ImmutableList.of("'test-table'"))) {
            String tableName = table.getName().split("\\.")[1];
            assertQuery("SELECT * FROM " + databaseSchemaTableName(sqlServerDatabaseName, "dbo", tableName), "VALUES 'test-table'");
        }
    }

    @Test
    public void testCreateWithDotAsSelect()
    {
        String baseTable = databaseSchemaTableName(sqlServerDatabaseName, "dbo", "\"base_table.with_dot_" + randomNameSuffix() + "\"");
        String newTableName = databaseSchemaTableName(sqlServerDatabaseName, "dbo", "\"create_as_select.with_dot_" + randomNameSuffix() + "\"");

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
    public void testInsertTable()
    {
        try (TestTable table = new TestTable(sqlServer::execute, "dbo.test_table_for_insert", "(a VARCHAR(25))", ImmutableList.of("'value-1'"))) {
            String tableName = table.getName().split("\\.")[1];
            assertUpdate(format("INSERT INTO %s (a) VALUES ('value-2')", databaseSchemaTableName(sqlServerDatabaseName, "dbo", tableName)), 1L);
            assertQuery("SELECT * FROM " + databaseSchemaTableName(sqlServerDatabaseName, "dbo", tableName), "VALUES 'value-1', 'value-2'");
        }
    }

    @Test
    public void testDropTable()
    {
        TestTable table = new TestTable(sqlServer::execute, "dbo.test_table_for_drop", "(a VARCHAR(25))");
        String tableName = table.getName().split("\\.")[1];
        assertThat(computeActual(format("SHOW TABLES FROM \"%s.dbo\"", sqlServerDatabaseName)).getOnlyColumnAsSet())
                .contains(tableName);
        assertQuerySucceeds(
                format("DROP TABLE %s", databaseSchemaTableName(sqlServerDatabaseName, "dbo", tableName)));
        assertThat(computeActual(format("SHOW TABLES FROM \"%s.dbo\"", sqlServerDatabaseName)).getOnlyColumnAsSet())
                .doesNotContain(tableName);
    }

    @Test
    public void testRenameTable()
    {
        TestTable table = new TestTable(sqlServer::execute, "dbo.test_table_for_drop", "(a VARCHAR(25))");
        String tableName = table.getName().split("\\.")[1];
        String renameTable = "renamed_table";
        assertThat(computeActual(format("SHOW TABLES FROM \"%s.dbo\"", sqlServerDatabaseName)).getOnlyColumnAsSet())
                .contains(tableName)
                .doesNotContain(renameTable);
        assertQuerySucceeds(
                format(
                        "ALTER TABLE %s RENAME TO %s",
                        databaseSchemaTableName(sqlServerDatabaseName, "dbo", tableName),
                        databaseSchemaTableName(sqlServerDatabaseName, "dbo", renameTable)));
        assertThat(computeActual(format("SHOW TABLES FROM \"%s.dbo\"", sqlServerDatabaseName)).getOnlyColumnAsSet())
                .doesNotContain(tableName)
                .contains(renameTable);
        assertQueryFails(
                format(
                        "ALTER TABLE %s RENAME TO %s",
                        databaseSchemaTableName(sqlServerDatabaseName, "dbo", renameTable),
                        databaseSchemaTableName(sqlServerDatabaseName, "rename_schema", renameTable)),
                "This connector does not support renaming tables across schemas");
        assertQuerySucceeds(
                format("DROP TABLE %s", databaseSchemaTableName(sqlServerDatabaseName, "dbo", renameTable)));
    }

    @Test
    public void testAddColumn()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, databaseSchemaTableName(sqlServerDatabaseName, "dbo", "test_table_for_add"), "(x VARCHAR)")) {
            String tableName = table.getName();
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
    public void testRenameColumn()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, databaseSchemaTableName(sqlServerDatabaseName, "dbo", "test_table_for_add_column"), "(x VARCHAR)")) {
            String tableName = table.getName();
            assertUpdate("INSERT INTO " + tableName + " (x) VALUES ('some value')", 1L);
            assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN x TO before_y");
            assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN IF EXISTS before_y TO y");
            assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN IF EXISTS columnNotExists TO y");
            assertQuery("SELECT y FROM " + tableName, "VALUES 'some value'");

            assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN y TO Z"); // 'Z' is upper-case, not delimited
            assertQuery(
                    "SELECT z FROM " + tableName, // 'z' is lower-case, not delimited
                    "VALUES 'some value'");

            assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN IF EXISTS z TO a");
            assertQuery(
                    "SELECT a FROM " + tableName,
                    "VALUES 'some value'");

            // There should be exactly one column
            assertQuery("SELECT * FROM " + tableName, "VALUES 'some value'");
        }
    }

    @Test
    public void testDropColumn()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, databaseSchemaTableName(sqlServerDatabaseName, "dbo", "test_drop_column"), "(x VARCHAR, y VARCHAR, a VARCHAR)")) {
            String tableName = table.getName();
            assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN x");
            assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN IF EXISTS y");
            assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN IF EXISTS notExistColumn");
            assertQueryFails("SELECT x FROM " + tableName, ".* Column 'x' cannot be resolved");
            assertQueryFails("SELECT y FROM " + tableName, ".* Column 'y' cannot be resolved");

            assertQueryFails("ALTER TABLE " + tableName + " DROP COLUMN a", ".* Cannot drop the only column in a table");
        }
    }

    @Test
    public void testShowCreateTable()
    {
        try (TestTable table = new TestTable(sqlServer::execute, "dbo.test_table_for_show_create", "(a VARCHAR(25))")) {
            String tableName = table.getName().split("\\.")[1];
            assertThat((String) computeActual("SHOW CREATE TABLE " + databaseSchemaTableName(sqlServerDatabaseName, "dbo", tableName)).getOnlyValue())
                    .isEqualTo(format("""
                                    CREATE TABLE sqlserver."%s.dbo".%s (
                                       a varchar(25)
                                    )
                                    WITH (
                                       data_compression = 'NONE'
                                    )""",
                            sqlServerDatabaseName,
                            tableName));
        }
    }

    private String databaseSchemaTableName(String databaseName, String schemaName, String tableName)
    {
        return format("\"%s\".%s", Joiner.on(DATABASE_SEPARATOR).join(databaseName, schemaName), tableName);
    }
}
