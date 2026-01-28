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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.base.mapping.TableMappingRule;
import io.trino.plugin.jdbc.BaseCaseInsensitiveMappingTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.base.mapping.testing.RuleBasedIdentifierMappingUtils.REFRESH_PERIOD_DURATION;
import static io.trino.plugin.base.mapping.testing.RuleBasedIdentifierMappingUtils.createRuleBasedIdentifierMappingFile;
import static io.trino.plugin.base.mapping.testing.RuleBasedIdentifierMappingUtils.updateRuleBasedIdentifierMappingFile;
import static io.trino.plugin.sqlite.SqliteQueryRunner.SCHEMA;
import static io.trino.plugin.sqlite.SqliteTpchTables.copyAndIngestTpchDataFromSourceToTarget;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

// With case-insensitive-name-matching enabled colliding schema/table names are considered as errors.
// Some tests here create colliding names which can cause any other concurrent test to fail.
final class TestSqliteCaseInsensitiveMapping
        extends BaseCaseInsensitiveMappingTest
{
    private Path mappingFile;
    private TestingSqliteServer server;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        mappingFile = createRuleBasedIdentifierMappingFile();
        server = closeAfterClass(new TestingSqliteServer());
        return SqliteQueryRunner.builder(server)
                .addConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("case-insensitive-name-matching", "true")
                        .put("case-insensitive-name-matching.config-file", mappingFile.toFile().getAbsolutePath())
                        .put("case-insensitive-name-matching.config-file.refresh-period", REFRESH_PERIOD_DURATION.toString())
                        .buildOrThrow())
                .build();
    }

    @Override
    protected Path getMappingFile()
    {
        return requireNonNull(mappingFile, "mappingFile is null");
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return server::execute;
    }

    @Override
    protected AutoCloseable withSchema(String schemaName)
    {
        return null;
    }

    @Override
    protected AutoCloseable withTable(String remoteSchemaName, String remoteTableName, String tableDefinition)
    {
        String quotedName = quoted(remoteTableName);
        onRemoteDatabase().execute(format("CREATE TABLE %s %s", quotedName, tableDefinition));
        return () -> onRemoteDatabase().execute("DROP TABLE IF EXISTS " + quotedName);
    }

    @Override
    @Test
    public void testNonLowerCaseSchemaName()
    {
        abort("Sqlite connector only supports a dummy schema 'main'.");
    }

    @Override
    @Test
    public void testSchemaAndTableNameRuleMapping()
    {
        abort("Sqlite connector only supports a dummy schema 'main'.");
    }

    @Override
    @Test
    public void testSchemaNameClash()
    {
        abort("Sqlite connector only supports a dummy schema 'main'.");
    }

    @Override
    @Test
    public void testSchemaNameClashWithRuleMapping()
    {
        abort("Sqlite connector only supports a dummy schema 'main'.");
    }

    @Override
    @Test
    public void testSchemaNameRuleMapping()
    {
        abort("Sqlite connector only supports a dummy schema 'main'.");
    }

    @Override
    @Test
    public void testTableNameClash()
            throws Exception
    {
        // FIXME: SQLite makes no distinction regarding the case on identifiers, even if they are quoted.
        assertThatThrownBy(super::testTableNameClash)
                .hasMessageContaining("[SQLITE_ERROR] SQL error or missing database (table \"CaseSensitiveName\" already exists)");
    }

    @Test
    @Override
    public void testTableNameRuleMapping()
            throws Exception
    {
        // FIXME: This test can only success if the schema is main
        String schema = SCHEMA;
        updateRuleBasedIdentifierMappingFile(
                getMappingFile(),
                ImmutableList.of(),
                ImmutableList.of(new TableMappingRule(schema, "remote_table", "trino_table")));

        try (AutoCloseable ignore = withSchema(schema);
                AutoCloseable ignore1 = withTable(schema, "remote_table", "(c varchar(5))")) {
            assertThat(computeActual("SHOW TABLES FROM " + schema).getOnlyColumn())
                    .contains("trino_table");
            assertThat(query("SHOW COLUMNS FROM " + schema + ".trino_table")).result().projected("Column").onlyColumnAsSet()
                    .contains("c");
            // Sqlite does not yet support writing via Trino
            onRemoteDatabase().execute("INSERT INTO \"remote_table\" VALUES ('dane')");
            assertQuery("SELECT * FROM " + schema + ".trino_table", "VALUES 'dane'");
        }
    }

    @Test
    public void testTableNameRuleMapping2()
            throws Exception
    {
        updateRuleBasedIdentifierMappingFile(
                getMappingFile(),
                ImmutableList.of(),
                ImmutableList.of(new TableMappingRule("main", "remote_table", "trino_table")));

        copyAndIngestTpchDataFromSourceToTarget(getQueryRunner().execute("SELECT * FROM tpch." + TINY_SCHEMA_NAME + ".region"), this.server, "remote_table");

        assertThat(computeActual("SHOW TABLES FROM main").getOnlyColumn())
                .contains("trino_table");
        assertQuery("SELECT COUNT(1) FROM sqlite.main.trino_table", "VALUES 5");
        onRemoteDatabase().execute("DROP TABLE IF EXISTS remote_table");
    }

    @Test
    @Override
    public void testTableNameClashWithRuleMapping()
    {
        // FIXME: SQLite makes no distinction regarding the case on identifiers, even if they are quoted.
        assertThatThrownBy(super::testTableNameClashWithRuleMapping)
                .hasMessageContaining("[SQLITE_ERROR] SQL error or missing database (table \"CaseSensitiveName\" already exists)");
    }

    @Test
    @Override
    public void testNonLowerCaseTableName()
            throws Exception
    {
        // FIXME: This test can only success if the schema is main
        String schema = SCHEMA;
        try (AutoCloseable ignore1 = withSchema("SomeSchema");
                AutoCloseable ignore2 = withTable(
                        schema,
                        "NonLowerCaseTable",
                        "(" +
                                quoted("lower_case_name") + " varchar(1), " +
                                quoted("Mixed_Case_Name") + " varchar(1), " +
                                quoted("UPPER_CASE_NAME") + " varchar(1))")) {
            @Language("SQL") String sql;
            if (optionalFromDual().isPresent()) {
                sql = format("INSERT INTO %s SELECT 'a', 'b', 'c' %s", quoted("NonLowerCaseTable"), optionalFromDual().get());
            }
            else {
                sql = format("INSERT INTO %s VALUES('a', 'b', 'c')", quoted("NonLowerCaseTable"));
            }
            // Sqlite does not yet support writing via Trino
            onRemoteDatabase().execute(sql);
            assertQuery(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name = 'nonlowercasetable'",
                    "VALUES 'lower_case_name', 'mixed_case_name', 'upper_case_name'");
            assertQuery(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = 'nonlowercasetable'",
                    "VALUES 'lower_case_name', 'mixed_case_name', 'upper_case_name'");
            assertThat(computeActual("SHOW COLUMNS FROM " + schema + ".nonlowercasetable").getMaterializedRows().stream()
                    .map(row -> row.getField(0))
                    .collect(toImmutableSet()))
                    .containsOnly("lower_case_name", "mixed_case_name", "upper_case_name");

            // Note: until https://github.com/prestodb/presto/issues/2863 is resolved, this is *the* way to access the tables.

            assertQuery("SELECT lower_case_name FROM " + schema + ".nonlowercasetable", "VALUES 'a'");
            assertQuery("SELECT mixed_case_name FROM " + schema + ".nonlowercasetable", "VALUES 'b'");
            assertQuery("SELECT upper_case_name FROM " + schema + ".nonlowercasetable", "VALUES 'c'");
            assertQuery("SELECT upper_case_name FROM " + schema + ".NonLowerCaseTable", "VALUES 'c'");
            assertQuery("SELECT upper_case_name FROM \"" + schema + "\".\"NonLowerCaseTable\"", "VALUES 'c'");

            // Sqlite does not yet support writing via Trino
            onRemoteDatabase().execute("INSERT INTO \"NonLowerCaseTable\" (\"lower_case_name\") VALUES ('l')");
            onRemoteDatabase().execute("INSERT INTO \"NonLowerCaseTable\" (\"Mixed_Case_Name\") VALUES ('m')");
            onRemoteDatabase().execute("INSERT INTO \"NonLowerCaseTable\" (\"UPPER_CASE_NAME\") VALUES ('u')");
            assertQuery(
                    "SELECT * FROM " + schema + ".nonlowercasetable",
                    "VALUES ('a', 'b', 'c')," +
                            "('l', NULL, NULL)," +
                            "(NULL, 'm', NULL)," +
                            "(NULL, NULL, 'u')");
        }
    }
}
