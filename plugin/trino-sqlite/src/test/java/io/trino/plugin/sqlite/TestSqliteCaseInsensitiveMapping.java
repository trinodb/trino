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
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.base.mapping.testing.RuleBasedIdentifierMappingUtils.REFRESH_PERIOD_DURATION;
import static io.trino.plugin.base.mapping.testing.RuleBasedIdentifierMappingUtils.createRuleBasedIdentifierMappingFile;
import static io.trino.plugin.base.mapping.testing.RuleBasedIdentifierMappingUtils.updateRuleBasedIdentifierMappingFile;
import static io.trino.plugin.sqlite.SqliteQueryRunner.SCHEMA;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
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

    @Override // Override because SQLite does not support schema creation
    protected AutoCloseable withSchema(String schemaName)
    {
        return null;
    }

    protected AutoCloseable withTable1(String remoteSchemaName, String remoteTableName, String tableDefinition)
    {
        String quotedName = quoted(remoteSchemaName) + "." + quoted(remoteTableName);
        onRemoteDatabase().execute(format("CREATE TABLE %s %s", quotedName, tableDefinition));
        return null;
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
    @Override // Override because SQLite support only a 'main' schema
    public void testTableNameRuleMapping()
            throws Exception
    {
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
    @Override // Override because SQLite makes no distinction regarding the case on identifiers, even if they are quoted.
    public void testTableNameClashWithRuleMapping()
            throws Exception
    {
        assertThatThrownBy(this::tableNameClashWithRuleMapping)
                .hasMessageContaining("[SQLITE_ERROR] SQL error or missing database (table \"CaseSensitiveName\" already exists)");
    }

    private void tableNameClashWithRuleMapping()
            throws Exception
    {
        String schema = SCHEMA;
        List<TableMappingRule> tableMappingRules = ImmutableList.of(
                new TableMappingRule(schema, "casesensitivename", "casesensitivename_a"),
                new TableMappingRule(schema, "CaseSensitiveName", "casesensitivename_b"),
                new TableMappingRule(schema, "CASESENSITIVENAME", "casesensitivename_c"));
        updateRuleBasedIdentifierMappingFile(getMappingFile(), ImmutableList.of(), tableMappingRules);

        String[] nameVariants = {"casesensitivename", "CaseSensitiveName", "CASESENSITIVENAME"};
        assertThat(Stream.of(nameVariants)
                .map(name -> name.toLowerCase(ENGLISH))
                .collect(toImmutableSet()))
                .hasSize(1);

        for (int i = 0; i < nameVariants.length; i++) {
            for (int j = i + 1; j < nameVariants.length; j++) {
                String remoteTable = nameVariants[i];
                String otherRemoteTable = nameVariants[j];
                try (AutoCloseable ignore = withSchema(schema);
                        AutoCloseable ignore1 = withTable(schema, remoteTable, "(c varchar(5))");
                        AutoCloseable ignore2 = withTable(schema, otherRemoteTable, "(d varchar(5))")) {
                    String table = tableMappingRules.stream()
                            .filter(rule -> rule.getRemoteTable().equals(remoteTable))
                            .map(TableMappingRule::getMapping)
                            .collect(onlyElement());

                    assertThat(computeActual("SHOW TABLES FROM " + schema)
                            .getOnlyColumn()
                            .map(String.class::cast)
                            .filter(anObject -> anObject.startsWith("casesensitivename")))
                            .hasSize(2);
                    assertQuery("SHOW COLUMNS FROM " + schema + "." + table, "SELECT 'c', 'varchar(5)', '', ''");
                    assertUpdate("INSERT INTO " + schema + "." + table + " VALUES 'dane'", 1);
                    assertQuery("SELECT * FROM " + schema + "." + table, "VALUES 'dane'");
                }
            }
        }
    }

    @Test
    @Override
    public void testNonLowerCaseTableName()
            throws Exception
    {
        abort("Override because SQLite makes no distinction regarding the case on identifiers, even if they are quoted.");
    }
}
