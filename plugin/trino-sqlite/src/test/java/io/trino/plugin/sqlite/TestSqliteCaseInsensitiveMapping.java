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

import static io.trino.plugin.base.mapping.testing.RuleBasedIdentifierMappingUtils.REFRESH_PERIOD_DURATION;
import static io.trino.plugin.base.mapping.testing.RuleBasedIdentifierMappingUtils.createRuleBasedIdentifierMappingFile;
import static io.trino.plugin.base.mapping.testing.RuleBasedIdentifierMappingUtils.updateRuleBasedIdentifierMappingFile;
import static io.trino.plugin.sqlite.SqliteQueryRunner.SCHEMA;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
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
    protected String canonicalize(String value)
    {
        return value.toLowerCase(ENGLISH);
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

    @Test
    @Override
    public void testSchemaNameRuleMapping()
    {
        abort("Sqlite connector only supports a dummy schema 'main'.");
    }

    @Test
    @Override
    public void testTableNameClash()
            throws Exception
    {
        // FIXME: SQLite makes no distinction regarding the case on identifiers, even if they are quoted.
        abort("Sqlite connector must be able to compare table name before insertion.");
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
    @Override
    public void testTableNameClashWithRuleMapping()
            throws Exception
    {
        // FIXME: SQLite makes no distinction regarding the case on identifiers, even if they are quoted.
        abort("Sqlite connector must be able to compare table name before insertion.");
    }

    @Test
    @Override
    public void testNonLowerCaseTableName()
            throws Exception
    {
        // FIXME: SQLite makes no distinction regarding the case on identifiers, even if they are quoted.
        abort("Sqlite connector must be able to compare table name before insertion.");
    }
}
