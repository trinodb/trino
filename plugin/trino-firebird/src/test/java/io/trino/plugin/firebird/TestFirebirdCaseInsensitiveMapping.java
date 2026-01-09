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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.base.mapping.TableMappingRule;
import io.trino.plugin.jdbc.BaseCaseInsensitiveMappingTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.base.mapping.testing.RuleBasedIdentifierMappingUtils.REFRESH_PERIOD_DURATION;
import static io.trino.plugin.base.mapping.testing.RuleBasedIdentifierMappingUtils.createRuleBasedIdentifierMappingFile;
import static io.trino.plugin.base.mapping.testing.RuleBasedIdentifierMappingUtils.updateRuleBasedIdentifierMappingFile;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

// With case-insensitive-name-matching enabled colliding schema/table names are considered as errors.
// Some tests here create colliding names which can cause any other concurrent test to fail.
final class TestFirebirdCaseInsensitiveMapping
        extends BaseCaseInsensitiveMappingTest
{
    private Path mappingFile;
    private TestingFirebirdServer server;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        mappingFile = createRuleBasedIdentifierMappingFile();
        server = closeAfterClass(new TestingFirebirdServer());
        return FirebirdQueryRunner.builder(server)
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
        return () -> {};
    }

    @Override
    protected AutoCloseable withTable(String remoteSchemaName, String remoteTableName, String tableDefinition)
    {
        // FIXME: testTableNameClash() creates a table with a “public” schema which in this specific case should not be quoted
        String quotedName = quoted(remoteTableName);
        onRemoteDatabase().execute(format("CREATE TABLE %s %s", quotedName, tableDefinition));
        return () -> onRemoteDatabase().execute("DROP TABLE " + quotedName);
    }

    @Override
    @Test
    public void testNonLowerCaseSchemaName()
    {
        abort("Firebird connector only supports schema 'default'.");
    }

    @Override
    @Test
    public void testSchemaAndTableNameRuleMapping()
    {
        abort("Firebird connector only supports schema 'default'.");
    }

    @Override
    @Test
    public void testSchemaNameClash()
    {
        abort("Firebird connector only supports schema 'default'.");
    }

    @Override
    @Test
    public void testSchemaNameClashWithRuleMapping()
    {
        abort("Firebird connector only supports schema 'default'.");
    }

    @Override
    @Test
    public void testSchemaNameRuleMapping()
    {
        abort("Firebird connector only supports schema 'default'.");
    }

    @Override
    @Test
    public void testTableNameClash()
            throws Exception
    {
        updateRuleBasedIdentifierMappingFile(getMappingFile(), ImmutableList.of(), ImmutableList.of());

        String[] nameVariants = {"casesensitivename", "CaseSensitiveName", "CASESENSITIVENAME"};
        assertThat(Stream.of(nameVariants)
                .map(name -> name.toLowerCase(ENGLISH))
                .collect(toImmutableSet()))
                .hasSize(1);

        for (int i = 0; i < nameVariants.length; i++) {
            for (int j = i + 1; j < nameVariants.length; j++) {
                try (AutoCloseable ignore1 = withTable(nameVariants[i], "(c varchar(5))");
                        AutoCloseable ignore2 = withTable(nameVariants[j], "(d varchar(5))");
                        AutoCloseable ignore3 = withTable("some_table", "(d varchar(5))")) {
                    assertThat(computeActual("SHOW TABLES FROM default").getOnlyColumn().filter("casesensitivename"::equals)).hasSize(1); // TODO, should be 2
                    assertQueryFails("SHOW COLUMNS FROM casesensitivename", "Failed to find remote table name: Ambiguous name: casesensitivename");
                    assertQueryFails("SELECT * FROM casesensitivename", "Failed to find remote table name: Ambiguous name: casesensitivename");
                    assertQuery("SHOW COLUMNS FROM some_table", "SELECT 'd', 'varchar(5)', '', ''");
                    assertQueryReturnsEmptyResult("SELECT * FROM some_table");
                }
            }
        }
    }

    @Test
    @Override
    public void testTableNameRuleMapping()
            throws Exception
    {
        String schema = "default";
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
            // Firebird does not yet support writing via Trino
            onRemoteDatabase().execute("INSERT INTO \"remote_table\" VALUES ('dane')");
            assertQuery("SELECT * FROM " + schema + ".trino_table", "VALUES 'dane'");
        }
    }

    @Test
    @Override
    public void testTableNameClashWithRuleMapping()
            throws Exception
    {
        String schema = "default";
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

                    assertThat(computeActual("SHOW TABLES FROM default")
                            .getOnlyColumn()
                            .map(String.class::cast)
                            .filter(anObject -> anObject.startsWith("casesensitivename")))
                            .hasSize(2);
                    assertQuery("SHOW COLUMNS FROM " + schema + "." + table, "SELECT 'c', 'varchar(5)', '', ''");
                    // Firebird does not yet support writing via Trino
                    onRemoteDatabase().execute("INSERT INTO \"" + remoteTable + "\" VALUES ('dane')");
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
        try (AutoCloseable ignore1 = withSchema("SomeSchema");
                AutoCloseable ignore2 = withTable(
                        "default",
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
            // Firebird does not yet support writing via Trino
            onRemoteDatabase().execute(sql);
            assertQuery(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema = 'default' AND table_name = 'nonlowercasetable'",
                    "VALUES 'lower_case_name', 'mixed_case_name', 'upper_case_name'");
            assertQuery(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = 'nonlowercasetable'",
                    "VALUES 'lower_case_name', 'mixed_case_name', 'upper_case_name'");
            assertThat(computeActual("SHOW COLUMNS FROM default.nonlowercasetable").getMaterializedRows().stream()
                    .map(row -> row.getField(0))
                    .collect(toImmutableSet()))
                    .containsOnly("lower_case_name", "mixed_case_name", "upper_case_name");

            // Note: until https://github.com/prestodb/presto/issues/2863 is resolved, this is *the* way to access the tables.

            assertQuery("SELECT lower_case_name FROM default.nonlowercasetable", "VALUES 'a'");
            assertQuery("SELECT mixed_case_name FROM default.nonlowercasetable", "VALUES 'b'");
            assertQuery("SELECT upper_case_name FROM default.nonlowercasetable", "VALUES 'c'");
            assertQuery("SELECT upper_case_name FROM default.NonLowerCaseTable", "VALUES 'c'");
            assertQuery("SELECT upper_case_name FROM \"default\".\"NonLowerCaseTable\"", "VALUES 'c'");

            // Firebird does not yet support writing via Trino
            onRemoteDatabase().execute("INSERT INTO \"NonLowerCaseTable\" (\"lower_case_name\") VALUES ('l')");
            onRemoteDatabase().execute("INSERT INTO \"NonLowerCaseTable\" (\"Mixed_Case_Name\") VALUES ('m')");
            onRemoteDatabase().execute("INSERT INTO \"NonLowerCaseTable\" (\"UPPER_CASE_NAME\") VALUES ('u')");
            assertQuery(
                    "SELECT * FROM default.nonlowercasetable",
                    "VALUES ('a', 'b', 'c')," +
                            "('l', NULL, NULL)," +
                            "(NULL, 'm', NULL)," +
                            "(NULL, NULL, 'u')");
        }
    }

    private AutoCloseable withTable(String remoteTableName, String definition)
    {
        return withTable(getSession().getSchema().orElseThrow(), remoteTableName, definition);
    }
}
