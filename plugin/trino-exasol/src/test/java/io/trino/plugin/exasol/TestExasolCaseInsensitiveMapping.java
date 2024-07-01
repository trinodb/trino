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
package io.trino.plugin.exasol;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.mapping.SchemaMappingRule;
import io.trino.plugin.base.mapping.TableMappingRule;
import io.trino.plugin.jdbc.BaseCaseInsensitiveMappingTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.base.mapping.RuleBasedIdentifierMappingUtils.REFRESH_PERIOD_DURATION;
import static io.trino.plugin.base.mapping.RuleBasedIdentifierMappingUtils.createRuleBasedIdentifierMappingFile;
import static io.trino.plugin.base.mapping.RuleBasedIdentifierMappingUtils.updateRuleBasedIdentifierMappingFile;
import static io.trino.plugin.exasol.TestingExasolServer.TEST_SCHEMA;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

@Isolated
final class TestExasolCaseInsensitiveMapping
        extends BaseCaseInsensitiveMappingTest
{
    private Path mappingFile;
    private TestingExasolServer exasolServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        mappingFile = createRuleBasedIdentifierMappingFile();
        exasolServer = closeAfterClass(new TestingExasolServer());
        return ExasolQueryRunner.builder(exasolServer)
                .addConnectorProperty("case-insensitive-name-matching", "true")
                .addConnectorProperty("case-insensitive-name-matching.config-file", mappingFile.toFile().getAbsolutePath())
                .addConnectorProperty("case-insensitive-name-matching.config-file.refresh-period", REFRESH_PERIOD_DURATION.toString())
                .build();
    }

    @Test
    @Override
    public void testTableNameRuleMapping()
            throws Exception
    {
        String schema = "remote_schema";
        updateRuleBasedIdentifierMappingFile(
                getMappingFile(),
                ImmutableList.of(),
                ImmutableList.of(new TableMappingRule(schema, "remote_table", "trino_table")));

        try (AutoCloseable ignore = withSchema(schema);
                AutoCloseable ignore1 = withTable(schema, "remote_table", "(c varchar(5))")) {
            assertThat(computeActual("SHOW TABLES FROM " + schema).getOnlyColumn())
                    .contains("trino_table");
            assertQuery("SHOW COLUMNS FROM " + schema + ".trino_table", "SELECT 'c', 'varchar(5)', '', ''");
            // Exasol does not support writing via Trino
            onRemoteDatabase().execute("INSERT INTO \"" + schema + "\".\"remote_table\" VALUES 'dane'");
            assertQuery("SELECT * FROM " + schema + ".trino_table", "VALUES 'dane'");
        }
    }

    @Test
    @Override
    public void testSchemaAndTableNameRuleMapping()
            throws Exception
    {
        updateRuleBasedIdentifierMappingFile(
                getMappingFile(),
                ImmutableList.of(new SchemaMappingRule("remote_schema", "trino_schema")),
                ImmutableList.of(new TableMappingRule("remote_schema", "remote_table", "trino_table")));

        try (AutoCloseable ignore1 = withSchema("remote_schema");
                AutoCloseable ignore2 = withTable("remote_schema", "remote_table", "(c varchar(5))")) {
            assertThat(computeActual("SHOW SCHEMAS").getOnlyColumn())
                    .contains("trino_schema");
            assertThat(computeActual("SHOW TABLES IN trino_schema").getOnlyColumn())
                    .contains("trino_table");
            assertQuery("SHOW COLUMNS FROM trino_schema.trino_table", "SELECT 'c', 'varchar(5)', '', ''");
            // Exasol does not support writing via Trino
            onRemoteDatabase().execute("INSERT INTO \"remote_schema\".\"remote_table\" VALUES 'dane'");
            assertQuery("SELECT * FROM trino_schema.trino_table", "VALUES 'dane'");
        }
    }

    @Test
    @Override
    public void testTableNameClashWithRuleMapping()
            throws Exception
    {
        String schema = "remote_schema";
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
                    // Exasol does not support writing via Trino
                    onRemoteDatabase().execute("INSERT INTO \"remote_schema\".\"" + remoteTable + "\" VALUES 'dane'");
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
                        "SomeSchema",
                        "NonLowerCaseTable",
                        "(" +
                                quoted("lower_case_name") + " varchar(1), " +
                                quoted("Mixed_Case_Name") + " varchar(1), " +
                                quoted("UPPER_CASE_NAME") + " varchar(1))")) {
            onRemoteDatabase().execute("INSERT INTO " + (quoted("SomeSchema") + "." + quoted("NonLowerCaseTable")) + " SELECT 'a', 'b', 'c'" + optionalFromDual().orElse(""));
            assertQuery(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema = 'someschema' AND table_name = 'nonlowercasetable'",
                    "VALUES 'lower_case_name', 'mixed_case_name', 'upper_case_name'");
            assertQuery(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = 'nonlowercasetable'",
                    "VALUES 'lower_case_name', 'mixed_case_name', 'upper_case_name'");
            assertThat(computeActual("SHOW COLUMNS FROM someschema.nonlowercasetable").getMaterializedRows().stream()
                    .map(row -> row.getField(0))
                    .collect(toImmutableSet()))
                    .containsOnly("lower_case_name", "mixed_case_name", "upper_case_name");

            // Note: until https://github.com/prestodb/presto/issues/2863 is resolved, this is *the* way to access the tables.

            assertQuery("SELECT lower_case_name FROM someschema.nonlowercasetable", "VALUES 'a'");
            assertQuery("SELECT mixed_case_name FROM someschema.nonlowercasetable", "VALUES 'b'");
            assertQuery("SELECT upper_case_name FROM someschema.nonlowercasetable", "VALUES 'c'");
            assertQuery("SELECT upper_case_name FROM SomeSchema.NonLowerCaseTable", "VALUES 'c'");
            assertQuery("SELECT upper_case_name FROM \"SomeSchema\".\"NonLowerCaseTable\"", "VALUES 'c'");

            // Exasol does not support writing via Trino
            onRemoteDatabase().execute("INSERT INTO \"SomeSchema\".\"NonLowerCaseTable\" (\"lower_case_name\") VALUES ('l')");
            onRemoteDatabase().execute("INSERT INTO \"SomeSchema\".\"NonLowerCaseTable\" (\"Mixed_Case_Name\") VALUES ('m')");
            onRemoteDatabase().execute("INSERT INTO \"SomeSchema\".\"NonLowerCaseTable\" (\"UPPER_CASE_NAME\") VALUES ('u')");
            assertQuery(
                    "SELECT * FROM someschema.nonlowercasetable",
                    "VALUES ('a', 'b', 'c')," +
                            "('l', NULL, NULL)," +
                            "(NULL, 'm', NULL)," +
                            "(NULL, NULL, 'u')");
        }
    }

    @Test
    @Override
    public void testSchemaNameRuleMapping()
            throws Exception
    {
        updateRuleBasedIdentifierMappingFile(
                getMappingFile(),
                ImmutableList.of(new SchemaMappingRule("remote_schema", "trino_schema")),
                ImmutableList.of());

        try (AutoCloseable ignore1 = withSchema("remote_schema");
                AutoCloseable ignore3 = withTable("remote_schema", "some_table_name", "(c varchar(5))")) {
            assertThat(computeActual("SHOW SCHEMAS ")
                    .getOnlyColumn())
                    .contains("trino_schema");
            assertQuery("SHOW TABLES FROM trino_schema", "VALUES 'some_table_name'");
            // Exasol does not support writing via Trino
            onRemoteDatabase().execute("INSERT INTO \"remote_schema\".\"some_table_name\" VALUES 'a'");
            assertQuery("SELECT * FROM trino_schema.some_table_name", "VALUES 'a'");
        }
    }

    @Test
    @Override
    public void testSchemaNameClashWithRuleMapping()
            throws Exception
    {
        List<SchemaMappingRule> schemaMappingRules = ImmutableList.of(
                new SchemaMappingRule("casesensitivename", "casesensitivename_a"),
                new SchemaMappingRule("CaseSensitiveName", "casesensitivename_b"),
                new SchemaMappingRule("CASESENSITIVENAME", "casesensitivename_c"));
        updateRuleBasedIdentifierMappingFile(getMappingFile(), schemaMappingRules, ImmutableList.of());

        String[] nameVariants = {"casesensitivename", "CaseSensitiveName", "CASESENSITIVENAME"};
        assertThat(Stream.of(nameVariants)
                .map(name -> name.toLowerCase(ENGLISH))
                .collect(toImmutableSet()))
                .hasSize(1);

        for (int i = 0; i < nameVariants.length; i++) {
            for (int j = i + 1; j < nameVariants.length; j++) {
                String remoteSchema = nameVariants[i];
                String otherRemoteSchema = nameVariants[j];

                try (AutoCloseable ignore1 = withSchema(remoteSchema);
                        AutoCloseable ignore2 = withSchema(otherRemoteSchema);
                        AutoCloseable ignore3 = withTable(remoteSchema, "some_table_name", "(c varchar(5))")) {
                    String schema = schemaMappingRules.stream()
                            .filter(rule -> rule.getRemoteSchema().equals(remoteSchema))
                            .map(SchemaMappingRule::getMapping)
                            .collect(onlyElement());

                    assertThat(computeActual("SHOW SCHEMAS ")
                            .getOnlyColumn()
                            .map(String.class::cast)
                            .filter(anObject -> anObject.startsWith("casesensitivename")))
                            .hasSize(2);
                    assertQuery("SHOW TABLES FROM " + schema, "VALUES 'some_table_name'");
                    // Exasol does not support writing via Trino
                    onRemoteDatabase().execute(format("INSERT INTO \"%s\".\"some_table_name\" VALUES 'a'", remoteSchema));
                    assertQuery(format("SELECT * FROM %s.some_table_name", schema), "VALUES 'a'");
                }
            }
        }
    }

    @Override
    protected AutoCloseable withTable(String remoteSchemaName, String remoteTableName, String tableDefinition)
    {
        String schemaName = quoted(remoteSchemaName);
        // The TEST_SCHEMA is created without quoting in TestingExasolServer, quoting it here causes "schema "tpch" not found"
        if (remoteSchemaName.equalsIgnoreCase(TEST_SCHEMA)) {
            schemaName = remoteSchemaName;
        }
        String quotedName = schemaName + "." + quoted(remoteTableName);
        onRemoteDatabase().execute(format("CREATE TABLE %s %s", quotedName, tableDefinition));
        return () -> onRemoteDatabase().execute("DROP TABLE " + quotedName);
    }

    @Override
    protected Path getMappingFile()
    {
        return requireNonNull(mappingFile, "mappingFile is null");
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return exasolServer::execute;
    }
}
