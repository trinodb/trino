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
import io.trino.plugin.base.mapping.SchemaMappingRule;
import io.trino.plugin.base.mapping.TableMappingRule;
import io.trino.plugin.jdbc.BaseCaseInsensitiveMappingTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
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
    protected Optional<String> optionalFromDual()
    {
        return Optional.of("FROM RDB$DATABASE");
    }

    @Test
    @Override
    public void testNonLowerCaseSchemaName()
    {
        abort(format("Firebird connector only supports schema '%s'.", getSession().getSchema().orElseThrow()));
    }

    @Test
    @Override
    public void testSchemaAndTableNameRuleMapping()
            throws Exception
    {
        String schema = getSession().getSchema().orElseThrow();
        updateRuleBasedIdentifierMappingFile(
                getMappingFile(),
                ImmutableList.of(new SchemaMappingRule(schema, "trino_schema")),
                ImmutableList.of(new TableMappingRule(schema, "remote_table", "trino_table")));

        try (AutoCloseable ignore1 = withSchema(schema);
                AutoCloseable ignore2 = withTable(schema, "remote_table", "(c varchar(5))")) {
            assertThat(computeActual("SHOW SCHEMAS")
                    .getOnlyColumn())
                    .contains("trino_schema");
            assertThat(computeActual(
                    format(
                            "SHOW TABLES IN %s",
                            delimited("trino_schema")))
                    .getOnlyColumn())
                    .contains("trino_table");
            assertThat(query(
                    format(
                            "SHOW COLUMNS FROM %s.%s",
                            delimited("trino_schema"),
                            delimited("trino_table"))))
                    .result().projected("Column").onlyColumnAsSet()
                    .contains(canonicalize("c"));
            assertUpdate(
                    format(
                            "INSERT INTO %s.%s VALUES 'dane'",
                            delimited("trino_schema"),
                            delimited("trino_table")),
                    1);
            assertQuery(
                    format(
                            "SELECT * FROM %s.%s",
                            delimited("trino_schema"),
                            delimited("trino_table")),
                    "VALUES 'dane'");
        }
    }

    @Test
    @Override
    public void testSchemaNameClash()
    {
        abort(format("Firebird connector only supports schema '%s'.", getSession().getSchema().orElseThrow()));
    }

    @Test
    @Override
    public void testSchemaNameClashWithRuleMapping()
    {
        abort(format("Firebird connector only supports schema '%s'.", getSession().getSchema().orElseThrow()));
    }

    @Test
    @Override
    public void testSchemaNameRuleMapping()
            throws Exception
    {
        String schema = getSession().getSchema().orElseThrow();
        updateRuleBasedIdentifierMappingFile(
                getMappingFile(),
                ImmutableList.of(new SchemaMappingRule(schema, "trino_schema")),
                ImmutableList.of());

        try (AutoCloseable ignore1 = withSchema(schema);
                AutoCloseable ignore3 = withTable(schema, "some_table_name", "(c varchar(5))")) {
            assertThat(computeActual("SHOW SCHEMAS ")
                    .getOnlyColumn())
                    .contains("trino_schema");
            assertQuery(
                    format(
                            "SHOW TABLES FROM %s",
                            delimited("trino_schema")),
                    "VALUES 'some_table_name'");
            assertUpdate(
                    format(
                            "INSERT INTO %s.%s VALUES 'a'",
                            delimited("trino_schema"),
                            delimited("some_table_name")),
                    1);
            assertQuery(
                    format(
                            "SELECT * FROM %s.%s",
                            delimited("trino_schema"),
                            delimited("some_table_name")),
                    "VALUES 'a'");
        }
    }

    @Test
    @Override
    public void testTableNameRuleMapping()
            throws Exception
    {
        String schema = getSession().getSchema().orElseThrow();
        updateRuleBasedIdentifierMappingFile(
                getMappingFile(),
                ImmutableList.of(),
                ImmutableList.of(new TableMappingRule(schema, "remote_table", "trino_table")));

        try (AutoCloseable ignore = withSchema(schema);
                AutoCloseable ignore1 = withTable(schema, "remote_table", "(c varchar(5))")) {
            assertThat(computeActual("SHOW TABLES FROM " + schema).getOnlyColumn())
                    .contains("trino_table");
            assertThat(query(
                    format(
                            "SHOW COLUMNS FROM %s.%s",
                            schema,
                            delimited("trino_table"))))
                    .result().projected("Column").onlyColumnAsSet()
                    .contains(canonicalize("c"));
            assertUpdate(
                    format(
                            "INSERT INTO %s.%s VALUES 'dane'",
                            schema,
                            delimited("trino_table")),
                    1);
            assertQuery(
                    format(
                            "SELECT * FROM %s.%s",
                            schema,
                            delimited("trino_table")),
                    "VALUES 'dane'");
        }
    }

    @Test
    @Override
    public void testTableNameClashWithRuleMapping()
            throws Exception
    {
        String schema = getSession().getSchema().orElseThrow();
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

                    assertThat(computeActual(
                            format(
                                    "SHOW TABLES FROM %s",
                                    delimited(schema)))
                            .getOnlyColumn()
                            .map(String.class::cast)
                            .filter(anObject -> anObject.startsWith("casesensitivename")))
                            .hasSize(2);
                    assertQuery(
                            format(
                                    "SHOW COLUMNS FROM %s.%s",
                                    delimited(schema),
                                    delimited(table)),
                            format(
                                    "SELECT '%s', 'varchar(5)', '', ''",
                                    canonicalize("c")));
                    assertUpdate(
                            format(
                                    "INSERT INTO %s.%s  VALUES 'dane'",
                                    delimited(schema),
                                    delimited(table)),
                            1);
                    assertQuery(
                            format(
                                    "SELECT * FROM %s.%s",
                                    delimited(schema),
                                    delimited(table)),
                            "VALUES 'dane'");
                }
            }
        }
    }

    @Test
    @Override
    public void testNonLowerCaseTableName()
            throws Exception
    {
        String schema = getSession().getSchema().orElseThrow();
        try (AutoCloseable ignore1 = withSchema("SomeSchema");
                AutoCloseable ignore2 = withTable(
                        schema,
                        "NonLowerCaseTable",
                        "(" +
                                quoted("lower_case_name") + " varchar(1), " +
                                quoted("Mixed_Case_Name") + " varchar(1), " +
                                quoted("UPPER_CASE_NAME") + " varchar(1))")) {
            if (optionalFromDual().isPresent()) {
                onRemoteDatabase().execute(
                        format(
                                "INSERT INTO %s SELECT 'a', 'b', 'c' %s",
                                quoted("NonLowerCaseTable"),
                                optionalFromDual().get()));
            }
            else {
                onRemoteDatabase().execute(
                        format(
                                "INSERT INTO %s VALUES('a', 'b', 'c')",
                                quoted("NonLowerCaseTable")));
            }

            assertQuery(
                    format(
                            "SELECT %s FROM %s.%s WHERE %s = '%s' AND %s = '%s'",
                            delimited("column_name"),
                            delimited("information_schema"),
                            delimited("columns"),
                            delimited("table_schema"),
                            schema,
                            delimited("table_name"),
                            getTrinoIdentifier("NonLowerCaseTable")),
                    format(
                            "VALUES 'lower_case_name', '%s', '%s'",
                            getTrinoIdentifier("Mixed_Case_Name"),
                            getTrinoIdentifier("UPPER_CASE_NAME")));
            assertQuery(
                    format(
                            "SELECT %s FROM %s.%s WHERE %s = '%s'",
                            delimited("column_name"),
                            delimited("information_schema"),
                            delimited("columns"),
                            delimited("table_name"),
                            getTrinoIdentifier("NonLowerCaseTable")),
                    format(
                            "VALUES 'lower_case_name', '%s', '%s'",
                            getTrinoIdentifier("Mixed_Case_Name"),
                            getTrinoIdentifier("UPPER_CASE_NAME")));
            assertThat(computeActual("SHOW COLUMNS FROM " + delimited(getTrinoIdentifier("NonLowerCaseTable")))
                    .getMaterializedRows().stream()
                    .map(row -> row.getField(0))
                    .collect(toImmutableSet()))
                    .containsOnly("lower_case_name", getTrinoIdentifier("Mixed_Case_Name"), getTrinoIdentifier("UPPER_CASE_NAME"));

            // Note: until https://github.com/prestodb/presto/issues/2863 is resolved, this is *the* way to access the tables.

            assertQuery(
                    format(
                            "SELECT %s FROM %s.%s",
                            delimited("lower_case_name"),
                            delimited(schema),
                            delimited(getTrinoIdentifier("NonLowerCaseTable"))),
                    "VALUES 'a'");
            assertQuery(
                    format(
                            "SELECT %s FROM %s.%s",
                            delimited(getTrinoIdentifier("Mixed_Case_Name")),
                            delimited(schema),
                            delimited(getTrinoIdentifier("NonLowerCaseTable"))),
                    "VALUES 'b'");
            assertQuery(
                    format(
                            "SELECT %s FROM %s.%s",
                            delimited(getTrinoIdentifier("UPPER_CASE_NAME")),
                            delimited(schema),
                            delimited(getTrinoIdentifier("NonLowerCaseTable"))),
                    "VALUES 'c'");

            assertUpdate(
                    format("INSERT INTO %s.%s (%s) VALUES ('l')",
                            delimited(getTrinoIdentifier("SomeSchema")),
                            delimited(getTrinoIdentifier("NonLowerCaseTable")),
                            delimited("lower_case_name")),
                    1);
            assertUpdate(
                    format("INSERT INTO %s.%s (%s) VALUES ('m')",
                            delimited(getTrinoIdentifier("SomeSchema")),
                            delimited(getTrinoIdentifier("NonLowerCaseTable")),
                            delimited(getTrinoIdentifier("Mixed_Case_Name"))),
                    1);
            assertUpdate(
                    format("INSERT INTO %s.%s (%s) VALUES ('u')",
                            delimited(getTrinoIdentifier("SomeSchema")),
                            delimited(getTrinoIdentifier("NonLowerCaseTable")),
                            delimited(getTrinoIdentifier("UPPER_CASE_NAME"))),
                    1);
            assertQuery(
                    format("SELECT * FROM %s.%s",
                            delimited(getTrinoIdentifier(schema)),
                            delimited(getTrinoIdentifier("NonLowerCaseTable"))),
                    "VALUES ('a', 'b', 'c')," +
                            "('l', NULL, NULL)," +
                            "(NULL, 'm', NULL)," +
                            "(NULL, NULL, 'u')");
        }
    }

    private String getTrinoIdentifier(String value)
    {
        return value;
    }

    private String delimited(String value)
    {
        return quoted(value);
    }
}
