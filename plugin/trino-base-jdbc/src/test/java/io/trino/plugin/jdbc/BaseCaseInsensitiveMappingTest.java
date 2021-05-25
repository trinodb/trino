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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logging;
import io.trino.plugin.jdbc.mapping.IdentifierMappingModule;
import io.trino.plugin.jdbc.mapping.SchemaMappingRule;
import io.trino.plugin.jdbc.mapping.TableMappingRule;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.sql.SqlExecutor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.airlift.log.Level.WARN;
import static io.trino.plugin.jdbc.mapping.RuleBasedIdentifierMappingUtils.updateRuleBasedIdentifierMappingFile;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

// Tests are using JSON based identifier mapping which is one for all tests
@Test(singleThreaded = true)
public abstract class BaseCaseInsensitiveMappingTest
        extends AbstractTestQueryFramework
{
    protected abstract Path getMappingFile();

    protected abstract SqlExecutor onRemoteDatabase();

    @BeforeClass
    public void disableMappingRefreshVerboseLogging()
    {
        Logging logging = Logging.initialize();
        logging.setLevel(IdentifierMappingModule.class.getName(), WARN);
    }

    @Test
    public void testNonLowerCaseSchemaName()
            throws Exception
    {
        try (AutoCloseable ignore1 = withSchema("NonLowerCaseSchema");
                AutoCloseable ignore2 = withTable("NonLowerCaseSchema", "lower_case_name", "(c varchar(5))");
                AutoCloseable ignore3 = withTable("NonLowerCaseSchema", "Mixed_Case_Name", "(c varchar(5))");
                AutoCloseable ignore4 = withTable("NonLowerCaseSchema", "UPPER_CASE_NAME", "(c varchar(5))")) {
            assertThat(computeActual("SHOW SCHEMAS").getOnlyColumn()).contains("nonlowercaseschema");
            assertQuery("SHOW SCHEMAS LIKE 'nonlowerc%'", "VALUES 'nonlowercaseschema'");
            assertQuery("SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE '%nonlowercaseschema'", "VALUES 'nonlowercaseschema'");
            assertQuery("SHOW TABLES FROM nonlowercaseschema", "VALUES 'lower_case_name', 'mixed_case_name', 'upper_case_name'");
            assertQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = 'nonlowercaseschema'", "VALUES 'lower_case_name', 'mixed_case_name', 'upper_case_name'");
            assertQueryReturnsEmptyResult("SELECT * FROM nonlowercaseschema.lower_case_name");
        }
    }

    @Test
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
            onRemoteDatabase().execute("INSERT INTO " + (quoted("SomeSchema") + "." + quoted("NonLowerCaseTable")) + " SELECT 'a', 'b', 'c'");
            assertQuery(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema = 'someschema' AND table_name = 'nonlowercasetable'",
                    "VALUES 'lower_case_name', 'mixed_case_name', 'upper_case_name'");
            assertQuery(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = 'nonlowercasetable'",
                    "VALUES 'lower_case_name', 'mixed_case_name', 'upper_case_name'");
            assertEquals(
                    computeActual("SHOW COLUMNS FROM someschema.nonlowercasetable").getMaterializedRows().stream()
                            .map(row -> row.getField(0))
                            .collect(toImmutableSet()),
                    ImmutableSet.of("lower_case_name", "mixed_case_name", "upper_case_name"));

            // Note: until https://github.com/prestodb/presto/issues/2863 is resolved, this is *the* way to access the tables.

            assertQuery("SELECT lower_case_name FROM someschema.nonlowercasetable", "VALUES 'a'");
            assertQuery("SELECT mixed_case_name FROM someschema.nonlowercasetable", "VALUES 'b'");
            assertQuery("SELECT upper_case_name FROM someschema.nonlowercasetable", "VALUES 'c'");
            assertQuery("SELECT upper_case_name FROM SomeSchema.NonLowerCaseTable", "VALUES 'c'");
            assertQuery("SELECT upper_case_name FROM \"SomeSchema\".\"NonLowerCaseTable\"", "VALUES 'c'");

            assertUpdate("INSERT INTO someschema.nonlowercasetable (lower_case_name) VALUES ('l')", 1);
            assertUpdate("INSERT INTO someschema.nonlowercasetable (mixed_case_name) VALUES ('m')", 1);
            assertUpdate("INSERT INTO someschema.nonlowercasetable (upper_case_name) VALUES ('u')", 1);
            assertQuery(
                    "SELECT * FROM someschema.nonlowercasetable",
                    "VALUES ('a', 'b', 'c')," +
                            "('l', NULL, NULL)," +
                            "(NULL, 'm', NULL)," +
                            "(NULL, NULL, 'u')");
        }
    }

    @Test
    public void testSchemaNameClash()
            throws Exception
    {
        String[] nameVariants = {"casesensitivename", "CaseSensitiveName", "CASESENSITIVENAME"};
        assertThat(Stream.of(nameVariants)
                .map(name -> name.toLowerCase(ENGLISH))
                .collect(toImmutableSet()))
                .hasSize(1);

        for (int i = 0; i < nameVariants.length; i++) {
            for (int j = i + 1; j < nameVariants.length; j++) {
                String schemaName = nameVariants[i];
                String otherSchemaName = nameVariants[j];
                try (AutoCloseable ignore1 = withSchema(schemaName);
                        AutoCloseable ignore2 = withSchema(otherSchemaName);
                        AutoCloseable ignore3 = withTable(schemaName, "some_table_name", "(c varchar(5))");
                        AutoCloseable ignore4 = withSchema("some_schema");
                        AutoCloseable ignore5 = withTable("some_schema", "some_table", "(c int)")) {
                    assertThat(computeActual("SHOW SCHEMAS").getOnlyColumn().filter("casesensitivename"::equals)).hasSize(1); // TODO change io.trino.plugin.jdbc.JdbcClient.getSchemaNames to return a List
                    assertQueryFails("SHOW TABLES FROM casesensitivename", "Failed to find remote schema name: Ambiguous name: casesensitivename");
                    assertQueryFails("SELECT * FROM casesensitivename.some_table_name", "Failed to find remote schema name: Ambiguous name: casesensitivename");
                    assertQuery("SHOW TABLES FROM some_schema", "VALUES 'some_table'");
                    assertQueryReturnsEmptyResult("SELECT * FROM some_schema.some_table");
                }
            }
        }
    }

    @Test
    public void testTableNameClash()
            throws Exception
    {
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
                    assertThat(computeActual("SHOW TABLES").getOnlyColumn().filter("casesensitivename"::equals)).hasSize(1); // TODO, should be 2
                    assertQueryFails("SHOW COLUMNS FROM casesensitivename", "Failed to find remote table name: Ambiguous name: casesensitivename");
                    assertQueryFails("SELECT * FROM casesensitivename", "Failed to find remote table name: Ambiguous name: casesensitivename");
                    assertQuery("SHOW COLUMNS FROM some_table", "SELECT 'd', 'varchar(5)', '', ''");
                    assertQueryReturnsEmptyResult("SELECT * FROM some_table");
                }
            }
        }
    }

    @Test
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
            assertUpdate("INSERT INTO trino_schema.some_table_name VALUES 'a'", 1);
            assertQuery("SELECT * FROM trino_schema.some_table_name", "VALUES 'a'");
        }
    }

    @Test
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
                    assertUpdate(format("INSERT INTO %s.some_table_name VALUES 'a'", schema), 1);
                    assertQuery(format("SELECT * FROM %s.some_table_name", schema), "VALUES 'a'");
                }
            }
        }
    }

    @Test
    public void testTableNameRuleMapping()
            throws Exception
    {
        updateRuleBasedIdentifierMappingFile(
                getMappingFile(),
                ImmutableList.of(),
                ImmutableList.of(new TableMappingRule(getSession().getSchema().orElseThrow(), "remote_table", "trino_table")));

        try (AutoCloseable ignore1 = withTable("remote_table", "(c varchar(5))")) {
            assertThat(computeActual("SHOW TABLES").getOnlyColumn())
                    .contains("trino_table");
            assertQuery("SHOW COLUMNS FROM trino_table", "SELECT 'c', 'varchar(5)', '', ''");
            assertUpdate("INSERT INTO trino_table VALUES 'dane'", 1);
            assertQuery("SELECT * FROM trino_table", "VALUES 'dane'");
        }
    }

    @Test
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
                try (AutoCloseable ignore1 = withTable(remoteTable, "(c varchar(5))");
                        AutoCloseable ignore2 = withTable(otherRemoteTable, "(d varchar(5))")) {
                    String table = tableMappingRules.stream()
                            .filter(rule -> rule.getRemoteTable().equals(remoteTable))
                            .map(TableMappingRule::getMapping)
                            .collect(onlyElement());

                    assertThat(computeActual("SHOW TABLES")
                            .getOnlyColumn()
                            .map(String.class::cast)
                            .filter(anObject -> anObject.startsWith("casesensitivename")))
                            .hasSize(2);
                    assertQuery("SHOW COLUMNS FROM " + table, "SELECT 'c', 'varchar(5)', '', ''");
                    assertUpdate("INSERT INTO " + table + " VALUES 'dane'", 1);
                    assertQuery("SELECT * FROM " + table, "VALUES 'dane'");
                }
            }
        }
    }

    @Test
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
            assertUpdate("INSERT INTO trino_schema.trino_table VALUES 'dane'", 1);
            assertQuery("SELECT * FROM trino_schema.trino_table", "VALUES 'dane'");
        }
    }

    protected AutoCloseable withSchema(String schemaName)
    {
        onRemoteDatabase().execute("CREATE SCHEMA " + quoted(schemaName));
        return () -> onRemoteDatabase().execute("DROP SCHEMA " + quoted(schemaName));
    }

    protected AutoCloseable withTable(String remoteSchemaName, String remoteTableName, String tableDefinition)
    {
        String quotedName = quoted(remoteSchemaName) + "." + quoted(remoteTableName);
        onRemoteDatabase().execute(format("CREATE TABLE %s %s", quotedName, tableDefinition));
        return () -> onRemoteDatabase().execute("DROP TABLE " + quotedName);
    }

    protected String quoted(String name)
    {
        String identifierQuote = "\"";
        name = name.replace(identifierQuote, identifierQuote + identifierQuote);
        return identifierQuote + name + identifierQuote;
    }

    private AutoCloseable withTable(String remoteTableName, String definition)
    {
        return withTable(getSession().getSchema().orElseThrow(), remoteTableName, definition);
    }
}
