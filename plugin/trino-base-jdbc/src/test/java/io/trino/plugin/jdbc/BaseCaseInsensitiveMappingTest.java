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
import io.airlift.log.Logging;
import io.trino.plugin.base.mapping.ColumnMappingRule;
import io.trino.plugin.base.mapping.IdentifierMappingModule;
import io.trino.plugin.base.mapping.SchemaMappingRule;
import io.trino.plugin.base.mapping.TableMappingRule;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.sql.SqlExecutor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.airlift.log.Level.WARN;
import static io.trino.plugin.base.mapping.RuleBasedIdentifierMappingUtils.updateRuleBasedIdentifierMappingFile;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

// Tests are using JSON based identifier mapping which is one for all tests
@TestInstance(PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
public abstract class BaseCaseInsensitiveMappingTest
        extends AbstractTestQueryFramework
{
    protected abstract Path getMappingFile();

    protected abstract SqlExecutor onRemoteDatabase();

    protected boolean useUpperCase;

    @BeforeAll
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

    /**
     * Must return a non-empty optional with the FROM clause to select from a dual table for databases that don't allow a SELECT without a FROM clause like Oracle.
     * Must return an empty optional for databases which don't require a FROM clause with a SELECT.
     */
    protected Optional<String> optionalFromDual()
    {
        return Optional.empty();
    }

    @Test
    public void testSchemaNameClash()
            throws Exception
    {
        updateRuleBasedIdentifierMappingFile(getMappingFile(), ImmutableList.of(), ImmutableList.of(), ImmutableList.of());

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
                    assertQueryFails("SHOW TABLES FROM casesensitivename", "Error listing tables for catalog \\w+: Failed to find remote schema name: Ambiguous name: casesensitivename");
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
        updateRuleBasedIdentifierMappingFile(getMappingFile(), ImmutableList.of(), ImmutableList.of(), ImmutableList.of());

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
                ImmutableList.of(),
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
        updateRuleBasedIdentifierMappingFile(getMappingFile(), schemaMappingRules, ImmutableList.of(), ImmutableList.of());

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
        String schema = "remote_schema";
        updateRuleBasedIdentifierMappingFile(
                getMappingFile(),
                ImmutableList.of(),
                ImmutableList.of(new TableMappingRule(schema, "remote_table", "trino_table")),
                ImmutableList.of());

        try (AutoCloseable ignore = withSchema(schema);
                AutoCloseable ignore1 = withTable(schema, "remote_table", "(c varchar(5))")) {
            assertThat(computeActual("SHOW TABLES FROM " + schema).getOnlyColumn())
                    .contains("trino_table");
            assertQuery("SHOW COLUMNS FROM " + schema + ".trino_table", "SELECT 'c', 'varchar(5)', '', ''");
            assertUpdate("INSERT INTO " + schema + ".trino_table VALUES 'dane'", 1);
            assertQuery("SELECT * FROM " + schema + ".trino_table", "VALUES 'dane'");
        }
    }

    @Test
    public void testTableNameClashWithRuleMapping()
            throws Exception
    {
        String schema = "remote_schema";
        List<TableMappingRule> tableMappingRules = ImmutableList.of(
                new TableMappingRule(schema, "casesensitivename", "casesensitivename_a"),
                new TableMappingRule(schema, "CaseSensitiveName", "casesensitivename_b"),
                new TableMappingRule(schema, "CASESENSITIVENAME", "casesensitivename_c"));
        updateRuleBasedIdentifierMappingFile(getMappingFile(), ImmutableList.of(), tableMappingRules, ImmutableList.of());

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
    public void testSchemaAndTableNameRuleMapping()
            throws Exception
    {
        updateRuleBasedIdentifierMappingFile(
                getMappingFile(),
                ImmutableList.of(new SchemaMappingRule("remote_schema", "trino_schema")),
                ImmutableList.of(new TableMappingRule("remote_schema", "remote_table", "trino_table")),
                ImmutableList.of());

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

    @Test
    public void testColumnRuleMapping()
            throws Exception
    {
        updateRuleBasedIdentifierMappingFile(
                getMappingFile(),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(new ColumnMappingRule("remote_schema", "remote_table", "c1", "remote_column")));

        try (var ignore1 = withSchema("remote_schema");
                var ignore2 = withTable("remote_schema", "remote_table", "(" + quoted("c1") + " varchar(5), s int)");
                var ignore3 = withTable("remote_schema", "remote_table1", "(" + quoted("c1") + " varchar(5), s int)")) {
            assertTableColumnNames("remote_schema.remote_table", "remote_column", "s");
            assertTableColumnNames("remote_schema.Remote_table1", "c1", "s");

            assertUpdate("INSERT INTO remote_schema.remote_table VALUES ('a', 1), ('a1', 2), ('b', 1)", 3);
            assertUpdate("INSERT INTO remote_schema.remote_table (remote_column, s) VALUES ('a', 1), ('a1', 2)", 2);
            assertUpdate("INSERT INTO remote_schema.remote_table1 VALUES ('b', 1), ('b1', 2), ('a', 1)", 3);
            assertUpdate("INSERT INTO remote_schema.remote_table1 (C1, s) VALUES ('b', 1), ('b1', 2)", 2);

            assertQuery("SELECT * FROM remote_schema.remote_table", "VALUES ('a', 1), ('a1', 2), ('b', 1), ('a', 1), ('a1', 2)");
            assertQuery("SELECT remote_column, s FROM remote_schema.remote_table", "VALUES ('a', 1), ('a1', 2), ('b', 1), ('a', 1), ('a1', 2)");
            assertQuery("SELECT * FROM remote_schema.remote_table1", "VALUES ('b', 1), ('b1', 2), ('a', 1), ('b', 1), ('b1', 2)");
            assertQuery("SELECT c1, s FROM remote_schema.remote_table1", "VALUES ('b', 1), ('b1', 2), ('a', 1), ('b', 1), ('b1', 2)");

            assertQuery("SELECT remote_column || 'a', sum(s) AS c1 FROM remote_schema.remote_table WHERE remote_column = 'a' GROUP BY remote_column HAVING sum(s) < 4 ORDER BY remote_column", "VALUES ('aa', 2)");
            assertQuery("SELECT c1 || 'b', sum(s) AS remote_column FROM remote_schema.remote_table1 WHERE c1 = 'b' GROUP BY c1 HAVING sum(s) < 4 ORDER BY c1", "VALUES ('bb', 2)");

            assertQuery("SELECT remote_column, remote_schema.remote_table.s, c1 FROM remote_schema.remote_table JOIN remote_schema.remote_table1 ON remote_column || 'a' = c1 || 'a' WHERE remote_column = 'a'", "VALUES ('a', 1, 'a'), ('a', 1, 'a')");
            assertQuery("SELECT c1, remote_schema.remote_table1.s, remote_column FROM remote_schema.remote_table1 JOIN remote_schema.remote_table ON c1 || 'a' = remote_column || 'a' WHERE c1 = 'a'", "VALUES ('a', 1, 'a'), ('a', 1, 'a')");
        }
    }

    @Test
    public void testChangeColumnMapping()
            throws Exception
    {
        updateRuleBasedIdentifierMappingFile(
                getMappingFile(),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(new ColumnMappingRule("remote_schema", "remote_table", "c1", "remote_column")));

        try (var ignore1 = withSchema("remote_schema");
                var ignore2 = withTable("remote_schema", "remote_table", "(" + quoted("c1") + " varchar(5), s int)");
                var ignore3 = withTable("remote_schema", "remote_table1", "(" + quoted("c1") + " varchar(5), s int)")) {
            assertTableColumnNames("remote_schema.remote_table", "remote_column", "s");
            assertTableColumnNames("remote_schema.Remote_table1", "c1", "s");

            assertUpdate("INSERT INTO remote_schema.remote_table VALUES ('a', 1), ('a1', 2), ('b', 1)", 3);
            assertUpdate("INSERT INTO remote_schema.remote_table (remote_column, s) VALUES ('a', 1), ('a1', 2)", 2);
            assertUpdate("INSERT INTO remote_schema.remote_table1 VALUES ('b', 1), ('b1', 2), ('a', 1)", 3);
            assertUpdate("INSERT INTO remote_schema.remote_table1 (C1, s) VALUES ('b', 1), ('b1', 2)", 2);

            assertUpdate("ALTER TABLE remote_schema.remote_table RENAME COLUMN remote_column TO c2");
            assertUpdate("ALTER TABLE remote_schema.remote_table ADD COLUMN c1 varchar(5)");

            updateRuleBasedIdentifierMappingFile(
                    getMappingFile(),
                    ImmutableList.of(),
                    ImmutableList.of(),
                    ImmutableList.of(new ColumnMappingRule("remote_schema", "remote_table", useUpperCase ? "C1" : "c1", "remote_column")));

            assertTableColumnNames("remote_schema.remote_table", "c2", "s", "remote_column");

            updateRuleBasedIdentifierMappingFile(
                    getMappingFile(),
                    ImmutableList.of(),
                    ImmutableList.of(),
                    ImmutableList.of(new ColumnMappingRule("remote_schema", "remote_table", useUpperCase ? "C2" : "c2", "remote_column"),
                            new ColumnMappingRule("remote_schema", "remote_table", useUpperCase ? "C1" : "c1", "remote_column1")));

            assertTableColumnNames("remote_schema.remote_table", "remote_column", "s", "remote_column1");
            assertUpdate("ALTER TABLE remote_schema.remote_table DROP COLUMN remote_column");
            assertTableColumnNames("remote_schema.remote_table", "s", "remote_column1");
            assertUpdate("ALTER TABLE remote_schema.remote_table ADD COLUMN c2 varchar(5)");
            assertTableColumnNames("remote_schema.remote_table", "s", "remote_column1", "remote_column");
        }
    }

    @Test
    public void testCreateTableAsSelectColumnMapping()
            throws Exception
    {
        updateRuleBasedIdentifierMappingFile(
                getMappingFile(),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(new ColumnMappingRule("remote_schema", "remote_table", "c1", "remote_column")));

        try (var ignore1 = withSchema("remote_schema");
                var ignore2 = withTable("remote_schema", "remote_table", "(" + quoted("c1") + " varchar(5), s int)");
                var ignore3 = withTable("remote_schema", "remote_table1", "(" + quoted("c1") + " varchar(5), s int)")) {
            assertUpdate("INSERT INTO remote_schema.remote_table VALUES ('a', 1), ('a1', 2), ('b', 1)", 3);
            assertUpdate("INSERT INTO remote_schema.remote_table (remote_column, s) VALUES ('a', 1), ('a1', 2)", 2);
            assertUpdate("INSERT INTO remote_schema.remote_table1 VALUES ('b', 1), ('b1', 2), ('a', 1)", 3);
            assertUpdate("INSERT INTO remote_schema.remote_table1 (C1, s) VALUES ('b', 1), ('b1', 2)", 2);

            assertUpdate("CREATE TABLE remote_schema.remote_table_select as SELECT remote_column FROM remote_schema.remote_table", 5);
            assertUpdate("CREATE TABLE remote_schema.remote_table1_select as SELECT c1 FROM remote_schema.remote_table1", 5);
            var remoteTableSelect = useUpperCase ? "remote_table_select".toUpperCase(ENGLISH) : "remote_table_select";
            assertQuery("SELECT remote_column FROM remote_schema." + remoteTableSelect, "VALUES 'a', 'a1', 'b', 'a', 'a1'");
            var remoteTable1Select = (useUpperCase ? "remote_table1_select".toUpperCase(ENGLISH) : "remote_table1_select");
            assertQuery("SELECT c1 FROM remote_schema." + remoteTable1Select, "VALUES 'b', 'b1', 'a', 'b', 'b1'");

            updateRuleBasedIdentifierMappingFile(
                    getMappingFile(),
                    ImmutableList.of(),
                    ImmutableList.of(),
                    ImmutableList.of(new ColumnMappingRule("remote_schema", "remote_table", "c1", "remote_column"),
                            new ColumnMappingRule("remote_schema", remoteTableSelect, useUpperCase ? "remote_column".toUpperCase(ENGLISH) : "remote_column", "c1"),
                            new ColumnMappingRule("remote_schema", remoteTable1Select, useUpperCase ? "C1" : "c1", "remote_column")));

            assertQuery("SELECT c1 FROM remote_schema." + remoteTableSelect, "VALUES 'a', 'a1', 'b', 'a', 'a1'");
            assertQuery("SELECT remote_column FROM remote_schema." + remoteTable1Select, "VALUES 'b', 'b1', 'a', 'b', 'b1'");

            assertUpdate("DELETE FROM remote_schema." + remoteTableSelect, 5);
            assertUpdate("DELETE FROM remote_schema." + remoteTable1Select, 5);
            assertUpdate("DROP TABLE remote_schema." + remoteTableSelect);
            assertUpdate("DROP TABLE remote_schema." + remoteTable1Select);
        }
    }

    @Test
    public void testSchemaAndTableMappingsWithColumnMappings()
            throws Exception
    {
        updateRuleBasedIdentifierMappingFile(
                getMappingFile(),
                ImmutableList.of(new SchemaMappingRule("RemoteSchema", "remote_schema")),
                ImmutableList.of(new TableMappingRule("RemoteSchema", "RemoteTable", "remote_table")),
                ImmutableList.of(
                        new ColumnMappingRule("RemoteSchema", "RemoteTable", "Col", "c1"),
                        new ColumnMappingRule("RemoteSchema", "RemoteTable", "col", "c2")));

        try (AutoCloseable ignore1 = withSchema("RemoteSchema");
                AutoCloseable ignore2 = withTable("RemoteSchema", "RemoteTable", "(" + quoted("col") + " varchar(5), " + quoted("Col") + " int)")) {
            assertTableColumnNames("remote_schema.remote_table", "c2", "c1");
            assertUpdate("INSERT INTO remote_schema.remote_table VALUES ('a', 1)", 1);
            assertUpdate("INSERT INTO remote_schema.remote_table (c2, c1) VALUES ('b', 2)", 1);
            assertUpdate("INSERT INTO remote_schema.remote_table (c2) VALUES ('c')", 1);

            assertQuery("SELECT * FROM remote_schema.remote_table", "VALUES ('a', 1), ('b', 2), ('c', null)");
            assertQuery("SELECT c2, c1 FROM remote_schema.remote_table", "VALUES ('a', 1), ('b', 2), ('c', null)");
            assertUpdate("DELETE FROM remote_schema.remote_table where c1 = 2", 1);
            assertQuery("SELECT c2 FROM remote_schema.remote_table", "VALUES 'a', 'c'");
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
