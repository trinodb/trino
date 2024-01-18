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
package io.trino.plugin.ignite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.base.mapping.ColumnMappingRule;
import io.trino.plugin.jdbc.BaseCaseInsensitiveMappingTest;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.base.mapping.RuleBasedIdentifierMappingUtils.createRuleBasedIdentifierMappingFile;
import static io.trino.plugin.base.mapping.RuleBasedIdentifierMappingUtils.updateRuleBasedIdentifierMappingFile;
import static io.trino.plugin.ignite.IgniteQueryRunner.createIgniteQueryRunner;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

// With case-insensitive-name-matching enabled colliding schema/table names are considered as errors.
// Some tests here create colliding names which can cause any other concurrent test to fail.
public class TestIgniteCaseInsensitiveMapping
        extends BaseCaseInsensitiveMappingTest
{
    private Path mappingFile;
    private TestingIgniteServer igniteServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        mappingFile = createRuleBasedIdentifierMappingFile();
        igniteServer = closeAfterClass(TestingIgniteServer.getInstance()).get();
        return createIgniteQueryRunner(
                igniteServer,
                ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        .put("case-insensitive-name-matching", "true")
                        .put("case-insensitive-name-matching.config-file", mappingFile.toFile().getAbsolutePath())
                        .put("case-insensitive-name-matching.config-file.refresh-period", "1ms") // ~always refresh
                        .buildOrThrow(),
                ImmutableList.of());
    }

    @Override
    protected AutoCloseable withSchema(String schemaName)
    {
        return null;
    }

    @Override
    protected String quoted(String name)
    {
        String identifierQuote = "`";
        name = name.replace(identifierQuote, identifierQuote + identifierQuote);
        return identifierQuote + name + identifierQuote;
    }

    @Test
    @Override
    public void testNonLowerCaseSchemaName()
            throws Exception
    {
        try (AutoCloseable ignore1 = withSchema("Public");
                AutoCloseable ignore2 = withTable("public", "lower_case_name", "(c varchar(5), id int primary key)");
                AutoCloseable ignore3 = withTable("PUbLic", "Mixed_Case_Name", "(c varchar(5), id int primary key)");
                AutoCloseable ignore4 = withTable("PUBLIC", "UPPER_CASE_NAME", "(c varchar(5), id int primary key)")) {
            assertThat(computeActual("SHOW SCHEMAS").getOnlyColumn()).contains("public");
            assertQuery("SHOW SCHEMAS LIKE 'publ%'", "VALUES 'public'");
            assertQuery("SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE '%lic'", "VALUES 'public'");
            // Ignite only has one schema `public` by default, hence there might some other tables exists when we reach here.
            assertThat(computeActual("SHOW TABLES FROM public").getMaterializedRows())
                    .doesNotContain(
                            new MaterializedRow(ImmutableList.of("Mixed_Case_Name")),
                            new MaterializedRow(ImmutableList.of("UPPER_CASE_NAME")))
                    .contains(
                            new MaterializedRow(ImmutableList.of("lower_case_name")),
                            new MaterializedRow(ImmutableList.of("mixed_case_name")),
                            new MaterializedRow(ImmutableList.of("upper_case_name")));

            assertQueryReturnsEmptyResult("SELECT * FROM public.lower_case_name");
        }
    }

    @Test
    @Override
    public void testNonLowerCaseTableName()
            throws Exception
    {
        try (AutoCloseable ignore1 = withSchema("PuBLic");
                AutoCloseable ignore2 = withTable(
                        "public",
                        "NonLowerCaseTable",
                        "(" +
                                quoted("lower_case_name") + " varchar(1) primary key, " +
                                quoted("Mixed_Case_Name") + " varchar(1), " +
                                quoted("UPPER_CASE_NAME") + " varchar(1))")) {
            onRemoteDatabase().execute("INSERT INTO " + (quoted("PubLic") + "." + quoted("NonLowerCaseTable")) + " SELECT 'a', 'b', 'c'");
            assertQuery(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'nonlowercasetable'",
                    "VALUES 'lower_case_name', 'mixed_case_name', 'upper_case_name'");
            assertQuery(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = 'nonlowercasetable'",
                    "VALUES 'lower_case_name', 'mixed_case_name', 'upper_case_name'");
            assertThat(computeActual("SHOW COLUMNS FROM public.nonlowercasetable").getMaterializedRows().stream()
                    .map(row -> row.getField(0))
                    .collect(toImmutableSet())).isEqualTo(ImmutableSet.of("lower_case_name", "mixed_case_name", "upper_case_name"));

            assertQuery("SELECT lower_case_name FROM public.nonlowercasetable", "VALUES 'a'");
            assertQuery("SELECT mixed_case_name FROM public.nonlowercasetable", "VALUES 'b'");
            assertQuery("SELECT upper_case_name FROM public.nonlowercasetable", "VALUES 'c'");
            assertQuery("SELECT upper_case_name FROM public.NonLowerCaseTable", "VALUES 'c'");
            assertQuery("SELECT upper_case_name FROM \"public\".\"NonLowerCaseTable\"", "VALUES 'c'");

            assertUpdate("INSERT INTO public.nonlowercasetable (lower_case_name) VALUES ('l')", 1);
            assertUpdate("INSERT INTO public.nonlowercasetable (lower_case_name, mixed_case_name) VALUES ('2', 'm')", 1);
            assertUpdate("INSERT INTO public.nonlowercasetable (lower_case_name, upper_case_name) VALUES ('3', 'u')", 1);
            assertQuery(
                    "SELECT * FROM public.nonlowercasetable",
                    "VALUES ('a', 'b', 'c')," +
                            "('l', NULL, NULL)," +
                            "('2', 'm', NULL)," +
                            "('3', NULL, 'u')");
        }
    }

    @Test
    @Override
    public void testSchemaNameClash()
            throws Exception
    {
        String[] nameVariants = {"public", "PuBlic", "PUBLIC"};
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
                        AutoCloseable ignore3 = withTable(schemaName, "some_table_name", "(c varchar(5) primary key, ignore int)")) {
                    assertThat(computeActual("SHOW SCHEMAS").getOnlyColumn().filter("public"::equals)).hasSize(1);
                }
            }
        }
    }

    @Test
    @Override
    public void testTableNameClash()
            throws Exception
    {
        String[] nameVariants = {"casesensitivename", "CaseSensitiveName", "CASESENSITIVENAME"};
        assertThat(Stream.of(nameVariants)
                .map(name -> name.toLowerCase(ENGLISH))
                .collect(toImmutableSet()))
                .hasSize(1);
        try (AutoCloseable ignore = withTable("public", nameVariants[0], "(a varchar(1), b int primary key)");
                AutoCloseable ignore1 = withTable("public", "some_table", "(d varchar(5), ignore varchar(1) primary key)")) {
            List<MaterializedRow> rows = computeActual("SHOW COLUMNS FROM some_table").getMaterializedRows();
            assertThat(rows != null && rows.size() == 2).isTrue();
            assertThat(rows.get(0).getField(0)).isEqualTo("d");
            assertThat(rows.get(0).getField(1)).isEqualTo("varchar(5)");
            assertThat(rows.get(0).getField(2)).isEqualTo("");
            assertThat(rows.get(1).getField(0)).isEqualTo("ignore");
            assertThat(rows.get(1).getField(1)).isEqualTo("varchar(1)");

            assertQueryReturnsEmptyResult("SELECT * FROM some_table");
            for (String nameVariant : nameVariants) {
                assertQueryFails("CREATE TABLE " + nameVariant + " (c varchar(5), ignore int) with (primary_key = ARRAY['ignore'])", ".* Table 'ignite.public.casesensitivename' already exists");
            }
        }
    }

    @Override
    @Test
    public void testColumnRuleMapping()
            throws Exception
    {
        String schema = "PUBLIC";
        updateRuleBasedIdentifierMappingFile(
                getMappingFile(),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(new ColumnMappingRule(schema, "REMOTE_TABLE", "C1", "remote_column")));

        try (AutoCloseable ignore1 = withTable(schema, "remote_table", "(" + quoted("c1") + " varchar(5), s int primary key)");
                AutoCloseable ignore2 = withTable(schema, "remote_table1", "(" + quoted("c1") + " varchar(5), s int primary key)")) {
            assertTableColumnNames(schema + ".remote_table", "remote_column", "s");
            assertTableColumnNames(schema + ".Remote_table1", "c1", "s");

            assertUpdate("INSERT INTO " + schema + ".remote_table VALUES ('a', 1), ('a1', 2), ('b', 3)", 3);
            assertUpdate("INSERT INTO " + schema + ".remote_table (remote_column, s) VALUES ('a', 4), ('a1', 5)", 2);
            assertUpdate("INSERT INTO " + schema + ".remote_table1 VALUES ('b', 1), ('b1', 2), ('a', 3)", 3);
            assertUpdate("INSERT INTO " + schema + ".remote_table1 (C1, s) VALUES ('b', 4), ('b1', 5)", 2);

            assertQuery("SELECT * FROM " + schema + ".remote_table", "VALUES ('a', 1), ('a1', 2), ('b', 3), ('a', 4), ('a1', 5)");
            assertQuery("SELECT remote_column, s FROM " + schema + ".remote_table", "VALUES ('a', 1), ('a1', 2), ('b', 3), ('a', 4), ('a1', 5)");
            assertQuery("SELECT * FROM " + schema + ".remote_table1", "VALUES ('b', 1), ('b1', 2), ('a', 3), ('b', 4), ('b1', 5)");
            assertQuery("SELECT c1, s FROM " + schema + ".remote_table1", "VALUES ('b', 1), ('b1', 2), ('a', 3), ('b', 4), ('b1', 5)");

            assertQuery("SELECT remote_column || 'a', sum(s) AS c1 FROM " + schema + ".remote_table WHERE remote_column = 'a' GROUP BY remote_column HAVING sum(s) < 6 ORDER BY remote_column", "VALUES ('aa', 5)");
            assertQuery("SELECT c1 || 'b', sum(s) AS remote_column FROM " + schema + ".remote_table1 WHERE c1 = 'b' GROUP BY c1 HAVING sum(s) < 6 ORDER BY c1", "VALUES ('bb', 5)");

            assertQuery("SELECT remote_column, " + schema + ".remote_table.s, c1 FROM " + schema + ".remote_table JOIN " + schema + ".remote_table1 ON remote_column || 'a' = c1 || 'a' WHERE remote_column = 'a'", "VALUES ('a', 1, 'a'), ('a', 4, 'a')");
            assertQuery("SELECT c1, " + schema + ".remote_table1.s, remote_column FROM " + schema + ".remote_table1 JOIN " + schema + ".remote_table ON c1 || 'a' = remote_column || 'a' WHERE c1 = 'a'", "VALUES ('a', 3, 'a'), ('a', 3, 'a')");
        }
    }

    @Override
    @Test
    public void testChangeColumnMapping()
            throws Exception
    {
        String schema = "PUBLIC";
        updateRuleBasedIdentifierMappingFile(
                getMappingFile(),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(new ColumnMappingRule(schema, "REMOTE_TABLE", "C1", "remote_column")));

        try (AutoCloseable ignore1 = withTable(schema, "remote_table", "(" + quoted("c1") + " varchar(5), s int primary key)");
                AutoCloseable ignore2 = withTable(schema, "remote_table1", "(" + quoted("c1") + " varchar(5), s int primary key)")) {
            assertTableColumnNames(schema + ".remote_table", "remote_column", "s");
            assertTableColumnNames(schema + ".Remote_table1", "c1", "s");

            assertUpdate("INSERT INTO " + schema + ".remote_table VALUES ('a', 1), ('a1', 2), ('b', 3)", 3);
            assertUpdate("INSERT INTO " + schema + ".remote_table (remote_column, s) VALUES ('a', 4), ('a1', 5)", 2);
            assertUpdate("INSERT INTO " + schema + ".remote_table1 VALUES ('b', 1), ('b1', 2), ('a', 3)", 3);
            assertUpdate("INSERT INTO " + schema + ".remote_table1 (C1, s) VALUES ('b', 4), ('b1', 5)", 2);

            assertQueryFails("ALTER TABLE " + schema + ".remote_table RENAME COLUMN remote_column TO c2", "This connector does not support renaming columns");

            updateRuleBasedIdentifierMappingFile(
                    getMappingFile(),
                    ImmutableList.of(),
                    ImmutableList.of(),
                    ImmutableList.of(new ColumnMappingRule(schema, "REMOTE_TABLE", "C2", "remote_column"),
                            new ColumnMappingRule(schema, "REMOTE_TABLE", "C1", "remote_column1")));

            assertTableColumnNames(schema + ".remote_table", "remote_column1", "s");
            assertUpdate("ALTER TABLE " + schema + ".remote_table DROP COLUMN remote_column1");
            assertTableColumnNames(schema + ".remote_table", "s");
            assertUpdate("ALTER TABLE " + schema + ".remote_table ADD COLUMN c2 varchar(5)");
            assertTableColumnNames(schema + ".remote_table", "s", "remote_column");
        }
    }

    @Override
    @Test
    public void testCreateTableAsSelectColumnMapping()
            throws Exception
    {
        String schema = "PUBLIC";
        updateRuleBasedIdentifierMappingFile(
                getMappingFile(),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(new ColumnMappingRule(schema, "REMOTE_TABLE", "C1", "remote_column")));

        try (AutoCloseable ignore1 = withTable(schema, "remote_table", "(" + quoted("c1") + " varchar(5), s int primary key)");
                AutoCloseable ignore2 = withTable(schema, "remote_table1", "(" + quoted("c1") + " varchar(5), s int primary key)")) {
            assertTableColumnNames(schema + ".remote_table", "remote_column", "s");
            assertTableColumnNames(schema + ".Remote_table1", "c1", "s");

            assertUpdate("INSERT INTO " + schema + ".remote_table VALUES ('a', 1), ('a1', 2), ('b', 3)", 3);
            assertUpdate("INSERT INTO " + schema + ".remote_table (remote_column, s) VALUES ('a', 4), ('a1', 5)", 2);
            assertUpdate("INSERT INTO " + schema + ".remote_table1 VALUES ('b', 1), ('b1', 2), ('a', 3)", 3);
            assertUpdate("INSERT INTO " + schema + ".remote_table1 (C1, s) VALUES ('b', 4), ('b1', 5)", 2);

            assertUpdate("CREATE TABLE " + schema + ".remote_table_select as SELECT remote_column FROM " + schema + ".remote_table", 5);
            assertUpdate("CREATE TABLE " + schema + ".remote_table1_select as SELECT c1 FROM " + schema + ".remote_table1", 5);
            String remoteTableSelect = "remote_table_select".toUpperCase(ENGLISH);
            assertQuery("SELECT remote_column FROM " + schema + "." + remoteTableSelect, "VALUES 'a', 'a1', 'b', 'a', 'a1'");
            String remoteTable1Select = "remote_table1_select".toUpperCase(ENGLISH);
            assertQuery("SELECT c1 FROM " + schema + "." + remoteTable1Select, "VALUES 'b', 'b1', 'a', 'b', 'b1'");

            updateRuleBasedIdentifierMappingFile(
                    getMappingFile(),
                    ImmutableList.of(),
                    ImmutableList.of(),
                    ImmutableList.of(new ColumnMappingRule(schema, "remote_table", "C1", "remote_column"),
                            new ColumnMappingRule(schema, remoteTableSelect, "remote_column".toUpperCase(ENGLISH), "c1"),
                            new ColumnMappingRule(schema, remoteTable1Select, "C1", "remote_column")));

            assertQuery("SELECT c1 FROM " + schema + "." + remoteTableSelect, "VALUES 'a', 'a1', 'b', 'a', 'a1'");
            assertQuery("SELECT remote_column FROM " + schema + "." + remoteTable1Select, "VALUES 'b', 'b1', 'a', 'b', 'b1'");

            assertUpdate("DELETE FROM " + schema + "." + remoteTableSelect, 5);
            assertUpdate("DELETE FROM " + schema + "." + remoteTable1Select, 5);
            assertUpdate("DROP TABLE " + schema + "." + remoteTableSelect);
            assertUpdate("DROP TABLE " + schema + "." + remoteTable1Select);
        }
    }

    @Override
    @Test
    public void testSchemaAndTableMappingsWithColumnMappings()
    {
        abort("Not support creating Ignite custom schema");
    }

    @Test
    @Override
    public void testTableNameClashWithRuleMapping()
    {
        abort("Not support creating Ignite custom schema");
    }

    @Test
    @Override
    public void testSchemaNameClashWithRuleMapping()
    {
        abort("Not support creating Ignite custom schema");
    }

    @Test
    @Override
    public void testSchemaAndTableNameRuleMapping()
    {
        abort("Not support creating Ignite custom schema");
    }

    @Test
    @Override
    public void testSchemaNameRuleMapping()
    {
        abort("Not support creating Ignite custom schema");
    }

    @Test
    @Override
    public void testTableNameRuleMapping()
    {
        abort("Not support creating Ignite custom schema");
    }

    @Override
    protected Path getMappingFile()
    {
        return requireNonNull(mappingFile, "mappingFile is null");
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return requireNonNull(igniteServer, "igniteServer is null")::execute;
    }
}
