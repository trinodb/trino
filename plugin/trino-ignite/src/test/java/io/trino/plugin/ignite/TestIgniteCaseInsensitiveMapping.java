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
import io.trino.plugin.jdbc.BaseCaseInsensitiveMappingTest;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.ignite.IgniteQueryRunner.createIgniteQueryRunner;
import static io.trino.plugin.jdbc.mapping.RuleBasedIdentifierMappingUtils.createRuleBasedIdentifierMappingFile;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

// With case-insensitive-name-matching enabled colliding schema/table names are considered as errors.
// Some tests here create colliding names which can cause any other concurrent test to fail.
@Test(singleThreaded = true)
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
            assertEquals(
                    computeActual("SHOW COLUMNS FROM public.nonlowercasetable").getMaterializedRows().stream()
                            .map(row -> row.getField(0))
                            .collect(toImmutableSet()),
                    ImmutableSet.of("lower_case_name", "mixed_case_name", "upper_case_name"));

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
            assertTrue(rows != null && rows.size() == 2);
            assertEquals(rows.get(0).getField(0), "d");
            assertEquals(rows.get(0).getField(1), "varchar(5)");
            assertEquals(rows.get(0).getField(2), "");
            assertEquals(rows.get(1).getField(0), "ignore");
            assertEquals(rows.get(1).getField(1), "varchar(1)");

            assertQueryReturnsEmptyResult("SELECT * FROM some_table");
            for (String nameVariant : nameVariants) {
                assertQueryFails("CREATE TABLE " + nameVariant + " (c varchar(5), ignore int) with (primary_key = ARRAY['ignore'])", ".* Table 'ignite.public.casesensitivename' already exists");
            }
        }
    }

    @Override
    public void testTableNameClashWithRuleMapping()
    {
        throw new SkipException("Not support creating Ignite custom schema");
    }

    @Override
    public void testSchemaNameClashWithRuleMapping()
    {
        throw new SkipException("Not support creating Ignite custom schema");
    }

    @Override
    public void testSchemaAndTableNameRuleMapping()
    {
        throw new SkipException("Not support creating Ignite custom schema");
    }

    @Override
    public void testSchemaNameRuleMapping()
    {
        throw new SkipException("Not support creating Ignite custom schema");
    }

    @Override
    public void testTableNameRuleMapping()
    {
        throw new SkipException("Not support creating Ignite custom schema");
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
