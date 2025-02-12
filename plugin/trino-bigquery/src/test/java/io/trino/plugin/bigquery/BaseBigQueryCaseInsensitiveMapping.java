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
package io.trino.plugin.bigquery;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.bigquery.BigQueryQueryRunner.BigQuerySqlExecutor;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TestView;
import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

// With case-insensitive-name-matching enabled colliding schema/table names are considered as errors.
// Some tests here create colliding names which can cause any other concurrent test to fail.
public abstract class BaseBigQueryCaseInsensitiveMapping
        // TODO extends BaseCaseInsensitiveMappingTest - https://github.com/trinodb/trino/issues/7864
        extends AbstractTestQueryFramework
{
    private final BigQuerySqlExecutor bigQuerySqlExecutor = new BigQuerySqlExecutor();

    @Test
    public void testNonLowerCaseSchemaName()
            throws Exception
    {
        // Ensure schema name starts with a letter and is prefixed with a random string to make sure LIKE 'schema%' returns a single result
        String fixedRandom = "a" + randomNameSuffix();
        String bigQuerySchema = fixedRandom + "_NonLowerCaseSchema";
        String trinoSchema = bigQuerySchema.toLowerCase(ENGLISH);
        try (AutoCloseable ignore1 = withSchema(bigQuerySchema);
                AutoCloseable ignore2 = withTable(bigQuerySchema + ".lower_case_name", "(c string)");
                AutoCloseable ignore3 = withTable(bigQuerySchema + ".Mixed_Case_Name", "(c string)");
                AutoCloseable ignore4 = withTable(bigQuerySchema + ".UPPER_CASE_NAME", "(c string)")) {
            assertThat(computeActual("SHOW SCHEMAS").getOnlyColumn()).contains(trinoSchema);
            assertQuery("SHOW SCHEMAS LIKE '" + fixedRandom + "%'", "VALUES '" + trinoSchema + "'");
            assertQuery("SHOW TABLES FROM " + trinoSchema, "VALUES 'lower_case_name', 'mixed_case_name', 'upper_case_name'");

            assertQuery("SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE '%" + trinoSchema + "'", "VALUES '" + trinoSchema + "'");
            assertQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = '" + trinoSchema + "'", "VALUES 'lower_case_name', 'mixed_case_name', 'upper_case_name'");

            assertQueryReturnsEmptyResult("SELECT * FROM " + trinoSchema + ".lower_case_name");
            // TODO: test with INSERT and CTAS https://github.com/trinodb/trino/issues/6868, https://github.com/trinodb/trino/issues/6869
        }
    }

    @Test
    public void testNonLowerCaseTableName()
            throws Exception
    {
        String bigQuerySchema = "SomeSchema_" + randomNameSuffix();
        String trinoSchema = bigQuerySchema.toLowerCase(ENGLISH);
        try (AutoCloseable ignore1 = withSchema(bigQuerySchema);
                AutoCloseable ignore2 = withTable(
                        bigQuerySchema + ".NonLowerCaseTable", "AS SELECT 'a' AS lower_case_name, 'b' AS Mixed_Case_Name, 'c' AS UPPER_CASE_NAME")) {
            assertThat(computeActual("SHOW TABLES FROM " + trinoSchema).getOnlyColumn()).contains("nonlowercasetable");
            assertThat(computeActual("SHOW COLUMNS FROM " + trinoSchema + ".nonlowercasetable").getMaterializedRows().stream()
                    .map(row -> row.getField(0))
                    .collect(toImmutableSet())).isEqualTo(ImmutableSet.of("lower_case_name", "mixed_case_name", "upper_case_name"));

            assertQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = '" + trinoSchema + "'", "VALUES 'nonlowercasetable'");
            assertQuery(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema = '" + trinoSchema + "' AND table_name = 'nonlowercasetable'",
                    "VALUES 'lower_case_name', 'mixed_case_name', 'upper_case_name'");

            // Note: until https://github.com/trinodb/trino/issues/17 is resolved, this is *the* way to access the tables.
            assertQuery("SELECT lower_case_name FROM " + trinoSchema + ".nonlowercasetable", "VALUES 'a'");
            assertQuery("SELECT mixed_case_name FROM " + trinoSchema + ".nonlowercasetable", "VALUES 'b'");
            assertQuery("SELECT upper_case_name FROM " + trinoSchema + ".nonlowercasetable", "VALUES 'c'");
            assertQuery("SELECT upper_case_name FROM " + bigQuerySchema + ".NonLowerCaseTable", "VALUES 'c'");
            assertQuery("SELECT upper_case_name FROM \"" + bigQuerySchema + "\".\"NonLowerCaseTable\"", "VALUES 'c'");

            assertUpdate("INSERT INTO " + trinoSchema + ".nonlowercasetable (lower_case_name) VALUES ('l')", 1);
            assertUpdate("INSERT INTO " + trinoSchema + ".nonlowercasetable (mixed_case_name) VALUES ('m')", 1);
            assertUpdate("INSERT INTO " + trinoSchema + ".nonlowercasetable (upper_case_name) VALUES ('u')", 1);
            assertQuery(
                    "SELECT * FROM " + trinoSchema + ".nonlowercasetable",
                    "VALUES ('a', 'b', 'c')," +
                            "('l', NULL, NULL)," +
                            "(NULL, 'm', NULL)," +
                            "(NULL, NULL, 'u')");

            assertUpdate("CREATE TABLE " + trinoSchema + ".test_ctas_in_nonlowercase_schema AS SELECT 1 x", 1);
            assertQuery("SELECt * FROM " + trinoSchema + ".test_ctas_in_nonlowercase_schema", "VALUES 1");
        }
    }

    @Test
    public void testNonLowerCaseViewName()
            throws Exception
    {
        String bigQuerySchema = "SomeSchema_" + randomNameSuffix();
        String trinoSchema = bigQuerySchema.toLowerCase(ENGLISH);
        String namePrefix = format("%s.Test_Case", bigQuerySchema);

        try (AutoCloseable _ = withSchema(bigQuerySchema);
                TestView view = new TestView(bigQuerySqlExecutor, namePrefix, "SELECT 'a' AS lower_case_name, 'b' AS Mixed_Case_Name, 'c' AS UPPER_CASE_NAME")) {
            String viewName = view.getName().substring(bigQuerySchema.length() + 1).toLowerCase(ENGLISH);
            assertThat(computeActual("SHOW TABLES FROM " + trinoSchema).getOnlyColumn()).contains(viewName);
            assertThat(computeActual("SHOW COLUMNS FROM " + trinoSchema + "." + viewName).getMaterializedRows().stream()
                    .map(row -> row.getField(0))
                    .collect(toImmutableSet())).isEqualTo(ImmutableSet.of("lower_case_name", "mixed_case_name", "upper_case_name"));

            assertQuery(
                    format("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'", trinoSchema),
                    format("VALUES '%s'", viewName));
            assertQuery(
                    format("SELECT column_name FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'", trinoSchema, viewName),
                    "VALUES 'lower_case_name', 'mixed_case_name', 'upper_case_name'");

            // Note: until https://github.com/trinodb/trino/issues/17 is resolved, this is *the* way to access the tables.
            assertQuery("SELECT lower_case_name FROM " + view.getName(), "VALUES 'a'");
            assertQuery("SELECT mixed_case_name FROM " + view.getName(), "VALUES 'b'");
            assertQuery("SELECT upper_case_name FROM " + view.getName(), "VALUES 'c'");
            assertQuery("SELECT upper_case_name FROM " + view.getName().toLowerCase(ENGLISH), "VALUES 'c'");
            // TODO: test with INSERT and CTAS https://github.com/trinodb/trino/issues/6868, https://github.com/trinodb/trino/issues/6869
        }
    }

    @Test
    public void testSchemaNameClash()
            throws Exception
    {
        // Ensure schema names start with a letter
        String random = "a" + randomNameSuffix();
        String[] nameVariants = {random + "_casesensitivename", random + "_CaseSensitiveName", random + "_CASESENSITIVENAME"};
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
                        AutoCloseable ignore3 = withTable(schemaName + ".some_table_name", "(c string)")) {
                    String trinoSchema = schemaName.toLowerCase(ENGLISH);

                    // listing must not fail but will filter out ambiguous names
                    assertThat(computeActual("SHOW SCHEMAS").getOnlyColumn()).doesNotContain(trinoSchema);
                    assertQueryReturnsEmptyResult("SHOW SCHEMAS LIKE '" + trinoSchema + "%'");
                    assertQueryReturnsEmptyResult("SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE '%" + trinoSchema + "'");
                    assertQueryReturnsEmptyResult("SELECT table_name FROM information_schema.tables WHERE table_schema = '" + trinoSchema + "'");

                    // queries which use the ambiguous object must fail
                    assertQueryFails("SHOW CREATE SCHEMA " + trinoSchema, "Found ambiguous names in BigQuery.*");
                    assertQueryFails("SHOW TABLES FROM " + trinoSchema, "Found ambiguous names in BigQuery.*");
                    assertQueryFails("SELECT * FROM " + trinoSchema + ".some_table_name", "Found ambiguous names in BigQuery.*");
                    // TODO: test with INSERT and CTAS https://github.com/trinodb/trino/issues/6868, https://github.com/trinodb/trino/issues/6869
                }
            }
        }
    }

    @Test
    public void testTableNameClash()
            throws Exception
    {
        // Ensure schema name starts with a letter and is lowercase to only test table name code path
        String schema = ("a" + randomNameSuffix() + "_tpch").toLowerCase(ENGLISH);
        String[] nameVariants = {"casesensitivename", "CaseSensitiveName", "CASESENSITIVENAME"};
        assertThat(Stream.of(nameVariants)
                .map(name -> name.toLowerCase(ENGLISH))
                .collect(toImmutableSet()))
                .hasSize(1);

        for (int i = 0; i < nameVariants.length; i++) {
            for (int j = i + 1; j < nameVariants.length; j++) {
                try (AutoCloseable ignore1 = withSchema(schema);
                        AutoCloseable ignore2 = withTable(schema + "." + nameVariants[i], "(c string)");
                        AutoCloseable ignore3 = withTable(schema + "." + nameVariants[j], "(d string)")) {
                    // listing must not fail but will filter out ambiguous names
                    assertThat(computeActual("SHOW TABLES FROM " + schema).getOnlyColumn()).doesNotContain("casesensitivename");
                    assertQueryReturnsEmptyResult("SELECT table_name FROM information_schema.tables WHERE table_schema = '" + schema + "'");
                    assertQueryReturnsEmptyResult("SELECT column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name = 'casesensitivename'");

                    // queries which use the ambiguous object must fail
                    assertQueryFails("SHOW CREATE TABLE " + schema + ".casesensitivename", "Found ambiguous names in BigQuery.*");
                    assertQueryFails("SHOW COLUMNS FROM " + schema + ".casesensitivename", "Found ambiguous names in BigQuery.*");
                    assertQueryFails("SELECT * FROM " + schema + ".casesensitivename", "Found ambiguous names in BigQuery.*");
                    // TODO: test with INSERT and CTAS https://github.com/trinodb/trino/issues/6868, https://github.com/trinodb/trino/issues/6869
                }
            }
        }
    }

    @Test
    public void testCreateSchema()
    {
        String schemaName = ("Test_Create_Case_Sensitive_" + randomNameSuffix()).toLowerCase(ENGLISH);
        try {
            assertUpdate("CREATE SCHEMA " + schemaName);
            assertQuery(format("SELECT schema_name FROM information_schema.schemata WHERE schema_name = '%s'", schemaName), format("VALUES '%s'", schemaName));
        }
        finally {
            assertUpdate("DROP SCHEMA IF EXISTS " + schemaName);
        }
    }

    @Test
    public void testCreateSchemaNameClash()
            throws Exception
    {
        String schemaName = "Test_Create_Case_Sensitive_Clash_" + randomNameSuffix();
        try (AutoCloseable _ = withSchema(schemaName)) {
            assertQueryFails("CREATE SCHEMA " + schemaName.toLowerCase(ENGLISH), ".*Schema 'bigquery\\.\\Q" + schemaName.toLowerCase(ENGLISH) + "\\E' already exists");
        }
    }

    @Test
    public void testDropSchema()
            throws Exception
    {
        String schemaName = "Test_Drop_Case_Sensitive_" + randomNameSuffix();
        try (AutoCloseable _ = withSchema(schemaName)) {
            assertUpdate("DROP SCHEMA " + schemaName.toLowerCase(ENGLISH));
        }
    }

    @Test
    public void testDropSchemaNameClash()
            throws Exception
    {
        String schemaName = "Test_Drop_Case_Sensitive_Clash_" + randomNameSuffix();
        try (AutoCloseable _ = withSchema(schemaName);
                AutoCloseable _ = withSchema(schemaName.toLowerCase(ENGLISH))) {
            assertQueryFails("DROP SCHEMA " + schemaName.toLowerCase(ENGLISH), "Found ambiguous names in BigQuery.*");
        }
    }

    private AutoCloseable withSchema(String schemaName)
    {
        bigQuerySqlExecutor.dropDatasetIfExists(schemaName);
        bigQuerySqlExecutor.createDataset(schemaName);
        return () -> bigQuerySqlExecutor.dropDatasetIfExists(schemaName);
    }

    /**
     * @deprecated Use {@link TestTable} instead.
     */
    @Deprecated
    private AutoCloseable withTable(String tableName, String tableDefinition)
    {
        bigQuerySqlExecutor.execute(format("CREATE TABLE %s %s", tableName, tableDefinition));
        return () -> bigQuerySqlExecutor.execute(format("DROP TABLE %s", tableName));
    }
}
