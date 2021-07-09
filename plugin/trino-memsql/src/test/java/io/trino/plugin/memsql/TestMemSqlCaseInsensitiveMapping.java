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
package io.trino.plugin.memsql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.jdbc.BaseCaseInsensitiveMappingTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.nio.file.Path;

import static io.trino.plugin.jdbc.mapping.RuleBasedIdentifierMappingUtils.createRuleBasedIdentifierMappingFile;
import static io.trino.plugin.memsql.MemSqlQueryRunner.createMemSqlQueryRunner;

// With case-insensitive-name-matching enabled colliding schema/table names are considered as errors.
// Some tests here create colliding names which can cause any other concurrent test to fail.
@Test(singleThreaded = true)
public class TestMemSqlCaseInsensitiveMapping
        extends BaseCaseInsensitiveMappingTest
{
    protected TestingMemSqlServer memSqlServer;
    protected Path mappingFile;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.memSqlServer = new TestingMemSqlServer();
        this.mappingFile = createRuleBasedIdentifierMappingFile();
        return createMemSqlQueryRunner(memSqlServer, ImmutableMap.of("case-insensitive-name-matching", "true"), ImmutableList.of());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        memSqlServer.close();
    }

<<<<<<< HEAD
    @Test
    public void testNonLowerCaseSchemaName()
            throws Exception
    {
        try (AutoCloseable ignore1 = withSchema("NonLowerCaseSchema");
             AutoCloseable ignore2 = withTable("NonLowerCaseSchema.lower_case_name", "(c varchar(5))");
             AutoCloseable ignore3 = withTable("NonLowerCaseSchema.Mixed_Case_Name", "(c varchar(5))");
             AutoCloseable ignore4 = withTable("NonLowerCaseSchema.UPPER_CASE_NAME", "(c varchar(5))")) {
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
                     "SomeSchema.NonLowerCaseTable", "(lower_case_name varchar(10), Mixed_Case_Name varchar(10), UPPER_CASE_NAME varchar(10))")) {
            execute("INSERT INTO SomeSchema.NonLowerCaseTable VALUES ('a', 'b', 'c')");

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

            assertUpdate("INSERT INTO someschema.nonlowercasetable (lower_case_name) VALUES ('lower')", 1);
            assertUpdate("INSERT INTO someschema.nonlowercasetable (mixed_case_name) VALUES ('mixed')", 1);
            assertUpdate("INSERT INTO someschema.nonlowercasetable (upper_case_name) VALUES ('upper')", 1);
            assertQuery(
                    "SELECT * FROM someschema.nonlowercasetable",
                    "VALUES ('a', 'b', 'c')," +
                            "('lower', NULL, NULL)," +
                            "(NULL, 'mixed', NULL)," +
                            "(NULL, NULL, 'upper')");
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
                     AutoCloseable ignore3 = withTable(schemaName + ".some_table_name", "(c varchar(5))")) {
                    assertThat(computeActual("SHOW SCHEMAS").getOnlyColumn()).contains("casesensitivename");
                    assertThat(computeActual("SHOW SCHEMAS").getOnlyColumn().filter("casesensitivename"::equals)).hasSize(1); // TODO change io.trino.plugin.jdbc.JdbcClient.getSchemaNames to return a List
                    assertQueryFails("SHOW TABLES FROM casesensitivename", "Failed to find remote schema name: Ambiguous name: casesensitivename");
                    assertQueryFails("SELECT * FROM casesensitivename.some_table_name", "Failed to find remote schema name: Ambiguous name: casesensitivename");
                }
            }
        }
=======
    private void execute(String sql)
    {
        memSqlServer.execute(sql);
>>>>>>> ab0a06943839fe48693d073508b2744d1f465076
    }

    @Override
    protected Path getMappingFile()
    {
<<<<<<< HEAD
        String[] nameVariants = {"casesensitivename", "CaseSensitiveName", "CASESENSITIVENAME"};
        assertThat(Stream.of(nameVariants)
                .map(name -> name.toLowerCase(ENGLISH))
                .collect(toImmutableSet()))
                .hasSize(1);

        for (int i = 0; i < nameVariants.length; i++) {
            for (int j = i + 1; j < nameVariants.length; j++) {
                try (AutoCloseable ignore1 = withTable("tpch." + nameVariants[i], "(c varchar(5))");
                     AutoCloseable ignore2 = withTable("tpch." + nameVariants[j], "(d varchar(5))")) {
                    assertThat(computeActual("SHOW TABLES").getOnlyColumn()).contains("casesensitivename");
                    assertThat(computeActual("SHOW TABLES").getOnlyColumn().filter("casesensitivename"::equals)).hasSize(1); // TODO, should be 2
                    assertQueryFails("SHOW COLUMNS FROM casesensitivename", "Failed to find remote table name: Ambiguous name: casesensitivename");
                    assertQueryFails("SELECT * FROM casesensitivename", "Failed to find remote table name: Ambiguous name: casesensitivename");
                }
            }
        }
=======
        return mappingFile;
>>>>>>> ab0a06943839fe48693d073508b2744d1f465076
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return memSqlServer::execute;
    }


}
