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
package io.trino.plugin.sqlserver;

import com.google.common.collect.ImmutableSet;
import io.trino.testing.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public abstract class BaseSqlServerCaseInsensitiveMappingTest
        extends AbstractTestQueryFramework
{
    protected abstract TestingSqlServer getSqlServer();

    @Test
    public void testSqlServerCollation()
            throws Exception
    {
        try (Connection connection = getSqlServer().createConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT SERVERPROPERTY ('Collation')")) {
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getString(1)).isEqualTo("Latin1_General_CS_AS");
            assertThat(resultSet.next()).isFalse();
        }
    }

    @Test
    public void testNonLowerCaseSchemaName()
            throws Exception
    {
        try (AutoCloseable ignore1 = getSqlServer().withSchema("\"NonLowerCaseSchema\"");
                AutoCloseable ignore2 = getSqlServer().withTable("\"NonLowerCaseSchema\".lower_case_name", "(c varchar(5))");
                AutoCloseable ignore3 = getSqlServer().withTable("\"NonLowerCaseSchema\".\"Mixed_Case_Name\"", "(c varchar(5))");
                AutoCloseable ignore4 = getSqlServer().withTable("\"NonLowerCaseSchema\".\"UPPER_CASE_NAME\"", "(c varchar(5))")) {
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
        try (AutoCloseable ignore1 = getSqlServer().withSchema("\"SomeSchema\"");
                AutoCloseable ignore2 = getSqlServer().withTable(
                        "\"SomeSchema\".\"NonLowerCaseTable\"", "(\"lower_case_name\" varchar(1), \"Mixed_Case_Name\" varchar(1), \"UPPER_CASE_NAME\" varchar(1))")) {
            getSqlServer().execute("INSERT INTO \"SomeSchema\".\"NonLowerCaseTable\" SELECT 'a', 'b', 'c'");
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
        String[] nameVariants = {"\"casesensitivename\"", "\"CaseSensitiveName\"", "\"CASESENSITIVENAME\""};
        assertThat(Stream.of(nameVariants)
                .map(name -> name.replace("\"", "").toLowerCase(ENGLISH))
                .collect(toImmutableSet()))
                .hasSize(1);

        for (int i = 0; i < nameVariants.length; i++) {
            for (int j = i + 1; j < nameVariants.length; j++) {
                String schemaName = nameVariants[i];
                String otherSchemaName = nameVariants[j];
                try (AutoCloseable ignore1 = getSqlServer().withSchema(schemaName);
                        AutoCloseable ignore2 = getSqlServer().withSchema(otherSchemaName);
                        AutoCloseable ignore3 = getSqlServer().withTable(schemaName + ".some_table_name", "(c varchar(5))");
                        AutoCloseable ignore4 = getSqlServer().withSchema("some_schema");
                        AutoCloseable ignore5 = getSqlServer().withTable("some_schema.some_table", "(c int)")) {
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
        String[] nameVariants = {"\"casesensitivename\"", "\"CaseSensitiveName\"", "\"CASESENSITIVENAME\""};
        assertThat(Stream.of(nameVariants)
                .map(name -> name.replace("\"", "").toLowerCase(ENGLISH))
                .collect(toImmutableSet()))
                .hasSize(1);

        for (int i = 0; i < nameVariants.length; i++) {
            for (int j = i + 1; j < nameVariants.length; j++) {
                try (AutoCloseable ignore1 = getSqlServer().withTable(nameVariants[i], "(c varchar(5))");
                        AutoCloseable ignore2 = getSqlServer().withTable(nameVariants[j], "(d varchar(5))");
                        AutoCloseable ignore3 = getSqlServer().withTable("some_table", "(d varchar(5))")) {
                    assertThat(computeActual("SHOW TABLES").getOnlyColumn().filter("casesensitivename"::equals)).hasSize(1); // TODO, should be 2
                    assertQueryFails("SHOW COLUMNS FROM casesensitivename", "Failed to find remote table name: Ambiguous name: casesensitivename");
                    assertQueryFails("SELECT * FROM casesensitivename", "Failed to find remote table name: Ambiguous name: casesensitivename");
                    assertQuery("SHOW COLUMNS FROM some_table", "SELECT 'd', 'varchar(5)', '', ''");
                    assertQueryReturnsEmptyResult("SELECT * FROM some_table");
                }
            }
        }
    }
}
