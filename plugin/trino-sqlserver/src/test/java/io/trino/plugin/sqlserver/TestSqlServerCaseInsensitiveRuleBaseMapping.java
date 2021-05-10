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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.jdbc.mapping.SchemaMappingRule;
import io.trino.plugin.jdbc.mapping.TableMappingRule;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.jdbc.mapping.RuleBasedIdentifierMappingUtils.createRuleBasedIdentifierMappingFile;
import static io.trino.plugin.jdbc.mapping.RuleBasedIdentifierMappingUtils.updateRuleBasedIdentifierMappingFile;
import static io.trino.plugin.sqlserver.SqlServerQueryRunner.createSqlServerQueryRunner;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

// With case-insensitive-name-matching enabled colliding schema/table names are considered as errors.
// Some tests here create colliding names which can cause any other concurrent test to fail.
@Test(singleThreaded = true)
public class TestSqlServerCaseInsensitiveRuleBaseMapping
        extends BaseSqlServerCaseInsensitiveMappingTest
{
    private Path mappingFile;
    private TestingSqlServer sqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        mappingFile = createRuleBasedIdentifierMappingFile();
        sqlServer = closeAfterClass(new TestingSqlServer());
        sqlServer.start();
        return createSqlServerQueryRunner(
                sqlServer,
                ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        .put("case-insensitive-name-matching", "true")
                        .put("case-insensitive-name-matching.config-file", mappingFile.toFile().getAbsolutePath())
                        .put("case-insensitive-name-matching.config-file.refresh-period", "1ms") // ~always refresh
                        .build(),
                ImmutableList.of());
    }

    @Override
    protected TestingSqlServer getSqlServer()
    {
        return requireNonNull(sqlServer, "sqlServer is null");
    }

    @Test
    public void testSchemaNameMapping()
            throws Exception
    {
        updateRuleBasedIdentifierMappingFile(
                mappingFile,
                ImmutableList.of(new SchemaMappingRule("remote_schema", "trino_schema")),
                ImmutableList.of());

        try (AutoCloseable ignore1 = getSqlServer().withSchema("remote_schema");
                AutoCloseable ignore3 = getSqlServer().withTable("remote_schema.some_table_name", "(c varchar(5))")) {
            assertThat(computeActual("SHOW SCHEMAS ")
                    .getOnlyColumn())
                    .contains("trino_schema");
            assertQuery("SHOW TABLES FROM trino_schema", "VALUES 'some_table_name'");
            assertUpdate("INSERT INTO trino_schema.some_table_name VALUES 'a'", 1);
            assertQuery("SELECT * FROM trino_schema.some_table_name", "VALUES 'a'");
        }
    }

    @Test
    public void testSchemaNameClashWithMapping()
            throws Exception
    {
        List<SchemaMappingRule> schemaMappingRules = ImmutableList.of(
                new SchemaMappingRule("casesensitivename", "casesensitivename_a"),
                new SchemaMappingRule("CaseSensitiveName", "casesensitivename_b"),
                new SchemaMappingRule("CASESENSITIVENAME", "casesensitivename_c"));
        updateRuleBasedIdentifierMappingFile(mappingFile, schemaMappingRules, ImmutableList.of());

        String[] nameVariants = {"casesensitivename", "CaseSensitiveName", "CASESENSITIVENAME"};
        assertThat(Stream.of(nameVariants)
                .map(name -> name.replace("\"", "").toLowerCase(ENGLISH))
                .collect(toImmutableSet()))
                .hasSize(1);

        for (int i = 0; i < nameVariants.length; i++) {
            for (int j = i + 1; j < nameVariants.length; j++) {
                String remoteSchema = nameVariants[i];
                String otherRemoteSchema = nameVariants[j];

                try (AutoCloseable ignore1 = getSqlServer().withSchema(remoteSchema);
                        AutoCloseable ignore2 = getSqlServer().withSchema(otherRemoteSchema);
                        AutoCloseable ignore3 = getSqlServer().withTable(remoteSchema + ".some_table_name", "(c varchar(5))")) {
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
    public void testTableNameMapping()
            throws Exception
    {
        updateRuleBasedIdentifierMappingFile(
                mappingFile,
                ImmutableList.of(),
                ImmutableList.of(new TableMappingRule("dbo", "remote_table", "trino_table")));

        try (AutoCloseable ignore1 = getSqlServer().withTable("remote_table", "(c varchar(5))")) {
            assertThat(computeActual("SHOW TABLES").getOnlyColumn())
                    .contains("trino_table");
            assertQuery("SHOW COLUMNS FROM trino_table", "SELECT 'c', 'varchar(5)', '', ''");
            assertUpdate("INSERT INTO trino_table VALUES 'dane'", 1);
            assertQuery("SELECT * FROM trino_table", "VALUES 'dane'");
        }
    }

    @Test
    public void testTableNameClashWithMapping()
            throws Exception
    {
        List<TableMappingRule> tableMappingRules = ImmutableList.of(
                new TableMappingRule("dbo", "casesensitivename", "casesensitivename_a"),
                new TableMappingRule("dbo", "CaseSensitiveName", "casesensitivename_b"),
                new TableMappingRule("dbo", "CASESENSITIVENAME", "casesensitivename_c"));
        updateRuleBasedIdentifierMappingFile(mappingFile, ImmutableList.of(), tableMappingRules);

        String[] nameVariants = {"casesensitivename", "CaseSensitiveName", "CASESENSITIVENAME"};
        assertThat(Stream.of(nameVariants)
                .map(name -> name.replace("\"", "").toLowerCase(ENGLISH))
                .collect(toImmutableSet()))
                .hasSize(1);

        for (int i = 0; i < nameVariants.length; i++) {
            for (int j = i + 1; j < nameVariants.length; j++) {
                String remoteTable = nameVariants[i];
                String otherRemoteTable = nameVariants[j];
                try (AutoCloseable ignore1 = getSqlServer().withTable(remoteTable, "(c varchar(5))");
                        AutoCloseable ignore2 = getSqlServer().withTable(otherRemoteTable, "(d varchar(5))")) {
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
    public void testSchemaAndTableNameMapping()
            throws Exception
    {
        updateRuleBasedIdentifierMappingFile(
                mappingFile,
                ImmutableList.of(new SchemaMappingRule("remote_schema", "trino_schema")),
                ImmutableList.of(new TableMappingRule("remote_schema", "remote_table", "trino_table")));

        try (AutoCloseable ignore1 = getSqlServer().withSchema("remote_schema");
                AutoCloseable ignore2 = getSqlServer().withTable("remote_schema.remote_table", "(c varchar(5))")) {
            assertThat(computeActual("SHOW SCHEMAS").getOnlyColumn())
                    .contains("trino_schema");
            assertThat(computeActual("SHOW TABLES IN trino_schema").getOnlyColumn())
                    .contains("trino_table");
            assertQuery("SHOW COLUMNS FROM trino_schema.trino_table", "SELECT 'c', 'varchar(5)', '', ''");
            assertUpdate("INSERT INTO trino_schema.trino_table VALUES 'dane'", 1);
            assertQuery("SELECT * FROM trino_schema.trino_table", "VALUES 'dane'");
        }
    }
}
