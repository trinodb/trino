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
package io.trino.plugin.kudu;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.base.mapping.TableMappingRule;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.kudu.client.KuduClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.List;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.base.mapping.RuleBasedIdentifierMappingUtils.REFRESH_PERIOD_DURATION;
import static io.trino.plugin.base.mapping.RuleBasedIdentifierMappingUtils.createRuleBasedIdentifierMappingFile;
import static io.trino.plugin.base.mapping.RuleBasedIdentifierMappingUtils.updateRuleBasedIdentifierMappingFile;
import static io.trino.plugin.kudu.KuduQueryRunnerFactory.createKuduClient;
import static io.trino.plugin.kudu.KuduQueryRunnerFactory.createKuduQueryRunner;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestKuduCaseInsensitiveMapping
        extends AbstractTestQueryFramework
{
    private static final String DEFAULT_SCHEMA = "default";
    private KuduClient kuduClient;
    private TestingKuduServer kuduServer;
    private Path mappingFile;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        mappingFile = createRuleBasedIdentifierMappingFile();
        kuduServer = new TestingKuduServer();
        kuduClient = createKuduClient(kuduServer);
        return createKuduQueryRunner(DEFAULT_SCHEMA, ImmutableMap.<String, String>builder()
                .put("kudu.schema-emulation.enabled", "false")
                .put("kudu.client.master-addresses", kuduServer.getMasterAddress().toString())
                .put("case-insensitive-name-matching", "true")
                // disable remote identifiers cache,
                // to prevent failures in case of clash names in cache,
                // during tests runs
                .put("case-insensitive-name-matching.cache-ttl", "0ms")
                .put("case-insensitive-name-matching.config-file", mappingFile.toFile().getAbsolutePath())
                .put("case-insensitive-name-matching.config-file.refresh-period", REFRESH_PERIOD_DURATION.toString())
                .buildOrThrow());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws Exception
    {
        kuduServer.close();
        kuduServer = null;
        kuduClient.close();
        kuduClient = null;
    }

    @Test
    public void testNonLowerCaseTableName()
            throws Exception
    {
        String schemaName = "default";
        String schemaNameLowerCase = schemaName.toLowerCase(ENGLISH);
        String schemaNameUpperCase = schemaName.toUpperCase(ENGLISH);
        String tableName = "NonLowerCaseTable" + randomNameSuffix();
        String tableNameLowerCase = tableName.toLowerCase(ENGLISH);
        ImmutableList.Builder<KuduTestColumn> builder = ImmutableList.builder();
        ImmutableList<KuduTestColumn> kuduTableColumns = builder
                .add(new KuduTestColumn(BIGINT, "id", "1", true))
                .add(new KuduTestColumn(VARCHAR, "lower_case_name", "a"))
                .add(new KuduTestColumn(VARCHAR, "Mixed_Case_Name", "b"))
                .add(new KuduTestColumn(VARCHAR, "UPPER_CASE_NAME", "c"))
                .build();
        try (AutoCloseable ignoreSchema = withSchema(schemaName);
                AutoCloseable ignoreTable = withTable(
                        tableName,
                        kuduTableColumns)) {
            assertQuery(
                    format("SELECT column_name FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'", schemaName, tableNameLowerCase),
                    "VALUES 'id', 'lower_case_name', 'mixed_case_name', 'upper_case_name'");
            assertQuery(
                    format("SELECT column_name FROM information_schema.columns WHERE table_name = '%s'", tableNameLowerCase),
                    "VALUES 'id', 'lower_case_name', 'mixed_case_name', 'upper_case_name'");
            assertThat(query(format("SHOW COLUMNS FROM %s.%s", schemaName, tableNameLowerCase)))
                    .projected("Column")
                    .skippingTypesCheck()
                    .matches("VALUES 'id', 'lower_case_name', 'mixed_case_name', 'upper_case_name'");

            assertQuery(format("SELECT lower_case_name FROM %s.%s", schemaNameLowerCase, tableNameLowerCase), "VALUES 'a'");
            assertQuery(format("SELECT mixed_case_name FROM %s.%s", schemaNameLowerCase, tableNameLowerCase), "VALUES 'b'");
            assertQuery(format("SELECT upper_case_name FROM %s.%s", schemaNameLowerCase, tableNameLowerCase), "VALUES 'c'");
            assertQuery(format("SELECT upper_case_name FROM %s.%s", schemaNameUpperCase, tableName), "VALUES 'c'");
            assertQuery(format("SELECT upper_case_name FROM \"%s\".\"%s\"", schemaNameUpperCase, tableName), "VALUES 'c'");
        }
    }

    @Test
    public void testTableNameClash()
            throws Exception
    {
        String schemaName = "default";
        ImmutableList.Builder<KuduTestColumn> builder = ImmutableList.builder();
        ImmutableList<KuduTestColumn> kuduTableColumns = builder
                .add(new KuduTestColumn(BIGINT, "id", "1", true))
                .build();
        try (AutoCloseable ignoreSchema = withSchema(schemaName);
                AutoCloseable ignoreTable = withTable("casesensitivename", kuduTableColumns);
                AutoCloseable ignoreOtherTable = withTable("CaseSensitiveName", kuduTableColumns);
                AutoCloseable ignoreSomeable = withTable("some_table", kuduTableColumns)) {
            assertThat(computeActual("SHOW TABLES FROM " + schemaName).getOnlyColumn()).filteredOn("casesensitivename"::equals).hasSize(1);
            assertQueryFails(format("SELECT * FROM %s.casesensitivename", schemaName), "Failed to find remote table name: Ambiguous name: casesensitivename");
            assertQuery(format("SELECT * FROM %s.some_table", schemaName), "VALUES '1'");
        }
    }

    @Test
    public void testTableNameRuleMapping()
            throws Exception
    {
        String schema = "default";
        updateRuleBasedIdentifierMappingFile(
                mappingFile,
                ImmutableList.of(),
                ImmutableList.of(new TableMappingRule("", "remote_table", "trino_table")));
        ImmutableList.Builder<KuduTestColumn> builder = ImmutableList.builder();
        ImmutableList<KuduTestColumn> kuduTableColumns = builder
                .add(new KuduTestColumn(BIGINT, "id", "1", true))
                .build();

        try (AutoCloseable ignoreSchema = withSchema(schema);
                AutoCloseable ignoreTable = withTable("remote_table", kuduTableColumns)) {
            assertThat(computeActual("SHOW TABLES FROM " + schema).getOnlyColumn())
                    .contains("trino_table");
            assertQuery("SELECT * FROM " + schema + ".trino_table", "VALUES '1'");
        }
    }

    @Test
    public void testTableNameClashWithRuleMapping()
            throws Exception
    {
        String schema = "default";
        List<TableMappingRule> tableMappingRules = ImmutableList.of(
                new TableMappingRule("", "casesensitivename", "casesensitivename_a"),
                new TableMappingRule("", "CaseSensitiveName", "casesensitivename_b"));
        updateRuleBasedIdentifierMappingFile(mappingFile, ImmutableList.of(), tableMappingRules);

        ImmutableList.Builder<KuduTestColumn> builder = ImmutableList.builder();
        ImmutableList<KuduTestColumn> kuduTableColumns = builder
                .add(new KuduTestColumn(BIGINT, "id", "1", true))
                .build();

        String remoteTable = "casesensitivename";
        String otherRemoteTable = "CaseSensitiveName";
        try (AutoCloseable ignoreSchema = withSchema(schema);
                AutoCloseable ignoreTable = withTable(remoteTable, kuduTableColumns);
                AutoCloseable ignoreOtherTable = withTable(otherRemoteTable, kuduTableColumns)) {
            String table = tableMappingRules.stream()
                    .filter(rule -> rule.getRemoteTable().equals(remoteTable))
                    .map(TableMappingRule::getMapping)
                    .collect(onlyElement());

            assertThat(computeActual("SHOW TABLES FROM " + schema).getOnlyColumn())
                    .map(String.class::cast)
                    .filteredOn(anObject -> anObject.startsWith("casesensitivename_"))
                    .hasSize(2);
            assertQuery("SELECT * FROM " + schema + "." + table, "VALUES '1'");
        }
    }

    @Test
    public void testCaseInsensitiveRenameTable()
            throws Exception
    {
        String schemaName = "default";
        ImmutableList.Builder<KuduTestColumn> builder = ImmutableList.builder();
        ImmutableList<KuduTestColumn> kuduTableColumns = builder
                .add(new KuduTestColumn(BIGINT, "id", "1", true))
                .build();
        try (AutoCloseable ignoreSchema = withSchema(schemaName);
                AutoCloseable ignoreTableLoweCase = withTable("testInsensitive_RenameTable", kuduTableColumns)) {
            assertQuery(format("SHOW TABLES FROM %s", schemaName), "VALUES 'testinsensitive_renametable'");
            assertQuery(format("SELECT * FROM %s.testinsensitive_renametable", schemaName), "VALUES '1'");

            assertUpdate(format("ALTER TABLE %s.testinsensitive_renametable RENAME TO %s.testinsensitive_renamed_table", schemaName, schemaName));

            assertQuery(format("SHOW TABLES IN %s", schemaName), "SELECT 'testinsensitive_renamed_table'");
            assertQuery(format("SELECT * FROM %s.testinsensitive_renamed_table", schemaName), "VALUES '1'");
        }
    }

    @Test
    public void testDropAndAddRangePartition()
            throws Exception
    {
        String schemaName = "default";
        ImmutableList.Builder<KuduTestColumn> builder = ImmutableList.builder();
        ImmutableList<KuduTestColumn> kuduTableColumns = builder
                .add(new KuduTestColumn(BIGINT, "id", "1", true))
                .add(new KuduTestColumn(BIGINT, "key", "1"))
                .build();
        try (AutoCloseable ignoreSchema = withSchema(schemaName);
                AutoCloseable ignoreTableLowerCase = withTable("Some_Table", kuduTableColumns)) {
            assertUpdate(format("CALL kudu.system.drop_range_partition('%s', '%s', '%s')", schemaName, "some_table", "{\"lower\": null, \"upper\": null}"));
            assertUpdate(format("CALL kudu.system.add_range_partition('%s', '%s', '%s')", schemaName, "some_table", "{\"lower\": 0, \"upper\": 1000}"));
        }
    }

    private AutoCloseable withSchema(String schemaName)
    {
        return () -> {
            // no-op without schema emulation enabled.
        };
    }

    private AutoCloseable withTable(String remoteTableName, List<KuduTestColumn> items)
    {
        return new KuduTestTable(
                remoteTableName,
                items,
                kuduClient);
    }
}
