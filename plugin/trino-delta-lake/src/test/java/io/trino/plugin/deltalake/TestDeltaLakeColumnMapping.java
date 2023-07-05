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
package io.trino.plugin.deltalake;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.ObjectMapperProvider;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.createDeltaLakeQueryRunner;
import static io.trino.plugin.deltalake.DeltaTestingConnectorSession.SESSION;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.TransactionLogTail.getEntriesFromJson;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.testing.DataProviders.toDataProvider;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeColumnMapping
        extends AbstractTestQueryFramework
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();
    // The col-{uuid} pattern for delta.columnMapping.physicalName
    private static final Pattern PHYSICAL_COLUMN_NAME_PATTERN = Pattern.compile("^col-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createDeltaLakeQueryRunner(DELTA_CATALOG, ImmutableMap.of(), ImmutableMap.of("delta.enable-non-concurrent-writes", "true"));
    }

    @Test(dataProvider = "columnMappingModeDataProvider")
    public void testCreateTableWithColumnMappingMode(String columnMappingMode)
            throws Exception
    {
        testCreateTableColumnMappingMode(tableName -> {
            assertUpdate("CREATE TABLE " + tableName + "(a_int integer, a_row row(x integer)) WITH (column_mapping_mode='" + columnMappingMode + "')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, row(11))", 1);
        });
    }

    @Test(dataProvider = "columnMappingModeDataProvider")
    public void testCreateTableAsSelectWithColumnMappingMode(String columnMappingMode)
            throws Exception
    {
        testCreateTableColumnMappingMode(tableName ->
                assertUpdate("CREATE TABLE " + tableName + " WITH (column_mapping_mode='" + columnMappingMode + "')" +
                        " AS SELECT 1 AS a_int, CAST(row(11) AS row(x integer)) AS a_row", 1));
    }

    private void testCreateTableColumnMappingMode(Consumer<String> createTable)
            throws IOException
    {
        String tableName = "test_create_table_column_mapping_mode_" + randomNameSuffix();
        createTable.accept(tableName);

        assertThat(query("SELECT * FROM " + tableName))
                .matches("VALUES (1, CAST(row(11) AS row(x integer)))");

        String tableLocation = getTableLocation(tableName);
        MetadataEntry metadata = loadMetadataEntry(0, Path.of(tableLocation));

        assertThat(metadata.getConfiguration().get("delta.columnMapping.maxColumnId"))
                .isEqualTo("3"); // 3 comes from a_int + a_row + a_row.x

        JsonNode schema = OBJECT_MAPPER.readTree(metadata.getSchemaString());
        List<JsonNode> fields = ImmutableList.copyOf(schema.get("fields").elements());
        assertThat(fields).hasSize(2);
        JsonNode intColumn = fields.get(0);
        JsonNode rowColumn = fields.get(1);
        List<JsonNode> rowFields = ImmutableList.copyOf(rowColumn.get("type").get("fields").elements());
        assertThat(rowFields).hasSize(1);
        JsonNode nestedInt = rowFields.get(0);

        // Verify delta.columnMapping.id and delta.columnMapping.physicalName values
        assertThat(intColumn.get("metadata").get("delta.columnMapping.id").asInt()).isEqualTo(1);
        assertThat(intColumn.get("metadata").get("delta.columnMapping.physicalName").asText()).containsPattern(PHYSICAL_COLUMN_NAME_PATTERN);

        assertThat(rowColumn.get("metadata").get("delta.columnMapping.id").asInt()).isEqualTo(3);
        assertThat(rowColumn.get("metadata").get("delta.columnMapping.physicalName").asText()).containsPattern(PHYSICAL_COLUMN_NAME_PATTERN);

        assertThat(nestedInt.get("metadata").get("delta.columnMapping.id").asInt()).isEqualTo(2);
        assertThat(nestedInt.get("metadata").get("delta.columnMapping.physicalName").asText()).containsPattern(PHYSICAL_COLUMN_NAME_PATTERN);

        assertUpdate("DROP TABLE " + tableName);
    }

    @DataProvider
    public Object[][] columnMappingModeDataProvider()
    {
        return ImmutableList.of("id", "name").stream()
                .collect(toDataProvider());
    }

    private String getTableLocation(String tableName)
    {
        Pattern locationPattern = Pattern.compile(".*location = '(.*?)'.*", Pattern.DOTALL);
        Matcher m = locationPattern.matcher((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue());
        if (m.find()) {
            String location = m.group(1);
            verify(!m.find(), "Unexpected second match");
            return location;
        }
        throw new IllegalStateException("Location not found in SHOW CREATE TABLE result");
    }

    private static MetadataEntry loadMetadataEntry(long entryNumber, Path tableLocation)
            throws IOException
    {
        TrinoFileSystem fileSystem = new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS).create(SESSION);
        DeltaLakeTransactionLogEntry transactionLog = getEntriesFromJson(entryNumber, tableLocation.resolve("_delta_log").toString(), fileSystem).orElseThrow().stream()
                .filter(log -> log.getMetaData() != null)
                .collect(onlyElement());
        return transactionLog.getMetaData();
    }
}
