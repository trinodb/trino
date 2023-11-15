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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.io.Resources;
import io.airlift.json.ObjectMapperProvider;
import io.trino.Session;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.filesystem.local.LocalInputFile;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.reader.MetadataReader;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeFileStatistics;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.parquet.TrinoParquetDataSource;
import io.trino.spi.type.TimeZoneKey;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingSession;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterators.getOnlyElement;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.createDeltaLakeQueryRunner;
import static io.trino.plugin.deltalake.DeltaTestingConnectorSession.SESSION;
import static io.trino.plugin.deltalake.TestingDeltaLakeUtils.copyDirectoryContents;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getColumnsMetadata;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.TransactionLogTail.getEntriesFromJson;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestDeltaLakeBasic
        extends AbstractTestQueryFramework
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    private static final List<ResourceTable> PERSON_TABLES = ImmutableList.of(
            new ResourceTable("person", "databricks73/person"),
            new ResourceTable("person_without_last_checkpoint", "databricks73/person_without_last_checkpoint"),
            new ResourceTable("person_without_old_jsons", "databricks73/person_without_old_jsons"),
            new ResourceTable("person_without_checkpoints", "databricks73/person_without_checkpoints"));
    private static final List<ResourceTable> OTHER_TABLES = ImmutableList.of(
            new ResourceTable("stats_with_minmax_nulls", "deltalake/stats_with_minmax_nulls"),
            new ResourceTable("no_column_stats", "databricks73/no_column_stats"),
            new ResourceTable("deletion_vectors", "databricks122/deletion_vectors"),
            new ResourceTable("timestamp_ntz", "databricks131/timestamp_ntz"),
            new ResourceTable("timestamp_ntz_partition", "databricks131/timestamp_ntz_partition"));

    // The col-{uuid} pattern for delta.columnMapping.physicalName
    private static final Pattern PHYSICAL_COLUMN_NAME_PATTERN = Pattern.compile("^col-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

    private static final TrinoFileSystem FILE_SYSTEM = new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS).create(SESSION);

    private final ZoneId jvmZone = ZoneId.systemDefault();
    private final ZoneId vilnius = ZoneId.of("Europe/Vilnius");
    private final ZoneId kathmandu = ZoneId.of("Asia/Kathmandu");

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createDeltaLakeQueryRunner(DELTA_CATALOG, ImmutableMap.of(), ImmutableMap.of(
                "delta.register-table-procedure.enabled", "true",
                "delta.enable-non-concurrent-writes", "true"));
    }

    @BeforeAll
    public void registerTables()
    {
        for (ResourceTable table : Iterables.concat(PERSON_TABLES, OTHER_TABLES)) {
            String dataPath = getResourceLocation(table.resourcePath()).toExternalForm();
            getQueryRunner().execute(
                    format("CALL system.register_table('%s', '%s', '%s')", getSession().getSchema().orElseThrow(), table.tableName(), dataPath));
        }
    }

    private URL getResourceLocation(String resourcePath)
    {
        return getClass().getClassLoader().getResource(resourcePath);
    }

    @Test
    public void testDescribeTable()
    {
        for (ResourceTable table : PERSON_TABLES) {
            // the schema is actually defined in the transaction log
            assertQuery(
                    format("DESCRIBE %s", table.tableName()),
                    "VALUES " +
                            "('name', 'varchar', '', ''), " +
                            "('age', 'integer', '', ''), " +
                            "('married', 'boolean', '', ''), " +
                            "('gender', 'varchar', '', ''), " +
                            "('phones', 'array(row(number varchar, label varchar))', '', ''), " +
                            "('address', 'row(street varchar, city varchar, state varchar, zip varchar)', '', ''), " +
                            "('income', 'double', '', '')");
        }
    }

    @Test
    public void testSimpleQueries()
    {
        for (ResourceTable table : PERSON_TABLES) {
            assertQuery(format("SELECT COUNT(*) FROM %s", table.tableName()), "VALUES 12");
            assertQuery(format("SELECT income FROM %s WHERE name = 'Bob'", table.tableName()), "VALUES 99000.00");
            assertQuery(format("SELECT name FROM %s WHERE name LIKE 'B%%'", table.tableName()), "VALUES ('Bob'), ('Betty')");
            assertQuery(format("SELECT DISTINCT gender FROM %s", table.tableName()), "VALUES ('M'), ('F'), (null)");
            assertQuery(format("SELECT DISTINCT age FROM %s", table.tableName()), "VALUES (21), (25), (28), (29), (30), (42)");
            assertQuery(format("SELECT name FROM %s WHERE age = 42", table.tableName()), "VALUES ('Alice'), ('Emma')");
        }
    }

    @Test
    public void testNoColumnStats()
    {
        // The table was created with delta.dataSkippingNumIndexedCols=0 property
        assertQuery("SELECT c_str FROM no_column_stats WHERE c_int = 42", "VALUES 'foo'");
    }

    /**
     * @see deltalake.column_mapping_mode_id
     * @see deltalake.column_mapping_mode_name
     */
    @Test
    public void testAddNestedColumnWithColumnMappingMode()
            throws Exception
    {
        testAddNestedColumnWithColumnMappingMode("id");
        testAddNestedColumnWithColumnMappingMode("name");
    }

    private void testAddNestedColumnWithColumnMappingMode(String columnMappingMode)
            throws Exception
    {
        // The table contains 'x' column with column mapping mode
        String tableName = "test_add_column_" + randomNameSuffix();
        Path tableLocation = Files.createTempFile(tableName, null);
        copyDirectoryContents(new File(Resources.getResource("deltalake/column_mapping_mode_" + columnMappingMode).toURI()).toPath(), tableLocation);

        assertUpdate("CALL system.register_table('%s', '%s', '%s')".formatted(getSession().getSchema().orElseThrow(), tableName, tableLocation.toUri()));
        assertThat(query("DESCRIBE " + tableName)).projected("Column", "Type").skippingTypesCheck().matches("VALUES ('x', 'integer')");
        assertQueryReturnsEmptyResult("SELECT * FROM " + tableName);

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN second_col row(a array(integer), b map(integer, integer), c row(field integer))");
        MetadataEntry metadata = loadMetadataEntry(1, tableLocation);
        assertThat(metadata.getConfiguration().get("delta.columnMapping.maxColumnId"))
                .isEqualTo("6"); // +5 comes from second_col + second_col.a + second_col.b + second_col.c + second_col.c.field

        JsonNode schema = OBJECT_MAPPER.readTree(metadata.getSchemaString());
        List<JsonNode> fields = ImmutableList.copyOf(schema.get("fields").elements());
        assertThat(fields).hasSize(2);
        JsonNode columnX = fields.get(0);
        JsonNode columnY = fields.get(1);

        List<JsonNode> rowFields = ImmutableList.copyOf(columnY.get("type").get("fields").elements());
        assertThat(rowFields).hasSize(3);
        JsonNode nestedArray = rowFields.get(0);
        JsonNode nestedMap = rowFields.get(1);
        JsonNode nestedRow = rowFields.get(2);

        // Verify delta.columnMapping.id and delta.columnMapping.physicalName values
        assertThat(columnX.get("metadata").get("delta.columnMapping.id").asInt()).isEqualTo(1);
        assertThat(columnX.get("metadata").get("delta.columnMapping.physicalName").asText()).containsPattern(PHYSICAL_COLUMN_NAME_PATTERN);
        assertThat(columnY.get("metadata").get("delta.columnMapping.id").asInt()).isEqualTo(6);
        assertThat(columnY.get("metadata").get("delta.columnMapping.physicalName").asText()).containsPattern(PHYSICAL_COLUMN_NAME_PATTERN);

        assertThat(nestedArray.get("metadata").get("delta.columnMapping.id").asInt()).isEqualTo(2);
        assertThat(nestedArray.get("metadata").get("delta.columnMapping.physicalName").asText()).containsPattern(PHYSICAL_COLUMN_NAME_PATTERN);

        assertThat(nestedMap.get("metadata").get("delta.columnMapping.id").asInt()).isEqualTo(3);
        assertThat(nestedMap.get("metadata").get("delta.columnMapping.physicalName").asText()).containsPattern(PHYSICAL_COLUMN_NAME_PATTERN);

        assertThat(nestedRow.get("metadata").get("delta.columnMapping.id").asInt()).isEqualTo(5);
        assertThat(nestedRow.get("metadata").get("delta.columnMapping.physicalName").asText()).containsPattern(PHYSICAL_COLUMN_NAME_PATTERN);
        assertThat(getOnlyElement(nestedRow.get("type").get("fields").elements()).get("metadata").get("delta.columnMapping.id").asInt()).isEqualTo(4);
        assertThat(getOnlyElement(nestedRow.get("type").get("fields").elements()).get("metadata").get("delta.columnMapping.physicalName").asText()).containsPattern(PHYSICAL_COLUMN_NAME_PATTERN);

        // Repeat adding a new column and verify the existing fields are preserved
        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN third_col row(a array(integer), b map(integer, integer), c row(field integer))");
        MetadataEntry thirdMetadata = loadMetadataEntry(2, tableLocation);
        JsonNode latestSchema = OBJECT_MAPPER.readTree(thirdMetadata.getSchemaString());
        List<JsonNode> latestFields = ImmutableList.copyOf(latestSchema.get("fields").elements());
        assertThat(latestFields).hasSize(3);
        JsonNode latestColumnX = latestFields.get(0);
        JsonNode latestColumnY = latestFields.get(1);
        assertThat(latestColumnX).isEqualTo(columnX);
        assertThat(latestColumnY).isEqualTo(columnY);

        assertThat(thirdMetadata.getConfiguration())
                .containsEntry("delta.columnMapping.maxColumnId", "11");
        assertThat(thirdMetadata.getSchemaString())
                .containsPattern("(delta\\.columnMapping\\.id.*?){11}")
                .containsPattern("(delta\\.columnMapping\\.physicalName.*?){11}");
    }

    /**
     * @see deltalake.column_mapping_mode_id
     * @see deltalake.column_mapping_mode_name
     */
    @Test
    public void testOptimizeWithColumnMappingMode()
            throws Exception
    {
        testOptimizeWithColumnMappingMode("id");
        testOptimizeWithColumnMappingMode("name");
    }

    private void testOptimizeWithColumnMappingMode(String columnMappingMode)
            throws Exception
    {
        // The table contains 'x' column with column mapping mode
        String tableName = "test_optimize_" + randomNameSuffix();
        Path tableLocation = Files.createTempFile(tableName, null);
        copyDirectoryContents(new File(Resources.getResource("deltalake/column_mapping_mode_" + columnMappingMode).toURI()).toPath(), tableLocation);

        assertUpdate("CALL system.register_table('%s', '%s', '%s')".formatted(getSession().getSchema().orElseThrow(), tableName, tableLocation.toUri()));
        assertThat(query("DESCRIBE " + tableName)).projected("Column", "Type").skippingTypesCheck().matches("VALUES ('x', 'integer')");
        assertQueryReturnsEmptyResult("SELECT * FROM " + tableName);

        MetadataEntry originalMetadata = loadMetadataEntry(0, tableLocation);
        JsonNode schema = OBJECT_MAPPER.readTree(originalMetadata.getSchemaString());
        List<JsonNode> fields = ImmutableList.copyOf(schema.get("fields").elements());
        assertThat(fields).hasSize(1);
        JsonNode column = fields.get(0);
        String physicalName = column.get("metadata").get("delta.columnMapping.physicalName").asText();
        int id = column.get("metadata").get("delta.columnMapping.id").asInt();

        assertUpdate("INSERT INTO " + tableName + " VALUES 10", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES 20", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES NULL", 1);
        // For optimize we need to set task_min_writer_count to 1, otherwise it will create more than one file.
        assertUpdate(Session.builder(getQueryRunner().getDefaultSession())
                        .setSystemProperty("task_min_writer_count", "1")
                        .build(),
                "ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");

        // Verify 'add' entry contains the expected physical name in the stats
        List<DeltaLakeTransactionLogEntry> transactionLog = getEntriesFromJson(4, tableLocation.resolve("_delta_log").toString(), FILE_SYSTEM).orElseThrow();
        assertThat(transactionLog).hasSize(5);
        assertThat(transactionLog.get(0).getCommitInfo()).isNotNull();
        assertThat(transactionLog.get(1).getRemove()).isNotNull();
        assertThat(transactionLog.get(2).getRemove()).isNotNull();
        assertThat(transactionLog.get(3).getRemove()).isNotNull();
        assertThat(transactionLog.get(4).getAdd()).isNotNull();
        AddFileEntry addFileEntry = transactionLog.get(4).getAdd();
        DeltaLakeFileStatistics stats = addFileEntry.getStats().orElseThrow();
        assertThat(stats.getMinValues().orElseThrow().get(physicalName)).isEqualTo(10);
        assertThat(stats.getMaxValues().orElseThrow().get(physicalName)).isEqualTo(20);
        assertThat(stats.getNullCount(physicalName).orElseThrow()).isEqualTo(1);

        // Verify optimized parquet file contains the expected physical id and name
        TrinoInputFile inputFile = new LocalInputFile(tableLocation.resolve(addFileEntry.getPath()).toFile());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(
                    new TrinoParquetDataSource(inputFile, new ParquetReaderOptions(), new FileFormatDataSourceStats()),
                    Optional.empty());
        FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
        PrimitiveType physicalType = getOnlyElement(fileMetaData.getSchema().getColumns().iterator()).getPrimitiveType();
        assertThat(physicalType.getName()).isEqualTo(physicalName);
        if (columnMappingMode.equals("id")) {
            assertThat(physicalType.getId().intValue()).isEqualTo(id);
        }
        else {
            assertThat(physicalType.getId()).isNull();
        }
    }

    /**
     * @see deltalake.column_mapping_mode_id
     * @see deltalake.column_mapping_mode_name
     */
    @Test
    public void testDropColumnWithColumnMappingMode()
            throws Exception
    {
        testDropColumnWithColumnMappingMode("id");
        testDropColumnWithColumnMappingMode("name");
    }

    private void testDropColumnWithColumnMappingMode(String columnMappingMode)
            throws Exception
    {
        // The table contains 'x' column with column mapping mode
        String tableName = "test_add_column_" + randomNameSuffix();
        Path tableLocation = Files.createTempFile(tableName, null);
        copyDirectoryContents(new File(Resources.getResource("deltalake/column_mapping_mode_" + columnMappingMode).toURI()).toPath(), tableLocation);

        assertUpdate("CALL system.register_table('%s', '%s', '%s')".formatted(getSession().getSchema().orElseThrow(), tableName, tableLocation.toUri()));
        assertThat(query("DESCRIBE " + tableName)).projected("Column", "Type").skippingTypesCheck().matches("VALUES ('x', 'integer')");
        assertQueryReturnsEmptyResult("SELECT * FROM " + tableName);

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN second_col row(a array(integer), b map(integer, integer), c row(field integer))");
        MetadataEntry metadata = loadMetadataEntry(1, tableLocation);
        assertThat(metadata.getConfiguration().get("delta.columnMapping.maxColumnId"))
                .isEqualTo("6"); // +5 comes from second_col + second_col.a + second_col.b + second_col.c + second_col.c.field
        assertThat(metadata.getSchemaString())
                .containsPattern("(delta\\.columnMapping\\.id.*?){6}")
                .containsPattern("(delta\\.columnMapping\\.physicalName.*?){6}");

        JsonNode schema = OBJECT_MAPPER.readTree(metadata.getSchemaString());
        List<JsonNode> fields = ImmutableList.copyOf(schema.get("fields").elements());
        assertThat(fields).hasSize(2);
        JsonNode nestedColumn = fields.get(1);
        List<JsonNode> rowFields = ImmutableList.copyOf(nestedColumn.get("type").get("fields").elements());
        assertThat(rowFields).hasSize(3);

        // Drop 'x' column and verify that nested metadata and table configuration are preserved
        assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN x");

        MetadataEntry droppedMetadata = loadMetadataEntry(2, tableLocation);
        JsonNode droppedSchema = OBJECT_MAPPER.readTree(droppedMetadata.getSchemaString());
        List<JsonNode> droppedFields = ImmutableList.copyOf(droppedSchema.get("fields").elements());
        assertThat(droppedFields).hasSize(1);
        assertThat(droppedFields.get(0)).isEqualTo(nestedColumn);

        assertThat(droppedMetadata.getConfiguration())
                .isEqualTo(metadata.getConfiguration());
        assertThat(droppedMetadata.getSchemaString())
                .containsPattern("(delta\\.columnMapping\\.id.*?){5}")
                .containsPattern("(delta\\.columnMapping\\.physicalName.*?){5}");
    }

    /**
     * @see deltalake.column_mapping_mode_id
     * @see deltalake.column_mapping_mode_name
     */
    @Test
    public void testRenameColumnWithColumnMappingMode()
            throws Exception
    {
        testRenameColumnWithColumnMappingMode("id");
        testRenameColumnWithColumnMappingMode("name");
    }

    private void testRenameColumnWithColumnMappingMode(String columnMappingMode)
            throws Exception
    {
        // The table contains 'x' column with column mapping mode
        String tableName = "test_rename_column_" + randomNameSuffix();
        Path tableLocation = Files.createTempFile(tableName, null);
        copyDirectoryContents(new File(Resources.getResource("deltalake/column_mapping_mode_" + columnMappingMode).toURI()).toPath(), tableLocation);

        assertUpdate("CALL system.register_table('%s', '%s', '%s')".formatted(getSession().getSchema().orElseThrow(), tableName, tableLocation.toUri()));
        assertQueryReturnsEmptyResult("SELECT * FROM " + tableName);

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN second_col row(a array(integer), b map(integer, integer), c row(field integer))");
        MetadataEntry metadata = loadMetadataEntry(1, tableLocation);
        assertThat(metadata.getConfiguration().get("delta.columnMapping.maxColumnId"))
                .isEqualTo("6"); // +5 comes from second_col + second_col.a + second_col.b + second_col.c + second_col.c.field
        assertThat(metadata.getSchemaString())
                .containsPattern("(delta\\.columnMapping\\.id.*?){6}")
                .containsPattern("(delta\\.columnMapping\\.physicalName.*?){6}");

        JsonNode schema = OBJECT_MAPPER.readTree(metadata.getSchemaString());
        List<JsonNode> fields = ImmutableList.copyOf(schema.get("fields").elements());
        assertThat(fields).hasSize(2);
        JsonNode integerColumn = fields.get(0);
        JsonNode nestedColumn = fields.get(1);
        List<JsonNode> rowFields = ImmutableList.copyOf(nestedColumn.get("type").get("fields").elements());
        assertThat(rowFields).hasSize(3);

        // Rename 'second_col' column and verify that nested metadata are same except for 'name' field and the table configuration are preserved
        assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN second_col TO renamed_col");

        MetadataEntry renamedMetadata = loadMetadataEntry(2, tableLocation);
        JsonNode renamedSchema = OBJECT_MAPPER.readTree(renamedMetadata.getSchemaString());
        List<JsonNode> renamedFields = ImmutableList.copyOf(renamedSchema.get("fields").elements());
        assertThat(renamedFields).hasSize(2);
        assertThat(renamedFields.get(0)).isEqualTo(integerColumn);
        assertThat(renamedFields.get(1)).isNotEqualTo(nestedColumn);
        JsonNode renamedColumn = ((ObjectNode) nestedColumn).put("name", "renamed_col");
        assertThat(renamedFields.get(1)).isEqualTo(renamedColumn);

        assertThat(renamedMetadata.getConfiguration())
                .isEqualTo(metadata.getConfiguration());
        assertThat(renamedMetadata.getSchemaString())
                .containsPattern("(delta\\.columnMapping\\.id.*?){6}")
                .containsPattern("(delta\\.columnMapping\\.physicalName.*?){6}");
    }

    /**
     * @see deltalake.column_mapping_mode_id
     * @see deltalake.column_mapping_mode_name
     */
    @Test
    public void testWriterAfterRenameColumnWithColumnMappingMode()
            throws Exception
    {
        testWriterAfterRenameColumnWithColumnMappingMode("id");
        testWriterAfterRenameColumnWithColumnMappingMode("name");
    }

    private void testWriterAfterRenameColumnWithColumnMappingMode(String columnMappingMode)
            throws Exception
    {
        String tableName = "test_writer_after_rename_column_" + randomNameSuffix();
        Path tableLocation = Files.createTempFile(tableName, null);
        copyDirectoryContents(new File(Resources.getResource("deltalake/column_mapping_mode_" + columnMappingMode).toURI()).toPath(), tableLocation);

        assertUpdate("CALL system.register_table('%s', '%s', '%s')".formatted(getSession().getSchema().orElseThrow(), tableName, tableLocation.toUri()));
        assertQueryReturnsEmptyResult("SELECT * FROM " + tableName);

        assertUpdate("INSERT INTO " + tableName + " VALUES 1", 1);
        assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN x to new_x");
        assertQuery("SELECT * FROM " + tableName, "VALUES 1");

        assertUpdate("UPDATE " + tableName + " SET new_x = 2", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES 2");

        assertUpdate("MERGE INTO " + tableName + " USING (VALUES 42) t(dummy) ON false " +
                " WHEN NOT MATCHED THEN INSERT VALUES (3)", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES 2, 3");

        assertUpdate("DELETE FROM " + tableName + " WHERE new_x = 2", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES 3");

        assertUpdate("DROP TABLE " + tableName);
    }

    /**
     * @see deltalake.case_sensitive
     */
    @Test
    public void testRequiresQueryPartitionFilterWithUppercaseColumnName()
            throws Exception
    {
        String tableName = "test_require_partition_filter_" + randomNameSuffix();
        Path tableLocation = Files.createTempFile(tableName, null);
        copyDirectoryContents(new File(Resources.getResource("deltalake/case_sensitive").toURI()).toPath(), tableLocation);

        assertUpdate("CALL system.register_table('%s', '%s', '%s')".formatted(getSession().getSchema().orElseThrow(), tableName, tableLocation.toUri()));
        assertQueryReturnsEmptyResult("SELECT * FROM " + tableName);

        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 11), (2, 22)", 2);

        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 11), (2, 22)");

        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "query_partition_filter_required", "true")
                .build();

        assertQuery(session, format("SELECT * FROM %s WHERE \"part\" = 11", tableName), "VALUES (1, 11)");
        assertQuery(session, format("SELECT * FROM %s WHERE \"PART\" = 11", tableName), "VALUES (1, 11)");
        assertQuery(session, format("SELECT * FROM %s WHERE \"Part\" = 11", tableName), "VALUES (1, 11)");

        assertUpdate("DROP TABLE " + tableName);
    }

    /**
     * @see deltalake.case_sensitive
     */
    @Test
    public void testStatisticsWithColumnCaseSensitivity()
            throws Exception
    {
        String tableName = "test_column_case_sensitivity_" + randomNameSuffix();
        Path tableLocation = Files.createTempFile(tableName, null);
        copyDirectoryContents(new File(Resources.getResource("deltalake/case_sensitive").toURI()).toPath(), tableLocation);

        assertUpdate("CALL system.register_table('%s', '%s', '%s')".formatted(getSession().getSchema().orElseThrow(), tableName, tableLocation.toUri()));
        assertQueryReturnsEmptyResult("SELECT * FROM " + tableName);

        assertUpdate("INSERT INTO " + tableName + " VALUES (10, 1), (20, 1), (null, 1)", 3);

        List<DeltaLakeTransactionLogEntry> transactionLog = getEntriesFromJson(1, tableLocation.resolve("_delta_log").toString(), FILE_SYSTEM).orElseThrow();
        assertThat(transactionLog).hasSize(2);
        AddFileEntry addFileEntry = transactionLog.get(1).getAdd();
        DeltaLakeFileStatistics stats = addFileEntry.getStats().orElseThrow();
        assertThat(stats.getMinValues().orElseThrow().get("UPPER_CASE")).isEqualTo(10);
        assertThat(stats.getMaxValues().orElseThrow().get("UPPER_CASE")).isEqualTo(20);
        assertThat(stats.getNullCount("UPPER_CASE").orElseThrow()).isEqualTo(1);

        assertUpdate("UPDATE " + tableName + " SET upper_case = upper_case + 10", 3);

        List<DeltaLakeTransactionLogEntry> transactionLogAfterUpdate = getEntriesFromJson(2, tableLocation.resolve("_delta_log").toString(), FILE_SYSTEM).orElseThrow();
        assertThat(transactionLogAfterUpdate).hasSize(3);
        AddFileEntry updateAddFileEntry = transactionLogAfterUpdate.get(2).getAdd();
        DeltaLakeFileStatistics updateStats = updateAddFileEntry.getStats().orElseThrow();
        assertThat(updateStats.getMinValues().orElseThrow().get("UPPER_CASE")).isEqualTo(20);
        assertThat(updateStats.getMaxValues().orElseThrow().get("UPPER_CASE")).isEqualTo(30);
        assertThat(updateStats.getNullCount("UPPER_CASE").orElseThrow()).isEqualTo(1);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                        ('upper_case', null, 2.0, 0.3333333333333333, null, 20, 30),
                        ('part', null, 1.0, 0.0, null, null, null),
                        (null, null, null, null, 3.0, null, null)
                        """);

        assertUpdate(format("ANALYZE %s WITH(mode = 'full_refresh')", tableName), 3);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                        ('upper_case', null, 2.0, 0.3333333333333333, null, 20, 30),
                        ('part', null, 1.0, 0.0, null, null, null),
                        (null, null, null, null, 3.0, null, null)
                        """);
    }

    /**
     * @see databricks131.timestamp_ntz
     */
    @Test
    public void testDeltaTimestampNtz()
            throws Exception
    {
        testDeltaTimestampNtz(UTC);
        testDeltaTimestampNtz(jvmZone);
        // using two non-JVM zones so that we don't need to worry what Postgres system zone is
        testDeltaTimestampNtz(vilnius);
        testDeltaTimestampNtz(kathmandu);
        testDeltaTimestampNtz(TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId());
    }

    private void testDeltaTimestampNtz(ZoneId sessionZone)
            throws Exception
    {
        String tableName = "timestamp_ntz" + randomNameSuffix();
        Path tableLocation = Files.createTempFile(tableName, null);
        copyDirectoryContents(new File(Resources.getResource("databricks131/timestamp_ntz").toURI()).toPath(), tableLocation);

        assertUpdate("CALL system.register_table('%s', '%s', '%s')".formatted(getSession().getSchema().orElseThrow(), tableName, tableLocation.toUri()));

        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        assertQuery(
                "DESCRIBE " + tableName,
                "VALUES ('x', 'timestamp(6)', '', '')");

        assertThat(query(session, "SELECT * FROM " + tableName))
                .matches("""
                            VALUES
                            NULL,
                            TIMESTAMP '-9999-12-31 23:59:59.999999',
                            TIMESTAMP '-0001-01-01 00:00:00',
                            TIMESTAMP '0000-01-01 00:00:00',
                            TIMESTAMP '1582-10-05 00:00:00',
                            TIMESTAMP '1582-10-14 23:59:59.999999',
                            TIMESTAMP '2020-12-31 01:02:03.123456',
                            TIMESTAMP '9999-12-31 23:59:59.999999'
                            """);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                            VALUES
                            ('x', null, null, 0.125, null, null, null),
                            (null, null, null, null, 8.0, null, null)
                            """);

        // Verify the connector can insert into tables created by Databricks
        assertUpdate(session, "INSERT INTO " + tableName + " VALUES TIMESTAMP '2023-01-02 03:04:05.123456'", 1);
        assertQuery(session, "SELECT true FROM " + tableName + " WHERE x = TIMESTAMP '2023-01-02 03:04:05.123456'", "VALUES true");
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                        ('x', null, 1.0, 0.1111111111111111, null, null, null),
                        (null, null, null, null, 9.0, null, null)
                        """);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testTrinoCreateTableWithTimestampNtz()
            throws Exception
    {
        testTrinoCreateTableWithTimestampNtz(UTC);
        testTrinoCreateTableWithTimestampNtz(jvmZone);
        // using two non-JVM zones so that we don't need to worry what Postgres system zone is
        testTrinoCreateTableWithTimestampNtz(vilnius);
        testTrinoCreateTableWithTimestampNtz(kathmandu);
        testTrinoCreateTableWithTimestampNtz(TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId());
    }

    private void testTrinoCreateTableWithTimestampNtz(ZoneId sessionZone)
            throws Exception
    {
        testTrinoCreateTableWithTimestampNtz(
                sessionZone,
                tableName -> {
                    assertUpdate("CREATE TABLE " + tableName + "(x timestamp(6))");
                    assertUpdate("INSERT INTO " + tableName + " VALUES timestamp '2023-01-02 03:04:05.123456'", 1);
                });
    }

    @Test
    public void testTrinoCreateTableAsSelectWithTimestampNtz()
            throws Exception
    {
        testTrinoCreateTableAsSelectWithTimestampNtz(UTC);
        testTrinoCreateTableAsSelectWithTimestampNtz(jvmZone);
        // using two non-JVM zones so that we don't need to worry what Postgres system zone is
        testTrinoCreateTableAsSelectWithTimestampNtz(vilnius);
        testTrinoCreateTableAsSelectWithTimestampNtz(kathmandu);
        testTrinoCreateTableAsSelectWithTimestampNtz(TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId());
    }

    private void testTrinoCreateTableAsSelectWithTimestampNtz(ZoneId sessionZone)
            throws Exception
    {
        testTrinoCreateTableWithTimestampNtz(
                sessionZone,
                tableName -> assertUpdate("CREATE TABLE " + tableName + " AS SELECT timestamp '2023-01-02 03:04:05.123456' AS x", 1));
    }

    private void testTrinoCreateTableWithTimestampNtz(ZoneId sessionZone, Consumer<String> createTable)
            throws IOException
    {
        String tableName = "test_create_table_timestamp_ntz" + randomNameSuffix();

        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        createTable.accept(tableName);

        assertQuery(session, "SELECT * FROM " + tableName, "VALUES TIMESTAMP '2023-01-02 03:04:05.123456'");

        // Verify reader/writer version and features in ProtocolEntry
        String tableLocation = getTableLocation(tableName);
        List<DeltaLakeTransactionLogEntry> transactionLogs = getEntriesFromJson(0, tableLocation + "/_delta_log", FILE_SYSTEM).orElseThrow();
        ProtocolEntry protocolEntry = transactionLogs.get(1).getProtocol();
        assertThat(protocolEntry).isNotNull();
        assertThat(protocolEntry.getMinReaderVersion()).isEqualTo(3);
        assertThat(protocolEntry.getMinWriterVersion()).isEqualTo(7);
        assertThat(protocolEntry.getReaderFeatures()).isEqualTo(Optional.of(ImmutableSet.of("timestampNtz")));
        assertThat(protocolEntry.getWriterFeatures()).isEqualTo(Optional.of(ImmutableSet.of("timestampNtz")));

        // Insert rows and verify results
        assertUpdate(session,
                "INSERT INTO " + tableName + " " + """
                        VALUES
                        NULL,
                        TIMESTAMP '-9999-12-31 23:59:59.999999',
                        TIMESTAMP '-0001-01-01 00:00:00',
                        TIMESTAMP '0000-01-01 00:00:00',
                        TIMESTAMP '1582-10-05 00:00:00',
                        TIMESTAMP '1582-10-14 23:59:59.999999',
                        TIMESTAMP '2020-12-31 01:02:03.123456',
                        TIMESTAMP '9999-12-31 23:59:59.999999'
                        """,
                8);

        assertThat(query(session, "SELECT * FROM " + tableName))
                .matches("""
                        VALUES
                        NULL,
                        TIMESTAMP '-9999-12-31 23:59:59.999999',
                        TIMESTAMP '-0001-01-01 00:00:00',
                        TIMESTAMP '0000-01-01 00:00:00',
                        TIMESTAMP '1582-10-05 00:00:00',
                        TIMESTAMP '1582-10-14 23:59:59.999999',
                        TIMESTAMP '2020-12-31 01:02:03.123456',
                        TIMESTAMP '2023-01-02 03:04:05.123456',
                        TIMESTAMP '9999-12-31 23:59:59.999999'
                        """);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                        ('x', null, 8.0, 0.1111111111111111, null, null, null),
                        (null, null, null, null, 9.0, null, null)
                        """);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testTrinoTimestampNtzComplexType()
    {
        testTrinoTimestampNtzComplexType(UTC);
        testTrinoTimestampNtzComplexType(jvmZone);
        // using two non-JVM zones so that we don't need to worry what Postgres system zone is
        testTrinoTimestampNtzComplexType(vilnius);
        testTrinoTimestampNtzComplexType(kathmandu);
        testTrinoTimestampNtzComplexType(TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId());
    }

    private void testTrinoTimestampNtzComplexType(ZoneId sessionZone)
    {
        String tableName = "test_timestamp_ntz_complex_type" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + "(id int, array_col array(timestamp(6)), map_col map(timestamp(6), timestamp(6)), row_col row(child timestamp(6)))");

        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        assertUpdate(
                session,
                "INSERT INTO " + tableName + " " + """
                        VALUES (
                         1,
                         ARRAY[TIMESTAMP '2020-12-31 01:02:03.123456'],
                         MAP(ARRAY[TIMESTAMP '2021-12-31 01:02:03.123456'], ARRAY[TIMESTAMP '2022-12-31 01:02:03.123456']),
                         ROW(TIMESTAMP '2023-12-31 01:02:03.123456')
                        )
                        """,
                1);

        assertThat(query(session, "SELECT * FROM " + tableName))
                .matches("""
                        VALUES (
                         1,
                         ARRAY[TIMESTAMP '2020-12-31 01:02:03.123456'],
                         MAP(ARRAY[TIMESTAMP '2021-12-31 01:02:03.123456'], ARRAY[TIMESTAMP '2022-12-31 01:02:03.123456']),
                         CAST(ROW(TIMESTAMP '2023-12-31 01:02:03.123456') AS ROW(child timestamp(6)))
                        )
                        """);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                        ('id', null, 1.0, 0.0, null, 1, 1),
                        ('array_col', null, null, null, null, null, null),
                        ('map_col', null, null, null, null, null, null),
                        ('row_col', null, null, null, null, null, null),
                        (null, null, null, null, 1.0, null, null)
                        """);

        assertUpdate("DROP TABLE " + tableName);
    }

    /**
     * @see databricks131.timestamp_ntz_partition
     */
    @Test
    public void testTimestampNtzPartitioned()
            throws Exception
    {
        testTimestampNtzPartitioned(UTC);
        testTimestampNtzPartitioned(jvmZone);
        // using two non-JVM zones so that we don't need to worry what Postgres system zone is
        testTimestampNtzPartitioned(vilnius);
        testTimestampNtzPartitioned(kathmandu);
        testTimestampNtzPartitioned(TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId());
    }

    private void testTimestampNtzPartitioned(ZoneId sessionZone)
            throws Exception
    {
        String tableName = "timestamp_ntz_partition" + randomNameSuffix();
        Path tableLocation = Files.createTempFile(tableName, null);
        copyDirectoryContents(new File(Resources.getResource("databricks131/timestamp_ntz_partition").toURI()).toPath(), tableLocation);
        assertUpdate("CALL system.register_table('%s', '%s', '%s')".formatted(getSession().getSchema().orElseThrow(), tableName, tableLocation.toUri()));

        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        assertQuery(
                "DESCRIBE " + tableName,
                "VALUES ('id', 'integer', '', ''), ('part', 'timestamp(6)', '', '')");
        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                .contains("partitioned_by = ARRAY['part']");

        assertThat(query(session, "SELECT * FROM " + tableName))
                .matches("""
                            VALUES
                            (1, NULL),
                            (2, TIMESTAMP '-9999-12-31 23:59:59.999999'),
                            (3, TIMESTAMP '-0001-01-01 00:00:00'),
                            (4, TIMESTAMP '0000-01-01 00:00:00'),
                            (5, TIMESTAMP '1582-10-05 00:00:00'),
                            (6, TIMESTAMP '1582-10-14 23:59:59.999999'),
                            (7, TIMESTAMP '2020-12-31 01:02:03.123456'),
                            (8, TIMESTAMP '9999-12-31 23:59:59.999999')
                        """);
        assertQuery(session, "SELECT id FROM " + tableName + " WHERE part = TIMESTAMP '2020-12-31 01:02:03.123456'", "VALUES 7");

        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                            VALUES
                            ('id', null, null, 0.0, null, 1, 8),
                            ('part', null, 7.0, 0.125, null, null, null),
                            (null, null, null, null, 8.0, null, null)
                            """);

        // Verify the connector can insert into tables created by Databricks
        assertUpdate(session, "INSERT INTO " + tableName + " VALUES (9, TIMESTAMP '2023-01-02 03:04:05.123456')", 1);
        assertQuery(session, "SELECT part FROM " + tableName + " WHERE id = 9", "VALUES TIMESTAMP '2023-01-02 03:04:05.123456'");
        assertQuery(session, "SELECT id FROM " + tableName + " WHERE part = TIMESTAMP '2023-01-02 03:04:05.123456'", "VALUES 9");
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                        ('id', null, 1.0, 0.0, null, 1, 9),
                        ('part', null, 8.0, 0.1111111111111111, null, null, null),
                        (null, null, null, null, 9.0, null, null)
                        """);
        List<DeltaLakeTransactionLogEntry> transactionLogs = getEntriesFromJson(2, tableLocation.resolve("_delta_log").toString(), FILE_SYSTEM).orElseThrow();
        assertThat(transactionLogs).hasSize(2);
        AddFileEntry addFileEntry = transactionLogs.get(1).getAdd();
        assertThat(addFileEntry).isNotNull();
        assertThat(addFileEntry.getPath()).startsWith("part=2023-01-02%2003%253A04%253A05.123456/");
        assertThat(addFileEntry.getPartitionValues()).containsExactly(Map.entry("part", "2023-01-02 03:04:05.123456"));

        assertUpdate("DROP TABLE " + tableName);
    }

    /**
     * @see databricks122.identity_columns
     */
    @Test
    public void testIdentityColumns()
            throws Exception
    {
        String tableName = "test_identity_columns_" + randomNameSuffix();
        Path tableLocation = Files.createTempFile(tableName, null);
        copyDirectoryContents(new File(Resources.getResource("databricks122/identity_columns").toURI()).toPath(), tableLocation);

        assertUpdate("CALL system.register_table('%s', '%s', '%s')".formatted(getSession().getSchema().orElseThrow(), tableName, tableLocation.toUri()));
        assertQueryReturnsEmptyResult("SELECT * FROM " + tableName);

        List<DeltaLakeTransactionLogEntry> transactionLog = getEntriesFromJson(0, tableLocation.resolve("_delta_log").toString(), FILE_SYSTEM).orElseThrow();
        assertThat(transactionLog).hasSize(3);
        MetadataEntry metadataEntry = transactionLog.get(2).getMetaData();
        assertThat(getColumnsMetadata(metadataEntry).get("b"))
                .containsExactly(
                        entry("delta.identity.start", 1),
                        entry("delta.identity.step", 1),
                        entry("delta.identity.allowExplicitInsert", false));

        // Verify a column operation preserves delta.identity.* column properties
        assertUpdate("COMMENT ON COLUMN " + tableName + ".b IS 'test column comment'");

        List<DeltaLakeTransactionLogEntry> transactionLogAfterComment = getEntriesFromJson(1, tableLocation.resolve("_delta_log").toString(), FILE_SYSTEM).orElseThrow();
        assertThat(transactionLogAfterComment).hasSize(3);
        MetadataEntry commentMetadataEntry = transactionLogAfterComment.get(2).getMetaData();
        assertThat(getColumnsMetadata(commentMetadataEntry).get("b"))
                .containsExactly(
                        entry("comment", "test column comment"),
                        entry("delta.identity.start", 1),
                        entry("delta.identity.step", 1),
                        entry("delta.identity.allowExplicitInsert", false));
    }

    /**
     * @see databricks122.deletion_vectors
     */
    @Test
    public void testDeletionVectors()
    {
        assertQuery("SELECT * FROM deletion_vectors", "VALUES (1, 11)");
    }

    @Test
    public void testCorruptedManagedTableLocation()
            throws Exception
    {
        String tableName = "bad_person_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 id, 'person1' name", 1);
        String tableLocation = (String) computeScalar("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '') FROM " + tableName);
        testCorruptedTableLocation(tableName, Path.of(URI.create(tableLocation)), true);
    }

    @Test
    public void testCorruptedExternalTableLocation()
            throws Exception
    {
        // create a bad_person table which is based on person table in temporary location
        String tableName = "bad_person_" + randomNameSuffix();
        Path tableLocation = Files.createTempFile(tableName, null);
        copyDirectoryContents(Path.of(getResourceLocation("databricks73/person").toURI()), tableLocation);
        getQueryRunner().execute(
                format("CALL system.register_table('%s', '%s', '%s')", getSession().getSchema().orElseThrow(), tableName, tableLocation));
        testCorruptedTableLocation(tableName, tableLocation, false);
    }

    private void testCorruptedTableLocation(String tableName, Path tableLocation, boolean isManaged)
            throws Exception
    {
        Path transactionLogDirectory = tableLocation.resolve("_delta_log");

        // break the table by deleting all its files under transaction log
        deleteRecursively(transactionLogDirectory, ALLOW_INSECURE);

        // Flush the metadata cache before verifying operations on the table
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => '" + tableName + "')");

        // Assert queries fail cleanly
        assertQueryFails("TABLE " + tableName, "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("SELECT * FROM \"" + tableName + "$history\"", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("SELECT * FROM \"" + tableName + "$properties\"", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("SELECT * FROM " + tableName + " WHERE false", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("SELECT 1 FROM " + tableName + " WHERE false", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("SHOW CREATE TABLE " + tableName, "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("CREATE TABLE a_new_table (LIKE " + tableName + " EXCLUDING PROPERTIES)", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("DESCRIBE " + tableName, "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("SHOW COLUMNS FROM " + tableName, "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("SHOW STATS FOR " + tableName, "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("ANALYZE " + tableName, "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("ALTER TABLE " + tableName + " EXECUTE optimize", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("ALTER TABLE " + tableName + " EXECUTE vacuum", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("ALTER TABLE " + tableName + " RENAME TO bad_person_some_new_name", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("ALTER TABLE " + tableName + " ADD COLUMN foo int", "Metadata not found in transaction log for tpch." + tableName);
        // TODO (https://github.com/trinodb/trino/issues/16248) ADD field
        assertQueryFails("ALTER TABLE " + tableName + " DROP COLUMN foo", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("ALTER TABLE " + tableName + " DROP COLUMN foo.bar", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("ALTER TABLE " + tableName + " SET PROPERTIES change_data_feed_enabled = true", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("INSERT INTO " + tableName + " VALUES (NULL)", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("UPDATE " + tableName + " SET foo = 'bar'", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("DELETE FROM " + tableName, "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("MERGE INTO  " + tableName + " USING (SELECT 1 a) input ON true WHEN MATCHED THEN DELETE", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("TRUNCATE TABLE " + tableName, "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("COMMENT ON TABLE " + tableName + " IS NULL", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("COMMENT ON COLUMN " + tableName + ".foo IS NULL", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("CALL system.vacuum(CURRENT_SCHEMA, '" + tableName + "', '7d')", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("SELECT * FROM TABLE(system.table_changes('tpch', '" + tableName + "'))", "Metadata not found in transaction log for tpch." + tableName);
        assertQuerySucceeds("CALL system.drop_extended_stats(CURRENT_SCHEMA, '" + tableName + "')");

        // Avoid failing metadata queries
        assertQuery("SHOW TABLES LIKE 'bad\\_person\\_%' ESCAPE '\\'", "VALUES '" + tableName + "'");
        assertQueryReturnsEmptyResult("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA AND table_name LIKE 'bad\\_person\\_%' ESCAPE '\\'");
        assertQueryReturnsEmptyResult("SELECT column_name, data_type FROM system.jdbc.columns WHERE table_cat = CURRENT_CATALOG AND table_schem = CURRENT_SCHEMA AND table_name LIKE 'bad\\_person\\_%' ESCAPE '\\'");

        // DROP TABLE should succeed so that users can remove their corrupted table
        getQueryRunner().execute("DROP TABLE " + tableName);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        if (isManaged) {
            assertThat(tableLocation.toFile()).doesNotExist().as("Table location should not exist");
        }
        else {
            assertThat(tableLocation.toFile()).exists().as("Table location should exist");
        }
    }

    /**
     * @see deltalake.stats_with_minmax_nulls
     */
    @Test
    public void testStatsWithMinMaxValuesAsNulls()
    {
        assertQuery(
                "SELECT * FROM stats_with_minmax_nulls",
                """
                   VALUES
                   (0, 1),
                   (1, 2),
                   (3, 4),
                   (3, 7),
                   (NULL, NULL),
                   (NULL, NULL)
                   """);
        assertQuery(
                "SHOW STATS FOR stats_with_minmax_nulls",
                """
                   VALUES
                   ('id', null, null, 0.3333333333333333, null, 0, 3),
                   ('id2', null, null, 0.3333333333333333, null, 1, 7),
                   (null, null, null, null, 6.0, null, null)
                   """);
    }

    /**
     * @see deltalake.multipart_checkpoint
     */
    @Test
    public void testReadMultipartCheckpoint()
            throws Exception
    {
        String tableName = "test_multipart_checkpoint_" + randomNameSuffix();
        Path tableLocation = Files.createTempFile(tableName, null);
        copyDirectoryContents(new File(Resources.getResource("deltalake/multipart_checkpoint").toURI()).toPath(), tableLocation);

        assertUpdate("CALL system.register_table('%s', '%s', '%s')".formatted(getSession().getSchema().orElseThrow(), tableName, tableLocation.toUri()));
        assertThat(query("DESCRIBE " + tableName)).projected("Column", "Type").skippingTypesCheck().matches("VALUES ('c', 'integer')");
        assertThat(query("SELECT * FROM " + tableName)).matches("VALUES 1, 2, 3, 4, 5, 6, 7");
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
}
