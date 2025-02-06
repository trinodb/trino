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
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.DeletionVectorEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.ColumnMappingMode;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointSchemaManager;
import io.trino.plugin.deltalake.transactionlog.checkpoint.TransactionLogTail;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeFileStatistics;
import io.trino.plugin.hive.parquet.TrinoParquetDataSource;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.TimeZoneKey;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingSession;
import io.trino.testing.sql.TestTable;
import org.apache.parquet.schema.PrimitiveType;
import org.intellij.lang.annotations.Language;
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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterators.getOnlyElement;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.parquet.ParquetTestUtils.createParquetReader;
import static io.trino.plugin.deltalake.DeltaLakeConfig.DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE;
import static io.trino.plugin.deltalake.DeltaTestingConnectorSession.SESSION;
import static io.trino.plugin.deltalake.TestingDeltaLakeUtils.copyDirectoryContents;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.extractPartitionColumns;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getColumnsMetadata;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.SqlDecimal.decimal;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.util.Files.fileNamesIn;
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
            new ResourceTable("allow_column_defaults", "deltalake/allow_column_defaults"),
            new ResourceTable("stats_with_minmax_nulls", "deltalake/stats_with_minmax_nulls"),
            new ResourceTable("no_column_stats", "databricks73/no_column_stats"),
            new ResourceTable("liquid_clustering", "deltalake/liquid_clustering"),
            new ResourceTable("region_91_lts", "databricks91/region"),
            new ResourceTable("timestamp_ntz", "databricks131/timestamp_ntz"),
            new ResourceTable("timestamp_ntz_partition", "databricks131/timestamp_ntz_partition"),
            new ResourceTable("uniform_hudi", "deltalake/uniform_hudi"),
            new ResourceTable("uniform_iceberg_v1", "databricks133/uniform_iceberg_v1"),
            new ResourceTable("uniform_iceberg_v2", "databricks143/uniform_iceberg_v2"),
            new ResourceTable("unsupported_writer_feature", "deltalake/unsupported_writer_feature"),
            new ResourceTable("unsupported_writer_version", "deltalake/unsupported_writer_version"),
            new ResourceTable("variant", "databricks153/variant"),
            new ResourceTable("type_widening", "databricks153/type_widening"),
            new ResourceTable("type_widening_partition", "databricks153/type_widening_partition"),
            new ResourceTable("type_widening_nested", "databricks153/type_widening_nested"));

    // The col-{uuid} pattern for delta.columnMapping.physicalName
    private static final Pattern PHYSICAL_COLUMN_NAME_PATTERN = Pattern.compile("^col-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

    private static final TrinoFileSystem FILE_SYSTEM = new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS).create(SESSION);

    private final ZoneId jvmZone = ZoneId.systemDefault();
    private final ZoneId vilnius = ZoneId.of("Europe/Vilnius");
    private final ZoneId kathmandu = ZoneId.of("Asia/Kathmandu");

    private Path catalogDir;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        catalogDir = Files.createTempDirectory("catalog-dir");
        closeAfterClass(() -> deleteRecursively(catalogDir, ALLOW_INSECURE));

        return DeltaLakeQueryRunner.builder()
                .addDeltaProperty("hive.metastore.catalog.dir", catalogDir.toUri().toString())
                .addDeltaProperty("delta.register-table-procedure.enabled", "true")
                .addDeltaProperty("delta.enable-non-concurrent-writes", "true")
                .addDeltaProperty("delta.transaction-log.max-cached-file-size", "0B") // Tests json log streaming code path
                .build();
    }

    @BeforeAll
    public void registerTables()
    {
        for (ResourceTable table : Iterables.concat(PERSON_TABLES, OTHER_TABLES)) {
            String dataPath = getResourceLocation(table.resourcePath()).toExternalForm();
            getQueryRunner().execute(
                    format("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')", table.tableName(), dataPath));
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
    void testDatabricks91()
    {
        assertThat(query("SELECT * FROM region_91_lts"))
                .skippingTypesCheck() // name and comment columns are unbounded varchar in Delta Lake and bounded varchar in TPCH
                .matches("SELECT * FROM tpch.tiny.region");
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
        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource("deltalake/column_mapping_mode_" + columnMappingMode).toURI()).toPath(), tableLocation);

        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, tableLocation.toUri()));
        assertThat(query("DESCRIBE " + tableName)).result().projected("Column", "Type").skippingTypesCheck().matches("VALUES ('x', 'integer')");
        assertQueryReturnsEmptyResult("SELECT * FROM " + tableName);

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN second_col row(a array(integer), b map(integer, integer), c row(field integer))");
        MetadataEntry metadata = loadMetadataEntry(1, tableLocation);
        assertThat(metadata.getConfiguration()).containsEntry(
                "delta.columnMapping.maxColumnId",
                "6"); // +5 comes from second_col + second_col.a + second_col.b + second_col.c + second_col.c.field

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

    @Test // regression test for https://github.com/trinodb/trino/issues/24121
    void testPartitionValuesParsedCheckpoint()
            throws Exception
    {
        for (ColumnMappingMode mode : List.of(ColumnMappingMode.ID, ColumnMappingMode.NAME, ColumnMappingMode.NONE)) {
            testPartitionValuesParsedCheckpoint(mode, "boolean", ImmutableList.of("true", "false"), ImmutableList.of(true, false));
            testPartitionValuesParsedCheckpoint(mode, "tinyint", ImmutableList.of("10", "20"), ImmutableList.of(Byte.valueOf("10"), Byte.valueOf("20")));
            testPartitionValuesParsedCheckpoint(mode, "smallint", ImmutableList.of("100", "200"), ImmutableList.of((short) 100, (short) 200));
            testPartitionValuesParsedCheckpoint(mode, "integer", ImmutableList.of("1000", "2000"), ImmutableList.of(1000, 2000));
            testPartitionValuesParsedCheckpoint(mode, "bigint", ImmutableList.of("10000", "20000"), ImmutableList.of(10000L, 20000L));
            testPartitionValuesParsedCheckpoint(mode, "real", ImmutableList.of("REAL '1.23'", "REAL '4.56'"), ImmutableList.of(1.23f, 4.56f));
            testPartitionValuesParsedCheckpoint(mode, "double", ImmutableList.of("DOUBLE '12.34'", "DOUBLE '56.78'"), ImmutableList.of(12.34d, 56.78d));
            testPartitionValuesParsedCheckpoint(
                    mode,
                    "decimal(3,2)",
                    ImmutableList.of("0.12", "3.45"),
                    ImmutableList.of(decimal("0.12", createDecimalType(3, 2)), decimal("3.45", createDecimalType(3, 2))));
            testPartitionValuesParsedCheckpoint(mode, "varchar", ImmutableList.of("'alice'", "'bob'"), ImmutableList.of("alice", "bob"));
            // TODO https://github.com/trinodb/trino/issues/24155 Cannot insert varbinary values into partitioned columns
            testPartitionValuesParsedCheckpoint(
                    mode,
                    "date",
                    ImmutableList.of("DATE '1970-01-01'", "DATE '9999-12-31'"),
                    ImmutableList.of(new SqlDate((int) LocalDate.of(1970, 1, 1).toEpochDay()), new SqlDate((int) LocalDate.of(9999, 12, 31).toEpochDay())));
            testPartitionValuesParsedCheckpoint(
                    mode,
                    "timestamp",
                    ImmutableList.of("TIMESTAMP '1970-01-01 00:00:00'", "TIMESTAMP '1970-01-02 00:00:00'"),
                    ImmutableList.of(SqlTimestamp.newInstance(3, 0, 0), SqlTimestamp.newInstance(3, 86400000000L, 0)));
            ZonedDateTime epochPlus1Day = LocalDateTime.of(1970, 1, 2, 0, 0, 0).atZone(UTC);
            long epochPlus1DayMillis = packDateTimeWithZone(epochPlus1Day.toInstant().toEpochMilli(), UTC_KEY);
            testPartitionValuesParsedCheckpoint(
                    mode,
                    "timestamp with time zone",
                    ImmutableList.of("TIMESTAMP '1970-01-01 00:00:00 +00:00'", "TIMESTAMP '1970-01-02 00:00:00 +00:00'"),
                    ImmutableList.of(SqlTimestamp.newInstance(3, 0, 0), SqlTimestamp.newInstance(3, epochPlus1DayMillis, 0)));
            // array, map, row types are unsupported as partition column type. This is tested in TestDeltaLakeConnectorTest.testCreateTableWithUnsupportedPartitionType.
        }
    }

    private void testPartitionValuesParsedCheckpoint(ColumnMappingMode columnMappingMode, String inputType, List<String> inputValues, List<Object> expectedPartitionValuesParsed)
            throws Exception
    {
        checkArgument(inputValues.size() == 2, "inputValues size must be 2");
        checkArgument(expectedPartitionValuesParsed.size() == 2, "expectedPartitionValuesParsed size must be 2");

        try (TestTable table = newTrinoTable(
                "test_partition_values_parsed_checkpoint",
                "(x int, part " + inputType + ") WITH (checkpoint_interval = 2, column_mapping_mode = '" + columnMappingMode + "', partitioned_by = ARRAY['part'])")) {
            for (String inputValue : inputValues) {
                assertUpdate("INSERT INTO " + table.getName() + " VALUES (" + inputValues.indexOf(inputValue) + ", " + inputValue + ")", 1);
            }
            assertQuery("SELECT x FROM " + table.getName() + " WHERE part = " + inputValues.getFirst(), "VALUES 0");

            Path tableLocation = Path.of(getTableLocation(table.getName()).replace("file://", ""));
            Path checkpoint = tableLocation.resolve("_delta_log/00000000000000000002.checkpoint.parquet");

            MetadataEntry metadataEntry = loadMetadataEntry(0, tableLocation);
            ProtocolEntry protocolEntry = loadProtocolEntry(0, tableLocation);

            DeltaLakeColumnHandle partitionColumn = extractPartitionColumns(metadataEntry, protocolEntry, TESTING_TYPE_MANAGER).stream().collect(onlyElement());
            String physicalColumnName = partitionColumn.basePhysicalColumnName();
            if (columnMappingMode == ColumnMappingMode.ID || columnMappingMode == ColumnMappingMode.NAME) {
                assertThat(physicalColumnName).matches(PHYSICAL_COLUMN_NAME_PATTERN);
            }
            else {
                assertThat(physicalColumnName).isEqualTo("part");
            }

            int partitionValuesParsedFieldPosition = 6;
            RowType addEntryType = new CheckpointSchemaManager(TESTING_TYPE_MANAGER).getAddEntryType(metadataEntry, protocolEntry, _ -> true, true, true, true);

            RowType.Field partitionValuesParsedField = addEntryType.getFields().get(partitionValuesParsedFieldPosition);
            assertThat(partitionValuesParsedField.getName().orElseThrow()).matches("partitionValues_parsed");
            RowType partitionValuesParsedType = (RowType) partitionValuesParsedField.getType();
            assertThat(partitionValuesParsedType.getFields().stream().collect(onlyElement()).getName().orElseThrow()).isEqualTo(physicalColumnName);

            TrinoParquetDataSource dataSource = new TrinoParquetDataSource(new LocalInputFile(checkpoint.toFile()), new ParquetReaderOptions(), new FileFormatDataSourceStats());
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
            try (ParquetReader reader = createParquetReader(dataSource, parquetMetadata, ImmutableList.of(addEntryType), List.of("add"))) {
                List<Object> actual = new ArrayList<>();
                Page page = reader.nextPage();
                while (page != null) {
                    Block block = page.getBlock(0);
                    for (int i = 0; i < block.getPositionCount(); i++) {
                        List<?> add = (List<?>) addEntryType.getObjectValue(SESSION, block, i);
                        if (add == null) {
                            continue;
                        }
                        actual.add(((List<?>) add.get(partitionValuesParsedFieldPosition)).stream().collect(onlyElement()));
                    }
                    page = reader.nextPage();
                }
                assertThat(actual).containsExactlyInAnyOrderElementsOf(expectedPartitionValuesParsed);
            }
        }
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
        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource("deltalake/column_mapping_mode_" + columnMappingMode).toURI()).toPath(), tableLocation);

        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, tableLocation.toUri()));
        assertThat(query("DESCRIBE " + tableName)).result().projected("Column", "Type").skippingTypesCheck().matches("VALUES ('x', 'integer')");
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
        List<DeltaLakeTransactionLogEntry> transactionLog = getEntriesFromJson(4, tableLocation.resolve("_delta_log").toString());
        assertThat(transactionLog).hasSize(5);
        assertThat(transactionLog.get(0).getCommitInfo()).isNotNull();
        assertThat(transactionLog.get(1).getRemove()).isNotNull();
        assertThat(transactionLog.get(2).getRemove()).isNotNull();
        assertThat(transactionLog.get(3).getRemove()).isNotNull();
        assertThat(transactionLog.get(4).getAdd()).isNotNull();
        AddFileEntry addFileEntry = transactionLog.get(4).getAdd();
        DeltaLakeFileStatistics stats = addFileEntry.getStats().orElseThrow();
        assertThat(stats.getMinValues().orElseThrow()).containsEntry(physicalName, 10);
        assertThat(stats.getMaxValues().orElseThrow()).containsEntry(physicalName, 20);
        assertThat(stats.getNullCount(physicalName).orElseThrow()).isEqualTo(1);

        // Verify optimized parquet file contains the expected physical id and name
        TrinoInputFile inputFile = new LocalInputFile(tableLocation.resolve(addFileEntry.getPath()).toFile());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(
                new TrinoParquetDataSource(inputFile, new ParquetReaderOptions(), new FileFormatDataSourceStats()),
                Optional.empty());
        FileMetadata fileMetaData = parquetMetadata.getFileMetaData();
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
        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource("deltalake/column_mapping_mode_" + columnMappingMode).toURI()).toPath(), tableLocation);

        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, tableLocation.toUri()));
        assertThat(query("DESCRIBE " + tableName)).result().projected("Column", "Type").skippingTypesCheck().matches("VALUES ('x', 'integer')");
        assertQueryReturnsEmptyResult("SELECT * FROM " + tableName);

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN second_col row(a array(integer), b map(integer, integer), c row(field integer))");
        MetadataEntry metadata = loadMetadataEntry(1, tableLocation);
        assertThat(metadata.getConfiguration()).containsEntry("delta.columnMapping.maxColumnId", "6"); // +5 comes from second_col + second_col.a + second_col.b + second_col.c + second_col.c.field
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
        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource("deltalake/column_mapping_mode_" + columnMappingMode).toURI()).toPath(), tableLocation);

        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, tableLocation.toUri()));
        assertQueryReturnsEmptyResult("SELECT * FROM " + tableName);

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN second_col row(a array(integer), b map(integer, integer), c row(field integer))");
        MetadataEntry metadata = loadMetadataEntry(1, tableLocation);
        assertThat(metadata.getConfiguration()).containsEntry("delta.columnMapping.maxColumnId", "6"); // +5 comes from second_col + second_col.a + second_col.b + second_col.c + second_col.c.field
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
        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource("deltalake/column_mapping_mode_" + columnMappingMode).toURI()).toPath(), tableLocation);

        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, tableLocation.toUri()));
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
        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource("deltalake/case_sensitive").toURI()).toPath(), tableLocation);

        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, tableLocation.toUri()));
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

    @Test
    public void testAppendOnly()
            throws Exception
    {
        String tableName = "test_append_only_" + randomNameSuffix();
        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource("deltalake/append_only").toURI()).toPath(), tableLocation);
        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, tableLocation.toUri()));
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 11), (2, 12)");

        assertQueryFails("UPDATE " + tableName + " SET a = a + 1", "Cannot modify rows from a table with 'delta.appendOnly' set to true");
        assertQueryFails("DELETE FROM " + tableName + " WHERE a = 1", "Cannot modify rows from a table with 'delta.appendOnly' set to true");
        assertQueryFails("DELETE FROM " + tableName, "Cannot modify rows from a table with 'delta.appendOnly' set to true");
        assertQueryFails("TRUNCATE TABLE " + tableName, "Cannot modify rows from a table with 'delta.appendOnly' set to true");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 11), (2, 12)");

        // Verify delta.appendOnly is preserved after DML
        assertUpdate("COMMENT ON COLUMN " + tableName + ".a IS 'test column comment'");
        assertUpdate("COMMENT ON TABLE " + tableName + " IS 'test table comment'");
        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN new_col INT");
        assertThat(query("SELECT * FROM \"" + tableName + "$properties\"")).result().rows()
                .contains(new MaterializedRow(List.of("delta.appendOnly", "true")));
    }

    @Test
    public void testCreateOrReplaceTableOnAppendOnlyTableFails()
            throws Exception
    {
        String tableName = "test_append_only_" + randomNameSuffix();
        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource("deltalake/append_only").toURI()).toPath(), tableLocation);
        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, tableLocation.toUri()));
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 11), (2, 12)");

        // Delta Lake disallows replacing a table when 'delta.appendOnly' is set to true
        assertQueryFails("CREATE OR REPLACE TABLE " + tableName + "(a INT, c INT)", "Cannot replace a table when 'delta.appendOnly' is set to true");
        assertQueryFails("CREATE OR REPLACE TABLE " + tableName + " AS SELECT 1 as e", "Cannot replace a table when 'delta.appendOnly' is set to true");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 11), (2, 12)");
    }

    /**
     * @see deltalake.case_sensitive
     */
    @Test
    public void testStatisticsWithColumnCaseSensitivity()
            throws Exception
    {
        String tableName = "test_column_case_sensitivity_" + randomNameSuffix();
        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource("deltalake/case_sensitive").toURI()).toPath(), tableLocation);

        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, tableLocation.toUri()));
        assertQueryReturnsEmptyResult("SELECT * FROM " + tableName);

        assertUpdate("INSERT INTO " + tableName + " VALUES (10, 1), (20, 1), (null, 1)", 3);

        List<DeltaLakeTransactionLogEntry> transactionLog = getEntriesFromJson(1, tableLocation.resolve("_delta_log").toString());
        assertThat(transactionLog).hasSize(2);
        AddFileEntry addFileEntry = transactionLog.get(1).getAdd();
        DeltaLakeFileStatistics stats = addFileEntry.getStats().orElseThrow();
        assertThat(stats.getMinValues().orElseThrow()).containsEntry("UPPER_CASE", 10);
        assertThat(stats.getMaxValues().orElseThrow()).containsEntry("UPPER_CASE", 20);
        assertThat(stats.getNullCount("UPPER_CASE").orElseThrow()).isEqualTo(1);

        assertUpdate("UPDATE " + tableName + " SET upper_case = upper_case + 10", 3);

        List<DeltaLakeTransactionLogEntry> transactionLogAfterUpdate = getEntriesFromJson(2, tableLocation.resolve("_delta_log").toString());
        assertThat(transactionLogAfterUpdate).hasSize(3);
        AddFileEntry updateAddFileEntry = transactionLogAfterUpdate.get(2).getAdd();
        DeltaLakeFileStatistics updateStats = updateAddFileEntry.getStats().orElseThrow();
        assertThat(updateStats.getMinValues().orElseThrow()).containsEntry("UPPER_CASE", 20);
        assertThat(updateStats.getMaxValues().orElseThrow()).containsEntry("UPPER_CASE", 30);
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
        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource("databricks131/timestamp_ntz").toURI()).toPath(), tableLocation);

        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, tableLocation.toUri()));

        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        assertQuery(
                "DESCRIBE " + tableName,
                "VALUES ('x', 'timestamp(6)', '', '')");

        assertThat(query(session, "SELECT * FROM " + tableName))
                .matches(
                        """
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
                ('x', null, 1.0, 0.1111111111111111, null, '2023-01-02 03:04:05.123000', '2023-01-02 03:04:05.124000'),
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
        List<DeltaLakeTransactionLogEntry> transactionLogs = getEntriesFromJson(0, tableLocation + "/_delta_log");
        ProtocolEntry protocolEntry = transactionLogs.get(1).getProtocol();
        assertThat(protocolEntry).isNotNull();
        assertThat(protocolEntry.minReaderVersion()).isEqualTo(3);
        assertThat(protocolEntry.minWriterVersion()).isEqualTo(7);
        assertThat(protocolEntry.readerFeatures()).hasValue(ImmutableSet.of("timestampNtz"));
        assertThat(protocolEntry.writerFeatures()).hasValue(ImmutableSet.of("timestampNtz"));

        // Insert rows and verify results
        assertUpdate(session,
                "INSERT INTO " + tableName + " " +
                        """
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
                .matches(
                        """
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
                ('x', null, 8.0, 0.1111111111111111, null, '2023-01-02 03:04:05.123000', '+10000-01-01 00:00:00.000000'),
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
                "INSERT INTO " + tableName + " " +
                        """
                        VALUES (
                         1,
                         ARRAY[TIMESTAMP '2020-12-31 01:02:03.123456'],
                         MAP(ARRAY[TIMESTAMP '2021-12-31 01:02:03.123456'], ARRAY[TIMESTAMP '2022-12-31 01:02:03.123456']),
                         ROW(TIMESTAMP '2023-12-31 01:02:03.123456')
                        )
                        """,
                1);

        assertThat(query(session, "SELECT * FROM " + tableName))
                .matches(
                        """
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
        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource("databricks131/timestamp_ntz_partition").toURI()).toPath(), tableLocation);
        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, tableLocation.toUri()));

        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        assertQuery(
                "DESCRIBE " + tableName,
                "VALUES ('id', 'integer', '', ''), ('part', 'timestamp(6)', '', '')");
        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                .contains("partitioned_by = ARRAY['part']");

        assertThat(query(session, "SELECT * FROM " + tableName))
                .matches(
                        """
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
        List<DeltaLakeTransactionLogEntry> transactionLogs = getEntriesFromJson(2, tableLocation.resolve("_delta_log").toString());
        assertThat(transactionLogs).hasSize(2);
        AddFileEntry addFileEntry = transactionLogs.get(1).getAdd();
        assertThat(addFileEntry).isNotNull();
        assertThat(addFileEntry.getPath()).startsWith("part=2023-01-02%2003%253A04%253A05.123456/");
        assertThat(addFileEntry.getPartitionValues()).containsExactly(Map.entry("part", "2023-01-02 03:04:05.123456"));

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAddTimestampNtzColumn()
            throws Exception
    {
        String tableName = "test_add_timestamp_ntz_column" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + "(id INT)");
        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN ts timestamp(6)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, TIMESTAMP '2023-01-02 03:04:05.123456')", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, TIMESTAMP '2023-01-02 03:04:05.123456')");

        String tableLocation = getTableLocation(tableName);
        List<DeltaLakeTransactionLogEntry> transactionLogsByCreateTable = getEntriesFromJson(0, tableLocation + "/_delta_log");
        ProtocolEntry protocolEntryByCreateTable = transactionLogsByCreateTable.get(1).getProtocol();
        assertThat(protocolEntryByCreateTable).isNotNull();
        assertThat(protocolEntryByCreateTable.minReaderVersion()).isEqualTo(1);
        assertThat(protocolEntryByCreateTable.minWriterVersion()).isEqualTo(2);
        assertThat(protocolEntryByCreateTable.readerFeatures()).isEmpty();
        assertThat(protocolEntryByCreateTable.writerFeatures()).isEmpty();

        List<DeltaLakeTransactionLogEntry> transactionLogsByAddColumn = getEntriesFromJson(1, tableLocation + "/_delta_log");
        ProtocolEntry protocolEntryByAddColumn = transactionLogsByAddColumn.get(1).getProtocol();
        assertThat(protocolEntryByAddColumn).isNotNull();
        assertThat(protocolEntryByAddColumn.minReaderVersion()).isEqualTo(3);
        assertThat(protocolEntryByAddColumn.minWriterVersion()).isEqualTo(7);
        assertThat(protocolEntryByAddColumn.readerFeatures()).hasValue(ImmutableSet.of("timestampNtz"));
        assertThat(protocolEntryByAddColumn.writerFeatures()).hasValue(ImmutableSet.of("timestampNtz"));

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
        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource("databricks122/identity_columns").toURI()).toPath(), tableLocation);

        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, tableLocation.toUri()));
        assertQueryReturnsEmptyResult("SELECT * FROM " + tableName);

        List<DeltaLakeTransactionLogEntry> transactionLog = getEntriesFromJson(0, tableLocation.resolve("_delta_log").toString());
        assertThat(transactionLog).hasSize(3);
        MetadataEntry metadataEntry = transactionLog.get(2).getMetaData();
        assertThat(getColumnsMetadata(metadataEntry).get("b"))
                .containsExactly(
                        entry("delta.identity.start", 1),
                        entry("delta.identity.step", 1),
                        entry("delta.identity.allowExplicitInsert", false));

        // Verify a column operation preserves delta.identity.* column properties
        assertUpdate("COMMENT ON COLUMN " + tableName + ".b IS 'test column comment'");

        List<DeltaLakeTransactionLogEntry> transactionLogAfterComment = getEntriesFromJson(1, tableLocation.resolve("_delta_log").toString());
        assertThat(transactionLogAfterComment).hasSize(3);
        MetadataEntry commentMetadataEntry = transactionLogAfterComment.get(2).getMetaData();
        assertThat(getColumnsMetadata(commentMetadataEntry).get("b"))
                .containsExactly(
                        entry("comment", "test column comment"),
                        entry("delta.identity.start", 1),
                        entry("delta.identity.step", 1),
                        entry("delta.identity.allowExplicitInsert", false));
    }

    @Test
    public void testWritesToTableWithIdentityColumnFails()
            throws Exception
    {
        String tableName = "test_identity_columns_" + randomNameSuffix();
        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource("databricks122/identity_columns").toURI()).toPath(), tableLocation);
        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, tableLocation.toUri()));

        // Disallowing all statements just in case though some statements may be unrelated to identity columns
        assertQueryFails(
                "INSERT INTO " + tableName + " VALUES (4, 4)",
                "Writing to tables with identity columns is not supported");
        assertQueryFails(
                "UPDATE " + tableName + " SET a = 3",
                "Writing to tables with identity columns is not supported");
        assertQueryFails(
                "DELETE FROM " + tableName,
                "Writing to tables with identity columns is not supported");
        assertQueryFails(
                "MERGE INTO " + tableName + " t USING " + tableName + " s ON (t.a = s.a) WHEN MATCHED THEN UPDATE SET a = 1",
                "Writing to tables with identity columns is not supported");
        assertQueryFails(
                "ALTER TABLE " + tableName + " EXECUTE optimize",
                "Writing to tables with identity columns is not supported");
    }

    @Test
    public void testIdentityColumnTableFeature()
            throws Exception
    {
        String tableName = "test_identity_columns_table_feature_" + randomNameSuffix();
        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource("databricks133/identity_columns_table_feature").toURI()).toPath(), tableLocation);
        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, tableLocation.toUri()));

        // Disallowing all statements just in case though some statements may be unrelated to identity columns
        assertQueryFails(
                "INSERT INTO " + tableName + " VALUES (4, 4)",
                "\\QUnsupported writer features: [identityColumns]");
        assertQueryFails(
                "UPDATE " + tableName + " SET a = 3",
                "\\QUnsupported writer features: [identityColumns]");
        assertQueryFails(
                "DELETE FROM " + tableName,
                "\\QUnsupported writer features: [identityColumns]");
        assertQueryFails(
                "MERGE INTO " + tableName + " t USING " + tableName + " s ON (t.a = s.a) WHEN MATCHED THEN UPDATE SET a = 1",
                "\\QUnsupported writer features: [identityColumns]");
    }

    /**
     * @see deltalake.allow_column_defaults
     */
    @Test
    public void testAllowColumnDefaults()
    {
        assertQuery("SELECT * FROM allow_column_defaults", "VALUES (1, 16)");

        // TODO (https://github.com/trinodb/trino/issues/22413) Add support for allowColumnDefaults writer feature
        assertQueryFails("INSERT INTO allow_column_defaults VALUES (2, 32)", "\\QUnsupported writer features: [allowColumnDefaults]");
        assertQueryFails("INSERT INTO allow_column_defaults (a) VALUES (2)", "\\QUnsupported writer features: [allowColumnDefaults]");
    }

    @Test
    public void testDeletionVectorsEnabledCreateTable()
            throws Exception
    {
        testDeletionVectorsEnabledCreateTable("(x int) WITH (deletion_vectors_enabled = true)");
        testDeletionVectorsEnabledCreateTable("WITH (deletion_vectors_enabled = true) AS SELECT 1 x");
    }

    private void testDeletionVectorsEnabledCreateTable(String tableDefinition)
            throws Exception
    {
        try (TestTable table = newTrinoTable("deletion_vectors", tableDefinition)) {
            assertThat((String) computeScalar("SHOW CREATE TABLE " + table.getName()))
                    .contains("deletion_vectors_enabled = true");

            String tableLocation = getTableLocation(table.getName());
            List<DeltaLakeTransactionLogEntry> transactionLogs = getEntriesFromJson(0, tableLocation + "/_delta_log");
            assertThat(transactionLogs.get(1).getProtocol())
                    .isEqualTo(new ProtocolEntry(3, 7, Optional.of(Set.of("deletionVectors")), Optional.of(Set.of("deletionVectors"))));

            assertUpdate("INSERT INTO " + table.getName() + " VALUES 2, 3", 2);
            assertUpdate("DELETE FROM " + table.getName() + " WHERE x = 2", 1);

            assertThat(fileNamesIn(new URI(tableLocation).getPath(), false))
                    .anyMatch(path -> path.matches(".*/deletion_vector_.*.bin"));

            // TODO Allow disabling deletion_vectors_enabled table property. Delta Lake allows the operation.
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " SET PROPERTIES deletion_vectors_enabled = false",
                    "The following properties cannot be updated: deletion_vectors_enabled");
        }
    }

    @Test
    public void testDeletionVectorsDisabledCreateTable()
            throws Exception
    {
        testDeletionVectorsDisabledCreateTable("(x int) WITH (deletion_vectors_enabled = false)");
        testDeletionVectorsDisabledCreateTable("WITH (deletion_vectors_enabled = false) AS SELECT 1 x");
    }

    private void testDeletionVectorsDisabledCreateTable(String tableDefinition)
            throws Exception
    {
        try (TestTable table = newTrinoTable("deletion_vectors", tableDefinition)) {
            assertThat((String) computeScalar("SHOW CREATE TABLE " + table.getName()))
                    .doesNotContain("deletion_vectors_enabled");

            String tableLocation = getTableLocation(table.getName());
            List<DeltaLakeTransactionLogEntry> transactionLogs = getEntriesFromJson(0, tableLocation + "/_delta_log");
            assertThat(transactionLogs.get(1).getProtocol())
                    .isEqualTo(new ProtocolEntry(1, 2, Optional.empty(), Optional.empty()));

            assertUpdate("INSERT INTO " + table.getName() + " VALUES 2, 3", 2);
            assertUpdate("DELETE FROM " + table.getName() + " WHERE x = 2", 1);

            assertThat(fileNamesIn(new URI(tableLocation).getPath(), false))
                    .noneSatisfy(path -> assertThat(path).matches(".*/deletion_vector_.*.bin"));

            // TODO Allow enabling deletion_vectors_enabled table property. Delta Lake allows the operation.
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " SET PROPERTIES deletion_vectors_enabled = true",
                    "The following properties cannot be updated: deletion_vectors_enabled");
        }
    }

    /**
     * @see databricks122.deletion_vectors
     */
    @Test
    public void testDeletionVectors()
            throws Exception
    {
        String tableName = "deletion_vectors" + randomNameSuffix();

        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource("databricks122/deletion_vectors").toURI()).toPath(), tableLocation);
        assertUpdate("CALL system.register_table('%s', '%s', '%s')".formatted(getSession().getSchema().orElseThrow(), tableName, tableLocation.toUri()));

        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                .contains("deletion_vectors_enabled = true");

        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 11)");

        assertUpdate("INSERT INTO " + tableName + " VALUES (3, 31), (3, 32)", 2);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 11), (3, 31), (3, 32)");

        assertUpdate("DELETE FROM " + tableName + " WHERE a = 3 AND b = 31", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 11), (3, 32)");

        assertUpdate("UPDATE " + tableName + " SET a = -3 WHERE b = 32", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 11), (-3, 32)");

        assertUpdate("UPDATE " + tableName + " SET a = -3 WHERE b = 32", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 11), (-3, 32)");

        assertUpdate("MERGE INTO " + tableName + " t " +
                "USING (SELECT * FROM (VALUES 1)) AS s(a) " +
                "ON (t.a = s.a) " +
                "WHEN MATCHED THEN UPDATE SET b = -11", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, -11), (-3, 32)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDeletionVectorsAllRows()
            throws Exception
    {
        String tableName = "deletion_vectors" + randomNameSuffix();

        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource("databricks122/deletion_vectors").toURI()).toPath(), tableLocation);
        assertUpdate("CALL system.register_table('%s', '%s', '%s')".formatted(getSession().getSchema().orElseThrow(), tableName, tableLocation.toUri()));

        assertUpdate("DELETE FROM " + tableName + " WHERE a != 999", 1);

        // 'remove' entry should have the same deletion vector as the previous operation when deleting all rows
        DeletionVectorEntry deletionVector = getEntriesFromJson(2, tableLocation + "/_delta_log").get(2).getAdd().getDeletionVector().orElseThrow();
        assertThat(getEntriesFromJson(3, tableLocation + "/_delta_log").get(1).getRemove().deletionVector().orElseThrow())
                .isEqualTo(deletionVector);

        assertUpdate("INSERT INTO " + tableName + " VALUES (3, 31), (3, 32)", 2);
        assertUpdate("DELETE FROM " + tableName + " WHERE a != 999", 2);
        assertQueryReturnsEmptyResult("SELECT * FROM " + tableName);

        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 10), (2, 20)", 2);
        assertUpdate("UPDATE " + tableName + " SET a = a + 10", 2);
        assertQuery("SELECT * FROM " + tableName, "VALUES (11, 10), (12, 20)");

        assertUpdate("MERGE INTO " + tableName + " t " +
                "USING (SELECT * FROM (VALUES 11, 12)) AS s(a) " +
                "ON (t.a = s.a) " +
                "WHEN MATCHED AND t.a = 11 THEN UPDATE SET b = 100 " +
                "WHEN MATCHED AND t.a = 12 THEN DELETE", 2);
        assertQuery("SELECT * FROM " + tableName, "VALUES (11, 100)");

        assertUpdate("TRUNCATE TABLE " + tableName);
        assertQueryReturnsEmptyResult("SELECT * FROM " + tableName);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDeletionVectorsLargeDelete()
            throws Exception
    {
        String tableName = "deletion_vectors" + randomNameSuffix();

        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource("databricks122/deletion_vectors_empty").toURI()).toPath(), tableLocation);
        assertUpdate("CALL system.register_table('%s', '%s', '%s')".formatted(getSession().getSchema().orElseThrow(), tableName, tableLocation.toUri()));

        assertUpdate("INSERT INTO " + tableName + " SELECT orderkey, custkey FROM tpch.tiny.orders", 15000);
        assertUpdate("DELETE FROM " + tableName + " WHERE a != 1", 14999);

        assertThat(query("SELECT * FROM " + tableName))
                .matches("SELECT CAST(orderkey AS integer), CAST(custkey AS integer) FROM tpch.tiny.orders WHERE orderkey = 1");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDeletionVectorsCheckPoint()
            throws Exception
    {
        String tableName = "deletion_vectors" + randomNameSuffix();

        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource("databricks122/deletion_vectors_empty").toURI()).toPath(), tableLocation);
        assertUpdate("CALL system.register_table('%s', '%s', '%s')".formatted(getSession().getSchema().orElseThrow(), tableName, tableLocation.toUri()));

        for (int i = 0; i < 9; i++) {
            assertUpdate("INSERT INTO " + tableName + " VALUES (" + i + ", " + i + ")", 1);
        }

        assertThat(tableLocation.resolve("_delta_log/00000000000000000010.checkpoint.parquet")).doesNotExist();
        assertUpdate("DELETE FROM " + tableName + " WHERE a != 1", 8);
        assertThat(tableLocation.resolve("_delta_log/00000000000000000010.checkpoint.parquet")).exists();

        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 1)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDeletionVectorsRandomPrefix()
            throws Exception
    {
        String tableName = "deletion_vectors_random_prefix" + randomNameSuffix();

        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource("deltalake/deletion_vector_random_prefix").toURI()).toPath(), tableLocation);
        assertUpdate("CALL system.register_table('%s', '%s', '%s')".formatted(getSession().getSchema().orElseThrow(), tableName, tableLocation.toUri()));

        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 10), (2, 20), (3, 30)", 3);
        assertUpdate("DELETE FROM " + tableName + " WHERE a = 1", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES (2, 20), (3, 30)");

        assertThat(fileNamesIn(new URI(tableLocation.toString()).getPath(), true))
                .anyMatch(path -> path.matches(tableLocation + "/[a-zA-Z0-9_]{3}/deletion_vector_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}.bin"));

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testDeletionVectorsRepeat()
    {
        try (TestTable table = newTrinoTable("test_dv", "(x int) WITH (deletion_vectors_enabled = true)", List.of("1", "2", "3"))) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE x = 1", 1);
            assertUpdate("DELETE FROM " + table.getName() + " WHERE x = 2", 1);
            assertThat(query("SELECT * FROM " + table.getName())).matches("VALUES 3");
        }
    }

    @Test
    void testDeletionVectorsPages()
            throws Exception
    {
        testDeletionVectorsPages(true);
        testDeletionVectorsPages(false);
    }

    private void testDeletionVectorsPages(boolean parquetUseColumnIndex)
            throws Exception
    {
        String tableName = "deletion_vectors_pages" + randomNameSuffix();
        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource("deltalake/deletion_vector_pages").toURI()).toPath(), tableLocation);
        assertUpdate("CALL system.register_table('%s', '%s', '%s')".formatted(getSession().getSchema().orElseThrow(), tableName, tableLocation.toUri()));

        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("delta", "parquet_use_column_index", Boolean.toString(parquetUseColumnIndex))
                .build();

        assertQueryReturnsEmptyResult(session, "SELECT * FROM " + tableName + " WHERE id = 20001");
        assertThat(query(session, "SELECT * FROM " + tableName + " WHERE id = 99999")).matches("VALUES 99999");

        assertThat(query(session, "SELECT id, _change_type, _commit_version FROM TABLE(system.table_changes('tpch', '" +tableName+ "')) WHERE id = 20001"))
                .matches("VALUES (20001, VARCHAR 'insert', BIGINT '1'), (20001, VARCHAR 'update_preimage', BIGINT '2')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testUnsupportedVacuumDeletionVectors()
            throws Exception
    {
        String tableName = "deletion_vectors" + randomNameSuffix();

        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource("databricks122/deletion_vectors_empty").toURI()).toPath(), tableLocation);
        assertUpdate("CALL system.register_table('%s', '%s', '%s')".formatted(getSession().getSchema().orElseThrow(), tableName, tableLocation.toUri()));

        // TODO https://github.com/trinodb/trino/issues/22809 Add support for vacuuming tables with deletion vectors
        assertQueryFails(
                "CALL delta.system.vacuum('tpch', '" + tableName + "', '7d')",
                "Cannot execute vacuum procedure with deletionVectors writer features");

        assertUpdate("DROP TABLE " + tableName);
    }

    /**
     * @see deltalake.liquid_clustering
     */
    @Test
    public void testLiquidClustering()
    {
        assertQuery("SELECT * FROM liquid_clustering", "VALUES ('test 1', 2024, 1), ('test 2', 2024, 2)");
        assertQuery("SELECT data FROM liquid_clustering WHERE year = 2024 AND month = 1", "VALUES 'test 1'");
        assertQuery("SELECT data FROM liquid_clustering WHERE year = 2024 AND month = 2", "VALUES 'test 2'");

        assertQueryReturnsEmptyResult("SELECT * FROM liquid_clustering FOR VERSION AS OF 0");
        assertQuery("SELECT * FROM liquid_clustering FOR VERSION AS OF 1", "VALUES ('test 1', 2024, 1)");
        assertQuery("SELECT * FROM liquid_clustering FOR VERSION AS OF 2", "VALUES ('test 1', 2024, 1), ('test 2', 2024, 2)");
        assertQuery("SELECT * FROM liquid_clustering FOR VERSION AS OF 3", "VALUES ('test 1', 2024, 1), ('test 2', 2024, 2)");

        assertQueryFails("INSERT INTO liquid_clustering VALUES ('test 3', 2024, 3)", "Unsupported writer features: .*");
    }

    /**
     * @see deltalake.uniform_hudi
     */
    @Test
    public void testUniFormHudi()
    {
        assertQuery("SELECT * FROM uniform_hudi", "VALUES (123)");
        assertQueryFails("INSERT INTO uniform_hudi VALUES (456)", "\\QUnsupported universal formats: [hudi]");
        assertQueryFails("CALL system.vacuum(CURRENT_SCHEMA, 'uniform_hudi', '7d')", "\\QUnsupported universal formats: [hudi]");
    }

    /**
     * @see databricks133.uniform_iceberg_v1
     */
    @Test
    public void testUniFormIcebergV1()
    {
        assertQuery("SELECT * FROM uniform_iceberg_v1", "VALUES (1, 'test data')");
        assertQueryFails("INSERT INTO uniform_iceberg_v1 VALUES (2, 'new data')", "\\QUnsupported universal formats: [iceberg]");
    }

    /**
     * @see databricks143.uniform_iceberg_v2
     */
    @Test
    public void testUniFormIcebergV2()
    {
        assertQuery("SELECT * FROM uniform_iceberg_v2", "VALUES (1, 'test data')");
        assertQueryFails("INSERT INTO uniform_iceberg_v2 VALUES (2, 'new data')", "\\QUnsupported universal formats: [iceberg]");
    }

    /**
     * @see databricks153.variant
     */
    @Test
    public void testVariant()
    {
        // TODO (https://github.com/trinodb/trino/issues/22309) Add support for variant type
        assertThat(query("DESCRIBE variant")).result().projected("Column", "Type")
                .skippingTypesCheck()
                .matches("VALUES ('col_int', 'integer'), ('col_string', 'varchar')");

        assertQuery("SELECT * FROM variant", "VALUES (1, 'test data')");

        assertQueryFails("INSERT INTO variant VALUES (2, 'new data')", "Unsupported writer features: .*");
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
        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(Path.of(getResourceLocation("databricks73/person").toURI()), tableLocation);
        getQueryRunner().execute(
                format("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')", tableName, tableLocation));
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
        assertQueryFails("SELECT * FROM \"" + tableName + "$partitions\"", "Metadata not found in transaction log for tpch." + tableName + "\\$partitions");
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
        assertQueryFails("SELECT * FROM TABLE(system.table_changes(CURRENT_SCHEMA, '" + tableName + "'))", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("CREATE OR REPLACE TABLE " + tableName + " (id INTEGER)", "Metadata not found in transaction log for tpch." + tableName);
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
        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource("deltalake/multipart_checkpoint").toURI()).toPath(), tableLocation);

        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, tableLocation.toUri()));
        assertThat(query("DESCRIBE " + tableName)).result().projected("Column", "Type").skippingTypesCheck().matches("VALUES ('c', 'integer')");
        assertThat(query("SELECT * FROM " + tableName)).matches("VALUES 1, 2, 3, 4, 5, 6, 7");
    }

    @Test
    public void testTimeTravelWithMultipartCheckpoint()
            throws Exception
    {
        String tableName = "test_time_travel_multipart_checkpoint_" + randomNameSuffix();
        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource("deltalake/multipart_checkpoint").toURI()).toPath(), tableLocation);
        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, tableLocation.toUri()));

        // Version 6 has multipart checkpoint
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 5")).matches("VALUES 1, 2, 3, 4, 5");
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 6")).matches("VALUES 1, 2, 3, 4, 5, 6");
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 7")).matches("VALUES 1, 2, 3, 4, 5, 6, 7");

        // Redo the time travel without _last_checkpoint file
        Files.delete(tableLocation.resolve("_delta_log/_last_checkpoint"));
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => '" + tableName + "')");
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 5")).matches("VALUES 1, 2, 3, 4, 5");
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 6")).matches("VALUES 1, 2, 3, 4, 5, 6");
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 7")).matches("VALUES 1, 2, 3, 4, 5, 6, 7");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testTimeTravelWithV2Checkpoint()
            throws Exception
    {
        testTimeTravelWithV2Checkpoint("deltalake/v2_checkpoint_json");
        testTimeTravelWithV2Checkpoint("deltalake/v2_checkpoint_parquet");
        testTimeTravelWithV2Checkpoint("databricks133/v2_checkpoint_json");
        testTimeTravelWithV2Checkpoint("databricks133/v2_checkpoint_parquet");
    }

    private void testTimeTravelWithV2Checkpoint(String resourceName)
            throws Exception
    {
        String tableName = "test_time_travel_v2_checkpoint_" + randomNameSuffix();
        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource(resourceName).toURI()).toPath(), tableLocation);
        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, tableLocation.toUri()));

        // Version 1 has v2 checkpoint
        assertQueryReturnsEmptyResult("SELECT * FROM " + tableName + " FOR VERSION AS OF 0");
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 1")).matches("VALUES (1, 2)");

        // Redo the time travel without _last_checkpoint file
        Files.delete(tableLocation.resolve("_delta_log/_last_checkpoint"));
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => '" + tableName + "')");
        assertQueryReturnsEmptyResult("SELECT * FROM " + tableName + " FOR VERSION AS OF 0");
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 1")).matches("VALUES (1, 2)");

        assertUpdate("DROP TABLE " + tableName);
    }

    /**
     * @see deltalake.partition_values_parsed
     */
    @Test
    public void testDeltaLakeWithPartitionValuesParsed()
            throws Exception
    {
        testPartitionValuesParsed("deltalake/partition_values_parsed");
    }

    /**
     * @see trino432.partition_values_parsed
     */
    @Test
    public void testTrinoWithoutPartitionValuesParsed()
            throws Exception
    {
        testPartitionValuesParsed("trino432/partition_values_parsed");
    }

    private void testPartitionValuesParsed(String resourceName)
            throws Exception
    {
        String tableName = "test_partition_values_parsed_checkpoint_" + randomNameSuffix();
        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource(resourceName).toURI()).toPath(), tableLocation);
        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, tableLocation.toUri()));

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty("delta", "checkpoint_filtering_enabled", "false")
                .build();

        assertThat(query("SELECT id FROM " + tableName + " WHERE int_part = 10 AND string_part = 'part1'"))
                .matches("VALUES 1");
        assertThat(query("SELECT id FROM " + tableName + " WHERE int_part != 10"))
                .matches("VALUES 2");
        assertThat(query("SELECT id FROM " + tableName + " WHERE int_part > 10"))
                .matches("VALUES 2");
        assertThat(query("SELECT id FROM " + tableName + " WHERE int_part >= 10"))
                .matches("VALUES 1, 2");
        assertThat(query("SELECT id FROM " + tableName + " WHERE int_part IN (10, 20)"))
                .matches("VALUES 1, 2");
        assertThat(query(session, "SELECT id FROM " + tableName + " WHERE int_part IS NULL AND string_part IS NULL"))
                .matches("VALUES 3");
        assertThat(query(session, "SELECT id FROM " + tableName + " WHERE int_part IS NOT NULL AND string_part IS NOT NULL"))
                .matches("VALUES 1, 2");

        assertThat(query(session, "SELECT id FROM " + tableName + " WHERE int_part = 10 AND string_part = 'unmatched partition condition'"))
                .returnsEmptyResult();
        assertThat(query(session, "SELECT id FROM " + tableName + " WHERE int_part IS NULL AND string_part IS NOT NULL"))
                .returnsEmptyResult();
    }

    /**
     * @see databricks133.parsed_stats_struct
     */
    @Test
    public void testCheckpointFilteringForParsedStatsContainingNestedRows()
            throws Exception
    {
        String tableName = "test_parsed_stats_struct_" + randomNameSuffix();
        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource("databricks133/parsed_stats_struct").toURI()).toPath(), tableLocation);

        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, tableLocation.toUri()));
        assertThat(query("SELECT * FROM " + tableName))
                .skippingTypesCheck()
                .matches(
                        """
                        VALUES
                        (100, 1, row(1, 'ala')),
                        (200, 2, row(2, 'kota')),
                        (300, 3, row(3, 'osla')),
                        (400, 4, row(4, 'zulu'))
                        """);

        assertThat(query("SELECT id FROM " + tableName + " WHERE part BETWEEN 100 AND 300")).matches("VALUES 1, 2, 3");
        assertThat(query("SELECT root.entry_two FROM " + tableName + " WHERE part BETWEEN 100 AND 300"))
                .skippingTypesCheck()
                .matches("VALUES 'ala', 'kota', 'osla'");
        // show stats with predicate
        assertThat(query("SHOW STATS FOR (SELECT id FROM " + tableName + " WHERE part = 100)"))
                .skippingTypesCheck()
                .matches(
                        """
                        VALUES
                        ('id', NULL, NULL, DOUBLE '0.0' , NULL, '1', '1'),
                        (NULL, NULL, NULL, NULL, DOUBLE '1.0', NULL, NULL)
                        """);
    }

    /**
     * @see databricks133.parsed_stats_case_sensitive
     */
    @Test
    public void testCheckpointFilteringForParsedStatsWithCaseSensitiveColumnNames()
            throws Exception
    {
        String tableName = "test_parsed_stats_case_sensitive_" + randomNameSuffix();
        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource("databricks133/parsed_stats_case_sensitive").toURI()).toPath(), tableLocation);

        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, tableLocation.toUri()));
        assertThat(query("SELECT * FROM " + tableName))
                .skippingTypesCheck()
                .matches(
                        """
                        VALUES
                        (100, 1, 'ala'),
                        (200, 2, 'kota'),
                        (300, 3, 'osla'),
                        (400, 4, 'zulu')
                        """);

        assertThat(query("SELECT a_NuMbEr FROM " + tableName + " WHERE part BETWEEN 100 AND 300")).matches("VALUES 1, 2, 3");
        assertThat(query("SELECT a_StRiNg FROM " + tableName + " WHERE part BETWEEN 100 AND 300"))
                .skippingTypesCheck()
                .matches("VALUES 'ala', 'kota', 'osla'");
        // show stats with predicate
        assertThat(query("SHOW STATS FOR (SELECT a_NuMbEr FROM " + tableName + " WHERE part BETWEEN 100 AND 300)"))
                .skippingTypesCheck()
                .matches(
                        """
                        VALUES
                        ('a_NuMbEr', NULL, NULL, DOUBLE '0.0' , NULL, '1', '3'),
                        (NULL, NULL, NULL, NULL, DOUBLE '3.0', NULL, NULL)
                        """);
    }

    /**
     * @see deltalake.partition_values_parsed_all_types
     */
    @Test
    public void testDeltaLakeWithPartitionValuesParsedAllTypes()
            throws Exception
    {
        String tableName = "test_partition_values_parsed_checkpoint_" + randomNameSuffix();
        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource("deltalake/partition_values_parsed_all_types").toURI()).toPath(), tableLocation);
        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, tableLocation.toUri()));

        assertPartitionValuesParsedCondition(tableName, 1, "part_boolean = true");
        assertPartitionValuesParsedCondition(tableName, 1, "part_tinyint = 1");
        assertPartitionValuesParsedCondition(tableName, 1, "part_smallint = 10");
        assertPartitionValuesParsedCondition(tableName, 1, "part_int = 100");
        assertPartitionValuesParsedCondition(tableName, 1, "part_bigint = 1000");
        assertPartitionValuesParsedCondition(tableName, 1, "part_short_decimal = CAST('123.12' AS DECIMAL(5,2))");
        assertPartitionValuesParsedCondition(tableName, 1, "part_long_decimal = CAST('123456789012345678.123' AS DECIMAL(21,3))");
        assertPartitionValuesParsedCondition(tableName, 1, "part_double = 1.2");
        assertPartitionValuesParsedCondition(tableName, 1, "part_float = 3.4");
        assertPartitionValuesParsedCondition(tableName, 1, "part_varchar = 'a'");
        assertPartitionValuesParsedCondition(tableName, 1, "part_date = DATE '2020-08-21'");
        assertPartitionValuesParsedCondition(tableName, 1, "part_timestamp = TIMESTAMP '2020-10-21 01:00:00.123 UTC'");
        assertPartitionValuesParsedCondition(tableName, 1, "part_timestamp_ntz = TIMESTAMP '2023-01-02 01:02:03.456'");

        assertPartitionValuesParsedCondition(tableName, 3, "part_boolean IS NULL");
        assertPartitionValuesParsedCondition(tableName, 3, "part_tinyint IS NULL");
        assertPartitionValuesParsedCondition(tableName, 3, "part_smallint IS NULL");
        assertPartitionValuesParsedCondition(tableName, 3, "part_int IS NULL");
        assertPartitionValuesParsedCondition(tableName, 3, "part_bigint IS NULL");
        assertPartitionValuesParsedCondition(tableName, 3, "part_short_decimal IS NULL");
        assertPartitionValuesParsedCondition(tableName, 3, "part_long_decimal IS NULL");
        assertPartitionValuesParsedCondition(tableName, 3, "part_double IS NULL");
        assertPartitionValuesParsedCondition(tableName, 3, "part_float IS NULL");
        assertPartitionValuesParsedCondition(tableName, 3, "part_varchar IS NULL");
        assertPartitionValuesParsedCondition(tableName, 3, "part_date IS NULL");
        assertPartitionValuesParsedCondition(tableName, 3, "part_timestamp IS NULL");
        assertPartitionValuesParsedCondition(tableName, 3, "part_timestamp_ntz IS NULL");
    }

    /**
     * @see deltalake.partition_values_parsed_all_types
     */
    @Test
    public void testDeltaLakeWritePartitionValuesParsedAllTypesInCheckpoint()
            throws Exception
    {
        String tableName = "test_write_partition_values_parsed_checkpoint_" + randomNameSuffix();
        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource("deltalake/partition_values_parsed_all_types").toURI()).toPath(), tableLocation);
        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, tableLocation.toUri()));

        assertThat(query("SELECT * FROM " + tableName))
                .skippingTypesCheck()
                .matches(
                        """
                        VALUES
                            (1, true, TINYINT '1', SMALLINT '10', 100, BIGINT '1000', CAST('123.12' AS DECIMAL(5,2)), CAST('123456789012345678.123' AS DECIMAL(21,3)), DOUBLE '1.2', REAL '3.4', 'a', DATE '2020-08-21', TIMESTAMP '2020-10-21 01:00:00.123 UTC', TIMESTAMP '2023-01-02 01:02:03.456'),
                            (2, false, TINYINT '2', SMALLINT '20', 200, BIGINT '2000', CAST('223.12' AS DECIMAL (5,2)), CAST('223456789012345678.123' AS DECIMAL(21,3)), DOUBLE '10.2', REAL '30.4', 'b', DATE '2020-08-22', TIMESTAMP '2020-10-22 01:00:00.123 UTC', TIMESTAMP '2023-01-03 01:02:03.456'),
                            (3, null, null, null, null, null, null, null, null, null, null, null, null, null)
                        """);

        // Create a new checkpoint
        assertUpdate("INSERT INTO " + tableName + " VALUES (4, false, TINYINT '4', SMALLINT '40', 400, BIGINT '4000', CAST('444.44' AS DECIMAL(5,2)), CAST('4444444.444' AS DECIMAL(21,3)), DOUBLE '4.4', REAL '4.4', 'd', DATE '2020-08-24', TIMESTAMP '2020-10-24 01:00:00.123 UTC', TIMESTAMP '2023-01-04 01:02:03.456')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (5, false, TINYINT '5', SMALLINT '50', 500, BIGINT '5000', CAST('555.5' AS DECIMAL(5,2)), CAST('55555.55' AS DECIMAL(21,3)), DOUBLE '5.55', REAL '5.5555', 'd', DATE '2020-08-25', TIMESTAMP '2020-10-25 01:00:00.123 UTC', TIMESTAMP '2023-01-05 01:02:03.456')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (6, null, null, null, null, null, null, null, null, null, null, null, null, null)", 1);

        assertThat(query("SELECT * FROM " + tableName))
                .skippingTypesCheck()
                .matches(
                        """
                        VALUES
                            (1, true, TINYINT '1', SMALLINT '10', 100, BIGINT '1000', CAST('123.12' AS DECIMAL(5,2)), CAST('123456789012345678.123' AS DECIMAL(21,3)), DOUBLE '1.2', REAL '3.4', 'a', DATE '2020-08-21', TIMESTAMP '2020-10-21 01:00:00.123 UTC', TIMESTAMP '2023-01-02 01:02:03.456'),
                            (2, false, TINYINT '2', SMALLINT '20', 200, BIGINT '2000', CAST('223.12' AS DECIMAL (5,2)), CAST('223456789012345678.123' AS DECIMAL(21,3)), DOUBLE '10.2', REAL '30.4', 'b', DATE '2020-08-22', TIMESTAMP '2020-10-22 01:00:00.123 UTC', TIMESTAMP '2023-01-03 01:02:03.456'),
                            (3, null, null, null, null, null, null, null, null, null, null, null, null, null),
                            (4, false, TINYINT '4', SMALLINT '40', 400, BIGINT '4000', CAST('444.44' AS DECIMAL(5,2)), CAST('4444444.444' AS DECIMAL(21,3)), DOUBLE '4.4', REAL '4.4', 'd', DATE '2020-08-24', TIMESTAMP '2020-10-24 01:00:00.123 UTC', TIMESTAMP '2023-01-04 01:02:03.456'),
                            (5, false, TINYINT '5', SMALLINT '50', 500, BIGINT '5000', CAST('555.5' AS DECIMAL(5,2)), CAST('55555.55' AS DECIMAL(21,3)), DOUBLE '5.55', REAL '5.5555', 'd', DATE '2020-08-25', TIMESTAMP '2020-10-25 01:00:00.123 UTC', TIMESTAMP '2023-01-05 01:02:03.456'),
                            (6, null, null, null, null, null, null, null, null, null, null, null, null, null)
                        """);

        assertThat(query(
                """
                SELECT id
                FROM %s
                WHERE
                    part_boolean = true AND
                    part_tinyint = TINYINT '1' AND
                    part_smallint= SMALLINT '10' AND
                    part_int = 100 AND
                    part_bigint = BIGINT '1000' AND
                    part_short_decimal = CAST('123.12' AS DECIMAL(5,2)) AND
                    part_long_decimal = CAST('123456789012345678.123' AS DECIMAL(21,3)) AND
                    part_double = DOUBLE '1.2' AND
                    part_float = REAL '3.4' AND
                    part_varchar = 'a' AND
                    part_date = DATE '2020-08-21' AND
                    part_timestamp = TIMESTAMP '2020-10-21 01:00:00.123 UTC' AND
                    part_timestamp_ntz =TIMESTAMP '2023-01-02 01:02:03.456'\
                    """.formatted(tableName)))
                .matches("VALUES 1");
    }

    /**
     * @see databricks133.partition_values_parsed_case_sensitive
     */
    @Test
    public void testDeltaLakeWritePartitionValuesParsedCaseSensitiveInCheckpoint()
            throws Exception
    {
        String tableName = "test_write_partition_values_parsed_checkpoint_" + randomNameSuffix();
        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource("databricks133/partition_values_parsed_case_sensitive").toURI()).toPath(), tableLocation);
        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, tableLocation.toUri()));

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty("delta", "checkpoint_filtering_enabled", "false")
                .build();
        assertThat(query(session, "SELECT * FROM " + tableName))
                .skippingTypesCheck()
                .matches(
                        """
                        VALUES
                            (100, 1, 'ala'),
                            (200, 2,'kota'),
                            (300, 3, 'osla')
                        """);

        // Create a new checkpoint
        assertUpdate("INSERT INTO " + tableName + " VALUES (400, 4, 'kon')", 1);
        assertThat(query(session, "SELECT * FROM " + tableName))
                .skippingTypesCheck()
                .matches(
                        """
                        VALUES
                            (100, 1, 'ala'),
                            (200, 2,'kota'),
                            (300, 3, 'osla'),
                            (400, 4, 'kon')
                        """);
        assertThat(query("SELECT id FROM " + tableName + " WHERE part_NuMbEr = 1 AND part_StRiNg = 'ala'"))
                .matches("VALUES 100");
    }

    private void assertPartitionValuesParsedCondition(String tableName, int id, @Language("SQL") String condition)
    {
        assertThat(query("SELECT id FROM " + tableName + " WHERE " + condition))
                .matches("VALUES " + id);
    }

    @Test
    public void testReadV2Checkpoint()
            throws Exception
    {
        testReadV2Checkpoint("deltalake/v2_checkpoint_json");
        testReadV2Checkpoint("deltalake/v2_checkpoint_parquet");
        testReadV2Checkpoint("databricks133/v2_checkpoint_json");
        testReadV2Checkpoint("databricks133/v2_checkpoint_parquet");
    }

    private void testReadV2Checkpoint(String resourceName)
            throws Exception
    {
        String tableName = "test_v2_checkpoint_" + randomNameSuffix();
        Path tableLocation = catalogDir.resolve(tableName);
        Path source = new File(Resources.getResource(resourceName).toURI()).toPath();
        copyDirectoryContents(source, tableLocation);
        assertThat(source.resolve("_delta_log/_last_checkpoint"))
                .content().contains("v2Checkpoint").contains("sidecar");

        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, tableLocation.toUri()));
        assertThat(query("DESCRIBE " + tableName))
                .result()
                .projected("Column", "Type")
                .skippingTypesCheck()
                .matches("VALUES ('a', 'integer'), ('b', 'integer')");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 2)");

        // Write-operations should fail
        assertQueryFails(
                "INSERT INTO " + tableName + " VALUES (3, 4)",
                "\\QUnsupported writer features: [v2Checkpoint]");
        assertQueryFails(
                "UPDATE " + tableName + " SET a = 10",
                "\\QUnsupported writer features: [v2Checkpoint]");
        assertQueryFails(
                "DELETE FROM " + tableName,
                "\\QUnsupported writer features: [v2Checkpoint]");
        assertQueryFails(
                "TRUNCATE TABLE " + tableName,
                "\\QUnsupported writer features: [v2Checkpoint]");
        assertQueryFails(
                "MERGE INTO " + tableName + " USING (VALUES 42) t(dummy) ON false WHEN NOT MATCHED THEN INSERT VALUES (3, 4)",
                "\\QUnsupported writer features: [v2Checkpoint]");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 2)");
    }

    @Test
    public void testTypeWidening()
            throws Exception
    {
        String tableName = "test_type_widening_" + randomNameSuffix();
        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource("deltalake/type_widening").toURI()).toPath(), tableLocation);
        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, tableLocation.toUri()));

        assertThat(query("DESCRIBE " + tableName)).result().projected("Column", "Type")
                .skippingTypesCheck()
                .matches("VALUES ('col', 'integer')");
        assertQuery("SELECT * FROM " + tableName, "VALUES 127, 32767, 2147483647");

        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 0"))
                .returnsEmptyResult();
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 1"))
                .matches("VALUES tinyint '127'");
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 2"))
                .matches("VALUES smallint '127'");
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 3"))
                .matches("VALUES smallint '127', smallint '32767'");
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 4"))
                .matches("VALUES integer '127', integer '32767'");
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 5"))
                .matches("VALUES integer '127', integer '32767', integer '2147483647'");
    }

    /**
     * @see databricks153.type_widening_partition
     */
    @Test
    public void testTypeWideningNotSkippingUnsupportedPartitionColumns()
    {
        assertThat(query("DESCRIBE type_widening_partition")).result().projected("Column", "Type")
                .skippingTypesCheck()
                .matches("VALUES ('col', 'tinyint'), " +
                        "('byte_to_short', 'smallint'), " +
                        "('byte_to_int', 'integer'), " +
                        "('byte_to_long', 'bigint'), " +
                        "('byte_to_decimal', 'decimal(10,0)'), " +
                        "('byte_to_double', 'double'), " +
                        "('short_to_int', 'integer'), " +
                        "('short_to_long', 'bigint'), " +
                        "('short_to_decimal', 'decimal(10,0)'), " +
                        "('short_to_double', 'double'), " +
                        "('int_to_long', 'bigint'), " +
                        "('int_to_decimal', 'decimal(10,0)'), " +
                        "('int_to_double', 'double'), " +
                        "('long_to_decimal', 'decimal(20,0)'), " +
                        "('float_to_double', 'double'), " +
                        "('decimal_to_decimal', 'decimal(12,2)'), " +
                        "('date_to_timestamp', 'timestamp(6)')");
        assertThat(query("SELECT * FROM type_widening_partition FOR VERSION AS OF 0"))
                .returnsEmptyResult();
        assertThat(query("SELECT * FROM type_widening_partition FOR VERSION AS OF 1"))
                .matches("VALUES (tinyint '1', " +
                        "tinyint '1', " +
                        "tinyint '1', " +
                        "tinyint '1', " +
                        "tinyint '1', " +
                        "tinyint '1', " +
                        "smallint '1', " +
                        "smallint '1', " +
                        "smallint '1', " +
                        "smallint '1', " +
                        "integer '1', " +
                        "integer '1', " +
                        "integer '1', " +
                        "bigint '1', " +
                        "real '1', " +
                        "CAST('1' AS decimal(10,0)), " +
                        "DATE '2024-06-19')");
        assertThat(query("SELECT * FROM type_widening_partition FOR VERSION AS OF 18"))
                .matches("VALUES (tinyint '1', " +
                        "smallint '1', " +
                        "integer '1', " +
                        "bigint '1', " +
                        "CAST('1' AS decimal(10,0)), " +
                        "double '1', " +
                        "integer '1', " +
                        "bigint '1', " +
                        "CAST('1' AS decimal(10,0)), " +
                        "double '1', " +
                        "bigint '1', " +
                        "CAST('1' AS decimal(10,0)), " +
                        "double '1', " +
                        "CAST('1' AS decimal(20,0)), " +
                        "double '1', " +
                        "CAST('1' AS decimal(12,2)), " +
                        "CAST('2024-06-19' AS timestamp(6))), " +
                        "(tinyint '2', " +
                        "smallint '256', " +
                        "integer '35000', " +
                        "bigint '2147483650', " +
                        "CAST('9223372036' AS decimal(10,0)), " +
                        "double '9.323372040456E9', " +
                        "integer '35000', " +
                        "bigint '2147483650', " +
                        "CAST('9223372036' AS decimal(10,0)), " +
                        "double '9.323372040456E9', " +
                        "bigint '2147483650', " +
                        "CAST('9223372036' AS decimal(10,0)), " +
                        "double '9.323372040456E9', " +
                        "CAST('92233720368547758073' AS decimal(20,0)), " +
                        "double '3.403E38', " +
                        "CAST('9223372036.25' AS decimal(12,2)), " +
                        "TIMESTAMP '2021-07-01 08:43:28.123456')");
    }

    /**
     * @see databricks153.type_widening
     */
    @Test
    public void testTypeWideningSkippingUnsupportedColumns()
    {
        assertQuery("SELECT * FROM type_widening FOR VERSION AS OF 1", "VALUES (1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1.0, 1, DATE '2024-06-19')");
        assertThat(query("DESCRIBE type_widening")).result().projected("Column", "Type")
                .skippingTypesCheck()
                .matches("VALUES ('byte_to_short', 'smallint'), ('byte_to_int', 'integer'), ('short_to_int', 'integer')");
        assertQuery("SELECT * FROM type_widening", "VALUES (1, 1, 1), (2, 2, 2)");
    }

    /**
     * @see databricks153.type_widening_nested
     */
    @Test
    public void testTypeWideningSkippingUnsupportedNested()
    {
        assertThat(query("DESCRIBE type_widening_nested")).result().projected("Column", "Type")
                .skippingTypesCheck()
                .isEmpty();
        assertQueryFails("SELECT s FROM type_widening_nested", "(.*)Column 's' cannot be resolved");
        assertQueryFails("SELECT * FROM type_widening_nested", "(.*)SELECT \\* not allowed from relation that has no columns");

        assertThat(query("SELECT * FROM type_widening_nested FOR VERSION AS OF 0"))
                .returnsEmptyResult();
        assertThat(query("SELECT * FROM type_widening_nested FOR VERSION AS OF 1"))
                .matches("VALUES (CAST(ROW(127) AS ROW (field tinyint)), CAST(ROW(127, ROW(15)) AS ROW (field tinyint, field2 ROW(inner_field tinyint))), MAP(ARRAY[tinyint '-128'], ARRAY[tinyint '127']), ARRAY[tinyint '127'])");

        // 2,3,4,5,6 versions changed nested fields from byte to short
        assertThat(query("SELECT * FROM type_widening_nested FOR VERSION AS OF 7"))
                .matches("VALUES (CAST(ROW(127) AS ROW (field smallint)), CAST(ROW(127, ROW(15)) AS ROW (field smallint, field2 ROW(inner_field smallint))), MAP(ARRAY[smallint '-128'], ARRAY[smallint '127']), ARRAY[smallint '127'])");

        assertThat(query("SELECT * FROM type_widening_nested FOR VERSION AS OF 8"))
                .matches("VALUES " +
                        "(CAST(ROW(127) AS ROW (field smallint)), CAST(ROW(127, ROW(15)) AS ROW (field smallint, field2 ROW(inner_field smallint))), MAP(ARRAY[smallint '-128'], ARRAY[smallint '127']), ARRAY[smallint '127'])," +
                        "(CAST(ROW(32767) AS ROW (field smallint)), CAST(ROW(32767, ROW(32767)) AS ROW (field smallint, field2 ROW(inner_field smallint))), MAP(ARRAY[smallint '-32768'], ARRAY[smallint '32767']), ARRAY[smallint '32767'])");

        // 9,10,11,12 versions changed nested fields from short to integer/double
        assertQueryFails("SELECT * FROM type_widening_nested FOR VERSION AS OF 13", "(.*)SELECT \\* not allowed from relation that has no columns");
    }

    @Test
    public void testTypeWideningNested()
            throws Exception
    {
        String tableName = "test_type_widening_nestd_" + randomNameSuffix();
        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource("deltalake/type_widening_nested").toURI()).toPath(), tableLocation);
        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, tableLocation.toUri()));

        assertThat(query("DESCRIBE " + tableName)).result().projected("Column", "Type")
                .skippingTypesCheck()
                .matches("VALUES ('s', 'row(field integer)'), ('m', 'map(integer, integer)'), ('a', 'array(integer)')");
        assertThat(query("SELECT * FROM " + tableName))
                .matches("VALUES " +
                        "(CAST(ROW(127) AS ROW(field integer)), MAP(ARRAY[-128], ARRAY[127]), ARRAY[127])," +
                        "(CAST(ROW(32767) AS ROW(field integer)), MAP(ARRAY[-32768], ARRAY[32767]), ARRAY[32767])," +
                        "(CAST(ROW(2147483647) AS ROW(field integer)), MAP(ARRAY[-2147483648], ARRAY[2147483647]), ARRAY[2147483647])");

        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 0"))
                .returnsEmptyResult();
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 1"))
                .matches("VALUES (CAST(ROW(127) AS ROW(field tinyint)), MAP(ARRAY[tinyint '-128'], ARRAY[tinyint '127']), ARRAY[tinyint '127'])");

        // 2,3,4 versions changed nested fields from byte to short
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 5"))
                .matches("VALUES (CAST(ROW(127) AS ROW(field smallint)), MAP(ARRAY[smallint '-128'], ARRAY[smallint '127']), ARRAY[smallint '127'])");

        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 6"))
                .matches("VALUES " +
                        "(CAST(ROW(127) AS ROW(field smallint)), MAP(ARRAY[smallint '-128'], ARRAY[smallint '127']), ARRAY[smallint '127'])," +
                        "(CAST(ROW(32767) AS ROW(field smallint)), MAP(ARRAY[smallint '-32768'], ARRAY[smallint '32767']), ARRAY[smallint '32767'])");

        // 7,8,9 versions changed nested fields from short to int
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 10"))
                .matches("VALUES " +
                        "(CAST(ROW(127) AS ROW(field integer)), MAP(ARRAY[integer '-128'], ARRAY[integer '127']), ARRAY[integer '127'])," +
                        "(CAST(ROW(32767) AS ROW(field integer)), MAP(ARRAY[integer '-32768'], ARRAY[integer '32767']), ARRAY[integer '32767'])");

        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 11"))
                .matches("VALUES " +
                        "(CAST(ROW(127) AS ROW(field integer)), MAP(ARRAY[-128], ARRAY[127]), ARRAY[127])," +
                        "(CAST(ROW(32767) AS ROW(field integer)), MAP(ARRAY[-32768], ARRAY[32767]), ARRAY[32767])," +
                        "(CAST(ROW(2147483647) AS ROW(field integer)), MAP(ARRAY[-2147483648], ARRAY[2147483647]), ARRAY[2147483647])");
    }

    @Test
    public void testTypeWideningUnsupported()
            throws Exception
    {
        String tableName = "test_type_widening_" + randomNameSuffix();
        Path tableLocation = catalogDir.resolve(tableName);
        copyDirectoryContents(new File(Resources.getResource("deltalake/type_widening_unsupported").toURI()).toPath(), tableLocation);
        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, tableLocation.toUri()));

        assertQueryFails("SELECT * FROM " + tableName, "(.*)SELECT \\* not allowed from relation that has no columns");
        assertQueryFails("SELECT col FROM " + tableName, "(.*)Column 'col' cannot be resolved");
    }

    /**
     * @see deltalake.unsupported_writer_feature
     */
    @Test
    public void testUnsupportedWriterFeature()
    {
        assertQueryReturnsEmptyResult("SELECT * FROM unsupported_writer_feature");

        assertQueryFails(
                "ALTER TABLE unsupported_writer_feature ADD COLUMN new_col int",
                "\\QUnsupported writer features: [generatedColumns]");
        assertQueryFails(
                "ALTER TABLE unsupported_writer_feature RENAME COLUMN a TO renamed",
                "\\QUnsupported writer features: [generatedColumns]");
        assertQueryFails(
                "ALTER TABLE unsupported_writer_feature DROP COLUMN b",
                "\\QUnsupported writer features: [generatedColumns]");
        assertQueryFails(
                "ALTER TABLE unsupported_writer_feature ALTER COLUMN b DROP NOT NULL",
                "\\QUnsupported writer features: [generatedColumns]");
        assertQueryFails(
                "ALTER TABLE unsupported_writer_feature EXECUTE OPTIMIZE",
                "\\QUnsupported writer features: [generatedColumns]");
        assertQueryFails(
                "ALTER TABLE unsupported_writer_feature ALTER COLUMN b SET DATA TYPE bigint",
                "This connector does not support setting column types");
        assertQueryFails(
                "COMMENT ON TABLE unsupported_writer_feature IS 'test comment'",
                "\\QUnsupported writer features: [generatedColumns]");
        assertQueryFails(
                "COMMENT ON COLUMN unsupported_writer_feature.a IS 'test column comment'",
                "\\QUnsupported writer features: [generatedColumns]");
        assertQueryFails(
                "CALL delta.system.vacuum('tpch', 'unsupported_writer_feature', '7d')",
                "\\QCannot execute vacuum procedure with [generatedColumns] writer features");
    }

    /**
     * @see deltalake.unsupported_writer_version
     */
    @Test
    public void testUnsupportedWriterVersion()
    {
        assertQueryReturnsEmptyResult("SELECT * FROM unsupported_writer_version");

        assertQueryFails(
                "ALTER TABLE unsupported_writer_version ADD COLUMN new_col int",
                "Table .* requires Delta Lake writer version 8 which is not supported");
        assertQueryFails(
                "COMMENT ON TABLE unsupported_writer_version IS 'test comment'",
                "Table .* requires Delta Lake writer version 8 which is not supported");
        assertQueryFails(
                "COMMENT ON COLUMN unsupported_writer_version.col IS 'test column comment'",
                "Table .* requires Delta Lake writer version 8 which is not supported");
        assertQueryFails(
                "ALTER TABLE unsupported_writer_version EXECUTE OPTIMIZE",
                "Table .* requires Delta Lake writer version 8 which is not supported");
        assertQueryFails(
                "CALL delta.system.vacuum('tpch', 'unsupported_writer_version', '7d')",
                "Cannot execute vacuum procedure with 8 writer version");
    }

    private static MetadataEntry loadMetadataEntry(long entryNumber, Path tableLocation)
            throws IOException
    {
        DeltaLakeTransactionLogEntry transactionLog = getEntriesFromJson(entryNumber, tableLocation.resolve("_delta_log").toString()).stream()
                .filter(log -> log.getMetaData() != null)
                .collect(onlyElement());
        return transactionLog.getMetaData();
    }

    private static ProtocolEntry loadProtocolEntry(long entryNumber, Path tableLocation)
            throws IOException
    {
        DeltaLakeTransactionLogEntry transactionLog = getEntriesFromJson(entryNumber, tableLocation.resolve("_delta_log").toString()).stream()
                .filter(log -> log.getProtocol() != null)
                .collect(onlyElement());
        return transactionLog.getProtocol();
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

    private static List<DeltaLakeTransactionLogEntry> getEntriesFromJson(long entryNumber, String transactionLogDir)
            throws IOException
    {
        return TransactionLogTail.getEntriesFromJson(entryNumber, transactionLogDir, FILE_SYSTEM, DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE)
                .orElseThrow()
                .getEntriesList(FILE_SYSTEM);
    }
}
