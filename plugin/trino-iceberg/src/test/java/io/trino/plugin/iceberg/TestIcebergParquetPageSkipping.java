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
package io.trino.plugin.iceberg;

import com.google.common.io.Resources;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.metastore.HiveMetastore;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.plugin.hive.BaseTestParquetPageSkipping;
import io.trino.spi.metrics.Count;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.parquet.Parquet;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.io.Resources.getResource;
import static io.trino.parquet.reader.ParquetReader.COLUMN_INDEX_ROWS_FILTERED;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.SESSION;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.getParquetFileMetadata;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING;
import static org.apache.iceberg.TableProperties.PARQUET_PAGE_ROW_LIMIT;
import static org.apache.iceberg.TableProperties.PARQUET_PAGE_SIZE_BYTES;
import static org.apache.iceberg.mapping.NameMappingParser.toJson;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergParquetPageSkipping
        extends BaseTestParquetPageSkipping
{
    private TrinoFileSystem fileSystem;
    private HiveMetastore metastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.file-format", "PARQUET")
                .addIcebergProperty("parquet.use-column-index", "true")
                .addIcebergProperty("parquet.max-buffer-size", "1MB")
                .build();
    }

    @BeforeAll
    public void setUp()
    {
        fileSystem = getFileSystemFactory(getQueryRunner()).create(SESSION);
        metastore = getHiveMetastore(getQueryRunner());
    }

    @Override
    protected String createTableWithDataFile(String tableNamePrefix, String columnsDefinition, String resourceFileName)
            throws Exception
    {
        String tableName = tableName(tableNamePrefix);
        assertUpdate(format("CREATE TABLE %s %s WITH (format = 'PARQUET')", tableName, columnsDefinition));
        appendIndexedFile(tableName, resourceFileName, Optional.empty());
        return tableName;
    }

    @Override
    protected String timestampMillisType()
    {
        return "timestamp(3)";
    }

    @Test
    public void testPartitionEvolutionDoesNotReturnEmpty()
            throws Exception
    {
        String tableName = createTableWithDataFile(
                "test_partition_evolution",
                """
                (
                   orderkey bigint,
                   custkey bigint,
                   orderstatus varchar,
                   totalprice double,
                   orderdate date,
                   orderpriority varchar,
                   clerk varchar,
                   shippriority integer,
                   comment varchar,
                   rvalues array(double))
                """,
                "parquet_page_skipping/orders_sorted_by_totalprice/data.parquet");
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES partitioning = ARRAY['orderstatus']");
        appendIndexedFile(
                tableName,
                "parquet_page_skipping/orders_sorted_by_totalprice/data.parquet",
                Optional.of("O"));
        @Language("SQL") String query = "SELECT orderkey FROM " + tableName +
                " WHERE orderstatus = 'O' AND totalprice BETWEEN 100000 AND 131280";
        assertThat(assertColumnIndexResults(query)).isGreaterThan(0);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testPositionDeletesWithPageSkipping()
            throws Exception
    {
        testDeletesWithPageSkipping(2);
    }

    @Test
    public void testDeletionVectorsWithPageSkipping()
            throws Exception
    {
        testDeletesWithPageSkipping(3);
    }

    private void testDeletesWithPageSkipping(int formatVersion)
            throws Exception
    {
        String tableName = createParquetV2IndexedTable(formatVersion);
        @Language("SQL") String neighbors = "SELECT id, payload FROM " + tableName + " WHERE id BETWEEN 8 AND 12 ORDER BY id";
        assertQuery(neighbors, "VALUES (8, 'row-8'), (9, 'row-9'), (10, 'row-10'), (11, 'row-11'), (12, 'row-12')");

        assertUpdateWithPageSkipping("DELETE FROM " + tableName + " WHERE id = 10", 1);
        assertQuery(neighbors, "VALUES (8, 'row-8'), (9, 'row-9'), (11, 'row-11'), (12, 'row-12')");
        assertQueryReturnsEmptyResult("SELECT id FROM " + tableName + " WHERE id = 10");
        verifyFilteringWithColumnIndex("SELECT id, payload FROM " + tableName + " WHERE id BETWEEN 8 AND 12");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testParquetV2PageSkipping()
            throws Exception
    {
        String tableName = createParquetV2IndexedTable(2);
        assertParquetV2Pages(tableName);
        verifyFilteringWithColumnIndex("SELECT * FROM " + tableName + " WHERE id = 10");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testRowIdWithPageSkipping()
            throws Exception
    {
        String tableName = createParquetV2IndexedTable(3);
        @Language("SQL") String query = "SELECT id, \"$row_id\" FROM " + tableName + " WHERE id BETWEEN 8 AND 12";
        verifyFilteringWithColumnIndex(query);
        assertQuery(query, "VALUES (8, 8), (9, 9), (10, 10), (11, 11), (12, 12)");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testUpdateWithPageSkipping()
            throws Exception
    {
        String tableName = createParquetV2IndexedTable(2);
        assertUpdateWithPageSkipping("UPDATE " + tableName + " SET payload = 'updated' WHERE id = 10", 1);
        assertQuery(
                "SELECT id, payload FROM " + tableName + " WHERE id BETWEEN 8 AND 12 ORDER BY id",
                "VALUES (8, 'row-8'), (9, 'row-9'), (10, 'updated'), (11, 'row-11'), (12, 'row-12')");
        verifyFilteringWithColumnIndex("SELECT id, payload FROM " + tableName + " WHERE id BETWEEN 8 AND 12");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testMergeWithPageSkipping()
            throws Exception
    {
        String tableName = createParquetV2IndexedTable(2);
        assertUpdateWithPageSkipping(
                "MERGE INTO " + tableName + " t USING (VALUES BIGINT '10') s(id) ON t.id = s.id " +
                        "WHEN MATCHED THEN UPDATE SET payload = 'updated'",
                1);
        assertQuery(
                "SELECT id, payload FROM " + tableName + " WHERE id BETWEEN 8 AND 12 ORDER BY id",
                "VALUES (8, 'row-8'), (9, 'row-9'), (10, 'updated'), (11, 'row-11'), (12, 'row-12')");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testSessionKillSwitch()
            throws Exception
    {
        String tableName = createTableWithDataFile(
                "test_session_kill_switch",
                "(suppkey bigint, extendedprice decimal(12, 2), shipmode varchar, comment varchar)",
                "parquet_page_skipping/lineitem_sorted_by_suppkey/data.parquet");
        MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(
                noParquetColumnIndexFiltering(getSession()),
                "SELECT * FROM " + tableName + " WHERE suppkey BETWEEN 25 AND 35");
        assertThat(getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(result.queryId())
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .filter(summary -> summary.getOperatorType().startsWith("TableScan") || summary.getOperatorType().startsWith("Scan"))
                .flatMap(summary -> summary.getConnectorMetrics().getMetrics().keySet().stream())
                .noneMatch(COLUMN_INDEX_ROWS_FILTERED::equals))
                .isTrue();
        assertUpdate("DROP TABLE " + tableName);
    }

    private String createParquetV2IndexedTable(int formatVersion)
            throws Exception
    {
        String tableName = "test_iceberg_page_skipping_v2_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id bigint, payload varchar) WITH (format = 'PARQUET', format_version = " + formatVersion + ")");
        BaseTable table = IcebergTestUtils.loadTable(tableName, metastore, getFileSystemFactory(getQueryRunner()), ICEBERG_CATALOG, "tpch");
        Schema schema = table.schema();
        String dataPath = table.location() + "/data/v2-indexed-" + randomNameSuffix() + ".parquet";
        FileAppender<Record> writer = Parquet.write(table.io().newOutputFile(dataPath))
                .schema(schema)
                .createWriterFunc(GenericParquetWriter::create)
                .writerVersion(PARQUET_2_0)
                .set(PARQUET_PAGE_ROW_LIMIT, "32")
                .set(PARQUET_PAGE_SIZE_BYTES, "256")
                .build();
        try {
            Record record = GenericRecord.create(schema);
            for (long id = 0; id < 2000; id++) {
                record.setField("id", id);
                record.setField("payload", "row-" + id);
                writer.add(record);
            }
        }
        finally {
            writer.close();
        }
        DataFile dataFile = DataFiles.builder(table.spec())
                .withPath(dataPath)
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(writer.length())
                .withMetrics(writer.metrics())
                .build();
        table.newAppend()
                .appendFile(dataFile)
                .commit();
        return tableName;
    }

    private void assertParquetV2Pages(String tableName)
            throws Exception
    {
        String filePath = (String) computeScalar(format("SELECT file_path FROM \"%s$files\"", tableName));
        ParquetMetadata parquetMetadata = getParquetFileMetadata(fileSystem.newInputFile(Location.of(filePath)));
        assertThat(parquetMetadata.getBlocks()).isNotEmpty();
        boolean usesV2Pages = parquetMetadata.getBlocks().stream()
                .flatMap(block -> block.columns().stream())
                .anyMatch(column -> column.getEncodingStats() != null && column.getEncodingStats().usesV2Pages());
        assertThat(usesV2Pages).isTrue();
        boolean hasColumnIndex = parquetMetadata.getBlocks().stream()
                .flatMap(block -> block.columns().stream())
                .anyMatch(column -> column.getColumnIndexReference() != null);
        assertThat(hasColumnIndex).isTrue();
    }

    private void appendIndexedFile(String tableName, String resourceName, Optional<String> orderstatus)
            throws Exception
    {
        BaseTable table = IcebergTestUtils.loadTable(tableName, metastore, getFileSystemFactory(getQueryRunner()), ICEBERG_CATALOG, "tpch");
        String dataPath = table.location() + "/data/" + randomNameSuffix() + ".parquet";
        byte[] parquetFileData = Resources.toByteArray(getResource(resourceName));
        fileSystem.newOutputFile(Location.of(dataPath)).createOrOverwrite(parquetFileData);
        ParquetMetadata parquetMetadata = getParquetFileMetadata(fileSystem.newInputFile(Location.of(dataPath)));
        long recordCount = parquetMetadata.getBlocks().stream()
                .mapToLong(BlockMetadata::rowCount)
                .sum();
        DataFiles.Builder builder = DataFiles.builder(table.spec())
                .withPath(dataPath)
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(parquetFileData.length)
                .withRecordCount(recordCount);
        orderstatus.ifPresent(value -> builder.withPartition(new PartitionData(new Object[] {value})));
        table.newAppend()
                .appendFile(builder.build())
                .commit();
        table.updateProperties()
                .set(DEFAULT_NAME_MAPPING, toJson(MappingUtil.create(table.schema())))
                .commit();
    }

    private void assertUpdateWithPageSkipping(@Language("SQL") String sql, long expectedUpdateCount)
    {
        MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(getSession(), sql);
        assertThat(result.result().getUpdateCount()).hasValue(expectedUpdateCount);
        long rowsFilteredByColumnIndex = getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(result.queryId())
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .filter(summary -> summary.getOperatorType().startsWith("TableScan") || summary.getOperatorType().startsWith("Scan"))
                .map(summary -> summary.getConnectorMetrics().getMetrics().get(COLUMN_INDEX_ROWS_FILTERED))
                .filter(Objects::nonNull)
                .mapToLong(metric -> ((Count<?>) metric).getTotal())
                .sum();
        assertThat(rowsFilteredByColumnIndex).isGreaterThan(0);
    }
}
