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

import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.iceberg.PartitionStatisticsComputation.UnreadableBaseException;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ImmutableGenericPartitionStatisticsFile;
import org.apache.iceberg.InternalData;
import org.apache.iceberg.PartitionStatistics;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.PartitionStatsHandler;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.loadTable;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.apache.iceberg.FileFormat.AVRO;
import static org.apache.iceberg.TableUtil.formatVersion;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestPartitionStatisticsComputation
        extends AbstractTestQueryFramework
{
    private static final long TAMPERED_RECORDS = 1000;

    private HiveMetastore metastore;
    private TrinoFileSystemFactory fileSystemFactory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder().build();
    }

    @BeforeAll
    public void setUp()
    {
        metastore = getHiveMetastore(getQueryRunner());
        fileSystemFactory = getFileSystemFactory(getQueryRunner());
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    public void testFullAppends(int formatVersion)
            throws Exception
    {
        String table = createTable(formatVersion, "id integer, part integer", "ARRAY['part']");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 2)", 2);
        assertUpdate("INSERT INTO " + table + " VALUES (3, 1)", 1);
        assertUpdate("INSERT INTO " + table + " VALUES (4, 2), (5, 2), (6, 1)", 3);

        assertMatchesLibrary(table);
        assertUpdate("DROP TABLE " + table);
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    public void testFullPositionDeletes(int formatVersion)
            throws Exception
    {
        // position delete files in format version 2, deletion vectors in format version 3
        String table = createTable(formatVersion, "id integer, part integer", "ARRAY['part']");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 1), (3, 2)", 3);
        assertUpdate("DELETE FROM " + table + " WHERE id = 1", 1);
        assertUpdate("INSERT INTO " + table + " VALUES (4, 2)", 1);
        assertUpdate("DELETE FROM " + table + " WHERE id = 4", 1);

        assertMatchesLibrary(table);
        assertUpdate("DROP TABLE " + table);
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    public void testFullOptimize(int formatVersion)
            throws Exception
    {
        String table = createTable(formatVersion, "id integer, part integer", "ARRAY['part']");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1)", 1);
        assertUpdate("INSERT INTO " + table + " VALUES (2, 1)", 1);
        assertUpdate("INSERT INTO " + table + " VALUES (3, 2)", 1);
        assertUpdate("INSERT INTO " + table + " VALUES (4, 2)", 1);
        // the removed files are DELETED manifest entries, which only update the last updated fields
        assertUpdate("ALTER TABLE " + table + " EXECUTE optimize");

        assertMatchesLibrary(table);
        assertUpdate("DROP TABLE " + table);
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    public void testFullSpecEvolution(int formatVersion)
            throws Exception
    {
        String table = createTable(formatVersion, "id integer, part integer", "ARRAY['part']");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 2)", 2);
        assertUpdate("ALTER TABLE " + table + " SET PROPERTIES partitioning = ARRAY['bucket(id, 4)']");
        assertUpdate("INSERT INTO " + table + " VALUES (3, 1), (4, 2), (5, 3)", 3);

        assertMatchesLibrary(table);
        assertUpdate("DROP TABLE " + table);
    }

    @ParameterizedTest
    @MethodSource("partitionValues")
    public void testFullPartitionValueTypes(String type, String partitioning, String otherValue, String value, String sameValue)
            throws Exception
    {
        String table = createTable(3, "id integer, col " + type, "ARRAY['" + partitioning + "']");
        assertUpdate("INSERT INTO " + table + " VALUES (1, " + otherValue + "), (2, " + value + "), (3, " + sameValue + ")", 3);
        assertUpdate("DELETE FROM " + table + " WHERE id = 2", 1);

        assertMatchesLibrary(table);
        assertUpdate("DROP TABLE " + table);
    }

    public static Stream<Object[]> partitionValues()
    {
        // the cases of the partition statistics reader tests, without the file format
        return Arrays.stream(TestIcebergPartitionStatistics.partitionValues())
                .filter(arguments -> arguments[arguments.length - 1] == AVRO)
                .map(arguments -> Arrays.copyOf(arguments, 5));
    }

    @Test
    public void testNoPartitionsNoRows()
    {
        String table = createTable(3, "id integer, part integer", "ARRAY['part']");
        // a snapshot, without data
        assertUpdate("INSERT INTO " + table + " SELECT 1, 1 WHERE false", 0);
        BaseTable icebergTable = loadTable(table, metastore, fileSystemFactory, "iceberg", "tpch");

        assertThat(PartitionStatisticsComputation.compute(icebergTable, icebergTable.currentSnapshot().snapshotId(), Optional.empty(), TESTING_TYPE_MANAGER, newDirectExecutorService())).isEmpty();
        assertUpdate("DROP TABLE " + table);
    }

    @ParameterizedTest
    @MethodSource("versionsAndFormats")
    public void testIncrementalAppends(int formatVersion, String format)
            throws Exception
    {
        String table = createTable(formatVersion, format, "id integer, part integer", "ARRAY['part']");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 2)", 2);
        PartitionStatisticsFile base = registeredBase(table);
        assertUpdate("INSERT INTO " + table + " VALUES (3, 1)", 1);
        assertUpdate("INSERT INTO " + table + " VALUES (4, 3), (5, 2)", 2);
        assertUpdate("INSERT INTO " + table + " VALUES (6, 1)", 1);

        // several manifests, read in parallel
        try (ExecutorService executor = newFixedThreadPool(4)) {
            assertIncrementalMatchesLibrary(table, base, formatVersion, executor);
        }
        assertUpdate("DROP TABLE " + table);
    }

    @ParameterizedTest
    @MethodSource("versionsAndFormats")
    public void testIncrementalDeletes(int formatVersion, String format)
            throws Exception
    {
        // position delete files in format version 2, deletion vectors in format version 3
        String table = createTable(formatVersion, format, "id integer, part integer", "ARRAY['part']");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 1), (3, 2), (4, 2)", 4);
        PartitionStatisticsFile base = registeredBase(table);
        assertUpdate("DELETE FROM " + table + " WHERE id = 1", 1);
        assertUpdate("INSERT INTO " + table + " VALUES (5, 3)", 1);
        assertUpdate("DELETE FROM " + table + " WHERE id = 3", 1);
        // a whole partition of the base is removed, so its row stays with zero counts
        assertUpdate("DELETE FROM " + table + " WHERE part = 2", 1);

        assertIncrementalMatchesLibrary(table, base, formatVersion, newDirectExecutorService());
        assertUpdate("DROP TABLE " + table);
    }

    @ParameterizedTest
    @MethodSource("versionsAndFormats")
    public void testIncrementalOptimize(int formatVersion, String format)
            throws Exception
    {
        String table = createTable(formatVersion, format, "id integer, part integer", "ARRAY['part']");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1)", 1);
        assertUpdate("INSERT INTO " + table + " VALUES (2, 1)", 1);
        PartitionStatisticsFile base = registeredBase(table);
        assertUpdate("INSERT INTO " + table + " VALUES (3, 2)", 1);
        assertUpdate("INSERT INTO " + table + " VALUES (4, 2)", 1);
        // the files removed by optimize are subtracted
        assertUpdate("ALTER TABLE " + table + " EXECUTE optimize");

        assertIncrementalMatchesLibrary(table, base, formatVersion, newDirectExecutorService());
        assertUpdate("DROP TABLE " + table);
    }

    @ParameterizedTest
    @MethodSource("versionsAndFormats")
    public void testIncrementalAfterRemovingPartitionField(int formatVersion, String format)
            throws Exception
    {
        String table = createTable(formatVersion, format, "id integer, part integer, other integer", "ARRAY['part', 'other']");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1, 10), (2, 2, 20), (3, 1, 30)", 3);
        PartitionStatisticsFile firstBase = unregisteredBase(table);
        // the new spec is not a prefix of the unified partition type
        assertUpdate("ALTER TABLE " + table + " SET PROPERTIES partitioning = ARRAY['other']");
        assertUpdate("INSERT INTO " + table + " VALUES (4, 1, 10), (5, 2, 20), (6, 3, 40)", 3);
        BaseTable icebergTable = loadTable(table, metastore, fileSystemFactory, "iceberg", "tpch");
        long secondSnapshotId = icebergTable.currentSnapshot().snapshotId();
        assertThat(computeIncrementally(icebergTable, secondSnapshotId, firstBase, newDirectExecutorService())).isNotEmpty();

        PartitionStatisticsFile secondBase = unregisteredBase(table);
        assertUpdate("INSERT INTO " + table + " VALUES (7, 1, 10), (8, 2, 30), (9, 1, 40)", 3);
        assertIncrementalMatchesLibraryInFull(table, secondBase, formatVersion);
        assertUpdate("DROP TABLE " + table);
    }

    @ParameterizedTest
    @MethodSource("versionsAndFormats")
    public void testIncrementalAfterReplacingPartitionField(int formatVersion, String format)
            throws Exception
    {
        String table = createTable(formatVersion, format, "id integer, part integer", "ARRAY['part']");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 2)", 2);
        assertUpdate("ALTER TABLE " + table + " SET PROPERTIES partitioning = ARRAY['bucket(id, 4)']");
        assertUpdate("INSERT INTO " + table + " VALUES (3, 1), (4, 2), (5, 3), (6, 4)", 4);
        PartitionStatisticsFile base = unregisteredBase(table);
        assertUpdate("INSERT INTO " + table + " VALUES (7, 1), (8, 2), (9, 3), (10, 4)", 4);
        assertUpdate("INSERT INTO " + table + " VALUES (11, 1), (12, 2)", 2);

        assertIncrementalMatchesLibraryInFull(table, base, formatVersion);
        assertUpdate("DROP TABLE " + table);
    }

    @ParameterizedTest
    @ValueSource(strings = {"PARQUET", "AVRO"})
    public void testIncrementalBaseBeforeUpgradeToVersion3(String format)
            throws Exception
    {
        String table = createTable(2, format, "id integer, part integer", "ARRAY['part']");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 1), (3, 2)", 3);
        // a base file without dv_count
        PartitionStatisticsFile base = unregisteredBase(table);
        assertUpdate("ALTER TABLE " + table + " SET PROPERTIES format_version = 3");
        assertUpdate("DELETE FROM " + table + " WHERE id = 1", 1);
        assertUpdate("INSERT INTO " + table + " VALUES (4, 2)", 1);

        assertIncrementalMatchesLibraryInFull(table, base, 2);
        assertUpdate("DROP TABLE " + table);
    }

    @Test
    public void testIncrementalBaseWithDuplicateRows()
            throws Exception
    {
        String table = createTable(2, "id integer, part integer", "ARRAY['part']");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 1)", 2);
        BaseTable icebergTable = loadTable(table, metastore, fileSystemFactory, "iceberg", "tpch");
        long baseSnapshotId = icebergTable.currentSnapshot().snapshotId();
        Types.StructType partitionType = Partitioning.partitionType(icebergTable);
        Schema schema = PartitionStatistics.schema(partitionType, 2);
        OutputFile outputFile = icebergTable.io().newOutputFile(icebergTable.location() + "/metadata/duplicate-rows-stats-" + randomNameSuffix() + ".avro");
        try (FileAppender<StructLike> writer = InternalData.write(FileFormat.AVRO, outputFile).schema(schema).build()) {
            writer.add(statisticsRow(schema, partitionType, 1, 100, 10));
            writer.add(statisticsRow(schema, partitionType, 1, 50, 5));
        }
        PartitionStatisticsFile base = ImmutableGenericPartitionStatisticsFile.builder()
                .snapshotId(baseSnapshotId)
                .path(outputFile.location())
                .fileSizeInBytes(outputFile.toInputFile().getLength())
                .build();
        assertUpdate("INSERT INTO " + table + " VALUES (3, 1)", 1);
        icebergTable = loadTable(table, metastore, fileSystemFactory, "iceberg", "tpch");

        // the last row of the base wins
        List<GenericRecord> rows = PartitionStatisticsComputation.compute(icebergTable, icebergTable.currentSnapshot().snapshotId(), Optional.of(base), TESTING_TYPE_MANAGER, newDirectExecutorService());
        assertThat(rows).hasSize(1);
        assertThat(rows.getFirst().get(PartitionStatistics.DATA_RECORD_COUNT_POSITION, Long.class)).isEqualTo(51);
        assertThat(rows.getFirst().get(PartitionStatistics.DATA_FILE_COUNT_POSITION, Integer.class)).isEqualTo(6);
        // total_record_count is not computed, so it is not carried forward
        assertThat(rows.getFirst().get(PartitionStatistics.TOTAL_RECORD_COUNT_POSITION, Long.class)).isNull();
        assertUpdate("DROP TABLE " + table);
    }

    @Test
    public void testUnreadableBaseThrows()
            throws Exception
    {
        String table = createTable(2, "id integer, part integer", "ARRAY['part']");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1)", 1);
        PartitionStatisticsFile base = unregisteredBase(table);
        assertUpdate("INSERT INTO " + table + " VALUES (2, 1)", 1);
        BaseTable icebergTable = loadTable(table, metastore, fileSystemFactory, "iceberg", "tpch");
        icebergTable.io().deleteFile(base.path());

        assertThatThrownBy(() -> PartitionStatisticsComputation.compute(icebergTable, icebergTable.currentSnapshot().snapshotId(), Optional.of(base), TESTING_TYPE_MANAGER, newDirectExecutorService()))
                .isInstanceOf(UnreadableBaseException.class);
        assertUpdate("DROP TABLE " + table);
    }

    @Test
    public void testBaseOfNonAncestorSnapshotThrows()
            throws Exception
    {
        String table = createTable(2, "id integer, part integer", "ARRAY['part']");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1)", 1);
        long firstSnapshotId = loadTable(table, metastore, fileSystemFactory, "iceberg", "tpch").currentSnapshot().snapshotId();
        assertUpdate("INSERT INTO " + table + " VALUES (2, 1)", 1);
        // the base is a descendant of the snapshot, so its manifests would be counted twice
        PartitionStatisticsFile base = unregisteredBase(table);
        BaseTable icebergTable = loadTable(table, metastore, fileSystemFactory, "iceberg", "tpch");

        assertThatThrownBy(() -> PartitionStatisticsComputation.compute(icebergTable, firstSnapshotId, Optional.of(base), TESTING_TYPE_MANAGER, newDirectExecutorService()))
                .isInstanceOf(UnreadableBaseException.class)
                .hasMessageContaining("not an ancestor");
        assertUpdate("DROP TABLE " + table);
    }

    public static Stream<Object[]> versionsAndFormats()
    {
        return Stream.of(2, 3)
                .flatMap(version -> Stream.of("PARQUET", "AVRO").map(format -> new Object[] {version, format}));
    }

    private static GenericRecord statisticsRow(Schema schema, Types.StructType partitionType, int part, long dataRecordCount, int dataFileCount)
    {
        GenericRecord partition = GenericRecord.create(partitionType);
        partition.set(0, part);
        GenericRecord row = GenericRecord.create(schema);
        row.set(PartitionStatistics.PARTITION_POSITION, partition);
        row.set(PartitionStatistics.SPEC_ID_POSITION, 0);
        row.set(PartitionStatistics.DATA_RECORD_COUNT_POSITION, dataRecordCount);
        row.set(PartitionStatistics.DATA_FILE_COUNT_POSITION, dataFileCount);
        row.set(PartitionStatistics.TOTAL_DATA_FILE_SIZE_IN_BYTES_POSITION, 1000L);
        row.set(PartitionStatistics.POSITION_DELETE_RECORD_COUNT_POSITION, 0L);
        row.set(PartitionStatistics.POSITION_DELETE_FILE_COUNT_POSITION, 0);
        row.set(PartitionStatistics.EQUALITY_DELETE_RECORD_COUNT_POSITION, 0L);
        row.set(PartitionStatistics.EQUALITY_DELETE_FILE_COUNT_POSITION, 0);
        row.set(PartitionStatistics.TOTAL_RECORD_COUNT_POSITION, 1234L);
        return row;
    }

    /**
     * The base is registered, so that the library computes the current snapshot incrementally from it.
     */
    private PartitionStatisticsFile registeredBase(String tableName)
            throws IOException
    {
        BaseTable table = loadTable(tableName, metastore, fileSystemFactory, "iceberg", "tpch");
        PartitionStatisticsFile base = PartitionStatsHandler.computeAndWriteStatsFile(table, table.currentSnapshot().snapshotId());
        table.updatePartitionStatistics().setPartitionStatistics(base).commit();
        return base;
    }

    /**
     * The library computes the snapshots after an unregistered base in full.
     */
    private PartitionStatisticsFile unregisteredBase(String tableName)
            throws IOException
    {
        BaseTable table = loadTable(tableName, metastore, fileSystemFactory, "iceberg", "tpch");
        return PartitionStatsHandler.computeAndWriteStatsFile(table, table.currentSnapshot().snapshotId());
    }

    private List<List<Object>> computeIncrementally(Table table, long snapshotId, PartitionStatisticsFile base, ExecutorService executor)
    {
        return trinoRows(PartitionStatisticsComputation.compute(table, snapshotId, Optional.of(base), TESTING_TYPE_MANAGER, executor));
    }

    /**
     * The rows of the base file have {@code TAMPERED_RECORDS} more data records than the library computed, so that a
     * computation that does not use the base cannot match the expected rows, which have the extra records in the
     * partitions of the base.
     */
    private void assertIncrementalMatchesLibrary(String tableName, PartitionStatisticsFile base, int baseFormatVersion, ExecutorService executor)
            throws IOException
    {
        BaseTable table = loadTable(tableName, metastore, fileSystemFactory, "iceberg", "tpch");
        long snapshotId = table.currentSnapshot().snapshotId();
        Set<List<Object>> baseKeys = new HashSet<>();
        PartitionStatisticsFile tamperedBase = tamperedBase(table, base, baseFormatVersion, baseKeys);
        assertThat(computeIncrementally(table, snapshotId, tamperedBase, executor))
                .isEqualTo(withTamperedRecords(libraryRows(table, snapshotId), baseKeys));
    }

    /**
     * The library's incremental computation mixes up partition specs that are not a prefix of the unified partition
     * type, so the oracle is its full computation. That has rows for partitions without data files, and other last
     * updated fields, which the incremental computation does not necessarily reproduce.
     */
    private void assertIncrementalMatchesLibraryInFull(String tableName, PartitionStatisticsFile base, int baseFormatVersion)
            throws IOException
    {
        BaseTable table = loadTable(tableName, metastore, fileSystemFactory, "iceberg", "tpch");
        long snapshotId = table.currentSnapshot().snapshotId();
        Set<List<Object>> baseKeys = new HashSet<>();
        PartitionStatisticsFile tamperedBase = tamperedBase(table, base, baseFormatVersion, baseKeys);
        List<List<Object>> actual = computeIncrementally(table, snapshotId, tamperedBase, newDirectExecutorService());
        assertThat(withoutEmptyPartitions(actual))
                .isNotEmpty()
                .isEqualTo(withoutEmptyPartitions(withTamperedRecords(libraryRows(table, snapshotId), baseKeys)));
    }

    private static List<List<Object>> withTamperedRecords(List<List<Object>> rows, Set<List<Object>> baseKeys)
    {
        return rows.stream()
                .map(row -> {
                    if (!baseKeys.contains(List.of(row.get(PartitionStatistics.PARTITION_POSITION), row.get(PartitionStatistics.SPEC_ID_POSITION)))) {
                        return row;
                    }
                    List<Object> copy = new ArrayList<>(row);
                    copy.set(PartitionStatistics.DATA_RECORD_COUNT_POSITION, (Long) row.get(PartitionStatistics.DATA_RECORD_COUNT_POSITION) + TAMPERED_RECORDS);
                    return copy;
                })
                .toList();
    }

    /**
     * Writes a copy of the base file, in the same format, with {@code TAMPERED_RECORDS} more data records in each row,
     * and adds the (partition, spec id) of its rows to {@code keys}.
     */
    private static PartitionStatisticsFile tamperedBase(Table table, PartitionStatisticsFile base, int baseFormatVersion, Set<List<Object>> keys)
            throws IOException
    {
        Types.StructType partitionType = Partitioning.partitionType(table);
        Schema schema = PartitionStatistics.schema(partitionType, baseFormatVersion);
        FileFormat format = FileFormat.fromFileName(base.path());
        OutputFile outputFile = table.io().newOutputFile(table.location() + "/metadata/" + format.addExtension("tampered-stats-" + randomNameSuffix()));
        try (CloseableIterable<StructLike> records = PartitionStatisticsReader.readRecords(table.io().newInputFile(base.path(), base.fileSizeInBytes()), schema, TESTING_TYPE_MANAGER);
                FileAppender<StructLike> writer = InternalData.write(format, outputFile).schema(schema).build()) {
            for (StructLike record : records) {
                GenericRecord row = GenericRecord.create(schema);
                for (int position = 0; position < schema.columns().size(); position++) {
                    Object value = record.get(position, Object.class);
                    if (value instanceof StructLike struct) {
                        GenericRecord partition = GenericRecord.create(partitionType);
                        for (int i = 0; i < partitionType.fields().size(); i++) {
                            partition.set(i, struct.get(i, Object.class));
                        }
                        value = partition;
                    }
                    row.set(position, value);
                }
                keys.add(List.of(flatten(row.get(PartitionStatistics.PARTITION_POSITION, StructLike.class), partitionType.fields().size()), row.get(PartitionStatistics.SPEC_ID_POSITION, Integer.class)));
                row.set(PartitionStatistics.DATA_RECORD_COUNT_POSITION, row.get(PartitionStatistics.DATA_RECORD_COUNT_POSITION, Long.class) + TAMPERED_RECORDS);
                writer.add(row);
            }
        }
        return ImmutableGenericPartitionStatisticsFile.builder()
                .snapshotId(base.snapshotId())
                .path(outputFile.location())
                .fileSizeInBytes(outputFile.toInputFile().getLength())
                .build();
    }

    private static List<List<Object>> withoutEmptyPartitions(List<List<Object>> rows)
    {
        return rows.stream()
                .filter(row -> (Long) row.get(PartitionStatistics.DATA_RECORD_COUNT_POSITION) != 0)
                .map(row -> {
                    List<Object> copy = new ArrayList<>(row);
                    copy.set(PartitionStatistics.LAST_UPDATED_AT_POSITION, null);
                    copy.set(PartitionStatistics.LAST_UPDATED_SNAPSHOT_ID_POSITION, null);
                    return copy;
                })
                .toList();
    }

    private String createTable(int formatVersion, String columns, String partitioning)
    {
        return createTable(formatVersion, "PARQUET", columns, partitioning);
    }

    private String createTable(int formatVersion, String format, String columns, String partitioning)
    {
        String table = "test_ps_computation_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (" + columns + ") WITH (format_version = " + formatVersion + ", format = '" + format + "', partitioning = " + partitioning + ")");
        return table;
    }

    private void assertMatchesLibrary(String tableName)
            throws IOException
    {
        BaseTable table = loadTable(tableName, metastore, fileSystemFactory, "iceberg", "tpch");
        long snapshotId = table.currentSnapshot().snapshotId();
        List<List<Object>> expected = libraryRows(table, snapshotId);
        assertThat(expected).isNotEmpty();
        assertThat(trinoRows(PartitionStatisticsComputation.compute(table, snapshotId, Optional.empty(), TESTING_TYPE_MANAGER, newDirectExecutorService())))
                .isEqualTo(expected);
    }

    /**
     * The rows of the Iceberg library's partition statistics file of the snapshot: computed in full, or incrementally
     * when the table has a statistics file for an ancestor. The file is registered, because the library's scan only
     * sees registered files.
     */
    static List<List<Object>> libraryRows(Table table, long snapshotId)
            throws IOException
    {
        PartitionStatisticsFile file = PartitionStatsHandler.computeAndWriteStatsFile(table, snapshotId);
        if (file == null) {
            return List.of();
        }
        table.updatePartitionStatistics().setPartitionStatistics(file).commit();
        int fields = fieldCount(table);
        List<List<Object>> rows = new ArrayList<>();
        try (CloseableIterable<PartitionStatistics> scan = table.newPartitionStatisticsScan().useSnapshot(snapshotId).scan()) {
            for (PartitionStatistics statistics : scan) {
                rows.add(flatten(statistics, fields));
            }
        }
        return rows;
    }

    static List<List<Object>> trinoRows(List<GenericRecord> rows)
    {
        return rows.stream()
                .map(row -> flatten(row, row.size()))
                .toList();
    }

    static int fieldCount(Table table)
    {
        return PartitionStatistics.schema(Partitioning.partitionType(table), formatVersion(table)).columns().size();
    }

    static List<Object> flatten(StructLike row, int fields)
    {
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < fields; i++) {
            Object value = row.get(i, Object.class);
            if (value instanceof StructLike struct) {
                values.add(flatten(struct, struct.size()));
            }
            else {
                values.add(value);
            }
        }
        return values;
    }
}
