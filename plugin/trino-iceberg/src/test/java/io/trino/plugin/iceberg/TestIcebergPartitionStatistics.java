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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.testing.TestingTicker;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.ImmutableGenericPartitionStatisticsFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatistics;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.UUID;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.plugin.iceberg.IcebergColumnHandle.partitionColumnHandle;
import static io.trino.plugin.iceberg.IcebergSessionProperties.EXPIRE_SNAPSHOTS_MIN_RETENTION;
import static io.trino.plugin.iceberg.IcebergTestUtils.SESSION;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.loadTable;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.DataProviders.cartesianProduct;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toMap;
import static org.apache.iceberg.FileFormat.AVRO;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.apache.iceberg.TableUtil.formatVersion;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergPartitionStatistics
        extends AbstractTestQueryFramework
{
    // the formats partition statistics files are read in
    private static final Object[][] PARTITION_STATISTICS_FORMATS = {{AVRO}, {PARQUET}};

    private HiveMetastore metastore;
    private TrinoFileSystemFactory fileSystemFactory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                // tests that write a partition statistics file expect it to be used however few data files there are
                .addIcebergProperty("iceberg.partition-statistics.min-data-files", "0")
                .build();
    }

    @BeforeAll
    public void setUp()
    {
        metastore = getHiveMetastore(getQueryRunner());
        fileSystemFactory = getFileSystemFactory(getQueryRunner());
    }

    @Test
    public void testUnfilteredRowCountFromSnapshotSummary()
    {
        assertThat(PartitionStatisticsReader.unfilteredRowCount(Map.of("total-records", "10", "total-position-deletes", "0"), 3)).hasValue(10);
        // the totals do not tell deletion vectors from position delete files, which are not subtracted
        assertThat(PartitionStatisticsReader.unfilteredRowCount(Map.of("total-records", "10", "total-position-deletes", "3"), 3)).isEmpty();
        assertThat(PartitionStatisticsReader.unfilteredRowCount(Map.of("total-position-deletes", "0"), 3)).isEmpty();
        assertThat(PartitionStatisticsReader.unfilteredRowCount(Map.of("total-records", "10"), 3)).isEmpty();
        assertThat(PartitionStatisticsReader.unfilteredRowCount(Map.of("total-records", "x", "total-position-deletes", "0"), 3)).isEmpty();
        // before format version 3, position delete files can outlive the data files they reference, so they are not subtracted
        assertThat(PartitionStatisticsReader.unfilteredRowCount(Map.of("total-records", "10", "total-position-deletes", "3"), 2)).hasValue(10);
        assertThat(PartitionStatisticsReader.unfilteredRowCount(Map.of("total-records", "10"), 2)).hasValue(10);
        assertThat(PartitionStatisticsReader.unfilteredRowCount(Map.of("total-position-deletes", "3"), 2)).isEmpty();
        assertThat(PartitionStatisticsReader.unfilteredRowCount(Map.of("total-records", "-1"), 2)).isEmpty();
    }

    @Test
    public void testCountDataFiles()
    {
        assertThat(PartitionStatisticsReader.countDataFiles(ImmutableList.of())).hasValue(0);
        assertThat(PartitionStatisticsReader.countDataFiles(ImmutableList.of(manifest(3, 4), manifest(6, 5)))).hasValue(18);
        assertThat(PartitionStatisticsReader.countDataFiles(ImmutableList.of(manifest(3, 4), manifest(null, 5)))).isEmpty();
        assertThat(PartitionStatisticsReader.countDataFiles(ImmutableList.of(manifest(6, null)))).isEmpty();
        assertThat(PartitionStatisticsReader.countDataFiles(ImmutableList.of(manifest(null, null)))).isEmpty();
    }

    private static ManifestFile manifest(Integer addedFiles, Integer existingFiles)
    {
        GenericManifestFile manifest = new GenericManifestFile(AvroSchemaUtil.convert(ManifestFile.schema(), "manifest_file"));
        List<Types.NestedField> fields = ManifestFile.schema().columns();
        manifest.put(fields.indexOf(ManifestFile.ADDED_FILES_COUNT), addedFiles);
        manifest.put(fields.indexOf(ManifestFile.EXISTING_FILES_COUNT), existingFiles);
        return manifest;
    }

    @ParameterizedTest
    @MethodSource("partitionStatisticsFormats")
    public void testPartitionStatisticsRowCountMatchesExpected(FileFormat format)
            throws Exception
    {
        String table = "test_ps_row_count_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id integer, part integer) WITH (format_version = 3, partitioning = ARRAY['part'])");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 1)", 2);
        assertUpdate("INSERT INTO " + table + " VALUES (3, 2), (4, 2), (5, 2)", 3);
        assertUpdate("INSERT INTO " + table + " VALUES (6, 3)", 1);

        // predicates on non-partition columns are not used here: their selectivity depends on column ranges, which the partition statistics path does not report
        List<String> filters = ImmutableList.of("", "WHERE part = 2", "WHERE part IN (1, 3)", "WHERE id IS NOT NULL AND part = 1");

        // a deletion vector in part 2: the manifest walk ignores it, the partition statistics file does not
        assertUpdate("DELETE FROM " + table + " WHERE id = 3", 1);
        assertThat(filters.stream().map(filter -> showStatsRowCount("SELECT * FROM " + table + " " + filter)).collect(toImmutableList()))
                .containsExactly(6.0, 3.0, 3.0, 2.0);

        writePartitionStatistics(table, format);

        List<Double> expected = filters.stream()
                .map(filter -> ((Number) computeScalar("SELECT count(*) FROM " + table + " " + filter)).doubleValue())
                .collect(toImmutableList());
        assertThat(expected).containsExactly(5.0, 2.0, 3.0, 2.0);
        assertThat(filters.stream().map(filter -> showStatsRowCount("SELECT * FROM " + table + " " + filter)).collect(toImmutableList()))
                .isEqualTo(expected);
        assertUpdate("DROP TABLE " + table);
    }

    @ParameterizedTest
    @MethodSource("formatVersionsAndFormats")
    public void testPartitionStatisticsPositionDeletes(int formatVersion, FileFormat format)
            throws Exception
    {
        String table = "test_ps_pos_deletes_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id integer, part integer) WITH (format_version = " + formatVersion + ", partitioning = ARRAY['part'])");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 1), (3, 2), (4, 2)", 4);
        assertUpdate("DELETE FROM " + table + " WHERE id = 1", 1);

        // the manifest walk ignores delete files, so it overestimates
        assertThat(showStatsRowCount("SELECT * FROM " + table + " WHERE part = 1")).isEqualTo(2.0);
        assertThat(showStatsRowCount("SELECT * FROM " + table + " WHERE part IN (1, 2)")).isEqualTo(4.0);

        writePartitionStatistics(table, format);

        // deletion vectors are subtracted; position delete files are not, as they can outlive the data files they reference
        long deletedRows = formatVersion >= 3 ? 1 : 0;
        assertThat(filteredRowCount(table, Expressions.equal("part", 1), Domain.all(VARCHAR))).hasValue(2 - deletedRows);
        assertThat(showStatsRowCount("SELECT * FROM " + table + " WHERE part = 1")).isEqualTo(2.0 - deletedRows);
        assertThat(showStatsRowCount("SELECT * FROM " + table + " WHERE part IN (1, 2)")).isEqualTo(4.0 - deletedRows);
        assertThat(showStatsRowCount("SELECT * FROM " + table)).isEqualTo(4.0 - deletedRows);
        assertUpdate("DROP TABLE " + table);
    }

    @ParameterizedTest
    @MethodSource("partitionStatisticsFormats")
    public void testPartitionStatisticsAfterUpgradeToFormatVersion3(FileFormat format)
            throws Exception
    {
        String table = "test_ps_upgraded_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id integer, part integer) WITH (format_version = 2, partitioning = ARRAY['part'])");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 1), (3, 2), (4, 2)", 4);
        // a position delete file in part 1, which the upgrade keeps
        assertUpdate("DELETE FROM " + table + " WHERE id = 1", 1);
        assertUpdate("ALTER TABLE " + table + " SET PROPERTIES format_version = 3");
        // a deletion vector in part 2
        assertUpdate("DELETE FROM " + table + " WHERE id = 3", 1);
        writePartitionStatistics(table, format);

        // only the partition whose position deletes are all deletion vectors is reduced
        assertThat(filteredRowCount(table, Expressions.equal("part", 1), Domain.all(VARCHAR))).hasValue(2);
        assertThat(filteredRowCount(table, Expressions.equal("part", 2), Domain.all(VARCHAR))).hasValue(1);
        assertThat(showStatsRowCount("SELECT * FROM " + table + " WHERE part = 1")).isEqualTo(2.0);
        assertThat(showStatsRowCount("SELECT * FROM " + table + " WHERE part = 2")).isEqualTo(1.0);
        // from the file: the snapshot summary does not tell the two kinds of position deletes apart, and the manifest walk reports 4
        assertThat(showStatsRowCount("SELECT * FROM " + table)).isEqualTo(3.0);
        assertUpdate("DROP TABLE " + table);
    }

    @ParameterizedTest
    @MethodSource("partitionStatisticsFormats")
    public void testPartitionStatisticsPartitionTypeChangeReloads(FileFormat format)
            throws Exception
    {
        String table = "test_ps_type_change_" + randomNameSuffix();
        // format version 3, so that the deleted row is subtracted and a fallback to the manifest walk, which ignores deletes, cannot pass
        assertUpdate("CREATE TABLE " + table + " (id integer, part integer) WITH (format_version = 3, partitioning = ARRAY['part'])");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 1), (3, 2)", 3);
        assertUpdate("DELETE FROM " + table + " WHERE id = 1", 1);
        writePartitionStatistics(table, format);

        // the reader caches the rows, read with the integer partition type
        PartitionStatisticsReader reader = new PartitionStatisticsReader();
        assertThat(filteredRowCount(reader, table, Expressions.equal("part", 1), Domain.all(VARCHAR))).hasValue(1);

        long snapshotId = loadIcebergTable(table).currentSnapshot().snapshotId();
        assertUpdate("ALTER TABLE " + table + " ALTER COLUMN part SET DATA TYPE bigint");
        // no new snapshot, so the file is still the one for the current snapshot
        assertThat(loadIcebergTable(table).currentSnapshot().snapshotId()).isEqualTo(snapshotId);

        // the same file with another partition type is read again, instead of using the rows cached for the old type
        assertThat(filteredRowCount(reader, table, Expressions.equal("part", 1), Domain.all(VARCHAR))).hasValue(1);
        assertThat(showStatsRowCount("SELECT * FROM " + table + " WHERE part = 1")).isEqualTo(1.0);
        assertUpdate("DROP TABLE " + table);
    }

    @ParameterizedTest
    @MethodSource("partitionStatisticsFormats")
    public void testPartitionStatisticsCacheIsBounded(FileFormat format)
            throws Exception
    {
        String table = "test_ps_cache_bound_" + randomNameSuffix();
        createPartitionedTable(table);
        writePartitionStatistics(table, format);

        assertThat(filteredRowCount(new PartitionStatisticsReader(), table, Expressions.equal("part", 1), Domain.all(VARCHAR))).hasValue(2);
        // a file whose rows do not fit in the cache is not used
        assertThat(filteredRowCount(new PartitionStatisticsReader(DataSize.ofBytes(1)), table, Expressions.equal("part", 1), Domain.all(VARCHAR))).isEmpty();
        assertUpdate("DROP TABLE " + table);
    }

    @ParameterizedTest
    @MethodSource("partitionStatisticsFormats")
    public void testPartitionStatisticsSkipsPartitionsWithoutRows(FileFormat format)
            throws Exception
    {
        String table = "test_ps_partition_without_rows_" + randomNameSuffix();
        createPartitionedTable(table);
        writePartitionStatistics(table, format);
        // removes the data files of part 2, as another engine would; the library continues from the file above, and keeps
        // part 2, without rows
        loadIcebergTable(table).newDelete().deleteFromRowFilter(Expressions.equal("part", 2)).commit();
        writePartitionStatistics(table, format);
        assertThat(partitionRowCounts(table, loadIcebergTable(table).currentSnapshot().snapshotId())).isEqualTo(ImmutableMap.of(1, 2L, 2, 0L));

        // room for one row of an integer partition, estimated at 148 bytes, so that the file is used only without part 2
        assertThat(filteredRowCount(new PartitionStatisticsReader(DataSize.ofBytes(200)), table, Expressions.alwaysTrue(), Domain.all(VARCHAR))).hasValue(2);
        assertUpdate("DROP TABLE " + table);
    }

    @ParameterizedTest
    @MethodSource("partitionStatisticsFormats")
    public void testPartitionStatisticsWithSpecEvolution(FileFormat format)
            throws Exception
    {
        String table = "test_ps_spec_evolution_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id integer, part integer) WITH (format_version = 3, partitioning = ARRAY['part'])");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1), (5, 1), (2, 2)", 3);
        assertUpdate("ALTER TABLE " + table + " SET PROPERTIES partitioning = ARRAY['part', 'bucket(id, 2)']");
        assertUpdate("INSERT INTO " + table + " VALUES (3, 1), (4, 2)", 2);
        assertUpdate("DELETE FROM " + table + " WHERE id = 5", 1);

        // the manifest walk ignores the deletion vector
        assertThat(showStatsRowCount("SELECT * FROM " + table + " WHERE part = 1")).isEqualTo(3.0);

        writePartitionStatistics(table, format);

        assertThat(showStatsRowCount("SELECT * FROM " + table + " WHERE part = 1")).isEqualTo(2.0);
        assertThat(filteredRowCount(table, Expressions.equal("part", 1), Domain.all(VARCHAR))).hasValue(2);
        assertUpdate("DROP TABLE " + table);
    }

    @ParameterizedTest
    @MethodSource("partitionStatisticsFormats")
    public void testPartitionStatisticsFileOlderThanCurrentSpec(FileFormat format)
            throws Exception
    {
        String table = "test_ps_older_than_spec_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id integer, part integer) WITH (format_version = 3, partitioning = ARRAY['part'])");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 1), (3, 2)", 3);
        assertUpdate("DELETE FROM " + table + " WHERE id = 1", 1);
        writePartitionStatistics(table, format);

        // no new snapshot: the file is for the current snapshot, but its partition struct lacks the new field
        assertUpdate("ALTER TABLE " + table + " SET PROPERTIES partitioning = ARRAY['part', 'bucket(id, 2)']");

        // the manifest walk would report 2
        assertThat(showStatsRowCount("SELECT * FROM " + table + " WHERE part = 1")).isEqualTo(1.0);
        assertThat(filteredRowCount(table, Expressions.equal("part", 1), Domain.all(VARCHAR))).hasValue(1);
        assertUpdate("DROP TABLE " + table);
    }

    @ParameterizedTest
    @MethodSource("partitionFieldReplacements")
    public void testPartitionStatisticsAfterReplacingPartitionField(String partitioning, String column, int formatVersion, FileFormat format)
            throws Exception
    {
        String table = "test_ps_replaced_field_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id integer, part integer, other integer) WITH (format_version = " + formatVersion + ", partitioning = ARRAY['part'])");
        // the ranges of id and other in these files include 5, so the manifest walk cannot skip them by file statistics
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1, 5), (9, 1, 6), (2, 2, 5), (8, 2, 6)", 4);
        // the new spec drops part, so its field is not at the same position in the table partition type as in the spec
        assertUpdate("ALTER TABLE " + table + " SET PROPERTIES partitioning = ARRAY['" + partitioning + "']");
        assertUpdate("INSERT INTO " + table + " VALUES (4, 3, 5), (5, 3, 5), (6, 3, 6), (7, 3, 6)", 4);
        String partition = (String) computeScalar("SELECT \"$partition\" FROM " + table + " WHERE id = 5");
        Types.NestedField field = loadIcebergTable(table).schema().findField(column);
        IcebergColumnHandle columnHandle = IcebergColumnHandle.optional(ColumnIdentity.primitiveColumnIdentity(field.fieldId(), column)).columnType(INTEGER).build();
        TupleDomain<IcebergColumnHandle> columnConstraint = TupleDomain.withColumnDomains(ImmutableMap.of(columnHandle, Domain.singleValue(INTEGER, 5L)));
        TupleDomain<IcebergColumnHandle> partitionConstraint = TupleDomain.withColumnDomains(ImmutableMap.of(partitionColumnHandle(), Domain.singleValue(VARCHAR, utf8Slice(partition))));
        double walkForColumn = estimatedRowCount(table, columnConstraint);
        double walkForPartition = estimatedRowCount(table, partitionConstraint);

        assertUpdate("DELETE FROM " + table + " WHERE id = 5", 1);
        writePartitionStatistics(table, format);

        // only deletion vectors are subtracted
        long deletedRows = formatVersion >= 3 ? 1 : 0;
        // the reader answers from the file for both versions
        assertThat(filteredRowCount(table, Expressions.equal(column, 5), Domain.all(VARCHAR))).hasValue((long) walkForColumn - deletedRows);
        assertThat(filteredRowCount(table, Expressions.alwaysTrue(), Domain.singleValue(VARCHAR, utf8Slice(partition)))).hasValue((long) walkForPartition - deletedRows);
        if (formatVersion >= 3) {
            // and so does the connector, which the deleted row makes visible
            assertThat(estimatedRowCount(table, columnConstraint)).isEqualTo(walkForColumn - 1);
            assertThat(estimatedRowCount(table, partitionConstraint)).isEqualTo(walkForPartition - 1);
            assertThat(showStatsRowCount("SELECT * FROM " + table + " WHERE \"$partition\" = '" + partition + "'")).isEqualTo(walkForPartition - 1);
        }
        assertUpdate("DROP TABLE " + table);
    }

    public static Object[][] partitionStatisticsFormats()
    {
        return PARTITION_STATISTICS_FORMATS;
    }

    public static Object[][] formatVersionsAndFormats()
    {
        return cartesianProduct(new Object[][] {{2}, {3}}, PARTITION_STATISTICS_FORMATS);
    }

    public static Object[][] partitionFieldReplacements()
    {
        return cartesianProduct(
                new Object[][] {{"other", "other"}, {"bucket(id, 2)", "id"}},
                new Object[][] {{2}, {3}},
                PARTITION_STATISTICS_FORMATS);
    }

    @ParameterizedTest
    @MethodSource("partitionValues")
    public void testPartitionStatisticsMatchPartitionValues(String type, String partitioning, String otherValue, String value, String sameValue, Object icebergValue, FileFormat format)
            throws Exception
    {
        String table = "test_ps_partition_values_" + randomNameSuffix();
        // format version 3, so that the deleted row is subtracted and a fallback to the manifest walk, which ignores deletes, cannot pass
        assertUpdate("CREATE TABLE " + table + " (id integer, col " + type + ") WITH (format_version = 3, partitioning = ARRAY['" + partitioning + "'])");
        // value and sameValue are in the same partition
        assertUpdate("INSERT INTO " + table + " VALUES (1, " + otherValue + "), (2, " + value + "), (3, " + sameValue + ")", 3);
        assertUpdate("DELETE FROM " + table + " WHERE id = 2", 1);
        String partition = (String) computeScalar("SELECT \"$partition\" FROM " + table + " WHERE id = 3");
        long expected = (long) computeScalar("SELECT count(*) FROM " + table + " WHERE \"$partition\" = '" + partition + "'");

        writePartitionStatistics(table, format);

        Expression predicate = icebergValue == null ? Expressions.isNull("col") : Expressions.equal("col", icebergValue);
        assertThat(filteredRowCount(table, predicate, Domain.all(VARCHAR))).hasValue(expected);
        assertThat(filteredRowCount(table, Expressions.alwaysTrue(), Domain.singleValue(VARCHAR, utf8Slice(partition)))).hasValue(expected);
        assertUpdate("DROP TABLE " + table);
    }

    public static Object[][] partitionValues()
    {
        String timestampWithTimeZone = "timestamp(6) with time zone";
        return cartesianProduct(new Object[][] {
                {"integer", "col", "1", "2", "2", 2},
                {"boolean", "col", "false", "true", "true", true},
                {"date", "col", "DATE '2024-01-14'", "DATE '2024-01-15'", "DATE '2024-01-15'", "2024-01-15"},
                {"timestamp(6)", "col", "TIMESTAMP '2024-01-14 10:30:00.123456'", "TIMESTAMP '2024-01-15 10:30:00.123456'", "TIMESTAMP '2024-01-15 10:30:00.123456'", "2024-01-15T10:30:00.123456"},
                {"timestamp(9)", "col", "TIMESTAMP '2024-01-14 10:30:00.123456789'", "TIMESTAMP '2024-01-15 10:30:00.123456789'", "TIMESTAMP '2024-01-15 10:30:00.123456789'", "2024-01-15T10:30:00.123456789"},
                {"varchar", "col", "'a'", "'b'", "'b'", "b"},
                {"uuid", "col", "UUID '11111111-1111-1111-1111-111111111111'", "UUID '22222222-2222-2222-2222-222222222222'", "UUID '22222222-2222-2222-2222-222222222222'", UUID.fromString("22222222-2222-2222-2222-222222222222")},
                {"decimal(10, 2)", "col", "1.50", "12.34", "12.34", new BigDecimal("12.34")},
                {"varbinary", "col", "X'01'", "X'0203'", "X'0203'", ByteBuffer.wrap(new byte[] {2, 3})},
                {timestampWithTimeZone, "col", "TIMESTAMP '2024-01-14 10:30:00.123456 UTC'", "TIMESTAMP '2024-01-15 10:30:00.123456 UTC'", "TIMESTAMP '2024-01-15 10:30:00.123456 UTC'", "2024-01-15T10:30:00.123456+00:00"},
                {timestampWithTimeZone, "day(col)", "TIMESTAMP '2024-01-14 23:00:00 UTC'", "TIMESTAMP '2024-01-15 01:00:00 UTC'", "TIMESTAMP '2024-01-15 23:00:00 UTC'", "2024-01-15T23:00:00+00:00"},
                {timestampWithTimeZone, "month(col)", "TIMESTAMP '2024-02-01 00:00:00 UTC'", "TIMESTAMP '2024-01-01 00:00:00 UTC'", "TIMESTAMP '2024-01-31 12:00:00 UTC'", "2024-01-31T12:00:00+00:00"},
                {timestampWithTimeZone, "hour(col)", "TIMESTAMP '2024-01-15 09:59:59 UTC'", "TIMESTAMP '2024-01-15 10:00:00 UTC'", "TIMESTAMP '2024-01-15 10:59:59 UTC'", "2024-01-15T10:59:59+00:00"},
                {"varchar", "truncate(col, 2)", "'zz'", "'abc'", "'abd'", "abd"},
                {"bigint", "bucket(col, 4)", "1", "2", "2", 2L},
                {"bigint", "col", "1", "2", "2", 2L},
                {"decimal(5, 2)", "col", "1.50", "12.34", "12.34", new BigDecimal("12.34")},
                {"decimal(38, 2)", "col", "1.50", "12.34", "12.34", new BigDecimal("12.34")},
                {"real", "col", "REAL '1.5'", "REAL '2.5'", "REAL '2.5'", 2.5f},
                {"double", "col", "DOUBLE '1.5'", "DOUBLE '2.5'", "DOUBLE '2.5'", 2.5},
                {"date", "year(col)", "DATE '2023-06-01'", "DATE '2024-03-01'", "DATE '2024-12-31'", "2024-12-31"},
                // a null partition value, matched by IS NULL
                {"integer", "col", "1", "NULL", "NULL", null},
        }, PARTITION_STATISTICS_FORMATS);
    }

    @ParameterizedTest
    @MethodSource("partitionStatisticsFormats")
    public void testPartitionStatisticsIgnoredWithPathPredicate(FileFormat format)
            throws Exception
    {
        String table = "test_ps_path_" + randomNameSuffix();
        createPartitionedTable(table);
        String query = "SELECT * FROM " + table + " WHERE \"$path\" IS NOT NULL";
        assertUpdate("DELETE FROM " + table + " WHERE id = 1", 1);

        // the manifest walk ignores the delete (4 rows); the partition statistics file would report 3
        assertThat(showStatsRowCount(query)).isEqualTo(4.0);

        writePartitionStatistics(table, format);

        assertThat(showStatsRowCount(query)).isEqualTo(4.0);
        assertUpdate("DROP TABLE " + table);
    }

    @ParameterizedTest
    @MethodSource("partitionStatisticsFormats")
    public void testPartitionStatisticsIgnoredForOlderSnapshot(FileFormat format)
            throws Exception
    {
        String table = "test_ps_older_snapshot_" + randomNameSuffix();
        createPartitionedTable(table);
        assertUpdate("DELETE FROM " + table + " WHERE id = 1", 1);
        writePartitionStatistics(table, format);
        // from the file: the manifest walk would report 2
        assertThat(showStatsRowCount("SELECT * FROM " + table + " WHERE part = 1")).isEqualTo(1.0);

        assertUpdate("INSERT INTO " + table + " VALUES (10, 1), (11, 1)", 2);

        // the file is for the previous snapshot, so the manifest walk answers, and ignores the deletion vector
        assertThat(showStatsRowCount("SELECT * FROM " + table + " WHERE part = 1")).isEqualTo(4.0);
        assertUpdate("DROP TABLE " + table);
    }

    @ParameterizedTest
    @MethodSource("partitionStatisticsFormats")
    public void testPartitionStatisticsColumnShape(FileFormat format)
            throws Exception
    {
        String table = "test_ps_column_shape_" + randomNameSuffix();
        // a single manifest, so the sampled manifest is representative of the whole table
        assertUpdate("CREATE TABLE " + table + " (id integer, part integer, name varchar) WITH (format_version = 3, partitioning = ARRAY['part'])");
        assertUpdate("INSERT INTO " + table + " VALUES (20, 1, 'a'), (21, 1, NULL), (22, 1, 'ccc'), (23, 1, NULL)", 4);
        assertUpdate("DELETE FROM " + table + " WHERE id = 20", 1);

        writePartitionStatistics(table, format);

        // called directly: SHOW STATS normalizes the statistics against the row count
        BaseTable loaded = loadIcebergTable(table);
        IcebergColumnHandle id = IcebergColumnHandle.optional(ColumnIdentity.primitiveColumnIdentity(1, "id")).columnType(INTEGER).build();
        IcebergColumnHandle part = IcebergColumnHandle.optional(ColumnIdentity.primitiveColumnIdentity(2, "part")).columnType(INTEGER).build();
        IcebergColumnHandle name = IcebergColumnHandle.optional(ColumnIdentity.primitiveColumnIdentity(3, "name")).columnType(VARCHAR).build();
        TableStatistics statistics = TableStatisticsReader.makeTableStatistics(
                TESTING_TYPE_MANAGER,
                loaded,
                OptionalLong.of(loaded.currentSnapshot().snapshotId()),
                TupleDomain.all(),
                TupleDomain.withColumnDomains(ImmutableMap.of(part, Domain.singleValue(INTEGER, 1L))),
                ImmutableSet.of(id, name),
                newDirectExecutorService(),
                new PartitionStatisticsReader(),
                true,
                0);

        // from the file: the manifest walk would report 4
        assertThat(statistics.getRowCount()).isEqualTo(Estimate.of(3));
        // from the sampled manifest
        assertThat(statistics.getColumnStatistics().get(name).getNullsFraction()).isEqualTo(Estimate.of(0.5));
        assertThat(statistics.getColumnStatistics().get(name).getDataSize().isUnknown()).isFalse();
        // no minimum and maximum values
        assertThat(statistics.getColumnStatistics().get(id).getRange()).isEmpty();
        assertThat(statistics.getColumnStatistics().get(name).getRange()).isEmpty();
        assertUpdate("DROP TABLE " + table);
    }

    @Test
    public void testPartitionStatisticsSampleIsBounded()
    {
        String table = "test_ps_sample_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id integer, part integer) WITH (partitioning = ARRAY['part'])");
        // one manifest with a data file per partition
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)", 5);
        BaseTable loaded = loadIcebergTable(table);
        Types.NestedField id = loaded.schema().findField("id");
        assertThat(sampleRecordCount(loaded, id, 2)).isEqualTo(2);
        assertThat(sampleRecordCount(loaded, id, PartitionStatisticsReader.SAMPLE_DATA_FILE_LIMIT)).isEqualTo(5);

        // a second, smaller manifest is sampled instead
        assertUpdate("INSERT INTO " + table + " VALUES (6, 1)", 1);
        assertThat(sampleRecordCount(loadIcebergTable(table), id, PartitionStatisticsReader.SAMPLE_DATA_FILE_LIMIT)).isEqualTo(1);
        assertUpdate("DROP TABLE " + table);
    }

    private static long sampleRecordCount(BaseTable table, Types.NestedField column, int dataFileLimit)
    {
        return PartitionStatisticsReader.sampleManifest(
                        table,
                        table.currentSnapshot().dataManifests(table.io()),
                        Expressions.alwaysTrue(),
                        ImmutableSet.of(column.fieldId()),
                        Domain.all(VARCHAR),
                        Domain.all(VARCHAR),
                        ImmutableList.of(column),
                        ImmutableList.of(INTEGER),
                        TESTING_TYPE_MANAGER,
                        dataFileLimit)
                .orElseThrow()
                .recordCount();
    }

    @ParameterizedTest
    @MethodSource("partitionStatisticsFormats")
    public void testPartitionStatisticsUnknownSpecFallsBack(FileFormat format)
            throws Exception
    {
        String table = "test_ps_unknown_spec_" + randomNameSuffix();
        createPartitionedTable(table);
        assertUpdate("ALTER TABLE " + table + " SET PROPERTIES partitioning = ARRAY['part', 'bucket(id, 2)']");
        assertUpdate("INSERT INTO " + table + " VALUES (30, 1)", 1);
        writePartitionStatistics(table, format);

        BaseTable loaded = loadIcebergTable(table);
        long snapshotId = loaded.currentSnapshot().snapshotId();
        PartitionStatisticsReader reader = new PartitionStatisticsReader();
        assertThat(reader.filteredRowCount(loaded, snapshotId, Expressions.alwaysTrue(), Domain.all(VARCHAR), 3)).isPresent();

        int firstSpecId = loaded.specs().keySet().stream().min(Integer::compare).orElseThrow();
        BaseTable withoutFirstSpec = new BaseTable(loaded.operations(), loaded.name())
        {
            @Override
            public Map<Integer, PartitionSpec> specs()
            {
                return super.specs().entrySet().stream()
                        .filter(entry -> entry.getKey() != firstSpecId)
                        .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
            }
        };
        assertThat(reader.filteredRowCount(withoutFirstSpec, snapshotId, Expressions.alwaysTrue(), Domain.all(VARCHAR), 3)).isEmpty();
        // the unknown spec was detected, not an exception caught
        assertThat(reader.failureCount()).isEqualTo(0);
        assertUpdate("DROP TABLE " + table);
    }

    @ParameterizedTest
    @MethodSource("partitionStatisticsFormats")
    public void testPartitionStatisticsTimeTravelUsesSnapshotFile(FileFormat format)
            throws Exception
    {
        String table = "test_ps_time_travel_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id integer, part integer) WITH (format_version = 3, partitioning = ARRAY['part'])");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 1), (3, 2)", 3);
        assertUpdate("DELETE FROM " + table + " WHERE id = 1", 1);
        long olderSnapshotId = loadIcebergTable(table).currentSnapshot().snapshotId();
        writePartitionStatistics(table, format);
        assertUpdate("INSERT INTO " + table + " VALUES (4, 1), (5, 1)", 2);
        writePartitionStatistics(table, format);
        assertThat(loadIcebergTable(table).partitionStatisticsFiles()).hasSize(2);

        // the older file, the manifest walk would report 2
        assertThat(showStatsRowCount("SELECT * FROM " + table + " FOR VERSION AS OF " + olderSnapshotId + " WHERE part = 1")).isEqualTo(1.0);
        assertThat(showStatsRowCount("SELECT * FROM " + table + " WHERE part = 1")).isEqualTo(3.0);
        assertUpdate("DROP TABLE " + table);
    }

    @ParameterizedTest
    @ValueSource(strings = {"stats", "orc"})
    public void testPartitionStatisticsUnknownFormatFallsBack(String extension)
            throws Exception
    {
        // ORC is unknown too, the Iceberg library has no ORC implementation for partition statistics files
        String table = "test_ps_unknown_format_" + randomNameSuffix();
        createPartitionedTable(table);
        assertUpdate("DELETE FROM " + table + " WHERE id = 1", 1);
        registerPartitionStatisticsWithExtension(table, extension);

        assertThat(filteredRowCount(table, Expressions.equal("part", 1), Domain.all(VARCHAR))).isEmpty();
        // the manifest walk, which ignores the deletion vector
        assertThat(showStatsRowCount("SELECT * FROM " + table + " WHERE part = 1")).isEqualTo(2.0);
        assertUpdate("DROP TABLE " + table);
    }

    // registers the partition statistics file of the current snapshot, written in Avro, under a name with another extension
    private String registerPartitionStatisticsWithExtension(String table, String extension)
            throws IOException
    {
        PartitionStatisticsFile avroFile = IcebergTestUtils.writePartitionStatistics(loadIcebergTable(table), AVRO);
        Location location = Location.of(avroFile.path().replace(".avro", "." + extension));
        TrinoFileSystem fileSystem = fileSystemFactory.create(SESSION);
        try (InputStream input = fileSystem.newInputFile(Location.of(avroFile.path())).newStream()) {
            fileSystem.newOutputFile(location).createOrOverwrite(input.readAllBytes());
        }
        loadIcebergTable(table).updatePartitionStatistics()
                .setPartitionStatistics(ImmutableGenericPartitionStatisticsFile.builder()
                        .snapshotId(avroFile.snapshotId())
                        .path(location.toString())
                        .fileSizeInBytes(avroFile.fileSizeInBytes())
                        .build())
                .commit();
        return location.toString();
    }

    @ParameterizedTest
    @MethodSource("partitionStatisticsFormats")
    public void testPartitionStatisticsUnreadableFileIsReadAgainLater(FileFormat format)
            throws Exception
    {
        String table = "test_ps_unreadable_" + randomNameSuffix();
        createPartitionedTable(table);
        assertUpdate("DELETE FROM " + table + " WHERE id = 1", 1);
        Location location = Location.of(IcebergTestUtils.writePartitionStatistics(loadIcebergTable(table), format).path());
        TrinoFileSystem fileSystem = fileSystemFactory.create(SESSION);
        byte[] content;
        try (InputStream input = fileSystem.newInputFile(location).newStream()) {
            content = input.readAllBytes();
        }

        TestingTicker ticker = new TestingTicker();
        PartitionStatisticsReader reader = new PartitionStatisticsReader(DataSize.of(64, MEGABYTE), ticker);
        fileSystem.newOutputFile(location).createOrOverwrite("not a partition statistics file".getBytes(UTF_8));
        assertThat(filteredRowCount(reader, table, Expressions.equal("part", 1), Domain.all(VARCHAR))).isEmpty();

        // a transient failure: the file is readable again, but the failure is remembered for a while
        fileSystem.newOutputFile(location).createOrOverwrite(content);
        assertThat(filteredRowCount(reader, table, Expressions.equal("part", 1), Domain.all(VARCHAR))).isEmpty();
        assertThat(reader.failureCount()).isEqualTo(1);

        ticker.increment(PartitionStatisticsReader.UNUSABLE_FILE_EXPIRATION.toNanos(), NANOSECONDS);
        assertThat(filteredRowCount(reader, table, Expressions.equal("part", 1), Domain.all(VARCHAR))).hasValue(1);
        assertUpdate("DROP TABLE " + table);
    }

    @Test
    public void testNegativePartitionStatisticsMinDataFiles()
    {
        assertQueryFails(
                "SET SESSION " + getSession().getCatalog().orElseThrow() + ".partition_statistics_min_data_files = -1",
                ".*partition_statistics_min_data_files must be at least 0: -1");
    }

    @ParameterizedTest
    @MethodSource("partitionStatisticsFormats")
    public void testPartitionStatisticsEmptySampleLeavesShapeUnknown(FileFormat format)
            throws Exception
    {
        String table = "test_ps_empty_sample_" + randomNameSuffix();
        createPartitionedTable(table);
        assertUpdate("ALTER TABLE " + table + " ADD COLUMN name varchar");
        assertUpdate("INSERT INTO " + table + " VALUES (20, 1, 'a'), (21, 1, NULL)", 2);
        assertUpdate("DELETE FROM " + table + " WHERE id = 20", 1);
        writePartitionStatistics(table, format);

        // id = -1 is not excluded by partition pruning, but no data file in the sampled manifest matches. The manifest walk
        // prunes every data file by its bounds and reports 0 rows, the file reports the row count of the partitions.
        // Call the reader directly: SHOW STATS applies a further filter estimate on top of the connector's row count.
        BaseTable loaded = loadIcebergTable(table);
        IcebergColumnHandle id = IcebergColumnHandle.optional(ColumnIdentity.primitiveColumnIdentity(1, "id")).columnType(INTEGER).build();
        IcebergColumnHandle name = IcebergColumnHandle.optional(ColumnIdentity.primitiveColumnIdentity(3, "name")).columnType(VARCHAR).build();
        TableStatistics statistics = TableStatisticsReader.makeTableStatistics(
                TESTING_TYPE_MANAGER,
                loaded,
                OptionalLong.of(loaded.currentSnapshot().snapshotId()),
                TupleDomain.all(),
                TupleDomain.withColumnDomains(ImmutableMap.of(id, Domain.singleValue(INTEGER, -1L))),
                ImmutableSet.of(name),
                newDirectExecutorService(),
                new PartitionStatisticsReader(),
                true,
                0);

        // the partition-wide count from the file, not 0 and not the manifest walk's 6
        assertThat(statistics.getRowCount()).isEqualTo(Estimate.of(5));
        assertThat(statistics.getColumnStatistics().get(name).getNullsFraction().isUnknown()).isTrue();
        assertThat(statistics.getColumnStatistics().get(name).getDataSize().isUnknown()).isTrue();
        assertUpdate("DROP TABLE " + table);
    }

    private void createPartitionedTable(String table)
    {
        // format version 3, so that partition statistics subtract deleted rows
        assertUpdate("CREATE TABLE " + table + " (id integer, part integer) WITH (format_version = 3, partitioning = ARRAY['part'])");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 1)", 2);
        assertUpdate("INSERT INTO " + table + " VALUES (3, 2), (4, 2)", 2);
    }

    private double showStatsRowCount(String query)
    {
        return (double) showStatsRow(query, null).getField(4);
    }

    private MaterializedRow showStatsRow(String query, String columnName)
    {
        return computeActual("SHOW STATS FOR (" + query + ")").getMaterializedRows().stream()
                .filter(row -> Objects.equals(row.getField(0), columnName))
                .findFirst()
                .orElseThrow();
    }

    private OptionalLong filteredRowCount(String tableName, Expression filter, Domain partitionDomain)
    {
        return filteredRowCount(new PartitionStatisticsReader(), tableName, filter, partitionDomain);
    }

    private OptionalLong filteredRowCount(PartitionStatisticsReader reader, String tableName, Expression filter, Domain partitionDomain)
    {
        BaseTable table = loadIcebergTable(tableName);
        return reader.filteredRowCount(table, table.currentSnapshot().snapshotId(), filter, partitionDomain, formatVersion(table));
    }

    // data record count of every partition (by its first column) in the partition statistics file of a snapshot
    private Map<Integer, Long> partitionRowCounts(String tableName, long snapshotId)
    {
        try (CloseableIterable<PartitionStatistics> statistics = loadIcebergTable(tableName).newPartitionStatisticsScan().useSnapshot(snapshotId).scan()) {
            ImmutableMap.Builder<Integer, Long> counts = ImmutableMap.builder();
            statistics.forEach(row -> counts.put(row.partition().get(0, Integer.class), row.dataRecordCount()));
            return counts.buildOrThrow();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private double estimatedRowCount(String tableName, TupleDomain<IcebergColumnHandle> constraint)
    {
        BaseTable table = loadIcebergTable(tableName);
        return TableStatisticsReader.makeTableStatistics(
                        TESTING_TYPE_MANAGER,
                        table,
                        OptionalLong.of(table.currentSnapshot().snapshotId()),
                        TupleDomain.all(),
                        constraint,
                        ImmutableSet.of(),
                        newDirectExecutorService(),
                        new PartitionStatisticsReader(),
                        true,
                        0)
                .getRowCount()
                .getValue();
    }

    @Test
    public void testComputePartitionStats()
    {
        String table = "test_compute_partition_stats_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id integer, part integer) WITH (format_version = 2, partitioning = ARRAY['part'])");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 1), (3, 2)", 3);
        assertThat(loadIcebergTable(table).partitionStatisticsFiles()).isEmpty();

        assertThat(query("ALTER TABLE " + table + " EXECUTE compute_partition_stats")).returnsEmptyResult();
        BaseTable loaded = loadIcebergTable(table);
        long firstSnapshotId = loaded.currentSnapshot().snapshotId();
        assertThat(loaded.partitionStatisticsFiles()).singleElement()
                .satisfies(file -> assertThat(file.snapshotId()).isEqualTo(firstSnapshotId));
        assertThat(partitionRowCounts(table, firstSnapshotId)).containsExactlyInAnyOrderEntriesOf(Map.of(1, 2L, 2, 1L));

        // a snapshot that has a file is left alone: nothing is committed
        String metadataLocation = loaded.operations().current().metadataFileLocation();
        assertUpdate("ALTER TABLE " + table + " EXECUTE compute_partition_stats");
        assertThat(loadIcebergTable(table).operations().current().metadataFileLocation()).isEqualTo(metadataLocation);

        assertUpdate("INSERT INTO " + table + " VALUES (4, 2), (5, 3)", 2);
        assertUpdate("ALTER TABLE " + table + " EXECUTE compute_partition_stats");
        long secondSnapshotId = loadIcebergTable(table).currentSnapshot().snapshotId();
        assertThat(loadIcebergTable(table).partitionStatisticsFiles()).extracting(PartitionStatisticsFile::snapshotId)
                .containsExactlyInAnyOrder(firstSnapshotId, secondSnapshotId);
        assertThat(partitionRowCounts(table, secondSnapshotId)).containsExactlyInAnyOrderEntriesOf(Map.of(1, 2L, 2, 2L, 3, 1L));
        assertThat(showStatsRowCount("SELECT * FROM " + table + " WHERE part = 2")).isEqualTo(2.0);
        assertUpdate("DROP TABLE " + table);
    }

    @Test
    public void testDropPartitionStats()
    {
        String table = "test_drop_partition_stats_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id integer, part integer) WITH (format_version = 2, partitioning = ARRAY['part'])");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 1), (3, 2)", 3);
        assertUpdate("ANALYZE " + table);
        assertUpdate("ALTER TABLE " + table + " EXECUTE compute_partition_stats");
        assertThat(loadIcebergTable(table).partitionStatisticsFiles()).hasSize(1);
        double rowCount = showStatsRowCount("SELECT * FROM " + table + " WHERE part = 1");

        assertThat(query("ALTER TABLE " + table + " EXECUTE drop_partition_stats"))
                .matches("VALUES (VARCHAR 'removed_partition_statistics_count', BIGINT '1')");

        BaseTable loaded = loadIcebergTable(table);
        assertThat(loaded.partitionStatisticsFiles()).isEmpty();
        // the extended statistics are left alone
        assertThat(loaded.statisticsFiles()).hasSize(1);
        assertThat(showStatsRowCount("SELECT * FROM " + table + " WHERE part = 1")).isEqualTo(rowCount);

        // nothing left to drop
        assertThat(query("ALTER TABLE " + table + " EXECUTE drop_partition_stats"))
                .matches("VALUES (VARCHAR 'removed_partition_statistics_count', BIGINT '0')");
        assertUpdate("DROP TABLE " + table);
    }

    @Test
    public void testDropExtendedStatsKeepsPartitionStatistics()
    {
        String table = "test_drop_extended_keeps_partition_stats_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id integer, part integer) WITH (format_version = 2, partitioning = ARRAY['part'])");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 1), (3, 2)", 3);
        assertUpdate("ANALYZE " + table);
        assertUpdate("ALTER TABLE " + table + " EXECUTE compute_partition_stats");

        assertThat(query("ALTER TABLE " + table + " EXECUTE drop_extended_stats"))
                .matches("VALUES (VARCHAR 'removed_statistics_count', BIGINT '1')");

        assertThat(loadIcebergTable(table).partitionStatisticsFiles()).hasSize(1);
        assertThat(loadIcebergTable(table).statisticsFiles()).isEmpty();
        assertUpdate("DROP TABLE " + table);
    }

    @Test
    public void testComputePartitionStatsForSnapshot()
    {
        String table = "test_compute_partition_stats_snapshot_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id integer, part integer) WITH (format_version = 2, partitioning = ARRAY['part'])");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1)", 1);
        long firstSnapshotId = loadIcebergTable(table).currentSnapshot().snapshotId();
        assertUpdate("INSERT INTO " + table + " VALUES (2, 1)", 1);
        long secondSnapshotId = loadIcebergTable(table).currentSnapshot().snapshotId();

        assertUpdate("ALTER TABLE " + table + " EXECUTE compute_partition_stats(snapshot_id => " + firstSnapshotId + ")");
        assertThat(loadIcebergTable(table).partitionStatisticsFiles()).extracting(PartitionStatisticsFile::snapshotId)
                .containsExactly(firstSnapshotId);
        assertThat(partitionRowCounts(table, firstSnapshotId)).containsExactlyInAnyOrderEntriesOf(Map.of(1, 1L));

        // the current snapshot continues from the file of its parent
        assertUpdate("ALTER TABLE " + table + " EXECUTE compute_partition_stats");
        assertThat(partitionRowCounts(table, secondSnapshotId)).containsExactlyInAnyOrderEntriesOf(Map.of(1, 2L));
        assertThat(partitionRowCounts(table, firstSnapshotId)).containsExactlyInAnyOrderEntriesOf(Map.of(1, 1L));
        assertUpdate("DROP TABLE " + table);
    }

    @Test
    public void testComputePartitionStatsInvalidSnapshot()
    {
        String table = "test_compute_partition_stats_invalid_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id integer, part integer) WITH (partitioning = ARRAY['part'])");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1)", 1);
        assertQueryFails("ALTER TABLE " + table + " EXECUTE compute_partition_stats(snapshot_id => 42)", "Snapshot not found: 42");
        assertUpdate("DROP TABLE " + table);
    }

    @Test
    public void testComputePartitionStatsUnpartitionedTable()
    {
        String table = "test_compute_partition_stats_unpartitioned_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id integer)");
        assertUpdate("INSERT INTO " + table + " VALUES (1)", 1);
        assertQueryFails("ALTER TABLE " + table + " EXECUTE compute_partition_stats", "Table is not partitioned");
        assertUpdate("DROP TABLE " + table);
    }

    @Test
    public void testComputePartitionStatsEmptyTable()
    {
        String table = "test_compute_partition_stats_empty_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id integer, part integer) WITH (partitioning = ARRAY['part'])");
        assertUpdate("ALTER TABLE " + table + " EXECUTE compute_partition_stats");
        assertThat(loadIcebergTable(table).partitionStatisticsFiles()).isEmpty();
        assertUpdate("DROP TABLE " + table);
    }

    @Test
    public void testComputePartitionStatsOrcTableNotSupported()
    {
        String table = "test_compute_partition_stats_orc_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id integer, part integer) WITH (format = 'ORC', partitioning = ARRAY['part'])");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 2)", 2);

        assertQueryFails(
                "ALTER TABLE " + table + " EXECUTE compute_partition_stats",
                "Partition statistics are not supported for tables with the ORC file format");
        assertThat(loadIcebergTable(table).partitionStatisticsFiles()).isEmpty();
        assertUpdate("DROP TABLE " + table);
    }

    @Test
    public void testComputePartitionStatsKeepsFileInOtherFormat()
            throws Exception
    {
        String table = "test_compute_partition_stats_other_format_" + randomNameSuffix();
        createPartitionedTable(table);
        String path = registerPartitionStatisticsWithExtension(table, "stats");
        String metadataLocation = loadIcebergTable(table).operations().current().metadataFileLocation();

        // the snapshot has a file, which the library does not compute again either, though the connector does not read it
        assertUpdate("ALTER TABLE " + table + " EXECUTE compute_partition_stats");

        BaseTable loaded = loadIcebergTable(table);
        assertThat(loaded.operations().current().metadataFileLocation()).isEqualTo(metadataLocation);
        assertThat(loaded.partitionStatisticsFiles()).extracting(PartitionStatisticsFile::path).containsExactly(path);
        assertUpdate("DROP TABLE " + table);
    }

    @ParameterizedTest
    @MethodSource("partitionStatisticsFormats")
    public void testComputePartitionStatsWritesTableFormat(FileFormat format)
            throws Exception
    {
        String table = "test_compute_partition_stats_format_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id integer, part integer) WITH (format = '" + format.name() + "', partitioning = ARRAY['part'])");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 1), (3, 2)", 3);
        assertUpdate("ALTER TABLE " + table + " EXECUTE compute_partition_stats");

        BaseTable loaded = loadIcebergTable(table);
        PartitionStatisticsFile file = getOnlyElement(loaded.partitionStatisticsFiles());
        assertThat(FileFormat.fromFileName(file.path())).isEqualTo(format);

        // readable by the library's own reader, as by other engines
        Schema schema = PartitionStatistics.schema(Partitioning.partitionType(loaded), TableUtil.formatVersion(loaded));
        List<StructLike> records = IcebergTestUtils.readWithLibrary(format, loaded.io().newInputFile(file.path()), schema);
        assertThat(records).hasSize(2);
        assertThat(showStatsRowCount("SELECT * FROM " + table + " WHERE part = 1")).isEqualTo(2.0);
        assertUpdate("DROP TABLE " + table);
    }

    @Test
    public void testComputePartitionStatsSurvivesOrphanFileRemovalButNotSnapshotExpiration()
    {
        String table = "test_compute_partition_stats_housekeeping_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id integer, part integer) WITH (partitioning = ARRAY['part'])");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1)", 1);
        assertUpdate("ALTER TABLE " + table + " EXECUTE compute_partition_stats");
        String path = getOnlyElement(loadIcebergTable(table).partitionStatisticsFiles()).path();
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", "remove_orphan_files_min_retention", "0s")
                .setCatalogSessionProperty("iceberg", EXPIRE_SNAPSHOTS_MIN_RETENTION, "0s")
                .build();

        assertUpdate(session, "ALTER TABLE " + table + " EXECUTE remove_orphan_files(retention_threshold => '0s')");
        assertThat(loadIcebergTable(table).io().newInputFile(path).exists()).isTrue();

        // a new snapshot, so that the first one expires with its file
        assertUpdate("INSERT INTO " + table + " VALUES (2, 2)", 1);
        assertUpdate(session, "ALTER TABLE " + table + " EXECUTE expire_snapshots(retention_threshold => '0s')");
        assertThat(loadIcebergTable(table).partitionStatisticsFiles()).isEmpty();
        assertThat(loadIcebergTable(table).io().newInputFile(path).exists()).isFalse();
        assertUpdate("DROP TABLE " + table);
    }

    private void writePartitionStatistics(String tableName, FileFormat format)
            throws IOException
    {
        IcebergTestUtils.writePartitionStatistics(loadIcebergTable(tableName), format);
    }

    private BaseTable loadIcebergTable(String tableName)
    {
        return loadTable(tableName, metastore, fileSystemFactory, "iceberg", "tpch");
    }
}
