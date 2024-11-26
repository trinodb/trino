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
package io.trino.plugin.deltalake.transactionlog.checkpoint;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.RemoveFileEntry;
import io.trino.plugin.deltalake.transactionlog.TransactionEntry;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeFileStatistics;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeJsonFileStatistics;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeParquetFileStatistics;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlRow;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.TypeManager;
import io.trino.util.DateTimeUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.deltalake.DeltaTestingConnectorSession.SESSION;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.ADD;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.METADATA;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.PROTOCOL;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.REMOVE;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.TRANSACTION;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static io.trino.util.DateTimeUtils.parseDate;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCheckpointWriter
{
    private final TypeManager typeManager = TESTING_TYPE_MANAGER;
    private final CheckpointSchemaManager checkpointSchemaManager = new CheckpointSchemaManager(typeManager);

    @Test
    public void testCheckpointWriteReadJsonRoundtrip()
            throws IOException
    {
        MetadataEntry metadataEntry = new MetadataEntry(
                "metadataId",
                "metadataName",
                "metadataDescription",
                new MetadataEntry.Format(
                        "metadataFormatProvider",
                        ImmutableMap.of(
                                "formatOptionX", "blah",
                                "fomatOptionY", "plah")),
                "{\"type\":\"struct\",\"fields\":" +
                        "[{\"name\":\"part_key\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"ts\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"ts_ntz\",\"type\":\"timestamp_ntz\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"str\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"dec_short\",\"type\":\"decimal(5,1)\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"dec_long\",\"type\":\"decimal(25,3)\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"l\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"in\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"sh\",\"type\":\"short\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"byt\",\"type\":\"byte\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"fl\",\"type\":\"float\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"dou\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"bool\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"bin\",\"type\":\"binary\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"dat\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"arr\",\"type\":{\"type\":\"array\",\"elementType\":\"integer\",\"containsNull\":true},\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"m\",\"type\":{\"type\":\"map\",\"keyType\":\"integer\",\"valueType\":\"string\",\"valueContainsNull\":true},\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"row\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"s1\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"s2\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}}]}",
                ImmutableList.of("part_key"),
                ImmutableMap.of(
                        "delta.checkpoint.writeStatsAsStruct", "false",
                        "configOption1", "blah",
                        "configOption2", "plah"),
                1000);
        ProtocolEntry protocolEntry = new ProtocolEntry(10, 20, Optional.empty(), Optional.empty());
        TransactionEntry transactionEntry = new TransactionEntry("appId", 1, 1001);
        AddFileEntry addFileEntryJsonStats = new AddFileEntry(
                "addFilePathJson",
                ImmutableMap.of("part_key", "7.0"),
                1000,
                1001,
                true,
                Optional.of("{" +
                        "\"numRecords\":20," +
                        "\"minValues\":{" +
                        "\"ts\":\"2960-10-31T01:00:00.000Z\"," +
                        "\"ts_ntz\":\"2020-01-01T01:02:03.123\"," +
                        "\"str\":\"a\"," +
                        "\"dec_short\":10.1," +
                        "\"dec_long\":111111111111.123," +
                        "\"l\":1000000000," +
                        "\"in\":100000," +
                        "\"sh\":100," +
                        "\"byt\":10," +
                        "\"fl\":0.100," +
                        "\"dou\":0.101," +
                        "\"dat\":\"2000-01-01\"," +
                        "\"row\":{\"s1\":1,\"s2\":\"a\"}" +
                        "}," +
                        "\"maxValues\":{" +
                        "\"ts\":\"2960-10-31T02:00:00.000Z\"," +
                        "\"ts_ntz\":\"3000-01-01T01:02:03.123\"," +
                        "\"str\":\"z\"," +
                        "\"dec_short\":20.1," +
                        "\"dec_long\":222222222222.123," +
                        "\"l\":2000000000," +
                        "\"in\":200000," +
                        "\"sh\":200," +
                        "\"byt\":20," +
                        "\"fl\":0.200," +
                        "\"dou\":0.202," +
                        "\"dat\":\"3000-01-01\"," +
                        "\"row\":{\"s1\":1,\"s2\":\"a\"}" +
                        "}," +
                        "\"nullCount\":{" +
                        "\"ts\":1," +
                        "\"str\":2," +
                        "\"dec_short\":3," +
                        "\"dec_long\":4," +
                        "\"l\":5," +
                        "\"in\":6," +
                        "\"sh\":7," +
                        "\"byt\":8," +
                        "\"fl\":9," +
                        "\"dou\":10," +
                        "\"bool\":11," +
                        "\"bin\":12," +
                        "\"dat\":13," +
                        "\"arr\":0,\"m\":14," +
                        "\"row\":{\"s1\":0,\"s2\":15}}}"),
                Optional.empty(),
                ImmutableMap.of(
                        "someTag", "someValue",
                        "otherTag", "otherValue"),
                Optional.empty());

        RemoveFileEntry removeFileEntry = new RemoveFileEntry(
                "removeFilePath",
                ImmutableMap.of("part_key", "7.0"),
                1000,
                true,
                Optional.empty());

        CheckpointEntries entries = new CheckpointEntries(
                metadataEntry,
                protocolEntry,
                ImmutableSet.of(transactionEntry),
                ImmutableSet.of(addFileEntryJsonStats),
                ImmutableSet.of(removeFileEntry));

        CheckpointWriter writer = new CheckpointWriter(typeManager, checkpointSchemaManager, "test");

        File targetFile = File.createTempFile("testCheckpointWriteReadRoundtrip-", ".checkpoint.parquet");
        targetFile.deleteOnExit();

        String targetPath = "file://" + targetFile.getAbsolutePath();
        targetFile.delete(); // file must not exist when writer is called
        writer.write(entries, createOutputFile(targetPath));

        CheckpointEntries readEntries = readCheckpoint(targetPath, metadataEntry, protocolEntry, true);
        assertThat(readEntries.transactionEntries()).isEqualTo(entries.transactionEntries());
        assertThat(readEntries.removeFileEntries()).isEqualTo(entries.removeFileEntries());
        assertThat(readEntries.metadataEntry()).isEqualTo(entries.metadataEntry());
        assertThat(readEntries.protocolEntry()).isEqualTo(entries.protocolEntry());
        assertThat(readEntries.addFileEntries().stream().map(this::makeComparable).collect(toImmutableSet())).isEqualTo(entries.addFileEntries().stream().map(this::makeComparable).collect(toImmutableSet()));
    }

    @Test
    public void testCheckpointWriteReadParquetStatisticsRoundtrip()
            throws IOException
    {
        MetadataEntry metadataEntry = new MetadataEntry(
                "metadataId",
                "metadataName",
                "metadataDescription",
                new MetadataEntry.Format(
                        "metadataFormatProvider",
                        ImmutableMap.of(
                                "formatOptionX", "blah",
                                "fomatOptionY", "plah")),
                "{\"type\":\"struct\",\"fields\":" +
                        "[{\"name\":\"part_key\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"ts\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"ts_ntz\",\"type\":\"timestamp_ntz\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"str\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"dec_short\",\"type\":\"decimal(5,1)\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"dec_long\",\"type\":\"decimal(25,3)\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"l\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"in\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"sh\",\"type\":\"short\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"byt\",\"type\":\"byte\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"fl\",\"type\":\"float\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"dou\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"bool\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"bin\",\"type\":\"binary\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"dat\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"arr\",\"type\":{\"type\":\"array\",\"elementType\":\"integer\",\"containsNull\":true},\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"m\",\"type\":{\"type\":\"map\",\"keyType\":\"integer\",\"valueType\":\"string\",\"valueContainsNull\":true},\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"row\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"s1\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"s2\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}}]}",
                ImmutableList.of("part_key"),
                ImmutableMap.of(
                        "configOption1", "blah",
                        "configOption2", "plah"),
                1000);
        ProtocolEntry protocolEntry = new ProtocolEntry(10, 20, Optional.empty(), Optional.empty());
        TransactionEntry transactionEntry = new TransactionEntry("appId", 1, 1001);

        Block[] minMaxRowFieldBlocks = new Block[] {
                nativeValueToBlock(IntegerType.INTEGER, 1L),
                nativeValueToBlock(createUnboundedVarcharType(), utf8Slice("a"))
        };
        Block[] nullCountRowFieldBlocks = new Block[] {
                nativeValueToBlock(BigintType.BIGINT, 0L),
                nativeValueToBlock(BigintType.BIGINT, 15L)
        };
        AddFileEntry addFileEntryParquetStats = new AddFileEntry(
                "addFilePathParquet",
                ImmutableMap.of("part_key", "7.0"),
                1000,
                1001,
                true,
                Optional.empty(),
                Optional.of(new DeltaLakeParquetFileStatistics(
                        Optional.of(5L),
                        Optional.of(ImmutableMap.<String, Object>builder()
                                .put("ts", DateTimeUtils.convertToTimestampWithTimeZone(UTC_KEY, "2060-10-31 01:00:00"))
                                .put("ts_ntz", convertToTimestamp("2060-10-31T01:00:00.123"))
                                .put("str", utf8Slice("a"))
                                .put("dec_short", 101L)
                                .put("dec_long", Int128.valueOf(111111111111123L))
                                .put("l", 1000000000L)
                                .put("in", 100000L)
                                .put("sh", 100L)
                                .put("byt", 10L)
                                .put("fl", (long) Float.floatToIntBits(0.100f))
                                .put("dou", 0.101d)
                                .put("dat", (long) parseDate("2000-01-01"))
                                .put("row", new SqlRow(0, minMaxRowFieldBlocks))
                                .buildOrThrow()),
                        Optional.of(ImmutableMap.<String, Object>builder()
                                .put("ts", DateTimeUtils.convertToTimestampWithTimeZone(UTC_KEY, "2060-10-31 02:00:00"))
                                .put("ts_ntz", convertToTimestamp("2060-10-31T02:00:00.123"))
                                .put("str", utf8Slice("a"))
                                .put("dec_short", 201L)
                                .put("dec_long", Int128.valueOf(222222222222123L))
                                .put("l", 2000000000L)
                                .put("in", 200000L)
                                .put("sh", 200L)
                                .put("byt", 20L)
                                .put("fl", (long) Float.floatToIntBits(0.200f))
                                .put("dou", 0.202d)
                                .put("dat", (long) parseDate("3000-01-01"))
                                .put("row", new SqlRow(0, minMaxRowFieldBlocks))
                                .buildOrThrow()),
                        Optional.of(ImmutableMap.<String, Object>builder()
                                .put("ts", 1L)
                                .put("ts_ntz", 16L)
                                .put("str", 2L)
                                .put("dec_short", 3L)
                                .put("dec_long", 4L)
                                .put("l", 5L)
                                .put("in", 6L)
                                .put("sh", 7L)
                                .put("byt", 8L)
                                .put("fl", 9L)
                                .put("dou", 10L)
                                .put("bool", 11L)
                                .put("bin", 12L)
                                .put("dat", 13L)
                                .put("arr", 14L)
                                .put("row", new SqlRow(0, nullCountRowFieldBlocks))
                                .buildOrThrow()))),
                ImmutableMap.of(
                        "someTag", "someValue",
                        "otherTag", "otherValue"),
                Optional.empty());

        RemoveFileEntry removeFileEntry = new RemoveFileEntry(
                "removeFilePath",
                ImmutableMap.of("part_key", "7.0"),
                1000,
                true,
                Optional.empty());

        CheckpointEntries entries = new CheckpointEntries(
                metadataEntry,
                protocolEntry,
                ImmutableSet.of(transactionEntry),
                ImmutableSet.of(addFileEntryParquetStats),
                ImmutableSet.of(removeFileEntry));

        CheckpointWriter writer = new CheckpointWriter(typeManager, checkpointSchemaManager, "test");

        File targetFile = File.createTempFile("testCheckpointWriteReadRoundtrip-", ".checkpoint.parquet");
        targetFile.deleteOnExit();

        String targetPath = "file://" + targetFile.getAbsolutePath();
        targetFile.delete(); // file must not exist when writer is called
        writer.write(entries, createOutputFile(targetPath));

        CheckpointEntries readEntries = readCheckpoint(targetPath, metadataEntry, protocolEntry, true);
        assertThat(readEntries.transactionEntries()).isEqualTo(entries.transactionEntries());
        assertThat(readEntries.removeFileEntries()).isEqualTo(entries.removeFileEntries());
        assertThat(readEntries.metadataEntry()).isEqualTo(entries.metadataEntry());
        assertThat(readEntries.protocolEntry()).isEqualTo(entries.protocolEntry());
        assertThat(readEntries.addFileEntries().stream().map(this::makeComparable).collect(toImmutableSet())).isEqualTo(entries.addFileEntries().stream().map(this::makeComparable).collect(toImmutableSet()));
    }

    private static long convertToTimestamp(String value)
    {
        LocalDateTime localDateTime = LocalDateTime.parse(value);
        return localDateTime.toEpochSecond(UTC) * MICROSECONDS_PER_SECOND
                + localDateTime.getNano() / NANOSECONDS_PER_MICROSECOND;
    }

    @Test
    public void testDisablingRowStatistics()
            throws IOException
    {
        MetadataEntry metadataEntry = new MetadataEntry(
                "metadataId",
                "metadataName",
                "metadataDescription",
                new MetadataEntry.Format(
                        "metadataFormatProvider",
                        ImmutableMap.of(
                                "formatOptionX", "blah",
                                "fomatOptionY", "plah")),
                "{\"type\":\"struct\",\"fields\":" +
                        "[{\"name\":\"part_key\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"row\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"s1\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"s2\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}}]}",
                ImmutableList.of("part_key"),
                ImmutableMap.of(),
                1000);
        ProtocolEntry protocolEntry = new ProtocolEntry(10, 20, Optional.empty(), Optional.empty());
        Block[] minMaxRowFieldBlocks = new Block[] {
                nativeValueToBlock(IntegerType.INTEGER, 1L),
                nativeValueToBlock(createUnboundedVarcharType(), utf8Slice("a"))
        };
        Block[] nullCountRowFieldBlocks = new Block[] {
                nativeValueToBlock(BigintType.BIGINT, 0L),
                nativeValueToBlock(BigintType.BIGINT, 15L)
        };
        AddFileEntry addFileEntryParquetStats = new AddFileEntry(
                "addFilePathParquet",
                ImmutableMap.of("part_key", "7.0"),
                1000,
                1001,
                true,
                Optional.empty(),
                Optional.of(new DeltaLakeParquetFileStatistics(
                        Optional.of(5L),
                        Optional.of(ImmutableMap.of("row", new SqlRow(0, minMaxRowFieldBlocks))),
                        Optional.of(ImmutableMap.of("row", new SqlRow(0, minMaxRowFieldBlocks))),
                        Optional.of(ImmutableMap.of("row", new SqlRow(0, nullCountRowFieldBlocks))))),
                ImmutableMap.of(),
                Optional.empty());

        CheckpointEntries entries = new CheckpointEntries(
                metadataEntry,
                protocolEntry,
                ImmutableSet.of(),
                ImmutableSet.of(addFileEntryParquetStats),
                ImmutableSet.of());

        CheckpointWriter writer = new CheckpointWriter(typeManager, checkpointSchemaManager, "test");

        File targetFile = File.createTempFile("testCheckpointWriteReadRoundtrip-", ".checkpoint.parquet");
        targetFile.deleteOnExit();

        String targetPath = "file://" + targetFile.getAbsolutePath();
        targetFile.delete(); // file must not exist when writer is called
        writer.write(entries, createOutputFile(targetPath));

        CheckpointEntries readEntries = readCheckpoint(targetPath, metadataEntry, protocolEntry, false);
        AddFileEntry addFileEntry = getOnlyElement(readEntries.addFileEntries());
        assertThat(addFileEntry.getStats()).isPresent();

        DeltaLakeParquetFileStatistics fileStatistics = (DeltaLakeParquetFileStatistics) addFileEntry.getStats().get();
        assertThat(fileStatistics.getMinValues().get()).isEmpty();
        assertThat(fileStatistics.getMaxValues().get()).isEmpty();
        assertThat(fileStatistics.getNullCount().get()).isEmpty();
    }

    private AddFileEntry makeComparable(AddFileEntry original)
    {
        return new AddFileEntry(
                original.getPath(),
                original.getPartitionValues(),
                original.getSize(),
                original.getModificationTime(),
                original.isDataChange(),
                original.getStatsString(),
                makeComparable(original.getStats()),
                original.getTags(),
                original.getDeletionVector());
    }

    private Optional<DeltaLakeParquetFileStatistics> makeComparable(Optional<? extends DeltaLakeFileStatistics> original)
    {
        if (original.isEmpty() || original.get() instanceof DeltaLakeJsonFileStatistics) {
            return Optional.empty();
        }

        DeltaLakeParquetFileStatistics originalStatistics = (DeltaLakeParquetFileStatistics) original.get();
        return Optional.of(
                new DeltaLakeParquetFileStatistics(
                        originalStatistics.getNumRecords(),
                        makeComparableStatistics(originalStatistics.getMinValues()),
                        makeComparableStatistics(originalStatistics.getMaxValues()),
                        makeComparableStatistics(originalStatistics.getNullCount())));
    }

    private Optional<Map<String, Object>> makeComparableStatistics(Optional<Map<String, Object>> original)
    {
        if (original.isEmpty()) {
            return Optional.empty();
        }

        Map<String, Object> stats = original.get();
        ImmutableMap.Builder<String, Object> comparableStats = ImmutableMap.builder();
        for (String key : stats.keySet()) {
            Object statsValue = stats.get(key);
            if (statsValue instanceof SqlRow sqlRow) {
                // todo: this validation is just broken. The only way to compare values is to use types.
                // see https://github.com/trinodb/trino/issues/19557
                ImmutableList<String> logicalSizes = sqlRow.getRawFieldBlocks().stream()
                        .map(block -> block.getUnderlyingValueBlock().getClass().getName())
                        .collect(toImmutableList());
                comparableStats.put(key, logicalSizes);
            }
            else if (statsValue instanceof Slice slice) {
                comparableStats.put(key, slice.toStringUtf8());
            }
            else {
                comparableStats.put(key, statsValue);
            }
        }

        return Optional.of(comparableStats.buildOrThrow());
    }

    private CheckpointEntries readCheckpoint(String checkpointPath, MetadataEntry metadataEntry, ProtocolEntry protocolEntry, boolean rowStatisticsEnabled)
            throws IOException
    {
        TrinoFileSystem fileSystem = new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS).create(SESSION);
        TrinoInputFile checkpointFile = fileSystem.newInputFile(Location.of(checkpointPath));

        Iterator<DeltaLakeTransactionLogEntry> checkpointEntryIterator = new CheckpointEntryIterator(
                checkpointFile,
                SESSION,
                checkpointFile.length(),
                checkpointSchemaManager,
                typeManager,
                ImmutableSet.of(METADATA, PROTOCOL, TRANSACTION, ADD, REMOVE),
                Optional.of(metadataEntry),
                Optional.of(protocolEntry),
                new FileFormatDataSourceStats(),
                new ParquetReaderConfig().toParquetReaderOptions(),
                rowStatisticsEnabled,
                new DeltaLakeConfig().getDomainCompactionThreshold(),
                TupleDomain.all(),
                Optional.of(alwaysTrue()));

        CheckpointBuilder checkpointBuilder = new CheckpointBuilder();
        while (checkpointEntryIterator.hasNext()) {
            DeltaLakeTransactionLogEntry entry = checkpointEntryIterator.next();
            checkpointBuilder.addLogEntry(entry);
        }

        return checkpointBuilder.build();
    }

    private static TrinoOutputFile createOutputFile(String path)
    {
        return new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS).create(SESSION).newOutputFile(Location.of(path));
    }
}
