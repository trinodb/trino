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
import com.google.common.collect.Iterators;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.RemoveFileEntry;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeParquetFileStatistics;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.io.Resources.getResource;
import static com.google.common.math.LongMath.divide;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.DeltaTestingConnectorSession.SESSION;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.ADD;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.COMMIT;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.METADATA;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.PROTOCOL;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.REMOVE;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.TRANSACTION;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.spi.predicate.Domain.notNull;
import static io.trino.spi.predicate.Domain.onlyNull;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.Float.floatToIntBits;
import static java.math.RoundingMode.UNNECESSARY;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestCheckpointEntryIterator
{
    private static final String TEST_CHECKPOINT = "databricks73/person/_delta_log/00000000000000000010.checkpoint.parquet";

    private CheckpointSchemaManager checkpointSchemaManager;

    @BeforeAll
    public void setUp()
    {
        checkpointSchemaManager = new CheckpointSchemaManager(TESTING_TYPE_MANAGER);
    }

    @AfterAll
    public void tearDown()
    {
        checkpointSchemaManager = null;
    }

    @Test
    public void testReadNoEntries()
            throws Exception
    {
        URI checkpointUri = getResource(TEST_CHECKPOINT).toURI();
        assertThatThrownBy(() -> createCheckpointEntryIterator(checkpointUri, ImmutableSet.of(), Optional.empty(), Optional.empty(), TupleDomain.all(), Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("fields is empty");
    }

    @Test
    public void testReadMetadataEntry()
            throws Exception
    {
        URI checkpointUri = getResource(TEST_CHECKPOINT).toURI();
        assertThat(readMetadataEntry(checkpointUri))
                .isEqualTo(
                        new MetadataEntry(
                                "b6aeffad-da73-4dde-b68e-937e468b1fde",
                                null,
                                null,
                                new MetadataEntry.Format("parquet", Map.of()),
                                "{\"type\":\"struct\",\"fields\":[" +
                                        "{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}," +
                                        "{\"name\":\"age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}," +
                                        "{\"name\":\"married\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}}," +

                                        "{\"name\":\"phones\",\"type\":{\"type\":\"array\",\"elementType\":{\"type\":\"struct\",\"fields\":[" +
                                        "{\"name\":\"number\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}," +
                                        "{\"name\":\"label\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}," +
                                        "\"containsNull\":true},\"nullable\":true,\"metadata\":{}}," +

                                        "{\"name\":\"address\",\"type\":{\"type\":\"struct\",\"fields\":[" +
                                        "{\"name\":\"street\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}," +
                                        "{\"name\":\"city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}," +
                                        "{\"name\":\"state\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}," +
                                        "{\"name\":\"zip\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}}," +

                                        "{\"name\":\"income\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]}",
                                List.of("age"),
                                Map.of(),
                                1579190100722L));
    }

    @Test
    public void testReadProtocolEntries()
            throws Exception
    {
        URI checkpointUri = getResource(TEST_CHECKPOINT).toURI();
        CheckpointEntryIterator checkpointEntryIterator = createCheckpointEntryIterator(checkpointUri, ImmutableSet.of(PROTOCOL), Optional.empty(), Optional.empty(), TupleDomain.all(), Optional.empty());
        List<DeltaLakeTransactionLogEntry> entries = ImmutableList.copyOf(checkpointEntryIterator);

        assertThat(entries).hasSize(1);

        assertThat(entries).element(0).extracting(DeltaLakeTransactionLogEntry::getProtocol).isEqualTo(
                new ProtocolEntry(
                        1,
                        2,
                        Optional.empty(),
                        Optional.empty()));
    }

    @Test
    public void testReadMetadataAndProtocolEntry()
            throws Exception
    {
        URI checkpointUri = getResource(TEST_CHECKPOINT).toURI();
        CheckpointEntryIterator checkpointEntryIterator = createCheckpointEntryIterator(
                checkpointUri,
                ImmutableSet.of(METADATA, PROTOCOL),
                Optional.empty(),
                Optional.empty(),
                TupleDomain.all(),
                Optional.empty());
        List<DeltaLakeTransactionLogEntry> entries = ImmutableList.copyOf(checkpointEntryIterator);

        assertThat(entries).hasSize(2);
        assertThat(entries).containsExactlyInAnyOrder(
                DeltaLakeTransactionLogEntry.metadataEntry(new MetadataEntry(
                        "b6aeffad-da73-4dde-b68e-937e468b1fde",
                        null,
                        null,
                        new MetadataEntry.Format("parquet", Map.of()),
                        "{\"type\":\"struct\",\"fields\":[" +
                                "{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}," +
                                "{\"name\":\"age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}," +
                                "{\"name\":\"married\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}}," +

                                "{\"name\":\"phones\",\"type\":{\"type\":\"array\",\"elementType\":{\"type\":\"struct\",\"fields\":[" +
                                "{\"name\":\"number\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}," +
                                "{\"name\":\"label\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}," +
                                "\"containsNull\":true},\"nullable\":true,\"metadata\":{}}," +

                                "{\"name\":\"address\",\"type\":{\"type\":\"struct\",\"fields\":[" +
                                "{\"name\":\"street\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}," +
                                "{\"name\":\"city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}," +
                                "{\"name\":\"state\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}," +
                                "{\"name\":\"zip\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}}," +

                                "{\"name\":\"income\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]}",
                        List.of("age"),
                        Map.of(),
                        1579190100722L)),
                DeltaLakeTransactionLogEntry.protocolEntry(
                        new ProtocolEntry(
                                1,
                                2,
                                Optional.empty(),
                                Optional.empty())));
    }

    @Test
    public void testReadAddEntries()
            throws Exception
    {
        URI checkpointUri = getResource(TEST_CHECKPOINT).toURI();
        CheckpointEntryIterator checkpointEntryIterator = createCheckpointEntryIterator(
                checkpointUri,
                ImmutableSet.of(ADD),
                Optional.of(readMetadataEntry(checkpointUri)),
                Optional.of(readProtocolEntry(checkpointUri)),
                TupleDomain.all(),
                Optional.of(alwaysTrue()));
        List<DeltaLakeTransactionLogEntry> entries = ImmutableList.copyOf(checkpointEntryIterator);

        assertThat(entries).hasSize(9);

        assertThat(entries).element(3).extracting(DeltaLakeTransactionLogEntry::getAdd).isEqualTo(
                new AddFileEntry(
                        "age=42/part-00003-0f53cae3-3e34-4876-b651-e1db9584dbc3.c000.snappy.parquet",
                        Map.of("age", "42"),
                        2634,
                        1579190165000L,
                        false,
                        Optional.of("{" +
                                "\"numRecords\":1," +
                                "\"minValues\":{\"name\":\"Alice\",\"address\":{\"street\":\"100 Main St\",\"city\":\"Anytown\",\"state\":\"NY\",\"zip\":\"12345\"},\"income\":111000.0}," +
                                "\"maxValues\":{\"name\":\"Alice\",\"address\":{\"street\":\"100 Main St\",\"city\":\"Anytown\",\"state\":\"NY\",\"zip\":\"12345\"},\"income\":111000.0}," +
                                "\"nullCount\":{\"name\":0,\"married\":0,\"phones\":0,\"address\":{\"street\":0,\"city\":0,\"state\":0,\"zip\":0},\"income\":0}" +
                                "}"),
                        Optional.empty(),
                        null,
                        Optional.empty()));

        assertThat(entries).element(7).extracting(DeltaLakeTransactionLogEntry::getAdd).isEqualTo(
                new AddFileEntry(
                        "age=30/part-00002-5800be2e-2373-47d8-8b86-776a8ea9d69f.c000.snappy.parquet",
                        Map.of("age", "30"),
                        2688,
                        1579190165000L,
                        false,
                        Optional.of("{" +
                                "\"numRecords\":1," +
                                "\"minValues\":{\"name\":\"Andy\",\"address\":{\"street\":\"101 Main St\",\"city\":\"Anytown\",\"state\":\"NY\",\"zip\":\"12345\"},\"income\":81000.0}," +
                                "\"maxValues\":{\"name\":\"Andy\",\"address\":{\"street\":\"101 Main St\",\"city\":\"Anytown\",\"state\":\"NY\",\"zip\":\"12345\"},\"income\":81000.0}," +
                                "\"nullCount\":{\"name\":0,\"married\":0,\"phones\":0,\"address\":{\"street\":0,\"city\":0,\"state\":0,\"zip\":0},\"income\":0}" +
                                "}"),
                        Optional.empty(),
                        null,
                        Optional.empty()));
    }

    @Test
    public void testReadAddEntriesPartitionPruning()
            throws Exception
    {
        String checkpoint = "deltalake/partition_values_parsed/_delta_log/00000000000000000003.checkpoint.parquet";
        URI checkpointUri = getResource(checkpoint).toURI();

        DeltaLakeColumnHandle stringPartField = new DeltaLakeColumnHandle(
                "string_part",
                VARCHAR,
                OptionalInt.empty(),
                "string_part",
                VARCHAR,
                REGULAR,
                Optional.empty());

        DeltaLakeColumnHandle intPartField = new DeltaLakeColumnHandle(
                "int_part",
                BIGINT,
                OptionalInt.empty(),
                "int_part",
                BIGINT,
                REGULAR,
                Optional.empty());

        // The domain specifies all partition columns
        CheckpointEntryIterator partitionsEntryIterator = createCheckpointEntryIterator(
                checkpointUri,
                ImmutableSet.of(ADD),
                Optional.of(readMetadataEntry(checkpointUri)),
                Optional.of(readProtocolEntry(checkpointUri)),
                TupleDomain.withColumnDomains(ImmutableMap.of(intPartField, singleValue(BIGINT, 10L), stringPartField, singleValue(VARCHAR, utf8Slice("part1")))),
                Optional.of(alwaysTrue()));
        List<DeltaLakeTransactionLogEntry> partitionsEntries = ImmutableList.copyOf(partitionsEntryIterator);

        assertThat(partitionsEntryIterator.getCompletedPositions().orElseThrow()).isEqualTo(5);
        assertThat(partitionsEntries)
                .hasSize(1)
                .extracting(entry -> entry.getAdd().getPath())
                .containsExactly("int_part=10/string_part=part1/part-00000-383afb1a-87de-4e70-86ab-c21ae44c7f3f.c000.snappy.parquet");

        // The domain specifies a part of partition columns
        CheckpointEntryIterator partitionEntryIterator = createCheckpointEntryIterator(
                checkpointUri,
                ImmutableSet.of(ADD),
                Optional.of(readMetadataEntry(checkpointUri)),
                Optional.of(readProtocolEntry(checkpointUri)),
                TupleDomain.withColumnDomains(ImmutableMap.of(intPartField, singleValue(BIGINT, 10L))),
                Optional.of(alwaysTrue()));
        List<DeltaLakeTransactionLogEntry> partitionEntries = ImmutableList.copyOf(partitionEntryIterator);

        assertThat(partitionEntryIterator.getCompletedPositions().orElseThrow()).isEqualTo(5);
        assertThat(partitionEntries)
                .hasSize(1)
                .extracting(entry -> entry.getAdd().getPath())
                .containsExactly("int_part=10/string_part=part1/part-00000-383afb1a-87de-4e70-86ab-c21ae44c7f3f.c000.snappy.parquet");

        // Verify empty iterator when the condition doesn't match
        CheckpointEntryIterator emptyIterator = createCheckpointEntryIterator(
                checkpointUri,
                ImmutableSet.of(ADD),
                Optional.of(readMetadataEntry(checkpointUri)),
                Optional.of(readProtocolEntry(checkpointUri)),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        intPartField, singleValue(BIGINT, 10L),
                        stringPartField, singleValue(VARCHAR, utf8Slice("unmatched partition condition")))),
                Optional.of(alwaysTrue()));
        assertThat(ImmutableList.copyOf(emptyIterator)).isEmpty();

        // Verify IS NULL condition
        CheckpointEntryIterator isNullIterator = createCheckpointEntryIterator(
                checkpointUri,
                ImmutableSet.of(ADD),
                Optional.of(readMetadataEntry(checkpointUri)),
                Optional.of(readProtocolEntry(checkpointUri)),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        intPartField, onlyNull(BIGINT),
                        stringPartField, onlyNull(VARCHAR))),
                Optional.of(alwaysTrue()));
        assertThat(ImmutableList.copyOf(isNullIterator))
                .hasSize(1)
                .extracting(entry -> entry.getAdd().getPath())
                .containsExactly("int_part=__HIVE_DEFAULT_PARTITION__/string_part=__HIVE_DEFAULT_PARTITION__/part-00000-dcb29d13-eeca-4fa6-a8bf-860da0131a5c.c000.snappy.parquet");

        // Verify IS NOT NULL condition
        CheckpointEntryIterator isNotNullIterator = createCheckpointEntryIterator(
                checkpointUri,
                ImmutableSet.of(ADD),
                Optional.of(readMetadataEntry(checkpointUri)),
                Optional.of(readProtocolEntry(checkpointUri)),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        intPartField, notNull(BIGINT),
                        stringPartField, notNull(VARCHAR))),
                Optional.of(alwaysTrue()));
        assertThat(ImmutableList.copyOf(isNotNullIterator))
                .hasSize(2)
                .extracting(entry -> entry.getAdd().getPath())
                .containsExactly(
                        "int_part=10/string_part=part1/part-00000-383afb1a-87de-4e70-86ab-c21ae44c7f3f.c000.snappy.parquet",
                        "int_part=20/string_part=part2/part-00000-e0b4887e-95f6-4ce1-b96c-32c5cf472476.c000.snappy.parquet");
    }

    @Test
    public void testReadAddEntriesPartitionPruningAllTypes()
            throws Exception
    {
        String checkpoint = "deltalake/partition_values_parsed_all_types/_delta_log/00000000000000000003.checkpoint.parquet";
        URI checkpointUri = getResource(checkpoint).toURI();

        assertPartitionValuesParsedCondition(checkpointUri, "part_boolean", BOOLEAN, true);
        assertPartitionValuesParsedCondition(checkpointUri, "part_tinyint", TINYINT, 1L);
        assertPartitionValuesParsedCondition(checkpointUri, "part_smallint", SMALLINT, 10L);
        assertPartitionValuesParsedCondition(checkpointUri, "part_int", INTEGER, 100L);
        assertPartitionValuesParsedCondition(checkpointUri, "part_bigint", BIGINT, 1000L);
        assertPartitionValuesParsedCondition(checkpointUri, "part_short_decimal", createDecimalType(5, 2), 12312L);
        assertPartitionValuesParsedCondition(checkpointUri, "part_long_decimal", createDecimalType(21, 3), Int128.valueOf("123456789012345678123"));
        assertPartitionValuesParsedCondition(checkpointUri, "part_double", DOUBLE, 1.2);
        assertPartitionValuesParsedCondition(checkpointUri, "part_float", REAL, (long) floatToIntBits(3.4f));
        assertPartitionValuesParsedCondition(checkpointUri, "part_varchar", VARCHAR, utf8Slice("a"));
        assertPartitionValuesParsedCondition(checkpointUri, "part_date", DATE, LocalDate.parse("2020-08-21").toEpochDay());
        ZonedDateTime zonedDateTime = LocalDateTime.parse("2020-10-21T01:00:00.123").atZone(UTC);
        long timestampValue = packDateTimeWithZone(zonedDateTime.toInstant().toEpochMilli(), UTC_KEY);
        assertPartitionValuesParsedCondition(checkpointUri, "part_timestamp", TIMESTAMP_TZ_MILLIS, timestampValue);
        LocalDateTime timestampNtz = LocalDateTime.parse("2023-01-02T01:02:03.456");
        long timestampNtzValue = timestampNtz.toEpochSecond(UTC) * MICROSECONDS_PER_SECOND + divide(timestampNtz.getNano(), NANOSECONDS_PER_MICROSECOND, UNNECESSARY);
        assertPartitionValuesParsedCondition(checkpointUri, "part_timestamp_ntz", TIMESTAMP_MICROS, timestampNtzValue);
    }

    private void assertPartitionValuesParsedCondition(URI checkpointUri, String columnName, Type type, Object value)
            throws IOException
    {
        DeltaLakeColumnHandle intPartField = new DeltaLakeColumnHandle(columnName, type, OptionalInt.empty(), columnName, type, REGULAR, Optional.empty());

        CheckpointEntryIterator partitionEntryIterator = createCheckpointEntryIterator(
                checkpointUri,
                ImmutableSet.of(ADD),
                Optional.of(readMetadataEntry(checkpointUri)),
                Optional.of(readProtocolEntry(checkpointUri)),
                TupleDomain.withColumnDomains(ImmutableMap.of(intPartField, singleValue(type, value))),
                Optional.of(alwaysTrue()));
        List<DeltaLakeTransactionLogEntry> partitionEntries = ImmutableList.copyOf(partitionEntryIterator);

        assertThat(partitionEntryIterator.getCompletedPositions().orElseThrow()).isEqualTo(5);
        assertThat(partitionEntries).hasSize(1);
    }

    @Test
    public void testReadAddEntriesStatsProjection()
            throws Exception
    {
        int countIntegerColumns = 20;
        int countStringColumns = 20;
        MetadataEntry metadataEntry = new MetadataEntry(
                "metadataId",
                "metadataName",
                "metadataDescription",
                new MetadataEntry.Format(
                        "metadataFormatProvider",
                        ImmutableMap.of()),
                "{\"type\":\"struct\",\"fields\":" +
                        "[" +
                        IntStream.rangeClosed(1, countIntegerColumns)
                                .boxed()
                                .map("{\"name\":\"intcol%s\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}"::formatted)
                                .collect(Collectors.joining(",", "", ",")) +
                        IntStream.rangeClosed(1, countStringColumns)
                                .boxed()
                                .map("{\"name\":\"stringcol%s\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}"::formatted)
                                .collect(Collectors.joining(",")) +
                        "]}",
                ImmutableList.of(),
                ImmutableMap.of("delta.checkpoint.writeStatsAsJson", "false", "delta.checkpoint.writeStatsAsStruct", "true"),
                1000);
        ProtocolEntry protocolEntry = new ProtocolEntry(10, 20, Optional.empty(), Optional.empty());

        int countAddEntries = 30;
        Set<AddFileEntry> addFileEntries = IntStream.rangeClosed(1, countAddEntries).mapToObj(fileIndex -> new AddFileEntry(
                "addFilePathParquetStats" + fileIndex,
                ImmutableMap.of(),
                1000,
                1001,
                true,
                Optional.empty(),
                Optional.of(createDeltaLakeParquetFileStatistics(countIntegerColumns, countStringColumns)),
                ImmutableMap.of(),
                Optional.empty()))
                .collect(toImmutableSet());

        CheckpointEntries entries = new CheckpointEntries(
                metadataEntry,
                protocolEntry,
                ImmutableSet.of(),
                addFileEntries,
                ImmutableSet.of());

        CheckpointWriter writer = new CheckpointWriter(
                TESTING_TYPE_MANAGER,
                checkpointSchemaManager,
                "test",
                ParquetWriterOptions.builder().build());

        File targetFile = File.createTempFile("testAddStatsProjection-", ".checkpoint.parquet");
        targetFile.deleteOnExit();

        String targetPath = "file://" + targetFile.getAbsolutePath();
        targetFile.delete(); // file must not exist when writer is called
        writer.write(entries, createOutputFile(targetPath));

        MetadataEntry readMetadataEntry = readMetadataEntry(URI.create(targetPath));
        ProtocolEntry readProtocolEntry = readProtocolEntry(URI.create(targetPath));
        CheckpointEntryIterator checkpointEntryWithNameColumnStatsIterator = createCheckpointEntryIterator(
                URI.create(targetPath),
                ImmutableSet.of(ADD),
                Optional.of(readMetadataEntry),
                Optional.of(readProtocolEntry),
                TupleDomain.all(),
                Optional.of(columnName -> ImmutableList.of("intcol1", "stringcol1").contains(columnName)));
        List<DeltaLakeTransactionLogEntry> checkpointEntryWithNameColumnStatsEntries = ImmutableList.copyOf(checkpointEntryWithNameColumnStatsIterator);
        assertThat(checkpointEntryWithNameColumnStatsEntries).hasSize(countAddEntries);
        CheckpointEntryIterator checkpointEntryWithAllColumnStatsIterator = createCheckpointEntryIterator(
                URI.create(targetPath),
                ImmutableSet.of(ADD),
                Optional.of(readMetadataEntry),
                Optional.of(readProtocolEntry),
                TupleDomain.all(),
                Optional.of(alwaysTrue()));
        List<DeltaLakeTransactionLogEntry> checkpointEntryWithAllStatsEntries = ImmutableList.copyOf(checkpointEntryWithAllColumnStatsIterator);
        assertThat(checkpointEntryWithAllStatsEntries).hasSize(countAddEntries);
        assertThat(checkpointEntryWithNameColumnStatsIterator.getCompletedBytes()).isLessThan(checkpointEntryWithAllColumnStatsIterator.getCompletedBytes() * 9 / 10);
    }

    private static DeltaLakeParquetFileStatistics createDeltaLakeParquetFileStatistics(int countIntegerColumns, int countStringColumns)
    {
        Random random = new Random();
        Map<String, Object> minValues = ImmutableMap.<String, Object>builder()
                .putAll(IntStream.rangeClosed(1, countIntegerColumns)
                        .boxed()
                        .collect(toImmutableMap("intcol%s"::formatted, columnIndex -> random.nextLong(0, 1000))))
                .putAll(IntStream.rangeClosed(1, countStringColumns)
                        .boxed()
                        .collect(toImmutableMap("stringcol%s"::formatted, columnIndex -> "A".repeat(random.nextInt(0, 10)) + UUID.randomUUID())))
                .buildOrThrow();
        Map<String, Object> maxValues = ImmutableMap.<String, Object>builder()
                .putAll(IntStream.rangeClosed(1, countIntegerColumns)
                        .boxed()
                        .collect(toImmutableMap("intcol%s"::formatted, columnIndex -> 1000L + random.nextLong(0, 1000))))
                .putAll(IntStream.rangeClosed(1, countStringColumns)
                        .boxed()
                        .collect(toImmutableMap("stringcol%s"::formatted, columnIndex -> "Z".repeat(random.nextInt(0, 10)) + UUID.randomUUID())))
                .buildOrThrow();
        Map<String, Object> nullCount = ImmutableMap.<String, Object>builder()
                .putAll(IntStream.rangeClosed(1, countIntegerColumns)
                        .boxed()
                        .collect(toImmutableMap("intcol%s"::formatted, columnIndex -> random.nextLong(0, 1000))))
                .putAll(IntStream.rangeClosed(1, countStringColumns)
                        .boxed()
                        .collect(toImmutableMap("stringcol%s"::formatted, columnIndex -> random.nextLong(0, 1000))))
                .buildOrThrow();
        return new DeltaLakeParquetFileStatistics(
                Optional.of(1000L),
                Optional.of(minValues),
                Optional.of(maxValues),
                Optional.of(nullCount));
    }

    @Test
    public void testReadAllEntries()
            throws Exception
    {
        URI checkpointUri = getResource(TEST_CHECKPOINT).toURI();
        MetadataEntry metadataEntry = readMetadataEntry(checkpointUri);
        CheckpointEntryIterator checkpointEntryIterator = createCheckpointEntryIterator(
                checkpointUri,
                ImmutableSet.of(METADATA, PROTOCOL, TRANSACTION, ADD, REMOVE, COMMIT),
                Optional.of(readMetadataEntry(checkpointUri)),
                Optional.of(readProtocolEntry(checkpointUri)),
                TupleDomain.all(),
                Optional.of(alwaysTrue()));
        List<DeltaLakeTransactionLogEntry> entries = ImmutableList.copyOf(checkpointEntryIterator);

        assertThat(entries).hasSize(17);

        // MetadataEntry
        assertThat(entries).element(12).extracting(DeltaLakeTransactionLogEntry::getMetaData).isEqualTo(metadataEntry);

        // ProtocolEntry
        assertThat(entries).element(11).extracting(DeltaLakeTransactionLogEntry::getProtocol).isEqualTo(new ProtocolEntry(1, 2, Optional.empty(), Optional.empty()));

        // TransactionEntry
        // not found in the checkpoint, TODO add a test
        assertThat(entries)
                .map(DeltaLakeTransactionLogEntry::getTxn)
                .filteredOn(Objects::nonNull)
                .isEmpty();

        // AddFileEntry
        assertThat(entries).element(8).extracting(DeltaLakeTransactionLogEntry::getAdd).isEqualTo(
                new AddFileEntry(
                        "age=42/part-00003-0f53cae3-3e34-4876-b651-e1db9584dbc3.c000.snappy.parquet",
                        Map.of("age", "42"),
                        2634,
                        1579190165000L,
                        false,
                        Optional.of("{" +
                                "\"numRecords\":1," +
                                "\"minValues\":{\"name\":\"Alice\",\"address\":{\"street\":\"100 Main St\",\"city\":\"Anytown\",\"state\":\"NY\",\"zip\":\"12345\"},\"income\":111000.0}," +
                                "\"maxValues\":{\"name\":\"Alice\",\"address\":{\"street\":\"100 Main St\",\"city\":\"Anytown\",\"state\":\"NY\",\"zip\":\"12345\"},\"income\":111000.0}," +
                                "\"nullCount\":{\"name\":0,\"married\":0,\"phones\":0,\"address\":{\"street\":0,\"city\":0,\"state\":0,\"zip\":0},\"income\":0}" +
                                "}"),
                        Optional.empty(),
                        null,
                        Optional.empty()));

        // RemoveFileEntry
        assertThat(entries).element(3).extracting(DeltaLakeTransactionLogEntry::getRemove).isEqualTo(
                new RemoveFileEntry(
                        "age=42/part-00000-951068bd-bcf4-4094-bb94-536f3c41d31f.c000.snappy.parquet",
                        // partitionValues information is missing in the checkpoint
                        null,
                        1579190155406L,
                        false,
                        Optional.empty()));

        // CommitInfoEntry
        // not found in the checkpoint, TODO add a test
        assertThat(entries)
                .map(DeltaLakeTransactionLogEntry::getCommitInfo)
                .filteredOn(Objects::nonNull)
                .isEmpty();
    }

    @Test
    public void testSkipAddEntriesThroughPartitionPruning()
            throws IOException
    {
        MetadataEntry metadataEntry = new MetadataEntry(
                "metadataId",
                "metadataName",
                "metadataDescription",
                new MetadataEntry.Format(
                        "metadataFormatProvider",
                        ImmutableMap.of()),
                "{\"type\":\"struct\",\"fields\":" +
                        "[{\"name\":\"ts\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"part_key\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}",
                ImmutableList.of("part_key"),
                ImmutableMap.of(),
                1000);
        ProtocolEntry protocolEntry = new ProtocolEntry(1, 1, Optional.empty(), Optional.empty());
        int numAddEntries = 100;
        Set<AddFileEntry> addFileEntries = IntStream.rangeClosed(1, numAddEntries).mapToObj(value ->
                        new AddFileEntry(
                                "addFilePath",
                                ImmutableMap.of("part_key", Integer.toString(value)),
                                1000,
                                1001,
                                true,
                                Optional.of("{" +
                                        "\"numRecords\":20," +
                                        "\"minValues\":{" +
                                        "\"ts\":\"1990-10-31T01:00:00.000Z\"" +
                                        "}," +
                                        "\"maxValues\":{" +
                                        "\"ts\":\"1990-10-31T02:00:00.000Z\"" +
                                        "}," +
                                        "\"nullCount\":{" +
                                        "\"ts\":1" +
                                        "}}"),
                                Optional.empty(),
                                ImmutableMap.of(),
                                Optional.empty()))
                .collect(toImmutableSet());

        CheckpointEntries entries = new CheckpointEntries(
                metadataEntry,
                protocolEntry,
                ImmutableSet.of(),
                addFileEntries,
                ImmutableSet.of());

        CheckpointWriter writer = new CheckpointWriter(
                TESTING_TYPE_MANAGER,
                checkpointSchemaManager,
                "test",
                ParquetWriterOptions.builder() // approximately 2 rows per row group
                        .setMaxBlockSize(DataSize.ofBytes(64L))
                        .setMaxPageSize(DataSize.ofBytes(64L))
                        .build());

        File targetFile = File.createTempFile("testSkipAddEntries-", ".checkpoint.parquet");
        targetFile.deleteOnExit();

        String targetPath = "file://" + targetFile.getAbsolutePath();
        targetFile.delete(); // file must not exist when writer is called
        writer.write(entries, createOutputFile(targetPath));

        CheckpointEntryIterator metadataAndProtocolEntryIterator = createCheckpointEntryIterator(
                URI.create(targetPath),
                ImmutableSet.of(METADATA, PROTOCOL),
                Optional.empty(),
                Optional.empty(),
                TupleDomain.all(),
                Optional.empty());

        DeltaLakeColumnHandle partitionKeyField = new DeltaLakeColumnHandle(
                "part_key",
                INTEGER,
                OptionalInt.empty(),
                "part_key",
                INTEGER,
                REGULAR,
                Optional.empty());

        CheckpointEntryIterator addEntryIterator = createCheckpointEntryIterator(
                URI.create(targetPath),
                ImmutableSet.of(ADD),
                Optional.of(metadataEntry),
                Optional.of(protocolEntry),
                TupleDomain.all(),
                Optional.of(alwaysTrue()));

        CheckpointEntryIterator addEntryIteratorWithEqualityPartitionFilter = createCheckpointEntryIterator(
                URI.create(targetPath),
                ImmutableSet.of(ADD),
                Optional.of(metadataEntry),
                Optional.of(protocolEntry),
                TupleDomain.withColumnDomains(ImmutableMap.of(partitionKeyField, singleValue(INTEGER, 10L))),
                Optional.of(alwaysTrue()));

        CheckpointEntryIterator addEntryIteratorWithRangePartitionFilter = createCheckpointEntryIterator(
                URI.create(targetPath),
                ImmutableSet.of(ADD),
                Optional.of(metadataEntry),
                Optional.of(protocolEntry),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        partitionKeyField,
                        Domain.create(ValueSet.ofRanges(range(INTEGER, 9L, true, 11L, true)), false))),
                Optional.of(alwaysTrue()));

        assertThat(Iterators.size(metadataAndProtocolEntryIterator)).isEqualTo(2);
        assertThat(metadataAndProtocolEntryIterator.getCompletedPositions().orElseThrow()).isEqualTo(3L);

        assertThat(Iterators.size(addEntryIterator)).isEqualTo(numAddEntries);
        assertThat(addEntryIterator.getCompletedPositions().orElseThrow()).isEqualTo(101L);

        assertThat(Iterators.size(addEntryIteratorWithEqualityPartitionFilter)).isEqualTo(1);
        assertThat(addEntryIteratorWithEqualityPartitionFilter.getCompletedPositions().orElseThrow()).isEqualTo(1L);

        assertThat(Iterators.size(addEntryIteratorWithRangePartitionFilter)).isEqualTo(3);
        assertThat(addEntryIteratorWithRangePartitionFilter.getCompletedPositions().orElseThrow()).isEqualTo(3L);
    }

    @Test
    public void testSkipAddEntriesThroughComposedPartitionPruning()
            throws IOException
    {
        MetadataEntry metadataEntry = new MetadataEntry(
                "metadataId",
                "metadataName",
                "metadataDescription",
                new MetadataEntry.Format(
                        "metadataFormatProvider",
                        ImmutableMap.of()),
                "{\"type\":\"struct\",\"fields\":" +
                        "[{\"name\":\"ts\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"part_day\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"part_hour\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
                ImmutableList.of(
                        "part_day",
                        "part_hour"),
                ImmutableMap.of(),
                1000);
        ProtocolEntry protocolEntry = new ProtocolEntry(1, 1, Optional.empty(), Optional.empty());
        LocalDateTime date = LocalDateTime.of(2023, 12, 1, 0, 0);
        DateTimeFormatter dayFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        DateTimeFormatter hourFormatter = DateTimeFormatter.ofPattern("HH");
        int numAddEntries = 100;
        Set<AddFileEntry> addFileEntries = IntStream.rangeClosed(1, numAddEntries).mapToObj(index ->
                        new AddFileEntry(
                                "addFilePath",
                                ImmutableMap.of(
                                        "part_day", date.plusHours(index).format(dayFormatter),
                                        "part_hour", date.plusHours(index).format(hourFormatter)),
                                1000,
                                1001,
                                true,
                                Optional.of("{" +
                                        "\"numRecords\":20," +
                                        "\"minValues\":{" +
                                        "\"ts\":\"1990-10-31T01:00:00.000Z\"" +
                                        "}," +
                                        "\"maxValues\":{" +
                                        "\"ts\":\"1990-10-31T02:00:00.000Z\"" +
                                        "}," +
                                        "\"nullCount\":{" +
                                        "\"ts\":1" +
                                        "}}"),
                                Optional.empty(),
                                ImmutableMap.of(),
                                Optional.empty()))
                .collect(toImmutableSet());

        CheckpointEntries entries = new CheckpointEntries(
                metadataEntry,
                protocolEntry,
                ImmutableSet.of(),
                addFileEntries,
                ImmutableSet.of());

        CheckpointWriter writer = new CheckpointWriter(
                TESTING_TYPE_MANAGER,
                checkpointSchemaManager,
                "test",
                ParquetWriterOptions.builder() // approximately 2 rows per row group
                        .setMaxBlockSize(DataSize.ofBytes(128L))
                        .setMaxPageSize(DataSize.ofBytes(128L))
                        .build());

        File targetFile = File.createTempFile("testSkipAddEntries-", ".checkpoint.parquet");
        targetFile.deleteOnExit();

        String targetPath = "file://" + targetFile.getAbsolutePath();
        targetFile.delete(); // file must not exist when writer is called
        writer.write(entries, createOutputFile(targetPath));

        CheckpointEntryIterator metadataAndProtocolEntryIterator = createCheckpointEntryIterator(
                URI.create(targetPath),
                ImmutableSet.of(METADATA, PROTOCOL),
                Optional.empty(),
                Optional.empty(),
                TupleDomain.all(),
                Optional.empty());

        DeltaLakeColumnHandle partitionDayField = new DeltaLakeColumnHandle(
                "part_day",
                VARCHAR,
                OptionalInt.empty(),
                "part_day",
                VARCHAR,
                REGULAR,
                Optional.empty());
        DeltaLakeColumnHandle partitionHourField = new DeltaLakeColumnHandle(
                "part_hour",
                VARCHAR,
                OptionalInt.empty(),
                "part_hour",
                VARCHAR,
                REGULAR,
                Optional.empty());

        CheckpointEntryIterator addEntryIterator = createCheckpointEntryIterator(
                URI.create(targetPath),
                ImmutableSet.of(ADD),
                Optional.of(metadataEntry),
                Optional.of(protocolEntry),
                TupleDomain.all(),
                Optional.of(alwaysTrue()));

        CheckpointEntryIterator addEntryIteratorEqualityDayPartitionFilter = createCheckpointEntryIterator(
                URI.create(targetPath),
                ImmutableSet.of(ADD),
                Optional.of(metadataEntry),
                Optional.of(protocolEntry),
                TupleDomain.withColumnDomains(ImmutableMap.of(partitionDayField, singleValue(VARCHAR, utf8Slice("20231202")))),
                Optional.of(alwaysTrue()));

        CheckpointEntryIterator addEntryIteratorWithDayAndHourEqualityPartitionFilter = createCheckpointEntryIterator(
                URI.create(targetPath),
                ImmutableSet.of(ADD),
                Optional.of(metadataEntry),
                Optional.of(protocolEntry),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        partitionDayField, singleValue(VARCHAR, utf8Slice("20231202")),
                        partitionHourField, singleValue(VARCHAR, utf8Slice("10")))),
                Optional.of(alwaysTrue()));

        assertThat(Iterators.size(metadataAndProtocolEntryIterator)).isEqualTo(2);
        assertThat(metadataAndProtocolEntryIterator.getCompletedPositions().orElseThrow()).isEqualTo(3L);

        assertThat(Iterators.size(addEntryIterator)).isEqualTo(numAddEntries);
        assertThat(addEntryIterator.getCompletedPositions().orElseThrow()).isEqualTo(101L);

        assertThat(Iterators.size(addEntryIteratorEqualityDayPartitionFilter)).isEqualTo(24);
        assertThat(addEntryIteratorEqualityDayPartitionFilter.getCompletedPositions().orElseThrow()).isEqualTo(24L);

        assertThat(Iterators.size(addEntryIteratorWithDayAndHourEqualityPartitionFilter)).isEqualTo(1);
        assertThat(addEntryIteratorWithDayAndHourEqualityPartitionFilter.getCompletedPositions().orElseThrow()).isEqualTo(1L);
    }

    @Test
    public void testSkipRemoveEntries()
            throws IOException
    {
        MetadataEntry metadataEntry = new MetadataEntry(
                "metadataId",
                "metadataName",
                "metadataDescription",
                new MetadataEntry.Format(
                        "metadataFormatProvider",
                        ImmutableMap.of()),
                "{\"type\":\"struct\",\"fields\":" +
                        "[{\"name\":\"ts\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}}," +
                        "{\"name\":\"part_key\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}}]}",
                ImmutableList.of("part_key"),
                ImmutableMap.of(),
                1000);
        ProtocolEntry protocolEntry = new ProtocolEntry(10, 20, Optional.empty(), Optional.empty());
        AddFileEntry addFileEntryJsonStats = new AddFileEntry(
                "addFilePathJson",
                ImmutableMap.of("part_key", "2023-01-01 00:00:00"),
                1000,
                1001,
                true,
                Optional.of("{" +
                        "\"numRecords\":20," +
                        "\"minValues\":{" +
                        "\"ts\":\"2960-10-31T01:00:00.000Z\"" +
                        "}," +
                        "\"maxValues\":{" +
                        "\"ts\":\"2960-10-31T02:00:00.000Z\"" +
                        "}," +
                        "\"nullCount\":{" +
                        "\"ts\":1" +
                        "}}"),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());

        int numRemoveEntries = 100;
        Set<RemoveFileEntry> removeEntries = IntStream.range(0, numRemoveEntries).mapToObj(x ->
                        new RemoveFileEntry(
                                UUID.randomUUID().toString(),
                                ImmutableMap.of("part_key", "2023-01-01 00:00:00"),
                                1000,
                                true,
                                Optional.empty()))
                .collect(toImmutableSet());

        CheckpointEntries entries = new CheckpointEntries(
                metadataEntry,
                protocolEntry,
                ImmutableSet.of(),
                ImmutableSet.of(addFileEntryJsonStats),
                removeEntries);

        CheckpointWriter writer = new CheckpointWriter(
                TESTING_TYPE_MANAGER,
                checkpointSchemaManager,
                "test",
                ParquetWriterOptions.builder() // approximately 2 rows per row group
                        .setMaxBlockSize(DataSize.ofBytes(64L))
                        .setMaxPageSize(DataSize.ofBytes(64L))
                        .build());

        File targetFile = File.createTempFile("testSkipRemoveEntries-", ".checkpoint.parquet");
        targetFile.deleteOnExit();

        String targetPath = "file://" + targetFile.getAbsolutePath();
        targetFile.delete(); // file must not exist when writer is called
        writer.write(entries, createOutputFile(targetPath));

        CheckpointEntryIterator metadataAndProtocolEntryIterator =
                createCheckpointEntryIterator(URI.create(targetPath), ImmutableSet.of(METADATA, PROTOCOL), Optional.empty(), Optional.empty(), TupleDomain.all(), Optional.empty());
        CheckpointEntryIterator addEntryIterator = createCheckpointEntryIterator(
                URI.create(targetPath),
                ImmutableSet.of(ADD),
                Optional.of(metadataEntry),
                Optional.of(protocolEntry),
                TupleDomain.all(),
                Optional.of(alwaysTrue()));
        CheckpointEntryIterator removeEntryIterator =
                createCheckpointEntryIterator(URI.create(targetPath), ImmutableSet.of(REMOVE), Optional.empty(), Optional.empty(), TupleDomain.all(), Optional.empty());
        CheckpointEntryIterator txnEntryIterator =
                createCheckpointEntryIterator(URI.create(targetPath), ImmutableSet.of(TRANSACTION), Optional.empty(), Optional.empty(), TupleDomain.all(), Optional.empty());

        assertThat(Iterators.size(metadataAndProtocolEntryIterator)).isEqualTo(2);
        assertThat(Iterators.size(addEntryIterator)).isEqualTo(1);
        assertThat(Iterators.size(removeEntryIterator)).isEqualTo(numRemoveEntries);
        assertThat(Iterators.size(txnEntryIterator)).isEqualTo(0);

        assertThat(metadataAndProtocolEntryIterator.getCompletedPositions().orElseThrow()).isEqualTo(3L);
        assertThat(addEntryIterator.getCompletedPositions().orElseThrow()).isEqualTo(2L);
        assertThat(removeEntryIterator.getCompletedPositions().orElseThrow()).isEqualTo(100L);
        assertThat(txnEntryIterator.getCompletedPositions().orElseThrow()).isEqualTo(0L);
    }

    private MetadataEntry readMetadataEntry(URI checkpointUri)
            throws IOException
    {
        CheckpointEntryIterator checkpointEntryIterator = createCheckpointEntryIterator(
                checkpointUri,
                ImmutableSet.of(METADATA),
                Optional.empty(),
                Optional.empty(),
                TupleDomain.all(),
                Optional.empty());
        return Iterators.getOnlyElement(checkpointEntryIterator).getMetaData();
    }

    private ProtocolEntry readProtocolEntry(URI checkpointUri)
            throws IOException
    {
        CheckpointEntryIterator checkpointEntryIterator = createCheckpointEntryIterator(
                checkpointUri,
                ImmutableSet.of(PROTOCOL),
                Optional.empty(),
                Optional.empty(),
                TupleDomain.all(),
                Optional.empty());
        return Iterators.getOnlyElement(checkpointEntryIterator).getProtocol();
    }

    private CheckpointEntryIterator createCheckpointEntryIterator(
            URI checkpointUri,
            Set<CheckpointEntryIterator.EntryType> entryTypes,
            Optional<MetadataEntry> metadataEntry,
            Optional<ProtocolEntry> protocolEntry,
            TupleDomain<DeltaLakeColumnHandle> partitionConstraint,
            Optional<Predicate<String>> addStatsMinMaxColumnFilter)
            throws IOException
    {
        TrinoFileSystem fileSystem = new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS).create(SESSION);
        TrinoInputFile checkpointFile = fileSystem.newInputFile(Location.of(checkpointUri.toString()));

        return new CheckpointEntryIterator(
                checkpointFile,
                SESSION,
                checkpointFile.length(),
                checkpointSchemaManager,
                TESTING_TYPE_MANAGER,
                entryTypes,
                metadataEntry,
                protocolEntry,
                new FileFormatDataSourceStats(),
                new ParquetReaderConfig()
                        .setMaxBufferSize(DataSize.ofBytes(500))
                        .setSmallFileThreshold(DataSize.of(1, KILOBYTE))
                        .toParquetReaderOptions(),
                true,
                new DeltaLakeConfig().getDomainCompactionThreshold(),
                partitionConstraint,
                addStatsMinMaxColumnFilter);
    }

    private static TrinoOutputFile createOutputFile(String path)
    {
        return new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS).create(SESSION).newOutputFile(Location.of(path));
    }
}
