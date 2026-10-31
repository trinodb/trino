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
import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.Location;
import io.trino.filesystem.local.LocalFileSystem;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.spi.NodeVersion;
import org.apache.iceberg.BasePartitionStatistics;
import org.apache.iceberg.InternalData;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionStatistics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.iceberg.IcebergTestUtils.readWithLibrary;
import static io.trino.plugin.iceberg.IcebergTestUtils.writeWithLibrary;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestTrinoInternalData
{
    // every primitive type, as partition fields can have them
    private static final Types.StructType PARTITION_TYPE = Types.StructType.of(
            optional(1000, "boolean_value", Types.BooleanType.get()),
            optional(1001, "int_value", Types.IntegerType.get()),
            optional(1002, "long_value", Types.LongType.get()),
            optional(1003, "float_value", Types.FloatType.get()),
            optional(1004, "double_value", Types.DoubleType.get()),
            // stored as INT32, INT64 and FIXED_LEN_BYTE_ARRAY in Parquet
            optional(1005, "decimal_9", Types.DecimalType.of(9, 2)),
            optional(1006, "decimal_18", Types.DecimalType.of(18, 3)),
            optional(1007, "decimal_38", Types.DecimalType.of(38, 4)),
            optional(1008, "date_value", Types.DateType.get()),
            optional(1009, "time_value", Types.TimeType.get()),
            optional(1010, "timestamp_value", Types.TimestampType.withoutZone()),
            optional(1011, "timestamptz_value", Types.TimestampType.withZone()),
            optional(1012, "timestamp_ns_value", Types.TimestampNanoType.withoutZone()),
            optional(1013, "timestamptz_ns_value", Types.TimestampNanoType.withZone()),
            optional(1014, "string_value", Types.StringType.get()),
            optional(1015, "uuid_value", Types.UUIDType.get()),
            optional(1016, "fixed_value", Types.FixedType.ofLength(3)),
            optional(1017, "binary_value", Types.BinaryType.get()));
    private static final Schema SCHEMA = PartitionStatistics.schema(PARTITION_TYPE, 3);
    private static final List<StructLike> RECORDS = ImmutableList.of(
            record(
                    partition(true,
                            1,
                            2L,
                            1.5f,
                            2.5,
                            new BigDecimal("12.34"),
                            new BigDecimal("123456789012.345"),
                            new BigDecimal("1234567890123456789012345678.9012"),
                            19787,
                            37_872_123_456L,
                            1_709_633_472_123_456L,
                            1_709_633_472_123_456L,
                            1_709_633_472_123_456_789L,
                            1_709_633_472_123_456_789L,
                            "x",
                            UUID.fromString("12151fd2-7586-11e9-8f9e-2a86e4085a59"),
                            ByteBuffer.wrap(new byte[] {1, 2, 3}),
                            ByteBuffer.wrap(new byte[] {4, 5})),
                    0,
                    10,
                    1,
                    100,
                    0,
                    0,
                    1000L,
                    5L,
                    0),
            // negative values and values before the epoch
            record(
                    partition(false,
                            -1,
                            -2L,
                            -1.5f,
                            -2.5,
                            new BigDecimal("-0.05"),
                            new BigDecimal("-0.005"),
                            new BigDecimal("-0.0005"),
                            -1,
                            0L,
                            -1_000_001L,
                            -1_000_001L,
                            -1_000_000_001L,
                            -1_000_000_001L,
                            "",
                            UUID.fromString("00000000-0000-0000-0000-000000000000"),
                            ByteBuffer.wrap(new byte[] {0, 0, 0}),
                            ByteBuffer.wrap(new byte[0])),
                    1,
                    20,
                    2,
                    200,
                    1,
                    1,
                    null,
                    null,
                    0),
            record(partition(new Object[PARTITION_TYPE.fields().size()]), 0, 30, 3, 300, 2, 0, 3000L, 7L, 1));

    private static final Schema TIMESTAMP_SCHEMA = new Schema(
            optional(1, "timestamp_value", Types.TimestampType.withoutZone()),
            optional(2, "timestamp_tz_value", Types.TimestampType.withZone()),
            optional(3, "timestamp_nano_value", Types.TimestampNanoType.withoutZone()),
            optional(4, "timestamp_nano_tz_value", Types.TimestampNanoType.withZone()));

    @BeforeAll
    static void registerTrinoInternalData()
    {
        // as the connector does
        new IcebergInternalData(TESTING_TYPE_MANAGER, new NodeVersion("test"));
    }

    @Test
    void testRoundTrip(@TempDir Path root)
            throws IOException
    {
        FileIO fileIo = new ForwardingFileIo(new LocalFileSystem(root), false);

        OutputFile trinoFile = fileIo.newOutputFile("local:///trino.parquet");
        // the library uses Trino's writer and reader
        assertThat(InternalData.write(PARQUET, trinoFile)).isInstanceOf(IcebergInternalDataWriteBuilder.class);
        assertThat(InternalData.read(PARQUET, trinoFile.toInputFile())).isInstanceOf(IcebergInternalDataReadBuilder.class);
        write(trinoFile, SCHEMA, RECORDS);
        assertThat(values(read(trinoFile.toInputFile(), SCHEMA))).isEqualTo(values(RECORDS));
        // the library's own readers, which Spark uses, read what Trino writes
        assertThat(values(readWithLibrary(PARQUET, trinoFile.toInputFile(), SCHEMA))).isEqualTo(values(RECORDS));

        // and Trino reads what the library's own writers write
        OutputFile libraryFile = fileIo.newOutputFile("local:///library.parquet");
        writeWithLibrary(PARQUET, libraryFile, SCHEMA, RECORDS);
        assertThat(values(read(libraryFile.toInputFile(), SCHEMA))).isEqualTo(values(RECORDS));
    }

    @Test
    void testCustomTypes(@TempDir Path root)
            throws IOException
    {
        FileIO fileIo = new ForwardingFileIo(new LocalFileSystem(root), false);
        OutputFile file = fileIo.newOutputFile("local:///file.parquet");
        writeWithLibrary(PARQUET, file, SCHEMA, RECORDS);

        // the types the library's partition statistics scan reads with, and expects
        List<StructLike> statistics = read(InternalData.read(PARQUET, file.toInputFile()).project(SCHEMA).setRootType(BasePartitionStatistics.class));
        assertThat(statistics).allSatisfy(struct -> {
            assertThat(struct).isInstanceOf(PartitionStatistics.class);
            assertThat(((PartitionStatistics) struct).partition()).isInstanceOf(GenericRecord.class);
        });
        assertThat(statistics.stream().map(struct -> ((PartitionStatistics) struct).dataRecordCount()).collect(toImmutableList())).containsExactly(10L, 20L, 30L);
        assertThat(values(statistics)).isEqualTo(values(RECORDS));

        List<StructLike> withCustomPartition = read(InternalData.read(PARQUET, file.toInputFile())
                .project(SCHEMA)
                .setCustomType(PartitionStatistics.EMPTY_PARTITION_FIELD.fieldId(), PartitionData.class));
        assertThat(withCustomPartition).allSatisfy(struct -> assertThat(struct.get(0, Object.class)).isInstanceOf(PartitionData.class));
    }

    @Test
    void testProjection(@TempDir Path root)
            throws IOException
    {
        FileIO fileIo = new ForwardingFileIo(new LocalFileSystem(root), false);
        OutputFile file = fileIo.newOutputFile("local:///file.parquet");
        // a file written for a format version 2 table, which has no deletion vector count
        Schema formatVersion2Schema = PartitionStatistics.schema(PARTITION_TYPE, 2);
        write(file, formatVersion2Schema, RECORDS.stream()
                .map(record -> project(formatVersion2Schema, record))
                .collect(toImmutableList()));

        // a nested field and a top level field
        Schema projection = TypeUtil.select(SCHEMA, ImmutableSet.of(1014, PartitionStatistics.DATA_RECORD_COUNT.fieldId()));
        assertThat(values(read(file.toInputFile(), projection))).isEqualTo(ImmutableList.of(
                ImmutableList.of(Arrays.asList("x"), 10L),
                ImmutableList.of(Arrays.asList(""), 20L),
                ImmutableList.of(Arrays.asList((Object) null), 30L)));

        // a field the file does not have, read with its initial default value
        Schema withDeletionVectorCount = TypeUtil.select(SCHEMA, ImmutableSet.of(PartitionStatistics.SPEC_ID.fieldId(), PartitionStatistics.DV_COUNT.fieldId()));
        assertThat(values(read(file.toInputFile(), withDeletionVectorCount))).isEqualTo(ImmutableList.of(
                ImmutableList.of(0, 0),
                ImmutableList.of(1, 0),
                ImmutableList.of(0, 0)));
    }

    @Test
    void testSplit(@TempDir Path root)
            throws IOException
    {
        FileIO fileIo = new ForwardingFileIo(new LocalFileSystem(root), false);
        OutputFile file = fileIo.newOutputFile("local:///file.parquet");
        write(file, SCHEMA, RECORDS);
        long length = file.toInputFile().getLength();

        // the only row group or stripe starts right after the magic bytes at the start of the file
        assertThat(read(InternalData.read(PARQUET, file.toInputFile()).project(SCHEMA).split(0, 8))).hasSize(RECORDS.size());
        assertThat(read(InternalData.read(PARQUET, file.toInputFile()).project(SCHEMA).split(8, length - 8))).isEmpty();
    }

    @Test
    void testByteBuffers(@TempDir Path root)
            throws IOException
    {
        FileIO fileIo = new ForwardingFileIo(new LocalFileSystem(root), false);
        OutputFile file = fileIo.newOutputFile("local:///file.parquet");
        Schema schema = new Schema(optional(1, "binary_value", Types.BinaryType.get()), optional(2, "fixed_value", Types.FixedType.ofLength(3)));
        ByteBuffer directBuffer = ByteBuffer.allocateDirect(3).put(new byte[] {7, 8, 9}).flip();
        List<StructLike> records = ImmutableList.of(
                // a slice of a larger array, with a non-zero position and limit
                binaryRecord(schema, ByteBuffer.wrap(new byte[] {9, 1, 2, 3, 9}, 1, 3), ByteBuffer.wrap(new byte[] {9, 4, 5, 6, 9}, 1, 3)),
                binaryRecord(schema, ByteBuffer.wrap(new byte[] {1, 2, 3}).asReadOnlyBuffer(), ByteBuffer.wrap(new byte[] {4, 5, 6}).asReadOnlyBuffer()),
                binaryRecord(schema, directBuffer, directBuffer.duplicate()));
        write(file, schema, records);

        List<List<Object>> expected = ImmutableList.of(
                ImmutableList.of(ByteBuffer.wrap(new byte[] {1, 2, 3}), ByteBuffer.wrap(new byte[] {4, 5, 6})),
                ImmutableList.of(ByteBuffer.wrap(new byte[] {1, 2, 3}), ByteBuffer.wrap(new byte[] {4, 5, 6})),
                ImmutableList.of(ByteBuffer.wrap(new byte[] {7, 8, 9}), ByteBuffer.wrap(new byte[] {7, 8, 9})));
        assertThat(values(readWithLibrary(PARQUET, file.toInputFile(), schema))).isEqualTo(expected);
        assertThat(values(read(file.toInputFile(), schema))).isEqualTo(expected);
    }

    @Test
    void testParquetTimestampsBeforeEpoch(@TempDir Path root)
            throws IOException
    {
        FileIO fileIo = new ForwardingFileIo(new LocalFileSystem(root), false);
        OutputFile file = fileIo.newOutputFile("local:///file.parquet");
        // the last second before the epoch
        List<StructLike> records = timestampRecords(-1L, -1L, -1L, -1L);
        write(file, TIMESTAMP_SCHEMA, records);

        assertThat(values(read(file.toInputFile(), TIMESTAMP_SCHEMA))).isEqualTo(values(records));
        assertThat(values(readWithLibrary(PARQUET, file.toInputFile(), TIMESTAMP_SCHEMA))).isEqualTo(values(records));
    }

    @Test
    void testFileMetadata(@TempDir Path root)
            throws IOException
    {
        FileIO fileIo = new ForwardingFileIo(new LocalFileSystem(root), false);
        // Trino's Parquet writer does not write key-value metadata
        assertThatThrownBy(() -> InternalData.write(PARQUET, fileIo.newOutputFile("local:///file.parquet")).meta("key", "value"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Trino's Parquet writer does not write file metadata: key");
    }

    @Test
    void testFailedAppendDoesNotProduceReadableFile(@TempDir Path root)
            throws IOException
    {
        FileIO fileIo = new ForwardingFileIo(new LocalFileSystem(root), false);
        String location = "local:///failed.parquet";
        OutputFile file = fileIo.newOutputFile(location);
        StructLike failing = new StructLike()
        {
            @Override
            public int size()
            {
                return SCHEMA.columns().size();
            }

            @Override
            public <T> T get(int position, Class<T> javaClass)
            {
                throw new IllegalStateException("record failure");
            }

            @Override
            public <T> void set(int position, T value)
            {
                throw new UnsupportedOperationException();
            }
        };

        FileAppender<StructLike> appender = InternalData.write(PARQUET, file).schema(SCHEMA).build();
        appender.add(RECORDS.get(0));
        assertThatThrownBy(() -> appender.add(failing))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("record failure");
        // further records are refused, as the page builder is half written
        assertThatThrownBy(() -> appender.add(RECORDS.get(0)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("appender failed to add a record");
        assertThatThrownBy(appender::close)
                .isInstanceOf(IOException.class)
                .hasMessage("Appender failed to add a record, the file is incomplete");

        // the file is absent, empty, or not readable
        if (new LocalFileSystem(root).newInputFile(Location.of(location)).exists()) {
            assertThatThrownBy(() -> readWithLibrary(PARQUET, fileIo.newInputFile(location), SCHEMA)).isNotNull();
        }
        // the writer construction failing is not reachable without a faulty file system: the writers are created from the
        // schema, which is validated before
    }

    @Test
    void testListsAndMapsAreNotSupported(@TempDir Path root)
    {
        FileIO fileIo = new ForwardingFileIo(new LocalFileSystem(root), false);
        Schema schema = new Schema(
                optional(1, "nested", Types.StructType.of(optional(2, "list", Types.ListType.ofOptional(3, Types.IntegerType.get())))));
        String location = "local:///file.parquet";
        assertThatThrownBy(() -> InternalData.write(PARQUET, fileIo.newOutputFile(location)).schema(schema).build())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Unsupported type in internal data: list: list<int>");
        assertThatThrownBy(() -> InternalData.read(PARQUET, fileIo.newInputFile(location)).project(schema).build())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Unsupported type in internal data: list: list<int>");
    }

    /**
     * Writes the partition statistics files the lakehouse tests register, with the library's own writers, which Spark
     * uses: {@code plugin/trino-lakehouse/src/test/resources/partition-stats/}.
     */
    static void main(String[] args)
            throws IOException
    {
        Path directory = Path.of(args.length > 0 ? args[0] : "plugin/trino-lakehouse/src/test/resources/partition-stats");
        // a format version 2 table partitioned by an integer column part
        Types.StructType partitionType = Types.StructType.of(optional(1000, "part", Types.IntegerType.get()));
        Schema schema = PartitionStatistics.schema(partitionType, 2);
        List<StructLike> records = ImmutableList.of(
                project(schema, record(partition(partitionType, 1), 0, 1000, 1, 10000, 0, 0, null, null, 0)),
                project(schema, record(partition(partitionType, 2), 0, 2000, 1, 20000, 0, 0, null, null, 0)));
        FileIO fileIo = new ForwardingFileIo(new LocalFileSystem(directory), false);
        String location = "local:///partition-stats.parquet";
        fileIo.deleteFile(location);
        writeWithLibrary(PARQUET, fileIo.newOutputFile(location), schema, records);
    }

    private static void write(OutputFile file, Schema schema, List<StructLike> records)
            throws IOException
    {
        try (FileAppender<StructLike> appender = InternalData.write(PARQUET, file).schema(schema).build()) {
            appender.addAll(records);
        }
    }

    private static List<StructLike> read(InputFile file, Schema projection)
            throws IOException
    {
        return read(InternalData.read(PARQUET, file).project(projection));
    }

    private static List<StructLike> read(InternalData.ReadBuilder builder)
            throws IOException
    {
        try (CloseableIterable<StructLike> records = builder.build()) {
            return ImmutableList.copyOf(records);
        }
    }

    private static StructLike project(Schema schema, StructLike record)
    {
        GenericRecord projected = GenericRecord.create(schema);
        for (int i = 0; i < schema.columns().size(); i++) {
            projected.set(i, record.get(SCHEMA.columns().indexOf(SCHEMA.findField(schema.columns().get(i).fieldId())), Object.class));
        }
        return projected;
    }

    private static StructLike record(
            StructLike partition,
            int specId,
            long dataRecordCount,
            int dataFileCount,
            long dataFileSize,
            long positionDeleteRecordCount,
            int positionDeleteFileCount,
            Long lastUpdatedAt,
            Long lastUpdatedSnapshotId,
            int deletionVectorCount)
    {
        GenericRecord record = GenericRecord.create(SCHEMA);
        record.set(0, partition);
        record.set(1, specId);
        record.set(2, dataRecordCount);
        record.set(3, dataFileCount);
        record.set(4, dataFileSize);
        record.set(5, positionDeleteRecordCount);
        record.set(6, positionDeleteFileCount);
        record.set(7, 0L);
        record.set(8, 0);
        // the total record count is not computed by the library
        record.set(9, null);
        record.set(10, lastUpdatedAt);
        record.set(11, lastUpdatedSnapshotId);
        record.set(12, deletionVectorCount);
        return record;
    }

    private static StructLike binaryRecord(Schema schema, ByteBuffer binary, ByteBuffer fixed)
    {
        GenericRecord record = GenericRecord.create(schema);
        record.set(0, binary);
        record.set(1, fixed);
        return record;
    }

    private static List<StructLike> timestampRecords(long... values)
    {
        GenericRecord record = GenericRecord.create(TIMESTAMP_SCHEMA);
        for (int i = 0; i < values.length; i++) {
            record.set(i, values[i]);
        }
        return ImmutableList.of(record);
    }

    private static StructLike partition(Object... values)
    {
        return partition(PARTITION_TYPE, values);
    }

    private static StructLike partition(Types.StructType partitionType, Object... values)
    {
        GenericRecord partition = GenericRecord.create(partitionType);
        for (int i = 0; i < values.length; i++) {
            partition.set(i, values[i]);
        }
        return partition;
    }

    // the values of records, with nested records as lists
    private static List<List<Object>> values(List<StructLike> records)
    {
        return records.stream()
                .map(TestTrinoInternalData::values)
                .collect(toImmutableList());
    }

    private static List<Object> values(StructLike struct)
    {
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < struct.size(); i++) {
            Object value = struct.get(i, Object.class);
            if (value instanceof StructLike nested) {
                value = values(nested);
            }
            values.add(value);
        }
        return values;
    }
}
