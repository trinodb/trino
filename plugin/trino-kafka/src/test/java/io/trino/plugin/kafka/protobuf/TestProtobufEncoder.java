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
package io.trino.plugin.kafka.protobuf;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.decoder.protobuf.FileDescriptorProvider;
import io.trino.decoder.protobuf.FixedSchemaDynamicMessageProvider;
import io.trino.decoder.protobuf.ProtobufRowDecoder;
import io.trino.plugin.kafka.KafkaColumnHandle;
import io.trino.plugin.kafka.encoder.EncoderColumnHandle;
import io.trino.plugin.kafka.encoder.RowEncoder;
import io.trino.plugin.kafka.encoder.RowEncoderSpec;
import io.trino.plugin.kafka.encoder.protobuf.ProtobufRowEncoder;
import io.trino.plugin.kafka.encoder.protobuf.ProtobufRowEncoderFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.Type;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.decoder.protobuf.ProtobufRowDecoderFactory.DEFAULT_MESSAGE;
import static io.trino.decoder.protobuf.ProtobufUtils.getFileDescriptor;
import static io.trino.decoder.protobuf.ProtobufUtils.getProtoFile;
import static io.trino.plugin.kafka.encoder.KafkaFieldType.MESSAGE;
import static io.trino.spi.StandardErrorCode.CONSTRAINT_VIOLATION;
import static io.trino.spi.block.ArrayBlock.fromElementBlock;
import static io.trino.spi.block.RowBlock.fromFieldBlocks;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.Float.floatToIntBits;
import static java.lang.Math.PI;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestProtobufEncoder
{
    private static final ProtobufRowEncoderFactory ENCODER_FACTORY = new ProtobufRowEncoderFactory();

    @Test
    public void testAllDataTypes()
            throws Exception
    {
        testAllDataTypes(
                "Trino",
                1,
                493857959588286460L,
                PI,
                3.14f,
                true,
                "ONE",
                sqlTimestampOf(3, LocalDateTime.parse("2020-12-12T15:35:45.923")),
                "X'65683F'".getBytes(UTF_8));

        testAllDataTypes(
                range(0, 5000)
                        .mapToObj(Integer::toString)
                        .collect(joining(", ")),
                Integer.MAX_VALUE,
                Long.MIN_VALUE,
                Double.MAX_VALUE,
                Float.MIN_VALUE,
                false,
                "ZERO",
                sqlTimestampOf(3, LocalDateTime.parse("1856-01-12T05:25:14.456")),
                new byte[0]);

        testAllDataTypes(
                range(5000, 10000)
                        .mapToObj(Integer::toString)
                        .collect(joining(", ")),
                Integer.MIN_VALUE,
                Long.MAX_VALUE,
                Double.NaN,
                Float.NEGATIVE_INFINITY,
                false,
                "ZERO",
                sqlTimestampOf(3, LocalDateTime.parse("0001-01-01T00:00:00.923")),
                "X'65683F'".getBytes(UTF_8));
    }

    private void testAllDataTypes(String stringData, Integer integerData, Long longData, Double doubleData, Float floatData, Boolean booleanData, String enumData, SqlTimestamp sqlTimestamp, byte[] bytesData)
            throws Exception
    {
        Descriptor descriptor = getDescriptor("all_datatypes.proto");
        DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(descriptor);
        messageBuilder.setField(descriptor.findFieldByName("stringColumn"), stringData);
        messageBuilder.setField(descriptor.findFieldByName("integerColumn"), integerData);
        messageBuilder.setField(descriptor.findFieldByName("longColumn"), longData);
        messageBuilder.setField(descriptor.findFieldByName("doubleColumn"), doubleData);
        messageBuilder.setField(descriptor.findFieldByName("floatColumn"), floatData);
        messageBuilder.setField(descriptor.findFieldByName("booleanColumn"), booleanData);
        messageBuilder.setField(descriptor.findFieldByName("numberColumn"), descriptor.findEnumTypeByName("Number").findValueByName(enumData));
        messageBuilder.setField(descriptor.findFieldByName("timestampColumn"), getTimestamp(sqlTimestamp));
        messageBuilder.setField(descriptor.findFieldByName("bytesColumn"), ByteString.copyFrom(bytesData));

        RowEncoder rowEncoder = createRowEncoder(
                "all_datatypes.proto",
                ImmutableList.of(
                        createEncoderColumnHandle("stringColumn", createVarcharType(100), "stringColumn"),
                        createEncoderColumnHandle("integerColumn", INTEGER, "integerColumn"),
                        createEncoderColumnHandle("longColumn", BIGINT, "longColumn"),
                        createEncoderColumnHandle("doubleColumn", DOUBLE, "doubleColumn"),
                        createEncoderColumnHandle("floatColumn", REAL, "floatColumn"),
                        createEncoderColumnHandle("booleanColumn", BOOLEAN, "booleanColumn"),
                        createEncoderColumnHandle("numberColumn", createVarcharType(4), "numberColumn"),
                        createEncoderColumnHandle("timestampColumn", createTimestampType(6), "timestampColumn"),
                        createEncoderColumnHandle("bytesColumn", VARBINARY, "bytesColumn")));

        rowEncoder.appendColumnValue(writeNativeValue(createVarcharType(5), utf8Slice(stringData)), 0);
        rowEncoder.appendColumnValue(writeNativeValue(INTEGER, integerData.longValue()), 0);
        rowEncoder.appendColumnValue(writeNativeValue(BIGINT, longData), 0);
        rowEncoder.appendColumnValue(writeNativeValue(DOUBLE, doubleData), 0);
        rowEncoder.appendColumnValue(writeNativeValue(REAL, (long) floatToIntBits(floatData)), 0);
        rowEncoder.appendColumnValue(writeNativeValue(BOOLEAN, booleanData), 0);
        rowEncoder.appendColumnValue(writeNativeValue(createVarcharType(4), utf8Slice(enumData)), 0);
        rowEncoder.appendColumnValue(writeNativeValue(createTimestampType(6), sqlTimestamp.getEpochMicros()), 0);
        rowEncoder.appendColumnValue(writeNativeValue(VARBINARY, wrappedBuffer(bytesData)), 0);

        assertThat(messageBuilder.build().toByteArray()).isEqualTo(rowEncoder.toByteArray());
    }

    @Test
    public void testStructuralDataTypes()
            throws Exception
    {
        testStructuralDataTypes(
                "Trino",
                1,
                493857959588286460L,
                PI,
                3.14f,
                true,
                "ONE",
                sqlTimestampOf(3, LocalDateTime.parse("2020-12-12T15:35:45.923")),
                "X'65683F'".getBytes(UTF_8));

        testStructuralDataTypes(
                range(0, 5000)
                        .mapToObj(Integer::toString)
                        .collect(joining(", ")),
                Integer.MAX_VALUE,
                Long.MIN_VALUE,
                Double.MAX_VALUE,
                Float.MIN_VALUE,
                false,
                "ZERO",
                sqlTimestampOf(3, LocalDateTime.parse("1856-01-12T05:25:14.456")),
                new byte[0]);

        testStructuralDataTypes(
                range(5000, 10000)
                        .mapToObj(Integer::toString)
                        .collect(joining(", ")),
                Integer.MIN_VALUE,
                Long.MAX_VALUE,
                Double.NaN,
                Float.NEGATIVE_INFINITY,
                false,
                "ZERO",
                sqlTimestampOf(3, LocalDateTime.parse("0001-01-01T00:00:00.923")),
                "X'65683F'".getBytes(UTF_8));
    }

    private void testStructuralDataTypes(String stringData, Integer integerData, Long longData, Double doubleData, Float floatData, Boolean booleanData, String enumData, SqlTimestamp sqlTimestamp, byte[] bytesData)
            throws Exception
    {
        Descriptor descriptor = getDescriptor("structural_datatypes.proto");
        DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(descriptor);

        messageBuilder.setField(descriptor.findFieldByName("list"), ImmutableList.of(stringData));

        Descriptor mapDescriptor = descriptor.findFieldByName("map").getMessageType();
        DynamicMessage.Builder mapBuilder = DynamicMessage.newBuilder(mapDescriptor);
        mapBuilder.setField(mapDescriptor.findFieldByName("key"), "Key");
        mapBuilder.setField(mapDescriptor.findFieldByName("value"), "Value");
        messageBuilder.setField(descriptor.findFieldByName("map"), ImmutableList.of(mapBuilder.build()));

        Descriptor rowDescriptor = descriptor.findFieldByName("row").getMessageType();
        DynamicMessage.Builder rowBuilder = DynamicMessage.newBuilder(rowDescriptor);
        rowBuilder.setField(rowDescriptor.findFieldByName("string_column"), stringData);
        rowBuilder.setField(rowDescriptor.findFieldByName("integer_column"), integerData);
        rowBuilder.setField(rowDescriptor.findFieldByName("long_column"), longData);
        rowBuilder.setField(rowDescriptor.findFieldByName("double_column"), doubleData);
        rowBuilder.setField(rowDescriptor.findFieldByName("float_column"), floatData);
        rowBuilder.setField(rowDescriptor.findFieldByName("boolean_column"), booleanData);
        rowBuilder.setField(rowDescriptor.findFieldByName("number_column"), descriptor.findEnumTypeByName("Number").findValueByName(enumData));
        rowBuilder.setField(rowDescriptor.findFieldByName("timestamp_column"), getTimestamp(sqlTimestamp));
        rowBuilder.setField(rowDescriptor.findFieldByName("bytes_column"), ByteString.copyFrom(bytesData));
        messageBuilder.setField(descriptor.findFieldByName("row"), rowBuilder.build());

        List<EncoderColumnHandle> columnHandles = ImmutableList.of(
                createEncoderColumnHandle("list", new ArrayType(createVarcharType(30000)), "list"),
                createEncoderColumnHandle("map", new MapType(VARCHAR, VARCHAR, TESTING_TYPE_MANAGER.getTypeOperators()), "map"),
                createEncoderColumnHandle(
                        "row",
                        RowType.from(ImmutableList.<RowType.Field>builder()
                                .add(RowType.field("string_column", createVarcharType(30000)))
                                .add(RowType.field("integer_column", INTEGER))
                                .add(RowType.field("long_column", BIGINT))
                                .add(RowType.field("double_column", DOUBLE))
                                .add(RowType.field("float_column", REAL))
                                .add(RowType.field("boolean_column", BOOLEAN))
                                .add(RowType.field("number_column", createVarcharType(4)))
                                .add(RowType.field("timestamp_column", createTimestampType(6)))
                                .add(RowType.field("bytes_column", VARBINARY))
                                .build()),
                        "row"));

        RowEncoder rowEncoder = createRowEncoder("structural_datatypes.proto", columnHandles.subList(0, 3));

        ArrayBlockBuilder arrayBlockBuilder = (ArrayBlockBuilder) columnHandles.get(0).getType()
                .createBlockBuilder(null, 1);
        arrayBlockBuilder.buildEntry(elementBuilder -> writeNativeValue(createVarcharType(5), elementBuilder, utf8Slice(stringData)));
        rowEncoder.appendColumnValue(arrayBlockBuilder.build(), 0);

        MapBlockBuilder mapBlockBuilder = (MapBlockBuilder) columnHandles.get(1).getType()
                .createBlockBuilder(null, 1);
        mapBlockBuilder.buildEntry((keyBuilder, valueBuilder) -> {
            writeNativeValue(VARCHAR, keyBuilder, utf8Slice("Key"));
            writeNativeValue(VARCHAR, valueBuilder, utf8Slice("Value"));
        });
        rowEncoder.appendColumnValue(mapBlockBuilder.build(), 0);

        RowBlockBuilder rowBlockBuilder = (RowBlockBuilder) columnHandles.get(2).getType()
                .createBlockBuilder(null, 1);
        rowBlockBuilder.buildEntry(fieldBuilders -> {
            writeNativeValue(VARCHAR, fieldBuilders.get(0), utf8Slice(stringData));
            writeNativeValue(INTEGER, fieldBuilders.get(1), integerData.longValue());
            writeNativeValue(BIGINT, fieldBuilders.get(2), longData);
            writeNativeValue(DOUBLE, fieldBuilders.get(3), doubleData);
            writeNativeValue(REAL, fieldBuilders.get(4), (long) floatToIntBits(floatData));
            writeNativeValue(BOOLEAN, fieldBuilders.get(5), booleanData);
            writeNativeValue(VARCHAR, fieldBuilders.get(6), utf8Slice(enumData));
            writeNativeValue(createTimestampType(6), fieldBuilders.get(7), sqlTimestamp.getEpochMicros());
            writeNativeValue(VARBINARY, fieldBuilders.get(8), wrappedBuffer(bytesData));
        });
        rowEncoder.appendColumnValue(rowBlockBuilder.build(), 0);

        assertThat(messageBuilder.build().toByteArray()).isEqualTo(rowEncoder.toByteArray());
    }

    @Test
    public void testNestedStructuralDataTypes()
            throws Exception
    {
        testNestedStructuralDataTypes(
                "Trino",
                1,
                493857959588286460L,
                PI,
                3.14f,
                true,
                "ONE",
                sqlTimestampOf(3, LocalDateTime.parse("2020-12-12T15:35:45.923")),
                "X'65683F'".getBytes(UTF_8));

        testNestedStructuralDataTypes(
                range(0, 5000)
                        .mapToObj(Integer::toString)
                        .collect(joining(", ")),
                Integer.MAX_VALUE,
                Long.MIN_VALUE,
                Double.MAX_VALUE,
                Float.MIN_VALUE,
                false,
                "ZERO",
                sqlTimestampOf(3, LocalDateTime.parse("1856-01-12T05:25:14.456")),
                new byte[0]);

        testNestedStructuralDataTypes(
                range(5000, 10000)
                        .mapToObj(Integer::toString)
                        .collect(joining(", ")),
                Integer.MIN_VALUE,
                Long.MAX_VALUE,
                Double.NaN,
                Float.NEGATIVE_INFINITY,
                false,
                "ZERO",
                sqlTimestampOf(3, LocalDateTime.parse("0001-01-01T00:00:00.923")),
                "X'65683F'".getBytes(UTF_8));
    }

    private void testNestedStructuralDataTypes(String stringData, Integer integerData, Long longData, Double doubleData, Float floatData, Boolean booleanData, String enumData, SqlTimestamp sqlTimestamp, byte[] bytesData)
            throws Exception
    {
        Descriptor descriptor = getDescriptor("structural_datatypes.proto");
        DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(descriptor);
        Descriptor nestedDescriptor = descriptor.findFieldByName("nested_row").getMessageType();
        DynamicMessage.Builder nestedMessageBuilder = DynamicMessage.newBuilder(nestedDescriptor);

        Descriptor rowDescriptor = nestedDescriptor.findFieldByName("row").getMessageType();
        DynamicMessage.Builder rowBuilder = DynamicMessage.newBuilder(rowDescriptor);
        rowBuilder.setField(rowDescriptor.findFieldByName("string_column"), stringData);
        rowBuilder.setField(rowDescriptor.findFieldByName("integer_column"), integerData);
        rowBuilder.setField(rowDescriptor.findFieldByName("long_column"), longData);
        rowBuilder.setField(rowDescriptor.findFieldByName("double_column"), doubleData);
        rowBuilder.setField(rowDescriptor.findFieldByName("float_column"), floatData);
        rowBuilder.setField(rowDescriptor.findFieldByName("boolean_column"), booleanData);
        rowBuilder.setField(rowDescriptor.findFieldByName("number_column"), descriptor.findEnumTypeByName("Number").findValueByName(enumData));
        rowBuilder.setField(rowDescriptor.findFieldByName("timestamp_column"), getTimestamp(sqlTimestamp));
        rowBuilder.setField(rowDescriptor.findFieldByName("bytes_column"), ByteString.copyFrom(bytesData));
        nestedMessageBuilder.setField(nestedDescriptor.findFieldByName("nested_list"), ImmutableList.of(rowBuilder.build()));

        Descriptor mapDescriptor = nestedDescriptor.findFieldByName("nested_map").getMessageType();
        DynamicMessage.Builder mapBuilder = DynamicMessage.newBuilder(mapDescriptor);
        mapBuilder.setField(mapDescriptor.findFieldByName("key"), "Key");
        mapBuilder.setField(mapDescriptor.findFieldByName("value"), rowBuilder.build());
        nestedMessageBuilder.setField(nestedDescriptor.findFieldByName("nested_map"), ImmutableList.of(mapBuilder.build()));

        nestedMessageBuilder.setField(nestedDescriptor.findFieldByName("row"), rowBuilder.build());

        messageBuilder.setField(descriptor.findFieldByName("nested_row"), nestedMessageBuilder.build());

        RowType rowType = RowType.from(ImmutableList.<RowType.Field>builder()
                .add(RowType.field("string_column", createVarcharType(30000)))
                .add(RowType.field("integer_column", INTEGER))
                .add(RowType.field("long_column", BIGINT))
                .add(RowType.field("double_column", DOUBLE))
                .add(RowType.field("float_column", REAL))
                .add(RowType.field("boolean_column", BOOLEAN))
                .add(RowType.field("number_column", createVarcharType(4)))
                .add(RowType.field("timestamp_column", createTimestampType(6)))
                .add(RowType.field("bytes_column", VARBINARY))
                .build());

        List<EncoderColumnHandle> columnHandles = ImmutableList.of(
                createEncoderColumnHandle(
                        "row",
                        RowType.from(ImmutableList.of(
                                RowType.field("nested_list", new ArrayType(rowType)),
                                RowType.field("nested_map", new MapType(VARCHAR, rowType, TESTING_TYPE_MANAGER.getTypeOperators())),
                                RowType.field("row", rowType))),
                        "nested_row"));

        RowEncoder rowEncoder = createRowEncoder("structural_datatypes.proto", columnHandles);

        RowBlockBuilder rowBlockBuilder = rowType.createBlockBuilder(null, 1);
        rowBlockBuilder.buildEntry(fieldBuilders -> {
            writeNativeValue(VARCHAR, fieldBuilders.get(0), utf8Slice(stringData));
            writeNativeValue(INTEGER, fieldBuilders.get(1), integerData.longValue());
            writeNativeValue(BIGINT, fieldBuilders.get(2), longData);
            writeNativeValue(DOUBLE, fieldBuilders.get(3), doubleData);
            writeNativeValue(REAL, fieldBuilders.get(4), (long) floatToIntBits(floatData));
            writeNativeValue(BOOLEAN, fieldBuilders.get(5), booleanData);
            writeNativeValue(VARCHAR, fieldBuilders.get(6), utf8Slice(enumData));
            writeNativeValue(createTimestampType(6), fieldBuilders.get(7), sqlTimestamp.getEpochMicros());
            writeNativeValue(VARBINARY, fieldBuilders.get(8), wrappedBuffer(bytesData));
        });

        RowType nestedRowType = (RowType) columnHandles.get(0).getType();

        MapType mapType = (MapType) nestedRowType.getFieldTypes().get(1);
        BlockBuilder mapBlockBuilder = mapType.createBlockBuilder(null, 1);
        Block mapBlock = mapType.createBlockFromKeyValue(
                Optional.empty(),
                new int[] {0, 1},
                writeNativeValue(VARCHAR, utf8Slice("Key")),
                rowBlockBuilder.build());
        mapBlockBuilder.append(mapBlock.getUnderlyingValueBlock(), mapBlock.getUnderlyingValuePosition(0));

        Type listType = nestedRowType.getFieldTypes().get(0);
        BlockBuilder listBlockBuilder = listType.createBlockBuilder(null, 1);
        Block arrayBlock = fromElementBlock(
                1,
                Optional.empty(),
                new int[] {0, rowBlockBuilder.getPositionCount()},
                rowBlockBuilder.build());
        listBlockBuilder.append(arrayBlock.getUnderlyingValueBlock(), arrayBlock.getUnderlyingValuePosition(0));

        BlockBuilder nestedBlockBuilder = nestedRowType.createBlockBuilder(null, 1);
        Block rowBlock = fromFieldBlocks(1, new Block[] {listBlockBuilder.build(), mapBlockBuilder.build(), rowBlockBuilder.build()});
        nestedBlockBuilder.append(rowBlock.getUnderlyingValueBlock(), rowBlock.getUnderlyingValuePosition(0));

        rowEncoder.appendColumnValue(nestedBlockBuilder.build(), 0);

        assertThat(messageBuilder.build().toByteArray()).isEqualTo(rowEncoder.toByteArray());
    }

    @Test
    public void testRowFlattening()
            throws Exception
    {
        testRowFlattening(
                "Trino",
                1,
                493857959588286460L,
                PI,
                3.14f,
                true,
                "ONE",
                sqlTimestampOf(3, LocalDateTime.parse("2020-12-12T15:35:45.923")),
                "X'65683F'".getBytes(UTF_8));

        testRowFlattening(
                range(0, 5000)
                        .mapToObj(Integer::toString)
                        .collect(joining(", ")),
                Integer.MAX_VALUE,
                Long.MIN_VALUE,
                Double.MAX_VALUE,
                Float.MIN_VALUE,
                false,
                "ZERO",
                sqlTimestampOf(3, LocalDateTime.parse("1856-01-12T05:25:14.456")),
                new byte[0]);

        testRowFlattening(
                range(5000, 10000)
                        .mapToObj(Integer::toString)
                        .collect(joining(", ")),
                Integer.MIN_VALUE,
                Long.MAX_VALUE,
                Double.NaN,
                Float.NEGATIVE_INFINITY,
                false,
                "ZERO",
                sqlTimestampOf(3, LocalDateTime.parse("0001-01-01T00:00:00.923")),
                "X'65683F'".getBytes(UTF_8));
    }

    private void testRowFlattening(String stringData, Integer integerData, Long longData, Double doubleData, Float floatData, Boolean booleanData, String enumData, SqlTimestamp sqlTimestamp, byte[] bytesData)
            throws Exception
    {
        Descriptor descriptor = getDescriptor("structural_datatypes.proto");
        DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(descriptor);

        Descriptor rowDescriptor = descriptor.findNestedTypeByName("Row");
        DynamicMessage.Builder rowBuilder = DynamicMessage.newBuilder(rowDescriptor);
        rowBuilder.setField(rowDescriptor.findFieldByName("string_column"), stringData);
        rowBuilder.setField(rowDescriptor.findFieldByName("integer_column"), integerData);
        rowBuilder.setField(rowDescriptor.findFieldByName("long_column"), longData);
        rowBuilder.setField(rowDescriptor.findFieldByName("double_column"), doubleData);
        rowBuilder.setField(rowDescriptor.findFieldByName("float_column"), floatData);
        rowBuilder.setField(rowDescriptor.findFieldByName("boolean_column"), booleanData);
        rowBuilder.setField(rowDescriptor.findFieldByName("number_column"), descriptor.findEnumTypeByName("Number").findValueByName(enumData));
        rowBuilder.setField(rowDescriptor.findFieldByName("timestamp_column"), getTimestamp(sqlTimestamp));
        rowBuilder.setField(rowDescriptor.findFieldByName("bytes_column"), ByteString.copyFrom(bytesData));
        messageBuilder.setField(descriptor.findFieldByName("row"), rowBuilder.build());

        RowEncoder rowEncoder = createRowEncoder(
                "structural_datatypes.proto",
                ImmutableList.of(
                        createEncoderColumnHandle("stringColumn", createVarcharType(100), "row/string_column"),
                        createEncoderColumnHandle("integerColumn", INTEGER, "row/integer_column"),
                        createEncoderColumnHandle("longColumn", BIGINT, "row/long_column"),
                        createEncoderColumnHandle("doubleColumn", DOUBLE, "row/double_column"),
                        createEncoderColumnHandle("floatColumn", REAL, "row/float_column"),
                        createEncoderColumnHandle("booleanColumn", BOOLEAN, "row/boolean_column"),
                        createEncoderColumnHandle("numberColumn", createVarcharType(4), "row/number_column"),
                        createEncoderColumnHandle("timestampColumn", createTimestampType(4), "row/timestamp_column"),
                        createEncoderColumnHandle("bytesColumn", VARBINARY, "row/bytes_column")));

        rowEncoder.appendColumnValue(writeNativeValue(createVarcharType(5), utf8Slice(stringData)), 0);
        rowEncoder.appendColumnValue(writeNativeValue(INTEGER, integerData.longValue()), 0);
        rowEncoder.appendColumnValue(writeNativeValue(BIGINT, longData), 0);
        rowEncoder.appendColumnValue(writeNativeValue(DOUBLE, doubleData), 0);
        rowEncoder.appendColumnValue(writeNativeValue(REAL, (long) floatToIntBits(floatData)), 0);
        rowEncoder.appendColumnValue(writeNativeValue(BOOLEAN, booleanData), 0);
        rowEncoder.appendColumnValue(writeNativeValue(createVarcharType(4), utf8Slice(enumData)), 0);
        rowEncoder.appendColumnValue(writeNativeValue(createTimestampType(6), sqlTimestamp.getEpochMicros()), 0);
        rowEncoder.appendColumnValue(writeNativeValue(VARBINARY, wrappedBuffer(bytesData)), 0);

        assertThat(messageBuilder.build().toByteArray()).isEqualTo(rowEncoder.toByteArray());
    }

    @Test
    public void testNullValues()
    {
        Descriptor descriptor = getFileDescriptor(
                """
                syntax = "proto3";
                message schema {
                  string name = 1;
                  optional int64 count = 2;
                  message Detail {
                    string note = 1;
                    optional int64 weight = 2;
                  }
                  Detail detail = 3;
                  repeated string tags = 4;
                  map<string, string> attributes = 5;
                }
                """).findMessageTypeByName(DEFAULT_MESSAGE);
        RowType detailType = RowType.from(ImmutableList.of(RowType.field("note", VARCHAR), RowType.field("weight", BIGINT)));
        ArrayType tagsType = new ArrayType(VARCHAR);
        MapType attributesType = new MapType(VARCHAR, VARCHAR, TESTING_TYPE_MANAGER.getTypeOperators());

        // fields that track presence are left unset by a NULL
        RowEncoder encoder = new ProtobufRowEncoder(descriptor, TestingConnectorSession.SESSION, ImmutableList.of(
                createEncoderColumnHandle("name", VARCHAR, "name"),
                createEncoderColumnHandle("count", BIGINT, "count"),
                createEncoderColumnHandle("detail", detailType, "detail")));
        encoder.appendColumnValue(nativeValueBlock(VARCHAR, utf8Slice("Trino")), 0);
        encoder.appendColumnValue(nullBlock(BIGINT), 0);
        encoder.appendColumnValue(nullBlock(detailType), 0);
        assertThat(encoder.toByteArray()).isEqualTo(DynamicMessage.newBuilder(descriptor).setField(descriptor.findFieldByName("name"), "Trino").build().toByteArray());

        // so are presence-tracking fields nested in rows, whether the row is a column or flattened into columns
        Descriptor detailDescriptor = descriptor.findFieldByName("detail").getMessageType();
        byte[] noteOnly = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("detail"), DynamicMessage.newBuilder(detailDescriptor).setField(detailDescriptor.findFieldByName("note"), "n").build())
                .build()
                .toByteArray();
        encoder = new ProtobufRowEncoder(descriptor, TestingConnectorSession.SESSION, ImmutableList.of(createEncoderColumnHandle("detail", detailType, "detail")));
        RowBlockBuilder rowBlockBuilder = (RowBlockBuilder) detailType.createBlockBuilder(null, 1);
        rowBlockBuilder.buildEntry(fieldBuilders -> {
            writeNativeValue(VARCHAR, fieldBuilders.get(0), utf8Slice("n"));
            fieldBuilders.get(1).appendNull();
        });
        encoder.appendColumnValue(rowBlockBuilder.build(), 0);
        assertThat(encoder.toByteArray()).isEqualTo(noteOnly);

        encoder = new ProtobufRowEncoder(descriptor, TestingConnectorSession.SESSION, ImmutableList.of(
                createEncoderColumnHandle("note", VARCHAR, "detail/note"),
                createEncoderColumnHandle("weight", BIGINT, "detail/weight")));
        encoder.appendColumnValue(nativeValueBlock(VARCHAR, utf8Slice("n")), 0);
        encoder.appendColumnValue(nullBlock(BIGINT), 0);
        assertThat(encoder.toByteArray()).isEqualTo(noteOnly);

        // fields without presence have no representation for NULL
        assertNullRejected(descriptor, createEncoderColumnHandle("name", VARCHAR, "name"), nullBlock(VARCHAR), "NULL value not allowed for protobuf field 'schema.name', which does not track presence; declare it optional to allow NULL");
        assertNullRejected(descriptor, createEncoderColumnHandle("note", VARCHAR, "detail/note"), nullBlock(VARCHAR), "NULL value not allowed for protobuf field 'schema.Detail.note', which does not track presence; declare it optional to allow NULL");
        assertNullRejected(descriptor, createEncoderColumnHandle("tags", tagsType, "tags"), nullBlock(tagsType), "NULL value not allowed for repeated protobuf field 'schema.tags'");
        assertNullRejected(descriptor, createEncoderColumnHandle("attributes", attributesType, "attributes"), nullBlock(attributesType), "NULL value not allowed for repeated protobuf field 'schema.attributes'");

        rowBlockBuilder = (RowBlockBuilder) detailType.createBlockBuilder(null, 1);
        rowBlockBuilder.buildEntry(fieldBuilders -> {
            fieldBuilders.get(0).appendNull();
            writeNativeValue(BIGINT, fieldBuilders.get(1), 1L);
        });
        assertNullRejected(descriptor, createEncoderColumnHandle("detail", detailType, "detail"), rowBlockBuilder.build(), "NULL value not allowed for protobuf field 'schema.Detail.note', which does not track presence; declare it optional to allow NULL");

        ArrayBlockBuilder arrayBlockBuilder = (ArrayBlockBuilder) tagsType.createBlockBuilder(null, 1);
        arrayBlockBuilder.buildEntry(elementBuilder -> elementBuilder.appendNull());
        assertNullRejected(descriptor, createEncoderColumnHandle("tags", tagsType, "tags"), arrayBlockBuilder.build(), "NULL element not allowed in repeated protobuf field 'schema.tags'");

        MapBlockBuilder mapBlockBuilder = (MapBlockBuilder) attributesType.createBlockBuilder(null, 1);
        mapBlockBuilder.buildEntry((keyBuilder, valueBuilder) -> {
            writeNativeValue(VARCHAR, keyBuilder, utf8Slice("key"));
            valueBuilder.appendNull();
        });
        assertNullRejected(descriptor, createEncoderColumnHandle("attributes", attributesType, "attributes"), mapBlockBuilder.build(), "NULL map value not allowed for protobuf field 'schema.attributes'");
    }

    @Test
    public void testNullValuesOfProto2Fields()
    {
        Descriptor descriptor = getFileDescriptor(
                """
                syntax = "proto2";
                message schema {
                  required string name = 1;
                  optional int64 count = 2 [default = 7];
                  oneof choice {
                    int64 number = 3;
                    string word = 4;
                  }
                }
                """).findMessageTypeByName(DEFAULT_MESSAGE);
        EncoderColumnHandle nameColumn = createEncoderColumnHandle("name", VARCHAR, "name");
        KafkaColumnHandle countColumn = new KafkaColumnHandle("count", BIGINT, "count", null, null, false, false, false, Optional.empty());
        KafkaColumnHandle numberColumn = new KafkaColumnHandle("number", BIGINT, "number", null, null, false, false, false, Optional.empty());

        // optional fields and oneof members are left unset, and read back as NULL
        RowEncoder encoder = new ProtobufRowEncoder(descriptor, TestingConnectorSession.SESSION, ImmutableList.of(nameColumn, countColumn, numberColumn));
        encoder.appendColumnValue(nativeValueBlock(VARCHAR, utf8Slice("Trino")), 0);
        encoder.appendColumnValue(nullBlock(BIGINT), 0);
        encoder.appendColumnValue(nullBlock(BIGINT), 0);
        byte[] record = encoder.toByteArray();
        assertThat(record).isEqualTo(DynamicMessage.newBuilder(descriptor).setField(descriptor.findFieldByName("name"), "Trino").build().toByteArray());
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = new ProtobufRowDecoder(
                new FixedSchemaDynamicMessageProvider(descriptor),
                ImmutableSet.of(countColumn, numberColumn),
                TESTING_TYPE_MANAGER,
                new FileDescriptorProvider())
                .decodeRow(record)
                .orElseThrow();
        assertThat(decodedRow.keySet()).containsExactlyInAnyOrder(countColumn, numberColumn);
        assertThat(decodedRow.values()).allMatch(FieldValueProvider::isNull);

        // a message without a required field cannot be built
        assertNullRejected(descriptor, nameColumn, nullBlock(VARCHAR), "NULL value not allowed for required protobuf field 'schema.name'");
    }

    @Test
    public void testFlattenedFieldsOfRequiredProto2Message()
    {
        Descriptor descriptor = getFileDescriptor(
                """
                syntax = "proto2";
                message schema {
                  required Child child = 1;
                  message Child {
                    required string name = 1;
                    required int64 count = 2;
                    optional string note = 3;
                  }
                }
                """).findMessageTypeByName(DEFAULT_MESSAGE);
        EncoderColumnHandle nameColumn = createEncoderColumnHandle("name", VARCHAR, "child/name");
        EncoderColumnHandle countColumn = createEncoderColumnHandle("count", BIGINT, "child/count");
        EncoderColumnHandle noteColumn = createEncoderColumnHandle("note", VARCHAR, "child/note");

        // the required fields of the nested message are set by separate columns
        RowEncoder encoder = new ProtobufRowEncoder(descriptor, TestingConnectorSession.SESSION, ImmutableList.of(nameColumn, countColumn));
        encoder.appendColumnValue(nativeValueBlock(VARCHAR, utf8Slice("Trino")), 0);
        encoder.appendColumnValue(nativeValueBlock(BIGINT, 3L), 0);
        Descriptor child = descriptor.findNestedTypeByName("Child");
        assertThat(encoder.toByteArray()).isEqualTo(DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("child"), DynamicMessage.newBuilder(child)
                        .setField(child.findFieldByName("name"), "Trino")
                        .setField(child.findFieldByName("count"), 3L)
                        .build())
                .build()
                .toByteArray());

        // a NULL for the only column of the nested message leaves the required message unset
        RowEncoder nullEncoder = new ProtobufRowEncoder(descriptor, TestingConnectorSession.SESSION, ImmutableList.of(noteColumn));
        nullEncoder.appendColumnValue(nullBlock(VARCHAR), 0);
        assertThatThrownBy(nullEncoder::toByteArray)
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(CONSTRAINT_VIOLATION.toErrorCode()))
                .hasMessage("Protobuf message 'schema' cannot be written: Message missing required fields: child");
    }

    private static void assertNullRejected(Descriptor descriptor, EncoderColumnHandle column, Block block, String message)
    {
        RowEncoder encoder = new ProtobufRowEncoder(descriptor, TestingConnectorSession.SESSION, ImmutableList.of(column));
        assertThatThrownBy(() -> encoder.appendColumnValue(block, 0))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(CONSTRAINT_VIOLATION.toErrorCode()))
                .hasMessage(message);
    }

    private static Block nativeValueBlock(Type type, Object value)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
        writeNativeValue(type, blockBuilder, value);
        return blockBuilder.build();
    }

    private static Block nullBlock(Type type)
    {
        return type.createBlockBuilder(null, 1).appendNull().build();
    }

    private Timestamp getTimestamp(SqlTimestamp sqlTimestamp)
    {
        return Timestamp.newBuilder()
                .setSeconds(floorDiv(sqlTimestamp.getEpochMicros(), MICROSECONDS_PER_SECOND))
                .setNanos(floorMod(sqlTimestamp.getEpochMicros(), MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND)
                .build();
    }

    private RowEncoder createRowEncoder(String fileName, List<EncoderColumnHandle> columns)
            throws Exception
    {
        return ENCODER_FACTORY.create(TestingConnectorSession.SESSION, new RowEncoderSpec(ProtobufRowEncoder.NAME, Optional.of(getProtoFile("decoder/protobuf/" + fileName)), Optional.empty(), columns, "ignored", MESSAGE));
    }

    private Descriptor getDescriptor(String fileName)
            throws Exception
    {
        return getFileDescriptor(getProtoFile("decoder/protobuf/" + fileName)).findMessageTypeByName(DEFAULT_MESSAGE);
    }

    private static EncoderColumnHandle createEncoderColumnHandle(String name, Type type, String mapping)
    {
        return new KafkaColumnHandle(name, type, mapping, null, null, false, false, false, Optional.empty());
    }
}
