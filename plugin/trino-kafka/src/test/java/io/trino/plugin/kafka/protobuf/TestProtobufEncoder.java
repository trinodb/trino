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
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import io.airlift.slice.Slices;
import io.trino.decoder.protobuf.ProtobufDataProviders;
import io.trino.plugin.kafka.KafkaColumnHandle;
import io.trino.plugin.kafka.encoder.EncoderColumnHandle;
import io.trino.plugin.kafka.encoder.RowEncoder;
import io.trino.plugin.kafka.encoder.RowEncoderSpec;
import io.trino.plugin.kafka.encoder.protobuf.ProtobufRowEncoder;
import io.trino.plugin.kafka.encoder.protobuf.ProtobufRowEncoderFactory;
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
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.decoder.protobuf.ProtobufRowDecoderFactory.DEFAULT_MESSAGE;
import static io.trino.decoder.protobuf.ProtobufUtils.getFileDescriptor;
import static io.trino.decoder.protobuf.ProtobufUtils.getProtoFile;
import static io.trino.plugin.kafka.encoder.KafkaFieldType.MESSAGE;
import static io.trino.spi.block.ArrayBlock.fromElementBlock;
import static io.trino.spi.block.RowBlock.fromFieldBlocks;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.Float.floatToIntBits;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static org.testng.Assert.assertEquals;

public class TestProtobufEncoder
{
    private static final ProtobufRowEncoderFactory ENCODER_FACTORY = new ProtobufRowEncoderFactory();

    @Test(dataProvider = "allTypesDataProvider", dataProviderClass = ProtobufDataProviders.class)
    public void testAllDataTypes(String stringData, Integer integerData, Long longData, Double doubleData, Float floatData, Boolean booleanData, String enumData, SqlTimestamp sqlTimestamp, byte[] bytesData)
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
        messageBuilder.setField(descriptor.findFieldByName("bytesColumn"), bytesData);

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

        rowEncoder.appendColumnValue(nativeValueToBlock(createVarcharType(5), utf8Slice(stringData)), 0);
        rowEncoder.appendColumnValue(nativeValueToBlock(INTEGER, integerData.longValue()), 0);
        rowEncoder.appendColumnValue(nativeValueToBlock(BIGINT, longData), 0);
        rowEncoder.appendColumnValue(nativeValueToBlock(DOUBLE, doubleData), 0);
        rowEncoder.appendColumnValue(nativeValueToBlock(REAL, (long) floatToIntBits(floatData)), 0);
        rowEncoder.appendColumnValue(nativeValueToBlock(BOOLEAN, booleanData), 0);
        rowEncoder.appendColumnValue(nativeValueToBlock(createVarcharType(4), utf8Slice(enumData)), 0);
        rowEncoder.appendColumnValue(nativeValueToBlock(createTimestampType(6), sqlTimestamp.getEpochMicros()), 0);
        rowEncoder.appendColumnValue(nativeValueToBlock(VARBINARY, wrappedBuffer(bytesData)), 0);

        assertEquals(messageBuilder.build().toByteArray(), rowEncoder.toByteArray());
    }

    @Test(dataProvider = "allTypesDataProvider", dataProviderClass = ProtobufDataProviders.class)
    public void testStructuralDataTypes(String stringData, Integer integerData, Long longData, Double doubleData, Float floatData, Boolean booleanData, String enumData, SqlTimestamp sqlTimestamp, byte[] bytesData)
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
        rowBuilder.setField(rowDescriptor.findFieldByName("bytes_column"), bytesData);
        messageBuilder.setField(descriptor.findFieldByName("row"), rowBuilder.build());

        List<EncoderColumnHandle> columnHandles = ImmutableList.of(
                createEncoderColumnHandle("list", new ArrayType(createVarcharType(30000)), "list"),
                createEncoderColumnHandle("map", TESTING_TYPE_MANAGER.getType(mapType(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature())), "map"),
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
            writeNativeValue(VARCHAR, fieldBuilders.get(6), enumData);
            writeNativeValue(createTimestampType(6), fieldBuilders.get(7), sqlTimestamp.getEpochMicros());
            writeNativeValue(VARBINARY, fieldBuilders.get(8), bytesData);
        });
        rowEncoder.appendColumnValue(rowBlockBuilder.build(), 0);

        assertEquals(messageBuilder.build().toByteArray(), rowEncoder.toByteArray());
    }

    @Test(dataProvider = "allTypesDataProvider", dataProviderClass = ProtobufDataProviders.class)
    public void testNestedStructuralDataTypes(String stringData, Integer integerData, Long longData, Double doubleData, Float floatData, Boolean booleanData, String enumData, SqlTimestamp sqlTimestamp, byte[] bytesData)
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
        rowBuilder.setField(rowDescriptor.findFieldByName("bytes_column"), bytesData);
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
                                RowType.field("nested_map", TESTING_TYPE_MANAGER.getType(mapType(VARCHAR.getTypeSignature(), rowType.getTypeSignature()))),
                                RowType.field("row", rowType))),
                        "nested_row"));

        RowEncoder rowEncoder = createRowEncoder("structural_datatypes.proto", columnHandles);

        RowBlockBuilder rowBlockBuilder = rowType.createBlockBuilder(null, 1);
        rowBlockBuilder.buildEntry(fieldBuilders -> {
            writeNativeValue(VARCHAR, fieldBuilders.get(0), Slices.utf8Slice(stringData));
            writeNativeValue(INTEGER, fieldBuilders.get(1), integerData.longValue());
            writeNativeValue(BIGINT, fieldBuilders.get(2), longData);
            writeNativeValue(DOUBLE, fieldBuilders.get(3), doubleData);
            writeNativeValue(REAL, fieldBuilders.get(4), (long) floatToIntBits(floatData));
            writeNativeValue(BOOLEAN, fieldBuilders.get(5), booleanData);
            writeNativeValue(VARCHAR, fieldBuilders.get(6), enumData);
            writeNativeValue(createTimestampType(6), fieldBuilders.get(7), sqlTimestamp.getEpochMicros());
            writeNativeValue(VARBINARY, fieldBuilders.get(8), bytesData);
        });

        RowType nestedRowType = (RowType) columnHandles.get(0).getType();

        MapType mapType = (MapType) nestedRowType.getTypeParameters().get(1);
        BlockBuilder mapBlockBuilder = mapType.createBlockBuilder(null, 1);
        Block mapBlock = mapType.createBlockFromKeyValue(
                Optional.empty(),
                new int[]{0, 1},
                nativeValueToBlock(VARCHAR, utf8Slice("Key")),
                rowBlockBuilder.build());
        mapType.appendTo(
                mapBlock,
                0,
                mapBlockBuilder);

        Type listType = nestedRowType.getTypeParameters().get(0);
        BlockBuilder listBlockBuilder = listType.createBlockBuilder(null, 1);
        Block arrayBlock = fromElementBlock(
                1,
                Optional.empty(),
                new int[]{0, rowBlockBuilder.getPositionCount()},
                rowBlockBuilder.build());
        listType.appendTo(arrayBlock, 0, listBlockBuilder);

        BlockBuilder nestedBlockBuilder = nestedRowType.createBlockBuilder(null, 1);
        Block rowBlock = fromFieldBlocks(
                1,
                Optional.empty(),
                new Block[]{listBlockBuilder.build(), mapBlockBuilder.build(), rowBlockBuilder.build()});
        nestedRowType.appendTo(rowBlock, 0, nestedBlockBuilder);

        rowEncoder.appendColumnValue(nestedBlockBuilder, 0);

        assertEquals(messageBuilder.build().toByteArray(), rowEncoder.toByteArray());
    }

    @Test(dataProvider = "allTypesDataProvider", dataProviderClass = ProtobufDataProviders.class)
    public void testRowFlattening(String stringData, Integer integerData, Long longData, Double doubleData, Float floatData, Boolean booleanData, String enumData, SqlTimestamp sqlTimestamp, byte[] bytesData)
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
        rowBuilder.setField(rowDescriptor.findFieldByName("bytes_column"), bytesData);
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

        rowEncoder.appendColumnValue(nativeValueToBlock(createVarcharType(5), utf8Slice(stringData)), 0);
        rowEncoder.appendColumnValue(nativeValueToBlock(INTEGER, integerData.longValue()), 0);
        rowEncoder.appendColumnValue(nativeValueToBlock(BIGINT, longData), 0);
        rowEncoder.appendColumnValue(nativeValueToBlock(DOUBLE, doubleData), 0);
        rowEncoder.appendColumnValue(nativeValueToBlock(REAL, (long) floatToIntBits(floatData)), 0);
        rowEncoder.appendColumnValue(nativeValueToBlock(BOOLEAN, booleanData), 0);
        rowEncoder.appendColumnValue(nativeValueToBlock(createVarcharType(4), utf8Slice(enumData)), 0);
        rowEncoder.appendColumnValue(nativeValueToBlock(createTimestampType(6), sqlTimestamp.getEpochMicros()), 0);
        rowEncoder.appendColumnValue(nativeValueToBlock(VARBINARY, wrappedBuffer(bytesData)), 0);

        assertEquals(messageBuilder.build().toByteArray(), rowEncoder.toByteArray());
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
        return ENCODER_FACTORY.create(TestingConnectorSession.SESSION, new RowEncoderSpec(ProtobufRowEncoder.NAME, Optional.of(getProtoFile("decoder/protobuf/" + fileName)), columns, "ignored", MESSAGE));
    }

    private Descriptor getDescriptor(String fileName)
            throws Exception
    {
        return getFileDescriptor(getProtoFile("decoder/protobuf/" + fileName)).findMessageTypeByName(DEFAULT_MESSAGE);
    }

    private static EncoderColumnHandle createEncoderColumnHandle(String name, Type type, String mapping)
    {
        return new KafkaColumnHandle(name, type, mapping, null, null, false, false, false);
    }
}
