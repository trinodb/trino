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
package io.trino.decoder.protobuf;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import io.airlift.slice.Slices;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.DecoderTestColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.decoder.RowDecoder;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlVarbinary;
import io.trino.testing.TestingSession;
import io.trino.type.JsonType;
import org.testng.annotations.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.io.Resources.getResource;
import static io.trino.decoder.protobuf.ProtobufRowDecoderFactory.DEFAULT_MESSAGE;
import static io.trino.decoder.util.DecoderTestUtil.checkValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.Math.PI;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestProtobufDecoder
{
    private static final ProtobufRowDecoderFactory DECODER_FACTORY = new ProtobufRowDecoderFactory(new FixedSchemaDynamicMessageProvider.Factory(), TESTING_TYPE_MANAGER);

    @Test(dataProvider = "allTypesDataProvider", dataProviderClass = ProtobufDataProviders.class)
    public void testAllDataTypes(String stringData, Integer integerData, Long longData, Double doubleData, Float floatData, Boolean booleanData, String enumData, SqlTimestamp sqlTimestamp, byte[] bytesData)
            throws Exception
    {
        DecoderTestColumnHandle stringColumn = new DecoderTestColumnHandle(0, "stringColumn", createVarcharType(30000), "stringColumn", null, null, false, false, false);
        DecoderTestColumnHandle integerColumn = new DecoderTestColumnHandle(1, "integerColumn", INTEGER, "integerColumn", null, null, false, false, false);
        DecoderTestColumnHandle longColumn = new DecoderTestColumnHandle(2, "longColumn", BIGINT, "longColumn", null, null, false, false, false);
        DecoderTestColumnHandle doubleColumn = new DecoderTestColumnHandle(3, "doubleColumn", DOUBLE, "doubleColumn", null, null, false, false, false);
        DecoderTestColumnHandle floatColumn = new DecoderTestColumnHandle(4, "floatColumn", REAL, "floatColumn", null, null, false, false, false);
        DecoderTestColumnHandle booleanColumn = new DecoderTestColumnHandle(5, "booleanColumn", BOOLEAN, "booleanColumn", null, null, false, false, false);
        DecoderTestColumnHandle numberColumn = new DecoderTestColumnHandle(6, "numberColumn", createVarcharType(4), "numberColumn", null, null, false, false, false);
        DecoderTestColumnHandle timestampColumn = new DecoderTestColumnHandle(7, "timestampColumn", createTimestampType(3), "timestampColumn", null, null, false, false, false);
        DecoderTestColumnHandle bytesColumn = new DecoderTestColumnHandle(8, "bytesColumn", VARBINARY, "bytesColumn", null, null, false, false, false);

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

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = createRowDecoder("all_datatypes.proto", ImmutableSet.of(stringColumn, integerColumn, longColumn, doubleColumn, floatColumn, booleanColumn, numberColumn, timestampColumn, bytesColumn))
                .decodeRow(messageBuilder.build().toByteArray())
                .orElseThrow(AssertionError::new);

        assertEquals(decodedRow.size(), 9);

        checkValue(decodedRow, stringColumn, stringData);
        checkValue(decodedRow, integerColumn, integerData);
        checkValue(decodedRow, longColumn, longData);
        checkValue(decodedRow, doubleColumn, doubleData);
        checkValue(decodedRow, floatColumn, floatData);
        checkValue(decodedRow, booleanColumn, booleanData);
        checkValue(decodedRow, numberColumn, enumData);
        checkValue(decodedRow, timestampColumn, sqlTimestamp.getEpochMicros());
        checkValue(decodedRow, bytesColumn, Slices.wrappedBuffer(bytesData));
    }

    @Test
    public void testOneofFixedSchemaProvider()
            throws Exception
    {
        Set<String> oneofColumnNames = Set.of(
                "stringColumn",
                "integerColumn",
                "longColumn",
                "doubleColumn",
                "floatColumn",
                "booleanColumn",
                "numberColumn",
                "timestampColumn",
                "bytesColumn",
                "rowColumn",
                "nestedRowColumn");

        // Uses the file-based schema parser which generates a Descriptor that does not have any oneof fields -- all are null
        Descriptor descriptor = getDescriptor("test_oneof.proto");
        for (String oneofColumnName : oneofColumnNames) {
            assertNull(descriptor.findFieldByName(oneofColumnName));
        }
    }

    @Test
    public void testOneofConfluentSchemaProvider()
            throws Exception
    {
        String stringData = "Trino";
        int integerData = 1;
        long longData = 493857959588286460L;
        double doubleData = PI;
        float floatData = 3.14f;
        boolean booleanData = true;
        String enumData = "ONE";
        SqlTimestamp sqlTimestamp = sqlTimestampOf(3, LocalDateTime.parse("2020-12-12T15:35:45.923"));
        byte[] bytesData = "X'65683F'".getBytes(UTF_8);

        // Uses the Confluent schema parser to generate the Descriptor which will include the oneof columns as fields
        Descriptor descriptor = ((ProtobufSchema) new ProtobufSchemaProvider()
                .parseSchema(Resources.toString(getResource("decoder/protobuf/test_oneof.proto"), UTF_8), List.of(), true)
                .get())
                .toDescriptor();

        // Build the Row message
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

        DynamicMessage.Builder rowMessage = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("rowColumn"), rowBuilder.build());

        Map<String, Object> expectedRowMessageValue = ImmutableMap.of("stringColumn", "Trino", "integerColumn", 1, "longColumn", "493857959588286460", "doubleColumn", 3.141592653589793, "floatColumn", 3.14, "booleanColumn", true, "numberColumn", "ONE", "timestampColumn", "2020-12-12T15:35:45.923Z", "bytesColumn", "WCc2NTY4M0Yn");

        // Build the NestedRow message
        Descriptor nestedDescriptor = descriptor.findNestedTypeByName("NestedRow");
        DynamicMessage.Builder nestedMessageBuilder = DynamicMessage.newBuilder(nestedDescriptor);

        nestedMessageBuilder.setField(nestedDescriptor.findFieldByName("nested_list"), ImmutableList.of(rowBuilder.build()));

        Descriptor mapDescriptor = nestedDescriptor.findFieldByName("nested_map").getMessageType();
        DynamicMessage.Builder mapBuilder = DynamicMessage.newBuilder(mapDescriptor);
        mapBuilder.setField(mapDescriptor.findFieldByName("key"), "Key");
        mapBuilder.setField(mapDescriptor.findFieldByName("value"), rowBuilder.build());
        nestedMessageBuilder.setField(nestedDescriptor.findFieldByName("nested_map"), ImmutableList.of(mapBuilder.build()));

        nestedMessageBuilder.setField(nestedDescriptor.findFieldByName("row"), rowBuilder.build());

        DynamicMessage nestedMessage = nestedMessageBuilder.build();

        {
            // Empty message
            assertOneof(DynamicMessage.newBuilder(descriptor), Map.of());
        }
        {
            DynamicMessage.Builder message = DynamicMessage.newBuilder(descriptor)
                    .setField(descriptor.findFieldByName("stringColumn"), stringData);
            assertOneof(message, Map.of("stringColumn", stringData));
        }
        {
            DynamicMessage.Builder message = DynamicMessage.newBuilder(descriptor)
                    .setField(descriptor.findFieldByName("integerColumn"), integerData);
            assertOneof(message, Map.of("integerColumn", integerData));
        }
        {
            DynamicMessage.Builder message = DynamicMessage.newBuilder(descriptor)
                    .setField(descriptor.findFieldByName("longColumn"), longData);
            assertOneof(message, Map.of("longColumn", Long.toString(longData)));
        }
        {
            DynamicMessage.Builder message = DynamicMessage.newBuilder(descriptor)
                    .setField(descriptor.findFieldByName("doubleColumn"), doubleData);
            assertOneof(message, Map.of("doubleColumn", doubleData));
        }
        {
            DynamicMessage.Builder message = DynamicMessage.newBuilder(descriptor)
                    .setField(descriptor.findFieldByName("floatColumn"), floatData);
            assertOneof(message, Map.of("floatColumn", floatData));
        }
        {
            DynamicMessage.Builder message = DynamicMessage.newBuilder(descriptor)
                    .setField(descriptor.findFieldByName("booleanColumn"), booleanData);
            assertOneof(message, Map.of("booleanColumn", booleanData));
        }
        {
            DynamicMessage.Builder message = DynamicMessage.newBuilder(descriptor)
                    .setField(descriptor.findFieldByName("numberColumn"), descriptor.findEnumTypeByName("Number").findValueByName(enumData));
            assertOneof(message, Map.of("numberColumn", enumData));
        }
        {
            DynamicMessage.Builder message = DynamicMessage.newBuilder(descriptor)
                    .setField(descriptor.findFieldByName("timestampColumn"), getTimestamp(sqlTimestamp));
            assertOneof(message, Map.of("timestampColumn", "2020-12-12T15:35:45.923Z"));
        }
        {
            DynamicMessage.Builder message = DynamicMessage.newBuilder(descriptor)
                    .setField(descriptor.findFieldByName("bytesColumn"), bytesData);
            assertOneof(message, Map.of("bytesColumn", bytesData));
        }
        {
            assertOneof(rowMessage, Map.of("rowColumn", expectedRowMessageValue));
        }
        {
            DynamicMessage.Builder message = DynamicMessage.newBuilder(descriptor)
                    .setField(descriptor.findFieldByName("nestedRowColumn"), nestedMessage);
            assertOneof(message,
                    Map.of("nestedRowColumn", ImmutableMap.of("nestedList", List.of(expectedRowMessageValue),
                            "nestedMap", ImmutableMap.of("Key", expectedRowMessageValue),
                            "row", expectedRowMessageValue)));
        }
    }

    private void assertOneof(DynamicMessage.Builder messageBuilder,
            Map<String, Object> setValue)
            throws Exception
    {
        DecoderTestColumnHandle testColumnHandle = new DecoderTestColumnHandle(0, "column", VARCHAR, "column", null, null, false, false, false);
        DecoderTestColumnHandle testOneofColumn = new DecoderTestColumnHandle(1, "testOneofColumn", JsonType.JSON, "testOneofColumn", null, null, false, false, false);

        final var message = messageBuilder.setField(messageBuilder.getDescriptorForType().findFieldByName("column"), "value").build();

        final var descriptor = ProtobufSchemaUtils.getSchema(message).toDescriptor();
        final var decoder = new ProtobufRowDecoder(new FixedSchemaDynamicMessageProvider(descriptor), ImmutableSet.of(testColumnHandle, testOneofColumn), TESTING_TYPE_MANAGER);

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = decoder
                .decodeRow(message.toByteArray())
                .orElseThrow(AssertionError::new);

        assertEquals(decodedRow.size(), 2);

        final var obj = new ObjectMapper();
        final var expected = obj.writeValueAsString(setValue);

        assertEquals(decodedRow.get(testColumnHandle).getSlice().toStringUtf8(), "value");
        assertEquals(decodedRow.get(testOneofColumn).getSlice().toStringUtf8(), expected);
    }

    @Test(dataProvider = "allTypesDataProvider", dataProviderClass = ProtobufDataProviders.class)
    public void testStructuralDataTypes(String stringData, Integer integerData, Long longData, Double doubleData, Float floatData, Boolean booleanData, String enumData, SqlTimestamp sqlTimestamp, byte[] bytesData)
            throws Exception
    {
        DecoderTestColumnHandle listColumn = new DecoderTestColumnHandle(0, "list", new ArrayType(createVarcharType(100)), "list", null, null, false, false, false);
        DecoderTestColumnHandle mapColumn = new DecoderTestColumnHandle(1, "map", TESTING_TYPE_MANAGER.getType(mapType(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature())), "map", null, null, false, false, false);
        DecoderTestColumnHandle rowColumn = new DecoderTestColumnHandle(
                2,
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
                "row",
                null,
                null,
                false,
                false,
                false);

        Descriptor descriptor = getDescriptor("structural_datatypes.proto");
        DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(descriptor);
        messageBuilder.setField(descriptor.findFieldByName("list"), ImmutableList.of("Presto"));

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

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = createRowDecoder("structural_datatypes.proto", ImmutableSet.of(listColumn, mapColumn, rowColumn))
                .decodeRow(messageBuilder.build().toByteArray())
                .orElseThrow(AssertionError::new);

        assertEquals(decodedRow.size(), 3);

        Block listBlock = decodedRow.get(listColumn).getBlock();
        assertEquals(VARCHAR.getSlice(listBlock, 0).toStringUtf8(), "Presto");

        Block mapBlock = decodedRow.get(mapColumn).getBlock();
        assertEquals(VARCHAR.getSlice(mapBlock, 0).toStringUtf8(), "Key");
        assertEquals(VARCHAR.getSlice(mapBlock, 1).toStringUtf8(), "Value");

        Block rowBlock = decodedRow.get(rowColumn).getBlock();
        ConnectorSession session = TestingSession.testSessionBuilder().build().toConnectorSession();
        assertEquals(VARCHAR.getObjectValue(session, rowBlock, 0), stringData);
        assertEquals(INTEGER.getObjectValue(session, rowBlock, 1), integerData);
        assertEquals(BIGINT.getObjectValue(session, rowBlock, 2), longData);
        assertEquals(DOUBLE.getObjectValue(session, rowBlock, 3), doubleData);
        assertEquals(REAL.getObjectValue(session, rowBlock, 4), floatData);
        assertEquals(BOOLEAN.getObjectValue(session, rowBlock, 5), booleanData);
        assertEquals(VARCHAR.getObjectValue(session, rowBlock, 6), enumData);
        assertEquals(TIMESTAMP_MICROS.getObjectValue(session, rowBlock, 7), sqlTimestamp.roundTo(6));
        assertEquals(VARBINARY.getObjectValue(session, rowBlock, 8), new SqlVarbinary(bytesData));
    }

    @Test
    public void testMissingFieldInRowType()
            throws Exception
    {
        DecoderTestColumnHandle rowColumn = new DecoderTestColumnHandle(
                2,
                "row",
                RowType.from(ImmutableList.of(RowType.field("unknown_mapping", createVarcharType(30000)))),
                "row",
                null,
                null,
                false,
                false,
                false);

        Descriptor descriptor = getDescriptor("structural_datatypes.proto");
        DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(descriptor);

        Descriptor rowDescriptor = descriptor.findFieldByName("row").getMessageType();
        DynamicMessage.Builder rowBuilder = DynamicMessage.newBuilder(rowDescriptor);
        rowBuilder.setField(rowDescriptor.findFieldByName("string_column"), "Test");
        messageBuilder.setField(descriptor.findFieldByName("row"), rowBuilder.build());

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = createRowDecoder("structural_datatypes.proto", ImmutableSet.of(rowColumn))
                .decodeRow(messageBuilder.build().toByteArray())
                .orElseThrow(AssertionError::new);

        assertThatThrownBy(() -> decodedRow.get(rowColumn).getBlock())
                .hasMessageMatching("Unknown Field unknown_mapping");
    }

    @Test(dataProvider = "allTypesDataProvider", dataProviderClass = ProtobufDataProviders.class)
    public void testRowFlattening(String stringData, Integer integerData, Long longData, Double doubleData, Float floatData, Boolean booleanData, String enumData, SqlTimestamp sqlTimestamp, byte[] bytesData)
            throws Exception
    {
        DecoderTestColumnHandle stringColumn = new DecoderTestColumnHandle(0, "stringColumn", createVarcharType(30000), "row/string_column", null, null, false, false, false);
        DecoderTestColumnHandle integerColumn = new DecoderTestColumnHandle(1, "integerColumn", INTEGER, "row/integer_column", null, null, false, false, false);
        DecoderTestColumnHandle longColumn = new DecoderTestColumnHandle(2, "longColumn", BIGINT, "row/long_column", null, null, false, false, false);
        DecoderTestColumnHandle doubleColumn = new DecoderTestColumnHandle(3, "doubleColumn", DOUBLE, "row/double_column", null, null, false, false, false);
        DecoderTestColumnHandle floatColumn = new DecoderTestColumnHandle(4, "floatColumn", REAL, "row/float_column", null, null, false, false, false);
        DecoderTestColumnHandle booleanColumn = new DecoderTestColumnHandle(5, "booleanColumn", BOOLEAN, "row/boolean_column", null, null, false, false, false);
        DecoderTestColumnHandle numberColumn = new DecoderTestColumnHandle(6, "numberColumn", createVarcharType(4), "row/number_column", null, null, false, false, false);
        DecoderTestColumnHandle timestampColumn = new DecoderTestColumnHandle(6, "timestampColumn", createTimestampType(3), "row/timestamp_column", null, null, false, false, false);
        DecoderTestColumnHandle bytesColumn = new DecoderTestColumnHandle(5, "bytesColumn", VARBINARY, "row/bytes_column", null, null, false, false, false);

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

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = createRowDecoder("structural_datatypes.proto", ImmutableSet.of(stringColumn, integerColumn, longColumn, doubleColumn, floatColumn, booleanColumn, numberColumn, timestampColumn, bytesColumn))
                .decodeRow(messageBuilder.build().toByteArray())
                .orElseThrow(AssertionError::new);

        assertEquals(decodedRow.size(), 9);

        checkValue(decodedRow, stringColumn, stringData);
        checkValue(decodedRow, integerColumn, integerData);
        checkValue(decodedRow, longColumn, longData);
        checkValue(decodedRow, doubleColumn, doubleData);
        checkValue(decodedRow, floatColumn, floatData);
        checkValue(decodedRow, booleanColumn, booleanData);
        checkValue(decodedRow, numberColumn, enumData);
        checkValue(decodedRow, timestampColumn, sqlTimestamp.getEpochMicros());
        checkValue(decodedRow, bytesColumn, Slices.wrappedBuffer(bytesData));
    }

    private Timestamp getTimestamp(SqlTimestamp sqlTimestamp)
    {
        return Timestamp.newBuilder()
                .setSeconds(floorDiv(sqlTimestamp.getEpochMicros(), MICROSECONDS_PER_SECOND))
                .setNanos(floorMod(sqlTimestamp.getEpochMicros(), MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND)
                .build();
    }

    private RowDecoder createRowDecoder(String fileName, Set<DecoderColumnHandle> columns)
            throws Exception
    {
        return DECODER_FACTORY.create(ImmutableMap.of("dataSchema", ProtobufUtils.getProtoFile("decoder/protobuf/" + fileName)), columns);
    }

    private Descriptor getDescriptor(String fileName)
            throws Exception
    {
        return ProtobufUtils.getFileDescriptor(ProtobufUtils.getProtoFile("decoder/protobuf/" + fileName)).findMessageTypeByName(DEFAULT_MESSAGE);
    }
}
