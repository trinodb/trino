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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import io.airlift.slice.Slices;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.DecoderTestColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.decoder.RowDecoder;
import io.trino.decoder.RowDecoderSpec;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlVarbinary;
import io.trino.type.JsonType;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.decoder.protobuf.ProtobufRowDecoderFactory.DEFAULT_MESSAGE;
import static io.trino.decoder.util.DecoderTestUtil.TESTING_SESSION;
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
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.Math.PI;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

public class TestProtobufDecoder
{
    private static final ProtobufRowDecoderFactory DECODER_FACTORY = new ProtobufRowDecoderFactory(new FixedSchemaDynamicMessageProvider.Factory(), TESTING_TYPE_MANAGER, new FileDescriptorProvider());

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

        assertThat(decodedRow).hasSize(9);

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
    public void testOneofFieldsArePresentInDescriptor()
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

        Descriptor descriptor = getDescriptor("test_oneof.proto");
        Descriptors.OneofDescriptor oneof = descriptor.getOneofs().getFirst();
        assertThat(oneof.getName()).isEqualTo("testOneofColumn");
        for (String oneofColumnName : oneofColumnNames) {
            FieldDescriptor field = descriptor.findFieldByName(oneofColumnName);
            assertThat(field).isNotNull();
            assertThat(field.getContainingOneof()).isEqualTo(oneof);
        }
        assertThat(descriptor.findFieldByName("column").getContainingOneof()).isNull();
    }

    @Test
    public void testOneof()
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

        Descriptor descriptor = getDescriptor("test_oneof.proto");

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
                    Map.of("nestedRowColumn", ImmutableMap.of(
                            "nestedList", List.of(expectedRowMessageValue),
                            "nestedMap", ImmutableMap.of("Key", expectedRowMessageValue),
                            "row", expectedRowMessageValue)));
        }
    }

    private void assertOneof(
            DynamicMessage.Builder messageBuilder,
            Map<String, Object> setValue)
            throws Exception
    {
        DecoderTestColumnHandle testColumnHandle = new DecoderTestColumnHandle(0, "column", VARCHAR, "column", null, null, false, false, false);
        DecoderTestColumnHandle testOneofColumn = new DecoderTestColumnHandle(1, "testOneofColumn", JsonType.JSON, "testOneofColumn", null, null, false, false, false);

        final var message = messageBuilder.setField(messageBuilder.getDescriptorForType().findFieldByName("column"), "value").build();

        final var decoder = new ProtobufRowDecoder(new FixedSchemaDynamicMessageProvider(message.getDescriptorForType()), ImmutableSet.of(testColumnHandle, testOneofColumn), TESTING_TYPE_MANAGER, new FileDescriptorProvider());

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = decoder
                .decodeRow(message.toByteArray())
                .orElseThrow(AssertionError::new);

        assertThat(decodedRow).hasSize(2);

        final var obj = new JsonMapper();
        final var expected = obj.writeValueAsString(setValue);

        assertThat(decodedRow.get(testColumnHandle).getSlice().toStringUtf8()).isEqualTo("value");
        assertThat(decodedRow.get(testOneofColumn).getSlice().toStringUtf8()).isEqualTo(expected);
    }

    @Test
    public void testAnyTypeWithDummyDescriptor()
            throws Exception
    {
        String stringData = "Trino";

        Descriptor allDataTypesDescriptor = getDescriptor("all_datatypes.proto");
        DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(allDataTypesDescriptor);
        messageBuilder.setField(allDataTypesDescriptor.findFieldByName("stringColumn"), stringData);

        Descriptor anyTypeDescriptor = getDescriptor("test_any.proto");
        DynamicMessage.Builder testAnyBuilder = DynamicMessage.newBuilder(anyTypeDescriptor);
        testAnyBuilder.setField(anyTypeDescriptor.findFieldByName("id"), 1);
        testAnyBuilder.setField(anyTypeDescriptor.findFieldByName("anyMessage"), Any.pack(messageBuilder.build()));
        DynamicMessage testAny = testAnyBuilder.build();

        DecoderTestColumnHandle testAnyColumn = new DecoderTestColumnHandle(0, "anyMessage", JsonType.JSON, "anyMessage", null, null, false, false, false);
        ProtobufRowDecoder decoder = new ProtobufRowDecoder(new FixedSchemaDynamicMessageProvider(anyTypeDescriptor), ImmutableSet.of(testAnyColumn), TESTING_TYPE_MANAGER, new DummyDescriptorProvider());

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = decoder
                .decodeRow(testAny.toByteArray())
                .orElseThrow(AssertionError::new);

        assertThat(decodedRow.get(testAnyColumn).isNull()).isTrue();
    }

    @Test
    public void testAnyTypeWithFileDescriptor()
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

        Descriptor allDataTypesDescriptor = getDescriptor("all_datatypes.proto");
        DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(allDataTypesDescriptor);
        messageBuilder.setField(allDataTypesDescriptor.findFieldByName("stringColumn"), stringData);
        messageBuilder.setField(allDataTypesDescriptor.findFieldByName("integerColumn"), integerData);
        messageBuilder.setField(allDataTypesDescriptor.findFieldByName("longColumn"), longData);
        messageBuilder.setField(allDataTypesDescriptor.findFieldByName("doubleColumn"), doubleData);
        messageBuilder.setField(allDataTypesDescriptor.findFieldByName("floatColumn"), floatData);
        messageBuilder.setField(allDataTypesDescriptor.findFieldByName("booleanColumn"), booleanData);
        messageBuilder.setField(allDataTypesDescriptor.findFieldByName("numberColumn"), allDataTypesDescriptor.findEnumTypeByName("Number").findValueByName(enumData));
        messageBuilder.setField(allDataTypesDescriptor.findFieldByName("timestampColumn"), getTimestamp(sqlTimestamp));
        messageBuilder.setField(allDataTypesDescriptor.findFieldByName("bytesColumn"), bytesData);

        // Get URI of parent directory of the descriptor file
        // Any.pack concatenates the message type's full name to the given prefix
        URI anySchemaTypeUrl = new File(Resources.getResource("decoder/protobuf/any/all_datatypes/schema").getFile()).getParentFile().toURI();
        Descriptor descriptor = getDescriptor("test_any.proto");
        DynamicMessage.Builder testAnyBuilder = DynamicMessage.newBuilder(descriptor);
        testAnyBuilder.setField(descriptor.findFieldByName("id"), 1);
        testAnyBuilder.setField(descriptor.findFieldByName("anyMessage"), Any.pack(messageBuilder.build(), anySchemaTypeUrl.toString()));
        DynamicMessage testAny = testAnyBuilder.build();

        DecoderTestColumnHandle testOneOfColumn = new DecoderTestColumnHandle(0, "anyMessage", JsonType.JSON, "anyMessage", null, null, false, false, false);
        ProtobufRowDecoder decoder = new ProtobufRowDecoder(new FixedSchemaDynamicMessageProvider(descriptor), ImmutableSet.of(testOneOfColumn), TESTING_TYPE_MANAGER, new FileDescriptorProvider());

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = decoder
                .decodeRow(testAny.toByteArray())
                .orElseThrow(AssertionError::new);

        JsonNode actual = new JsonMapper().readTree(decodedRow.get(testOneOfColumn).getSlice().toStringUtf8());
        assertThat(actual.get("@type").textValue()).contains("schema");
        assertThat(actual.get("stringColumn").textValue()).isEqualTo(stringData);
        assertThat(actual.get("integerColumn").intValue()).isEqualTo(integerData);
        assertThat(actual.get("longColumn").textValue()).isEqualTo(Long.toString(longData));
        assertThat(actual.get("doubleColumn").doubleValue()).isEqualTo(doubleData);
        assertThat(actual.get("floatColumn").floatValue()).isEqualTo(floatData);
        assertThat(actual.get("booleanColumn").booleanValue()).isEqualTo(booleanData);
        assertThat(actual.get("numberColumn").textValue()).isEqualTo(enumData);
        assertThat(actual.get("timestampColumn").textValue()).isEqualTo("2020-12-12T15:35:45.923Z");
        assertThat(actual.get("bytesColumn").binaryValue()).isEqualTo(bytesData);
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
        DecoderTestColumnHandle listColumn = new DecoderTestColumnHandle(0, "list", new ArrayType(createVarcharType(100)), "list", null, null, false, false, false);
        DecoderTestColumnHandle mapColumn = new DecoderTestColumnHandle(1, "map", new MapType(VARCHAR, VARCHAR, TESTING_TYPE_MANAGER.getTypeOperators()), "map", null, null, false, false, false);
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

        assertThat(decodedRow).hasSize(3);

        Block listBlock = (Block) decodedRow.get(listColumn).getObject();
        assertThat(VARCHAR.getSlice(listBlock, 0).toStringUtf8()).isEqualTo("Presto");

        SqlMap sqlMap = (SqlMap) decodedRow.get(mapColumn).getObject();
        assertThat(VARCHAR.getSlice(sqlMap.getRawKeyBlock(), sqlMap.getRawOffset()).toStringUtf8()).isEqualTo("Key");
        assertThat(VARCHAR.getSlice(sqlMap.getRawValueBlock(), sqlMap.getRawOffset()).toStringUtf8()).isEqualTo("Value");

        SqlRow sqlRow = (SqlRow) decodedRow.get(rowColumn).getObject();
        int rawIndex = sqlRow.getRawIndex();
        assertThat(VARCHAR.getObjectValue(sqlRow.getRawFieldBlock(0), rawIndex)).isEqualTo(stringData);
        assertThat(INTEGER.getObjectValue(sqlRow.getRawFieldBlock(1), rawIndex)).isEqualTo(integerData);
        assertThat(BIGINT.getObjectValue(sqlRow.getRawFieldBlock(2), rawIndex)).isEqualTo(longData);
        assertThat(DOUBLE.getObjectValue(sqlRow.getRawFieldBlock(3), rawIndex)).isEqualTo(doubleData);
        assertThat(REAL.getObjectValue(sqlRow.getRawFieldBlock(4), rawIndex)).isEqualTo(floatData);
        assertThat(BOOLEAN.getObjectValue(sqlRow.getRawFieldBlock(5), rawIndex)).isEqualTo(booleanData);
        assertThat(VARCHAR.getObjectValue(sqlRow.getRawFieldBlock(6), rawIndex)).isEqualTo(enumData);
        assertThat(TIMESTAMP_MICROS.getObjectValue(sqlRow.getRawFieldBlock(7), rawIndex)).isEqualTo(sqlTimestamp.roundTo(6));
        assertThat(VARBINARY.getObjectValue(sqlRow.getRawFieldBlock(8), rawIndex)).isEqualTo(new SqlVarbinary(bytesData));
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

        // a field the writer's schema does not have decodes as NULL, as for top-level columns
        SqlRow row = (SqlRow) decodedRow.get(rowColumn).getObject();
        assertThat(row.getFieldCount()).isEqualTo(1);
        assertThat(row.getRawFieldBlock(0).isNull(row.getRawIndex())).isTrue();
    }

    @Test
    public void testUnsetProto3OptionalFieldsAreNull()
    {
        Descriptor descriptor = ProtobufUtils.getFileDescriptor(
                """
                syntax = "proto3";
                message schema {
                  optional int64 count = 1;
                  int64 plain = 2;
                  message Detail {
                    optional string note = 1;
                    string label = 2;
                  }
                  Detail detail = 3;
                }
                """).findMessageTypeByName(DEFAULT_MESSAGE);
        Descriptor detailDescriptor = descriptor.findFieldByName("detail").getMessageType();
        DecoderTestColumnHandle countColumn = new DecoderTestColumnHandle(0, "count", BIGINT, "count", null, null, false, false, false);
        DecoderTestColumnHandle plainColumn = new DecoderTestColumnHandle(1, "plain", BIGINT, "plain", null, null, false, false, false);
        DecoderTestColumnHandle noteColumn = new DecoderTestColumnHandle(2, "note", VARCHAR, "detail/note", null, null, false, false, false);
        DecoderTestColumnHandle detailColumn = new DecoderTestColumnHandle(
                3,
                "detail",
                RowType.from(ImmutableList.of(RowType.field("note", VARCHAR), RowType.field("label", VARCHAR))),
                "detail",
                null,
                null,
                false,
                false,
                false);
        ProtobufRowDecoder decoder = new ProtobufRowDecoder(
                new FixedSchemaDynamicMessageProvider(descriptor),
                ImmutableSet.of(countColumn, plainColumn, noteColumn, detailColumn),
                TESTING_TYPE_MANAGER,
                new FileDescriptorProvider());

        // unset optional fields are NULL, while fields without presence decode as their default value
        DynamicMessage unset = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("detail"), DynamicMessage.newBuilder(detailDescriptor).build())
                .build();
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = decoder.decodeRow(unset.toByteArray()).orElseThrow(AssertionError::new);
        assertThat(decodedRow.get(countColumn).isNull()).isTrue();
        assertThat(decodedRow.get(plainColumn).getLong()).isEqualTo(0);
        assertThat(decodedRow.get(noteColumn).isNull()).isTrue();
        SqlRow detail = (SqlRow) decodedRow.get(detailColumn).getObject();
        assertThat(detail.getRawFieldBlock(0).isNull(detail.getRawIndex())).isTrue();
        assertThat(VARCHAR.getObjectValue(detail.getRawFieldBlock(1), detail.getRawIndex())).isEqualTo("");

        // an optional field set to its default value is not NULL
        DynamicMessage setToDefault = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("count"), 0L)
                .setField(descriptor.findFieldByName("detail"), DynamicMessage.newBuilder(detailDescriptor)
                        .setField(detailDescriptor.findFieldByName("note"), "")
                        .build())
                .build();
        decodedRow = decoder.decodeRow(setToDefault.toByteArray()).orElseThrow(AssertionError::new);
        assertThat(decodedRow.get(countColumn).isNull()).isFalse();
        assertThat(decodedRow.get(countColumn).getLong()).isEqualTo(0);
        assertThat(decodedRow.get(noteColumn).isNull()).isFalse();
        assertThat(decodedRow.get(noteColumn).getSlice().toStringUtf8()).isEmpty();
        detail = (SqlRow) decodedRow.get(detailColumn).getObject();
        assertThat(VARCHAR.getObjectValue(detail.getRawFieldBlock(0), detail.getRawIndex())).isEqualTo("");
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

        assertThat(decodedRow).hasSize(9);

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
    public void testUnsetMessageFieldsAreNull()
    {
        Descriptor descriptor = messageDescriptor(
                """
                syntax = "proto3";
                import "google/protobuf/timestamp.proto";
                message schema {
                  message Detail {
                    string note = 1;
                    google.protobuf.Timestamp created = 2;
                  }
                  Detail detail = 1;
                  google.protobuf.Timestamp updated = 2;
                  message Wrapper {
                    Detail detail = 1;
                  }
                  Wrapper wrapper = 3;
                }
                """);
        RowType detailType = RowType.from(ImmutableList.of(RowType.field("note", VARCHAR), RowType.field("created", TIMESTAMP_MICROS)));
        DecoderTestColumnHandle detailColumn = new DecoderTestColumnHandle(0, "detail", detailType, "detail", null, null, false, false, false);
        DecoderTestColumnHandle noteColumn = new DecoderTestColumnHandle(1, "note", VARCHAR, "detail/note", null, null, false, false, false);
        DecoderTestColumnHandle updatedColumn = new DecoderTestColumnHandle(2, "updated", TIMESTAMP_MICROS, "updated", null, null, false, false, false);
        DecoderTestColumnHandle wrapperColumn = new DecoderTestColumnHandle(3, "wrapper", RowType.from(ImmutableList.of(RowType.field("detail", detailType))), "wrapper", null, null, false, false, false);
        ProtobufRowDecoder decoder = new ProtobufRowDecoder(
                new FixedSchemaDynamicMessageProvider(descriptor),
                ImmutableSet.of(detailColumn, noteColumn, updatedColumn, wrapperColumn),
                TESTING_TYPE_MANAGER,
                new FileDescriptorProvider());

        // a message field that is not set is NULL, as a column, through a flattened column and within a row
        Descriptor wrapperDescriptor = descriptor.findFieldByName("wrapper").getMessageType();
        Map<DecoderColumnHandle, FieldValueProvider> unset = decoder.decodeRow(DynamicMessage.newBuilder(descriptor)
                        .setField(descriptor.findFieldByName("wrapper"), DynamicMessage.getDefaultInstance(wrapperDescriptor))
                        .build()
                        .toByteArray())
                .orElseThrow();
        assertThat(unset.get(detailColumn).isNull()).isTrue();
        assertThat(unset.get(noteColumn).isNull()).isTrue();
        assertThat(unset.get(updatedColumn).isNull()).isTrue();
        SqlRow wrapper = (SqlRow) unset.get(wrapperColumn).getObject();
        assertThat(wrapper.getRawFieldBlock(0).isNull(wrapper.getRawIndex())).isTrue();

        // an empty message is a value: its fields have their default values
        Descriptor detailDescriptor = descriptor.findFieldByName("detail").getMessageType();
        Map<DecoderColumnHandle, FieldValueProvider> empty = decoder.decodeRow(DynamicMessage.newBuilder(descriptor)
                        .setField(descriptor.findFieldByName("detail"), DynamicMessage.getDefaultInstance(detailDescriptor))
                        .setField(descriptor.findFieldByName("updated"), Timestamp.getDefaultInstance())
                        .build()
                        .toByteArray())
                .orElseThrow();
        SqlRow detail = (SqlRow) empty.get(detailColumn).getObject();
        assertThat(VARCHAR.getSlice(detail.getRawFieldBlock(0), detail.getRawIndex()).toStringUtf8()).isEmpty();
        assertThat(detail.getRawFieldBlock(1).isNull(detail.getRawIndex())).isTrue();
        checkValue(empty, noteColumn, "");
        checkValue(empty, updatedColumn, 0L);
    }

    @Test
    public void testRenamedFieldsAreReadByNumber()
    {
        // the table columns come from the reader schema, the record was written with a schema that renamed every field
        Descriptor reader = messageDescriptor(
                """
                syntax = "proto3";
                message schema {
                  string name = 1;
                  int64 count = 2;
                  message Detail {
                    string note = 1;
                    int64 weight = 2;
                  }
                  Detail detail = 3;
                  repeated Detail details = 4;
                  map<string, Detail> detail_map = 5;
                }
                """);
        Descriptor writer = messageDescriptor(
                """
                syntax = "proto3";
                message schema {
                  string title = 1;
                  int64 total = 2;
                  message Info {
                    string text = 1;
                    int64 mass = 2;
                  }
                  Info info = 3;
                  repeated Info infos = 4;
                  map<string, Info> info_map = 5;
                }
                """);
        RowType detailType = RowType.from(ImmutableList.of(RowType.field("note", VARCHAR), RowType.field("weight", BIGINT)));
        DecoderTestColumnHandle nameColumn = new DecoderTestColumnHandle(0, "name", VARCHAR, "name", null, null, false, false, false);
        DecoderTestColumnHandle countColumn = new DecoderTestColumnHandle(1, "count", BIGINT, "count", null, null, false, false, false);
        DecoderTestColumnHandle detailColumn = new DecoderTestColumnHandle(2, "detail", detailType, "detail", null, null, false, false, false);
        DecoderTestColumnHandle weightColumn = new DecoderTestColumnHandle(3, "weight", BIGINT, "detail/weight", null, null, false, false, false);
        DecoderTestColumnHandle detailsColumn = new DecoderTestColumnHandle(4, "details", new ArrayType(detailType), "details", null, null, false, false, false);
        DecoderTestColumnHandle detailMapColumn = new DecoderTestColumnHandle(5, "detail_map", new MapType(VARCHAR, detailType, TESTING_TYPE_MANAGER.getTypeOperators()), "detail_map", null, null, false, false, false);
        ProtobufRowDecoder decoder = new ProtobufRowDecoder(
                new EvolvedSchemaProvider(writer, Optional.of(reader)),
                ImmutableSet.of(nameColumn, countColumn, detailColumn, weightColumn, detailsColumn, detailMapColumn),
                TESTING_TYPE_MANAGER,
                new FileDescriptorProvider());

        Descriptor info = writer.findFieldByName("info").getMessageType();
        DynamicMessage infoMessage = DynamicMessage.newBuilder(info)
                .setField(info.findFieldByName("text"), "renamed")
                .setField(info.findFieldByName("mass"), 42L)
                .build();
        Descriptor entry = writer.findFieldByName("info_map").getMessageType();
        DynamicMessage message = DynamicMessage.newBuilder(writer)
                .setField(writer.findFieldByName("title"), "Trino")
                .setField(writer.findFieldByName("total"), 7L)
                .setField(writer.findFieldByName("info"), infoMessage)
                .addRepeatedField(writer.findFieldByName("infos"), infoMessage)
                .addRepeatedField(writer.findFieldByName("info_map"), DynamicMessage.newBuilder(entry)
                        .setField(entry.findFieldByName("key"), "first")
                        .setField(entry.findFieldByName("value"), infoMessage)
                        .build())
                .build();
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = decoder.decodeRow(message.toByteArray()).orElseThrow();

        checkValue(decodedRow, nameColumn, "Trino");
        checkValue(decodedRow, countColumn, 7L);
        checkValue(decodedRow, weightColumn, 42L);
        assertDetail((SqlRow) decodedRow.get(detailColumn).getObject());
        Block details = (Block) decodedRow.get(detailsColumn).getObject();
        assertThat(details.getPositionCount()).isEqualTo(1);
        assertDetail(detailType.getObject(details, 0));
        SqlMap detailMap = (SqlMap) decodedRow.get(detailMapColumn).getObject();
        assertThat(detailMap.getSize()).isEqualTo(1);
        assertThat(VARCHAR.getSlice(detailMap.getRawKeyBlock(), detailMap.getRawOffset()).toStringUtf8()).isEqualTo("first");
        assertDetail(detailType.getObject(detailMap.getRawValueBlock(), detailMap.getRawOffset()));
    }

    private static void assertDetail(SqlRow detail)
    {
        assertThat(VARCHAR.getSlice(detail.getRawFieldBlock(0), detail.getRawIndex()).toStringUtf8()).isEqualTo("renamed");
        assertThat(BIGINT.getLong(detail.getRawFieldBlock(1), detail.getRawIndex())).isEqualTo(42L);
    }

    @Test
    public void testFieldNumberIdentifiesTheField()
    {
        // the writer has fields of the same names under other numbers: on the wire those are different fields
        Descriptor reader = messageDescriptor(
                """
                syntax = "proto3";
                message schema {
                  string name = 1;
                  int64 count = 2;
                  message Detail {
                    string note = 1;
                  }
                  Detail detail = 3;
                }
                """);
        Descriptor writer = messageDescriptor(
                """
                syntax = "proto3";
                message schema {
                  string name = 1;
                  int64 count = 7;
                  message Detail {
                    string note = 8;
                  }
                  Detail detail = 3;
                }
                """);
        RowType detailType = RowType.from(ImmutableList.of(RowType.field("note", VARCHAR)));
        DecoderTestColumnHandle nameColumn = new DecoderTestColumnHandle(0, "name", VARCHAR, "name", null, null, false, false, false);
        DecoderTestColumnHandle countColumn = new DecoderTestColumnHandle(1, "count", BIGINT, "count", null, null, false, false, false);
        DecoderTestColumnHandle detailColumn = new DecoderTestColumnHandle(2, "detail", detailType, "detail", null, null, false, false, false);
        DecoderTestColumnHandle noteColumn = new DecoderTestColumnHandle(3, "note", VARCHAR, "detail/note", null, null, false, false, false);
        Set<DecoderColumnHandle> columns = ImmutableSet.of(nameColumn, countColumn, detailColumn, noteColumn);

        Descriptor writerDetail = writer.findFieldByName("detail").getMessageType();
        byte[] record = DynamicMessage.newBuilder(writer)
                .setField(writer.findFieldByName("name"), "Trino")
                .setField(writer.findFieldByName("count"), 7L)
                .setField(writer.findFieldByName("detail"), DynamicMessage.newBuilder(writerDetail).setField(writerDetail.findFieldByName("note"), "n").build())
                .build()
                .toByteArray();

        Map<DecoderColumnHandle, FieldValueProvider> byNumber = new ProtobufRowDecoder(new EvolvedSchemaProvider(writer, Optional.of(reader)), columns, TESTING_TYPE_MANAGER, new FileDescriptorProvider())
                .decodeRow(record)
                .orElseThrow();
        checkValue(byNumber, nameColumn, "Trino");
        assertThat(byNumber.get(countColumn).isNull()).isTrue();
        assertThat(byNumber.get(noteColumn).isNull()).isTrue();
        SqlRow detail = (SqlRow) byNumber.get(detailColumn).getObject();
        assertThat(detail.getRawFieldBlock(0).isNull(detail.getRawIndex())).isTrue();

        // without a reader schema, the names identify the fields
        Map<DecoderColumnHandle, FieldValueProvider> byName = new ProtobufRowDecoder(new EvolvedSchemaProvider(writer, Optional.empty()), columns, TESTING_TYPE_MANAGER, new FileDescriptorProvider())
                .decodeRow(record)
                .orElseThrow();
        checkValue(byName, nameColumn, "Trino");
        checkValue(byName, countColumn, 7L);
        checkValue(byName, noteColumn, "n");
    }

    @Test
    public void testFieldNumberReusedWithAnotherEncodingIsNull()
    {
        Descriptor reader = messageDescriptor(
                """
                syntax = "proto3";
                message schema {
                  string name = 1;
                  message Detail {
                    string note = 1;
                  }
                  Detail detail = 2;
                  repeated string tags = 3;
                  int64 count = 4;
                  int64 total = 5;
                }
                """);
        Descriptor writer = messageDescriptor(
                """
                syntax = "proto3";
                message schema {
                  int64 name = 1;
                  string detail = 2;
                  string tags = 3;
                  int32 count = 4;
                  uint64 total = 5;
                }
                """);
        DecoderTestColumnHandle nameColumn = new DecoderTestColumnHandle(0, "name", VARCHAR, "name", null, null, false, false, false);
        DecoderTestColumnHandle detailColumn = new DecoderTestColumnHandle(1, "detail", RowType.from(ImmutableList.of(RowType.field("note", VARCHAR))), "detail", null, null, false, false, false);
        DecoderTestColumnHandle tagsColumn = new DecoderTestColumnHandle(2, "tags", new ArrayType(VARCHAR), "tags", null, null, false, false, false);
        DecoderTestColumnHandle countColumn = new DecoderTestColumnHandle(3, "count", BIGINT, "count", null, null, false, false, false);
        DecoderTestColumnHandle totalColumn = new DecoderTestColumnHandle(4, "total", BIGINT, "total", null, null, false, false, false);
        ProtobufRowDecoder decoder = new ProtobufRowDecoder(
                new EvolvedSchemaProvider(writer, Optional.of(reader)),
                ImmutableSet.of(nameColumn, detailColumn, tagsColumn, countColumn, totalColumn),
                TESTING_TYPE_MANAGER,
                new FileDescriptorProvider());

        byte[] record = DynamicMessage.newBuilder(writer)
                .setField(writer.findFieldByName("name"), 1L)
                .setField(writer.findFieldByName("detail"), "d")
                .setField(writer.findFieldByName("tags"), "t")
                .setField(writer.findFieldByName("count"), 5)
                .setField(writer.findFieldByName("total"), 6L)
                .build()
                .toByteArray();
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = decoder.decodeRow(record).orElseThrow();

        // a field whose number now carries another kind of value is not the column's field
        assertThat(decodedRow.get(nameColumn).isNull()).isTrue();
        assertThat(decodedRow.get(detailColumn).isNull()).isTrue();
        assertThat(decodedRow.get(tagsColumn).isNull()).isTrue();
        // integer types that share an encoding are the same field
        checkValue(decodedRow, countColumn, 5L);
        checkValue(decodedRow, totalColumn, 6L);
    }

    @Test
    public void testWireCompatibleTypeChangesAreReadAsReaderType()
            throws Exception
    {
        // the writer changed each field to another type with the same encoding, which reads the same bytes differently
        Descriptor reader = messageDescriptor(
                """
                syntax = "proto3";
                message schema {
                  bool flag = 1;
                  int32 small = 2;
                  int64 large = 3;
                  enum Color {
                    RED = 0;
                    GREEN = 1;
                    BLUE = 2;
                  }
                  Color color = 4;
                  int64 color_number = 5;
                  string text = 6;
                  bytes data = 7;
                  sint64 delta = 8;
                  repeated int64 counts = 9;
                  map<string, int64> totals = 10;
                  enum Size {
                    SMALL = 0;
                    LARGE = 1;
                  }
                  Size size = 11;
                  sint32 narrow = 12;
                }
                """);
        Descriptor writer = messageDescriptor(
                """
                syntax = "proto3";
                message schema {
                  int32 flag = 1;
                  int64 small = 2;
                  uint32 large = 3;
                  int32 color = 4;
                  enum Color {
                    RED = 0;
                    GREEN = 1;
                    BLUE = 2;
                  }
                  Color color_number = 5;
                  bytes text = 6;
                  string data = 7;
                  sint32 delta = 8;
                  repeated int32 counts = 9;
                  map<string, int32> totals = 10;
                  enum Size {
                    TINY = 0;
                    HUGE = 1;
                  }
                  Size size = 11;
                  sint64 narrow = 12;
                }
                """);
        DecoderTestColumnHandle flagColumn = new DecoderTestColumnHandle(0, "flag", BOOLEAN, "flag", null, null, false, false, false);
        DecoderTestColumnHandle smallColumn = new DecoderTestColumnHandle(1, "small", INTEGER, "small", null, null, false, false, false);
        DecoderTestColumnHandle largeColumn = new DecoderTestColumnHandle(2, "large", BIGINT, "large", null, null, false, false, false);
        DecoderTestColumnHandle colorColumn = new DecoderTestColumnHandle(3, "color", VARCHAR, "color", null, null, false, false, false);
        DecoderTestColumnHandle colorNumberColumn = new DecoderTestColumnHandle(4, "color_number", BIGINT, "color_number", null, null, false, false, false);
        DecoderTestColumnHandle textColumn = new DecoderTestColumnHandle(5, "text", VARCHAR, "text", null, null, false, false, false);
        DecoderTestColumnHandle dataColumn = new DecoderTestColumnHandle(6, "data", VARBINARY, "data", null, null, false, false, false);
        DecoderTestColumnHandle deltaColumn = new DecoderTestColumnHandle(7, "delta", BIGINT, "delta", null, null, false, false, false);
        DecoderTestColumnHandle countsColumn = new DecoderTestColumnHandle(8, "counts", new ArrayType(BIGINT), "counts", null, null, false, false, false);
        MapType totalsType = new MapType(VARCHAR, BIGINT, TESTING_TYPE_MANAGER.getTypeOperators());
        DecoderTestColumnHandle totalsColumn = new DecoderTestColumnHandle(9, "totals", totalsType, "totals", null, null, false, false, false);
        DecoderTestColumnHandle sizeColumn = new DecoderTestColumnHandle(10, "size", VARCHAR, "size", null, null, false, false, false);
        DecoderTestColumnHandle narrowColumn = new DecoderTestColumnHandle(11, "narrow", INTEGER, "narrow", null, null, false, false, false);
        ProtobufRowDecoder decoder = new ProtobufRowDecoder(
                new EvolvedSchemaProvider(writer, Optional.of(reader)),
                ImmutableSet.of(flagColumn, smallColumn, largeColumn, colorColumn, colorNumberColumn, textColumn, dataColumn, deltaColumn, countsColumn, totalsColumn, sizeColumn, narrowColumn),
                TESTING_TYPE_MANAGER,
                new FileDescriptorProvider());

        Descriptor totalsEntry = writer.findFieldByName("totals").getMessageType();
        byte[] record = DynamicMessage.newBuilder(writer)
                .setField(writer.findFieldByName("flag"), 7)
                .setField(writer.findFieldByName("small"), (1L << 32) + 5)
                .setField(writer.findFieldByName("large"), (int) 3_000_000_000L)
                .setField(writer.findFieldByName("color"), 2)
                .setField(writer.findFieldByName("color_number"), writer.findEnumTypeByName("Color").findValueByName("BLUE"))
                .setField(writer.findFieldByName("text"), ByteString.copyFromUtf8("héllo"))
                .setField(writer.findFieldByName("data"), "bytes")
                .setField(writer.findFieldByName("delta"), -5)
                .addRepeatedField(writer.findFieldByName("counts"), 1)
                .addRepeatedField(writer.findFieldByName("counts"), -2)
                .addRepeatedField(writer.findFieldByName("totals"), DynamicMessage.newBuilder(totalsEntry)
                        .setField(totalsEntry.findFieldByName("key"), "a")
                        .setField(totalsEntry.findFieldByName("value"), -3)
                        .build())
                .setField(writer.findFieldByName("size"), writer.findEnumTypeByName("Size").findValueByName("HUGE"))
                .setField(writer.findFieldByName("narrow"), (1L << 31) + 7)
                .build()
                .toByteArray();
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = decoder.decodeRow(record).orElseThrow();

        checkValue(decodedRow, flagColumn, true);
        checkValue(decodedRow, smallColumn, 5);
        checkValue(decodedRow, largeColumn, 3_000_000_000L);
        checkValue(decodedRow, colorColumn, "BLUE");
        checkValue(decodedRow, colorNumberColumn, 2);
        checkValue(decodedRow, textColumn, "héllo");
        checkValue(decodedRow, dataColumn, Slices.utf8Slice("bytes"));
        checkValue(decodedRow, deltaColumn, -5);
        Block counts = (Block) decodedRow.get(countsColumn).getObject();
        assertThat(counts.getPositionCount()).isEqualTo(2);
        assertThat(BIGINT.getLong(counts, 0)).isEqualTo(1);
        assertThat(BIGINT.getLong(counts, 1)).isEqualTo(-2);
        SqlMap totals = (SqlMap) decodedRow.get(totalsColumn).getObject();
        assertThat(totals.getSize()).isEqualTo(1);
        assertThat(BIGINT.getLong(totals.getRawValueBlock(), totals.getRawOffset())).isEqualTo(-3);
        // an enum value renamed in the reader schema reads with its new name
        checkValue(decodedRow, sizeColumn, "LARGE");
        // a 32 bit field reads the low 32 bits of the zigzag encoded value
        checkValue(decodedRow, narrowColumn, 7);

        // these are the values protobuf reads with the reader schema
        DynamicMessage readerMessage = DynamicMessage.parseFrom(reader, record);
        assertThat(readerMessage.getField(reader.findFieldByName("flag"))).isEqualTo(true);
        assertThat(readerMessage.getField(reader.findFieldByName("small"))).isEqualTo(5);
        assertThat(readerMessage.getField(reader.findFieldByName("large"))).isEqualTo(3_000_000_000L);
        assertThat(readerMessage.getField(reader.findFieldByName("color"))).hasToString("BLUE");
        assertThat(readerMessage.getField(reader.findFieldByName("color_number"))).isEqualTo(2L);
        assertThat(readerMessage.getField(reader.findFieldByName("delta"))).isEqualTo(-5L);
        assertThat(readerMessage.getField(reader.findFieldByName("counts"))).isEqualTo(ImmutableList.of(1L, -2L));
        assertThat(readerMessage.getField(reader.findFieldByName("size"))).hasToString("LARGE");
        assertThat(readerMessage.getField(reader.findFieldByName("narrow"))).isEqualTo(7);
    }

    @Test
    public void testIncompatibleMessageTypesAreNull()
    {
        Descriptor reader = messageDescriptor(
                """
                syntax = "proto3";
                import "google/protobuf/timestamp.proto";
                message schema {
                  google.protobuf.Timestamp created = 1;
                  message Detail {
                    int64 seconds = 1;
                  }
                  Detail detail = 2;
                  map<string, string> labels = 3;
                  map<string, string> notes = 4;
                }
                """);
        Descriptor writer = messageDescriptor(
                """
                syntax = "proto3";
                import "google/protobuf/timestamp.proto";
                message schema {
                  message Instant {
                    int64 seconds = 1;
                  }
                  Instant created = 1;
                  google.protobuf.Timestamp detail = 2;
                  map<int32, string> labels = 3;
                  map<string, bytes> notes = 4;
                }
                """);
        DecoderTestColumnHandle createdColumn = new DecoderTestColumnHandle(0, "created", TIMESTAMP_MICROS, "created", null, null, false, false, false);
        DecoderTestColumnHandle detailColumn = new DecoderTestColumnHandle(1, "detail", RowType.from(ImmutableList.of(RowType.field("seconds", BIGINT))), "detail", null, null, false, false, false);
        DecoderTestColumnHandle labelsColumn = new DecoderTestColumnHandle(2, "labels", new MapType(VARCHAR, VARCHAR, TESTING_TYPE_MANAGER.getTypeOperators()), "labels", null, null, false, false, false);
        DecoderTestColumnHandle notesColumn = new DecoderTestColumnHandle(3, "notes", new MapType(VARCHAR, VARCHAR, TESTING_TYPE_MANAGER.getTypeOperators()), "notes", null, null, false, false, false);
        ProtobufRowDecoder decoder = new ProtobufRowDecoder(
                new EvolvedSchemaProvider(writer, Optional.of(reader)),
                ImmutableSet.of(createdColumn, detailColumn, labelsColumn, notesColumn),
                TESTING_TYPE_MANAGER,
                new FileDescriptorProvider());

        Descriptor instant = writer.findFieldByName("created").getMessageType();
        Descriptor labelsEntry = writer.findFieldByName("labels").getMessageType();
        Descriptor notesEntry = writer.findFieldByName("notes").getMessageType();
        byte[] record = DynamicMessage.newBuilder(writer)
                .setField(writer.findFieldByName("created"), DynamicMessage.newBuilder(instant).setField(instant.findFieldByName("seconds"), 1L).build())
                .setField(writer.findFieldByName("detail"), Timestamp.newBuilder().setSeconds(2).build())
                .addRepeatedField(writer.findFieldByName("labels"), DynamicMessage.newBuilder(labelsEntry)
                        .setField(labelsEntry.findFieldByName("key"), 1)
                        .setField(labelsEntry.findFieldByName("value"), "one")
                        .build())
                .addRepeatedField(writer.findFieldByName("notes"), DynamicMessage.newBuilder(notesEntry)
                        .setField(notesEntry.findFieldByName("key"), "a")
                        .setField(notesEntry.findFieldByName("value"), ByteString.copyFromUtf8("b"))
                        .build())
                .build()
                .toByteArray();
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = decoder.decodeRow(record).orElseThrow();

        // a message type mapped to a column type of its own is not interchangeable with other message types
        assertThat(decodedRow.get(createdColumn).isNull()).isTrue();
        assertThat(decodedRow.get(detailColumn).isNull()).isTrue();
        // the key and value of a map must be compatible like fields
        assertThat(decodedRow.get(labelsColumn).isNull()).isTrue();
        SqlMap notes = (SqlMap) decodedRow.get(notesColumn).getObject();
        assertThat(VARCHAR.getSlice(notes.getRawValueBlock(), notes.getRawOffset()).toStringUtf8()).isEqualTo("b");
    }

    @Test
    public void testRecordOfAnotherMessageTypeIsReadByName()
    {
        Descriptor reader = messageDescriptor(
                """
                syntax = "proto3";
                message schema {
                  string name = 1;
                  int64 count = 2;
                }
                """);
        Descriptor writer = ProtobufUtils.getFileDescriptor(
                """
                syntax = "proto3";
                message other {
                  int64 count = 1;
                  string name = 2;
                }
                """).findMessageTypeByName("other");
        DecoderTestColumnHandle nameColumn = new DecoderTestColumnHandle(0, "name", VARCHAR, "name", null, null, false, false, false);
        DecoderTestColumnHandle countColumn = new DecoderTestColumnHandle(1, "count", BIGINT, "count", null, null, false, false, false);
        ProtobufRowDecoder decoder = new ProtobufRowDecoder(
                new EvolvedSchemaProvider(writer, Optional.of(reader)),
                ImmutableSet.of(nameColumn, countColumn),
                TESTING_TYPE_MANAGER,
                new FileDescriptorProvider());

        byte[] record = DynamicMessage.newBuilder(writer)
                .setField(writer.findFieldByName("count"), 3L)
                .setField(writer.findFieldByName("name"), "Trino")
                .build()
                .toByteArray();
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = decoder.decodeRow(record).orElseThrow();

        // field numbers identify fields only within a message type
        checkValue(decodedRow, nameColumn, "Trino");
        checkValue(decodedRow, countColumn, 3);
    }

    @Test
    public void testUnsetFieldsWithPresenceAreNull()
    {
        Descriptor descriptor = messageDescriptor(
                """
                syntax = "proto2";
                message schema {
                  optional int64 count = 1;
                  optional string label = 2 [default = "none"];
                  required int32 id = 3;
                  oneof choice {
                    int64 number = 4;
                    string word = 5;
                  }
                  repeated int64 values = 6;
                }
                """);
        DecoderTestColumnHandle countColumn = new DecoderTestColumnHandle(0, "count", BIGINT, "count", null, null, false, false, false);
        DecoderTestColumnHandle labelColumn = new DecoderTestColumnHandle(1, "label", VARCHAR, "label", null, null, false, false, false);
        DecoderTestColumnHandle idColumn = new DecoderTestColumnHandle(2, "id", INTEGER, "id", null, null, false, false, false);
        DecoderTestColumnHandle numberColumn = new DecoderTestColumnHandle(3, "number", BIGINT, "number", null, null, false, false, false);
        DecoderTestColumnHandle wordColumn = new DecoderTestColumnHandle(4, "word", VARCHAR, "word", null, null, false, false, false);
        DecoderTestColumnHandle valuesColumn = new DecoderTestColumnHandle(5, "values", new ArrayType(BIGINT), "values", null, null, false, false, false);
        ProtobufRowDecoder decoder = new ProtobufRowDecoder(
                new FixedSchemaDynamicMessageProvider(descriptor),
                ImmutableSet.of(countColumn, labelColumn, idColumn, numberColumn, wordColumn, valuesColumn),
                TESTING_TYPE_MANAGER,
                new FileDescriptorProvider());

        byte[] record = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("id"), 1)
                .setField(descriptor.findFieldByName("word"), "w")
                .build()
                .toByteArray();
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = decoder.decodeRow(record).orElseThrow();

        // an unset field that tracks presence is NULL, even with a declared default value
        assertThat(decodedRow.get(countColumn).isNull()).isTrue();
        assertThat(decodedRow.get(labelColumn).isNull()).isTrue();
        assertThat(decodedRow.get(numberColumn).isNull()).isTrue();
        checkValue(decodedRow, idColumn, 1);
        checkValue(decodedRow, wordColumn, "w");
        // a repeated field has no presence: unset, it is empty
        assertThat(((Block) decodedRow.get(valuesColumn).getObject()).getPositionCount()).isZero();
    }

    @Test
    public void testDecoderWithoutColumnsDoesNotLookUpReaderSchema()
    {
        DynamicMessageProvider provider = new DynamicMessageProvider()
        {
            @Override
            public DynamicMessage parseDynamicMessage(byte[] data)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Optional<Descriptor> getReaderDescriptor()
            {
                throw new AssertionError("reader schema looked up");
            }
        };
        assertThatNoException().isThrownBy(() -> new ProtobufRowDecoder(provider, ImmutableSet.of(), TESTING_TYPE_MANAGER, new FileDescriptorProvider()));
    }

    private static Descriptor messageDescriptor(String protoFile)
    {
        return ProtobufUtils.getFileDescriptor(protoFile).findMessageTypeByName(DEFAULT_MESSAGE);
    }

    /**
     * Parses records with a schema other than the one the columns were derived from.
     */
    private record EvolvedSchemaProvider(Descriptor writerDescriptor, Optional<Descriptor> readerDescriptor)
            implements DynamicMessageProvider
    {
        @Override
        public DynamicMessage parseDynamicMessage(byte[] data)
        {
            return new FixedSchemaDynamicMessageProvider(writerDescriptor).parseDynamicMessage(data);
        }

        @Override
        public Optional<Descriptor> getReaderDescriptor()
        {
            return readerDescriptor;
        }
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
        return DECODER_FACTORY.create(TESTING_SESSION, new RowDecoderSpec(ProtobufRowDecoder.NAME, ImmutableMap.of("dataSchema", ProtobufUtils.getProtoFile("decoder/protobuf/" + fileName)), columns));
    }

    private Descriptor getDescriptor(String fileName)
            throws IOException
    {
        return ProtobufUtils.getFileDescriptor(ProtobufUtils.getProtoFile("decoder/protobuf/" + fileName)).findMessageTypeByName(DEFAULT_MESSAGE);
    }
}
