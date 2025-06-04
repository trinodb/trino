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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.google.protobuf.Any;
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
import io.trino.decoder.RowDecoderSpec;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlVarbinary;
import io.trino.testing.TestingSession;
import io.trino.type.JsonType;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.io.Resources.getResource;
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
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
            assertThat(descriptor.findFieldByName(oneofColumnName)).isNull();
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
        final var decoder = new ProtobufRowDecoder(new FixedSchemaDynamicMessageProvider(descriptor), ImmutableSet.of(testColumnHandle, testOneofColumn), TESTING_TYPE_MANAGER, new FileDescriptorProvider());

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = decoder
                .decodeRow(message.toByteArray())
                .orElseThrow(AssertionError::new);

        assertThat(decodedRow).hasSize(2);

        final var obj = new ObjectMapper();
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

        JsonNode actual = new ObjectMapper().readTree(decodedRow.get(testOneOfColumn).getSlice().toStringUtf8());
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

        assertThat(decodedRow).hasSize(3);

        Block listBlock = (Block) decodedRow.get(listColumn).getObject();
        assertThat(VARCHAR.getSlice(listBlock, 0).toStringUtf8()).isEqualTo("Presto");

        SqlMap sqlMap = (SqlMap) decodedRow.get(mapColumn).getObject();
        assertThat(VARCHAR.getSlice(sqlMap.getRawKeyBlock(), sqlMap.getRawOffset()).toStringUtf8()).isEqualTo("Key");
        assertThat(VARCHAR.getSlice(sqlMap.getRawValueBlock(), sqlMap.getRawOffset()).toStringUtf8()).isEqualTo("Value");

        SqlRow sqlRow = (SqlRow) decodedRow.get(rowColumn).getObject();
        int rawIndex = sqlRow.getRawIndex();
        ConnectorSession session = TestingSession.testSessionBuilder().build().toConnectorSession();
        assertThat(VARCHAR.getObjectValue(session, sqlRow.getRawFieldBlock(0), rawIndex)).isEqualTo(stringData);
        assertThat(INTEGER.getObjectValue(session, sqlRow.getRawFieldBlock(1), rawIndex)).isEqualTo(integerData);
        assertThat(BIGINT.getObjectValue(session, sqlRow.getRawFieldBlock(2), rawIndex)).isEqualTo(longData);
        assertThat(DOUBLE.getObjectValue(session, sqlRow.getRawFieldBlock(3), rawIndex)).isEqualTo(doubleData);
        assertThat(REAL.getObjectValue(session, sqlRow.getRawFieldBlock(4), rawIndex)).isEqualTo(floatData);
        assertThat(BOOLEAN.getObjectValue(session, sqlRow.getRawFieldBlock(5), rawIndex)).isEqualTo(booleanData);
        assertThat(VARCHAR.getObjectValue(session, sqlRow.getRawFieldBlock(6), rawIndex)).isEqualTo(enumData);
        assertThat(TIMESTAMP_MICROS.getObjectValue(session, sqlRow.getRawFieldBlock(7), rawIndex)).isEqualTo(sqlTimestamp.roundTo(6));
        assertThat(VARBINARY.getObjectValue(session, sqlRow.getRawFieldBlock(8), rawIndex)).isEqualTo(new SqlVarbinary(bytesData));
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

        assertThatThrownBy(() -> decodedRow.get(rowColumn).getObject())
                .hasMessageMatching("Unknown Field unknown_mapping");
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
            throws Exception
    {
        return ProtobufUtils.getFileDescriptor(ProtobufUtils.getProtoFile("decoder/protobuf/" + fileName)).findMessageTypeByName(DEFAULT_MESSAGE);
    }
}
