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
package io.trino.decoder.avro;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slices;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.DecoderTestColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.decoder.RowDecoder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.FieldBuilder;
import org.apache.avro.SchemaBuilder.GenericDefault;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.EnumSymbol;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.assertj.core.api.ThrowableAssert;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.trino.decoder.avro.AvroDecoderTestUtil.checkArrayValues;
import static io.trino.decoder.avro.AvroDecoderTestUtil.checkMapValues;
import static io.trino.decoder.avro.AvroDecoderTestUtil.checkRowValues;
import static io.trino.decoder.util.DecoderTestUtil.checkIsNull;
import static io.trino.decoder.util.DecoderTestUtil.checkValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestAvroDecoder
{
    private static final String DATA_SCHEMA = "dataSchema";
    private static final AvroRowDecoderFactory DECODER_FACTORY = new AvroRowDecoderFactory(new FixedSchemaAvroReaderSupplier.Factory(), new AvroFileDeserializer.Factory());

    private static final Type VARCHAR_MAP_TYPE = TESTING_TYPE_MANAGER.getType(mapType(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()));
    private static final Type DOUBLE_MAP_TYPE = TESTING_TYPE_MANAGER.getType(mapType(VARCHAR.getTypeSignature(), DOUBLE.getTypeSignature()));
    private static final Type REAL_MAP_TYPE = TESTING_TYPE_MANAGER.getType(mapType(VARCHAR.getTypeSignature(), REAL.getTypeSignature()));
    private static final Type MAP_OF_REAL_MAP_TYPE = TESTING_TYPE_MANAGER.getType(mapType(VARCHAR.getTypeSignature(), REAL_MAP_TYPE.getTypeSignature()));
    private static final Type MAP_OF_ARRAY_OF_MAP_TYPE = TESTING_TYPE_MANAGER.getType(mapType(VARCHAR.getTypeSignature(), new ArrayType(REAL_MAP_TYPE).getTypeSignature()));
    private static final Type MAP_OF_RECORD = TESTING_TYPE_MANAGER.getType(mapType(VARCHAR.getTypeSignature(),
            RowType.from(ImmutableList.<RowType.Field>builder()
                    .add(RowType.field("sf1", DOUBLE))
                    .add(RowType.field("sf2", BOOLEAN))
                    .build()).getTypeSignature()));

    private static Schema getAvroSchema(Map<String, String> fields)
    {
        Schema.Parser parser = new Schema.Parser();
        FieldAssembler<Schema> fieldAssembler = getFieldBuilder();
        for (Map.Entry<String, String> field : fields.entrySet()) {
            FieldBuilder<Schema> fieldBuilder = fieldAssembler.name(field.getKey());
            Schema fieldSchema = parser.parse(field.getValue());
            GenericDefault<Schema> genericDefault = fieldBuilder.type(fieldSchema);
            switch (fieldSchema.getType()) {
                case ARRAY:
                    genericDefault.withDefault(ImmutableList.of());
                    break;
                case MAP:
                    genericDefault.withDefault(ImmutableMap.of());
                    break;
                case UNION:
                    if (fieldSchema.getTypes().stream()
                            .map(Schema::getType)
                            .anyMatch(Schema.Type.NULL::equals)) {
                        genericDefault.withDefault(null);
                    }
                    else {
                        genericDefault.noDefault();
                    }
                    break;
                case NULL:
                    genericDefault.withDefault(null);
                    break;
                default:
                    genericDefault.noDefault();
            }
        }
        return fieldAssembler.endRecord();
    }

    private static FieldAssembler<Schema> getFieldBuilder()
    {
        return SchemaBuilder.record("test_schema")
                .namespace("io.trino.decoder.avro")
                .fields();
    }

    private Map<DecoderColumnHandle, FieldValueProvider> buildAndDecodeColumns(Set<DecoderColumnHandle> columns, Map<String, String> fieldSchema, Map<String, Object> fieldValue)
    {
        Schema schema = getAvroSchema(fieldSchema);
        byte[] avroData = buildAvroData(schema, fieldValue);

        return decodeRow(
                avroData,
                columns,
                ImmutableMap.of(DATA_SCHEMA, schema.toString()));
    }

    private Map<DecoderColumnHandle, FieldValueProvider> buildAndDecodeColumn(DecoderTestColumnHandle column, String columnName, String columnType, Object actualValue)
    {
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumns(
                ImmutableSet.of(column),
                ImmutableMap.of(columnName, columnType),
                ImmutableMap.of(columnName, actualValue));

        assertEquals(decodedRow.size(), 1);
        return decodedRow;
    }

    private static Map<DecoderColumnHandle, FieldValueProvider> decodeRow(byte[] avroData, Set<DecoderColumnHandle> columns, Map<String, String> dataParams)
    {
        RowDecoder rowDecoder = DECODER_FACTORY.create(dataParams, columns);
        return rowDecoder.decodeRow(avroData)
                .orElseThrow(AssertionError::new);
    }

    private static byte[] buildAvroData(Schema schema, String name, Object value)
    {
        return buildAvroData(schema, ImmutableMap.of(name, value));
    }

    private static byte[] buildAvroData(Schema schema, Map<String, Object> values)
    {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        buildAvroRecord(schema, outputStream, values);
        return outputStream.toByteArray();
    }

    private static <V> Map<String, V> buildMapFromKeysAndValues(List<String> keys, List<V> values)
    {
        assertEquals(keys.size(), values.size());
        Map<String, V> map = new HashMap<>();
        for (int i = 0; i < keys.size(); i++) {
            map.put(keys.get(i), values.get(i));
        }
        return map;
    }

    private static GenericData.Record buildAvroRecord(Schema schema, ByteArrayOutputStream outputStream, Map<String, Object> values)
    {
        GenericData.Record record = new GenericData.Record(schema);
        values.forEach(record::put);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            dataFileWriter.create(schema, outputStream);
            dataFileWriter.append(record);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to convert to Avro.", e);
        }
        return record;
    }

    @Test
    public void testStringDecodedAsVarchar()
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", VARCHAR, "string_field", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "string_field", "\"string\"", "Mon Jul 28 20:38:07 +0000 2014");

        checkValue(decodedRow, row, "Mon Jul 28 20:38:07 +0000 2014");
    }

    @Test
    public void testEnumDecodedAsVarchar()
    {
        Schema schema = SchemaBuilder.record("record")
                .fields()
                .name("enum_field")
                .type()
                .enumeration("Weekday")
                .symbols("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
                .noDefault()
                .endRecord();
        Schema enumType = schema.getField("enum_field").schema();
        EnumSymbol enumValue = new GenericData.EnumSymbol(enumType, "Wednesday");
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", VARCHAR, "enum_field", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "enum_field", enumType.toString(), enumValue);

        checkValue(decodedRow, row, "Wednesday");
    }

    @Test
    public void testSchemaEvolutionAddingColumn()
    {
        DecoderTestColumnHandle originalColumn = new DecoderTestColumnHandle(0, "row0", VARCHAR, "string_field", null, null, false, false, false);
        DecoderTestColumnHandle newlyAddedColumn = new DecoderTestColumnHandle(1, "row1", VARCHAR, "string_field_added", null, null, false, false, false);

        // the decoded avro data file does not have string_field_added
        byte[] originalData = buildAvroData(getFieldBuilder()
                        .name("string_field").type().stringType().noDefault()
                        .endRecord(),
                "string_field", "string_field_value");
        String addedColumnSchema = getFieldBuilder()
                .name("string_field").type().stringType().noDefault()
                .name("string_field_added").type().optional().stringType()
                .endRecord().toString();
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = decodeRow(
                originalData,
                ImmutableSet.of(originalColumn, newlyAddedColumn),
                ImmutableMap.of(DATA_SCHEMA, addedColumnSchema));

        assertEquals(decodedRow.size(), 2);
        checkValue(decodedRow, originalColumn, "string_field_value");
        checkIsNull(decodedRow, newlyAddedColumn);
    }

    @Test
    public void testSchemaEvolutionRenamingColumn()
    {
        byte[] originalData = buildAvroData(getFieldBuilder()
                        .name("string_field").type().stringType().noDefault()
                        .endRecord(),
                "string_field", "string_field_value");

        DecoderTestColumnHandle renamedColumn = new DecoderTestColumnHandle(0, "row0", VARCHAR, "string_field_renamed", null, null, false, false, false);
        String renamedColumnSchema = getFieldBuilder()
                .name("string_field_renamed").type().optional().stringType()
                .endRecord()
                .toString();
        Map<DecoderColumnHandle, FieldValueProvider> decodedEvolvedRow = decodeRow(
                originalData,
                ImmutableSet.of(renamedColumn),
                ImmutableMap.of(DATA_SCHEMA, renamedColumnSchema));

        assertEquals(decodedEvolvedRow.size(), 1);
        checkIsNull(decodedEvolvedRow, renamedColumn);
    }

    @Test
    public void testSchemaEvolutionRemovingColumn()
    {
        byte[] originalData = buildAvroData(getFieldBuilder()
                        .name("string_field").type().stringType().noDefault()
                        .name("string_field_to_be_removed").type().optional().stringType()
                        .endRecord(),
                ImmutableMap.of(
                        "string_field", "string_field_value",
                        "string_field_to_be_removed", "removed_field_value"));

        DecoderTestColumnHandle evolvedColumn = new DecoderTestColumnHandle(0, "row0", VARCHAR, "string_field", null, null, false, false, false);
        String removedColumnSchema = getFieldBuilder()
                .name("string_field").type().stringType().noDefault()
                .endRecord()
                .toString();
        Map<DecoderColumnHandle, FieldValueProvider> decodedEvolvedRow = decodeRow(
                originalData,
                ImmutableSet.of(evolvedColumn),
                ImmutableMap.of(DATA_SCHEMA, removedColumnSchema));

        assertEquals(decodedEvolvedRow.size(), 1);
        checkValue(decodedEvolvedRow, evolvedColumn, "string_field_value");
    }

    @Test
    public void testSchemaEvolutionIntToLong()
    {
        byte[] originalIntData = buildAvroData(getFieldBuilder()
                        .name("int_to_long_field").type().intType().noDefault()
                        .endRecord(),
                "int_to_long_field", 100);

        DecoderTestColumnHandle longColumnReadingIntData = new DecoderTestColumnHandle(0, "row0", BIGINT, "int_to_long_field", null, null, false, false, false);
        String changedTypeSchema = getFieldBuilder()
                .name("int_to_long_field").type().longType().noDefault()
                .endRecord()
                .toString();
        Map<DecoderColumnHandle, FieldValueProvider> decodedEvolvedRow = decodeRow(
                originalIntData,
                ImmutableSet.of(longColumnReadingIntData),
                ImmutableMap.of(DATA_SCHEMA, changedTypeSchema));

        assertEquals(decodedEvolvedRow.size(), 1);
        checkValue(decodedEvolvedRow, longColumnReadingIntData, 100);
    }

    @Test
    public void testSchemaEvolutionIntToDouble()
    {
        byte[] originalIntData = buildAvroData(getFieldBuilder()
                        .name("int_to_double_field").type().intType().noDefault()
                        .endRecord(),
                "int_to_double_field", 100);

        DecoderTestColumnHandle doubleColumnReadingIntData = new DecoderTestColumnHandle(0, "row0", DOUBLE, "int_to_double_field", null, null, false, false, false);
        String changedTypeSchema = getFieldBuilder()
                .name("int_to_double_field").type().doubleType().noDefault()
                .endRecord()
                .toString();
        Map<DecoderColumnHandle, FieldValueProvider> decodedEvolvedRow = decodeRow(
                originalIntData,
                ImmutableSet.of(doubleColumnReadingIntData),
                ImmutableMap.of(DATA_SCHEMA, changedTypeSchema));

        assertEquals(decodedEvolvedRow.size(), 1);
        checkValue(decodedEvolvedRow, doubleColumnReadingIntData, 100.0);
    }

    @Test
    public void testSchemaEvolutionToIncompatibleType()
    {
        byte[] originalIntData = buildAvroData(getFieldBuilder()
                        .name("int_to_string_field").type().intType().noDefault()
                        .endRecord(),
                "int_to_string_field", 100);

        DecoderTestColumnHandle stringColumnReadingIntData = new DecoderTestColumnHandle(0, "row0", VARCHAR, "int_to_string_field", null, null, false, false, false);
        String changedTypeSchema = getFieldBuilder()
                .name("int_to_string_field").type().stringType().noDefault()
                .endRecord()
                .toString();

        assertThatThrownBy(() -> decodeRow(originalIntData, ImmutableSet.of(stringColumnReadingIntData), ImmutableMap.of(DATA_SCHEMA, changedTypeSchema)))
                .isInstanceOf(TrinoException.class)
                .hasCauseExactlyInstanceOf(AvroTypeException.class)
                .hasStackTraceContaining("Found int, expecting string")
                .hasMessageMatching("Decoding Avro record failed.");
    }

    @Test
    public void testLongDecodedAsBigint()
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", BIGINT, "id", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "id", "\"long\"", 493857959588286460L);

        checkValue(decodedRow, row, 493857959588286460L);
    }

    @Test
    public void testIntDecodedAsBigint()
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", BIGINT, "id", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "id", "\"int\"", 100);

        checkValue(decodedRow, row, 100);
    }

    @Test
    public void testIntDecodedAsInteger()
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", INTEGER, "id", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "id", "\"int\"", 100_000);

        checkValue(decodedRow, row, 100_000);
    }

    @Test
    public void testIntDecodedAsSmallInt()
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", SMALLINT, "id", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "id", "\"int\"", 1000);

        checkValue(decodedRow, row, 1000);
    }

    @Test
    public void testIntDecodedAsTinyInt()
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", TINYINT, "id", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "id", "\"int\"", 100);

        checkValue(decodedRow, row, 100);
    }

    @Test
    public void testFloatDecodedAsDouble()
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", DOUBLE, "float_field", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "float_field", "\"float\"", 10.2f);

        checkValue(decodedRow, row, 10.2);
    }

    @Test
    public void testFloatDecodedAsReal()
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", REAL, "float_field", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "float_field", "\"float\"", 10.2f);

        checkValue(decodedRow, row, 10.2);
    }

    @Test
    public void testBytesDecodedAsVarbinary()
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", VARBINARY, "encoded", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "encoded", "\"bytes\"", ByteBuffer.wrap("mytext".getBytes(UTF_8)));

        checkValue(decodedRow, row, "mytext");
    }

    @Test
    public void testFixedDecodedAsVarbinary()
    {
        Schema schema = SchemaBuilder.record("record")
                .fields().name("fixed_field")
                .type()
                .fixed("fixed5")
                .size(5)
                .noDefault()
                .endRecord();
        Schema fixedType = schema.getField("fixed_field").schema();
        GenericData.Fixed fixedValue = new GenericData.Fixed(schema.getField("fixed_field").schema());
        byte[] bytes = {5, 4, 3, 2, 1};
        fixedValue.bytes(bytes);
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", VARBINARY, "fixed_field", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "fixed_field", fixedType.toString(), fixedValue);

        checkValue(decodedRow, row, Slices.wrappedBuffer(bytes));
    }

    @Test
    public void testDoubleDecodedAsDouble()
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", DOUBLE, "double_field", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "double_field", "\"double\"", 56.898);

        checkValue(decodedRow, row, 56.898);
    }

    @Test
    public void testStringDecodedAsVarcharN()
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", createVarcharType(10), "varcharn_field", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "varcharn_field", "\"string\"", "abcdefghijklmno");

        checkValue(decodedRow, row, "abcdefghij");
    }

    @Test
    public void testNestedRecord()
    {
        String schema = "{\"type\" : \"record\", " +
                "  \"name\" : \"nested_schema\"," +
                "  \"namespace\" : \"io.trino.decoder.avro\"," +
                "  \"fields\" :" +
                "  [{" +
                "            \"name\":\"nested\"," +
                "            \"type\":{" +
                "                      \"type\":\"record\"," +
                "                      \"name\":\"Nested\"," +
                "                      \"fields\":" +
                "                      [" +
                "                          {" +
                "                              \"name\":\"id\"," +
                "                              \"type\":[\"long\", \"null\"]" +
                "                          }" +
                "                      ]" +
                "                  }" +
                "  }]}";
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", BIGINT, "nested/id", null, null, false, false, false);

        Schema nestedSchema = new Schema.Parser().parse(schema);
        Schema userSchema = nestedSchema.getField("nested").schema();
        GenericData.Record userRecord = buildAvroRecord(userSchema, new ByteArrayOutputStream(), ImmutableMap.of("id", 98247748L));
        byte[] avroData = buildAvroData(nestedSchema, "nested", userRecord);

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = decodeRow(
                avroData,
                ImmutableSet.of(row),
                ImmutableMap.of(DATA_SCHEMA, schema));

        assertEquals(decodedRow.size(), 1);

        checkValue(decodedRow, row, 98247748);
    }

    @Test
    public void testNonExistentFieldsAreNull()
    {
        DecoderTestColumnHandle row1 = new DecoderTestColumnHandle(0, "row1", createVarcharType(100), "very/deep/varchar", null, null, false, false, false);
        DecoderTestColumnHandle row2 = new DecoderTestColumnHandle(1, "row2", BIGINT, "no_bigint", null, null, false, false, false);
        DecoderTestColumnHandle row3 = new DecoderTestColumnHandle(2, "row3", DOUBLE, "double_record/is_missing", null, null, false, false, false);
        DecoderTestColumnHandle row4 = new DecoderTestColumnHandle(3, "row4", BOOLEAN, "hello", null, null, false, false, false);

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow1 = buildAndDecodeColumn(row1, "dummy", "\"long\"", 0L);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow2 = buildAndDecodeColumn(row2, "dummy", "\"long\"", 0L);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow3 = buildAndDecodeColumn(row3, "dummy", "\"long\"", 0L);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow4 = buildAndDecodeColumn(row4, "dummy", "\"long\"", 0L);

        checkIsNull(decodedRow1, row1);
        checkIsNull(decodedRow2, row2);
        checkIsNull(decodedRow3, row3);
        checkIsNull(decodedRow4, row4);
    }

    @Test
    public void testRuntimeDecodingFailure()
    {
        DecoderTestColumnHandle booleanColumn = new DecoderTestColumnHandle(0, "some_column", BOOLEAN, "long_field", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(booleanColumn, "long_field", "\"long\"", (long) 1);

        assertThatThrownBy(decodedRow.get(booleanColumn)::getBoolean)
                .isInstanceOf(TrinoException.class)
                .hasMessageMatching("cannot decode object of 'class java.lang.Long' as 'boolean' for column 'some_column'");
    }

    @Test
    public void testArrayDecodedAsArray()
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", new ArrayType(BIGINT), "array_field", null, null, false, false, false);

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "array_field", "{\"type\": \"array\", \"items\": [\"long\"]}", ImmutableList.of(114L, 136L));
        checkArrayValue(decodedRow, row, ImmutableList.of(114L, 136L));
    }

    @Test
    public void testNestedLongArray()
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", new ArrayType(new ArrayType(BIGINT)), "array_field", null, null, false, false, false);
        Schema schema = SchemaBuilder.array().items().array().items().longType();
        List<List<Long>> data = ImmutableList.<List<Long>>builder()
                .add(ImmutableList.of(12L, 15L, 17L))
                .add(ImmutableList.of(22L, 25L, 27L, 29L))
                .build();
        GenericArray<List<Long>> list = new GenericData.Array<>(schema, data);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "array_field", schema.toString(), list);

        checkArrayValue(decodedRow, row, list);
    }

    @Test
    public void testNestedLongArrayWithNulls()
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", new ArrayType(new ArrayType(BIGINT)), "array_field", null, null, false, false, false);
        Schema schema = SchemaBuilder.array().items().nullable().array().items().nullable().longType();
        List<List<Long>> data = Arrays.asList(
                ImmutableList.of(12L, 15L, 17L),
                ImmutableList.of(22L, 25L, 27L, 29L),
                null,
                Arrays.asList(3L, 5L, null, 6L));

        GenericArray<List<Long>> list = new GenericData.Array<>(schema, data);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "array_field", schema.toString(), list);

        checkArrayValue(decodedRow, row, list);
    }

    @Test
    public void testNestedStringArray()
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", new ArrayType(new ArrayType(VARCHAR)), "array_field", null, null, false, false, false);
        Schema schema = SchemaBuilder.array().items().array().items().stringType();
        List<List<String>> data = ImmutableList.<List<String>>builder()
                .add(ImmutableList.of("a", "bb", "ccc"))
                .add(ImmutableList.of("foo", "bar", "baz", "car"))
                .build();
        GenericArray<List<String>> list = new GenericData.Array<>(schema, data);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "array_field", schema.toString(), list);

        checkArrayValue(decodedRow, row, list);
    }

    @Test
    public void testNestedStringArrayWithNulls()
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", new ArrayType(new ArrayType(VARCHAR)), "array_field", null, null, false, false, false);
        Schema schema = SchemaBuilder.array().items().nullable().array().items().nullable().stringType();
        List<List<String>> data = Arrays.asList(
                ImmutableList.of("a", "bb", "ccc"),
                ImmutableList.of("foo", "bar", "baz", "car"),
                null,
                Arrays.asList("boo", "hoo", null, "hoo"));

        GenericArray<List<String>> list = new GenericData.Array<>(schema, data);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "array_field", schema.toString(), list);

        checkArrayValue(decodedRow, row, list);
    }

    @Test
    public void testDeeplyNestedLongArray()
    {
        Schema schema = SchemaBuilder.array()
                .items()
                .array()
                .items()
                .array()
                .items()
                .longType();

        List<List<List<Long>>> data = ImmutableList.<List<List<Long>>>builder()
                .add(ImmutableList.<List<Long>>builder()
                        .add(ImmutableList.of(12L, 15L, 17L))
                        .add(ImmutableList.of(22L, 25L, 27L, 29L))
                        .build())
                .build();

        GenericArray<List<List<Long>>> list = new GenericData.Array<>(schema, data);
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", new ArrayType(new ArrayType(new ArrayType(BIGINT))), "array_field", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "array_field", schema.toString(), list);

        checkArrayValue(decodedRow, row, list);
    }

    @Test
    public void testDeeplyNestedLongArrayWithNulls()
    {
        Schema schema = SchemaBuilder.array()
                .items()
                .nullable().array()
                .items()
                .nullable().array()
                .items()
                .nullable().longType();

        List<List<List<Long>>> data = Arrays.asList(
                Arrays.asList(
                        ImmutableList.of(12L, 15L, 17L),
                        null,
                        Arrays.asList(3L, 5L, null, 6L),
                        ImmutableList.of(22L, 25L, 27L, 29L)),
                null);

        GenericArray<List<List<Long>>> list = new GenericData.Array<>(schema, data);
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", new ArrayType(new ArrayType(new ArrayType(BIGINT))), "array_field", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "array_field", schema.toString(), list);

        checkArrayValue(decodedRow, row, list);
    }

    @Test
    public void testDeeplyNestedStringArray()
    {
        Schema schema = SchemaBuilder.array()
                .items()
                .array()
                .items()
                .array()
                .items()
                .stringType();

        List<List<List<String>>> data = ImmutableList.<List<List<String>>>builder()
                .add(ImmutableList.<List<String>>builder()
                        .add(ImmutableList.of("a", "bb", "ccc"))
                        .add(ImmutableList.of("foo", "bar", "baz", "car"))
                        .build())
                .build();

        GenericArray<List<List<String>>> list = new GenericData.Array<>(schema, data);
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", new ArrayType(new ArrayType(new ArrayType(VARCHAR))), "array_field", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "array_field", schema.toString(), list);

        checkArrayValue(decodedRow, row, list);
    }

    @Test
    public void testDeeplyNestedStringArrayWithNulls()
    {
        Schema schema = SchemaBuilder.array()
                .items()
                .nullable().array()
                .items()
                .nullable().array()
                .items()
                .nullable().stringType();

        List<List<List<String>>> data = Arrays.asList(
                Arrays.asList(
                        ImmutableList.of("a", "bb", "ccc"),
                        null,
                        Arrays.asList("boo", "hoo", null, "hoo"),
                        ImmutableList.of("foo", "bar", "baz", "car")),
                null);

        GenericArray<List<List<String>>> list = new GenericData.Array<>(schema, data);
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", new ArrayType(new ArrayType(new ArrayType(VARCHAR))), "array_field", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "array_field", schema.toString(), list);

        checkArrayValue(decodedRow, row, list);
    }

    @Test
    public void testArrayOfMaps()
    {
        Schema schema = SchemaBuilder.array()
                .items()
                .map()
                .values()
                .floatType();
        List<Map<String, Float>> data = ImmutableList.<Map<String, Float>>builder()
                .add(buildMapFromKeysAndValues(ImmutableList.of("key1", "key2", "key3"), ImmutableList.of(1.3F, 2.3F, -.5F)))
                .add(buildMapFromKeysAndValues(ImmutableList.of("key10", "key20", "key30"), ImmutableList.of(11.3F, 12.3F, -1.5F)))
                .build();

        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", new ArrayType(REAL_MAP_TYPE), "array_field", null, null, false, false, false);
        GenericArray<Map<String, Float>> list = new GenericData.Array<>(schema, data);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "array_field", schema.toString(), list);
        checkArrayValues(getBlock(decodedRow, row), row.getType(), data);
    }

    @Test
    public void testArrayOfMapsWithNulls()
    {
        Schema schema = SchemaBuilder.array()
                .items()
                .nullable().map()
                .values()
                .nullable().floatType();
        List<Map<String, Float>> data = Arrays.asList(
                buildMapFromKeysAndValues(ImmutableList.of("key1", "key2", "key3"), ImmutableList.of(1.3F, 2.3F, -.5F)),
                null,
                buildMapFromKeysAndValues(ImmutableList.of("key10", "key20", "key30"), ImmutableList.of(11.3F, 12.3F, -1.5F)),
                buildMapFromKeysAndValues(ImmutableList.of("key100", "key200", "key300"), Arrays.asList(111.3F, null, -11.5F)));

        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", new ArrayType(REAL_MAP_TYPE), "array_field", null, null, false, false, false);
        GenericArray<Map<String, Float>> list = new GenericData.Array<>(schema, data);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "array_field", schema.toString(), list);
        checkArrayValues(getBlock(decodedRow, row), row.getType(), data);
    }

    @Test
    public void testMapOfMaps()
    {
        Schema schema = SchemaBuilder.map()
                .values()
                .map()
                .values()
                .floatType();

        Map<String, Map<String, Float>> data = ImmutableMap.<String, Map<String, Float>>builder()
                .put("k1", buildMapFromKeysAndValues(ImmutableList.of("key1", "key2", "key3"), ImmutableList.of(1.3F, 2.3F, -.5F)))
                .put("k2", buildMapFromKeysAndValues(ImmutableList.of("key10", "key20", "key30"), ImmutableList.of(11.3F, 12.3F, -1.5F)))
                .buildOrThrow();
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", MAP_OF_REAL_MAP_TYPE, "map_field", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "map_field", schema.toString(), data);
        checkMapValue(decodedRow, row, data);
    }

    @Test
    public void testMapOfMapsWithNulls()
    {
        Schema schema = SchemaBuilder.map()
                .values()
                .nullable().map()
                .values()
                .nullable().floatType();

        Map<String, Map<String, Float>> data = buildMapFromKeysAndValues(ImmutableList.of("k1", "k2", "k3"),
                Arrays.asList(buildMapFromKeysAndValues(ImmutableList.of("key1", "key2", "key3"), ImmutableList.of(1.3F, 2.3F, -.5F)),
                        null,
                        buildMapFromKeysAndValues(ImmutableList.of("key10", "key20", "key30"), Arrays.asList(11.3F, null, -1.5F))));
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", MAP_OF_REAL_MAP_TYPE, "map_field", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "map_field", schema.toString(), data);
        checkMapValue(decodedRow, row, data);
    }

    @Test
    public void testMapOfArrayOfMaps()
    {
        Schema schema = SchemaBuilder.map()
                .values()
                .array()
                .items()
                .map()
                .values()
                .floatType();

        Map<String, List<Map<String, Float>>> data = buildMapFromKeysAndValues(ImmutableList.of("k1", "k2"),
                Arrays.asList(Arrays.asList(buildMapFromKeysAndValues(ImmutableList.of("sk1", "sk2", "sk3"), Arrays.asList(1.3F, -5.3F, 2.3F))),
                        Arrays.asList(buildMapFromKeysAndValues(ImmutableList.of("sk11", "sk21", "sk31"), Arrays.asList(11.3F, -1.5F, 12.3F)))));
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", MAP_OF_ARRAY_OF_MAP_TYPE, "map_field", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "map_field", schema.toString(), data);
        checkMapValue(decodedRow, row, data);
    }

    @Test
    public void testMapOfArrayOfMapsWithNulls()
    {
        Schema schema = SchemaBuilder.map()
                .values()
                .nullable().array()
                .items()
                .nullable().map()
                .values()
                .nullable().floatType();

        Map<String, List<Map<String, Float>>> data = buildMapFromKeysAndValues(ImmutableList.of("k1", "k2", "k3"),
                Arrays.asList(Arrays.asList(buildMapFromKeysAndValues(ImmutableList.of("sk1", "sk2", "sk3"), Arrays.asList(1.3F, null, 2.3F))),
                        null,
                        Arrays.asList(buildMapFromKeysAndValues(ImmutableList.of("sk11", "sk21", "sk31"), Arrays.asList(11.3F, -1.5F, 12.3F)))));
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", MAP_OF_ARRAY_OF_MAP_TYPE, "map_field", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "map_field", schema.toString(), data);
        checkMapValue(decodedRow, row, data);
    }

    @Test
    public void testMapOfArrayOfMapsWithDifferentKeys()
    {
        Schema schema = SchemaBuilder.map()
                .values()
                .array()
                .items()
                .map()
                .values()
                .floatType();

        Map<String, List<Map<String, Float>>> data = buildMapFromKeysAndValues(ImmutableList.of("k1", "k2"),
                Arrays.asList(Arrays.asList(buildMapFromKeysAndValues(ImmutableList.of("sk1", "sk2", "sk3"), Arrays.asList(1.3F, -5.3F, 2.3F))),
                        Arrays.asList(buildMapFromKeysAndValues(ImmutableList.of("sk11", "sk21", "sk31"), Arrays.asList(11.3F, -1.5F, 12.3F)))));
        Map<String, List<Map<String, Float>>> mismatchedData = buildMapFromKeysAndValues(ImmutableList.of("k1", "k2"),
                Arrays.asList(Arrays.asList(buildMapFromKeysAndValues(ImmutableList.of("sk1", "sk2", "badKey"), Arrays.asList(1.3F, -5.3F, 2.3F))),
                        Arrays.asList(buildMapFromKeysAndValues(ImmutableList.of("sk11", "sk21", "sk31"), Arrays.asList(11.3F, -1.5F, 12.3F)))));

        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", MAP_OF_ARRAY_OF_MAP_TYPE, "map_field", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "map_field", schema.toString(), data);
        assertThatThrownBy(() -> checkArrayValue(decodedRow, row, mismatchedData))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Unexpected type expected [true] but found [false]");
    }

    @Test
    public void testMapOfArrayOfMapsWithDifferentValues()
    {
        Schema schema = SchemaBuilder.map()
                .values()
                .array()
                .items()
                .map()
                .values()
                .floatType();

        Map<String, List<Map<String, Float>>> data = buildMapFromKeysAndValues(ImmutableList.of("k1", "k2"),
                Arrays.asList(Arrays.asList(buildMapFromKeysAndValues(ImmutableList.of("sk1", "sk2", "sk3"), Arrays.asList(1.3F, -5.3F, 2.3F))),
                        Arrays.asList(buildMapFromKeysAndValues(ImmutableList.of("sk11", "sk21", "sk31"), Arrays.asList(11.3F, -1.5F, 12.3F)))));
        Map<String, List<Map<String, Float>>> mismatchedData = buildMapFromKeysAndValues(ImmutableList.of("k1", "k2"),
                Arrays.asList(Arrays.asList(buildMapFromKeysAndValues(ImmutableList.of("sk1", "sk2", "sk3"), Arrays.asList(1.3F, -5.3F, -2.3F))),
                        Arrays.asList(buildMapFromKeysAndValues(ImmutableList.of("sk11", "sk21", "sk31"), Arrays.asList(11.3F, -1.5F, 12.3F)))));

        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", MAP_OF_ARRAY_OF_MAP_TYPE, "map_field", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "map_field", schema.toString(), data);
        assertThatThrownBy(() -> checkArrayValue(decodedRow, row, mismatchedData))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Unexpected type expected [true] but found [false]");
    }

    @Test
    public void testArrayWithNulls()
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", new ArrayType(BIGINT), "array_field", null, null, false, false, false);

        List<Long> values = new ArrayList<>();
        values.add(null);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "array_field", "{\"type\": \"array\", \"items\": [\"long\", \"null\"]}", values);
        checkArrayItemIsNull(decodedRow, row, new long[] {0});
    }

    @Test
    public void testMapDecodedAsMap()
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", VARCHAR_MAP_TYPE, "map_field", null, null, false, false, false);

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "map_field", "{\"type\": \"map\", \"values\": \"string\"}", ImmutableMap.of(
                "key1", "abc",
                "key2", "def",
                "key3", "zyx"));
        checkMapValue(decodedRow, row, ImmutableMap.of(
                "key1", "abc",
                "key2", "def",
                "key3", "zyx"));
    }

    @Test
    public void testMapWithNull()
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", VARCHAR_MAP_TYPE, "map_field", null, null, false, false, false);

        Map<String, String> expectedValues = new HashMap<>();
        expectedValues.put("key1", null);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "map_field", "{\"type\": \"map\", \"values\": \"null\"}", expectedValues);

        checkMapValue(decodedRow, row, expectedValues);
    }

    @Test
    public void testMapWithDifferentKeys()
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", VARCHAR_MAP_TYPE, "map_field", null, null, false, false, false);

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "map_field", "{\"type\": \"map\", \"values\": \"string\"}", ImmutableMap.of(
                "key1", "abc",
                "key2", "def",
                "key3", "zyx"));
        assertThatThrownBy(() -> checkMapValue(decodedRow, row, ImmutableMap.of(
                "key1", "abc",
                "key4", "def",
                "key3", "zyx")))
                .isInstanceOf(AssertionError.class)
                .hasMessage("expected [true] but found [false]");
    }

    @Test
    public void testMapWithDifferentValues()
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", VARCHAR_MAP_TYPE, "map_field", null, null, false, false, false);

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "map_field", "{\"type\": \"map\", \"values\": \"string\"}", ImmutableMap.of(
                "key1", "abc",
                "key2", "def",
                "key3", "zyx"));
        assertThatThrownBy(() -> checkMapValue(decodedRow, row, ImmutableMap.of(
                "key1", "abc",
                "key2", "fed",
                "key3", "zyx")))
                .isInstanceOf(AssertionError.class)
                .hasMessage("expected [fed] but found [def]");
    }

    @Test
    public void testRow()
    {
        Schema schema = SchemaBuilder.record("record_field")
                .fields()
                .name("f1").type().floatType().noDefault()
                .name("f2").type().doubleType().noDefault()
                .name("f3").type().intType().noDefault()
                .name("f4").type().longType().noDefault()
                .name("f5").type().stringType().noDefault()
                .name("f6").type().enumeration("color").symbols("red", "blue", "green").noDefault()
                .name("f7").type().fixed("fixed5").size(5).noDefault()
                .name("f8").type().bytesType().noDefault()
                .name("f9").type().booleanType().noDefault()
                .name("f10").type().array().items()
                .record("sub_array_field").fields()
                .name("sf1").type().stringType().noDefault()
                .name("sf2").type().longType().noDefault()
                .endRecord().noDefault()
                .name("f11").type().map().values()
                .record("sub_map_field").fields()
                .name("sf1").type().doubleType().noDefault()
                .name("sf2").type().booleanType().noDefault()
                .endRecord().noDefault()
                .name("f12").type().record("sub_row_field").fields()
                .name("sf1").type().intType().noDefault()
                .name("sf2").type().enumeration("state").symbols("initialized", "running", "finished", "failed").noDefault()
                .endRecord().noDefault()
                .endRecord();
        RowType rowType = RowType.from(ImmutableList.<RowType.Field>builder()
                .add(RowType.field("f1", REAL))
                .add(RowType.field("f2", DOUBLE))
                .add(RowType.field("f3", INTEGER))
                .add(RowType.field("f4", BIGINT))
                .add(RowType.field("f5", VARCHAR))
                .add(RowType.field("f6", VARCHAR))
                .add(RowType.field("f7", VARBINARY))
                .add(RowType.field("f8", VARBINARY))
                .add(RowType.field("f9", BOOLEAN))
                .add(RowType.field("f10", new ArrayType(RowType.from(ImmutableList.<RowType.Field>builder()
                        .add(RowType.field("sf1", VARCHAR))
                        .add(RowType.field("sf2", BIGINT))
                        .build()))))
                .add(RowType.field("f11", MAP_OF_RECORD))
                .add(RowType.field("f12", RowType.from(ImmutableList.<RowType.Field>builder()
                        .add(RowType.field("sf1", INTEGER))
                        .add(RowType.field("sf2", VARCHAR))
                        .build())))
                .build());
        GenericRecord data = new GenericRecordBuilder(schema)
                .set("f1", 1.5F)
                .set("f2", 1.6D)
                .set("f3", 5)
                .set("f4", 6L)
                .set("f5", "hello")
                .set("f6", new GenericData.EnumSymbol(schema.getField("f6").schema(), "blue"))
                .set("f7", new GenericData.Fixed(schema.getField("f7").schema(), new byte[] {5, 4, 3, 2, 1}))
                .set("f8", ByteBuffer.wrap("mytext".getBytes(UTF_8)))
                .set("f9", true)
                .set("f10", ImmutableList.builder()
                        .add(new GenericRecordBuilder(schema.getField("f10").schema().getElementType())
                                .set("sf1", "string text")
                                .set("sf2", 365_000_000L)
                                .build())
                        .add(new GenericRecordBuilder(schema.getField("f10").schema().getElementType())
                                .set("sf1", "more string text")
                                .set("sf2", 365_000_000_000L)
                                .build())
                        .build())
                .set("f11", ImmutableMap.builder()
                        .put("key1", new GenericRecordBuilder(schema.getField("f11").schema().getValueType())
                                .set("sf1", 3.5D)
                                .set("sf2", true)
                                .build())
                        .put("key2", new GenericRecordBuilder(schema.getField("f11").schema().getValueType())
                                .set("sf1", 4.5D)
                                .set("sf2", false)
                                .build())
                        .buildOrThrow())
                .set("f12", new GenericRecordBuilder(schema.getField("f12").schema())
                        .set("sf1", 3)
                        .set("sf2", new GenericData.EnumSymbol(schema.getField("f12").schema().getField("sf2").schema(), "running"))
                        .build())
                .build();
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "record_field", rowType, "record_field", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "record_field", schema.toString(), data);
        checkRowValue(decodedRow, row, data);
    }

    @Test
    public void testRowWithNulls()
    {
        Schema schema = SchemaBuilder.record("record_field")
                .fields()
                .name("f1").type().optional().floatType()
                .name("f2").type().optional().doubleType()
                .name("f3").type().optional().intType()
                .name("f4").type().optional().longType()
                .name("f5").type().optional().stringType()
                .name("f6").type().optional().enumeration("color").symbols("red", "blue", "green")
                .name("f7").type().optional().fixed("fixed5").size(5)
                .name("f8").type().optional().bytesType()
                .name("f9").type().optional().booleanType()
                .name("f10").type().optional().array().items()
                .unionOf().nullType().and().record("sub_array_field").fields()
                .name("sf1").type().optional().stringType()
                .name("sf2").type().optional().longType()
                .endRecord().endUnion()
                .name("f11").type().optional().map().values()
                .unionOf().nullType().and()
                .record("sub_map_field").fields()
                .name("sf1").type().optional().doubleType()
                .name("sf2").type().optional().booleanType()
                .endRecord()
                .endUnion()
                .name("f12").type().optional().record("sub_row_field").fields()
                .name("sf1").type().optional().intType()
                .name("sf2").type().optional().enumeration("state").symbols("initialized", "running", "finished", "failed")
                .endRecord()
                .endRecord();
        RowType rowType = RowType.from(ImmutableList.<RowType.Field>builder()
                .add(RowType.field("f1", REAL))
                .add(RowType.field("f2", DOUBLE))
                .add(RowType.field("f3", INTEGER))
                .add(RowType.field("f4", BIGINT))
                .add(RowType.field("f5", VARCHAR))
                .add(RowType.field("f6", VARCHAR))
                .add(RowType.field("f7", VARBINARY))
                .add(RowType.field("f8", VARBINARY))
                .add(RowType.field("f9", BOOLEAN))
                .add(RowType.field("f10", new ArrayType(RowType.from(ImmutableList.<RowType.Field>builder()
                        .add(RowType.field("sf1", VARCHAR))
                        .add(RowType.field("sf2", BIGINT))
                        .build()))))
                .add(RowType.field("f11", MAP_OF_RECORD))
                .add(RowType.field("f12", RowType.from(ImmutableList.<RowType.Field>builder()
                        .add(RowType.field("sf1", INTEGER))
                        .add(RowType.field("sf2", VARCHAR))
                        .build())))
                .build());
        GenericRecord data = new GenericRecordBuilder(schema).build();
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "record_field", rowType, "record_field", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "record_field", schema.toString(), data);
        checkRowValue(decodedRow, row, data);

        // Check nested fields with null values
        GenericData.Array<GenericRecord> array = new GenericData.Array<GenericRecord>(schema.getField("f10").schema().getTypes().get(1),
                Arrays.asList(
                        new GenericRecordBuilder(schema.getField("f10").schema().getTypes().get(1).getElementType().getTypes().get(1)).build(),
                        null));
        data = new GenericRecordBuilder(schema)
                .set("f10", array)
                .set("f11", ImmutableMap.builder()
                        .put("key1", new GenericRecordBuilder(schema.getField("f11").schema().getTypes().get(1).getValueType().getTypes().get(1)).build())
                        .buildOrThrow())
                .set("f12", new GenericRecordBuilder(schema.getField("f12").schema().getTypes().get(1)).build())
                .build();
        decodedRow = buildAndDecodeColumn(row, "record_field", schema.toString(), data);
        checkRowValue(decodedRow, row, data);
    }

    @Test
    public void testArrayOfRow()
    {
        Schema schema = SchemaBuilder.array()
                .items()
                .record("record")
                .fields()
                .name("f1").type().intType().noDefault()
                .name("f2").type().stringType().noDefault()
                .endRecord();
        ImmutableList.Builder<GenericRecord> dataBuilder = ImmutableList.builder();
        for (int i = 0; i < 10; i++) {
            dataBuilder.add(new GenericRecordBuilder(schema.getElementType()).set("f1", 100 + i).set("f2", "hi-" + i).build());
        }
        List<GenericRecord> data = dataBuilder.build();
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "array_field", new ArrayType(RowType.from(ImmutableList.<RowType.Field>builder()
                .add(RowType.field("f1", INTEGER))
                .add(RowType.field("f2", VARCHAR))
                .build())), "array_field", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "array_field", schema.toString(), data);
        checkArrayValue(decodedRow, row, data);
    }

    private static void checkRowValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, Object expected)
    {
        checkRowValues(getBlock(decodedRow, handle), handle.getType(), expected);
    }

    private static void checkArrayValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, Object expected)
    {
        checkArrayValues(getBlock(decodedRow, handle), handle.getType(), expected);
    }

    private static void checkArrayItemIsNull(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, long[] expected)
    {
        Block actualBlock = getBlock(decodedRow, handle);
        assertEquals(actualBlock.getPositionCount(), expected.length);

        for (int i = 0; i < actualBlock.getPositionCount(); i++) {
            assertTrue(actualBlock.isNull(i));
            assertEquals(BIGINT.getLong(actualBlock, i), expected[i]);
        }
    }

    private static void checkMapValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderTestColumnHandle handle, Object expected)
    {
        checkMapValues(getBlock(decodedRow, handle), handle.getType(), expected);
    }

    private static Block getBlock(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle)
    {
        FieldValueProvider provider = decodedRow.get(handle);
        assertNotNull(provider);
        return provider.getBlock();
    }

    @Test
    public void testInvalidExtraneousParameters()
    {
        assertThatThrownBy(() -> singleColumnDecoder(BigintType.BIGINT, "mapping", null, "hint", false, false, false))
                .isInstanceOf(TrinoException.class)
                .hasMessageMatching("unexpected format hint 'hint' defined for column 'some_column'");

        assertThatThrownBy(() -> singleColumnDecoder(BigintType.BIGINT, "mapping", null, null, false, false, true))
                .isInstanceOf(TrinoException.class)
                .hasMessageMatching("unexpected internal column 'some_column'");
    }

    @Test
    public void testSupportedDataTypeValidation()
    {
        // supported types
        singleColumnDecoder(BigintType.BIGINT);
        singleColumnDecoder(VarbinaryType.VARBINARY);
        singleColumnDecoder(BooleanType.BOOLEAN);
        singleColumnDecoder(DoubleType.DOUBLE);
        singleColumnDecoder(createUnboundedVarcharType());
        singleColumnDecoder(createVarcharType(100));
        singleColumnDecoder(new ArrayType(BigintType.BIGINT));
        singleColumnDecoder(VARCHAR_MAP_TYPE);
        singleColumnDecoder(DOUBLE_MAP_TYPE);

        // some unsupported types
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(DecimalType.createDecimalType(10, 4)));
    }

    private void assertUnsupportedColumnTypeException(ThrowableAssert.ThrowingCallable callable)
    {
        assertThatThrownBy(callable)
                .isInstanceOf(TrinoException.class)
                .hasMessageMatching("Unsupported column type .* for column .*");
    }

    private void singleColumnDecoder(Type columnType)
    {
        String someSchema = getFieldBuilder()
                .name("dummy").type().longType().noDefault()
                .endRecord()
                .toString();
        DECODER_FACTORY.create(ImmutableMap.of(DATA_SCHEMA, someSchema), ImmutableSet.of(new DecoderTestColumnHandle(0, "some_column", columnType, "0", null, null, false, false, false)));
    }

    private void singleColumnDecoder(Type columnType, String mapping, String dataFormat, String formatHint, boolean keyDecoder, boolean hidden, boolean internal)
    {
        String someSchema = getFieldBuilder()
                .name("dummy").type().longType().noDefault()
                .endRecord()
                .toString();

        DECODER_FACTORY.create(ImmutableMap.of(DATA_SCHEMA, someSchema), ImmutableSet.of(new DecoderTestColumnHandle(0, "some_column", columnType, mapping, dataFormat, formatHint, keyDecoder, hidden, internal)));
    }
}
