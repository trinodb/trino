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
package io.trino.hive.formats.avro;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.local.LocalFileSystem;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.MapBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.RandomData;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Verify.verify;
import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.block.BlockAssertions.createIntsBlock;
import static io.trino.block.BlockAssertions.createRowBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.within;

public class TestAvroPageDataReaderWithoutTypeManager
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final ArrayType ARRAY_INTEGER = new ArrayType(INTEGER);
    private static final MapType MAP_VARCHAR_VARCHAR = new MapType(VARCHAR, VARCHAR, TYPE_OPERATORS);
    private static final MapType MAP_VARCHAR_INTEGER = new MapType(VARCHAR, INTEGER, TYPE_OPERATORS);
    private static final TrinoFileSystem TRINO_LOCAL_FILESYSTEM = new LocalFileSystem(Path.of("/"));

    private static final Schema SIMPLE_RECORD_SCHEMA = SchemaBuilder.record("simpleRecord")
            .fields()
            .name("a")
            .type().intType().noDefault()
            .name("b")
            .type().doubleType().noDefault()
            .name("c")
            .type().stringType().noDefault()
            .endRecord();

    private static final Schema SIMPLE_ENUM_SCHEMA = SchemaBuilder.enumeration("myEnumType").symbols("A", "B", "C");

    private static final Schema SIMPLE_ENUM_SUPER_SCHEMA = SchemaBuilder.enumeration("myEnumType").symbols("A", "B", "C", "D");

    private static final Schema SIMPLE_ENUM_REORDERED = SchemaBuilder.enumeration("myEnumType").symbols("C", "D", "B", "A");

    private static final Schema ALL_TYPES_RECORD_SCHEMA = SchemaBuilder.builder()
            .record("all")
            .fields()
            .name("aBoolean")
            .type().booleanType().noDefault()
            .name("aInt")
            .type().intType().noDefault()
            .name("aLong")
            .type().longType().noDefault()
            .name("aFloat")
            .type().floatType().noDefault()
            .name("aDouble")
            .type().doubleType().noDefault()
            .name("aString")
            .type().stringType().noDefault()
            .name("aBytes")
            .type().bytesType().noDefault()
            .name("aFixed")
            .type().fixed("myFixedType").size(16).noDefault()
            .name("anArray")
            .type().array().items().intType().noDefault()
            .name("aMap")
            .type().map().values().intType().noDefault()
            .name("anEnum")
            .type(SIMPLE_ENUM_SCHEMA).noDefault()
            .name("aRecord")
            .type(SIMPLE_RECORD_SCHEMA).noDefault()
            .name("aUnion")
            .type().optional().stringType()
            .endRecord();

    private static final GenericRecord ALL_TYPES_GENERIC_RECORD;
    private static final GenericRecord SIMPLE_GENERIC_RECORD;
    private static final String A_STRING_VALUE = "a test string";
    private static final ByteBuffer A_BYTES_VALUE = ByteBuffer.wrap("a test byte array".getBytes(StandardCharsets.UTF_8));
    private static final GenericData.Fixed A_FIXED_VALUE;

    static {
        SIMPLE_GENERIC_RECORD = new GenericData.Record(SIMPLE_RECORD_SCHEMA);
        SIMPLE_GENERIC_RECORD.put("a", 5);
        SIMPLE_GENERIC_RECORD.put("b", 3.14159265358979);
        SIMPLE_GENERIC_RECORD.put("c", "Simple Record String Field");

        UUID fixed = UUID.nameUUIDFromBytes("a test fixed".getBytes(StandardCharsets.UTF_8));
        A_FIXED_VALUE = new GenericData.Fixed(SchemaBuilder.builder().fixed("myFixedType").size(16), Bytes.concat(Longs.toByteArray(fixed.getMostSignificantBits()), Longs.toByteArray(fixed.getLeastSignificantBits())));

        ALL_TYPES_GENERIC_RECORD = new GenericData.Record(ALL_TYPES_RECORD_SCHEMA);
        ALL_TYPES_GENERIC_RECORD.put("aBoolean", true);
        ALL_TYPES_GENERIC_RECORD.put("aInt", 42);
        ALL_TYPES_GENERIC_RECORD.put("aLong", 3400L);
        ALL_TYPES_GENERIC_RECORD.put("aFloat", 3.14f);
        ALL_TYPES_GENERIC_RECORD.put("aDouble", 9.81);
        ALL_TYPES_GENERIC_RECORD.put("aString", A_STRING_VALUE);
        ALL_TYPES_GENERIC_RECORD.put("aBytes", A_BYTES_VALUE);
        ALL_TYPES_GENERIC_RECORD.put("aFixed", A_FIXED_VALUE);
        ALL_TYPES_GENERIC_RECORD.put("anArray", ImmutableList.of(1, 2, 3, 4));
        ALL_TYPES_GENERIC_RECORD.put("aMap", ImmutableMap.of("key1", 1, "key2", 2));
        ALL_TYPES_GENERIC_RECORD.put("anEnum", new GenericData.EnumSymbol(SIMPLE_ENUM_SCHEMA, "A"));
        ALL_TYPES_GENERIC_RECORD.put("aRecord", SIMPLE_GENERIC_RECORD);
        ALL_TYPES_GENERIC_RECORD.put("aUnion", null);
    }

    @Test
    public void testAllTypesSimple()
            throws IOException, AvroTypeException
    {
        TrinoInputFile input = createWrittenFileWithData(ALL_TYPES_RECORD_SCHEMA, ImmutableList.of(ALL_TYPES_GENERIC_RECORD));
        try (AvroFileReader avroFileReader = new AvroFileReader(input, ALL_TYPES_RECORD_SCHEMA, NoOpAvroTypeManager.INSTANCE)) {
            int totalRecords = 0;
            while (avroFileReader.hasNext()) {
                Page p = avroFileReader.next();
                assertIsAllTypesGenericRecord(p);
                totalRecords += p.getPositionCount();
            }
            assertThat(totalRecords).isEqualTo(1);
        }
    }

    @Test
    public void testSchemaWithSkips()
            throws IOException, AvroTypeException
    {
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.builder().record("simpleRecord").fields();
        fieldAssembler.name("notInAllTypeRecordSchema").type().optional().array().items().intType();
        Schema readSchema = fieldAssembler.endRecord();

        int count = ThreadLocalRandom.current().nextInt(10000, 100000);
        TrinoInputFile input = createWrittenFileWithSchema(count, ALL_TYPES_RECORD_SCHEMA);
        try (AvroFileReader avroFileReader = new AvroFileReader(input, readSchema, NoOpAvroTypeManager.INSTANCE)) {
            int totalRecords = 0;
            while (avroFileReader.hasNext()) {
                Page p = avroFileReader.next();
                for (int pos = 0; pos < p.getPositionCount(); pos++) {
                    assertThat(p.getBlock(0).isNull(pos)).isTrue();
                }
                totalRecords += p.getPositionCount();
            }
            assertThat(totalRecords).isEqualTo(count);
        }
    }

    @Test
    public void testSchemaWithDefaults()
            throws IOException, AvroTypeException
    {
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.builder().record("simpleRecord").fields();
        fieldAssembler.name("defaultedField1").type().map().values().stringType().mapDefault(ImmutableMap.of("key1", "value1"));
        for (Schema.Field field : SIMPLE_RECORD_SCHEMA.getFields()) {
            fieldAssembler = fieldAssembler.name(field.name()).type(field.schema()).noDefault();
        }
        fieldAssembler.name("defaultedField2").type().booleanType().booleanDefault(true);
        Schema readerSchema = fieldAssembler.endRecord();

        int count = ThreadLocalRandom.current().nextInt(10000, 100000);
        TrinoInputFile input = createWrittenFileWithSchema(count, SIMPLE_RECORD_SCHEMA);
        try (AvroFileReader avroFileReader = new AvroFileReader(input, readerSchema, NoOpAvroTypeManager.INSTANCE)) {
            int totalRecords = 0;
            while (avroFileReader.hasNext()) {
                Page p = avroFileReader.next();
                MapBlock mb = (MapBlock) p.getBlock(0);
                MapBlock expected = (MapBlock) MAP_VARCHAR_VARCHAR.createBlockFromKeyValue(Optional.empty(),
                        new int[] {0, 1},
                        createStringsBlock("key1"),
                        createStringsBlock("value1"));
                mb = (MapBlock) mb.getRegion(0, 1);
                assertBlockEquals(MAP_VARCHAR_VARCHAR, mb, expected);

                ByteArrayBlock block = (ByteArrayBlock) p.getBlock(readerSchema.getFields().size() - 1);
                assertThat(block.getByte(0, 0)).isGreaterThan((byte) 0);
                totalRecords += p.getPositionCount();
            }
            assertThat(totalRecords).isEqualTo(count);
        }
    }

    @Test
    public void testSchemaWithReorders()
            throws IOException, AvroTypeException
    {
        Schema writerSchema = reverseSchema(ALL_TYPES_RECORD_SCHEMA);
        TrinoInputFile input = createWrittenFileWithData(writerSchema, ImmutableList.of(reverseGenericRecord(ALL_TYPES_GENERIC_RECORD)));
        try (AvroFileReader avroFileReader = new AvroFileReader(input, ALL_TYPES_RECORD_SCHEMA, NoOpAvroTypeManager.INSTANCE)) {
            int totalRecords = 0;
            while (avroFileReader.hasNext()) {
                Page p = avroFileReader.next();
                assertIsAllTypesGenericRecord(p);
                totalRecords += p.getPositionCount();
            }
            assertThat(totalRecords).isEqualTo(1);
        }
    }

    @Test
    public void testPromotions()
            throws IOException, AvroTypeException
    {
        SchemaBuilder.FieldAssembler<Schema> writeSchemaBuilder = SchemaBuilder.builder().record("writeRecord").fields();
        SchemaBuilder.FieldAssembler<Schema> readSchemaBuilder = SchemaBuilder.builder().record("readRecord").fields();

        AtomicInteger fieldNum = new AtomicInteger(0);
        Map<Integer, Class<?>> expectedBlockPerChannel = new HashMap<>();
        for (Schema.Type readType : Schema.Type.values()) {
            List<Schema.Type> promotesFrom = switch (readType) {
                case STRING -> ImmutableList.of(Schema.Type.BYTES);
                case BYTES -> ImmutableList.of(Schema.Type.STRING);
                case LONG -> ImmutableList.of(Schema.Type.INT);
                case FLOAT -> ImmutableList.of(Schema.Type.INT, Schema.Type.LONG);
                case DOUBLE -> ImmutableList.of(Schema.Type.INT, Schema.Type.LONG, Schema.Type.FLOAT);
                case RECORD, ENUM, ARRAY, MAP, UNION, FIXED, INT, BOOLEAN, NULL -> ImmutableList.of();
            };
            for (Schema.Type writeType : promotesFrom) {
                expectedBlockPerChannel.put(fieldNum.get(), switch (readType) {
                    case STRING, BYTES -> VariableWidthBlock.class;
                    case LONG, DOUBLE -> LongArrayBlock.class;
                    case FLOAT -> IntArrayBlock.class;
                    case RECORD, ENUM, ARRAY, MAP, UNION, FIXED, INT, BOOLEAN, NULL -> throw new IllegalStateException();
                });
                String fieldName = "field" + fieldNum.getAndIncrement();
                writeSchemaBuilder = writeSchemaBuilder.name(fieldName).type(Schema.create(writeType)).noDefault();
                readSchemaBuilder = readSchemaBuilder.name(fieldName).type(Schema.create(readType)).noDefault();
            }
        }

        int count = ThreadLocalRandom.current().nextInt(10000, 100000);
        TrinoInputFile input = createWrittenFileWithSchema(count, writeSchemaBuilder.endRecord());
        try (AvroFileReader avroFileReader = new AvroFileReader(input, readSchemaBuilder.endRecord(), NoOpAvroTypeManager.INSTANCE)) {
            int totalRecords = 0;
            while (avroFileReader.hasNext()) {
                Page p = avroFileReader.next();
                for (Map.Entry<Integer, Class<?>> channelClass : expectedBlockPerChannel.entrySet()) {
                    assertThat(p.getBlock(channelClass.getKey())).isInstanceOf(channelClass.getValue());
                }
                totalRecords += p.getPositionCount();
            }
            assertThat(totalRecords).isEqualTo(count);
        }
    }

    @Test
    public void testEnum()
            throws IOException, AvroTypeException
    {
        Schema base = SchemaBuilder.record("test").fields()
                .name("myEnum")
                .type(SIMPLE_ENUM_SCHEMA).noDefault()
                .endRecord();
        Schema superSchema = SchemaBuilder.record("test").fields()
                .name("myEnum")
                .type(SIMPLE_ENUM_SUPER_SCHEMA).noDefault()
                .endRecord();
        Schema reorderdSchema = SchemaBuilder.record("test").fields()
                .name("myEnum")
                .type(SIMPLE_ENUM_REORDERED).noDefault()
                .endRecord();

        GenericRecord expected = (GenericRecord) new RandomData(base, 1).iterator().next();

        //test superset
        TrinoInputFile input = createWrittenFileWithData(base, ImmutableList.of(expected));
        try (AvroFileReader avroFileReader = new AvroFileReader(input, superSchema, NoOpAvroTypeManager.INSTANCE)) {
            int totalRecords = 0;
            while (avroFileReader.hasNext()) {
                Page p = avroFileReader.next();
                String actualSymbol = new String(((Slice) VARCHAR.getObject(p.getBlock(0), 0)).getBytes(), StandardCharsets.UTF_8);
                assertThat(actualSymbol).isEqualTo(expected.get("myEnum").toString());
                totalRecords += p.getPositionCount();
            }
            assertThat(totalRecords).isEqualTo(1);
        }

        //test reordered
        input = createWrittenFileWithData(base, ImmutableList.of(expected));
        try (AvroFileReader avroFileReader = new AvroFileReader(input, reorderdSchema, NoOpAvroTypeManager.INSTANCE)) {
            int totalRecords = 0;
            while (avroFileReader.hasNext()) {
                Page p = avroFileReader.next();
                String actualSymbol = new String(((Slice) VarcharType.VARCHAR.getObject(p.getBlock(0), 0)).getBytes(), StandardCharsets.UTF_8);
                assertThat(actualSymbol).isEqualTo(expected.get("myEnum").toString());
                totalRecords += p.getPositionCount();
            }
            assertThat(totalRecords).isEqualTo(1);
        }
    }

    @Test
    public void testCoercionOfUnionToStruct()
            throws IOException, AvroTypeException
    {
        Schema complexUnion = Schema.createUnion(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL));

        Schema readSchema = SchemaBuilder.builder()
                .record("testComplexUnions")
                .fields()
                .name("readStraightUp")
                .type(complexUnion)
                .noDefault()
                .name("readFromReverse")
                .type(complexUnion)
                .noDefault()
                .name("readFromDefault")
                .type(complexUnion)
                .withDefault(42)
                .endRecord();

        Schema writeSchema = SchemaBuilder.builder()
                .record("testComplexUnions")
                .fields()
                .name("readStraightUp")
                .type(complexUnion)
                .noDefault()
                .name("readFromReverse")
                .type(Schema.createUnion(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT)))
                .noDefault()
                .endRecord();

        GenericRecord stringsOnly = new GenericData.Record(writeSchema);
        stringsOnly.put("readStraightUp", "I am in column 0 field 1");
        stringsOnly.put("readFromReverse", "I am in column 1 field 1");

        GenericRecord ints = new GenericData.Record(writeSchema);
        ints.put("readStraightUp", 5);
        ints.put("readFromReverse", 21);

        GenericRecord nulls = new GenericData.Record(writeSchema);
        nulls.put("readStraightUp", null);
        nulls.put("readFromReverse", null);

        TrinoInputFile input = createWrittenFileWithData(writeSchema, ImmutableList.of(stringsOnly, ints, nulls));
        try (AvroFileReader avroFileReader = new AvroFileReader(input, readSchema, NoOpAvroTypeManager.INSTANCE)) {
            int totalRecords = 0;
            while (avroFileReader.hasNext()) {
                Page p = avroFileReader.next();
                assertThat(p.getPositionCount()).withFailMessage("Page Batch should be at least 3").isEqualTo(3);
                //check first column
                //check first column first row coerced struct
                Block readStraightUpStringsOnly = p.getBlock(0).getSingleValueBlock(0);
                assertThat(readStraightUpStringsOnly.getChildren().size()).isEqualTo(3); // tag, int and string block fields
                assertThat(readStraightUpStringsOnly.getChildren().get(1).isNull(0)).isTrue(); // int field null
                assertThat(VARCHAR.getObjectValue(null, readStraightUpStringsOnly.getChildren().get(2), 0)).isEqualTo("I am in column 0 field 1"); //string field expected value
                // check first column second row coerced struct
                Block readStraightUpInts = p.getBlock(0).getSingleValueBlock(1);
                assertThat(readStraightUpInts.getChildren().size()).isEqualTo(3); // tag, int and string block fields
                assertThat(readStraightUpInts.getChildren().get(2).isNull(0)).isTrue(); // string field null
                assertThat(INTEGER.getObjectValue(null, readStraightUpInts.getChildren().get(1), 0)).isEqualTo(5);

                //check first column third row is null
                assertThat(p.getBlock(0).isNull(2)).isTrue();
                //check second column
                //check second column first row coerced struct
                Block readFromReverseStringsOnly = p.getBlock(1).getSingleValueBlock(0);
                assertThat(readFromReverseStringsOnly.getChildren().size()).isEqualTo(3); // tag, int and string block fields
                assertThat(readFromReverseStringsOnly.getChildren().get(1).isNull(0)).isTrue(); // int field null
                assertThat(VARCHAR.getObjectValue(null, readFromReverseStringsOnly.getChildren().get(2), 0)).isEqualTo("I am in column 1 field 1");
                //check second column second row coerced struct
                Block readFromReverseUpInts = p.getBlock(1).getSingleValueBlock(1);
                assertThat(readFromReverseUpInts.getChildren().size()).isEqualTo(3); // tag, int and string block fields
                assertThat(readFromReverseUpInts.getChildren().get(2).isNull(0)).isTrue(); // string field null
                assertThat(INTEGER.getObjectValue(null, readFromReverseUpInts.getChildren().get(1), 0)).isEqualTo(21);
                //check second column third row is null
                assertThat(p.getBlock(1).isNull(2)).isTrue();

                //check third column (default of 42 always)
                //check third column first row coerced struct
                Block readFromDefaultStringsOnly = p.getBlock(2).getSingleValueBlock(0);
                assertThat(readFromDefaultStringsOnly.getChildren().size()).isEqualTo(3); // tag, int and string block fields
                assertThat(readFromDefaultStringsOnly.getChildren().get(2).isNull(0)).isTrue(); // string field null
                assertThat(INTEGER.getObjectValue(null, readFromDefaultStringsOnly.getChildren().get(1), 0)).isEqualTo(42);
                //check third column second row coerced struct
                Block readFromDefaultInts = p.getBlock(2).getSingleValueBlock(1);
                assertThat(readFromDefaultInts.getChildren().size()).isEqualTo(3); // tag, int and string block fields
                assertThat(readFromDefaultInts.getChildren().get(2).isNull(0)).isTrue(); // string field null
                assertThat(INTEGER.getObjectValue(null, readFromDefaultInts.getChildren().get(1), 0)).isEqualTo(42);
                //check third column third row coerced struct
                Block readFromDefaultNulls = p.getBlock(2).getSingleValueBlock(2);
                assertThat(readFromDefaultNulls.getChildren().size()).isEqualTo(3); // int and string block fields
                assertThat(readFromDefaultNulls.getChildren().get(2).isNull(0)).isTrue(); // string field null
                assertThat(INTEGER.getObjectValue(null, readFromDefaultNulls.getChildren().get(1), 0)).isEqualTo(42);

                totalRecords += p.getPositionCount();
            }
            assertThat(totalRecords).isEqualTo(3);
        }
    }

    protected static TrinoInputFile createWrittenFileWithData(Schema schema, List<GenericRecord> records)
            throws IOException
    {
        File tempFile = File.createTempFile("testingAvroReading", null);
        try (DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>())) {
            fileWriter.create(schema, tempFile);
            for (GenericRecord genericRecord : records) {
                fileWriter.append(genericRecord);
            }
        }
        tempFile.deleteOnExit();
        return TRINO_LOCAL_FILESYSTEM.newInputFile(Location.of("local://" + tempFile.getAbsolutePath()));
    }

    protected static TrinoInputFile createWrittenFileWithSchema(int count, Schema schema)
            throws IOException
    {
        Iterator<Object> randomData = new RandomData(schema, count).iterator();
        File tempFile = File.createTempFile("testingAvroReading", null);
        try (DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>())) {
            fileWriter.create(schema, tempFile);
            while (randomData.hasNext()) {
                fileWriter.append((GenericRecord) randomData.next());
            }
        }
        tempFile.deleteOnExit();
        return TRINO_LOCAL_FILESYSTEM.newInputFile(Location.of("local://" + tempFile.getAbsolutePath()));
    }

    private static GenericRecord reverseGenericRecord(GenericRecord record)
    {
        Schema reversedSchema = reverseSchema(record.getSchema());
        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(reversedSchema);
        for (Schema.Field field : reversedSchema.getFields()) {
            if (field.schema().getType() == Schema.Type.RECORD) {
                recordBuilder.set(field, reverseGenericRecord((GenericRecord) record.get(field.name())));
            }
            else {
                recordBuilder.set(field, record.get(field.name()));
            }
        }
        return recordBuilder.build();
    }

    private static Schema reverseSchema(Schema schema)
    {
        verify(schema.getType() == Schema.Type.RECORD);
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.builder().record(schema.getName()).fields();
        for (Schema.Field field : Lists.reverse(schema.getFields())) {
            if (field.schema().getType() == Schema.Type.ENUM) {
                fieldAssembler = fieldAssembler.name(field.name())
                        .type(Schema.createEnum(field.schema().getName(), field.schema().getDoc(), field.schema().getNamespace(), Lists.reverse(field.schema().getEnumSymbols())))
                        .noDefault();
            }
            else if (field.schema().getType() == Schema.Type.UNION) {
                fieldAssembler = fieldAssembler.name(field.name())
                        .type(Schema.createUnion(Lists.reverse(field.schema().getTypes())))
                        .noDefault();
            }
            else if (field.schema().getType() == Schema.Type.RECORD) {
                fieldAssembler = fieldAssembler.name(field.name())
                        .type(reverseSchema(field.schema()))
                        .noDefault();
            }
            else {
                fieldAssembler = fieldAssembler.name(field.name()).type(field.schema()).noDefault();
            }
        }
        return fieldAssembler.endRecord();
    }

    private static void assertIsAllTypesGenericRecord(Page p)
    {
        // test boolean
        assertThat(p.getBlock(0)).isInstanceOf(ByteArrayBlock.class);
        assertThat(BooleanType.BOOLEAN.getBoolean(p.getBlock(0), 0)).isTrue();
        // test int
        assertThat(p.getBlock(1)).isInstanceOf(IntArrayBlock.class);
        assertThat(INTEGER.getInt(p.getBlock(1), 0)).isEqualTo(42);
        // test long
        assertThat(p.getBlock(2)).isInstanceOf(LongArrayBlock.class);
        assertThat(BigintType.BIGINT.getLong(p.getBlock(2), 0)).isEqualTo(3400L);
        // test float
        assertThat(p.getBlock(3)).isInstanceOf(IntArrayBlock.class);
        assertThat(RealType.REAL.getFloat(p.getBlock(3), 0)).isCloseTo(3.14f, within(0.001f));
        // test double
        assertThat(p.getBlock(4)).isInstanceOf(LongArrayBlock.class);
        assertThat(DoubleType.DOUBLE.getDouble(p.getBlock(4), 0)).isCloseTo(9.81, within(0.001));
        // test string
        assertThat(p.getBlock(5)).isInstanceOf(VariableWidthBlock.class);
        assertThat(VARCHAR.getObject(p.getBlock(5), 0)).isEqualTo(Slices.utf8Slice(A_STRING_VALUE));
        // test bytes
        assertThat(p.getBlock(6)).isInstanceOf(VariableWidthBlock.class);
        assertThat(VarbinaryType.VARBINARY.getObject(p.getBlock(6), 0)).isEqualTo(Slices.wrappedBuffer(A_BYTES_VALUE));
        // test fixed
        assertThat(p.getBlock(7)).isInstanceOf(VariableWidthBlock.class);
        assertThat(VarbinaryType.VARBINARY.getObject(p.getBlock(7), 0)).isEqualTo(Slices.wrappedBuffer(A_FIXED_VALUE.bytes()));
        //test array
        assertThat(p.getBlock(8)).isInstanceOf(ArrayBlock.class);
        assertThat(ARRAY_INTEGER.getObject(p.getBlock(8), 0)).isInstanceOf(IntArrayBlock.class);
        assertBlockEquals(INTEGER, ARRAY_INTEGER.getObject(p.getBlock(8), 0), createIntsBlock(1, 2, 3, 4));
        // test map
        assertThat(p.getBlock(9)).isInstanceOf(MapBlock.class);
        assertThat(MAP_VARCHAR_INTEGER.getObjectValue(null, p.getBlock(9), 0)).isEqualTo(ImmutableMap.of("key1", 1, "key2", 2));
        // test enum
        assertThat(p.getBlock(10)).isInstanceOf(VariableWidthBlock.class);
        assertThat(VARCHAR.getObject(p.getBlock(10), 0)).isEqualTo(Slices.utf8Slice("A"));
        // test record
        assertThat(p.getBlock(11)).isInstanceOf(RowBlock.class);
        Block expected = createRowBlock(ImmutableList.of(INTEGER, DoubleType.DOUBLE, VARCHAR), new Object[] {5, 3.14159265358979, "Simple Record String Field"});
        assertBlockEquals(RowType.anonymousRow(INTEGER, DoubleType.DOUBLE, VARCHAR), p.getBlock(11), expected);
        // test nullable union
        assertThat(p.getBlock(12)).isInstanceOf(VariableWidthBlock.class);
        assertThat(p.getBlock(12).isNull(0)).isTrue();
    }
}
