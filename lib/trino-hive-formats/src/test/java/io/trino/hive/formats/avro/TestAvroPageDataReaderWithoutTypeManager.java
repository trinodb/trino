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
import io.airlift.slice.Slice;
import io.trino.filesystem.TrinoInputFile;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.MapBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.type.VarcharType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.RandomData;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestAvroPageDataReaderWithoutTypeManager
        extends TestAvroBase
{
    private static final Schema SIMPLE_ENUM_SCHEMA = SchemaBuilder.enumeration("myEnumType").symbols("A", "B", "C");
    private static final Schema SIMPLE_ENUM_SUPER_SCHEMA = SchemaBuilder.enumeration("myEnumType").symbols("A", "B", "C", "D");
    private static final Schema SIMPLE_ENUM_REORDERED = SchemaBuilder.enumeration("myEnumType").symbols("C", "D", "B", "A");

    @Test
    public void testAllTypesSimple()
            throws IOException, AvroTypeException
    {
        TrinoInputFile input = createWrittenFileWithData(ALL_TYPES_RECORD_SCHEMA, ImmutableList.of(ALL_TYPES_GENERIC_RECORD));
        try (AvroFileReader avroFileReader = new AvroFileReader(input, ALL_TYPES_RECORD_SCHEMA, NoOpAvroTypeManager.INSTANCE)) {
            int totalRecords = 0;
            while (avroFileReader.hasNext()) {
                Page p = avroFileReader.next();
                assertIsAllTypesPage(p);
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
        Schema writerSchema = reorderSchema(ALL_TYPES_RECORD_SCHEMA);
        TrinoInputFile input = createWrittenFileWithData(writerSchema, ImmutableList.of(reorderGenericRecord(writerSchema, ALL_TYPES_GENERIC_RECORD)));
        try (AvroFileReader avroFileReader = new AvroFileReader(input, ALL_TYPES_RECORD_SCHEMA, NoOpAvroTypeManager.INSTANCE)) {
            int totalRecords = 0;
            while (avroFileReader.hasNext()) {
                Page p = avroFileReader.next();
                assertIsAllTypesPage(p);
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
}
