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
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.MapBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.type.VarcharType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.RandomData;
import org.junit.jupiter.api.Test;

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
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

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
        try (AvroFileReader avroFileReader = new AvroFileReader(input, ALL_TYPES_RECORD_SCHEMA, new BaseAvroTypeBlockHandler())) {
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
        try (AvroFileReader avroFileReader = new AvroFileReader(input, readSchema, new BaseAvroTypeBlockHandler())) {
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
        try (AvroFileReader avroFileReader = new AvroFileReader(input, readerSchema, new BaseAvroTypeBlockHandler())) {
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
                assertThat(block.getByte(0)).isGreaterThan((byte) 0);
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
        try (AvroFileReader avroFileReader = new AvroFileReader(input, ALL_TYPES_RECORD_SCHEMA, new BaseAvroTypeBlockHandler())) {
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
        try (AvroFileReader avroFileReader = new AvroFileReader(input, readSchemaBuilder.endRecord(), new BaseAvroTypeBlockHandler())) {
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
        try (AvroFileReader avroFileReader = new AvroFileReader(input, superSchema, new BaseAvroTypeBlockHandler())) {
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
        try (AvroFileReader avroFileReader = new AvroFileReader(input, reorderdSchema, new BaseAvroTypeBlockHandler())) {
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
    public void testReadUnionWithNonUnionAllCoercions()
            throws IOException, AvroTypeException
    {
        Schema nonUnion = Schema.create(Schema.Type.STRING);
        Schema union = Schema.createUnion(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.BYTES));

        Schema nonUnionRecord = SchemaBuilder.builder()
                .record("aRecord")
                .fields()
                .name("aField")
                .type(nonUnion)
                .noDefault()
                .endRecord();

        Schema unionRecord = SchemaBuilder.builder()
                .record("aRecord")
                .fields()
                .name("aField")
                .type(union)
                .noDefault()
                .endRecord();

        TrinoInputFile inputFile = createWrittenFileWithSchema(1000, unionRecord);

        //read the file with the non-union schema and ensure that no error thrown
        try (AvroFileReader avroFileReader = new AvroFileReader(inputFile, nonUnionRecord, new BaseAvroTypeBlockHandler())) {
            while (avroFileReader.hasNext()) {
                assertThat(avroFileReader.next()).isNotNull();
            }
        }
    }
}
