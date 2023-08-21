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
import io.airlift.slice.Slices;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.local.LocalFileSystem;
import io.trino.hive.formats.TrinoDataInputStream;
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
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.RandomData;
import org.apache.avro.util.Utf8;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.block.BlockAssertions.createIntsBlock;
import static io.trino.block.BlockAssertions.createRowBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Float.floatToIntBits;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.within;

public abstract class TestAvroBase
{
    protected static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    protected static final ArrayType ARRAY_INTEGER = new ArrayType(INTEGER);
    protected static final MapType MAP_VARCHAR_VARCHAR = new MapType(VARCHAR, VARCHAR, TYPE_OPERATORS);
    protected static final MapType MAP_VARCHAR_INTEGER = new MapType(VARCHAR, INTEGER, TYPE_OPERATORS);
    protected TrinoFileSystem trinoLocalFilesystem;
    protected Path tempDirectory;

    protected static final Schema SIMPLE_RECORD_SCHEMA = SchemaBuilder.record("simpleRecord")
            .fields()
            .name("a")
            .type().intType().noDefault()
            .name("b")
            .type().doubleType().noDefault()
            .name("c")
            .type().stringType().noDefault()
            .endRecord();

    protected static final Schema SIMPLE_ENUM_SCHEMA = SchemaBuilder.enumeration("myEnumType").symbols("A", "B", "C");

    protected static final Schema ALL_TYPES_RECORD_SCHEMA = SchemaBuilder.builder()
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
    protected static final GenericRecord ALL_TYPES_GENERIC_RECORD;
    protected static final Page ALL_TYPES_PAGE;
    protected static final GenericRecord SIMPLE_GENERIC_RECORD;
    protected static final String A_STRING_VALUE = "a test string";
    protected static final ByteBuffer A_BYTES_VALUE = ByteBuffer.wrap("a test byte array".getBytes(StandardCharsets.UTF_8));
    protected static final GenericData.Fixed A_FIXED_VALUE;

    static {
        ImmutableList.Builder<Block> allTypeBlocks = ImmutableList.builder();
        SIMPLE_GENERIC_RECORD = new GenericData.Record(SIMPLE_RECORD_SCHEMA);
        SIMPLE_GENERIC_RECORD.put("a", 5);
        SIMPLE_GENERIC_RECORD.put("b", 3.14159265358979);
        SIMPLE_GENERIC_RECORD.put("c", "Simple Record String Field");

        UUID fixed = UUID.nameUUIDFromBytes("a test fixed".getBytes(StandardCharsets.UTF_8));
        A_FIXED_VALUE = new GenericData.Fixed(SchemaBuilder.builder().fixed("myFixedType").size(16), Bytes.concat(Longs.toByteArray(fixed.getMostSignificantBits()), Longs.toByteArray(fixed.getLeastSignificantBits())));

        ALL_TYPES_GENERIC_RECORD = new GenericData.Record(ALL_TYPES_RECORD_SCHEMA);
        ALL_TYPES_GENERIC_RECORD.put("aBoolean", true);
        allTypeBlocks.add(new ByteArrayBlock(1, Optional.empty(), new byte[]{1}));
        ALL_TYPES_GENERIC_RECORD.put("aInt", 42);
        allTypeBlocks.add(new IntArrayBlock(1, Optional.empty(), new int[]{42}));
        ALL_TYPES_GENERIC_RECORD.put("aLong", 3400L);
        allTypeBlocks.add(new LongArrayBlock(1, Optional.empty(), new long[]{3400L}));
        ALL_TYPES_GENERIC_RECORD.put("aFloat", 3.14f);
        allTypeBlocks.add(new IntArrayBlock(1, Optional.empty(), new int[]{floatToIntBits(3.14f)}));
        ALL_TYPES_GENERIC_RECORD.put("aDouble", 9.81);
        allTypeBlocks.add(new LongArrayBlock(1, Optional.empty(), new long[]{doubleToLongBits(9.81)}));
        ALL_TYPES_GENERIC_RECORD.put("aString", A_STRING_VALUE);
        allTypeBlocks.add(new VariableWidthBlock(1, Slices.utf8Slice(A_STRING_VALUE), new int[] {0, Slices.utf8Slice(A_STRING_VALUE).length()}, Optional.empty()));
        ALL_TYPES_GENERIC_RECORD.put("aBytes", A_BYTES_VALUE);
        allTypeBlocks.add(new VariableWidthBlock(1, Slices.wrappedHeapBuffer(A_BYTES_VALUE), new int[] {0, A_BYTES_VALUE.limit()}, Optional.empty()));
        ALL_TYPES_GENERIC_RECORD.put("aFixed", A_FIXED_VALUE);
        allTypeBlocks.add(new VariableWidthBlock(1, Slices.wrappedBuffer(A_FIXED_VALUE.bytes()), new int[] {0, A_FIXED_VALUE.bytes().length}, Optional.empty()));
        ALL_TYPES_GENERIC_RECORD.put("anArray", ImmutableList.of(1, 2, 3, 4));
        allTypeBlocks.add(ArrayBlock.fromElementBlock(1, Optional.empty(), new int[] {0, 4}, createIntsBlock(1, 2, 3, 4)));
        ALL_TYPES_GENERIC_RECORD.put("aMap", ImmutableMap.of(new Utf8("key1"), 1, new Utf8("key2"), 2));
        allTypeBlocks.add(MAP_VARCHAR_INTEGER.createBlockFromKeyValue(Optional.empty(),
                new int[] {0, 2},
                createStringsBlock("key1", "key2"),
                createIntsBlock(1, 2)));
        ALL_TYPES_GENERIC_RECORD.put("anEnum", new GenericData.EnumSymbol(SIMPLE_ENUM_SCHEMA, "A"));
        allTypeBlocks.add(new VariableWidthBlock(1, Slices.utf8Slice("A"), new int[] {0, 1}, Optional.empty()));
        ALL_TYPES_GENERIC_RECORD.put("aRecord", SIMPLE_GENERIC_RECORD);
        allTypeBlocks.add(createRowBlock(ImmutableList.of(INTEGER, DoubleType.DOUBLE, VARCHAR), new Object[] {5, 3.14159265358979, "Simple Record String Field"}));
        ALL_TYPES_GENERIC_RECORD.put("aUnion", null);
        allTypeBlocks.add(new VariableWidthBlock(1, Slices.wrappedBuffer(), new int[] {0, 0}, Optional.of(new boolean[] {true})));
        ALL_TYPES_PAGE = new Page(allTypeBlocks.build().toArray(Block[]::new));
    }

    @DataProvider(name = "testSchemas")
    public Object[][] testSchemaProvider()
    {
        return new Object[][] {
                {
                        SIMPLE_RECORD_SCHEMA
                },
                {
                     new Schema.Parser().parse("""
                             {
                                "type":"record",
                                "name":"test",
                                "fields":[
                                    {
                                        "name":"a",
                                        "type":"int"
                                    },
                                    {
                                        "name":"b",
                                        "type":["null", {
                                            "type":"array",
                                            "items":[""" + SIMPLE_RECORD_SCHEMA.toString() + """
                                            , "null"]
                                        }]
                                    },
                                    {
                                        "name":"c",
                                        "type":
                                        {
                                            "type":"map",
                                            "values":{
                                                "type":"enum",
                                                "name":"testingEnum",
                                                "symbols":["Apples","Bananas","Kiwi"]
                                            }
                                        }
                                    }
                                ]
                             }
                             """)
                },
                {
                    SchemaBuilder.builder().record("level1")
                            .fields()
                            .name("level1Field1")
                            .type(SchemaBuilder.record("level2")
                                    .fields()
                                    .name("level2Field1")
                                    .type(SchemaBuilder.record("level3")
                                            .fields()
                                            .name("level3Field1")
                                            .type(ALL_TYPES_RECORD_SCHEMA)
                                            .noDefault()
                                            .endRecord())
                                    .noDefault()
                                    .name("level2Field2")
                                    .type().optional().type(ALL_TYPES_RECORD_SCHEMA)
                                    .endRecord())
                            .noDefault()
                            .name("level1Field2")
                            .type(ALL_TYPES_RECORD_SCHEMA)
                            .noDefault()
                            .endRecord()
                }
        };
    }

    @BeforeClass
    public void setup()
    {
        try {
            tempDirectory = Files.createTempDirectory("testingAvro");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        trinoLocalFilesystem = new LocalFileSystem(tempDirectory.toAbsolutePath());
        // test identity
        assertIsAllTypesPage(ALL_TYPES_PAGE);
    }

    @Test(dataProvider = "testSchemas")
    public void testSerdeCycles(Schema schema)
            throws IOException, AvroTypeException
    {
        for (AvroCompressionKind compressionKind : AvroCompressionKind.values()) {
            if (compressionKind.isSupportedLocally()) {
                testSerdeCycles(schema, compressionKind);
            }
        }
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
    {
        try {
            trinoLocalFilesystem.deleteDirectory((Location.of("local:///")));
            Files.deleteIfExists(tempDirectory.toAbsolutePath());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void testSerdeCycles(Schema schema, AvroCompressionKind compressionKind)
            throws IOException, AvroTypeException
    {
        assertThat(schema.getType()).isEqualTo(Schema.Type.RECORD);
        Location temp1 = createLocalTempLocation();
        Location temp2 = createLocalTempLocation();

        int count = ThreadLocalRandom.current().nextInt(1000, 10000);
        ImmutableList.Builder<GenericRecord> testRecordsExpected = ImmutableList.builder();
        for (Object o : new RandomData(schema, count, true)) {
            testRecordsExpected.add((GenericRecord) o);
        }

        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        try (AvroFileReader fileReader = new AvroFileReader(
                createWrittenFileWithData(schema, testRecordsExpected.build(), temp1),
                schema,
                NoOpAvroTypeManager.INSTANCE)) {
            while (fileReader.hasNext()) {
                pages.add(fileReader.next());
            }
        }

        try (AvroFileWriter fileWriter = new AvroFileWriter(
                trinoLocalFilesystem.newOutputFile(temp2).create(),
                schema,
                NoOpAvroTypeManager.INSTANCE,
                compressionKind,
                ImmutableMap.of(),
                schema.getFields().stream().map(Schema.Field::name).collect(toImmutableList()),
                AvroTypeUtils.typeFromAvro(schema, NoOpAvroTypeManager.INSTANCE).getTypeParameters())) {
            for (Page p : pages.build()) {
                fileWriter.write(p);
            }
        }

        ImmutableList.Builder<GenericRecord> testRecordsActual = ImmutableList.builder();
        try (DataFileReader<GenericRecord> genericRecordDataFileReader = new DataFileReader<>(
                new AvroFileReader.TrinoDataInputStreamAsAvroSeekableInput(new TrinoDataInputStream(trinoLocalFilesystem.newInputFile(temp2).newStream()), trinoLocalFilesystem.newInputFile(temp2).length()),
                new GenericDatumReader<>())) {
            while (genericRecordDataFileReader.hasNext()) {
                testRecordsActual.add(genericRecordDataFileReader.next());
            }
        }
        assertThat(testRecordsExpected.build().size()).isEqualTo(testRecordsActual.build().size());
        List<GenericRecord> expected = testRecordsExpected.build();
        List<GenericRecord> actual = testRecordsActual.build();
        for (int i = 0; i < expected.size(); i++) {
            assertThat(expected.get(i)).isEqualTo(actual.get(i));
        }
    }

    protected Location createLocalTempLocation()
    {
        return Location.of("local:///" + UUID.randomUUID());
    }

    protected static void assertIsAllTypesPage(Page p)
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
        assertThat(VarbinaryType.VARBINARY.getObject(p.getBlock(6), 0)).isEqualTo(Slices.wrappedHeapBuffer(A_BYTES_VALUE));
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

    protected TrinoInputFile createWrittenFileWithData(Schema schema, List<GenericRecord> records)
            throws IOException
    {
        return createWrittenFileWithData(schema, records, createLocalTempLocation());
    }

    protected TrinoInputFile createWrittenFileWithData(Schema schema, List<GenericRecord> records, Location location)
            throws IOException
    {
        try (DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>())) {
            fileWriter.create(schema, trinoLocalFilesystem.newOutputFile(location).createOrOverwrite());
            for (GenericRecord genericRecord : records) {
                fileWriter.append(genericRecord);
            }
        }
        return trinoLocalFilesystem.newInputFile(location);
    }

    protected TrinoInputFile createWrittenFileWithSchema(int count, Schema schema)
            throws IOException
    {
        Iterator<Object> randomData = new RandomData(schema, count).iterator();
        Location tempFile = createLocalTempLocation();
        try (DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>())) {
            fileWriter.create(schema, trinoLocalFilesystem.newOutputFile(tempFile).createOrOverwrite());
            while (randomData.hasNext()) {
                fileWriter.append((GenericRecord) randomData.next());
            }
        }
        return trinoLocalFilesystem.newInputFile(tempFile);
    }

    static GenericRecord reorderGenericRecord(Schema reorderTo, GenericRecord record)
    {
        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(reorderTo);
        for (Schema.Field field : reorderTo.getFields()) {
            if (field.schema().getType() == Schema.Type.RECORD) {
                recordBuilder.set(field, reorderGenericRecord(field.schema(), (GenericRecord) record.get(field.name())));
            }
            else {
                recordBuilder.set(field, record.get(field.name()));
            }
        }
        return recordBuilder.build();
    }

    static <T> List<T> reorder(List<T> list)
    {
        List<T> l = new ArrayList<>(list);
        Collections.shuffle(l);
        return ImmutableList.copyOf(l);
    }

    static Schema reorderSchema(Schema schema)
    {
        verify(schema.getType() == Schema.Type.RECORD);
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.builder().record(schema.getName()).fields();
        for (Schema.Field field : reorder(schema.getFields())) {
            if (field.schema().getType() == Schema.Type.ENUM) {
                fieldAssembler = fieldAssembler.name(field.name())
                        .type(Schema.createEnum(field.schema().getName(), field.schema().getDoc(), field.schema().getNamespace(), Lists.reverse(field.schema().getEnumSymbols())))
                        .noDefault();
            }
            else if (field.schema().getType() == Schema.Type.UNION) {
                fieldAssembler = fieldAssembler.name(field.name())
                        .type(Schema.createUnion(reorder(field.schema().getTypes())))
                        .noDefault();
            }
            else if (field.schema().getType() == Schema.Type.RECORD) {
                fieldAssembler = fieldAssembler.name(field.name())
                        .type(reorderSchema(field.schema()))
                        .noDefault();
            }
            else {
                fieldAssembler = fieldAssembler.name(field.name()).type(field.schema()).noDefault();
            }
        }
        return fieldAssembler.endRecord();
    }
}
