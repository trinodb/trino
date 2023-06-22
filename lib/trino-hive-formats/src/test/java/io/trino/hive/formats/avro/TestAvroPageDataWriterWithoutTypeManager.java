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
import io.airlift.slice.Slices;
import io.trino.filesystem.Location;
import io.trino.hive.formats.TrinoDataInputStream;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.block.BlockAssertions.createRowBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestAvroPageDataWriterWithoutTypeManager
        extends TestAvroBase
{
    @Test
    public void testAllTypesSimple()
            throws IOException, AvroTypeException
    {
        testAllTypesWriting(ALL_TYPES_RECORD_SCHEMA);
    }

    @Test
    public void testAllTypesReordered()
            throws IOException, AvroTypeException
    {
        testAllTypesWriting(reorderSchema(ALL_TYPES_RECORD_SCHEMA));
    }

    private void testAllTypesWriting(Schema writeSchema)
            throws AvroTypeException, IOException
    {
        Location tempTestLocation = createLocalTempLocation();
        try (AvroFileWriter fileWriter = new AvroFileWriter(
                trinoLocalFilesystem.newOutputFile(tempTestLocation).create(),
                writeSchema,
                NoOpAvroTypeManager.INSTANCE,
                AvroCompressionKind.NULL,
                ImmutableMap.of(),
                ALL_TYPES_RECORD_SCHEMA.getFields().stream().map(Schema.Field::name).collect(toImmutableList()),
                AvroTypeUtils.typeFromAvro(ALL_TYPES_RECORD_SCHEMA, NoOpAvroTypeManager.INSTANCE).getTypeParameters())) {
            fileWriter.write(ALL_TYPES_PAGE);
        }

        try (AvroFileReader fileReader = new AvroFileReader(trinoLocalFilesystem.newInputFile(tempTestLocation), ALL_TYPES_RECORD_SCHEMA, NoOpAvroTypeManager.INSTANCE)) {
            assertThat(fileReader.hasNext()).isTrue();
            assertIsAllTypesPage(fileReader.next());
            assertThat(fileReader.hasNext()).isFalse();
        }

        try (DataFileReader<GenericRecord> genericRecordDataFileReader = new DataFileReader<>(
                new AvroFileReader.TrinoDataInputStreamAsAvroSeekableInput(new TrinoDataInputStream(trinoLocalFilesystem.newInputFile(tempTestLocation).newStream()), trinoLocalFilesystem.newInputFile(tempTestLocation).length()),
                new GenericDatumReader<>(ALL_TYPES_RECORD_SCHEMA))) {
            assertThat(genericRecordDataFileReader.hasNext()).isTrue();
            assertThat(genericRecordDataFileReader.next()).isEqualTo(ALL_TYPES_GENERIC_RECORD);
            assertThat(genericRecordDataFileReader.hasNext()).isFalse();
        }
    }

    @Test
    public void testRLEAndDictionaryBlocks()
            throws IOException, AvroTypeException
    {
        Type simepleRecordType = RowType.anonymousRow(INTEGER, DoubleType.DOUBLE, VARCHAR);
        Schema testBlocksSchema = SchemaBuilder.builder()
                .record("testRLEAndDictionary")
                .fields()
                .name("rleInt")
                .type().intType().noDefault()
                .name("rleString")
                .type().stringType().noDefault()
                .name("dictString")
                .type().stringType().noDefault()
                .name("rleRow")
                .type(SIMPLE_RECORD_SCHEMA).noDefault()
                .name("dictRow")
                .type(SIMPLE_RECORD_SCHEMA).noDefault()
                .endRecord();

        Block expectedRLERow = createRowBlock(ImmutableList.of(INTEGER, DoubleType.DOUBLE, VARCHAR), new Object[] {5, 3.14159265358979, "Simple Record String Field"});
        Block expectedDictionaryRow = createRowBlock(ImmutableList.of(INTEGER, DoubleType.DOUBLE, VARCHAR), new Object[] {2, 27.9, "Sting1"});
        Page toWrite = new Page(
                RunLengthEncodedBlock.create(IntegerType.INTEGER, 2L, 2),
                RunLengthEncodedBlock.create(VarcharType.VARCHAR, Slices.utf8Slice("rleString"), 2),
                DictionaryBlock.create(2,
                        VarcharType.VARCHAR.createBlockBuilder(null, 3, 1)
                                .writeEntry(Slices.utf8Slice("A"))
                                .writeEntry(Slices.utf8Slice("B"))
                                .writeEntry(Slices.utf8Slice("C"))
                                .build(),
                        new int[] {1, 2}),
                RunLengthEncodedBlock.create(
                        expectedRLERow, 2),
                DictionaryBlock.create(2,
                        expectedDictionaryRow,
                        new int[] {0, 0}));

        Location testLocation = createLocalTempLocation();
        try (AvroFileWriter avroFileWriter = new AvroFileWriter(
                trinoLocalFilesystem.newOutputFile(testLocation).create(),
                testBlocksSchema,
                NoOpAvroTypeManager.INSTANCE,
                AvroCompressionKind.NULL,
                ImmutableMap.of(),
                testBlocksSchema.getFields().stream().map(Schema.Field::name).collect(toImmutableList()),
                AvroTypeUtils.typeFromAvro(testBlocksSchema, NoOpAvroTypeManager.INSTANCE).getTypeParameters())) {
            avroFileWriter.write(toWrite);
        }

        try (AvroFileReader avroFileReader = new AvroFileReader(
                trinoLocalFilesystem.newInputFile(testLocation),
                testBlocksSchema,
                NoOpAvroTypeManager.INSTANCE)) {
            assertThat(avroFileReader.hasNext()).isTrue();
            Page readPage = avroFileReader.next();
            assertThat(INTEGER.getInt(readPage.getBlock(0), 0)).isEqualTo(2);
            assertThat(INTEGER.getInt(readPage.getBlock(0), 1)).isEqualTo(2);
            assertThat(VarcharType.VARCHAR.getSlice(readPage.getBlock(1), 0)).isEqualTo(Slices.utf8Slice("rleString"));
            assertThat(VarcharType.VARCHAR.getSlice(readPage.getBlock(1), 1)).isEqualTo(Slices.utf8Slice("rleString"));
            assertThat(VarcharType.VARCHAR.getSlice(readPage.getBlock(2), 0)).isEqualTo(Slices.utf8Slice("B"));
            assertThat(VarcharType.VARCHAR.getSlice(readPage.getBlock(2), 1)).isEqualTo(Slices.utf8Slice("C"));
            assertBlockEquals(simepleRecordType, readPage.getBlock(3).getSingleValueBlock(0), expectedRLERow);
            assertBlockEquals(simepleRecordType, readPage.getBlock(3).getSingleValueBlock(1), expectedRLERow);
            assertBlockEquals(simepleRecordType, readPage.getBlock(4).getSingleValueBlock(0), expectedDictionaryRow);
            assertBlockEquals(simepleRecordType, readPage.getBlock(4).getSingleValueBlock(1), expectedDictionaryRow);
            assertThat(avroFileReader.hasNext()).isFalse();
        }
    }

    @Test
    public void testBlockUpcasting()
            throws IOException, AvroTypeException
    {
        Schema testCastingSchema = SchemaBuilder.builder()
                .record("testUpCasting")
                .fields()
                .name("byteToInt")
                .type().intType().noDefault()
                .name("shortToInt")
                .type().intType().noDefault()
                .name("byteToLong")
                .type().longType().noDefault()
                .name("shortToLong")
                .type().longType().noDefault()
                .name("intToLong")
                .type().longType().noDefault()
                .endRecord();

        BlockBuilder byteBlockBuilder = TINYINT.createBlockBuilder(null, 1);
        TINYINT.writeByte(byteBlockBuilder, (byte) 1);
        Block byteBlock = byteBlockBuilder.build();

        BlockBuilder shortBlockBuilder = SMALLINT.createBlockBuilder(null, 1);
        SMALLINT.writeShort(shortBlockBuilder, (short) 2);
        Block shortBlock = shortBlockBuilder.build();

        BlockBuilder integerBlockBuilder = INTEGER.createBlockBuilder(null, 1);
        INTEGER.writeInt(integerBlockBuilder, 4);
        Block integerBlock = integerBlockBuilder.build();

        Page toWrite = new Page(byteBlock, shortBlock, byteBlock, shortBlock, integerBlock);
        Location testLocation = createLocalTempLocation();
        try (AvroFileWriter avroFileWriter = new AvroFileWriter(
                trinoLocalFilesystem.newOutputFile(testLocation).create(),
                testCastingSchema,
                NoOpAvroTypeManager.INSTANCE,
                AvroCompressionKind.NULL,
                ImmutableMap.of(),
                ImmutableList.of("byteToInt", "shortToInt", "byteToLong", "shortToLong", "intToLong"),
                ImmutableList.of(TINYINT, SMALLINT, TINYINT, SMALLINT, INTEGER))) {
            avroFileWriter.write(toWrite);
        }

        try (AvroFileReader avroFileReader = new AvroFileReader(
                trinoLocalFilesystem.newInputFile(testLocation),
                testCastingSchema,
                NoOpAvroTypeManager.INSTANCE)) {
            assertThat(avroFileReader.hasNext()).isTrue();
            Page readPage = avroFileReader.next();
            assertThat(INTEGER.getInt(readPage.getBlock(0), 0)).isEqualTo(1);
            assertThat(INTEGER.getInt(readPage.getBlock(1), 0)).isEqualTo(2);
            assertThat(BIGINT.getLong(readPage.getBlock(2), 0)).isEqualTo(1);
            assertThat(BIGINT.getLong(readPage.getBlock(3), 0)).isEqualTo(2);
            assertThat(BIGINT.getLong(readPage.getBlock(4), 0)).isEqualTo(4);
            assertThat(avroFileReader.hasNext()).isFalse();
        }
    }
}
