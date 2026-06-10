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
import io.airlift.slice.Slices;
import io.trino.filesystem.TrinoInputFile;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.VariableWidthBlock;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static io.trino.hive.formats.avro.AvroHiveConstants.HIVE_UNION_TYPE_SCHEMA_PROP;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that Hive UNIONTYPE columns backed by simple nullable Avro unions produce a RowBlock
 * (not a scalar block) when the {@code HIVE_UNION_TYPE_SCHEMA_PROP} marker is set on the reader schema.
 *
 * <p>A Hive {@code uniontype<string>} column has Avro schema {@code ["null", "string"]}, which the Avro
 * reader normally optimizes to a scalar VARCHAR. Hive maps UNIONTYPE to {@code ROW(tag tinyint, field0, ...)},
 * so without the marker the column dereference projection would throw
 * {@code IllegalArgumentException: Unexpected block type: VariableWidthBlock} on sub-field access.
 */
public class TestHiveAvroUnionTypeReading
        extends TestAvroBase
{
    /**
     * A Hive {@code uniontype<string>} column has Avro writer schema {@code ["null", "string"]}.
     * With the marker on the reader schema the reader must produce a RowBlock with
     * {@code ROW(tag tinyint, field0 varchar)} instead of the scalar VariableWidthBlock.
     */
    @Test
    public void testSimpleNullableUnionWithMarkerProducesRowBlock()
            throws IOException, AvroTypeException
    {
        Schema writerSchema = SchemaBuilder.record("testRecord")
                .fields()
                .name("col").type().nullable().stringType().noDefault()
                .endRecord();
        Schema readerSchema = recordWithMarkedUnion(writerSchema.getField("col").schema());

        GenericRecord withValue = new GenericRecordBuilder(writerSchema).set("col", "hello").build();
        GenericRecord withNull = new GenericRecordBuilder(writerSchema).set("col", null).build();
        TrinoInputFile inputFile = createWrittenFileWithData(writerSchema, ImmutableList.of(withValue, withNull));

        try (AvroFileReader reader = new AvroFileReader(inputFile, readerSchema, new HiveAvroTypeBlockHandler(TIMESTAMP_MILLIS))) {
            Block block = readSingleColumnBlock(reader, 2);

            // Block must be dereferenceable as a RowBlock — without the fix it would be a VariableWidthBlock.
            List<Block> fields = RowBlock.getRowFieldsFromBlock(block);
            assertThat(fields).hasSize(2); // tag + field0

            // Row 0: col = "hello" → tag=0, field0="hello"
            assertThat(block.isNull(0)).isFalse();
            assertThat(TINYINT.getLong(fields.get(0), 0)).isEqualTo(0L);
            assertThat(VARCHAR.getSlice(fields.get(1), 0)).isEqualTo(Slices.utf8Slice("hello"));

            // Row 1: col = null → the whole row is null
            assertThat(block.isNull(1)).isTrue();
        }
    }

    /**
     * A Hive {@code uniontype<int>} column has Avro writer schema {@code ["null", "int"]}. Since the writer
     * is a union, Avro resolution produces a {@code WrittenUnionReadAction}; with the marker it must coerce
     * to a RowBlock instead of taking the scalar (IntArrayBlock) path.
     */
    @Test
    public void testWrittenNullableIntUnionWithMarkerProducesRowBlock()
            throws IOException, AvroTypeException
    {
        Schema writerSchema = SchemaBuilder.record("testRecord")
                .fields()
                .name("col").type().nullable().intType().noDefault()
                .endRecord();
        Schema readerSchema = recordWithMarkedUnion(writerSchema.getField("col").schema());

        GenericRecord withValue = new GenericRecordBuilder(writerSchema).set("col", 42).build();
        GenericRecord withNull = new GenericRecordBuilder(writerSchema).set("col", null).build();
        TrinoInputFile inputFile = createWrittenFileWithData(writerSchema, ImmutableList.of(withValue, withNull));

        try (AvroFileReader reader = new AvroFileReader(inputFile, readerSchema, new HiveAvroTypeBlockHandler(TIMESTAMP_MILLIS))) {
            Block block = readSingleColumnBlock(reader, 2);

            List<Block> fields = RowBlock.getRowFieldsFromBlock(block);
            assertThat(fields).hasSize(2); // tag + field0

            assertThat(block.isNull(0)).isFalse();
            assertThat(TINYINT.getLong(fields.get(0), 0)).isEqualTo(0L);
            assertThat(INTEGER.getLong(fields.get(1), 0)).isEqualTo(42L);

            assertThat(block.isNull(1)).isTrue();
        }
    }

    /**
     * A single-branch union {@code ["string"]} with the marker must also produce a RowBlock.
     * {@code isSimpleNullableUnion} treats a one-non-null-branch union as optimizable to a scalar, so the
     * marker is required to keep it a row.
     */
    @Test
    public void testSingleBranchUnionWithMarkerProducesRowBlock()
            throws IOException, AvroTypeException
    {
        Schema writerSchema = SchemaBuilder.record("testRecord")
                .fields()
                .name("col").type(Schema.createUnion(Schema.create(Schema.Type.STRING))).noDefault()
                .endRecord();
        Schema readerSchema = recordWithMarkedUnion(writerSchema.getField("col").schema());

        GenericRecord record = new GenericRecordBuilder(writerSchema).set("col", "world").build();
        TrinoInputFile inputFile = createWrittenFileWithData(writerSchema, ImmutableList.of(record));

        try (AvroFileReader reader = new AvroFileReader(inputFile, readerSchema, new HiveAvroTypeBlockHandler(TIMESTAMP_MILLIS))) {
            Block block = readSingleColumnBlock(reader, 1);

            List<Block> fields = RowBlock.getRowFieldsFromBlock(block);
            assertThat(fields).hasSize(2); // tag + field0
            assertThat(block.isNull(0)).isFalse();
            assertThat(TINYINT.getLong(fields.get(0), 0)).isEqualTo(0L);
            assertThat(VARCHAR.getSlice(fields.get(1), 0)).isEqualTo(Slices.utf8Slice("world"));
        }
    }

    /**
     * A multi-branch union {@code ["null", "int", "string"]} already coerces to a RowBlock without a marker
     * (it is not a simple nullable union). Regression test ensuring that path is unchanged.
     */
    @Test
    public void testMultiBranchUnionProducesRowBlockWithoutMarker()
            throws IOException, AvroTypeException
    {
        Schema schema = SchemaBuilder.record("testRecord")
                .fields()
                .name("col").type(Schema.createUnion(
                        Schema.create(Schema.Type.NULL),
                        Schema.create(Schema.Type.INT),
                        Schema.create(Schema.Type.STRING)))
                .noDefault()
                .endRecord();

        GenericRecord withInt = new GenericRecordBuilder(schema).set("col", 42).build();
        GenericRecord withNull = new GenericRecordBuilder(schema).set("col", null).build();
        TrinoInputFile inputFile = createWrittenFileWithData(schema, ImmutableList.of(withInt, withNull));

        try (AvroFileReader reader = new AvroFileReader(inputFile, schema, new HiveAvroTypeBlockHandler(TIMESTAMP_MILLIS))) {
            Block block = readSingleColumnBlock(reader, 2);

            List<Block> fields = RowBlock.getRowFieldsFromBlock(block);
            assertThat(fields).hasSize(3); // tag + field0(int) + field1(varchar)

            assertThat(block.isNull(0)).isFalse();
            assertThat(TINYINT.getLong(fields.get(0), 0)).isEqualTo(0L); // int branch → channel 0
            assertThat(block.isNull(1)).isTrue();
        }
    }

    /**
     * Without the marker, a simple nullable union {@code ["null", "string"]} still optimizes to a scalar type
     * (VariableWidthBlock for string). Ensures regular nullable Avro columns that are not Hive UNIONTYPE are
     * unaffected by the fix.
     */
    @Test
    public void testSimpleNullableUnionWithoutMarkerPreservesScalarBehavior()
            throws IOException, AvroTypeException
    {
        Schema schema = SchemaBuilder.record("testRecord")
                .fields()
                .name("col").type().nullable().stringType().noDefault()
                .endRecord();

        GenericRecord record = new GenericRecordBuilder(schema).set("col", "hello").build();
        TrinoInputFile inputFile = createWrittenFileWithData(schema, ImmutableList.of(record));

        try (AvroFileReader reader = new AvroFileReader(inputFile, schema, new HiveAvroTypeBlockHandler(TIMESTAMP_MILLIS))) {
            Block block = readSingleColumnBlock(reader, 1);
            assertThat(block).isInstanceOf(VariableWidthBlock.class);
            assertThat(VARCHAR.getSlice(block, 0)).isEqualTo(Slices.utf8Slice("hello"));
        }
    }

    private static Schema recordWithMarkedUnion(Schema unionFieldSchema)
    {
        Schema markedFieldSchema = Schema.createUnion(unionFieldSchema.getTypes());
        markedFieldSchema.addProp(HIVE_UNION_TYPE_SCHEMA_PROP, true);
        return Schema.createRecord(
                "testRecord",
                null,
                null,
                false,
                List.of(new Schema.Field("col", markedFieldSchema, null)));
    }

    private static Block readSingleColumnBlock(AvroFileReader reader, int expectedPositions)
            throws IOException
    {
        List<Page> pages = new ArrayList<>();
        while (reader.hasNext()) {
            pages.add(reader.next());
        }
        assertThat(pages).hasSize(1);
        Block block = pages.getFirst().getBlock(0);
        assertThat(block.getPositionCount()).isEqualTo(expectedPositions);
        return block;
    }
}
