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
package io.trino.plugin.iceberg;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.block.RowBlock.getRowFieldsFromBlock;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.RowType.rowType;
import static io.trino.type.UnknownType.UNKNOWN;
import static org.assertj.core.api.Assertions.assertThat;

class TestUnknownTypes
{
    private static final Types.StructType ROW = Types.StructType.of(
            Types.NestedField.optional(4, "missing", Types.UnknownType.get()),
            Types.NestedField.required(5, "value", Types.IntegerType.get()));
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.optional(1, "missing", Types.UnknownType.get()),
            Types.NestedField.required(2, "id", Types.IntegerType.get()),
            Types.NestedField.optional(3, "data", ROW));

    @Test
    void testSchemaProjectionPreservesIdsAndRequiredFields()
    {
        Schema result = UnknownTypes.removeUnknown(SCHEMA);
        assertThat(result.columns()).extracting(Types.NestedField::fieldId).containsExactly(2, 3);
        assertThat(result.findField(2)).isEqualTo(SCHEMA.findField(2));
        assertThat(result.findField(3).isOptional()).isTrue();
        assertThat(result.findField(3).type().asStructType().fields()).containsExactly(ROW.field(5));
        assertThat(result.findField(1)).isNull();
        assertThat(result.findField(4)).isNull();
        assertThat(UnknownTypes.containsUnknown(result.asStruct())).isFalse();
        assertThat(UnknownTypes.isWriteSupported(SCHEMA)).isTrue();
    }

    @Test
    void testUnsupportedOmittedShapes()
    {
        assertThat(UnknownTypes.isWriteSupported(new Schema(
                Types.NestedField.optional(1, "missing", Types.UnknownType.get())))).isFalse();
        assertThat(UnknownTypes.isWriteSupported(new Schema(
                Types.NestedField.optional(1, "row", Types.StructType.of(
                        Types.NestedField.optional(2, "missing", Types.UnknownType.get())))))).isFalse();
        assertThat(UnknownTypes.isWriteSupported(new Schema(
                Types.NestedField.optional(1, "array", Types.ListType.ofOptional(2, ROW))))).isFalse();
        assertThat(UnknownTypes.isWriteSupported(new Schema(
                Types.NestedField.optional(1, "map", Types.MapType.ofOptional(2, 3, Types.StringType.get(), Types.UnknownType.get()))))).isFalse();
        Schema known = new Schema(Types.NestedField.optional(1, "array", Types.ListType.ofOptional(2, Types.IntegerType.get())));
        assertThat(UnknownTypes.isWriteSupported(known)).isTrue();
        assertThat(UnknownTypes.removeUnknown(known)).isSameAs(known);
    }

    @Test
    void testNullableDictionaryRowProjection()
    {
        Block row = RowBlock.fromNotNullSuppressedFieldBlocks(
                3,
                Optional.of(new long[] {0b101}),
                new Block[] {nulls(3), new IntArrayBlock(3, Optional.of(new long[] {0b101}), new int[] {10, 0, 20})});
        Block dictionary = DictionaryBlock.create(4, row, new int[] {2, 1, 0, 2});
        Page input = new Page(nulls(4), new IntArrayBlock(4, Optional.empty(), new int[] {3, 2, 1, 3}), dictionary);
        Page result = UnknownTypes.removeUnknown(input, List.of(UNKNOWN, INTEGER, rowType(field("missing", UNKNOWN), field("value", INTEGER))), SCHEMA);

        assertThat(result.getPositionCount()).isEqualTo(4);
        assertThat(result.getChannelCount()).isEqualTo(2);
        assertThat(INTEGER.getInt(result.getBlock(0), 0)).isEqualTo(3);
        assertThat(INTEGER.getInt(result.getBlock(0), 2)).isEqualTo(1);
        Block projected = result.getBlock(1);
        assertThat(projected.isNull(1)).isTrue();
        assertThat(projected.isNull(0)).isFalse();
        List<Block> fields = getRowFieldsFromBlock(projected);
        assertThat(fields).hasSize(1);
        assertThat(INTEGER.getInt(fields.getFirst(), 0)).isEqualTo(20);
        assertThat(INTEGER.getInt(fields.getFirst(), 2)).isEqualTo(10);
        assertThat(INTEGER.getInt(fields.getFirst(), 3)).isEqualTo(20);
    }

    @Test
    void testAllNullRunLengthEncodedRowProjection()
    {
        Block row = RowBlock.fromNotNullSuppressedFieldBlocks(
                1,
                Optional.of(new long[] {0}),
                new Block[] {nulls(1), new IntArrayBlock(1, Optional.of(new long[] {0}), new int[] {0})});
        Page input = new Page(nulls(4), new IntArrayBlock(4, Optional.empty(), new int[] {1, 2, 3, 4}), RunLengthEncodedBlock.create(row, 4));
        Page result = UnknownTypes.removeUnknown(input, List.of(UNKNOWN, INTEGER, rowType(field("missing", UNKNOWN), field("value", INTEGER))), SCHEMA);
        assertThat(result.getPositionCount()).isEqualTo(4);
        assertThat(result.getChannelCount()).isEqualTo(2);
        for (int position = 0; position < 4; position++) {
            assertThat(result.getBlock(1).isNull(position)).isTrue();
            assertThat(INTEGER.getInt(result.getBlock(0), position)).isEqualTo(position + 1);
        }
    }

    private static Block nulls(int count)
    {
        return RunLengthEncodedBlock.create(UNKNOWN.createBlockBuilder(null, 1).appendNull().build(), count);
    }
}
