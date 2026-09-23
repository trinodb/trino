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
package io.trino.parquet.reader;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.variant.Variant;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.RowType.rowType;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.variant.Metadata.EMPTY_METADATA;
import static org.assertj.core.api.Assertions.assertThat;

final class TestShreddedVariantReader
{
    private static final RowType SHREDDED_NODE_BIGINT = rowType(field("value", VARBINARY), field("typed_value", BIGINT));
    private static final Slice[] ONE_NULL_SLICE = new Slice[] {null};
    private static final Long[] ONE_NULL_LONG = new Long[] {null};

    @Test
    void testTopLevelScalar()
    {
        RowType structType = rowType(field("metadata", VARBINARY), field("value", VARBINARY), field("typed_value", BIGINT));
        Block struct = RowBlock.fromFieldBlocks(1, new Block[] {
                varbinary(EMPTY_METADATA.toSlice()),
                varbinary(ONE_NULL_SLICE),
                bigint(5L),
        });
        assertThat(reconstruct(structType, struct)).isEqualTo(5L);
    }

    @Test
    void testPartiallyShreddedObject()
    {
        // top value carries the non-shredded field c; typed_value shreds a (present) and b (missing)
        Variant partialValue = Variant.ofObject(ImmutableMap.of(utf8Slice("c"), Variant.ofLong(3)));
        RowType typedValueType = rowType(field("a", SHREDDED_NODE_BIGINT), field("b", SHREDDED_NODE_BIGINT));
        RowType structType = rowType(field("metadata", VARBINARY), field("value", VARBINARY), field("typed_value", typedValueType));

        Block fieldA = RowBlock.fromFieldBlocks(1, new Block[] {varbinary(ONE_NULL_SLICE), bigint(1L)});
        Block fieldB = RowBlock.fromFieldBlocks(1, new Block[] {varbinary(ONE_NULL_SLICE), bigint(ONE_NULL_LONG)});
        Block typedValue = RowBlock.fromFieldBlocks(1, new Block[] {fieldA, fieldB});
        Block struct = RowBlock.fromFieldBlocks(1, new Block[] {
                varbinary(partialValue.metadata().toSlice()),
                varbinary(partialValue.data()),
                typedValue,
        });

        assertThat(reconstruct(structType, struct)).isEqualTo(ImmutableMap.of("a", 1L, "c", 3L));
    }

    @Test
    void testShreddedArray()
    {
        RowType structType = rowType(field("metadata", VARBINARY), field("value", VARBINARY), field("typed_value", new ArrayType(SHREDDED_NODE_BIGINT)));
        Block elements = RowBlock.fromFieldBlocks(2, new Block[] {varbinary(null, null), bigint(1L, 2L)});
        Block array = ArrayBlock.fromElementBlock(1, Optional.empty(), new int[] {0, 2}, elements);
        Block struct = RowBlock.fromFieldBlocks(1, new Block[] {
                varbinary(EMPTY_METADATA.toSlice()),
                varbinary(ONE_NULL_SLICE),
                array,
        });
        assertThat(reconstruct(structType, struct)).isEqualTo(ImmutableList.of(1L, 2L));
    }

    private static Object reconstruct(RowType structType, Block struct)
    {
        List<Optional<Variant>> variants = ShreddedVariantReader.reconstructColumn(structType, struct, struct.getPositionCount());
        return variants.get(0).orElseThrow().toObject();
    }

    private static Block varbinary(Slice... values)
    {
        BlockBuilder builder = VARBINARY.createBlockBuilder(null, values.length);
        for (Slice value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                VARBINARY.writeSlice(builder, value);
            }
        }
        return builder.build();
    }

    private static Block bigint(Long... values)
    {
        BlockBuilder builder = BIGINT.createBlockBuilder(null, values.length);
        for (Long value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                BIGINT.writeLong(builder, value);
            }
        }
        return builder.build();
    }
}
