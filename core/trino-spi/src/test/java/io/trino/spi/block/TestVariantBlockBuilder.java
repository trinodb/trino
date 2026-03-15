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
package io.trino.spi.block;

import io.trino.spi.variant.Header;
import io.trino.spi.variant.Variant;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

final class TestVariantBlockBuilder
        extends AbstractTestBlockBuilder<Variant>
{
    private static final int VARIANT_ENTRY_SIZE = Integer.BYTES + Byte.BYTES;
    private static final int NULL_VARIANT_FIELD_ENTRY_SIZE = (Integer.BYTES + Byte.BYTES) * 2;

    @Test
    public void testAppendRangeUpdatesStatus()
    {
        int length = 4;
        VariantBlock source = createAllNullSourceBlock(length + 1);
        PageBuilderStatus pageBuilderStatus = new PageBuilderStatus(Integer.MAX_VALUE);
        VariantBlockBuilder blockBuilder = new VariantBlockBuilder(pageBuilderStatus.createBlockBuilderStatus(), 1);

        blockBuilder.appendRange(source, 1, length);

        assertThat(pageBuilderStatus.getSizeInBytes())
                .isEqualTo((long) length * (VARIANT_ENTRY_SIZE + NULL_VARIANT_FIELD_ENTRY_SIZE));
    }

    @Test
    public void testAppendPositionsUpdatesStatusWithOffsetSource()
    {
        int length = 3;
        VariantBlock source = createAllNullSourceBlock(length + 3).getRegion(1, length + 1);
        PageBuilderStatus pageBuilderStatus = new PageBuilderStatus(Integer.MAX_VALUE);
        VariantBlockBuilder blockBuilder = new VariantBlockBuilder(pageBuilderStatus.createBlockBuilderStatus(), 1);

        blockBuilder.appendPositions(source, new int[] {-1, 3, 0, 2, -1}, 1, length);

        assertThat(pageBuilderStatus.getSizeInBytes())
                .isEqualTo((long) length * (VARIANT_ENTRY_SIZE + NULL_VARIANT_FIELD_ENTRY_SIZE));
    }

    @Test
    public void testResetToRecomputesNullFlags()
    {
        VariantBlockBuilder blockBuilder = new VariantBlockBuilder(null, 2);
        blockBuilder.appendNull();
        blockBuilder.resetTo(0);
        blockBuilder.writeEntry(Variant.ofLong(1));

        VariantBlock block = blockBuilder.buildValueBlock();

        assertThat(block.mayHaveNull()).isFalse();
        assertThat(block.isNull(0)).isFalse();
    }

    @Test
    public void testGetBasicTypeWithDictionaryBackedFields()
    {
        VariantBlockBuilder sourceBuilder = new VariantBlockBuilder(null, 2);
        sourceBuilder.writeEntry(Variant.ofBoolean(true));
        sourceBuilder.writeEntry(Variant.ofString("abc"));
        VariantBlock source = sourceBuilder.buildValueBlock();

        int[] ids = {1, 0};
        Block metadata = DictionaryBlock.create(ids.length, source.getRawMetadata(), ids);
        Block values = DictionaryBlock.create(ids.length, source.getRawValues(), ids);
        VariantBlock dictionaryBlock = VariantBlock.create(ids.length, metadata, values, Optional.empty());

        assertThat(dictionaryBlock.getBasicType(0)).isEqualTo(Header.BasicType.SHORT_STRING);
        assertThat(dictionaryBlock.getBasicType(1)).isEqualTo(Header.BasicType.PRIMITIVE);
    }

    @Override
    protected BlockBuilder createBlockBuilder()
    {
        return new VariantBlockBuilder(null, 1);
    }

    @Override
    protected List<Variant> getTestValues()
    {
        return List.of(Variant.ofBoolean(true), Variant.ofByte((byte) 90), Variant.ofInt(91), Variant.ofDouble(92.12), Variant.ofString("ninty three"));
    }

    @Override
    protected Variant getUnusedTestValue()
    {
        return Variant.ofString("unused value");
    }

    @Override
    protected ValueBlock blockFromValues(Iterable<Variant> values)
    {
        VariantBlockBuilder blockBuilder = new VariantBlockBuilder(null, 1);
        for (Variant value : values) {
            if (value == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeEntry(value);
            }
        }
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected List<Variant> blockToValues(ValueBlock valueBlock)
    {
        VariantBlock variantBlock = (VariantBlock) valueBlock;
        List<Variant> actualValues = new ArrayList<>(variantBlock.getPositionCount());
        for (int i = 0; i < variantBlock.getPositionCount(); i++) {
            if (variantBlock.isNull(i)) {
                actualValues.add(null);
            }
            else {
                actualValues.add(variantBlock.getVariant(i));
            }
        }
        return actualValues;
    }

    private static VariantBlock createAllNullSourceBlock(int positions)
    {
        VariantBlockBuilder blockBuilder = new VariantBlockBuilder(null, 1);
        for (int i = 0; i < positions; i++) {
            blockBuilder.appendNull();
        }
        return blockBuilder.buildValueBlock();
    }
}
