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
package io.trino.type;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.operator.FlatHashStrategy;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.variant.Variant;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.operator.FlatHashStrategyCompiler.compileFlatHashStrategy;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.VALUE_BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FLAT_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.VariantType.VARIANT;
import static io.trino.spi.variant.Metadata.EMPTY_METADATA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestVariantType
        extends AbstractTestType
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    TestVariantType()
    {
        super(VARIANT, Variant.class, createTestBlock());
    }

    public static ValueBlock createTestBlock()
    {
        BlockBuilder blockBuilder = VARIANT.createBlockBuilder(null, 15);
        VARIANT.writeObject(blockBuilder, Variant.NULL_VALUE);
        VARIANT.writeObject(blockBuilder, Variant.NULL_VALUE);
        VARIANT.writeObject(blockBuilder, Variant.ofBoolean(false));
        VARIANT.writeObject(blockBuilder, Variant.ofBoolean(false));
        VARIANT.writeObject(blockBuilder, Variant.ofBoolean(true));
        VARIANT.writeObject(blockBuilder, Variant.ofBoolean(true));
        VARIANT.writeObject(blockBuilder, Variant.ofInt(11));
        VARIANT.writeObject(blockBuilder, Variant.ofInt(11));
        VARIANT.writeObject(blockBuilder, Variant.ofString("hello"));
        VARIANT.writeObject(blockBuilder, Variant.ofString("hello"));
        VARIANT.writeObject(blockBuilder, Variant.ofDouble(44.44));
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        throw new UnsupportedOperationException();
    }

    @Test
    public void testRange()
    {
        assertThat(type.getRange())
                .isEmpty();
    }

    @Test
    public void testPreviousValue()
    {
        assertThatThrownBy(() -> type.getPreviousValue(getSampleValue()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Type is not orderable: " + type);
    }

    @Test
    public void testNextValue()
    {
        assertThatThrownBy(() -> type.getNextValue(getSampleValue()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Type is not orderable: " + type);
    }

    @Test
    public void testFlatWithRegion()
    {
        BlockBuilder blockBuilder = VARIANT.createBlockBuilder(null, 6);
        VARIANT.writeObject(blockBuilder, Variant.ofString("value before the region"));
        VARIANT.writeObject(blockBuilder, Variant.ofObject(ImmutableMap.of(utf8Slice("skipped"), Variant.ofInt(1))));
        VARIANT.writeObject(blockBuilder, Variant.ofInt(7));
        VARIANT.writeObject(blockBuilder, Variant.ofObject(ImmutableMap.of(
                utf8Slice("key"), Variant.ofString("value"),
                utf8Slice("another key"), Variant.ofArray(ImmutableList.of(Variant.ofLong(42), Variant.ofBoolean(true))))));
        VARIANT.writeObject(blockBuilder, Variant.ofString("hello"));
        VARIANT.writeObject(blockBuilder, Variant.ofArray(ImmutableList.of(Variant.ofObject(ImmutableMap.of(utf8Slice("nested"), Variant.ofDouble(1.5))))));
        ValueBlock block = blockBuilder.buildValueBlock();

        int offset = 2;
        ValueBlock region = block.getRegion(offset, block.getPositionCount() - offset);
        Block[] blocks = {region};
        FlatHashStrategy flatHashStrategy = compileFlatHashStrategy(ImmutableList.of(VARIANT), TYPE_OPERATORS);
        for (int position = 0; position < region.getPositionCount(); position++) {
            int variableWidth = VARIANT.getFlatVariableWidthSize(region, position);
            assertThat(variableWidth).isEqualTo(VARIANT.getFlatVariableWidthSize(block, offset + position));
            assertThat(flatHashStrategy.getTotalVariableWidth(blocks, position)).isEqualTo(variableWidth);

            byte[] fixedChunk = new byte[flatHashStrategy.getTotalFlatFixedLength()];
            byte[] variableChunk = new byte[variableWidth];
            flatHashStrategy.writeFlat(blocks, position, fixedChunk, 0, variableChunk, 0);
            assertThat(flatHashStrategy.valueIdentical(fixedChunk, 0, variableChunk, 0, blocks, position)).isTrue();
            assertThat(flatHashStrategy.hash(fixedChunk, 0, variableChunk, 0)).isEqualTo(flatHashStrategy.hash(blocks, position));

            BlockBuilder[] blockBuilders = {VARIANT.createBlockBuilder(null, 1)};
            flatHashStrategy.readFlat(fixedChunk, 0, variableChunk, 0, blockBuilders);
            assertThat(VARIANT.getObject(blockBuilders[0].build(), 0)).isEqualTo(VARIANT.getObject(region, position));
        }
    }

    @Test
    public void testFlatWriteFromStackMatchesBlock()
            throws Throwable
    {
        MethodHandle writeFlatFromStack = TYPE_OPERATORS.getReadValueOperator(VARIANT, simpleConvention(FLAT_RETURN, NEVER_NULL));
        MethodHandle writeFlatFromBlock = TYPE_OPERATORS.getReadValueOperator(VARIANT, simpleConvention(FLAT_RETURN, VALUE_BLOCK_POSITION_NOT_NULL));

        // a container without field names uses the shared empty metadata instance
        Variant arrayWithoutFieldNames = Variant.ofArray(ImmutableList.of(Variant.ofInt(1), Variant.ofString("two")));
        assertThat(arrayWithoutFieldNames.metadata()).isSameAs(EMPTY_METADATA);

        ImmutableList<Variant> values = ImmutableList.of(
                Variant.ofInt(7),
                Variant.ofString("hello"),
                arrayWithoutFieldNames,
                Variant.ofObject(ImmutableMap.of(utf8Slice("key"), Variant.ofString("value"))));
        for (Variant value : values) {
            BlockBuilder blockBuilder = VARIANT.createBlockBuilder(null, 1);
            VARIANT.writeObject(blockBuilder, value);
            ValueBlock block = blockBuilder.buildValueBlock();
            int variableWidth = VARIANT.getFlatVariableWidthSize(block, 0);

            byte[] stackFixed = new byte[VARIANT.getFlatFixedSize()];
            byte[] stackVariable = new byte[variableWidth];
            writeFlatFromStack.invoke(value, stackFixed, 0, stackVariable, 0);
            assertThat(VARIANT.getFlatVariableWidthLength(stackFixed, 0)).isEqualTo(variableWidth);

            byte[] blockFixed = new byte[VARIANT.getFlatFixedSize()];
            byte[] blockVariable = new byte[variableWidth];
            writeFlatFromBlock.invoke(block, 0, blockFixed, 0, blockVariable, 0);
            assertThat(stackFixed).isEqualTo(blockFixed);
            assertThat(stackVariable).isEqualTo(blockVariable);
        }
    }
}
