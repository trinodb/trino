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

import com.google.common.collect.Iterables;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.nCopies;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractTestBlockBuilder<T>
{
    protected abstract BlockBuilder createBlockBuilder();

    protected abstract List<T> getTestValues();

    protected abstract T getUnusedTestValue();

    protected abstract ValueBlock blockFromValues(Iterable<T> values);

    protected abstract List<T> blockToValues(ValueBlock valueBlock);

    @Test
    public void verifyTestData()
    {
        List<T> values = getTestValues();
        assertThat(values)
                .hasSize(5)
                .doesNotHaveDuplicates()
                .doesNotContainNull()
                .doesNotContain(getUnusedTestValue());

        ValueBlock valueBlock = blockFromValues(values);
        assertThat(blockToValues(valueBlock)).isEqualTo(values);
    }

    @Test
    public void testAppend()
    {
        List<T> values = getTestValues();
        ValueBlock inputValues = createOffsetBlock(values);

        BlockBuilder blockBuilder = createBlockBuilder();
        blockBuilder.append(inputValues, 1);
        blockBuilder.append(inputValues, 3);
        blockBuilder.append(inputValues, 1);
        blockBuilder.append(inputValues, 3);
        ValueBlock valueBlock = blockBuilder.buildValueBlock();

        assertThat(valueBlock.mayHaveNull()).isFalse();

        List<T> actualValues = blockToValues(valueBlock);
        assertThat(actualValues).containsExactly(values.get(1), values.get(3), values.get(1), values.get(3));
    }

    @Test
    public void testAppendWithNulls()
    {
        List<T> values = getTestValues();
        ValueBlock inputValues = createOffsetBlockWithOddPositionsNull(values);

        BlockBuilder blockBuilder = createBlockBuilder();
        blockBuilder.append(inputValues, 1);
        blockBuilder.append(inputValues, 3);
        ValueBlock valueBlock = blockBuilder.buildValueBlock();

        assertThat(valueBlock.mayHaveNull()).isTrue();

        List<T> actualValues = blockToValues(valueBlock);
        assertThat(actualValues).hasSize(2).containsOnlyNulls();

        // add a non-null value
        blockBuilder.append(inputValues, 2);
        valueBlock = blockBuilder.buildValueBlock();

        actualValues = blockToValues(valueBlock);
        assertThat(actualValues).containsExactly(null, null, values.get(2));
    }

    @Test
    public void testAppendRepeated()
    {
        List<T> values = getTestValues();
        ValueBlock inputValues = createOffsetBlock(values);

        BlockBuilder blockBuilder = createBlockBuilder();
        blockBuilder.appendRepeated(inputValues, 1, 10);
        blockBuilder.appendRepeated(inputValues, 3, 10);
        ValueBlock valueBlock = blockBuilder.buildValueBlock();

        assertThat(valueBlock.mayHaveNull()).isFalse();

        List<T> actualValues = blockToValues(valueBlock);
        assertThat(actualValues).containsExactlyElementsOf(Iterables.concat(nCopies(10, values.get(1)), nCopies(10, values.get(3))));
    }

    @Test
    public void testAppendRepeatedWithNulls()
    {
        List<T> values = getTestValues();
        ValueBlock inputValues = createOffsetBlockWithOddPositionsNull(values);

        BlockBuilder blockBuilder = createBlockBuilder();
        blockBuilder.appendRepeated(inputValues, 1, 10);
        blockBuilder.appendRepeated(inputValues, 3, 10);
        ValueBlock valueBlock = blockBuilder.buildValueBlock();

        assertThat(valueBlock.mayHaveNull()).isTrue();

        List<T> actualValues = blockToValues(valueBlock);
        assertThat(actualValues).hasSize(20).containsOnlyNulls();

        // an all-null block should be converted to a RunLengthEncodedBlock
        assertThat(blockBuilder.build()).isInstanceOf(RunLengthEncodedBlock.class);

        // add some non-null values
        blockBuilder.appendRepeated(inputValues, 2, 10);
        valueBlock = blockBuilder.buildValueBlock();

        actualValues = blockToValues(valueBlock);
        assertThat(actualValues).containsExactlyElementsOf(Iterables.concat(nCopies(20, null), nCopies(10, values.get(2))));
    }

    @Test
    public void testAppendRange()
    {
        List<T> values = getTestValues();
        ValueBlock inputValues = createOffsetBlock(values);

        BlockBuilder blockBuilder = createBlockBuilder();
        blockBuilder.appendRange(inputValues, 1, 3);
        blockBuilder.appendRange(inputValues, 2, 3);
        ValueBlock valueBlock = blockBuilder.buildValueBlock();

        assertThat(valueBlock.mayHaveNull()).isFalse();

        List<T> actualValues = blockToValues(valueBlock);
        assertThat(actualValues).containsExactlyElementsOf(Iterables.concat(values.subList(1, 4), values.subList(2, 5)));
    }

    @Test
    public void testAppendRangeWithNulls()
    {
        List<T> values = getTestValues();
        ValueBlock inputValues = createOffsetBlockWithOddPositionsNull(values);

        BlockBuilder blockBuilder = createBlockBuilder();
        blockBuilder.appendRange(inputValues, 1, 3);
        blockBuilder.appendRange(inputValues, 2, 3);
        ValueBlock valueBlock = blockBuilder.buildValueBlock();

        assertThat(valueBlock.mayHaveNull()).isTrue();

        List<T> actualValues = blockToValues(valueBlock);
        assertThat(actualValues).containsExactly(null, values.get(2), null, values.get(2), null, values.get(4));
    }

    @Test
    public void testAppendPositions()
    {
        List<T> values = getTestValues();
        ValueBlock inputValues = createOffsetBlock(values);

        BlockBuilder blockBuilder = createBlockBuilder();
        blockBuilder.appendPositions(inputValues, new int[] {-100, 1, 3, 2, -100}, 1, 3);
        blockBuilder.appendPositions(inputValues, new int[] {-100, 4, 0, -100}, 1, 2);
        ValueBlock valueBlock = blockBuilder.buildValueBlock();

        assertThat(valueBlock.mayHaveNull()).isFalse();

        List<T> actualValues = blockToValues(valueBlock);
        assertThat(actualValues).containsExactly(values.get(1), values.get(3), values.get(2), values.get(4), values.get(0));
    }

    @Test
    public void testAppendPositionsWithNull()
    {
        List<T> values = getTestValues();
        ValueBlock inputValues = createOffsetBlockWithOddPositionsNull(values);

        BlockBuilder blockBuilder = createBlockBuilder();
        blockBuilder.appendPositions(inputValues, new int[] {-100, 1, 3, 2, -100}, 1, 3);
        blockBuilder.appendPositions(inputValues, new int[] {-100, 4, 0, -100}, 1, 2);
        ValueBlock valueBlock = blockBuilder.buildValueBlock();

        assertThat(valueBlock.mayHaveNull()).isTrue();

        List<T> actualValues = blockToValues(valueBlock);
        assertThat(actualValues).containsExactly(null, null, values.get(2), values.get(4), values.get(0));
    }

    @Test
    public void testResetTo()
    {
        List<T> values = getTestValues();
        ValueBlock inputValues = createOffsetBlock(values);

        BlockBuilder blockBuilder = createBlockBuilder();
        blockBuilder.appendRange(inputValues, 0, inputValues.getPositionCount());
        assertThat(blockToValues(blockBuilder.buildValueBlock())).containsExactlyElementsOf(values);

        blockBuilder.resetTo(4);
        assertThat(blockToValues(blockBuilder.buildValueBlock())).containsExactlyElementsOf(values.subList(0, 4));

        blockBuilder.appendRange(inputValues, 0, inputValues.getPositionCount());
        assertThat(blockToValues(blockBuilder.buildValueBlock())).containsExactlyElementsOf(Iterables.concat(values.subList(0, 4), values));

        blockBuilder.resetTo(0);
        assertThat(blockToValues(blockBuilder.buildValueBlock())).isEmpty();

        blockBuilder.appendRange(inputValues, 0, inputValues.getPositionCount());
        assertThat(blockToValues(blockBuilder.buildValueBlock())).containsExactlyElementsOf(values);
    }

    /**
     * Create a block that is offset from the start of the underlying array
     */
    private ValueBlock createOffsetBlock(List<T> values)
    {
        return blockFromValues(Iterables.concat(nCopies(2, getUnusedTestValue()), values, nCopies(2, getUnusedTestValue())))
                .getRegion(2, values.size());
    }

    /**
     * Create a block that is offset from the start of the underlying array
     */
    private ValueBlock createOffsetBlockWithOddPositionsNull(List<T> values)
    {
        ArrayList<T> blockValues = new ArrayList<>();
        blockValues.add(getUnusedTestValue());
        blockValues.add(getUnusedTestValue());
        for (int i = 0; i < values.size(); i++) {
            T value = values.get(i);
            if (i % 2 == 0) {
                blockValues.add(value);
            }
            else {
                blockValues.add(null);
            }
        }
        blockValues.add(getUnusedTestValue());
        blockValues.add(getUnusedTestValue());
        return blockFromValues(blockValues).getRegion(2, values.size());
    }
}
