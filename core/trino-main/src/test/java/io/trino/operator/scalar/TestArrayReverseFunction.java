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
package io.trino.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.type.ArrayType;
import io.trino.sql.gen.ColumnarProjectionAssertions;
import io.trino.sql.gen.ColumnarProjectionAssertions.Input;
import io.trino.sql.ir.Reference;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.IntStream;

import static io.trino.block.BlockAssertions.createArrayBigintBlock;
import static io.trino.spi.function.SelectedPositions.positionsList;
import static io.trino.spi.function.SelectedPositions.positionsRange;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestArrayReverseFunction
{
    private static final ArrayType ARRAY_TYPE = new ArrayType(BIGINT);
    private static final ColumnarProjectionAssertions COLUMNAR = new ColumnarProjectionAssertions(new TestingFunctionResolution());
    private static final int POSITION_COUNT = 40;

    @Test
    public void testColumnarProjection()
    {
        ArrayBlock input = arrays(ImmutableList.of(1L, 2L, 3L), null, ImmutableList.of(), ImmutableList.of(4L, 5L));
        Block result = ArrayReverseFunction.reverseColumnar(positionsRange(0, 4), input);

        assertArrays(result, ImmutableList.of(3L, 2L, 1L), null, ImmutableList.of(), ImmutableList.of(5L, 4L));
        assertThat(result).isInstanceOf(ArrayBlock.class);
        ArrayBlock resultArray = (ArrayBlock) result;
        assertThat(resultArray.getElementsBlock()).isInstanceOf(DictionaryBlock.class);
        assertThat(resultArray.getRawOffsets()).isSameAs(input.getRawOffsets());
        assertThat(resultArray.getRawValueIsValid()).isSameAs(input.getRawValueIsValid());
    }

    @Test
    public void testColumnarProjectionSelectedPositions()
    {
        ArrayBlock input = arrays(ImmutableList.of(1L, 2L, 3L), null, ImmutableList.of(6L), ImmutableList.of(4L, 5L));
        Block result = ArrayReverseFunction.reverseColumnar(positionsList(new int[] {3, 1, 0}, 0, 3), input);

        assertArrays(result, ImmutableList.of(5L, 4L), null, ImmutableList.of(3L, 2L, 1L));
        // the unselected array is not reversed
        assertThat(((ArrayBlock) result).getElementsBlock().getPositionCount()).isEqualTo(5);
    }

    @SafeVarargs
    private static ArrayBlock arrays(List<Long>... values)
    {
        ArrayBlockBuilder builder = ARRAY_TYPE.createBlockBuilder(null, values.length);
        for (List<Long> value : values) {
            if (value == null) {
                builder.appendNull();
                continue;
            }
            builder.buildEntry(elements -> value.forEach(element -> BIGINT.writeLong(elements, element)));
        }
        return (ArrayBlock) builder.build();
    }

    @SafeVarargs
    private static void assertArrays(Block block, List<Long>... expected)
    {
        assertThat(block.getPositionCount()).isEqualTo(expected.length);
        for (int position = 0; position < expected.length; position++) {
            if (expected[position] == null) {
                assertThat(block.isNull(position)).isTrue();
            }
            else {
                assertThat(ARRAY_TYPE.getObjectValue(block, position)).isEqualTo(expected[position]);
            }
        }
    }

    @Test
    public void testColumnarEquivalence()
    {
        // every seventh array is null, and the arrays have up to three elements, some of them null
        Block arrays = createArrayBigintBlock(IntStream.range(0, POSITION_COUNT)
                .mapToObj(position -> position % 7 == 3 ? null : elements(position, position % 4))
                .toList());
        COLUMNAR.assertEquivalent(
                COLUMNAR.function("reverse", new Reference(ARRAY_TYPE, "input")),
                new Input("input", ARRAY_TYPE, arrays));
    }

    private static List<Long> elements(int value, int count)
    {
        return IntStream.range(0, count)
                .mapToObj(element -> (value + element) % 6 == 5 ? null : value * 10L + element)
                .toList();
    }
}
