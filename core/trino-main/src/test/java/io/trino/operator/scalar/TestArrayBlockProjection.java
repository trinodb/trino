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
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.type.ArrayType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

public class TestArrayBlockProjection
{
    private static final ArrayType ARRAY_TYPE = new ArrayType(BIGINT);

    @Test
    public void testTrim()
    {
        ArrayBlock input = arrays(ImmutableList.of(1L, 2L, 3L), null, ImmutableList.of(), ImmutableList.of(4L, 5L));
        Block result = ArrayTrimFunction.trimColumnar(input, createLongsBlock(1L, 0L, null, 2L));

        assertArrays(result, ImmutableList.of(1L, 2L), null, null, ImmutableList.of());
        assertThat(((ArrayBlock) result).getElementsBlock()).isInstanceOf(DictionaryBlock.class);

        assertInvalidArgument(
                () -> ArrayTrimFunction.trimColumnar(input, createLongsBlock(-1L, 0L, 0L, 0L)),
                "size must not be negative: -1");
        assertInvalidArgument(
                () -> ArrayTrimFunction.trimColumnar(input, createLongsBlock(4L, 0L, 0L, 0L)),
                "size must not exceed array cardinality 3: 4");
    }

    @Test
    public void testSlice()
    {
        ArrayBlock input = arrays(ImmutableList.of(1L, 2L, 3L), null, ImmutableList.of(), ImmutableList.of(4L, 5L));
        Block result = ArraySliceFunction.sliceColumnar(
                input,
                createLongsBlock(2L, 1L, 1L, -2L),
                createLongsBlock(5L, 1L, 1L, 1L));

        assertArrays(result, ImmutableList.of(2L, 3L), null, ImmutableList.of(), ImmutableList.of(4L));
        assertThat(((ArrayBlock) result).getElementsBlock()).isInstanceOf(DictionaryBlock.class);

        assertInvalidArgument(
                () -> ArraySliceFunction.sliceColumnar(input, createLongsBlock(0L, 1L, 1L, 1L), createLongsBlock(1L, 1L, 1L, 1L)),
                "SQL array indices start at 1");
        assertInvalidArgument(
                () -> ArraySliceFunction.sliceColumnar(input, createLongsBlock(1L, 1L, 1L, 1L), createLongsBlock(-1L, 1L, 1L, 1L)),
                "length must be greater than or equal to 0");
    }

    private static void assertInvalidArgument(Runnable runnable, String message)
    {
        assertTrinoExceptionThrownBy(runnable::run)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage(message);
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
}
