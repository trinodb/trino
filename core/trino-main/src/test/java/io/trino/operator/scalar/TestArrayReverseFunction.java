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

import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestArrayReverseFunction
{
    private static final ArrayType ARRAY_TYPE = new ArrayType(BIGINT);

    @Test
    public void testColumnarProjection()
    {
        ArrayBlock input = arrays(ImmutableList.of(1L, 2L, 3L), null, ImmutableList.of(), ImmutableList.of(4L, 5L));
        Block result = ArrayReverseFunction.reverseColumnar(input);

        assertArrays(result, ImmutableList.of(3L, 2L, 1L), null, ImmutableList.of(), ImmutableList.of(5L, 4L));
        assertThat(result).isInstanceOf(ArrayBlock.class);
        ArrayBlock resultArray = (ArrayBlock) result;
        assertThat(resultArray.getElementsBlock()).isInstanceOf(DictionaryBlock.class);
        assertThat(resultArray.getRawOffsets()).isSameAs(input.getRawOffsets());
        assertThat(resultArray.getRawValueIsValid()).isSameAs(input.getRawValueIsValid());
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
