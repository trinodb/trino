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
package io.trino.operator.aggregation.arrayagg;

import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.LongArrayBlockBuilder;
import io.trino.spi.block.ValueBlock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;
import java.util.Optional;
import java.util.stream.IntStream;

import static io.trino.operator.PagesIndex.TestingFactory.TYPE_OPERATORS;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FLAT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.VALUE_BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.BLOCK_BUILDER;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FLAT_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.BigintType.BIGINT;

public class TestFlatArrayBuilder
{
    private final MethodHandle valueReadFlat = TYPE_OPERATORS.getReadValueOperator(BIGINT, simpleConvention(BLOCK_BUILDER, FLAT));
    private final MethodHandle valueWriteFlat = TYPE_OPERATORS.getReadValueOperator(BIGINT, simpleConvention(FLAT_RETURN, VALUE_BLOCK_POSITION_NOT_NULL));

    @Test
    public void testWriteAll()
    {
        int size = 1024;
        FlatArrayBuilder flatArrayBuilder = new FlatArrayBuilder(BIGINT, valueReadFlat, valueWriteFlat, false);

        ValueBlock valueBlock = new LongArrayBlock(size, Optional.empty(),
                IntStream.range(0, size).mapToLong(i -> i).toArray());

        for (int i = 0; i < size; i++) {
            flatArrayBuilder.add(valueBlock, i);
        }

        LongArrayBlockBuilder blockBuilder = new LongArrayBlockBuilder(null, size);
        flatArrayBuilder.writeAll(blockBuilder);

        Block block = blockBuilder.build();
        Assertions.assertEquals(size, block.getPositionCount());
        for (int i = 0; i < size; i++) {
            Assertions.assertEquals(i, BIGINT.getLong(block, i));
        }
    }

    @Test
    public void testWrite()
    {
        int size = 1024;
        FlatArrayBuilder flatArrayBuilder = new FlatArrayBuilder(BIGINT, valueReadFlat, valueWriteFlat, true);

        ValueBlock valueBlock = new LongArrayBlock(size, Optional.empty(),
                IntStream.range(0, size).mapToLong(i -> i).toArray());

        for (int i = 0; i < size; i++) {
            flatArrayBuilder.add(valueBlock, i);
            if (i != size - 1) {
                flatArrayBuilder.setNextIndex(i, i + 1);
            }
        }

        LongArrayBlockBuilder blockBuilder = new LongArrayBlockBuilder(null, size);
        long nextIndex = 0;
        while (nextIndex != -1) {
            nextIndex = flatArrayBuilder.write(nextIndex, blockBuilder);
        }

        Block block = blockBuilder.build();
        Assertions.assertEquals(size, block.getPositionCount());
        for (int i = 0; i < size; i++) {
            Assertions.assertEquals(i, BIGINT.getLong(block, i));
        }
    }
}
