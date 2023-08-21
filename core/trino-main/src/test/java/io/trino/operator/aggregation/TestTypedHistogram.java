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
package io.trino.operator.aggregation;

import io.trino.operator.aggregation.histogram.TypedHistogram;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import java.util.function.IntUnaryOperator;
import java.util.function.ObjIntConsumer;
import java.util.stream.IntStream;

import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FLAT;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.BLOCK_BUILDER;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FLAT_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.util.StructuralTestUtil.mapType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestTypedHistogram
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    @Test
    public void testMassive()
    {
        testMassive(false, BIGINT, BIGINT::writeLong);
        testMassive(false, VARCHAR, (blockBuilder, value) -> VARCHAR.writeString(blockBuilder, String.valueOf(value)));
        testMassive(true, BIGINT, BIGINT::writeLong);
        testMassive(true, VARCHAR, (blockBuilder, value) -> VARCHAR.writeString(blockBuilder, String.valueOf(value)));
    }

    private static void testMassive(boolean grouped, Type type, ObjIntConsumer<BlockBuilder> writeData)
    {
        BlockBuilder inputBlockBuilder = type.createBlockBuilder(null, 5000);
        IntStream.range(1, 2000)
                .flatMap(value -> IntStream.iterate(value, IntUnaryOperator.identity()).limit(value))
                .forEach(value -> writeData.accept(inputBlockBuilder, value));
        Block inputBlock = inputBlockBuilder.build();

        TypedHistogram typedHistogram = new TypedHistogram(
                type,
                TYPE_OPERATORS.getReadValueOperator(type, simpleConvention(BLOCK_BUILDER, FLAT)),
                TYPE_OPERATORS.getReadValueOperator(type, simpleConvention(FLAT_RETURN, BLOCK_POSITION_NOT_NULL)),
                TYPE_OPERATORS.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, FLAT)),
                TYPE_OPERATORS.getDistinctFromOperator(type, simpleConvention(FAIL_ON_NULL, FLAT, BLOCK_POSITION_NOT_NULL)),
                TYPE_OPERATORS.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL)),
                grouped);

        int groupId = 0;
        if (grouped) {
            groupId = 10;
            typedHistogram.setMaxGroupId(groupId);
        }
        for (int i = 0; i < inputBlock.getPositionCount(); i++) {
            typedHistogram.add(groupId, inputBlock, i, 1);
        }

        MapType mapType = mapType(type, BIGINT);
        MapBlockBuilder actualBuilder = mapType.createBlockBuilder(null, 1);
        typedHistogram.serialize(groupId, actualBuilder);
        Block actualBlock = actualBuilder.build();

        MapBlockBuilder expectedBuilder = mapType.createBlockBuilder(null, 1);
        expectedBuilder.buildEntry((keyBuilder, valueBuilder) -> IntStream.range(1, 2000)
                .forEach(value -> {
                    writeData.accept(keyBuilder, value);
                    BIGINT.writeLong(valueBuilder, value);
                }));
        Block expectedBlock = expectedBuilder.build();
        assertBlockEquals(mapType, actualBlock, expectedBlock);
        assertEquals(typedHistogram.size(), 1999);

        if (grouped) {
            actualBuilder = mapType.createBlockBuilder(null, 1);
            typedHistogram.serialize(3, actualBuilder);
            actualBlock = actualBuilder.build();
            assertThat(actualBlock.getPositionCount()).isEqualTo(1);
            assertThat(actualBlock.isNull(0)).isTrue();
        }
    }
}
