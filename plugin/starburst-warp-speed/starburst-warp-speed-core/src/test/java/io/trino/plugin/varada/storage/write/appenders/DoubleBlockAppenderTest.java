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
package io.trino.plugin.varada.storage.write.appenders;

import io.trino.plugin.varada.storage.write.WarmupElementStats;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.LongBuffer;
import java.nio.ShortBuffer;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.when;

class DoubleBlockAppenderTest
        extends BlockAppenderTest
{
    @Override
    @BeforeEach
    public void beforeEach()
    {
        super.beforeEach();
        blockAppender = new DoubleBlockAppender(writeJuffersWarmUpElement);
    }

    static Stream<Arguments> params()
    {
        DoubleType doubleType = DoubleType.DOUBLE;
        BlockBuilder blockBuilder = doubleType.createBlockBuilder(null, 5);
        List<Double> values = List.of(-1.5, 2.7, 3.9, 4.2, 5.1);
        for (double val : values) {
            doubleType.writeDouble(blockBuilder, val);
        }
        double expectedMaxValue = values.stream()
                .max(Double::compare)
                .orElse(null);
        double expectedMinValue = values.stream()
                .min(Double::compare)
                .orElse(null);
        Block blockWithoutNull = blockBuilder.build();
        blockBuilder.appendNull();
        Block blockWithNull = blockBuilder.build();
        return Stream.of(
                arguments(blockWithoutNull, DoubleType.DOUBLE, new WarmupElementStats(0, expectedMinValue, expectedMaxValue)),
                arguments(blockWithNull, DoubleType.DOUBLE, new WarmupElementStats(1, expectedMinValue, expectedMaxValue)));
    }

    @Override
    @ParameterizedTest
    @MethodSource("params")
    public void writeWithoutDictionary(Block block, Type blockType, WarmupElementStats expectedResult)
    {
        when(writeJuffersWarmUpElement.getRecordBuffer()).thenReturn(LongBuffer.allocate(100));
        runTest(block, blockType, expectedResult, Optional.empty());
    }

    @Override
    @ParameterizedTest
    @MethodSource("params")
    public void writeWithDictionary(Block block, Type blockType, WarmupElementStats expectedResult)
    {
        RecTypeCode recTypeCode = getRecTypeCode(blockType);
        if (recTypeCode.isSupportedDictionary()) {
            assertThrows(UnsupportedOperationException.class, () -> {
                when(writeJuffersWarmUpElement.getRecordBuffer()).thenReturn(ShortBuffer.allocate(100));
                runTest(block, blockType, expectedResult, getWriteDictionary(recTypeCode));
            });
        }
    }
}
