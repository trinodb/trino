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
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.type.DateType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.IntBuffer;
import java.nio.ShortBuffer;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.when;

class IntBlockAppenderTest
        extends BlockAppenderTest
{
    @Override
    @BeforeEach
    public void beforeEach()
    {
        super.beforeEach();
        blockAppender = new IntBlockAppender(writeJuffersWarmUpElement);
    }

    static Stream<Arguments> params()
    {
        List<LocalDate> dateValues = List.of(LocalDate.of(2023, 7, 19),
                LocalDate.of(2023, 7, 20),
                LocalDate.of(2020, 7, 20));
        int expectedMax = dateValues.stream().mapToInt(x -> (int) x.toEpochDay()).max().getAsInt();
        int expectedMin = dateValues.stream().mapToInt(x -> (int) x.toEpochDay()).min().getAsInt();
        BlockBuilder dateBlockBuilder = createDateBlockBuilder(DateType.DATE, dateValues);
        Block dateBlockWithoutNull = dateBlockBuilder.build();
        dateBlockBuilder.appendNull();
        Block dateBlockWithNull = dateBlockBuilder.build();
        return Stream.of(
                arguments(
                        dateBlockWithoutNull,
                        DateType.DATE,
                        new WarmupElementStats(0, expectedMin, expectedMax)),
                arguments(
                        dateBlockWithNull,
                        DateType.DATE,
                        new WarmupElementStats(1, expectedMin, expectedMax)),
                arguments(
                        new IntArrayBlock(3, Optional.empty(), new int[] {1, 2, 3}),
                        IntegerType.INTEGER,
                        new WarmupElementStats(0, 1, 3)),
                arguments(
                        new IntArrayBlock(4, Optional.of(new boolean[] {false, false, false, true}), new int[] {1, -50, 30, 33333}),
                        IntegerType.INTEGER,
                        new WarmupElementStats(1, -50, 30)));
    }

    private static BlockBuilder createDateBlockBuilder(Type dateType, List<LocalDate> values)
    {
        BlockBuilder blockBuilder = dateType.createBlockBuilder(null, 5);
        for (LocalDate val : values) {
            long epochDayVal = val.toEpochDay();
            dateType.writeLong(blockBuilder, epochDayVal);
        }
        return blockBuilder;
    }

    @Override
    @ParameterizedTest
    @MethodSource("params")
    public void writeWithoutDictionary(Block block, Type blockType, WarmupElementStats expectedResult)
    {
        when(writeJuffersWarmUpElement.getRecordBuffer()).thenReturn(IntBuffer.allocate(100));
        runTest(block, blockType, expectedResult, Optional.empty());
    }

    @Override
    @ParameterizedTest
    @MethodSource("params")
    public void writeWithDictionary(Block block, Type blockType, WarmupElementStats expectedResult)
    {
        RecTypeCode recTypeCode = getRecTypeCode(blockType);
        if (recTypeCode.isSupportedDictionary()) {
            when(writeJuffersWarmUpElement.getRecordBuffer()).thenReturn(ShortBuffer.allocate(100));
            runTest(block, blockType, expectedResult, getWriteDictionary(recTypeCode));
        }
    }
}
