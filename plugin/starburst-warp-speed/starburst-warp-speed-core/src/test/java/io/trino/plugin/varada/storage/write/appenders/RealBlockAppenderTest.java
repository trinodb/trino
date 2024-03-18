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

import io.trino.plugin.varada.juffer.BlockPosHolder;
import io.trino.plugin.varada.storage.write.WarmupElementStats;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.IntBuffer;
import java.nio.ShortBuffer;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.when;

class RealBlockAppenderTest
        extends BlockAppenderTest
{
    private static final RealType realType = RealType.REAL;

    @Override
    @BeforeEach
    public void beforeEach()
    {
        super.beforeEach();
        blockAppender = new RealBlockAppender(writeJuffersWarmUpElement);
    }

    static Stream<Arguments> params()
    {
        BlockBuilder blockBuilder = realType.createBlockBuilder(null, 5);
        List<Float> values = List.of(1.1f, 10.3e0f, -10.3e0f, 5.5f);
        for (Float val : values) {
            realType.writeLong(blockBuilder, Float.floatToIntBits(val));
        }
        float expectedMaxValue = values.stream().max(Float::compareTo).orElse(null);
        float expectedMinValue = values.stream().min(Float::compareTo).orElse(null);
        Block blockWithoutNull = blockBuilder.build();
        blockBuilder.appendNull();
        Block blockWithNull = blockBuilder.build();
        return Stream.of(
                arguments(blockWithoutNull, realType, new WarmupElementStats(0, expectedMinValue, expectedMaxValue)),
                arguments(blockWithNull, realType, new WarmupElementStats(1, expectedMinValue, expectedMaxValue)));
    }

    @Override
    @ParameterizedTest
    @MethodSource("params")
    public void writeWithoutDictionary(Block block, Type blockType, WarmupElementStats expectedResult)
    {
        when(writeJuffersWarmUpElement.getRecordBuffer()).thenReturn(IntBuffer.allocate(100));
        runTest(block, blockType, expectedResult, Optional.empty());
    }

    @Disabled
    @Override
    @ParameterizedTest
    @MethodSource("params")
    public void writeWithDictionary(Block block, Type blockType, WarmupElementStats expectedResult)
    {
        RecTypeCode recTypeCode = getRecTypeCode(blockType);
        if (recTypeCode.isSupportedDictionary()) {
            assertThrows(UnsupportedOperationException.class, () -> {
                when(writeJuffersWarmUpElement.getRecordBuffer()).thenReturn(ShortBuffer.allocate(100));
                BlockPosHolder blockPosHolder = new BlockPosHolder(block, blockType, 0, block.getPositionCount());
                WarmupElementStats warmupElementStats = new WarmupElementStats(0, null, null);
                blockAppender.appendWithDictionary(blockPosHolder, false, getWriteDictionary(recTypeCode).get(), warmupElementStats);
            });
        }
    }
}
