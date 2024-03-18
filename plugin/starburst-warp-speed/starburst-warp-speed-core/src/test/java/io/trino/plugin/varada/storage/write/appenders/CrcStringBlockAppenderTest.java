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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.varada.storage.engine.StubsStorageEngineConstants;
import io.trino.plugin.varada.storage.write.WarmupElementStats;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.nio.ShortBuffer;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.when;

class CrcStringBlockAppenderTest
        extends BlockAppenderTest
{
    private static final VarcharType VARCHAR_TYPE = VarcharType.createVarcharType(5);

    @Override
    @BeforeEach
    public void beforeEach()
    {
        super.beforeEach();
        blockAppender = new CrcStringBlockAppender(writeJuffersWarmUpElement,
                new StubsStorageEngineConstants(),
                bufferAllocator,
                VARCHAR_TYPE,
                false);
    }

    static Stream<Arguments> params()
    {
        List<Slice> values = List.of(Slices.utf8Slice("aaaaa"),
                Slices.utf8Slice("bbbbb"),
                Slices.utf8Slice("ccccc"),
                Slices.utf8Slice("ddddd"));
        Slice expectedMaxValue = values.stream()
                .max(Slice::compareTo)
                .orElse(null);
        Slice expectedMinValue = values.stream()
                .min(Slice::compareTo)
                .orElse(null);
        BlockBuilder blockBuilder = buildVarcharBlockBuilder(values);
        Block blockWithoutNull = blockBuilder.build();
        blockBuilder.appendNull();
        Block blockWithNull = blockBuilder.build();
        return Stream.of(
                arguments(blockWithoutNull, VARCHAR_TYPE, new WarmupElementStats(0, expectedMinValue, expectedMaxValue)),
                arguments(blockWithNull, VARCHAR_TYPE, new WarmupElementStats(1, expectedMinValue, expectedMaxValue)));
    }

    @Override
    @ParameterizedTest
    @MethodSource("params")
    public void writeWithoutDictionary(Block block, Type blockType, WarmupElementStats expectedResult)
    {
        when(writeJuffersWarmUpElement.getRecordBuffer()).thenReturn(ByteBuffer.allocate(100));
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

    public static VariableWidthBlockBuilder buildVarcharBlockBuilder(List<Slice> values)
    {
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, values.size() + 1, values.size() + 1);
        for (Slice value : values) {
            VARCHAR_TYPE.writeSlice(blockBuilder, value);
        }
        return blockBuilder;
    }
}
