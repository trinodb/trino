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
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;
import java.time.LocalTime;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.when;

class LongBlockAppenderTest
        extends BlockAppenderTest
{
    @Override
    @BeforeEach
    public void beforeEach()
    {
        super.beforeEach();
        blockAppender = new LongBlockAppender(writeJuffersWarmUpElement);
    }

    static Stream<Arguments> params()
    {
        TimeType timeType = TimeType.createTimeType(5);
        List<LocalTime> timeValuesList = List.of(
                LocalTime.of(12, 34, 56, 123_456_000),
                LocalTime.of(23, 59, 59, 999_999_000),
                LocalTime.of(6, 30, 45, 987_654_321),
                LocalTime.of(18, 15, 30, 500_000_000));
        long expectedTimeMax = timeValuesList.stream().mapToLong(LocalTime::toNanoOfDay).max().getAsLong();
        long expectedTimeMin = timeValuesList.stream().mapToLong(LocalTime::toNanoOfDay).min().getAsLong();
        BlockBuilder timeBlockBuilder = createTimeBlockBuilder(timeType, timeValuesList);
        Block timeBlockWithoutNull = timeBlockBuilder.build();
        timeBlockBuilder.appendNull();
        Block timeBlockWithNull = timeBlockBuilder.build();

        TimestampType timestampType = TimestampType.createTimestampType(5);
        BlockBuilder timestampBlockBuilder = createTimeBlockBuilder(timestampType, timeValuesList);
        Block timestampBlockWithoutNull = timestampBlockBuilder.build();
        timestampBlockBuilder.appendNull();
        Block timestampBlockWithNull = timestampBlockBuilder.build();

        DecimalType shortDecimalType = DecimalType.createDecimalType(9, 2);
        assertThat(TypeUtils.isShortDecimalType(shortDecimalType)).isTrue();
        List<BigDecimal> shortDecimalValues = List.of(new BigDecimal("123456789"),
                new BigDecimal("123.45"),
                new BigDecimal("67.89"),
                new BigDecimal("1.23"),
                new BigDecimal("-123456.78"));
        long expectedShortDecimalMax = shortDecimalValues.stream().mapToLong(BigDecimal::longValue).max().getAsLong();
        long expectedShortDecimalMin = shortDecimalValues.stream().mapToLong(BigDecimal::longValue).min().getAsLong();
        BlockBuilder shortDecimalBlockBuilder = createShortDecimalBlockBuilder(shortDecimalType, shortDecimalValues);
        Block shortDecimalWithoutNull = shortDecimalBlockBuilder.build();
        shortDecimalBlockBuilder.appendNull();
        Block shortDecimalWithNull = shortDecimalBlockBuilder.build();
        return Stream.of(
                arguments(
                        timestampBlockWithoutNull,
                        timeType,
                        new WarmupElementStats(0, expectedTimeMin, expectedTimeMax)),
                arguments(
                        timestampBlockWithNull,
                        timeType,
                        new WarmupElementStats(1, expectedTimeMin, expectedTimeMax)),
                arguments(
                        timeBlockWithoutNull,
                        timeType,
                        new WarmupElementStats(0, expectedTimeMin, expectedTimeMax)),
                arguments(
                        timeBlockWithNull,
                        timeType,
                        new WarmupElementStats(1, expectedTimeMin, expectedTimeMax)),
                arguments(
                        shortDecimalWithoutNull,
                        shortDecimalType,
                        new WarmupElementStats(0, expectedShortDecimalMin, expectedShortDecimalMax)),
                arguments(
                        shortDecimalWithNull,
                        shortDecimalType,
                        new WarmupElementStats(1, expectedShortDecimalMin, expectedShortDecimalMax)),
                arguments(
                        new LongArrayBlock(3, Optional.empty(), new long[] {1, 2, 3}),
                        BigintType.BIGINT,
                        new WarmupElementStats(0, 1L, 3L)),
                arguments(
                        new LongArrayBlock(4, Optional.of(new boolean[] {false, false, false, true}), new long[] {1, -50, 30, 33333}),
                        BigintType.BIGINT,
                        new WarmupElementStats(1, -50L, 30L)));
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
            when(writeJuffersWarmUpElement.getRecordBuffer()).thenReturn(ShortBuffer.allocate(100));
            runTest(block, blockType, expectedResult, getWriteDictionary(recTypeCode));
        }
    }

    /**
     *  same for TimeType and TimeStampType
     */
    private static BlockBuilder createTimeBlockBuilder(Type timeType, List<LocalTime> values)
    {
        BlockBuilder timeBlockBuilder = timeType.createBlockBuilder(null, 5);

        for (LocalTime val : values) {
            long epochDayVal = val.toNanoOfDay();
            timeType.writeLong(timeBlockBuilder, epochDayVal);
        }
        return timeBlockBuilder;
    }

    private static BlockBuilder createShortDecimalBlockBuilder(Type shortDecimalType, List<BigDecimal> values)
    {
        BlockBuilder shortDecimalBlockBuilder = shortDecimalType.createBlockBuilder(null, 5);

        for (BigDecimal val : values) {
            shortDecimalType.writeLong(shortDecimalBlockBuilder, val.longValue());
        }
        return shortDecimalBlockBuilder;
    }
}
