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

import com.google.common.collect.ImmutableList;
import io.trino.operator.aggregation.state.LongDecimalWithOverflowAndLongState;
import io.trino.operator.aggregation.state.LongDecimalWithOverflowAndLongStateFactory;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.type.IntervalDayTimeType;
import io.trino.type.LongInterval;
import io.trino.type.SqlIntervalDayTime;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.type.Decimals.longTenToNth;
import static io.trino.spi.type.IntervalField.DAY;
import static io.trino.spi.type.IntervalField.SECOND;
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.trino.type.IntervalDayTimeType.createIntervalDayTimeType;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.round;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIntervalDayToSecondAverageAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    protected Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder blockBuilder = INTERVAL_DAY_TIME.createFixedSizeBlockBuilder(length);
        for (int i = start; i < start + length; i++) {
            INTERVAL_DAY_TIME.writeLong(blockBuilder, i * 250L);
        }
        return new Block[] {blockBuilder.build()};
    }

    @Override
    protected SqlIntervalDayTime getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }

        double sum = 0;
        for (int i = start; i < start + length; i++) {
            sum += i * 250;
        }
        return new SqlIntervalDayTime(round(sum / length));
    }

    @Override
    protected String getFunctionName()
    {
        return "avg";
    }

    @Override
    protected List<Type> getFunctionParameterTypes()
    {
        return ImmutableList.of(INTERVAL_DAY_TIME);
    }

    @Test
    public void testRoundingToOutputPrecision()
    {
        for (int precision = 0; precision <= 12; precision++) {
            long quantum = longTenToNth(12 - precision);
            for (long base : new long[] {-1001, -1, 0, 1, 1001}) {
                for (int sign : new int[] {-1, 1}) {
                    // Values on either side of the midpoint, and exactly at the midpoint.
                    assertAverage(precision, base * quantum, sign * quantum, 5, 11, base * quantum);
                    assertAverage(precision, base * quantum, sign * quantum, 6, 11, (base + sign) * quantum);
                    assertAverage(precision, base * quantum, sign * quantum, 1, 2, (base + (sign > 0 ? 1 : 0)) * quantum);
                }
            }
        }
    }

    private static void assertAverage(int precision, long basePicos, long incrementPicos, int incrementCount, int count, long expectedPicos)
    {
        IntervalDayTimeType type = createIntervalDayTimeType(DAY, SECOND, 9, precision);
        LongDecimalWithOverflowAndLongState direct = new LongDecimalWithOverflowAndLongStateFactory().createSingleState();
        LongDecimalWithOverflowAndLongState combined = new LongDecimalWithOverflowAndLongStateFactory().createSingleState();
        for (int index = 0; index < count; index++) {
            long picos = basePicos + (index < incrementCount ? incrementPicos : 0);
            LongDecimalWithOverflowAndLongState partial = new LongDecimalWithOverflowAndLongStateFactory().createSingleState();
            if (type.isShort()) {
                IntervalDayToSecondAverageAggregation.averageShort(direct, picos / 1_000_000);
                IntervalDayToSecondAverageAggregation.averageShort(partial, picos / 1_000_000);
            }
            else {
                LongInterval value = new LongInterval(floorDiv(picos, 1_000_000), floorMod(picos, 1_000_000));
                IntervalDayToSecondAverageAggregation.averageLong(direct, value);
                IntervalDayToSecondAverageAggregation.averageLong(partial, value);
            }
            IntervalDayToSecondAverageAggregation.combine(combined, partial);
        }
        for (LongDecimalWithOverflowAndLongState state : new LongDecimalWithOverflowAndLongState[] {direct, combined}) {
            BlockBuilder builder = type.createBlockBuilder(null, 1);
            IntervalDayToSecondAverageAggregation.output(precision, state, builder);
            assertThat(type.getObjectValue(builder.build(), 0))
                    .as("precision %s, base %s, increment %s, count %s/%s", precision, basePicos, incrementPicos, incrementCount, count)
                    .isEqualTo(new SqlIntervalDayTime(floorDiv(expectedPicos, 1_000_000), floorMod(expectedPicos, 1_000_000), precision));
        }
    }
}
