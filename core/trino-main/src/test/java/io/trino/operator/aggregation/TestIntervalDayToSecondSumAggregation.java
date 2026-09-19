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
import io.trino.operator.aggregation.state.LongIntervalState;
import io.trino.operator.aggregation.state.StateCompiler;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.type.LongInterval;
import io.trino.type.SqlIntervalDayTime;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIntervalDayToSecondSumAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    protected Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder blockBuilder = INTERVAL_DAY_TIME.createFixedSizeBlockBuilder(length);
        for (int i = start; i < start + length; i++) {
            INTERVAL_DAY_TIME.writeLong(blockBuilder, i * 1000L);
        }
        return new Block[] {blockBuilder.build()};
    }

    @Override
    protected SqlIntervalDayTime getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }

        long sum = 0;
        for (int i = start; i < start + length; i++) {
            sum += i * 1000L;
        }
        return new SqlIntervalDayTime(sum);
    }

    @Override
    protected String getFunctionName()
    {
        return "sum";
    }

    @Override
    protected List<Type> getFunctionParameterTypes()
    {
        return ImmutableList.of(INTERVAL_DAY_TIME);
    }

    @Test
    public void testFractionalCarryAtStorageBoundary()
    {
        LongInterval large = new LongInterval(Long.MIN_VALUE, 500_000);
        LongInterval small = new LongInterval(-1, 500_000);
        for (boolean reverse : new boolean[] {false, true}) {
            LongInterval first = reverse ? small : large;
            LongInterval second = reverse ? large : small;
            LongIntervalState inputState = StateCompiler.generateStateFactory(LongIntervalState.class).createSingleState();
            IntervalDayToSecondSumAggregation.sumLong(inputState, first);
            IntervalDayToSecondSumAggregation.sumLong(inputState, second);
            assertThat(inputState.getMicros()).isEqualTo(Long.MIN_VALUE);
            assertThat(inputState.getPicosOfMicro()).isZero();

            LongIntervalState firstPartial = StateCompiler.generateStateFactory(LongIntervalState.class).createSingleState();
            LongIntervalState secondPartial = StateCompiler.generateStateFactory(LongIntervalState.class).createSingleState();
            IntervalDayToSecondSumAggregation.sumLong(firstPartial, first);
            IntervalDayToSecondSumAggregation.sumLong(secondPartial, second);
            IntervalDayToSecondSumAggregation.combine(firstPartial, secondPartial);
            assertThat(firstPartial.getMicros()).isEqualTo(Long.MIN_VALUE);
            assertThat(firstPartial.getPicosOfMicro()).isZero();
        }
    }

    @Test
    public void testFractionalSumOverflow()
    {
        for (LongInterval value : new LongInterval[] {new LongInterval(Long.MAX_VALUE, 500_000), new LongInterval(Long.MIN_VALUE, 0)}) {
            LongInterval increment = value.getMicros() > 0 ? new LongInterval(0, 500_000) : new LongInterval(-1, 999_999);
            LongIntervalState inputState = StateCompiler.generateStateFactory(LongIntervalState.class).createSingleState();
            IntervalDayToSecondSumAggregation.sumLong(inputState, value);
            assertTrinoExceptionThrownBy(() -> IntervalDayToSecondSumAggregation.sumLong(inputState, increment))
                    .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

            LongIntervalState firstPartial = StateCompiler.generateStateFactory(LongIntervalState.class).createSingleState();
            LongIntervalState secondPartial = StateCompiler.generateStateFactory(LongIntervalState.class).createSingleState();
            IntervalDayToSecondSumAggregation.sumLong(firstPartial, value);
            IntervalDayToSecondSumAggregation.sumLong(secondPartial, increment);
            assertTrinoExceptionThrownBy(() -> IntervalDayToSecondSumAggregation.combine(firstPartial, secondPartial))
                    .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
        }
    }
}
