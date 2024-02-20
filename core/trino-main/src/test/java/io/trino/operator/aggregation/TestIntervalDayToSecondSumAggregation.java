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
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.type.SqlIntervalDayTime;

import java.util.List;

import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;

public class TestIntervalDayToSecondSumAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    protected Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder blockBuilder = INTERVAL_DAY_TIME.createBlockBuilder(null, length);
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
}
