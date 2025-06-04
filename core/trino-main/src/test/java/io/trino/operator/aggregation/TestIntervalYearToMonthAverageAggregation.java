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
import io.trino.type.SqlIntervalYearMonth;

import java.util.List;

import static io.trino.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static java.lang.Math.round;
import static java.lang.Math.toIntExact;

public class TestIntervalYearToMonthAverageAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    protected Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder blockBuilder = INTERVAL_YEAR_MONTH.createFixedSizeBlockBuilder(length);
        for (int i = start; i < start + length; i++) {
            INTERVAL_YEAR_MONTH.writeLong(blockBuilder, i);
        }
        return new Block[] {blockBuilder.build()};
    }

    @Override
    protected SqlIntervalYearMonth getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }

        double sum = 0;
        for (int i = start; i < start + length; i++) {
            sum += i;
        }
        return new SqlIntervalYearMonth(toIntExact(round(sum / length)));
    }

    @Override
    protected String getFunctionName()
    {
        return "avg";
    }

    @Override
    protected List<Type> getFunctionParameterTypes()
    {
        return ImmutableList.of(INTERVAL_YEAR_MONTH);
    }
}
