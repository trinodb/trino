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

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.SqlDecimal;

import java.math.BigDecimal;

import static java.math.BigDecimal.ROUND_DOWN;

public abstract class AbstractTestDecimalSumAggregation
        extends AbstractTestAggregationFunction
{
    protected abstract DecimalType getDecimalType();

    protected abstract DecimalType getExpectedType();

    @Override
    protected Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder blockBuilder = getDecimalType().createBlockBuilder(null, length);
        for (int i = start; i < start + length; i++) {
            writeDecimalToBlock(getBigDecimalForCounter(i), blockBuilder);
        }
        return new Block[] {blockBuilder.build()};
    }

    protected abstract void writeDecimalToBlock(BigDecimal decimal, BlockBuilder blockBuilder);

    private static BigDecimal getBigDecimalForCounter(int i)
    {
        String iAsString = String.valueOf(Math.abs(i));
        return new BigDecimal(String.valueOf(i) + "." + iAsString + iAsString).setScale(2, ROUND_DOWN);
    }

    @Override
    protected SqlDecimal getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }
        BigDecimal sum = BigDecimal.ZERO;
        for (int i = start; i < start + length; i++) {
            sum = sum.add(getBigDecimalForCounter(i));
        }

        DecimalType expectedType = getExpectedType();
        return new SqlDecimal(sum.unscaledValue(), expectedType.getPrecision(), expectedType.getScale());
    }

    @Override
    protected String getFunctionName()
    {
        return "sum";
    }
}
