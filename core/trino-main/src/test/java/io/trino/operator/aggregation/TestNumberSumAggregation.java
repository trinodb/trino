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
import io.trino.spi.type.SqlNumber;
import io.trino.spi.type.TrinoNumber;
import io.trino.spi.type.Type;

import java.math.BigDecimal;
import java.util.List;

import static io.trino.spi.type.NumberType.NUMBER;

public class TestNumberSumAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    protected Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder blockBuilder = NUMBER.createBlockBuilder(null, length);
        for (int i = start; i < start + length; i++) {
            NUMBER.writeObject(blockBuilder, TrinoNumber.from(BigDecimal.valueOf(i)));
        }
        return new Block[] {blockBuilder.build()};
    }

    @Override
    protected Object getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }

        BigDecimal sum = BigDecimal.ZERO;
        for (int i = start; i < start + length; i++) {
            sum = sum.add(BigDecimal.valueOf(i));
        }
        return new SqlNumber(sum.stripTrailingZeros());
    }

    @Override
    protected String getFunctionName()
    {
        return "sum";
    }

    @Override
    protected List<Type> getFunctionParameterTypes()
    {
        return ImmutableList.of(NUMBER);
    }
}
