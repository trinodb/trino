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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import io.trino.operator.aggregation.AbstractTestAggregationFunction;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.sql.tree.QualifiedName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.block.BlockAssertions.createBlockOfReals;
import static io.trino.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static java.lang.Float.floatToRawIntBits;

public class TestRealAverageAggregation
        extends AbstractTestAggregationFunction
{
    @Test
    public void averageOfNullIsNull()
    {
        assertAggregation(
                functionResolution,
                QualifiedName.of("avg"),
                fromTypes(REAL),
                null,
                createBlockOfReals(null, null));
    }

    @Test
    public void averageOfSingleValueEqualsThatValue()
    {
        assertAggregation(
                functionResolution,
                QualifiedName.of("avg"),
                fromTypes(REAL),
                1.23f,
                createBlockOfReals(1.23f));
    }

    @Test
    public void averageOfTwoMaxFloatsEqualsMaxFloat()
    {
        assertAggregation(
                functionResolution,
                QualifiedName.of("avg"),
                fromTypes(REAL),
                Float.MAX_VALUE,
                createBlockOfReals(Float.MAX_VALUE, Float.MAX_VALUE));
    }

    @Override
    protected Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder blockBuilder = REAL.createBlockBuilder(null, length);
        for (int i = start; i < start + length; i++) {
            REAL.writeLong(blockBuilder, floatToRawIntBits((float) i));
        }
        return new Block[] {blockBuilder.build()};
    }

    @Override
    protected String getFunctionName()
    {
        return "avg";
    }

    @Override
    protected List<Type> getFunctionParameterTypes()
    {
        return ImmutableList.of(REAL);
    }

    @Override
    protected Object getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }

        float sum = 0;
        for (int i = start; i < start + length; i++) {
            sum += i;
        }
        return sum / length;
    }
}
