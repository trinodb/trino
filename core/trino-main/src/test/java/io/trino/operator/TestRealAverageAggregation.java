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
import io.trino.block.BlockAssertions;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.aggregation.AbstractTestAggregationFunction;
import io.trino.operator.window.PagesWindowIndex;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationImplementation;
import io.trino.spi.function.WindowAccumulator;
import io.trino.spi.function.WindowIndex;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.block.BlockAssertions.createBlockOfReals;
import static io.trino.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.trino.operator.aggregation.AggregationTestUtils.makeValidityAssertion;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static java.lang.Float.floatToRawIntBits;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRealAverageAggregation
        extends AbstractTestAggregationFunction
{
    @Test
    public void averageOfNullIsNull()
    {
        assertAggregation(
                functionResolution,
                "avg",
                fromTypes(REAL),
                null,
                createBlockOfReals(null, null));
    }

    @Test
    public void averageOfSingleValueEqualsThatValue()
    {
        assertAggregation(
                functionResolution,
                "avg",
                fromTypes(REAL),
                1.23f,
                createBlockOfReals(1.23f));
    }

    @Test
    public void averageOfTwoMaxFloatsEqualsMaxFloat()
    {
        assertAggregation(
                functionResolution,
                "avg",
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

    protected Block[] getSequenceBlocksForRealNaNTest(int start, int length)
    {
        BlockBuilder blockBuilder = REAL.createBlockBuilder(null, length);
        for (int i = start; i < start + length - 5; i++) {
            REAL.writeLong(blockBuilder, floatToRawIntBits((float) i));
        }
        REAL.writeLong(blockBuilder, floatToRawIntBits(Float.NaN));
        for (int i = start + length - 4; i < start + length; i++) {
            REAL.writeLong(blockBuilder, floatToRawIntBits((float) i));
        }
        return new Block[] {blockBuilder.build()};
    }

    protected Block[] getSequenceBlocksForRealInfinityTest(int start, int length)
    {
        BlockBuilder blockBuilder = REAL.createBlockBuilder(null, length);
        for (int i = start; i < start + length - 5; i++) {
            REAL.writeLong(blockBuilder, floatToRawIntBits((float) i));
        }
        REAL.writeLong(blockBuilder, floatToRawIntBits(Float.POSITIVE_INFINITY));
        for (int i = start + length - 4; i < start + length; i++) {
            REAL.writeLong(blockBuilder, floatToRawIntBits((float) i));
        }
        return new Block[] {blockBuilder.build()};
    }

    @Test
    public void testSlidingWindowForNaNAndInfinity()
    {
        int totalPositions = 12;
        int[] windowWidths = new int[totalPositions];
        Object[] expectedValues = new Object[totalPositions];
        Object[] expectedValues2 = new Object[totalPositions];

        for (int i = 0; i < totalPositions; ++i) {
            int windowWidth = Integer.min(i, totalPositions - 1 - i);
            windowWidths[i] = windowWidth;
            if (i >= 4) {
                expectedValues[i] = Float.NaN;
                expectedValues2[i] = Float.POSITIVE_INFINITY;
            }
            else {
                expectedValues[i] = getExpectedValue(i, windowWidth);
                expectedValues2[i] = getExpectedValue(i, windowWidth);
            }
        }
        Page inputPage = new Page(totalPositions, getSequenceBlocksForRealNaNTest(0, totalPositions));

        PagesIndex pagesIndex = new PagesIndex.TestingFactory(false).newPagesIndex(getFunctionParameterTypes(), totalPositions);
        pagesIndex.addPage(inputPage);
        WindowIndex windowIndex = new PagesWindowIndex(pagesIndex, 0, totalPositions - 1);

        ResolvedFunction resolvedFunction = functionResolution.resolveFunction(getFunctionName(), fromTypes(getFunctionParameterTypes()));
        AggregationImplementation aggregationImplementation = functionResolution.getPlannerContext().getFunctionManager().getAggregationImplementation(resolvedFunction);
        WindowAccumulator aggregation = createWindowAccumulator(resolvedFunction, aggregationImplementation);
        assertThat(resolvedFunction.signature().getReturnType().toString().contains("real")).isTrue();
        assertThat(resolvedFunction.signature().getName().toString().contains("avg")).isTrue();
        int oldStart = 0;
        int oldWidth = 0;
        for (int start = 0; start < totalPositions; ++start) {
            int width = windowWidths[start];
            for (int oldi = oldStart; oldi < oldStart + oldWidth; ++oldi) {
                if (oldi < start || oldi >= start + width) {
                    boolean res = aggregation.removeInput(windowIndex, oldi, oldi);
                    if (oldi >= 4) {
                        assertThat(res).isFalse();
                    }
                    else {
                        assertThat(res).isTrue();
                    }
                }
            }
            for (int newi = start; newi < start + width; ++newi) {
                if (newi < oldStart || newi >= oldStart + oldWidth) {
                    aggregation.addInput(windowIndex, newi, newi);
                }
            }
            oldStart = start;
            oldWidth = width;

            Type outputType = resolvedFunction.signature().getReturnType();
            BlockBuilder blockBuilder = outputType.createBlockBuilder(null, 1000);
            aggregation.output(blockBuilder);
            Block block = blockBuilder.build();

            assertThat(makeValidityAssertion(expectedValues[start]).apply(
                    BlockAssertions.getOnlyValue(outputType, block),
                    expectedValues[start]))
                    .isTrue();
        }

        Page inputPage2 = new Page(totalPositions, getSequenceBlocksForRealInfinityTest(0, totalPositions));

        PagesIndex pagesIndex2 = new PagesIndex.TestingFactory(false).newPagesIndex(getFunctionParameterTypes(), totalPositions);
        pagesIndex2.addPage(inputPage2);
        WindowIndex windowIndex2 = new PagesWindowIndex(pagesIndex2, 0, totalPositions - 1);
        WindowAccumulator aggregation2 = createWindowAccumulator(resolvedFunction, aggregationImplementation);
        oldStart = 0;
        oldWidth = 0;
        for (int start = 0; start < totalPositions; ++start) {
            int width = windowWidths[start];
            for (int oldi = oldStart; oldi < oldStart + oldWidth; ++oldi) {
                if (oldi < start || oldi >= start + width) {
                    boolean res = aggregation2.removeInput(windowIndex2, oldi, oldi);
                    if (oldi >= 4) {
                        assertThat(res).isFalse();
                    }
                    else {
                        assertThat(res).isTrue();
                    }
                }
            }
            for (int newi = start; newi < start + width; ++newi) {
                if (newi < oldStart || newi >= oldStart + oldWidth) {
                    aggregation2.addInput(windowIndex2, newi, newi);
                }
            }
            oldStart = start;
            oldWidth = width;

            Type outputType = resolvedFunction.signature().getReturnType();
            BlockBuilder blockBuilder = outputType.createBlockBuilder(null, 1000);
            aggregation2.output(blockBuilder);
            Block block = blockBuilder.build();

            assertThat(makeValidityAssertion(expectedValues2[start]).apply(
                    BlockAssertions.getOnlyValue(outputType, block),
                    expectedValues2[start]))
                    .isTrue();
        }
    }
}
