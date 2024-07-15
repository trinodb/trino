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
import io.trino.block.BlockAssertions;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.PagesIndex;
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

import static io.trino.operator.aggregation.AggregationTestUtils.makeValidityAssertion;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDoubleAverageAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    protected Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(null, length);
        for (int i = start; i < start + length; i++) {
            DOUBLE.writeDouble(blockBuilder, i);
        }
        return new Block[] {blockBuilder.build()};
    }

    @Override
    protected Number getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }

        double sum = 0;
        for (int i = start; i < start + length; i++) {
            sum += i;
        }
        return sum / length;
    }

    @Override
    protected String getFunctionName()
    {
        return "avg";
    }

    @Override
    protected List<Type> getFunctionParameterTypes()
    {
        return ImmutableList.of(DOUBLE);
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
                expectedValues[i] = Double.NaN;
                expectedValues2[i] = Double.POSITIVE_INFINITY;
            }
            else {
                expectedValues[i] = getExpectedValue(i, windowWidth);
                expectedValues2[i] = getExpectedValue(i, windowWidth);
            }
        }
        Page inputPage = new Page(totalPositions, TestDoubleSumAggregation.getSequenceBlocksForDoubleNaNTest(0, totalPositions));

        PagesIndex pagesIndex = new PagesIndex.TestingFactory(false).newPagesIndex(getFunctionParameterTypes(), totalPositions);
        pagesIndex.addPage(inputPage);
        WindowIndex windowIndex = new PagesWindowIndex(pagesIndex, 0, totalPositions - 1);

        ResolvedFunction resolvedFunction = functionResolution.resolveFunction(getFunctionName(), fromTypes(getFunctionParameterTypes()));
        AggregationImplementation aggregationImplementation = functionResolution.getPlannerContext().getFunctionManager().getAggregationImplementation(resolvedFunction);
        WindowAccumulator aggregation = createWindowAccumulator(resolvedFunction, aggregationImplementation);
        assertThat(resolvedFunction.signature().getReturnType().toString().contains("double")).isTrue();
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

        Page inputPage2 = new Page(totalPositions, TestDoubleSumAggregation.getSequenceBlocksForDoubleInfinityTest(0, totalPositions));

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
