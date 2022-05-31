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
import io.trino.metadata.FunctionBundle;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.PagesIndex;
import io.trino.operator.window.PagesWindowIndex;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.function.WindowIndex;
import io.trino.spi.type.Type;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.lang.reflect.Constructor;
import java.util.List;

import static io.trino.operator.aggregation.AccumulatorCompiler.generateWindowAccumulatorClass;
import static io.trino.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.trino.operator.aggregation.AggregationTestUtils.makeValidityAssertion;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractTestAggregationFunction
{
    protected final TestingFunctionResolution functionResolution;

    protected AbstractTestAggregationFunction()
    {
        functionResolution = new TestingFunctionResolution();
    }

    protected AbstractTestAggregationFunction(FunctionBundle functions)
    {
        functionResolution = new TestingFunctionResolution(functions);
    }

    protected abstract Block[] getSequenceBlocks(int start, int length);

    protected abstract String getFunctionName();

    protected abstract List<Type> getFunctionParameterTypes();

    protected abstract Object getExpectedValue(int start, int length);

    protected Object getExpectedValueIncludingNulls(int start, int length, int lengthIncludingNulls)
    {
        return getExpectedValue(start, length);
    }

    @Test
    public void testNoPositions()
    {
        testAggregation(getExpectedValue(0, 0), getSequenceBlocks(0, 0));
    }

    @Test
    public void testSinglePosition()
    {
        testAggregation(getExpectedValue(0, 1), getSequenceBlocks(0, 1));
    }

    @Test
    public void testMultiplePositions()
    {
        testAggregation(getExpectedValue(0, 5), getSequenceBlocks(0, 5));
    }

    @Test
    public void testAllPositionsNull()
    {
        // if there are no parameters skip this test
        List<Type> parameterTypes = getFunctionParameterTypes();
        if (parameterTypes.isEmpty()) {
            return;
        }
        Block[] blocks = new Block[parameterTypes.size()];
        for (int i = 0; i < parameterTypes.size(); i++) {
            blocks[i] = RunLengthEncodedBlock.create(parameterTypes.get(0), null, 10);
        }

        testAggregation(getExpectedValueIncludingNulls(0, 0, 10), blocks);
    }

    @Test
    public void testMixedNullAndNonNullPositions()
    {
        // if there are no parameters skip this test
        List<Type> parameterTypes = getFunctionParameterTypes();
        if (parameterTypes.isEmpty()) {
            return;
        }

        Block[] alternatingNullsBlocks = createAlternatingNullsBlock(parameterTypes, getSequenceBlocks(0, 10));
        testAggregation(getExpectedValueIncludingNulls(0, 10, 20), alternatingNullsBlocks);
    }

    @Test
    public void testNegativeOnlyValues()
    {
        testAggregation(getExpectedValue(-10, 5), getSequenceBlocks(-10, 5));
    }

    @Test
    public void testPositiveOnlyValues()
    {
        testAggregation(getExpectedValue(2, 4), getSequenceBlocks(2, 4));
    }

    @Test
    public void testSlidingWindow()
    {
        // Builds trailing windows of length 0, 1, 2, 3, 4, 5, 5, 4, 3, 2, 1, 0
        int totalPositions = 12;
        int[] windowWidths = new int[totalPositions];
        Object[] expectedValues = new Object[totalPositions];

        for (int i = 0; i < totalPositions; ++i) {
            int windowWidth = Integer.min(i, totalPositions - 1 - i);
            windowWidths[i] = windowWidth;
            expectedValues[i] = getExpectedValue(i, windowWidth);
        }
        Page inputPage = new Page(totalPositions, getSequenceBlocks(0, totalPositions));

        PagesIndex pagesIndex = new PagesIndex.TestingFactory(false).newPagesIndex(getFunctionParameterTypes(), totalPositions);
        pagesIndex.addPage(inputPage);
        WindowIndex windowIndex = new PagesWindowIndex(pagesIndex, 0, totalPositions - 1);

        ResolvedFunction resolvedFunction = functionResolution.resolveFunction(QualifiedName.of(getFunctionName()), fromTypes(getFunctionParameterTypes()));
        AggregationMetadata aggregationMetadata = functionResolution.getPlannerContext().getFunctionManager().getAggregateFunctionImplementation(resolvedFunction);
        WindowAccumulator aggregation = createWindowAccumulator(resolvedFunction, aggregationMetadata);
        int oldStart = 0;
        int oldWidth = 0;
        for (int start = 0; start < totalPositions; ++start) {
            int width = windowWidths[start];
            // Note that add/removeInput's interval is inclusive on both ends
            if (aggregationMetadata.getRemoveInputFunction().isPresent()) {
                for (int oldi = oldStart; oldi < oldStart + oldWidth; ++oldi) {
                    if (oldi < start || oldi >= start + width) {
                        aggregation.removeInput(windowIndex, oldi, oldi);
                    }
                }
                for (int newi = start; newi < start + width; ++newi) {
                    if (newi < oldStart || newi >= oldStart + oldWidth) {
                        aggregation.addInput(windowIndex, newi, newi);
                    }
                }
            }
            else {
                aggregation = createWindowAccumulator(resolvedFunction, aggregationMetadata);
                aggregation.addInput(windowIndex, start, start + width - 1);
            }
            oldStart = start;
            oldWidth = width;

            Type outputType = resolvedFunction.getSignature().getReturnType();
            BlockBuilder blockBuilder = outputType.createBlockBuilder(null, 1000);
            aggregation.evaluateFinal(blockBuilder);
            Block block = blockBuilder.build();

            assertThat(makeValidityAssertion(expectedValues[start]).apply(
                    BlockAssertions.getOnlyValue(outputType, block),
                    expectedValues[start]))
                    .isTrue();
        }
    }

    private static WindowAccumulator createWindowAccumulator(ResolvedFunction resolvedFunction, AggregationMetadata aggregationMetadata)
    {
        try {
            Constructor<? extends WindowAccumulator> constructor = generateWindowAccumulatorClass(
                    resolvedFunction.getSignature(),
                    aggregationMetadata,
                    resolvedFunction.getFunctionNullability());
            return constructor.newInstance(ImmutableList.of());
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    protected static Block[] createAlternatingNullsBlock(List<Type> types, Block... sequenceBlocks)
    {
        Block[] alternatingNullsBlocks = new Block[sequenceBlocks.length];
        for (int i = 0; i < sequenceBlocks.length; i++) {
            int positionCount = sequenceBlocks[i].getPositionCount();
            Type type = types.get(i);
            BlockBuilder blockBuilder = type.createBlockBuilder(null, positionCount);
            for (int position = 0; position < positionCount; position++) {
                // append null
                blockBuilder.appendNull();
                // append value
                type.appendTo(sequenceBlocks[i], position, blockBuilder);
            }
            alternatingNullsBlocks[i] = blockBuilder.build();
        }
        return alternatingNullsBlocks;
    }

    protected void testAggregation(Object expectedValue, Block... blocks)
    {
        assertAggregation(
                functionResolution,
                QualifiedName.of(getFunctionName()),
                fromTypes(getFunctionParameterTypes()),
                expectedValue,
                blocks);
    }

    protected void assertInvalidAggregation(Runnable runnable)
    {
        assertTrinoExceptionThrownBy(runnable::run)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }
}
