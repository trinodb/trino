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
package io.trino.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.ArrayType;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.InputReferenceExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.google.common.collect.Iterators.getOnlyElement;
import static io.trino.block.BlockAssertions.createLongDictionaryBlock;
import static io.trino.block.BlockAssertions.createRepeatedValuesBlock;
import static io.trino.block.BlockAssertions.createSlicesBlock;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.operator.project.PageProcessor.MAX_BATCH_SIZE;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.relational.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.Expressions.field;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPageProcessorCompiler
{
    private final TestingFunctionResolution functionResolution = new TestingFunctionResolution();
    private final ExpressionCompiler compiler = functionResolution.getExpressionCompiler();

    @Test
    public void testNoCaching()
    {
        ImmutableList.Builder<RowExpression> projectionsBuilder = ImmutableList.builder();
        ArrayType arrayType = new ArrayType(VARCHAR);
        ResolvedFunction resolvedFunction = functionResolution.resolveFunction(QualifiedName.of("concat"), fromTypes(arrayType, arrayType));
        projectionsBuilder.add(new CallExpression(resolvedFunction, ImmutableList.of(field(0, arrayType), field(1, arrayType))));

        ImmutableList<RowExpression> projections = projectionsBuilder.build();
        PageProcessor pageProcessor = compiler.compilePageProcessor(Optional.empty(), projections).get();
        PageProcessor pageProcessor2 = compiler.compilePageProcessor(Optional.empty(), projections).get();
        assertTrue(pageProcessor != pageProcessor2);
    }

    @Test
    public void testSanityRLE()
    {
        PageProcessor processor = compiler.compilePageProcessor(Optional.empty(), ImmutableList.of(field(0, BIGINT), field(1, VARCHAR)), MAX_BATCH_SIZE).get();

        Slice varcharValue = Slices.utf8Slice("hello");
        Page page = new Page(RunLengthEncodedBlock.create(BIGINT, 123L, 100), RunLengthEncodedBlock.create(VARCHAR, varcharValue, 100));
        Page outputPage = getOnlyElement(
                processor.process(
                        null,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        page))
                .orElseThrow(() -> new AssertionError("page is not present"));

        assertEquals(outputPage.getPositionCount(), 100);
        assertTrue(outputPage.getBlock(0) instanceof RunLengthEncodedBlock);
        assertTrue(outputPage.getBlock(1) instanceof RunLengthEncodedBlock);

        RunLengthEncodedBlock rleBlock = (RunLengthEncodedBlock) outputPage.getBlock(0);
        assertEquals(BIGINT.getLong(rleBlock.getValue(), 0), 123L);

        RunLengthEncodedBlock rleBlock1 = (RunLengthEncodedBlock) outputPage.getBlock(1);
        assertEquals(VARCHAR.getSlice(rleBlock1.getValue(), 0), varcharValue);
    }

    @Test
    public void testSanityFilterOnDictionary()
    {
        CallExpression lengthVarchar = new CallExpression(
                functionResolution.resolveFunction(QualifiedName.of("length"), fromTypes(VARCHAR)),
                ImmutableList.of(field(0, VARCHAR)));
        ResolvedFunction lessThan = functionResolution.resolveOperator(LESS_THAN, ImmutableList.of(BIGINT, BIGINT));
        CallExpression filter = new CallExpression(lessThan, ImmutableList.of(lengthVarchar, constant(10L, BIGINT)));

        PageProcessor processor = compiler.compilePageProcessor(Optional.of(filter), ImmutableList.of(field(0, VARCHAR)), MAX_BATCH_SIZE).get();

        Page page = new Page(createDictionaryBlock(createExpectedValues(10), 100));
        Page outputPage = getOnlyElement(
                processor.process(
                        null,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        page))
                .orElseThrow(() -> new AssertionError("page is not present"));

        assertEquals(outputPage.getPositionCount(), 100);
        assertTrue(outputPage.getBlock(0) instanceof DictionaryBlock);

        DictionaryBlock dictionaryBlock = (DictionaryBlock) outputPage.getBlock(0);
        assertEquals(dictionaryBlock.getDictionary().getPositionCount(), 10);

        // test filter caching
        Page outputPage2 = getOnlyElement(
                processor.process(
                        null,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        page)).orElseThrow(() -> new AssertionError("page is not present"));
        assertEquals(outputPage2.getPositionCount(), 100);
        assertTrue(outputPage2.getBlock(0) instanceof DictionaryBlock);

        DictionaryBlock dictionaryBlock2 = (DictionaryBlock) outputPage2.getBlock(0);
        // both output pages must have the same dictionary
        assertEquals(dictionaryBlock2.getDictionary(), dictionaryBlock.getDictionary());
    }

    @Test
    public void testSanityFilterOnRLE()
    {
        ResolvedFunction lessThan = functionResolution.resolveOperator(LESS_THAN, ImmutableList.of(BIGINT, BIGINT));
        CallExpression filter = new CallExpression(lessThan, ImmutableList.of(field(0, BIGINT), constant(10L, BIGINT)));

        PageProcessor processor = compiler.compilePageProcessor(Optional.of(filter), ImmutableList.of(field(0, BIGINT)), MAX_BATCH_SIZE).get();

        Page page = new Page(createRepeatedValuesBlock(5L, 100));
        Page outputPage = getOnlyElement(
                processor.process(
                        null,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        page))
                .orElseThrow(() -> new AssertionError("page is not present"));

        assertEquals(outputPage.getPositionCount(), 100);
        assertTrue(outputPage.getBlock(0) instanceof RunLengthEncodedBlock);

        RunLengthEncodedBlock rle = (RunLengthEncodedBlock) outputPage.getBlock(0);
        assertEquals(BIGINT.getLong(rle.getValue(), 0), 5L);
    }

    @Test
    public void testSanityColumnarDictionary()
    {
        PageProcessor processor = compiler.compilePageProcessor(Optional.empty(), ImmutableList.of(field(0, VARCHAR)), MAX_BATCH_SIZE).get();

        Page page = new Page(createDictionaryBlock(createExpectedValues(10), 100));
        Page outputPage = getOnlyElement(
                processor.process(
                        null,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        page))
                .orElseThrow(() -> new AssertionError("page is not present"));

        assertEquals(outputPage.getPositionCount(), 100);
        assertTrue(outputPage.getBlock(0) instanceof DictionaryBlock);

        DictionaryBlock dictionaryBlock = (DictionaryBlock) outputPage.getBlock(0);
        assertEquals(dictionaryBlock.getDictionary().getPositionCount(), 10);
    }

    @Test
    public void testNonDeterministicProject()
    {
        ResolvedFunction lessThan = functionResolution.resolveOperator(LESS_THAN, ImmutableList.of(BIGINT, BIGINT));
        CallExpression random = new CallExpression(
                functionResolution.resolveFunction(QualifiedName.of("random"), fromTypes(BIGINT)),
                singletonList(constant(10L, BIGINT)));
        InputReferenceExpression col0 = field(0, BIGINT);
        CallExpression lessThanRandomExpression = new CallExpression(lessThan, ImmutableList.of(col0, random));

        PageProcessor processor = compiler.compilePageProcessor(Optional.empty(), ImmutableList.of(lessThanRandomExpression), MAX_BATCH_SIZE).get();

        assertFalse(isDeterministic(lessThanRandomExpression));

        Page page = new Page(createLongDictionaryBlock(1, 100));
        Page outputPage = getOnlyElement(
                processor.process(
                        null,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        page))
                .orElseThrow(() -> new AssertionError("page is not present"));
        assertFalse(outputPage.getBlock(0) instanceof DictionaryBlock);
    }

    private static Block createDictionaryBlock(Slice[] expectedValues, int positionCount)
    {
        int dictionarySize = expectedValues.length;
        int[] ids = new int[positionCount];

        for (int i = 0; i < positionCount; i++) {
            ids[i] = i % dictionarySize;
        }
        return DictionaryBlock.create(ids.length, createSlicesBlock(expectedValues), ids);
    }

    private static Slice[] createExpectedValues(int positionCount)
    {
        Slice[] expectedValues = new Slice[positionCount];
        for (int position = 0; position < positionCount; position++) {
            expectedValues[position] = createExpectedValue(position);
        }
        return expectedValues;
    }

    private static Slice createExpectedValue(int length)
    {
        DynamicSliceOutput dynamicSliceOutput = new DynamicSliceOutput(16);
        for (int index = 0; index < length; index++) {
            dynamicSliceOutput.writeByte(length * (index + 1));
        }
        return dynamicSliceOutput.slice();
    }
}
