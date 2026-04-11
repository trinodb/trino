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
import com.google.common.collect.ImmutableMap;
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
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.ArrayType;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;

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
import static io.trino.sql.ir.IrExpressions.call;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPageProcessorCompiler
{
    private final TestingFunctionResolution functionResolution = new TestingFunctionResolution();
    private final ExpressionCompiler compiler = functionResolution.getExpressionCompiler();

    @Test
    public void testNoCaching()
    {
        ArrayType arrayType = new ArrayType(VARCHAR);
        ResolvedFunction resolvedFunction = functionResolution.resolveFunction("concat", fromTypes(arrayType, arrayType));
        Reference col0 = new Reference(arrayType, "$col_0");
        Reference col1 = new Reference(arrayType, "$col_1");
        Map<Symbol, Integer> layout = ImmutableMap.of(
                new Symbol(arrayType, "$col_0"), 3,
                new Symbol(arrayType, "$col_1"), 1);
        List<Expression> projections = ImmutableList.of(call(resolvedFunction, col0, col1));

        Function<DynamicFilter, PageProcessor> factory1 = compiler.compilePageProcessor(true, Optional.empty(), Optional.empty(), projections, layout, Optional.empty(), OptionalInt.empty());
        Function<DynamicFilter, PageProcessor> factory2 = compiler.compilePageProcessor(true, Optional.empty(), Optional.empty(), projections, layout, Optional.empty(), OptionalInt.empty());
        assertThat(factory1 != factory2).isTrue();
    }

    @Test
    public void testSanityRLE()
    {
        Map<Symbol, Integer> layout = ImmutableMap.of(
                new Symbol(BIGINT, "$col_0"), 3,
                new Symbol(VARCHAR, "$col_1"), 1);
        PageProcessor processor = compiler.compilePageProcessor(true, Optional.empty(), Optional.empty(),
                        ImmutableList.of(new Reference(BIGINT, "$col_0"), new Reference(VARCHAR, "$col_1")),
                        layout, Optional.empty(), OptionalInt.of(MAX_BATCH_SIZE))
                .apply(DynamicFilter.EMPTY);

        Slice varcharValue = Slices.utf8Slice("hello");
        Page page = new Page(
                RunLengthEncodedBlock.create(BIGINT, 0L, 100),             // channel 0 - dummy
                RunLengthEncodedBlock.create(VARCHAR, varcharValue, 100),   // channel 1 - $col_1
                RunLengthEncodedBlock.create(BIGINT, 0L, 100),             // channel 2 - dummy
                RunLengthEncodedBlock.create(BIGINT, 123L, 100));           // channel 3 - $col_0
        Page outputPage = getOnlyElement(
                processor.process(
                        null,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        SourcePage.create(page)))
                .orElseThrow(() -> new AssertionError("page is not present"));

        assertThat(outputPage.getPositionCount()).isEqualTo(100);
        assertThat(outputPage.getBlock(0) instanceof RunLengthEncodedBlock).isTrue();
        assertThat(outputPage.getBlock(1) instanceof RunLengthEncodedBlock).isTrue();

        RunLengthEncodedBlock rleBlock = (RunLengthEncodedBlock) outputPage.getBlock(0);
        assertThat(BIGINT.getLong(rleBlock.getValue(), 0)).isEqualTo(123L);

        RunLengthEncodedBlock rleBlock1 = (RunLengthEncodedBlock) outputPage.getBlock(1);
        assertThat(VARCHAR.getSlice(rleBlock1.getValue(), 0)).isEqualTo(varcharValue);
    }

    @Test
    public void testSanityFilterOnDictionary()
    {
        Reference col0 = new Reference(VARCHAR, "$col_0");
        Map<Symbol, Integer> layout = ImmutableMap.of(new Symbol(VARCHAR, "$col_0"), 2);
        Call lengthVarchar = call(
                functionResolution.resolveFunction("length", fromTypes(VARCHAR)),
                col0);
        ResolvedFunction lessThan = functionResolution.resolveOperator(LESS_THAN, ImmutableList.of(BIGINT, BIGINT));
        Call filter = call(lessThan, lengthVarchar, new Constant(BIGINT, 10L));

        PageProcessor processor = compiler.compilePageProcessor(true, Optional.of(filter), Optional.empty(),
                        ImmutableList.of(col0), layout, Optional.empty(), OptionalInt.of(MAX_BATCH_SIZE))
                .apply(DynamicFilter.EMPTY);

        Block inputDictionaryBlock = createDictionaryBlock(createExpectedValues(10), 100);
        Page page = new Page(createRepeatedValuesBlock(0L, 100), createRepeatedValuesBlock(0L, 100), inputDictionaryBlock);
        Page outputPage = getOnlyElement(
                processor.process(
                        null,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        SourcePage.create(page)))
                .orElseThrow(() -> new AssertionError("page is not present"));

        assertThat(outputPage.getPositionCount()).isEqualTo(100);
        assertThat(outputPage.getBlock(0) instanceof DictionaryBlock).isTrue();

        DictionaryBlock dictionaryBlock = (DictionaryBlock) outputPage.getBlock(0);
        assertThat(dictionaryBlock.getDictionary().getPositionCount()).isEqualTo(10);

        // test filter caching
        Page outputPage2 = getOnlyElement(
                processor.process(
                        null,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        SourcePage.create(page))).orElseThrow(() -> new AssertionError("page is not present"));
        assertThat(outputPage2.getPositionCount()).isEqualTo(100);
        assertThat(outputPage2.getBlock(0) instanceof DictionaryBlock).isTrue();

        DictionaryBlock dictionaryBlock2 = (DictionaryBlock) outputPage2.getBlock(0);
        // both output pages must have the same dictionary
        assertThat(dictionaryBlock2.getDictionary()).isEqualTo(dictionaryBlock.getDictionary());
    }

    @Test
    public void testSanityFilterOnRLE()
    {
        Reference col0 = new Reference(BIGINT, "$col_0");
        Map<Symbol, Integer> layout = ImmutableMap.of(new Symbol(BIGINT, "$col_0"), 2);
        ResolvedFunction lessThan = functionResolution.resolveOperator(LESS_THAN, ImmutableList.of(BIGINT, BIGINT));
        Call filter = call(lessThan, col0, new Constant(BIGINT, 10L));

        PageProcessor processor = compiler.compilePageProcessor(true, Optional.of(filter), Optional.empty(),
                        ImmutableList.of(col0), layout, Optional.empty(), OptionalInt.of(MAX_BATCH_SIZE))
                .apply(DynamicFilter.EMPTY);

        Page page = new Page(createRepeatedValuesBlock(0L, 100), createRepeatedValuesBlock(0L, 100), createRepeatedValuesBlock(5L, 100));
        Page outputPage = getOnlyElement(
                processor.process(
                        null,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        SourcePage.create(page)))
                .orElseThrow(() -> new AssertionError("page is not present"));

        assertThat(outputPage.getPositionCount()).isEqualTo(100);
        assertThat(outputPage.getBlock(0) instanceof RunLengthEncodedBlock).isTrue();

        RunLengthEncodedBlock rle = (RunLengthEncodedBlock) outputPage.getBlock(0);
        assertThat(BIGINT.getLong(rle.getValue(), 0)).isEqualTo(5L);
    }

    @Test
    public void testSanityColumnarDictionary()
    {
        Map<Symbol, Integer> layout = ImmutableMap.of(new Symbol(VARCHAR, "$col_0"), 2);
        PageProcessor processor = compiler.compilePageProcessor(true, Optional.empty(), Optional.empty(),
                        ImmutableList.of(new Reference(VARCHAR, "$col_0")), layout, Optional.empty(), OptionalInt.of(MAX_BATCH_SIZE))
                .apply(DynamicFilter.EMPTY);

        Block inputDictionaryBlock = createDictionaryBlock(createExpectedValues(10), 100);
        Page page = new Page(createRepeatedValuesBlock(0L, 100), createRepeatedValuesBlock(0L, 100), inputDictionaryBlock);
        Page outputPage = getOnlyElement(
                processor.process(
                        null,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        SourcePage.create(page)))
                .orElseThrow(() -> new AssertionError("page is not present"));

        assertThat(outputPage.getPositionCount()).isEqualTo(100);
        assertThat(outputPage.getBlock(0) instanceof DictionaryBlock).isTrue();

        DictionaryBlock dictionaryBlock = (DictionaryBlock) outputPage.getBlock(0);
        assertThat(dictionaryBlock.getDictionary().getPositionCount()).isEqualTo(10);
    }

    @Test
    public void testNonDeterministicProject()
    {
        Reference col0 = new Reference(BIGINT, "$col_0");
        Map<Symbol, Integer> layout = ImmutableMap.of(new Symbol(BIGINT, "$col_0"), 2);
        ResolvedFunction lessThan = functionResolution.resolveOperator(LESS_THAN, ImmutableList.of(BIGINT, BIGINT));
        Call random = call(
                functionResolution.resolveFunction("random", fromTypes(BIGINT)),
                new Constant(BIGINT, 10L));
        Call lessThanRandomExpression = call(lessThan, col0, random);

        PageProcessor processor = compiler.compilePageProcessor(true, Optional.empty(), Optional.empty(),
                        ImmutableList.of(lessThanRandomExpression), layout, Optional.empty(), OptionalInt.of(MAX_BATCH_SIZE))
                .apply(DynamicFilter.EMPTY);

        assertThat(isDeterministic(lessThanRandomExpression)).isFalse();

        Page page = new Page(createRepeatedValuesBlock(0L, 100), createRepeatedValuesBlock(0L, 100), createLongDictionaryBlock(1, 100));
        Page outputPage = getOnlyElement(
                processor.process(
                        null,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        SourcePage.create(page)))
                .orElseThrow(() -> new AssertionError("page is not present"));
        assertThat(outputPage.getBlock(0) instanceof DictionaryBlock).isFalse();
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
