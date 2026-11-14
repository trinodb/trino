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
package io.trino.sql.gen;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.project.GeneratedPageProjection;
import io.trino.operator.project.InputChannels;
import io.trino.operator.project.PageFilter;
import io.trino.operator.project.PageProcessor;
import io.trino.operator.project.PageProjection;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.sql.gen.columnar.PageFilterEvaluator;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.Symbol;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.IntPredicate;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.IrExpressions.call;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Asserts that the columnar projection of an expression produces the result of the generated row evaluation
 * for every block encoding of its first input, every filter, and every projection batch size.
 */
public final class ColumnarProjectionAssertions
{
    private static final List<Optional<IntPredicate>> FILTERS = ImmutableList.of(
            Optional.empty(),
            Optional.of(position -> position % 2 == 0),
            Optional.of(position -> position % 5 == 3));

    private final TestingFunctionResolution functions;
    private final PageFunctionCompiler compiler;

    public ColumnarProjectionAssertions(TestingFunctionResolution functions)
    {
        this.functions = requireNonNull(functions, "functions is null");
        this.compiler = functions.getPageFunctionCompiler();
    }

    /**
     * Returns a call of the function with the given name, resolved for the types of the arguments.
     */
    public Expression function(String name, Expression... arguments)
    {
        return call(functions.resolveFunction(name, fromTypes(argumentTypes(arguments))), arguments);
    }

    /**
     * Returns a call of the operator, resolved for the types of the arguments.
     */
    public Expression operator(OperatorType operatorType, Expression... arguments)
    {
        return call(functions.resolveOperator(operatorType, argumentTypes(arguments)), arguments);
    }

    /**
     * Asserts that the expression has a columnar projection, and that the projection produces the result
     * of the generated row evaluation for every encoding of the first input, filter, and batch size.
     */
    public void assertEquivalent(Expression expression, Input... inputs)
    {
        assertEquivalent(expression, ImmutableList.copyOf(inputs), FILTERS);
    }

    /**
     * Asserts that evaluating every position fails with an invalid argument, and that the columnar projection
     * produces the result of the generated row evaluation once a filter removes the invalid positions,
     * which shows that the columnar implementation does not evaluate the positions a filter removed.
     */
    public void assertEquivalentForValidPositions(Expression expression, IntPredicate invalid, Input... inputs)
    {
        List<Input> inputList = ImmutableList.copyOf(inputs);
        Page page = page(inputList, Encoding.FLAT);
        PageProjection projection = compile(expression, inputList, true);
        assertTrinoExceptionThrownBy(() -> process(projection, expression.type(), page, Optional.empty(), page.getPositionCount()))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertEquivalent(expression, inputList, ImmutableList.of(
                Optional.of(invalid.negate()),
                Optional.of(position -> !invalid.test(position) && position % 2 == 0)));
    }

    /**
     * Asserts that the expression is evaluated by the generated row loop, as no columnar implementation applies to it.
     */
    public void assertGenerated(Expression expression, Input... inputs)
    {
        assertThat(compile(expression, ImmutableList.copyOf(inputs), true)).isInstanceOf(GeneratedPageProjection.class);
    }

    /**
     * Returns the values as a region that does not start at the first position of its block, so the offsets
     * of its arrays or maps do not start at the first element or entry.
     */
    public static Block region(Type type, Block block)
    {
        int positionCount = block.getPositionCount();
        BlockBuilder builder = type.createBlockBuilder(null, positionCount * 2);
        builder.appendBlockRange(block, 0, positionCount);
        builder.appendBlockRange(block, 0, positionCount);
        return builder.build().getRegion(positionCount / 2, positionCount);
    }

    /**
     * Returns the values through a dictionary whose ids start past its first id and select the second copy of the values.
     */
    public static Block dictionaryOverRegion(Type type, Block block)
    {
        int positionCount = block.getPositionCount();
        BlockBuilder builder = type.createBlockBuilder(null, positionCount * 2);
        builder.appendBlockRange(block, 0, positionCount);
        builder.appendBlockRange(block, 0, positionCount);
        int[] ids = new int[positionCount + 3];
        for (int position = 0; position < positionCount; position++) {
            ids[position + 3] = positionCount + position;
        }
        return DictionaryBlock.create(ids.length, builder.build(), ids).getRegion(3, positionCount);
    }

    private void assertEquivalent(Expression expression, List<Input> inputs, List<Optional<IntPredicate>> filters)
    {
        int positionCount = positionCount(inputs);
        for (Encoding encoding : Encoding.values()) {
            Page page = page(inputs, encoding);
            for (Optional<IntPredicate> filter : filters) {
                for (int batchSize : new int[] {1, 2, positionCount}) {
                    PageProjection columnar = compile(expression, inputs, true);
                    assertThat(columnar).isNotInstanceOf(GeneratedPageProjection.class);
                    PageProjection generated = compile(expression, inputs, false);
                    assertThat(generated).isInstanceOf(GeneratedPageProjection.class);

                    Block expected = process(generated, expression.type(), page, filter, batchSize);
                    Block actual = process(columnar, expression.type(), page, filter, batchSize);
                    assertBlockEquals(expression.type(), actual, expected);
                }
            }
        }
    }

    private PageProjection compile(Expression expression, List<Input> inputs, boolean preferColumnar)
    {
        ImmutableMap.Builder<Symbol, Integer> layout = ImmutableMap.builder();
        for (int channel = 0; channel < inputs.size(); channel++) {
            Input input = inputs.get(channel);
            layout.put(new Symbol(input.type(), input.name()), channel);
        }
        return compiler.compileProjection(expression, layout.buildOrThrow(), SQL_STANDARD, Optional.empty(), preferColumnar).get();
    }

    private static Block process(PageProjection projection, Type type, Page page, Optional<IntPredicate> filter, int batchSize)
    {
        PageProcessor processor = new PageProcessor(
                filter.map(predicate -> new PageFilterEvaluator(new PositionsPageFilter(predicate))),
                Optional.empty(),
                ImmutableList.of(projection),
                OptionalInt.of(batchSize));
        Iterator<Optional<Page>> output = processor.process(
                SESSION,
                newSimpleAggregatedMemoryContext().newLocalMemoryContext("test"),
                SourcePage.create(page));

        BlockBuilder result = type.createBlockBuilder(null, page.getPositionCount());
        while (output.hasNext()) {
            output.next().ifPresent(outputPage -> {
                Block block = outputPage.getBlock(0);
                result.appendBlockRange(block, 0, block.getPositionCount());
            });
        }
        return result.build();
    }

    private static Page page(List<Input> inputs, Encoding encoding)
    {
        Block[] blocks = new Block[inputs.size()];
        for (int channel = 0; channel < blocks.length; channel++) {
            Block block = inputs.get(channel).block();
            // only the first input is encoded, so the remaining arguments stay aligned with their positions
            if (channel == 0) {
                block = encoding.encode(block);
            }
            blocks[channel] = block;
        }
        return new Page(positionCount(inputs), blocks);
    }

    private static int positionCount(List<Input> inputs)
    {
        checkArgument(!inputs.isEmpty(), "inputs is empty");
        int positionCount = inputs.getFirst().block().getPositionCount();
        for (Input input : inputs) {
            checkArgument(input.block().getPositionCount() == positionCount, "inputs have different position counts");
        }
        return positionCount;
    }

    private static List<Type> argumentTypes(Expression[] arguments)
    {
        return Arrays.stream(arguments)
                .map(Expression::type)
                .collect(toImmutableList());
    }

    public record Input(String name, Type type, Block block)
    {
        public Input
        {
            requireNonNull(name, "name is null");
            requireNonNull(type, "type is null");
            requireNonNull(block, "block is null");
        }
    }

    private enum Encoding
    {
        FLAT,
        DICTIONARY,
        RLE;

        public Block encode(Block block)
        {
            int positionCount = block.getPositionCount();
            return switch (this) {
                case FLAT -> block;
                // reverse the positions, so dictionary ids are not sequential
                case DICTIONARY -> DictionaryBlock.create(
                        positionCount,
                        block,
                        IntStream.range(0, positionCount).map(position -> positionCount - 1 - position).toArray());
                case RLE -> RunLengthEncodedBlock.create(block.getSingleValueBlock(2), positionCount);
            };
        }
    }

    private static final class PositionsPageFilter
            implements PageFilter
    {
        private final IntPredicate predicate;

        private PositionsPageFilter(IntPredicate predicate)
        {
            this.predicate = requireNonNull(predicate, "predicate is null");
        }

        @Override
        public boolean isDeterministic()
        {
            return true;
        }

        @Override
        public InputChannels getInputChannels()
        {
            return new InputChannels();
        }

        @Override
        public SelectedPositions filter(ConnectorSession session, SourcePage page)
        {
            int[] positions = IntStream.range(0, page.getPositionCount()).filter(predicate).toArray();
            return SelectedPositions.positionsList(positions, 0, positions.length);
        }
    }
}
