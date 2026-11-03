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
import io.airlift.slice.Slices;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.project.GeneratedPageProjection;
import io.trino.operator.project.InputChannels;
import io.trino.operator.project.PageFilter;
import io.trino.operator.project.PageProcessor;
import io.trino.operator.project.PageProjection;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.columnar.PageFilterEvaluator;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.IntPredicate;
import java.util.function.IntToLongFunction;
import java.util.stream.IntStream;

import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.OperatorType.SUBSCRIPT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.IrExpressions.call;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that columnar projections produce the same results as generated row evaluation
 * for every block encoding, filter, and projection batch size.
 */
public class TestColumnarProjectionEquivalence
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final int POSITION_COUNT = 40;

    private static final ArrayType VARCHAR_ARRAY = new ArrayType(VARCHAR);
    private static final ArrayType NESTED_ARRAY = new ArrayType(VARCHAR_ARRAY);
    private static final MapType MAP = new MapType(BIGINT, VARCHAR, new TypeOperators());
    private static final RowType ROW = RowType.anonymous(ImmutableList.of(BIGINT, VARCHAR));

    // positions with invalid arguments for the functions that can fail
    private static final IntPredicate INVALID = position -> position % 3 == 1;

    private static final List<Optional<IntPredicate>> FILTERS = ImmutableList.of(
            Optional.empty(),
            Optional.of(position -> position % 2 == 0),
            Optional.of(position -> position % 5 == 3),
            Optional.of(INVALID.negate()));
    private static final List<Optional<IntPredicate>> VALID_FILTERS = ImmutableList.of(
            Optional.of(INVALID.negate()),
            Optional.of(position -> position % 6 == 0 || position % 6 == 5));

    @Test
    public void testMapFunctions()
    {
        for (String function : ImmutableList.of("map_keys", "map_values", "map_entries")) {
            assertEquivalent(
                    call(FUNCTIONS.resolveFunction(function, fromTypes(MAP)), new Reference(MAP, "input")),
                    ImmutableList.of(new Column("input", MAP, mapBlock())),
                    FILTERS);
        }
    }

    @Test
    public void testFlatten()
    {
        assertEquivalent(
                call(FUNCTIONS.resolveFunction("flatten", fromTypes(NESTED_ARRAY)), new Reference(NESTED_ARRAY, "input")),
                ImmutableList.of(new Column("input", NESTED_ARRAY, nestedArrayBlock())),
                FILTERS);
    }

    @Test
    public void testReverse()
    {
        assertEquivalent(
                call(FUNCTIONS.resolveFunction("reverse", fromTypes(VARCHAR_ARRAY)), new Reference(VARCHAR_ARRAY, "input")),
                ImmutableList.of(new Column("input", VARCHAR_ARRAY, varcharArrayBlock())),
                FILTERS);
    }

    @Test
    public void testTrimArray()
    {
        // trim_array fails for a negative size
        assertEquivalentForValidPositions(
                call(
                        FUNCTIONS.resolveFunction("trim_array", fromTypes(VARCHAR_ARRAY, BIGINT)),
                        new Reference(VARCHAR_ARRAY, "input"),
                        new Reference(BIGINT, "size")),
                ImmutableList.of(
                        new Column("input", VARCHAR_ARRAY, varcharArrayBlock()),
                        new Column("size", BIGINT, longBlock(position -> INVALID.test(position) ? -1L : 0L))));
    }

    @Test
    public void testSlice()
    {
        // slice fails for a zero start index or a negative length
        assertEquivalentForValidPositions(
                call(
                        FUNCTIONS.resolveFunction("slice", fromTypes(VARCHAR_ARRAY, BIGINT, BIGINT)),
                        new Reference(VARCHAR_ARRAY, "input"),
                        new Reference(BIGINT, "start"),
                        new Reference(BIGINT, "length")),
                ImmutableList.of(
                        new Column("input", VARCHAR_ARRAY, varcharArrayBlock()),
                        new Column("start", BIGINT, longBlock(position -> INVALID.test(position) && position % 2 == 0 ? 0L : 1L)),
                        new Column("length", BIGINT, longBlock(position -> INVALID.test(position) && position % 2 == 1 ? -1L : 2L))));
    }

    @Test
    public void testArrayFirstAndLast()
    {
        for (String function : ImmutableList.of("array_first", "array_last")) {
            assertEquivalent(
                    call(FUNCTIONS.resolveFunction(function, fromTypes(NESTED_ARRAY)), new Reference(NESTED_ARRAY, "input")),
                    ImmutableList.of(new Column("input", NESTED_ARRAY, nestedArrayBlock())),
                    FILTERS);
        }
    }

    @Test
    public void testElementAt()
    {
        // element_at fails for a zero index, and returns null for an index past the end of the array
        assertEquivalentForValidPositions(
                call(
                        FUNCTIONS.resolveFunction("element_at", fromTypes(NESTED_ARRAY, BIGINT)),
                        new Reference(NESTED_ARRAY, "input"),
                        new Reference(BIGINT, "index")),
                ImmutableList.of(
                        new Column("input", NESTED_ARRAY, nestedArrayBlock()),
                        new Column("index", BIGINT, longBlock(TestColumnarProjectionEquivalence::elementAtIndex))));
    }

    @Test
    public void testSubscript()
    {
        // subscript fails for an index past the end of the array
        assertEquivalentForValidPositions(
                call(
                        FUNCTIONS.resolveOperator(SUBSCRIPT, ImmutableList.of(NESTED_ARRAY, BIGINT)),
                        new Reference(NESTED_ARRAY, "input"),
                        new Reference(BIGINT, "index")),
                ImmutableList.of(
                        new Column("input", NESTED_ARRAY, nestedArrayBlock()),
                        new Column("index", BIGINT, longBlock(position -> INVALID.test(position) ? 100L : 1L))));
    }

    @Test
    public void testRowProjections()
    {
        assertEquivalent(
                new FieldReference(new Reference(ROW, "input"), 1),
                ImmutableList.of(new Column("input", ROW, rowBlock())),
                FILTERS);
        assertEquivalent(
                new Row(ImmutableList.of(new Reference(VARCHAR_ARRAY, "input"), new Reference(BIGINT, "value"))),
                ImmutableList.of(
                        new Column("input", VARCHAR_ARRAY, varcharArrayBlock()),
                        new Column("value", BIGINT, longBlock(position -> position))),
                FILTERS);
    }

    private static long elementAtIndex(int position)
    {
        if (INVALID.test(position)) {
            return 0;
        }
        // first, last, second, and past the end of the array
        return switch (position % 4) {
            case 0 -> 1;
            case 1 -> -1;
            case 2 -> 2;
            default -> 5;
        };
    }

    private static void assertEquivalentForValidPositions(Expression expression, List<Column> columns)
    {
        // evaluating all positions must fail, so the filtered results show that unselected positions are not evaluated
        Page page = page(columns, Encoding.FLAT);
        PageProjection projection = compile(expression, columns, true);
        assertTrinoExceptionThrownBy(() -> process(projection, expression.type(), page, Optional.empty(), POSITION_COUNT))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertEquivalent(expression, columns, VALID_FILTERS);
    }

    private static void assertEquivalent(Expression expression, List<Column> columns, List<Optional<IntPredicate>> filters)
    {
        for (Encoding encoding : Encoding.values()) {
            Page page = page(columns, encoding);
            for (Optional<IntPredicate> filter : filters) {
                for (int batchSize : new int[] {1, 2, POSITION_COUNT}) {
                    PageProjection columnar = compile(expression, columns, true);
                    assertThat(columnar).isNotInstanceOf(GeneratedPageProjection.class);
                    PageProjection generated = compile(expression, columns, false);
                    assertThat(generated).isInstanceOf(GeneratedPageProjection.class);

                    Block expected = process(generated, expression.type(), page, filter, batchSize);
                    Block actual = process(columnar, expression.type(), page, filter, batchSize);
                    assertBlockEquals(expression.type(), actual, expected);
                }
            }
        }
    }

    private static PageProjection compile(Expression expression, List<Column> columns, boolean preferColumnar)
    {
        ImmutableMap.Builder<Symbol, Integer> layout = ImmutableMap.builder();
        for (int channel = 0; channel < columns.size(); channel++) {
            Column column = columns.get(channel);
            layout.put(new Symbol(column.type(), column.name()), channel);
        }
        return FUNCTIONS.getPageFunctionCompiler()
                .compileProjection(expression, layout.buildOrThrow(), SQL_STANDARD, Optional.empty(), preferColumnar)
                .get();
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

        BlockBuilder result = type.createBlockBuilder(null, POSITION_COUNT);
        while (output.hasNext()) {
            output.next().ifPresent(outputPage -> {
                Block block = outputPage.getBlock(0);
                result.appendBlockRange(block, 0, block.getPositionCount());
            });
        }
        return result.build();
    }

    private static Page page(List<Column> columns, Encoding encoding)
    {
        Block[] blocks = new Block[columns.size()];
        for (int channel = 0; channel < blocks.length; channel++) {
            Block block = columns.get(channel).block();
            // only the first column is encoded, so the remaining arguments stay aligned with their positions
            if (channel == 0) {
                block = encoding.encode(block);
            }
            blocks[channel] = block;
        }
        return new Page(POSITION_COUNT, blocks);
    }

    private static Block mapBlock()
    {
        MapBlockBuilder builder = MAP.createBlockBuilder(null, POSITION_COUNT);
        for (int position = 0; position < POSITION_COUNT; position++) {
            if (position % 7 == 3) {
                builder.appendNull();
                continue;
            }
            int value = position;
            builder.buildEntry((keyBuilder, valueBuilder) -> {
                for (int entry = 0; entry < value % 4; entry++) {
                    BIGINT.writeLong(keyBuilder, value * 10L + entry);
                    if (entry == 1) {
                        valueBuilder.appendNull();
                    }
                    else {
                        VARCHAR.writeSlice(valueBuilder, Slices.utf8Slice("value-" + value + "-" + entry));
                    }
                }
            });
        }
        return builder.build();
    }

    private static Block varcharArrayBlock()
    {
        ArrayBlockBuilder builder = VARCHAR_ARRAY.createBlockBuilder(null, POSITION_COUNT);
        for (int position = 0; position < POSITION_COUNT; position++) {
            if (position % 7 == 3) {
                builder.appendNull();
                continue;
            }
            int value = position;
            builder.buildEntry(elements -> writeVarchars(elements, value, value % 4));
        }
        return builder.build();
    }

    private static Block nestedArrayBlock()
    {
        // every non-null outer array has at least one element, so the first element can always be selected
        ArrayBlockBuilder builder = NESTED_ARRAY.createBlockBuilder(null, POSITION_COUNT);
        for (int position = 0; position < POSITION_COUNT; position++) {
            if (position % 7 == 3) {
                builder.appendNull();
                continue;
            }
            int value = position;
            builder.buildEntry(innerArrays -> {
                ArrayBlockBuilder innerArrayBuilder = (ArrayBlockBuilder) innerArrays;
                for (int array = 0; array < (value % 3) + 1; array++) {
                    if ((value + array) % 5 == 4) {
                        innerArrayBuilder.appendNull();
                        continue;
                    }
                    int arrayIndex = array;
                    innerArrayBuilder.buildEntry(elements -> writeVarchars(elements, value * 10 + arrayIndex, (value + arrayIndex) % 3));
                }
            });
        }
        return builder.build();
    }

    private static Block rowBlock()
    {
        RowBlockBuilder builder = ROW.createBlockBuilder(null, POSITION_COUNT);
        for (int position = 0; position < POSITION_COUNT; position++) {
            if (position % 7 == 3) {
                builder.appendNull();
                continue;
            }
            int value = position;
            builder.buildEntry(fields -> {
                BIGINT.writeLong(fields.get(0), value);
                if (value % 4 == 1) {
                    fields.get(1).appendNull();
                }
                else {
                    VARCHAR.writeSlice(fields.get(1), Slices.utf8Slice("value-" + value));
                }
            });
        }
        return builder.build();
    }

    private static void writeVarchars(BlockBuilder builder, int value, int count)
    {
        for (int element = 0; element < count; element++) {
            if ((value + element) % 6 == 5) {
                builder.appendNull();
            }
            else {
                VARCHAR.writeSlice(builder, Slices.utf8Slice("value-" + value + "-" + element));
            }
        }
    }

    private static Block longBlock(IntToLongFunction value)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(POSITION_COUNT);
        for (int position = 0; position < POSITION_COUNT; position++) {
            BIGINT.writeLong(builder, value.applyAsLong(position));
        }
        return builder.build();
    }

    private record Column(String name, Type type, Block block) {}

    private enum Encoding
    {
        FLAT,
        DICTIONARY,
        RLE;

        public Block encode(Block block)
        {
            return switch (this) {
                case FLAT -> block;
                // reverse the positions, so dictionary ids are not sequential
                case DICTIONARY -> DictionaryBlock.create(
                        POSITION_COUNT,
                        block,
                        IntStream.range(0, POSITION_COUNT).map(position -> POSITION_COUNT - 1 - position).toArray());
                case RLE -> RunLengthEncodedBlock.create(block.getSingleValueBlock(2), POSITION_COUNT);
            };
        }
    }

    private static final class PositionsPageFilter
            implements PageFilter
    {
        private final IntPredicate predicate;

        private PositionsPageFilter(IntPredicate predicate)
        {
            this.predicate = predicate;
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
