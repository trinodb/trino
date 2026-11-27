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
import io.trino.jmh.Benchmarks;
import io.trino.metadata.TestingFunctionResolution;
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
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.columnar.FilterEvaluator;
import io.trino.sql.gen.columnar.PageFilterEvaluator;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.function.IntToLongFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.function.OperatorType.SUBSCRIPT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.IrExpressions.call;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(3)
@Warmup(iterations = 5, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 8, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkColumnarScalarFunctions
{
    private static final int POSITION_COUNT = 1024;
    private static final int ELEMENTS_PER_VALUE = 16;
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    @Param({
            "map_keys",
            "map_values",
            "map_entries",
            "map_cardinality",
            "flatten",
            "reverse",
            "trim_array",
            "slice",
            "array_first",
            "array_last",
            "element_at",
            "element_at_nulls",
            "element_at_column",
            "cardinality",
            "cardinality_nulls",
            "concat",
            "concat_nulls",
            "map_element_at",
            "map_element_at_bigint",
            "map_element_at_missing",
            "map_subscript",
            "lower_noop",
            "upper",
            "lower_utf8",
            "lower_mixed",
            "substr",
            "trim",
            "trim_noop",
            "split_part",
            "replace",
            "replace_noop",
            "row_field",
            "row_constructor",
    })
    public String function;

    @Param({"flat", "dictionary", "fresh_dictionary", "rle"})
    public String encoding;

    @Param({"all", "sparse", "very_sparse"})
    public String selection;

    private Page[] inputPages;
    private int nextPage;
    private Type outputType;
    private PageProcessor columnarProcessor;
    private PageProcessor rowProcessor;

    @Setup
    public void setup()
    {
        Type inputType;
        Expression expression;
        Map<Symbol, Integer> layout;
        Supplier<Page> pageSupplier;
        if (ImmutableList.of("flatten", "reverse", "trim_array", "slice", "array_first", "array_last", "element_at", "element_at_nulls", "element_at_column", "cardinality", "cardinality_nulls").contains(function)) {
            ArrayType innerArrayType = new ArrayType(VARCHAR);
            inputType = new ArrayType(innerArrayType);
            Block arrays = createNestedArrayBlock((ArrayType) inputType, function.endsWith("_nulls"));
            pageSupplier = () -> new Page(encode(arrays, encoding));
            layout = ImmutableMap.of(new Symbol(inputType, "input"), 0);
            if (function.equals("element_at_column")) {
                // the index is a second input, so the projection is not evaluated over the dictionary of the arrays
                expression = call(
                        FUNCTIONS.resolveFunction("element_at", fromTypes(inputType, BIGINT)),
                        new Reference(inputType, "input"),
                        new Reference(BIGINT, "index"));
                Block indexes = createLongsBlock(position -> (position % 4) + 1);
                pageSupplier = () -> new Page(encode(arrays, encoding), indexes);
                layout = ImmutableMap.of(new Symbol(inputType, "input"), 0, new Symbol(BIGINT, "index"), 1);
            }
            else if (function.equals("element_at") || function.equals("element_at_nulls") || function.equals("trim_array")) {
                expression = call(
                        FUNCTIONS.resolveFunction(function.equals("trim_array") ? "trim_array" : "element_at", fromTypes(inputType, BIGINT)),
                        new Reference(inputType, "input"),
                        new Constant(BIGINT, 2L));
            }
            else if (function.equals("slice")) {
                expression = call(
                        FUNCTIONS.resolveFunction(function, fromTypes(inputType, BIGINT, BIGINT)),
                        new Reference(inputType, "input"),
                        new Constant(BIGINT, 2L),
                        new Constant(BIGINT, 4L));
            }
            else {
                expression = call(
                        FUNCTIONS.resolveFunction(function.equals("cardinality_nulls") ? "cardinality" : function, fromTypes(inputType)),
                        new Reference(inputType, "input"));
            }
        }
        else if (function.equals("row_field")) {
            inputType = RowType.anonymous(ImmutableList.of(VARCHAR, VARCHAR, VARCHAR, VARCHAR));
            expression = new FieldReference(new Reference(inputType, "input"), 2);
            Block rows = createRowBlock();
            pageSupplier = () -> new Page(encode(rows, encoding));
            layout = ImmutableMap.of(new Symbol(inputType, "input"), 0);
        }
        else if (function.startsWith("map_element_at") || function.equals("map_subscript")) {
            Type valueType = function.equals("map_element_at_bigint") ? BIGINT : VARCHAR;
            inputType = new MapType(VARCHAR, valueType, TYPE_OPERATORS);
            Constant key = new Constant(VARCHAR, Slices.utf8Slice(function.equals("map_element_at_missing") ? "missing" : "key-3"));
            expression = call(
                    function.equals("map_subscript")
                            ? FUNCTIONS.resolveOperator(SUBSCRIPT, ImmutableList.of(inputType, VARCHAR))
                            : FUNCTIONS.resolveFunction("element_at", fromTypes(inputType, VARCHAR)),
                    new Reference(inputType, "input"),
                    key);
            Block maps = createStringKeyMapBlock((MapType) inputType);
            pageSupplier = () -> new Page(encode(maps, encoding));
            layout = ImmutableMap.of(new Symbol(inputType, "input"), 0);
        }
        else if (function.equals("lower_mixed")) {
            inputType = VARCHAR;
            expression = call(FUNCTIONS.resolveFunction("lower", fromTypes(VARCHAR)), new Reference(VARCHAR, "input"));
            // the values have upper case letters, and every eighth value is not ASCII, but none of the sparsely selected values
            Block values = createTextBlock(position -> (position % 8 == 5 ? "Żółw-" : "Field-Value-") + position);
            pageSupplier = () -> new Page(encode(values, encoding));
            layout = ImmutableMap.of(new Symbol(VARCHAR, "input"), 0);
        }
        else if (function.equals("lower_noop") || function.equals("upper") || function.equals("lower_utf8")) {
            inputType = VARCHAR;
            expression = call(
                    FUNCTIONS.resolveFunction(function.equals("upper") ? "upper" : "lower", fromTypes(VARCHAR)),
                    new Reference(VARCHAR, "input"));
            // the values are lower case, so lower returns its input, and every eighth value of the utf8 input is not ASCII
            Block values = createTextBlock(position -> (function.equals("lower_utf8") && position % 8 == 0 ? "Żółw-" : "field-value-") + position);
            pageSupplier = () -> new Page(encode(values, encoding));
            layout = ImmutableMap.of(new Symbol(VARCHAR, "input"), 0);
        }
        else if (function.equals("trim") || function.equals("trim_noop")) {
            inputType = VARCHAR;
            expression = call(FUNCTIONS.resolveFunction("trim", fromTypes(VARCHAR)), new Reference(VARCHAR, "input"));
            // the values of trim_noop have no whitespace to trim, so trim returns its input
            Block values = createTextBlock(position -> function.equals("trim") ? "  field-value-" + position + " " : "field-value-" + position);
            pageSupplier = () -> new Page(encode(values, encoding));
            layout = ImmutableMap.of(new Symbol(VARCHAR, "input"), 0);
        }
        else if (function.equals("replace") || function.equals("replace_noop")) {
            inputType = VARCHAR;
            // the values do not contain the search string of replace_noop, so replace returns its input
            expression = call(
                    FUNCTIONS.resolveFunction("replace", fromTypes(VARCHAR, VARCHAR, VARCHAR)),
                    new Reference(VARCHAR, "input"),
                    new Constant(VARCHAR, Slices.utf8Slice(function.equals("replace") ? "-" : "#")),
                    new Constant(VARCHAR, Slices.utf8Slice("_")));
            Block values = createTextBlock(position -> "field-value-" + position);
            pageSupplier = () -> new Page(encode(values, encoding));
            layout = ImmutableMap.of(new Symbol(VARCHAR, "input"), 0);
        }
        else if (function.equals("split_part")) {
            inputType = VARCHAR;
            expression = call(
                    FUNCTIONS.resolveFunction("split_part", fromTypes(VARCHAR, VARCHAR, BIGINT)),
                    new Reference(VARCHAR, "input"),
                    new Constant(VARCHAR, Slices.utf8Slice("-")),
                    new Constant(BIGINT, 2L));
            Block values = createTextBlock(position -> "field-value-" + position);
            pageSupplier = () -> new Page(encode(values, encoding));
            layout = ImmutableMap.of(new Symbol(VARCHAR, "input"), 0);
        }
        else if (function.equals("substr")) {
            inputType = VARCHAR;
            expression = call(
                    FUNCTIONS.resolveFunction("substr", fromTypes(VARCHAR, BIGINT, BIGINT)),
                    new Reference(VARCHAR, "input"),
                    new Constant(BIGINT, 7L),
                    new Constant(BIGINT, 5L));
            Block values = createTextBlock(position -> "field-value-" + position);
            pageSupplier = () -> new Page(encode(values, encoding));
            layout = ImmutableMap.of(new Symbol(VARCHAR, "input"), 0);
        }
        else if (function.equals("concat") || function.equals("concat_nulls")) {
            inputType = VARCHAR;
            expression = call(
                    FUNCTIONS.resolveFunction("concat", fromTypes(VARCHAR, VARCHAR, VARCHAR)),
                    new Reference(VARCHAR, "field0"),
                    new Constant(VARCHAR, Slices.utf8Slice(" ")),
                    new Reference(VARCHAR, "field1"));
            Block first = createVarcharBlock(0);
            Block second = function.equals("concat_nulls") ? createVarcharBlockWithNulls(1) : createVarcharBlock(1);
            pageSupplier = () -> new Page(encode(first, encoding), encode(second, encoding));
            layout = ImmutableMap.of(new Symbol(VARCHAR, "field0"), 0, new Symbol(VARCHAR, "field1"), 1);
        }
        else if (function.equals("row_constructor")) {
            inputType = VARCHAR;
            ImmutableList.Builder<Expression> fields = ImmutableList.builder();
            ImmutableMap.Builder<Symbol, Integer> layoutBuilder = ImmutableMap.builder();
            Block[] blocks = new Block[4];
            for (int field = 0; field < blocks.length; field++) {
                String name = "field" + field;
                fields.add(new Reference(VARCHAR, name));
                layoutBuilder.put(new Symbol(VARCHAR, name), field);
                blocks[field] = createVarcharBlock(field);
            }
            expression = new Row(fields.build());
            pageSupplier = () -> new Page(Arrays.stream(blocks).map(block -> encode(block, encoding)).toArray(Block[]::new));
            layout = layoutBuilder.buildOrThrow();
        }
        else {
            inputType = new MapType(BIGINT, VARCHAR, TYPE_OPERATORS);
            expression = call(
                    FUNCTIONS.resolveFunction(function.equals("map_cardinality") ? "cardinality" : function, fromTypes(inputType)),
                    new Reference(inputType, "input"));
            Block maps = createMapBlock((MapType) inputType);
            pageSupplier = () -> new Page(encode(maps, encoding));
            layout = ImmutableMap.of(new Symbol(inputType, "input"), 0);
        }

        // every page of a fresh dictionary encoding has its own dictionary, so the projection of a dictionary cannot be reused
        inputPages = Stream.generate(pageSupplier)
                .limit(encoding.equals("fresh_dictionary") ? 16 : 1)
                .toArray(Page[]::new);
        nextPage = 0;

        PageFunctionCompiler compiler = FUNCTIONS.getPageFunctionCompiler();
        outputType = expression.type();
        PageProjection columnar = compiler.compileProjection(expression, layout, SQL_STANDARD, Optional.empty(), true).get();
        PageProjection row = compiler.compileProjection(expression, layout, SQL_STANDARD, Optional.empty(), false).get();
        Optional<FilterEvaluator> filter = switch (selection) {
            case "all" -> Optional.empty();
            case "sparse" -> Optional.of(new PageFilterEvaluator(new SparsePageFilter(POSITION_COUNT, 8)));
            case "very_sparse" -> Optional.of(new PageFilterEvaluator(new SparsePageFilter(POSITION_COUNT, 64)));
            default -> throw new IllegalArgumentException("Unknown selection: " + selection);
        };
        columnarProcessor = new PageProcessor(filter, Optional.empty(), ImmutableList.of(columnar), OptionalInt.of(POSITION_COUNT));
        rowProcessor = new PageProcessor(filter, Optional.empty(), ImmutableList.of(row), OptionalInt.of(POSITION_COUNT));
    }

    @Benchmark
    public void columnar(Blackhole blackhole)
    {
        blackhole.consume(process(columnarProcessor));
    }

    @Benchmark
    public void rowOriented(Blackhole blackhole)
    {
        blackhole.consume(process(rowProcessor));
    }

    private List<Optional<Page>> process(PageProcessor processor)
    {
        Page page = inputPages[nextPage];
        nextPage = (nextPage + 1) % inputPages.length;
        return ImmutableList.copyOf(processor.process(
                SESSION,
                newSimpleAggregatedMemoryContext().newLocalMemoryContext("benchmark"),
                SourcePage.create(page)));
    }

    private static Block createMapBlock(MapType mapType)
    {
        int positions = POSITION_COUNT;
        MapBlockBuilder builder = mapType.createBlockBuilder(null, positions);
        for (int position = 0; position < positions; position++) {
            int value = position;
            builder.buildEntry((keyBuilder, valueBuilder) -> {
                for (int element = 0; element < ELEMENTS_PER_VALUE; element++) {
                    BIGINT.writeLong(keyBuilder, value * ELEMENTS_PER_VALUE + element);
                    VARCHAR.writeSlice(valueBuilder, Slices.utf8Slice("value-" + value + "-" + element));
                }
            });
        }
        return builder.build();
    }

    private static Block createStringKeyMapBlock(MapType mapType)
    {
        MapBlockBuilder builder = mapType.createBlockBuilder(null, POSITION_COUNT);
        for (int position = 0; position < POSITION_COUNT; position++) {
            int value = position;
            builder.buildEntry((keyBuilder, valueBuilder) -> {
                for (int element = 0; element < ELEMENTS_PER_VALUE; element++) {
                    VARCHAR.writeSlice(keyBuilder, Slices.utf8Slice("key-" + element));
                    if (mapType.getValueType() == BIGINT) {
                        BIGINT.writeLong(valueBuilder, value * ELEMENTS_PER_VALUE + element);
                    }
                    else {
                        VARCHAR.writeSlice(valueBuilder, Slices.utf8Slice("value-" + value + "-" + element));
                    }
                }
            });
        }
        return builder.build();
    }

    private static Block createNestedArrayBlock(ArrayType outerArrayType, boolean withNulls)
    {
        ArrayBlockBuilder builder = outerArrayType.createBlockBuilder(null, POSITION_COUNT);
        for (int position = 0; position < POSITION_COUNT; position++) {
            if (withNulls && position % 7 == 3) {
                builder.appendNull();
                continue;
            }
            int value = position;
            builder.buildEntry(innerArrays -> {
                ArrayBlockBuilder innerArrayBuilder = (ArrayBlockBuilder) innerArrays;
                for (int array = 0; array < 4; array++) {
                    int arrayIndex = array;
                    innerArrayBuilder.buildEntry(elements -> {
                        for (int element = 0; element < ELEMENTS_PER_VALUE / 4; element++) {
                            VARCHAR.writeSlice(elements, Slices.utf8Slice("value-" + value + "-" + arrayIndex + "-" + element));
                        }
                    });
                }
            });
        }
        return builder.build();
    }

    private static Block createLongsBlock(IntToLongFunction value)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(POSITION_COUNT);
        for (int position = 0; position < POSITION_COUNT; position++) {
            BIGINT.writeLong(builder, value.applyAsLong(position));
        }
        return builder.build();
    }

    private static Block createRowBlock()
    {
        Block[] fields = new Block[4];
        for (int field = 0; field < fields.length; field++) {
            fields[field] = createVarcharBlock(field);
        }
        return RowBlock.fromFieldBlocks(POSITION_COUNT, fields);
    }

    private static Block createVarcharBlock(int field)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, POSITION_COUNT);
        for (int position = 0; position < POSITION_COUNT; position++) {
            VARCHAR.writeSlice(builder, Slices.utf8Slice("field-" + field + "-value-" + position));
        }
        return builder.build();
    }

    private static Block createTextBlock(IntFunction<String> value)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, POSITION_COUNT);
        for (int position = 0; position < POSITION_COUNT; position++) {
            VARCHAR.writeSlice(builder, Slices.utf8Slice(value.apply(position)));
        }
        return builder.build();
    }

    private static Block createVarcharBlockWithNulls(int field)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, POSITION_COUNT);
        for (int position = 0; position < POSITION_COUNT; position++) {
            if (position % 7 == 3) {
                builder.appendNull();
            }
            else {
                VARCHAR.writeSlice(builder, Slices.utf8Slice("field-" + field + "-value-" + position));
            }
        }
        return builder.build();
    }

    private static final class SparsePageFilter
            implements PageFilter
    {
        private final SelectedPositions selectedPositions;

        private SparsePageFilter(int positionCount, int step)
        {
            int[] positions = new int[positionCount / step];
            for (int index = 0; index < positions.length; index++) {
                positions[index] = index * step;
            }
            selectedPositions = SelectedPositions.positionsList(positions, 0, positions.length);
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
            return selectedPositions;
        }
    }

    private static Block encode(Block block, String encoding)
    {
        return switch (encoding) {
            case "flat" -> block;
            case "dictionary", "fresh_dictionary" -> {
                int[] ids = new int[POSITION_COUNT];
                for (int position = 0; position < ids.length; position++) {
                    ids[position] = position % 128;
                }
                yield DictionaryBlock.create(POSITION_COUNT, block.getRegion(0, 128), ids);
            }
            case "rle" -> RunLengthEncodedBlock.create(block.getSingleValueBlock(0), POSITION_COUNT);
            default -> throw new IllegalArgumentException("Unknown encoding: " + encoding);
        };
    }

    @Test
    public void testBenchmark()
    {
        for (String function : ImmutableList.of("map_keys", "map_values", "map_entries", "map_cardinality", "flatten", "reverse", "trim_array", "slice", "array_first", "array_last", "element_at", "element_at_nulls", "element_at_column", "cardinality", "cardinality_nulls", "concat", "concat_nulls", "map_element_at", "map_element_at_bigint", "map_element_at_missing", "map_subscript", "lower_noop", "upper", "lower_utf8", "lower_mixed", "substr", "trim", "trim_noop", "split_part", "replace", "replace_noop", "row_field", "row_constructor")) {
            for (String encoding : ImmutableList.of("flat", "dictionary", "fresh_dictionary", "rle")) {
                for (String selection : ImmutableList.of("all", "sparse", "very_sparse")) {
                    this.function = function;
                    this.encoding = encoding;
                    this.selection = selection;
                    setup();
                    Page columnarResult = process(columnarProcessor).getFirst().orElseThrow();
                    Page rowResult = process(rowProcessor).getFirst().orElseThrow();
                    assertBlockEquals(outputType, columnarResult.getBlock(0), rowResult.getBlock(0));
                    columnar(new Blackhole("Today's password is swordfish. I understand instantiating Blackholes directly is dangerous."));
                    rowOriented(new Blackhole("Today's password is swordfish. I understand instantiating Blackholes directly is dangerous."));
                }
            }
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Benchmarks.benchmark(BenchmarkColumnarScalarFunctions.class).run();
    }
}
