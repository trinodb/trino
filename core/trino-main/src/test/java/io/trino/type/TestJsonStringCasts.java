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
package io.trino.type;

import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.IrExpressionOptimizer;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.operator.scalar.JsonStringToArrayCast.JSON_STRING_TO_ARRAY_NAME;
import static io.trino.operator.scalar.JsonStringToMapCast.JSON_STRING_TO_MAP_NAME;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.JsonType.JSON;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

public class TestJsonStringCasts
{
    private final TestingFunctionResolution functions = new TestingFunctionResolution();

    @Test
    public void testScalarConversions()
    {
        for (Type type : List.of(BIGINT, INTEGER, SMALLINT, TINYINT, DOUBLE, REAL, createDecimalType(5, 2), createDecimalType(30, 2), NUMBER, VARCHAR)) {
            assertEquivalent(new ArrayType(type), List.of("[]", "null", "[null,0,1,12.30,1.23E1,-0,-0.0,-0e0,\"1\"]"));
            assertEquivalent(mapType(type), List.of("{}", "null", "{\"a\":null,\"b\":12.30,\"c\":1.23E1,\"d\":-0e0,\"e\":\"1\"}"));
        }
        assertEquivalent(new ArrayType(BOOLEAN), List.of("[true,false,null,0,1,-1,1e300,\"true\"]"));
        assertEquivalent(mapType(BOOLEAN), List.of("{\"a\":true,\"b\":false,\"c\":null,\"d\":0,\"e\":1}"));
        assertEquivalent(new ArrayType(BIGINT), List.of("[1234567890123456789.0,9223372036854775807,-9223372036854775808]"));
        assertEquivalent(new ArrayType(VARCHAR), List.of("[12.30,1.23E1,12345678901234567890123456789012345678901,\"a\\nb\",\"雪\"]"));
        assertEquivalent(new ArrayType(JSON), List.of("[12.30,1.23E1,{\"a\":1,\"a\":2},[null,\"x\"]]"));
        assertEquivalent(mapType(new ArrayType(INTEGER)), List.of("{\"a\":[1,2,null],\"b\":[],\"c\":null}"));
        assertEquivalent(new ArrayType(mapType(JSON)), List.of("[{\"a\":12.30,\"b\":[1,2]},null,{}]"));
    }

    @Test
    public void testFloatingPointConversions()
    {
        List<String> arrays = new ArrayList<>();
        List<String> maps = new ArrayList<>();
        Random random = new Random(17);
        for (int i = 0; i < 1000; i++) {
            String decimal = BigDecimal.valueOf(random.nextLong(), random.nextInt(25)).toPlainString();
            String approximate = BigDecimal.valueOf(random.nextDouble()).toPlainString() + "e0";
            String integer = Long.toString(random.nextLong());
            arrays.add("[" + decimal + "," + approximate + "," + integer + "]");
            maps.add("{\"decimal\":" + decimal + ",\"approximate\":" + approximate + ",\"integer\":" + integer + "}");
        }
        arrays.add("[-0,-0.0,-0e0,-1e-999,1e300,-1e300,4611686293305294849,12345678901234567890123456789012345678901]");
        arrays.add("[\"NaN\",\"Infinity\",\"-Infinity\"]");
        for (Type type : List.of(DOUBLE, REAL)) {
            assertEquivalent(new ArrayType(type), arrays);
            assertEquivalent(mapType(type), maps);
        }
    }

    @Test
    public void testErrors()
    {
        assertEquivalentErrors(new ArrayType(INTEGER), List.of(
                "",
                " ",
                "[",
                "{}",
                "1",
                "[1,]",
                "[1] 2",
                "null 1",
                "[NaN]",
                "[\"x\"]",
                "[2147483648]",
                "[ 2147483648 ]",
                "[2147483648,]",
                "[\"x\",]",
                "[1e999]",
                "[1e999,]",
                "[{}]"));
        assertEquivalentErrors(mapType(INTEGER), List.of(
                "",
                "{",
                "[]",
                "1",
                "null 1",
                "{\"a\":1} 2",
                "{\"a\":1,}",
                "{\"a\":\"x\"}",
                "{\"a\":2147483648}",
                "{\"a\":2147483648,}",
                "{\"a\":\"x\",}",
                "{\"a\":1,\"a\":2}",
                "{\"a\" : 1 , \"a\" : 2}"));
        assertEquivalentErrors(new MapType(INTEGER, INTEGER, functions.getPlannerContext().getTypeOperators()), List.of(
                "{\"x\":1}", "{\"x\":1,}", "{\"1\":1,\"01\":2}"));
        assertEquivalentErrors(new ArrayType(SMALLINT), List.of("[32768]", "[-32769]", "[123456]", "[123456,]"));
    }

    @Test
    public void testInputValidation()
    {
        assertEquivalentErrors(new ArrayType(DOUBLE), List.of(
                "[1e400]",
                "[0." + "0".repeat(20000) + "1]",
                "\uFEFF[1]",
                "\0[\0001\0]"));
        assertEquivalentErrors(new ArrayType(VARCHAR), List.of("[\"\\ud800\"]", "[\"\\udc00\"]"));
        assertEquivalentErrors(mapType(DOUBLE), List.of("{\"\\ud800\":1}", "{\"a\":1e400}"));
        assertEquivalent(new ArrayType(VARCHAR), List.of("[\"\\ud83d\\ude00\"]"));
        assertEquivalent(mapType(DOUBLE), List.of("{\"\\ud83d\\ude00\":1.25}"));
    }

    @Test
    public void testNestingLimit()
    {
        assertEquivalent(new ArrayType(JSON), List.of("[".repeat(1024) + "0" + "]".repeat(1024)));
        assertEquivalentErrors(new ArrayType(JSON), List.of("[".repeat(1025) + "0" + "]".repeat(1025)));

        TrinoException failure = catchThrowableOfType(TrinoException.class, () -> compile(new ArrayType(INTEGER), true).evaluate(List.of("[2147483648,]")));
        assertThat(failure).isNotNull();
        assertThat(failure.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode());
    }

    private MapType mapType(Type valueType)
    {
        return new MapType(VARCHAR, valueType, functions.getPlannerContext().getTypeOperators());
    }

    private void assertEquivalent(Type type, List<String> inputs)
    {
        List<String> nullableInputs = new ArrayList<>(inputs);
        nullableInputs.add(null);
        assertThat(compile(type, true).evaluate(nullableInputs))
                .as("fused and unfused casts to %s", type)
                .isEqualTo(compile(type, false).evaluate(nullableInputs));
    }

    private void assertEquivalentErrors(Type type, List<String> inputs)
    {
        Evaluator fused = compile(type, true);
        Evaluator unfused = compile(type, false);
        for (String input : inputs) {
            TrinoException expected = catchThrowableOfType(TrinoException.class, () -> unfused.evaluate(List.of(input)));
            TrinoException actual = catchThrowableOfType(TrinoException.class, () -> fused.evaluate(List.of(input)));
            assertThat(expected).isNotNull();
            assertThat(actual).isNotNull();
            assertThat(actual.getErrorCode()).as("error code for %s to %s", input, type).isEqualTo(expected.getErrorCode());
            assertThat(actual.getMessage()).as("error message for %s to %s", input, type).isEqualTo(expected.getMessage());
        }
    }

    private Evaluator compile(Type type, boolean fused)
    {
        Reference input = new Reference(VARCHAR, "x");
        Expression expression = new Cast(new Call(functions.resolveFunction("json_parse", fromTypes(VARCHAR)), List.of(input)), type);
        if (fused) {
            expression = IrExpressionOptimizer.newOptimizer(functions.getPlannerContext())
                    .process(expression, TEST_SESSION, new SymbolAllocator(List.of(new Symbol(VARCHAR, "x"))), Map.of())
                    .orElseThrow();
            assertThat(expression).isInstanceOf(Call.class);
            assertThat(((Call) expression).function().name().functionName())
                    .isEqualTo(type instanceof ArrayType ? JSON_STRING_TO_ARRAY_NAME : JSON_STRING_TO_MAP_NAME);
        }
        PageProcessor processor = functions.getExpressionCompiler()
                .compilePageProcessor(TEST_SESSION, Optional.empty(), List.of(expression), Map.of(new Symbol(VARCHAR, "x"), 0))
                .get();
        return new Evaluator(type, processor);
    }

    private record Evaluator(Type type, PageProcessor processor)
    {
        public List<Object> evaluate(List<String> inputs)
        {
            BlockBuilder builder = VARCHAR.createBlockBuilder(null, inputs.size());
            for (String input : inputs) {
                if (input == null) {
                    builder.appendNull();
                }
                else {
                    VARCHAR.writeSlice(builder, utf8Slice(input));
                }
            }
            List<Object> results = new ArrayList<>();
            var memory = newSimpleAggregatedMemoryContext();
            try {
                var pages = processor.process(SESSION, memory.newLocalMemoryContext("json casts"), SourcePage.create(new Page(builder.build())));
                while (pages.hasNext()) {
                    Optional<Page> output = pages.next();
                    if (output.isPresent()) {
                        Page page = output.orElseThrow();
                        for (int position = 0; position < page.getPositionCount(); position++) {
                            results.add(type.getObjectValue(page.getBlock(0), position));
                        }
                    }
                }
            }
            finally {
                memory.close();
            }
            assertThat(results).hasSize(inputs.size());
            return results;
        }
    }
}
