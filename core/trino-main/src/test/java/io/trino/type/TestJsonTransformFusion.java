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
import io.trino.operator.scalar.JsonPath;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.FunctionType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.IrExpressionOptimizer;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.operator.scalar.ArrayTransformFunction.ARRAY_TRANSFORM_NAME;
import static io.trino.operator.scalar.JsonStringArrayExtractScalar.JSON_STRING_ARRAY_EXTRACT_SCALAR_NAME;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.JsonPathType.JSON_PATH;
import static io.trino.type.JsonType.JSON;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

public class TestJsonTransformFusion
{
    private final TestingFunctionResolution functions = new TestingFunctionResolution();

    @Test
    public void testValidInput()
    {
        assertEquivalent("$", List.of("[]", "null", "[null,1,\"text\",{},[]]"));
        assertEquivalent("$.a", List.of("[{\"a\":1},{\"a\":2}]", "[null,{},1]"));
    }

    @Test
    public void testValueFidelity()
    {
        assertEquivalent("$", List.of("[]", "null", "[null,1,1.20,1e0,-0e0,\"text\",{},[]]"));
        assertEquivalent("$.a", List.of("[{\"a\":1,\"a\":2},{\"a\":3,\"a\":4}]", "[null,{},1]"));
    }

    @Test
    public void testInvalidInput()
    {
        assertEquivalentErrors("$", List.of(
                "",
                " ",
                "[",
                "[1,]",
                "null 1",
                "[] 1",
                "[1] trailing",
                "[1e309]",
                "[0." + "0".repeat(20000) + "1]",
                "[\"\\ud800\"]",
                "[{\"\\udc00\":1}]",
                "\uFEFF[1]",
                "\0[\0001\0]",
                "1",
                "{}",
                "{\"a\":1}",
                "{\"a\":1,}",
                "1 trailing"));
    }

    @Test
    public void testNestingLimit()
    {
        assertEquivalent("$", List.of("[".repeat(1024) + "0" + "]".repeat(1024)));
        assertEquivalentErrors("$", List.of("[".repeat(1025) + "0" + "]".repeat(1025)));
    }

    private void assertEquivalent(String path, List<String> inputs)
    {
        List<String> nullableInputs = new ArrayList<>(inputs);
        nullableInputs.add(null);
        assertThat(compile(path, true).evaluate(nullableInputs))
                .as("fused and unfused extraction at %s", path)
                .isEqualTo(compile(path, false).evaluate(nullableInputs));
    }

    private void assertEquivalentErrors(String path, List<String> inputs)
    {
        Evaluator fused = compile(path, true);
        Evaluator unfused = compile(path, false);
        for (String input : inputs) {
            TrinoException expected = catchThrowableOfType(TrinoException.class, () -> unfused.evaluate(List.of(input)));
            TrinoException actual = catchThrowableOfType(TrinoException.class, () -> fused.evaluate(List.of(input)));
            assertThat(expected).isNotNull();
            assertThat(actual).isNotNull();
            assertThat(actual.getErrorCode()).as("error code for %s to %s", input, path).isEqualTo(expected.getErrorCode());
            assertThat(actual.getMessage()).as("error message for %s to %s", input, path).isEqualTo(expected.getMessage());
        }
    }

    private Evaluator compile(String path, boolean fused)
    {
        Reference input = new Reference(VARCHAR, "x");
        ArrayType jsonArray = new ArrayType(JSON);
        Expression expression = new Call(
                functions.resolveFunction(ARRAY_TRANSFORM_NAME, fromTypes(jsonArray, new FunctionType(List.of(JSON), VARCHAR))),
                List.of(
                        new Cast(new Call(functions.resolveFunction("json_parse", fromTypes(VARCHAR)), List.of(input)), jsonArray),
                        new Lambda(List.of(new Symbol(JSON, "item")),
                                new Call(
                                        functions.resolveFunction("json_extract_scalar", fromTypes(JSON, JSON_PATH)),
                                        List.of(new Reference(JSON, "item"), new Constant(JSON_PATH, new JsonPath(path)))))));
        if (fused) {
            expression = IrExpressionOptimizer.newOptimizer(functions.getPlannerContext())
                    .process(expression, TEST_SESSION, new SymbolAllocator(List.of(new Symbol(VARCHAR, "x"))), Map.of())
                    .orElseThrow();
            assertThat(expression).isInstanceOf(Call.class);
            assertThat(((Call) expression).function().name().functionName())
                    .isEqualTo(JSON_STRING_ARRAY_EXTRACT_SCALAR_NAME);
        }
        PageProcessor processor = functions.getExpressionCompiler()
                .compilePageProcessor(TEST_SESSION, Optional.empty(), List.of(expression), Map.of(new Symbol(VARCHAR, "x"), 0))
                .get();
        return new Evaluator(new ArrayType(VARCHAR), processor);
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
