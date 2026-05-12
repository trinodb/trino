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
import io.airlift.slice.SliceOutput;
import io.trino.FullConnectorSession;
import io.trino.jmh.Benchmarks;
import io.trino.jsonpath.ir.IrContextVariable;
import io.trino.jsonpath.ir.IrJsonPath;
import io.trino.jsonpath.ir.IrMemberAccessor;
import io.trino.jsonpath.ir.IrPathNode;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.FunctionType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.testing.TestingSession;
import io.trino.type.SqlJsonPathType;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.options.WarmupMode;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.operator.scalar.json.JsonInputFunctions.VARCHAR_TO_JSON;
import static io.trino.operator.scalar.json.JsonQueryFunction.JSON_QUERY_FUNCTION_NAME;
import static io.trino.operator.scalar.json.JsonValueFunction.JSON_VALUE_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.ExpressionAnalyzer.JSON_NO_PARAMETERS_ROW_TYPE;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.IrExpressions.call;
import static io.trino.sql.ir.IrExpressions.constantNull;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.JsonPathType.JSON_PATH;
import static io.trino.type.JsonType.JSON;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Benchmark new vs old JSON functions.
 * `json_extract` and `json_extract_scalar` are JSON-processing functions which have very limited capabilities, and are not compliant with the spec.
 * However, they have simple lightweight implementation, optimized for the use case.
 * `json_query` and `json_value` are new spec-compliant JSON-processing functions, which support the complete specification for JSON path.
 * Their implementation is much more complicated, resulting both from the scope of the supported feature, and the "streaming" semantics.
 * This benchmark is to compare both implementations applied to the common use-case.
 * <p>
 * Compare: `benchmarkJsonValueFunction` vs `benchmarkJsonExtractScalarFunction`
 * and `benchmarkJsonQueryFunction` vs `benchmarkJsonExtractFunction`.
 */
@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkJsonFunctions
{
    private static final int POSITION_COUNT = 100_000;
    private static final FullConnectorSession FULL_CONNECTOR_SESSION = new FullConnectorSession(TestingSession.testSessionBuilder().build(), ConnectorIdentity.ofUser("test"));

    @Benchmark
    @OperationsPerInvocation(POSITION_COUNT)
    public List<Optional<Page>> benchmarkJsonValueFunction(BenchmarkData data)
    {
        return ImmutableList.copyOf(
                data.getJsonValuePageProcessor().process(
                        FULL_CONNECTOR_SESSION,
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        SourcePage.create(data.getPage())));
    }

    @Benchmark
    @OperationsPerInvocation(POSITION_COUNT)
    public List<Optional<Page>> benchmarkJsonExtractScalarFunction(BenchmarkData data)
    {
        return ImmutableList.copyOf(
                data.getJsonExtractScalarPageProcessor().process(
                        FULL_CONNECTOR_SESSION,
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        SourcePage.create(data.getPage())));
    }

    @Benchmark
    @OperationsPerInvocation(POSITION_COUNT)
    public List<Optional<Page>> benchmarkJsonQueryFunction(BenchmarkData data)
    {
        return ImmutableList.copyOf(
                data.getJsonQueryPageProcessor().process(
                        FULL_CONNECTOR_SESSION,
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        SourcePage.create(data.getPage())));
    }

    @Benchmark
    @OperationsPerInvocation(POSITION_COUNT)
    public List<Optional<Page>> benchmarkJsonExtractFunction(BenchmarkData data)
    {
        return ImmutableList.copyOf(
                data.getJsonExtractPageProcessor().process(
                        SESSION,
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        SourcePage.create(data.getPage())));
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"1", "3", "10"})
        int depth;

        /// Number of keys per object level. The path engine looks up "key{depth}" at each
        /// level; the remaining width-1 keys are filler that exercise lookup cost. Width
        /// >= INDEXED_CONTAINER_THRESHOLD (8) puts the encoded objects in OBJECT_INDEXED
        /// form, where member lookup is O(log n) via binary search.
        @Param({"1", "8", "32"})
        int width;

        private Page page;
        private PageProcessor jsonValuePageProcessor;
        private PageProcessor jsonExtractScalarPageProcessor;
        private PageProcessor jsonQueryPageProcessor;
        private PageProcessor jsonExtractPageProcessor;

        @Setup
        public void setup()
        {
            page = new Page(createChannel(POSITION_COUNT, depth, width));

            TestingFunctionResolution functionResolution = new TestingFunctionResolution();
            Type jsonPath2016Type = PLANNER_CONTEXT.getTypeManager().getType(TypeId.of(SqlJsonPathType.NAME));

            jsonValuePageProcessor = createJsonValuePageProcessor(depth, functionResolution, jsonPath2016Type);
            jsonExtractScalarPageProcessor = createJsonExtractScalarPageProcessor(depth, functionResolution);
            jsonQueryPageProcessor = createJsonQueryPageProcessor(depth, functionResolution, jsonPath2016Type);
            jsonExtractPageProcessor = createJsonExtractPageProcessor(depth, functionResolution);
        }

        private static PageProcessor createJsonValuePageProcessor(int depth, TestingFunctionResolution functionResolution, Type jsonPath2016Type)
        {
            IrPathNode pathRoot = new IrContextVariable(Optional.empty());
            for (int i = 1; i <= depth; i++) {
                pathRoot = new IrMemberAccessor(pathRoot, Optional.of("key" + i), Optional.empty());
            }
            List<Expression> jsonValueProjection = ImmutableList.of(new Call(
                    functionResolution.resolveFunction(
                            JSON_VALUE_FUNCTION_NAME,
                            fromTypes(ImmutableList.of(
                                    JSON,
                                    jsonPath2016Type,
                                    JSON_NO_PARAMETERS_ROW_TYPE,
                                    VARCHAR,
                                    TINYINT,
                                    new FunctionType(ImmutableList.of(), VARCHAR),
                                    TINYINT,
                                    new FunctionType(ImmutableList.of(), VARCHAR)))),
                    ImmutableList.of(
                            call(functionResolution.resolveFunction(VARCHAR_TO_JSON, fromTypes(VARCHAR, BOOLEAN)),
                                    new Reference(VARCHAR, "$col_0"),
                                    new Constant(BOOLEAN, true)),
                            new Constant(jsonPath2016Type, new IrJsonPath(false, pathRoot)),
                            constantNull(JSON_NO_PARAMETERS_ROW_TYPE),
                            constantNull(VARCHAR),
                            new Constant(TINYINT, 0L),
                            new Lambda(ImmutableList.of(), constantNull(VARCHAR)),
                            new Constant(TINYINT, 0L),
                            new Lambda(ImmutableList.of(), constantNull(VARCHAR)))));

            return functionResolution.getExpressionCompiler()
                    .compilePageProcessor(Optional.empty(), jsonValueProjection, ImmutableMap.of(new Symbol(VARCHAR, "$col_0"), 0))
                    .get();
        }

        private static PageProcessor createJsonExtractScalarPageProcessor(int depth, TestingFunctionResolution functionResolution)
        {
            StringBuilder pathString = new StringBuilder("$");
            for (int i = 1; i <= depth; i++) {
                pathString
                        .append(".key")
                        .append(i);
            }
            Type boundedVarcharType = createVarcharType(100);
            List<Expression> jsonExtractScalarProjection = ImmutableList.of(call(
                    functionResolution.resolveFunction("json_extract_scalar", fromTypes(ImmutableList.of(boundedVarcharType, JSON_PATH))),
                    new Reference(boundedVarcharType, "$col_0"),
                    new Constant(JSON_PATH, new JsonPath(pathString.toString()))));

            return functionResolution.getExpressionCompiler()
                    .compilePageProcessor(Optional.empty(), jsonExtractScalarProjection, ImmutableMap.of(new Symbol(boundedVarcharType, "$col_0"), 0))
                    .get();
        }

        private static PageProcessor createJsonQueryPageProcessor(int depth, TestingFunctionResolution functionResolution, Type jsonPath2016Type)
        {
            IrPathNode pathRoot = new IrContextVariable(Optional.empty());
            for (int i = 1; i <= depth - 1; i++) {
                pathRoot = new IrMemberAccessor(pathRoot, Optional.of("key" + i), Optional.empty());
            }
            List<Expression> jsonQueryProjection = ImmutableList.of(new Call(
                    functionResolution.resolveFunction(
                            JSON_QUERY_FUNCTION_NAME,
                            fromTypes(ImmutableList.of(
                                    JSON,
                                    jsonPath2016Type,
                                    JSON_NO_PARAMETERS_ROW_TYPE,
                                    TINYINT,
                                    TINYINT,
                                    TINYINT))),
                    ImmutableList.of(
                            call(functionResolution.resolveFunction(VARCHAR_TO_JSON, fromTypes(VARCHAR, BOOLEAN)),
                                    new Reference(VARCHAR, "$col_0"),
                                    new Constant(BOOLEAN, true)),
                            new Constant(jsonPath2016Type, new IrJsonPath(false, pathRoot)),
                            constantNull(JSON_NO_PARAMETERS_ROW_TYPE),
                            new Constant(TINYINT, 0L),
                            new Constant(TINYINT, 0L),
                            new Constant(TINYINT, 0L))));

            return functionResolution.getExpressionCompiler()
                    .compilePageProcessor(Optional.empty(), jsonQueryProjection, ImmutableMap.of(new Symbol(VARCHAR, "$col_0"), 0))
                    .get();
        }

        private static PageProcessor createJsonExtractPageProcessor(int depth, TestingFunctionResolution functionResolution)
        {
            StringBuilder pathString = new StringBuilder("$");
            for (int i = 1; i <= depth - 1; i++) {
                pathString
                        .append(".key")
                        .append(i);
            }
            Type boundedVarcharType = createVarcharType(100);
            List<Expression> jsonExtractScalarProjection = ImmutableList.of(call(
                    functionResolution.resolveFunction("json_extract", fromTypes(ImmutableList.of(boundedVarcharType, JSON_PATH))),
                    new Reference(boundedVarcharType, "$col_0"),
                    new Constant(JSON_PATH, new JsonPath(pathString.toString()))));

            return functionResolution.getExpressionCompiler()
                    .compilePageProcessor(Optional.empty(), jsonExtractScalarProjection, ImmutableMap.of(new Symbol(boundedVarcharType, "$col_0"), 0))
                    .get();
        }

        private static Block createChannel(int positionCount, int depth, int width)
        {
            BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, positionCount);
            for (int position = 0; position < positionCount; position++) {
                Slice jsonText = generateNestedObject(depth, width);
                VARCHAR.writeSlice(blockBuilder, jsonText);
            }
            return blockBuilder.build();
        }

        /// Generates a chain of `depth` nested objects, each with `width` keys. At every
        /// level, "key{level}" is the one followed by the path; the remaining keys are
        /// filler that the path-engine member lookup must walk past.
        private static Slice generateNestedObject(int depth, int width)
        {
            SliceOutput slice = new DynamicSliceOutput(64);
            buildLevel(slice, 1, depth, width);
            return slice.slice();
        }

        private static void buildLevel(SliceOutput slice, int level, int depth, int width)
        {
            slice.appendByte('{');
            // Filler keys before the path target so the lookup walks through them.
            int targetIndex = ThreadLocalRandom.current().nextInt(width);
            for (int i = 0; i < width; i++) {
                if (i > 0) {
                    slice.appendByte(',');
                }
                if (i == targetIndex) {
                    slice.appendBytes(("\"key" + level + "\":").getBytes(UTF_8));
                    if (level < depth) {
                        buildLevel(slice, level + 1, depth, width);
                    }
                    else {
                        slice.appendBytes(generateRandomJsonText().getBytes(UTF_8));
                    }
                }
                else {
                    slice.appendBytes(("\"filler" + i + "\":\"x\"").getBytes(UTF_8));
                }
            }
            slice.appendByte('}');
        }

        private static String generateRandomJsonText()
        {
            String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";

            int length = ThreadLocalRandom.current().nextInt(10) + 1;
            StringBuilder builder = new StringBuilder(length + 2);
            builder.append('"');
            for (int i = 0; i < length; i++) {
                builder.append(characters.charAt(ThreadLocalRandom.current().nextInt(characters.length())));
            }
            builder.append('"');
            return builder.toString();
        }

        public PageProcessor getJsonValuePageProcessor()
        {
            return jsonValuePageProcessor;
        }

        public PageProcessor getJsonExtractScalarPageProcessor()
        {
            return jsonExtractScalarPageProcessor;
        }

        public PageProcessor getJsonQueryPageProcessor()
        {
            return jsonQueryPageProcessor;
        }

        public PageProcessor getJsonExtractPageProcessor()
        {
            return jsonExtractPageProcessor;
        }

        public Page getPage()
        {
            return page;
        }
    }

    @Test
    public void verify()
    {
        BenchmarkData data = new BenchmarkData();
        data.depth = 3;
        data.width = 8;
        data.setup();
        new BenchmarkJsonFunctions().benchmarkJsonValueFunction(data);
        new BenchmarkJsonFunctions().benchmarkJsonExtractScalarFunction(data);
        new BenchmarkJsonFunctions().benchmarkJsonQueryFunction(data);
        new BenchmarkJsonFunctions().benchmarkJsonExtractFunction(data);
    }

    static void main()
            throws Exception
    {
        Benchmarks.benchmark(BenchmarkJsonFunctions.class, WarmupMode.BULK_INDI).run();
    }
}
