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
import io.airlift.slice.SliceOutput;
import io.trino.FullConnectorSession;
import io.trino.jmh.Benchmarks;
import io.trino.json.ir.IrContextVariable;
import io.trino.json.ir.IrJsonPath;
import io.trino.json.ir.IrMemberAccessor;
import io.trino.json.ir.IrPathNode;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.tree.QualifiedName;
import io.trino.testing.TestingSession;
import io.trino.type.JsonPath2016Type;
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
import org.testng.annotations.Test;

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
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.Expressions.constantNull;
import static io.trino.sql.relational.Expressions.field;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.Json2016Type.JSON_2016;
import static io.trino.type.JsonPathType.JSON_PATH;
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
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        data.getPage()));
    }

    @Benchmark
    @OperationsPerInvocation(POSITION_COUNT)
    public List<Optional<Page>> benchmarkJsonExtractScalarFunction(BenchmarkData data)
    {
        return ImmutableList.copyOf(
                data.getJsonExtractScalarPageProcessor().process(
                        FULL_CONNECTOR_SESSION,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        data.getPage()));
    }

    @Benchmark
    @OperationsPerInvocation(POSITION_COUNT)
    public List<Optional<Page>> benchmarkJsonQueryFunction(BenchmarkData data)
    {
        return ImmutableList.copyOf(
                data.getJsonQueryPageProcessor().process(
                        FULL_CONNECTOR_SESSION,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        data.getPage()));
    }

    @Benchmark
    @OperationsPerInvocation(POSITION_COUNT)
    public List<Optional<Page>> benchmarkJsonExtractFunction(BenchmarkData data)
    {
        return ImmutableList.copyOf(
                data.getJsonExtractPageProcessor().process(
                        SESSION,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        data.getPage()));
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"1", "3", "10"})
        private int depth;

        private Page page;
        private PageProcessor jsonValuePageProcessor;
        private PageProcessor jsonExtractScalarPageProcessor;
        private PageProcessor jsonQueryPageProcessor;
        private PageProcessor jsonExtractPageProcessor;

        @Setup
        public void setup()
        {
            page = new Page(createChannel(POSITION_COUNT, depth));

            TestingFunctionResolution functionResolution = new TestingFunctionResolution();
            Type jsonPath2016Type = PLANNER_CONTEXT.getTypeManager().getType(TypeId.of(JsonPath2016Type.NAME));

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
            List<RowExpression> jsonValueProjection = ImmutableList.of(new CallExpression(
                    functionResolution.resolveFunction(
                            QualifiedName.of(JSON_VALUE_FUNCTION_NAME),
                            fromTypes(ImmutableList.of(
                                    JSON_2016,
                                    jsonPath2016Type,
                                    JSON_NO_PARAMETERS_ROW_TYPE,
                                    TINYINT,
                                    VARCHAR,
                                    TINYINT,
                                    VARCHAR))),
                    ImmutableList.of(
                            new CallExpression(
                                    functionResolution.resolveFunction(QualifiedName.of(VARCHAR_TO_JSON), fromTypes(VARCHAR, BOOLEAN)),
                                    ImmutableList.of(field(0, VARCHAR), constant(true, BOOLEAN))),
                            constant(new IrJsonPath(false, pathRoot), jsonPath2016Type),
                            constantNull(JSON_NO_PARAMETERS_ROW_TYPE),
                            constant(0L, TINYINT),
                            constantNull(VARCHAR),
                            constant(0L, TINYINT),
                            constantNull(VARCHAR))));

            return functionResolution.getExpressionCompiler()
                    .compilePageProcessor(Optional.empty(), jsonValueProjection)
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
            List<RowExpression> jsonExtractScalarProjection = ImmutableList.of(new CallExpression(
                    functionResolution.resolveFunction(QualifiedName.of("json_extract_scalar"), fromTypes(ImmutableList.of(boundedVarcharType, JSON_PATH))),
                    ImmutableList.of(field(0, boundedVarcharType), constant(new JsonPath(pathString.toString()), JSON_PATH))));

            return functionResolution.getExpressionCompiler()
                    .compilePageProcessor(Optional.empty(), jsonExtractScalarProjection)
                    .get();
        }

        private static PageProcessor createJsonQueryPageProcessor(int depth, TestingFunctionResolution functionResolution, Type jsonPath2016Type)
        {
            IrPathNode pathRoot = new IrContextVariable(Optional.empty());
            for (int i = 1; i <= depth - 1; i++) {
                pathRoot = new IrMemberAccessor(pathRoot, Optional.of("key" + i), Optional.empty());
            }
            List<RowExpression> jsonQueryProjection = ImmutableList.of(new CallExpression(
                    functionResolution.resolveFunction(
                            QualifiedName.of(JSON_QUERY_FUNCTION_NAME),
                            fromTypes(ImmutableList.of(
                                    JSON_2016,
                                    jsonPath2016Type,
                                    JSON_NO_PARAMETERS_ROW_TYPE,
                                    TINYINT,
                                    TINYINT,
                                    TINYINT))),
                    ImmutableList.of(
                            new CallExpression(
                                    functionResolution.resolveFunction(QualifiedName.of(VARCHAR_TO_JSON), fromTypes(VARCHAR, BOOLEAN)),
                                    ImmutableList.of(field(0, VARCHAR), constant(true, BOOLEAN))),
                            constant(new IrJsonPath(false, pathRoot), jsonPath2016Type),
                            constantNull(JSON_NO_PARAMETERS_ROW_TYPE),
                            constant(0L, TINYINT),
                            constant(0L, TINYINT),
                            constant(0L, TINYINT))));

            return functionResolution.getExpressionCompiler()
                    .compilePageProcessor(Optional.empty(), jsonQueryProjection)
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
            List<RowExpression> jsonExtractScalarProjection = ImmutableList.of(new CallExpression(
                    functionResolution.resolveFunction(QualifiedName.of("json_extract"), fromTypes(ImmutableList.of(boundedVarcharType, JSON_PATH))),
                    ImmutableList.of(field(0, boundedVarcharType), constant(new JsonPath(pathString.toString()), JSON_PATH))));

            return functionResolution.getExpressionCompiler()
                    .compilePageProcessor(Optional.empty(), jsonExtractScalarProjection)
                    .get();
        }

        private static Block createChannel(int positionCount, int depth)
        {
            BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, positionCount);
            for (int position = 0; position < positionCount; position++) {
                SliceOutput slice = new DynamicSliceOutput(20);
                for (int i = 1; i <= depth; i++) {
                    slice.appendBytes(("{\"key" + i + "\" : ").getBytes(UTF_8));
                }
                slice.appendBytes(generateRandomJsonText().getBytes(UTF_8));
                for (int i = 1; i <= depth; i++) {
                    slice.appendByte('}');
                }

                VARCHAR.writeSlice(blockBuilder, slice.slice());
            }
            return blockBuilder.build();
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
        data.setup();
        new BenchmarkJsonFunctions().benchmarkJsonValueFunction(data);
        new BenchmarkJsonFunctions().benchmarkJsonExtractScalarFunction(data);
        new BenchmarkJsonFunctions().benchmarkJsonQueryFunction(data);
        new BenchmarkJsonFunctions().benchmarkJsonExtractFunction(data);
    }

    public static void main(String[] args)
            throws Exception
    {
        Benchmarks.benchmark(BenchmarkJsonFunctions.class, WarmupMode.BULK_INDI).run();
    }
}
