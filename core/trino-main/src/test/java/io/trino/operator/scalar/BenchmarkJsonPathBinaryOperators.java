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
import io.trino.json.ir.IrArithmeticBinary;
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
import io.trino.spi.type.Decimals;
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
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.trino.json.ir.IrArithmeticBinary.Operator.ADD;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.operator.scalar.json.JsonInputFunctions.VARCHAR_TO_JSON;
import static io.trino.operator.scalar.json.JsonValueFunction.JSON_VALUE_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.ExpressionAnalyzer.JSON_NO_PARAMETERS_ROW_TYPE;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.Expressions.constantNull;
import static io.trino.sql.relational.Expressions.field;
import static io.trino.type.Json2016Type.JSON_2016;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkJsonPathBinaryOperators
{
    private static final int POSITION_COUNT = 100_000;
    private static final FullConnectorSession FULL_CONNECTOR_SESSION = new FullConnectorSession(TestingSession.testSessionBuilder().build(), ConnectorIdentity.ofUser("test"));

    @Benchmark
    @OperationsPerInvocation(POSITION_COUNT)
    public List<Optional<Page>> benchmarkJsonValueFunctionConstantTypes(BenchmarkData data)
    {
        return ImmutableList.copyOf(
                data.getJsonValuePageProcessor().process(
                        FULL_CONNECTOR_SESSION,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        data.getPageConstantTypes()));
    }

    @Benchmark
    @OperationsPerInvocation(POSITION_COUNT)
    public List<Optional<Page>> benchmarkJsonValueFunctionVaryingTypes(BenchmarkData data)
    {
        return ImmutableList.copyOf(
                data.getJsonValuePageProcessor().process(
                        FULL_CONNECTOR_SESSION,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        data.getPageVaryingTypes()));
    }

    @Benchmark
    @OperationsPerInvocation(POSITION_COUNT)
    public List<Optional<Page>> benchmarkJsonValueFunctionMultipleVaryingTypes(BenchmarkData data)
    {
        return ImmutableList.copyOf(
                data.getJsonValuePageProcessor().process(
                        FULL_CONNECTOR_SESSION,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        data.getPageMultipleVaryingTypes()));
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private Page pageConstantTypes;
        private Page pageVaryingTypes;
        private Page pageMultipleVaryingTypes;
        private PageProcessor jsonValuePageProcessor;

        @Setup
        public void setup()
        {
            pageConstantTypes = new Page(createChannelConstantTypes(POSITION_COUNT));
            pageVaryingTypes = new Page(createChannelVaryingTypes(POSITION_COUNT));
            pageMultipleVaryingTypes = new Page(createChannelMultipleVaryingTypes(POSITION_COUNT));
            jsonValuePageProcessor = createJsonValuePageProcessor();
        }

        private static PageProcessor createJsonValuePageProcessor()
        {
            TestingFunctionResolution functionResolution = new TestingFunctionResolution();
            Type jsonPath2016Type = PLANNER_CONTEXT.getTypeManager().getType(TypeId.of(JsonPath2016Type.NAME));

            IrPathNode path = new IrArithmeticBinary(
                    ADD,
                    new IrMemberAccessor(new IrContextVariable(Optional.empty()), Optional.of("first"), Optional.empty()),
                    new IrMemberAccessor(new IrContextVariable(Optional.empty()), Optional.of("second"), Optional.empty()),
                    Optional.empty());
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
                            constant(new IrJsonPath(false, path), jsonPath2016Type),
                            constantNull(JSON_NO_PARAMETERS_ROW_TYPE),
                            constant(0L, TINYINT),
                            constantNull(VARCHAR),
                            constant(0L, TINYINT),
                            constantNull(VARCHAR))));

            return functionResolution.getExpressionCompiler()
                    .compilePageProcessor(Optional.empty(), jsonValueProjection)
                    .get();
        }

        private static Block createChannelConstantTypes(int positionCount)
        {
            BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, positionCount);
            for (int position = 0; position < positionCount; position++) {
                SliceOutput slice = new DynamicSliceOutput(20);
                slice.appendBytes(("{\"first\" : ").getBytes(UTF_8))
                        .appendBytes(format("%e", (double) position % 100).getBytes(UTF_8)) // real
                        .appendBytes((", \"second\" : ").getBytes(UTF_8))
                        .appendBytes(format("%s", position % 10).getBytes(UTF_8)) // int
                        .appendByte('}');
                VARCHAR.writeSlice(blockBuilder, slice.slice());
            }
            return blockBuilder.build();
        }

        private static Block createChannelVaryingTypes(int positionCount)
        {
            BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, positionCount);
            for (int position = 0; position < positionCount; position++) {
                SliceOutput slice = new DynamicSliceOutput(20);
                slice.appendBytes(("{\"first\" : ").getBytes(UTF_8));
                if (position % 3 == 0) {
                    slice.appendBytes(format("%e", (double) position % 100).getBytes(UTF_8)); // real
                }
                else if (position % 3 == 1) {
                    slice.appendBytes(format("%s", (position % 100) * 1000000000000L).getBytes(UTF_8)); // bigint
                }
                else {
                    slice.appendBytes(format("%s", position % 100).getBytes(UTF_8)); // int
                }
                slice.appendBytes((", \"second\" : ").getBytes(UTF_8))
                        .appendBytes(format("%s", position % 10).getBytes(UTF_8)) // int
                        .appendByte('}');
                VARCHAR.writeSlice(blockBuilder, slice.slice());
            }
            return blockBuilder.build();
        }

        private static Block createChannelMultipleVaryingTypes(int positionCount)
        {
            BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, positionCount);
            for (int position = 0; position < positionCount; position++) {
                SliceOutput slice = new DynamicSliceOutput(20);

                slice.appendBytes(("{\"first\" : ").getBytes(UTF_8));
                if (position % 3 == 0) {
                    slice.appendBytes(format("%e", (double) position % 100).getBytes(UTF_8)); // real
                }
                else if (position % 3 == 1) {
                    slice.appendBytes(format("%s", (position % 100) * 1000000000000L).getBytes(UTF_8)); // bigint
                }
                else {
                    slice.appendBytes(format("%s", position % 100).getBytes(UTF_8)); // int
                }

                slice.appendBytes((", \"second\" : ").getBytes(UTF_8));
                if (position % 4 == 0) {
                    slice.appendBytes(format("%e", (double) position % 10).getBytes(UTF_8)); // real
                }
                else if (position % 4 == 1) {
                    slice.appendBytes(Decimals.toString(position % 10, 2).getBytes(UTF_8)); // decimal
                }
                else if (position % 4 == 2) {
                    slice.appendBytes(format("%s", (position % 10) * 1000000000000L).getBytes(UTF_8)); // bigint
                }
                else {
                    slice.appendBytes(format("%s", position % 10).getBytes(UTF_8)); // int
                }

                slice.appendByte('}');
                VARCHAR.writeSlice(blockBuilder, slice.slice());
            }
            return blockBuilder.build();
        }

        public PageProcessor getJsonValuePageProcessor()
        {
            return jsonValuePageProcessor;
        }

        public Page getPageConstantTypes()
        {
            return pageConstantTypes;
        }

        public Page getPageVaryingTypes()
        {
            return pageVaryingTypes;
        }

        public Page getPageMultipleVaryingTypes()
        {
            return pageMultipleVaryingTypes;
        }
    }

    @Test
    public void verify()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkJsonPathBinaryOperators().benchmarkJsonValueFunctionConstantTypes(data);
        new BenchmarkJsonPathBinaryOperators().benchmarkJsonValueFunctionVaryingTypes(data);
        new BenchmarkJsonPathBinaryOperators().benchmarkJsonValueFunctionMultipleVaryingTypes(data);
    }

    public static void main(String[] args)
            throws Exception
    {
        Benchmarks.benchmark(BenchmarkJsonPathBinaryOperators.class).run();
    }
}
