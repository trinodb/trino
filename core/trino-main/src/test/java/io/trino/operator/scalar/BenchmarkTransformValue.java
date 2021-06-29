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
import io.airlift.slice.Slices;
import io.trino.jmh.Benchmarks;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.gen.PageFunctionCompiler;
import io.trino.sql.relational.LambdaDefinitionExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.VariableReferenceExpression;
import io.trino.sql.tree.QualifiedName;
import io.trino.type.FunctionType;
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
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.relational.Expressions.call;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.Expressions.field;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.util.StructuralTestUtil.mapType;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkTransformValue
{
    private static final int POSITIONS = 100_000;
    private static final int NUM_TYPES = 3;

    @Benchmark
    @OperationsPerInvocation(POSITIONS * NUM_TYPES)
    public List<Optional<Page>> benchmark(BenchmarkData data)
    {
        return ImmutableList.copyOf(
                data.getPageProcessor().process(
                        SESSION,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        data.getPage()));
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"BIGINT", "DOUBLE", "VARCHAR"})
        private String type = "VARCHAR";

        private String name = "transform_values";
        private Page page;
        private PageProcessor pageProcessor;

        @Setup
        public void setup()
        {
            Metadata metadata = createTestMetadataManager();
            ExpressionCompiler compiler = new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 0));
            ImmutableList.Builder<RowExpression> projectionsBuilder = ImmutableList.builder();
            Type elementType;
            Object compareValue;
            switch (type) {
                case "BIGINT":
                    elementType = BIGINT;
                    compareValue = 0L;
                    break;
                case "DOUBLE":
                    elementType = DOUBLE;
                    compareValue = 0.0d;
                    break;
                case "VARCHAR":
                    elementType = VARCHAR;
                    compareValue = Slices.utf8Slice("0");
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
            MapType mapType = mapType(elementType, elementType);
            ResolvedFunction resolvedFunction = metadata.resolveFunction(
                    QualifiedName.of(name),
                    fromTypes(mapType, new FunctionType(ImmutableList.of(elementType), elementType)));
            ResolvedFunction lessThan = metadata.resolveOperator(LESS_THAN, ImmutableList.of(elementType, elementType));
            projectionsBuilder.add(call(resolvedFunction, ImmutableList.of(
                    field(0, mapType),
                    new LambdaDefinitionExpression(
                            ImmutableList.of(elementType, elementType),
                            ImmutableList.of("x", "y"),
                            call(lessThan, ImmutableList.of(
                                    constant(compareValue, elementType),
                                    new VariableReferenceExpression("y", elementType)))))));
            Block block = createChannel(POSITIONS, mapType, elementType);

            ImmutableList<RowExpression> projections = projectionsBuilder.build();
            pageProcessor = compiler.compilePageProcessor(Optional.empty(), projections).get();
            page = new Page(block);
        }

        private static Block createChannel(int positionCount, MapType mapType, Type elementType)
        {
            BlockBuilder mapBlockBuilder = mapType.createBlockBuilder(null, 1);
            BlockBuilder singleMapBlockWriter = mapBlockBuilder.beginBlockEntry();
            Object key;
            Object value;
            for (int position = 0; position < positionCount; position++) {
                if (elementType.equals(BIGINT)) {
                    key = position;
                    value = ThreadLocalRandom.current().nextLong();
                }
                else if (elementType.equals(DOUBLE)) {
                    key = position;
                    value = ThreadLocalRandom.current().nextDouble();
                }
                else if (elementType.equals(VARCHAR)) {
                    key = Slices.utf8Slice(Integer.toString(position));
                    value = Slices.utf8Slice(Double.toString(ThreadLocalRandom.current().nextDouble()));
                }
                else {
                    throw new UnsupportedOperationException();
                }
                // Use position as the key to avoid collision
                writeNativeValue(elementType, singleMapBlockWriter, key);
                writeNativeValue(elementType, singleMapBlockWriter, value);
            }
            mapBlockBuilder.closeEntry();
            return mapBlockBuilder.build();
        }

        public PageProcessor getPageProcessor()
        {
            return pageProcessor;
        }

        public Page getPage()
        {
            return page;
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkTransformValue().benchmark(data);

        Benchmarks.benchmark(BenchmarkTransformValue.class, WarmupMode.BULK).run();
    }
}
