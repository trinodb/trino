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
package io.trino.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.sql.tree.QualifiedName;
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

import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static org.openjdk.jmh.annotations.Level.Invocation;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkArrayAggregation
{
    private static final int ARRAY_SIZE = 10_000_000;

    @Benchmark
    @OperationsPerInvocation(ARRAY_SIZE)
    public void arrayAggregation(BenchmarkData data)
    {
        data.getAccumulator().addInput(data.getPage());
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"BIGINT", "VARCHAR", "DOUBLE", "BOOLEAN"})
        private String type = "BIGINT";

        private Page page;
        private Accumulator accumulator;

        @Setup(Invocation)
        public void setup()
        {
            Metadata metadata = createTestMetadataManager();
            Block block;
            Type elementType;
            switch (type) {
                case "BIGINT":
                    elementType = BIGINT;
                    break;
                case "VARCHAR":
                    elementType = VARCHAR;
                    break;
                case "DOUBLE":
                    elementType = DOUBLE;
                    break;
                case "BOOLEAN":
                    elementType = BOOLEAN;
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
            ResolvedFunction resolvedFunction = metadata.resolveFunction(QualifiedName.of("array_agg"), fromTypes(elementType));
            InternalAggregationFunction function = metadata.getAggregateFunctionImplementation(resolvedFunction);
            accumulator = function.bind(ImmutableList.of(0), Optional.empty()).createAccumulator();

            block = createChannel(ARRAY_SIZE, elementType);
            page = new Page(block);
        }

        private static Block createChannel(int arraySize, Type elementType)
        {
            BlockBuilder blockBuilder = elementType.createBlockBuilder(null, arraySize);
            for (int i = 0; i < arraySize; i++) {
                if (elementType.getJavaType() == long.class) {
                    elementType.writeLong(blockBuilder, i);
                }
                else if (elementType.getJavaType() == double.class) {
                    elementType.writeDouble(blockBuilder, ThreadLocalRandom.current().nextDouble());
                }
                else if (elementType.getJavaType() == boolean.class) {
                    elementType.writeBoolean(blockBuilder, ThreadLocalRandom.current().nextBoolean());
                }
                else if (elementType.equals(VARCHAR)) {
                    // make sure the size of a varchar is rather small; otherwise the aggregated slice may overflow
                    elementType.writeSlice(blockBuilder, Slices.utf8Slice(Long.toString(ThreadLocalRandom.current().nextLong() % 100)));
                }
                else {
                    throw new UnsupportedOperationException();
                }
            }
            return blockBuilder.build();
        }

        public Accumulator getAccumulator()
        {
            return accumulator;
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
        new BenchmarkArrayAggregation().arrayAggregation(data);

        benchmark(BenchmarkArrayAggregation.class, WarmupMode.BULK).run();
    }
}
