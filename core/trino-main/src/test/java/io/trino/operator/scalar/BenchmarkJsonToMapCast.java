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
import io.trino.jmh.Benchmarks;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.RowExpression;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.options.WarmupMode;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.relational.Expressions.field;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.JsonType.JSON;
import static io.trino.util.StructuralTestUtil.mapType;
import static java.nio.charset.StandardCharsets.UTF_8;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(10)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkJsonToMapCast
{
    private static final int POSITION_COUNT = 100_000;
    private static final int MAP_SIZE = 10;

    @Benchmark
    @OperationsPerInvocation(POSITION_COUNT)
    public List<Optional<Page>> benchmark(BenchmarkData data)
    {
        return ImmutableList.copyOf(
                data.getPageProcessor().process(
                        SESSION,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        SourcePage.create(data.getPage())));
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"BIGINT", "DOUBLE", "VARCHAR"})
        private String valueTypeName = "BIGINT";

        private Page page;
        private PageProcessor pageProcessor;

        @Setup
        public void setup()
        {
            Type valueType;
            switch (valueTypeName) {
                case "BIGINT":
                    valueType = BIGINT;
                    break;
                case "DOUBLE":
                    valueType = DOUBLE;
                    break;
                case "VARCHAR":
                    valueType = VARCHAR;
                    break;
                default:
                    throw new UnsupportedOperationException();
            }

            TestingFunctionResolution functionResolution = new TestingFunctionResolution();
            MapType mapType = mapType(VARCHAR, valueType);
            List<RowExpression> projections = ImmutableList.of(new CallExpression(
                    functionResolution.getCoercion(JSON, mapType),
                    ImmutableList.of(field(0, JSON))));

            pageProcessor = functionResolution.getExpressionCompiler()
                    .compilePageProcessor(Optional.empty(), projections)
                    .get();

            page = new Page(createChannel(POSITION_COUNT, MAP_SIZE, valueType));
        }

        private static Block createChannel(int positionCount, int mapSize, Type valueType)
        {
            BlockBuilder blockBuilder = JSON.createBlockBuilder(null, positionCount);
            for (int position = 0; position < positionCount; position++) {
                SliceOutput jsonSlice = new DynamicSliceOutput(20 * mapSize);
                jsonSlice.appendByte('{');
                for (int i = 0; i < mapSize; i++) {
                    if (i != 0) {
                        jsonSlice.appendByte(',');
                    }
                    String key = "key" + i;
                    String value = generateRandomJsonValue(valueType);
                    jsonSlice.appendByte('"');
                    jsonSlice.appendBytes(key.getBytes(UTF_8));
                    jsonSlice.appendBytes("\":".getBytes(UTF_8));
                    jsonSlice.appendBytes(value.getBytes(UTF_8));
                }
                jsonSlice.appendByte('}');

                JSON.writeSlice(blockBuilder, jsonSlice.slice());
            }
            return blockBuilder.build();
        }

        private static String generateRandomJsonValue(Type valueType)
        {
            if (valueType == BIGINT) {
                return Long.toString(ThreadLocalRandom.current().nextLong());
            }
            if (valueType == DOUBLE) {
                return Double.toString(ThreadLocalRandom.current().nextDouble());
            }
            if (valueType == VARCHAR) {
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
            throw new UnsupportedOperationException();
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

    @Test
    public void verify()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkJsonToMapCast().benchmark(data);
    }

    public static void main(String[] args)
            throws Exception
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkJsonToMapCast().benchmark(data);

        Benchmarks.benchmark(BenchmarkJsonToMapCast.class, WarmupMode.BULK_INDI).run();
    }
}
