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
import io.airlift.slice.Slice;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.RowExpression;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.block.BlockAssertions.createSlicesBlock;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.function.OperatorType.SUBSCRIPT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
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
public class BenchmarkMapSubscript
{
    private static final int POSITIONS = 1024;

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public List<Optional<Page>> mapSubscript(BenchmarkData data)
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
        @Param({"fix-width", "var-width", "dictionary"})
        private String name = "dictionary";

        @Param({"1", "13"})
        private int mapSize = 13;

        private Page page;
        private PageProcessor pageProcessor;

        @Setup
        public void setup()
        {
            TestingFunctionResolution functionResolution = new TestingFunctionResolution();
            ExpressionCompiler compiler = functionResolution.getExpressionCompiler();

            List<String> keys;
            switch (mapSize) {
                case 1:
                    keys = ImmutableList.of("do_not_use");
                    break;
                case 13:
                    keys = ImmutableList.of("is_inverted", "device_model", "country", "carrier_id", "network_type", "os_version",
                            "device_brand", "device_type", "interface", "device_os", "app_version", "device_type_class", "browser");
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
            verify(keys.size() == mapSize);

            MapType mapType;
            Block valueBlock;
            switch (name) {
                case "fix-width":
                    mapType = mapType(createUnboundedVarcharType(), DOUBLE);
                    valueBlock = createFixWidthValueBlock(POSITIONS, mapSize);
                    break;
                case "var-width":
                    mapType = mapType(createUnboundedVarcharType(), createUnboundedVarcharType());
                    valueBlock = createVarWidthValueBlock(POSITIONS, mapSize);
                    break;
                case "dictionary":
                    mapType = mapType(createUnboundedVarcharType(), createUnboundedVarcharType());
                    valueBlock = createDictionaryValueBlock(POSITIONS, mapSize);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }

            Block keyBlock = createKeyBlock(POSITIONS, keys);
            Block block = createMapBlock(mapType, POSITIONS, keyBlock, valueBlock);

            ImmutableList.Builder<RowExpression> projectionsBuilder = ImmutableList.builder();

            ResolvedFunction resolvedFunction = functionResolution.resolveOperator(SUBSCRIPT, ImmutableList.of(mapType, mapType.getKeyType()));
            for (int i = 0; i < mapSize; i++) {
                projectionsBuilder.add(new CallExpression(
                        resolvedFunction,
                        ImmutableList.of(field(0, mapType), constant(utf8Slice(keys.get(i)), createUnboundedVarcharType()))));
            }

            ImmutableList<RowExpression> projections = projectionsBuilder.build();
            pageProcessor = compiler.compilePageProcessor(Optional.empty(), projections).get();
            page = new Page(block);
        }

        public PageProcessor getPageProcessor()
        {
            return pageProcessor;
        }

        public Page getPage()
        {
            return page;
        }

        private static Block createMapBlock(MapType mapType, int positionCount, Block keyBlock, Block valueBlock)
        {
            int[] offsets = new int[positionCount + 1];
            int mapSize = keyBlock.getPositionCount() / positionCount;
            for (int i = 0; i < offsets.length; i++) {
                offsets[i] = mapSize * i;
            }
            return mapType.createBlockFromKeyValue(Optional.empty(), offsets, keyBlock, valueBlock);
        }

        private static Block createKeyBlock(int positionCount, List<String> keys)
        {
            Block keyDictionaryBlock = createSliceArrayBlock(keys);
            int[] keyIds = new int[positionCount * keys.size()];
            for (int i = 0; i < keyIds.length; i++) {
                keyIds[i] = i % keys.size();
            }
            return DictionaryBlock.create(keyIds.length, keyDictionaryBlock, keyIds);
        }

        private static Block createFixWidthValueBlock(int positionCount, int mapSize)
        {
            BlockBuilder valueBlockBuilder = DOUBLE.createFixedSizeBlockBuilder(positionCount * mapSize);
            for (int i = 0; i < positionCount * mapSize; i++) {
                DOUBLE.writeDouble(valueBlockBuilder, ThreadLocalRandom.current().nextDouble());
            }
            return valueBlockBuilder.build();
        }

        private static Block createVarWidthValueBlock(int positionCount, int mapSize)
        {
            Type valueType = createUnboundedVarcharType();
            BlockBuilder valueBlockBuilder = valueType.createBlockBuilder(null, positionCount * mapSize);
            for (int i = 0; i < positionCount * mapSize; i++) {
                int wordLength = ThreadLocalRandom.current().nextInt(5, 10);
                valueType.writeSlice(valueBlockBuilder, utf8Slice(randomString(wordLength)));
            }
            return valueBlockBuilder.build();
        }

        private static Block createDictionaryValueBlock(int positionCount, int mapSize)
        {
            double distinctRatio = 0.82;

            int dictionarySize = (int) (positionCount * mapSize * distinctRatio);
            List<String> dictionaryStrings = new ArrayList<>(dictionarySize);
            for (int i = 0; i < dictionarySize; i++) {
                int wordLength = ThreadLocalRandom.current().nextInt(5, 10);
                dictionaryStrings.add(randomString(wordLength));
            }
            Block dictionaryBlock = createSliceArrayBlock(dictionaryStrings);

            int[] keyIds = new int[positionCount * mapSize];
            for (int i = 0; i < keyIds.length; i++) {
                keyIds[i] = ThreadLocalRandom.current().nextInt(0, dictionarySize);
            }
            return DictionaryBlock.create(keyIds.length, dictionaryBlock, keyIds);
        }

        private static String randomString(int length)
        {
            String symbols = "abcdefghijklmnopqrstuvwxyz";
            char[] chars = new char[length];
            for (int i = 0; i < length; i++) {
                chars[i] = symbols.charAt(ThreadLocalRandom.current().nextInt(symbols.length()));
            }
            return new String(chars);
        }

        private static Block createSliceArrayBlock(List<String> keys)
        {
            // last position is reserved for null
            Slice[] sliceArray = new Slice[keys.size() + 1];
            for (int i = 0; i < keys.size(); i++) {
                sliceArray[i] = utf8Slice(keys.get(i));
            }
            return createSlicesBlock(sliceArray);
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkMapSubscript().mapSubscript(data);

        benchmark(BenchmarkMapSubscript.class, WarmupMode.INDI).run();
    }
}
