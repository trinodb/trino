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
package io.trino.operator.project;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.type.MapType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.IntStream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.block.BlockAssertions.createSlicesBlock;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(MICROSECONDS)
@Fork(3)
@Warmup(iterations = 15)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkDictionaryBlock
{
    @Benchmark
    public long getSizeInBytes(BenchmarkData data)
    {
        return data.getDictionaryBlock().getSizeInBytes();
    }

    @Benchmark
    public long getPositionsThenGetSizeInBytes(BenchmarkData data)
    {
        int[] positionIds = data.getPositionsIds();
        return data.getAllPositionsDictionaryBlock().getPositions(positionIds, 0, positionIds.length).getSizeInBytes();
    }

    @Benchmark
    public Block copyPositions(BenchmarkData data)
    {
        int[] positionIds = data.getPositionsIds();
        return data.getAllPositionsDictionaryBlock().copyPositions(positionIds, 0, positionIds.length);
    }

    @Benchmark
    public Block copyPositionsCompactDictionary(BenchmarkData data)
    {
        int[] positionIds = data.getPositionsIds();
        return data.getAllPositionsCompactDictionaryBlock().copyPositions(positionIds, 0, positionIds.length);
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private static final int POSITIONS = 100_000;
        @Param({"100", "1000", "10000", "100000"})
        private String selectedPositions = "100";

        @Param({"varchar", "integer"})
        private String valueType = "integer";

        private int[] positionsIds;
        private DictionaryBlock dictionaryBlock;
        private DictionaryBlock allPositionsDictionaryBlock;
        private DictionaryBlock allPositionsCompactDictionaryBlock;
        private boolean[] selectedPositionsMask;
        private int selectedPositionCount;

        @Setup(Level.Trial)
        public void setup()
        {
            positionsIds = generateIds(Integer.parseInt(selectedPositions), POSITIONS);
            selectedPositionsMask = new boolean[POSITIONS];
            for (int position : positionsIds) {
                if (!selectedPositionsMask[position]) {
                    selectedPositionsMask[position] = true;
                    selectedPositionCount++;
                }
            }
            Block mapBlock;
            switch (valueType) {
                case "varchar":
                    mapBlock = createVarcharMapBlock(POSITIONS);
                    break;
                case "integer":
                    mapBlock = createIntMapBlock(POSITIONS);
                    break;
                default:
                    throw new IllegalArgumentException("Unrecognized value type: " + valueType);
            }
            dictionaryBlock = (DictionaryBlock) DictionaryBlock.create(positionsIds.length, mapBlock, positionsIds);
            int[] allPositions = IntStream.range(0, POSITIONS).toArray();
            allPositionsDictionaryBlock = (DictionaryBlock) DictionaryBlock.create(allPositions.length, mapBlock, allPositions);
            allPositionsCompactDictionaryBlock = (DictionaryBlock) DictionaryBlock.create(POSITIONS, mapBlock, allPositions);
        }

        private static Block createVarcharMapBlock(int positionCount)
        {
            MapType mapType = (MapType) TESTING_TYPE_MANAGER.getType(new TypeSignature(StandardTypes.MAP, TypeSignatureParameter.typeParameter(VARCHAR.getTypeSignature()), TypeSignatureParameter.typeParameter(VARCHAR.getTypeSignature())));
            Block keyBlock = createVarcharDictionaryBlock(generateList("key", positionCount));
            Block valueBlock = createVarcharDictionaryBlock(generateList("value", positionCount));
            int[] offsets = new int[positionCount + 1];
            int mapSize = keyBlock.getPositionCount() / positionCount;
            for (int i = 0; i < offsets.length; i++) {
                offsets[i] = mapSize * i;
            }
            return mapType.createBlockFromKeyValue(Optional.empty(), offsets, keyBlock, valueBlock);
        }

        private static Block createVarcharDictionaryBlock(List<String> values)
        {
            Block dictionary = createSliceArrayBlock(values);
            int[] ids = new int[values.size()];
            for (int i = 0; i < ids.length; i++) {
                ids[i] = i;
            }
            return DictionaryBlock.create(ids.length, dictionary, ids);
        }

        private static Block createIntMapBlock(int positionCount)
        {
            MapType mapType = (MapType) TESTING_TYPE_MANAGER.getType(new TypeSignature(StandardTypes.MAP, TypeSignatureParameter.typeParameter(INTEGER.getTypeSignature()), TypeSignatureParameter.typeParameter(INTEGER.getTypeSignature())));
            Block keyBlock = createIntDictionaryBlock(positionCount);
            Block valueBlock = createIntDictionaryBlock(positionCount);
            int[] offsets = new int[positionCount + 1];
            int mapSize = keyBlock.getPositionCount() / positionCount;
            for (int i = 0; i < offsets.length; i++) {
                offsets[i] = mapSize * i;
            }
            return mapType.createBlockFromKeyValue(Optional.empty(), offsets, keyBlock, valueBlock);
        }

        private static Block createIntDictionaryBlock(int positionCount)
        {
            Block dictionary = createIntBlock(positionCount);
            int[] ids = new int[positionCount];
            for (int i = 0; i < ids.length; i++) {
                ids[i] = i;
            }
            return DictionaryBlock.create(ids.length, dictionary, ids);
        }

        private static Block createIntBlock(int positionCount)
        {
            BlockBuilder builder = INTEGER.createFixedSizeBlockBuilder(positionCount);
            for (int i = 0; i < positionCount; i++) {
                INTEGER.writeLong(builder, i);
            }
            return builder.build();
        }

        private static Block createSliceArrayBlock(List<String> values)
        {
            // last position is reserved for null
            Slice[] sliceArray = new Slice[values.size() + 1];
            for (int i = 0; i < values.size(); i++) {
                sliceArray[i] = utf8Slice(values.get(i));
            }
            return createSlicesBlock(sliceArray);
        }

        private static List<String> generateList(String prefix, int count)
        {
            ImmutableList.Builder<String> list = ImmutableList.builder();
            for (int i = 0; i < count; i++) {
                list.add(prefix + i);
            }
            return list.build();
        }

        private static int[] generateIds(int count, int range)
        {
            return new Random(42).ints(count, 0, range).toArray();
        }

        public int[] getPositionsIds()
        {
            return positionsIds;
        }

        public boolean[] getSelectedPositionsMask()
        {
            return selectedPositionsMask;
        }

        public int getSelectedPositionCount()
        {
            return selectedPositionCount;
        }

        public DictionaryBlock getDictionaryBlock()
        {
            return dictionaryBlock;
        }

        public DictionaryBlock getAllPositionsDictionaryBlock()
        {
            return allPositionsDictionaryBlock;
        }

        public DictionaryBlock getAllPositionsCompactDictionaryBlock()
        {
            return allPositionsCompactDictionaryBlock;
        }
    }

    @Test
    public void testGetSizeInBytes()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        getSizeInBytes(data);
    }

    @Test
    public void testGetPositionsThenGetSizeInBytes()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        getPositionsThenGetSizeInBytes(data);
    }

    @Test
    public void testCopyPositions()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        copyPositions(data);
    }

    @Test
    public void testCopyPositionsCompactDictionary()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        copyPositionsCompactDictionary(data);
    }

    public static void main(String[] args)
            throws Exception
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkDictionaryBlock().getSizeInBytes(data);

        benchmark(BenchmarkDictionaryBlock.class).run();
    }
}
