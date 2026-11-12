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
package io.trino.parquet.writer;

import com.google.common.collect.ImmutableList;
import io.trino.parquet.writer.repdef.DefLevelWriterProvider;
import io.trino.parquet.writer.repdef.DefLevelWriterProviders;
import io.trino.parquet.writer.repdef.RepLevelWriterProvider;
import io.trino.parquet.writer.repdef.RepLevelWriterProviders;
import io.trino.parquet.writer.valuewriter.ColumnDescriptorValuesWriter;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Bitmap;
import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.ColumnarMap;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TypeOperators;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.List;
import java.util.Optional;
import java.util.Random;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VariantType.VARIANT;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

/**
 * Writes the definition and repetition levels of a block, without writing its values.
 */
@State(Scope.Thread)
@OutputTimeUnit(MICROSECONDS)
@BenchmarkMode(AverageTime)
@Fork(2)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
public class BenchmarkLevelWriters
{
    private static final int POSITION_COUNT = 65_536;
    private static final int ARRAY_LENGTH = 4;

    public enum Scenario
    {
        VALUE_BLOCK_NULLS,
        VALUE_BLOCK_HALF_NULLS,
        DICTIONARY_BLOCK_NULLS,
        RLE_NULL_BLOCK,
        RLE_NULL_ROW,
        RLE_NULL_ARRAY,
        RLE_NULL_MAP,
        RLE_NULL_VARIANT,
        ARRAY,
        ARRAY_NULL_ELEMENTS,
        ARRAY_NULL_ARRAYS,
    }

    @Param
    public Scenario scenario;

    private List<DefLevelWriterProvider> definitionProviders;
    private List<RepLevelWriterProvider> repetitionProviders;
    private ColumnDescriptorValuesWriter definitionWriter;
    private ColumnDescriptorValuesWriter repetitionWriter;
    private boolean repeated;

    @Setup
    public void setup()
    {
        Random random = new Random(872364);
        // optional int (max definition level 1) or optional array of optional int (max definition level 3, max repetition level 1)
        repeated = scenario == Scenario.ARRAY
                || scenario == Scenario.ARRAY_NULL_ELEMENTS
                || scenario == Scenario.ARRAY_NULL_ARRAYS
                || scenario == Scenario.RLE_NULL_ARRAY
                || scenario == Scenario.RLE_NULL_MAP;
        // optional row of optional int has max definition level 2, and a map of required int keys has 2
        int maxDefinitionLevel = switch (scenario) {
            case RLE_NULL_ROW, RLE_NULL_MAP, RLE_NULL_VARIANT -> 2;
            default -> repeated ? 3 : 1;
        };
        int maxRepetitionLevel = repeated ? 1 : 0;
        ColumnDescriptor descriptor = new ColumnDescriptor(new String[] {"test"}, Types.optional(INT32).named("test"), maxRepetitionLevel, maxDefinitionLevel);
        definitionWriter = ColumnDescriptorValuesWriter.newDefinitionLevelWriter(descriptor, 4 * 1024 * 1024);
        repetitionWriter = ColumnDescriptorValuesWriter.newRepetitionLevelWriter(descriptor, 4 * 1024 * 1024);

        switch (scenario) {
            case VALUE_BLOCK_NULLS -> {
                Block block = intBlock(random, POSITION_COUNT, 10);
                definitionProviders = ImmutableList.of(DefLevelWriterProviders.of(block, maxDefinitionLevel));
            }
            case VALUE_BLOCK_HALF_NULLS -> {
                Block block = intBlock(random, POSITION_COUNT, 50);
                definitionProviders = ImmutableList.of(DefLevelWriterProviders.of(block, maxDefinitionLevel));
            }
            case DICTIONARY_BLOCK_NULLS -> {
                // The dictionary holds a null in 1 of 10 entries, so about 10% of the positions are null
                int[] ids = new int[POSITION_COUNT];
                for (int i = 0; i < ids.length; i++) {
                    ids[i] = random.nextInt(10);
                }
                Block nullable = new IntArrayBlock(10, Optional.of(nullAt(10, 0)), new int[10]);
                definitionProviders = ImmutableList.of(DefLevelWriterProviders.of(DictionaryBlock.create(ids.length, nullable, ids), maxDefinitionLevel));
            }
            case RLE_NULL_BLOCK -> definitionProviders = ImmutableList.of(DefLevelWriterProviders.of(RunLengthEncodedBlock.create(INTEGER, null, POSITION_COUNT), maxDefinitionLevel));
            case RLE_NULL_ROW -> {
                Block row = RunLengthEncodedBlock.create(RowType.anonymous(ImmutableList.of(INTEGER)), null, POSITION_COUNT);
                definitionProviders = ImmutableList.of(
                        DefLevelWriterProviders.of(row, 1),
                        DefLevelWriterProviders.of(RowBlock.getNullSuppressedRowFieldsFromBlock(row).get(0), maxDefinitionLevel));
            }
            case RLE_NULL_VARIANT -> definitionProviders = ImmutableList.of(
                    DefLevelWriterProviders.of(RunLengthEncodedBlock.create(VARIANT, null, POSITION_COUNT), 1),
                    // The leaves of a null variant have no positions
                    DefLevelWriterProviders.of(intBlock(random, 0, 0), maxDefinitionLevel));
            case RLE_NULL_ARRAY -> {
                Block arrayBlock = RunLengthEncodedBlock.create(new ArrayType(INTEGER), null, POSITION_COUNT);
                ColumnarArray columnarArray = ColumnarArray.toColumnarArray(arrayBlock);
                definitionProviders = ImmutableList.of(
                        DefLevelWriterProviders.of(columnarArray, arrayBlock, 2),
                        DefLevelWriterProviders.of(columnarArray.getElementsBlock(), maxDefinitionLevel));
                repetitionProviders = ImmutableList.of(
                        RepLevelWriterProviders.of(columnarArray, arrayBlock, maxRepetitionLevel),
                        RepLevelWriterProviders.of(columnarArray.getElementsBlock()));
            }
            case RLE_NULL_MAP -> {
                Block mapBlock = RunLengthEncodedBlock.create(new MapType(INTEGER, INTEGER, new TypeOperators()), null, POSITION_COUNT);
                ColumnarMap columnarMap = ColumnarMap.toColumnarMap(mapBlock);
                definitionProviders = ImmutableList.of(
                        DefLevelWriterProviders.of(columnarMap, mapBlock, 2),
                        DefLevelWriterProviders.of(columnarMap.getKeysBlock(), maxDefinitionLevel));
                repetitionProviders = ImmutableList.of(
                        RepLevelWriterProviders.of(columnarMap, mapBlock, maxRepetitionLevel),
                        RepLevelWriterProviders.of(columnarMap.getKeysBlock()));
            }
            case ARRAY, ARRAY_NULL_ELEMENTS, ARRAY_NULL_ARRAYS -> {
                int arrays = POSITION_COUNT / ARRAY_LENGTH;
                Block elements = intBlock(random, POSITION_COUNT, scenario == Scenario.ARRAY_NULL_ELEMENTS ? 10 : 0);
                int[] offsets = new int[arrays + 1];
                for (int i = 0; i <= arrays; i++) {
                    offsets[i] = i * ARRAY_LENGTH;
                }
                boolean[] nullArrays = null;
                if (scenario == Scenario.ARRAY_NULL_ARRAYS) {
                    nullArrays = new boolean[arrays];
                    for (int i = 0; i < arrays; i++) {
                        nullArrays[i] = random.nextInt(10) == 0;
                    }
                    // Null arrays are empty
                    int offset = 0;
                    for (int i = 0; i < arrays; i++) {
                        offsets[i] = offset;
                        offset += nullArrays[i] ? 0 : ARRAY_LENGTH;
                    }
                    offsets[arrays] = offset;
                    elements = intBlock(random, offset, 0);
                }
                Block arrayBlock = ArrayBlock.fromElementBlock(arrays, Optional.ofNullable(nullArrays).map(BenchmarkLevelWriters::validity), offsets, elements);
                ColumnarArray columnarArray = ColumnarArray.toColumnarArray(arrayBlock);
                definitionProviders = ImmutableList.of(
                        DefLevelWriterProviders.of(columnarArray, arrayBlock, 2),
                        DefLevelWriterProviders.of(columnarArray.getElementsBlock(), maxDefinitionLevel));
                repetitionProviders = ImmutableList.of(
                        RepLevelWriterProviders.of(columnarArray, arrayBlock, maxRepetitionLevel),
                        RepLevelWriterProviders.of(columnarArray.getElementsBlock()));
            }
        }
    }

    @Benchmark
    public long write()
    {
        definitionWriter.reset();
        DefLevelWriterProvider.getRootDefinitionLevelWriter(definitionProviders, definitionWriter).writeDefinitionLevels();
        long size = definitionWriter.getBufferedSize();
        if (repeated) {
            repetitionWriter.reset();
            RepLevelWriterProvider.getRootRepetitionLevelWriter(repetitionProviders, repetitionWriter).writeRepetitionLevels(0);
            size += repetitionWriter.getBufferedSize();
        }
        return size;
    }

    private static Block intBlock(Random random, int positionCount, int nullPercentage)
    {
        int[] values = new int[positionCount];
        long[] validity = Bitmap.allocateWords(positionCount, true);
        boolean hasNulls = false;
        for (int i = 0; i < positionCount; i++) {
            values[i] = i;
            if (nullPercentage > 0 && random.nextInt(100) < nullPercentage) {
                Bitmap.clear(validity, 0, i);
                hasNulls = true;
            }
        }
        return new IntArrayBlock(positionCount, hasNulls ? Optional.of(validity) : Optional.empty(), values);
    }

    private static long[] nullAt(int positionCount, int nullPosition)
    {
        long[] validity = Bitmap.allocateWords(positionCount, true);
        Bitmap.clear(validity, 0, nullPosition);
        return validity;
    }

    private static long[] validity(boolean[] isNull)
    {
        long[] validity = Bitmap.allocateWords(isNull.length, true);
        for (int i = 0; i < isNull.length; i++) {
            if (isNull[i]) {
                Bitmap.clear(validity, 0, i);
            }
        }
        return validity;
    }

    @Test
    public void verify()
    {
        for (Scenario value : Scenario.values()) {
            scenario = value;
            setup();
            write();
        }
    }

    static void main()
            throws Exception
    {
        benchmark(BenchmarkLevelWriters.class).run();
    }
}
