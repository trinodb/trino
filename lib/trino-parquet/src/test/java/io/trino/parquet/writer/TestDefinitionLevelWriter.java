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
import io.trino.parquet.writer.repdef.DefLevelWriterProviders;
import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.ColumnarMap;
import io.trino.spi.block.ColumnarRow;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.ValuesWriter;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Stream;

import static io.trino.parquet.writer.repdef.DefLevelWriterProvider.DefinitionLevelWriter;
import static io.trino.parquet.writer.repdef.DefLevelWriterProvider.ValuesCount;
import static io.trino.parquet.writer.repdef.DefLevelWriterProvider.getRootDefinitionLevelWriter;
import static io.trino.spi.block.ArrayBlock.fromElementBlock;
import static io.trino.spi.block.ColumnarArray.toColumnarArray;
import static io.trino.spi.block.ColumnarMap.toColumnarMap;
import static io.trino.spi.block.ColumnarRow.toColumnarRow;
import static io.trino.spi.block.MapBlock.fromKeyValueBlock;
import static io.trino.spi.block.RowBlock.fromFieldBlocks;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.DataProviders.toDataProvider;
import static java.lang.Math.toIntExact;
import static java.util.Collections.nCopies;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDefinitionLevelWriter
{
    private static final int POSITIONS = 8096;
    private static final Random RANDOM = new Random(42);
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    private static final boolean[] ALL_NULLS_ARRAY = new boolean[POSITIONS];
    private static final boolean[] RANDOM_NULLS_ARRAY = new boolean[POSITIONS];
    private static final boolean[] GROUPED_NULLS_ARRAY = new boolean[POSITIONS];

    static {
        Arrays.fill(ALL_NULLS_ARRAY, true);
        for (int i = 0; i < POSITIONS; i++) {
            RANDOM_NULLS_ARRAY[i] = RANDOM.nextBoolean();
        }

        int maxGroupSize = 23;
        int position = 0;
        while (position < POSITIONS) {
            int remaining = POSITIONS - position;
            int groupSize = Math.min(RANDOM.nextInt(maxGroupSize) + 1, remaining);
            Arrays.fill(GROUPED_NULLS_ARRAY, position, position + groupSize, RANDOM.nextBoolean());
            position += groupSize;
        }
    }

    @Test(dataProvider = "primitiveBlockProvider")
    public void testWritePrimitiveDefinitionLevels(PrimitiveBlockProvider blockProvider)
    {
        Block block = blockProvider.getInputBlock();
        int maxDefinitionLevel = 3;
        // Write definition levels for all positions
        assertDefinitionLevels(block, ImmutableList.of(), maxDefinitionLevel);

        // Write definition levels for all positions one-at-a-time
        assertDefinitionLevels(block, nCopies(block.getPositionCount(), 1), maxDefinitionLevel);

        // Write definition levels for all positions with different group sizes
        assertDefinitionLevels(block, generateGroupSizes(block.getPositionCount()), maxDefinitionLevel);
    }

    @DataProvider
    public static Object[][] primitiveBlockProvider()
    {
        return Stream.of(PrimitiveBlockProvider.values())
                .collect(toDataProvider());
    }

    private enum PrimitiveBlockProvider
    {
        NO_NULLS {
            @Override
            Block getInputBlock()
            {
                return new LongArrayBlock(POSITIONS, Optional.empty(), new long[POSITIONS]);
            }
        },
        NO_NULLS_WITH_MAY_HAVE_NULL {
            @Override
            Block getInputBlock()
            {
                return new LongArrayBlock(POSITIONS, Optional.of(new boolean[POSITIONS]), new long[POSITIONS]);
            }
        },
        ALL_NULLS {
            @Override
            Block getInputBlock()
            {
                return new LongArrayBlock(POSITIONS, Optional.of(ALL_NULLS_ARRAY), new long[POSITIONS]);
            }
        },
        RANDOM_NULLS {
            @Override
            Block getInputBlock()
            {
                return new LongArrayBlock(POSITIONS, Optional.of(RANDOM_NULLS_ARRAY), new long[POSITIONS]);
            }
        },
        GROUPED_NULLS {
            @Override
            Block getInputBlock()
            {
                return new LongArrayBlock(POSITIONS, Optional.of(GROUPED_NULLS_ARRAY), new long[POSITIONS]);
            }
        };

        abstract Block getInputBlock();
    }

    @Test(dataProvider = "rowBlockProvider")
    public void testWriteRowDefinitionLevels(RowBlockProvider blockProvider)
    {
        ColumnarRow columnarRow = toColumnarRow(blockProvider.getInputBlock());
        int fieldMaxDefinitionLevel = 2;
        // Write definition levels for all positions
        for (int field = 0; field < columnarRow.getFieldCount(); field++) {
            assertDefinitionLevels(columnarRow, ImmutableList.of(), field, fieldMaxDefinitionLevel);
        }

        // Write definition levels for all positions one-at-a-time
        for (int field = 0; field < columnarRow.getFieldCount(); field++) {
            assertDefinitionLevels(
                    columnarRow,
                    nCopies(columnarRow.getPositionCount(), 1),
                    field,
                    fieldMaxDefinitionLevel);
        }

        // Write definition levels for all positions with different group sizes
        for (int field = 0; field < columnarRow.getFieldCount(); field++) {
            assertDefinitionLevels(
                    columnarRow,
                    generateGroupSizes(columnarRow.getPositionCount()),
                    field,
                    fieldMaxDefinitionLevel);
        }
    }

    @DataProvider
    public static Object[][] rowBlockProvider()
    {
        return Stream.of(RowBlockProvider.values())
                .collect(toDataProvider());
    }

    private enum RowBlockProvider
    {
        NO_NULLS {
            @Override
            Block getInputBlock()
            {
                return createRowBlock(Optional.empty());
            }
        },
        NO_NULLS_WITH_MAY_HAVE_NULL {
            @Override
            Block getInputBlock()
            {
                return createRowBlock(Optional.of(new boolean[POSITIONS]));
            }
        },
        ALL_NULLS {
            @Override
            Block getInputBlock()
            {
                return createRowBlock(Optional.of(ALL_NULLS_ARRAY));
            }
        },
        RANDOM_NULLS {
            @Override
            Block getInputBlock()
            {
                return createRowBlock(Optional.of(RANDOM_NULLS_ARRAY));
            }
        },
        GROUPED_NULLS {
            @Override
            Block getInputBlock()
            {
                return createRowBlock(Optional.of(GROUPED_NULLS_ARRAY));
            }
        };

        abstract Block getInputBlock();

        private static Block createRowBlock(Optional<boolean[]> rowIsNull)
        {
            int positionCount = rowIsNull.map(isNull -> isNull.length).orElse(0) - toIntExact(rowIsNull.stream().count());
            int fieldCount = 4;
            Block[] fieldBlocks = new Block[fieldCount];
            // no nulls block
            fieldBlocks[0] = new LongArrayBlock(positionCount, Optional.empty(), new long[positionCount]);
            // no nulls with mayHaveNull block
            fieldBlocks[1] = new LongArrayBlock(positionCount, Optional.of(new boolean[positionCount]), new long[positionCount]);
            // all nulls block
            boolean[] allNulls = new boolean[positionCount];
            Arrays.fill(allNulls, false);
            fieldBlocks[2] = new LongArrayBlock(positionCount, Optional.of(allNulls), new long[positionCount]);
            // random nulls block
            fieldBlocks[3] = createLongsBlockWithRandomNulls(positionCount);

            return fromFieldBlocks(positionCount, rowIsNull, fieldBlocks);
        }
    }

    @Test(dataProvider = "arrayBlockProvider")
    public void testWriteArrayDefinitionLevels(ArrayBlockProvider blockProvider)
    {
        ColumnarArray columnarArray = toColumnarArray(blockProvider.getInputBlock());
        int maxDefinitionLevel = 3;
        // Write definition levels for all positions
        assertDefinitionLevels(
                columnarArray,
                ImmutableList.of(),
                maxDefinitionLevel);

        // Write definition levels for all positions one-at-a-time
        assertDefinitionLevels(
                columnarArray,
                nCopies(columnarArray.getPositionCount(), 1),
                maxDefinitionLevel);

        // Write definition levels for all positions with different group sizes
        assertDefinitionLevels(
                columnarArray,
                generateGroupSizes(columnarArray.getPositionCount()),
                maxDefinitionLevel);
    }

    @DataProvider
    public static Object[][] arrayBlockProvider()
    {
        return Stream.of(ArrayBlockProvider.values())
                .collect(toDataProvider());
    }

    private enum ArrayBlockProvider
    {
        NO_NULLS {
            @Override
            Block getInputBlock()
            {
                return createArrayBlock(Optional.empty());
            }
        },
        NO_NULLS_WITH_MAY_HAVE_NULL {
            @Override
            Block getInputBlock()
            {
                return createArrayBlock(Optional.of(new boolean[POSITIONS]));
            }
        },
        ALL_NULLS {
            @Override
            Block getInputBlock()
            {
                return createArrayBlock(Optional.of(ALL_NULLS_ARRAY));
            }
        },
        RANDOM_NULLS {
            @Override
            Block getInputBlock()
            {
                return createArrayBlock(Optional.of(RANDOM_NULLS_ARRAY));
            }
        },
        GROUPED_NULLS {
            @Override
            Block getInputBlock()
            {
                return createArrayBlock(Optional.of(GROUPED_NULLS_ARRAY));
            }
        };

        abstract Block getInputBlock();

        private static Block createArrayBlock(Optional<boolean[]> valueIsNull)
        {
            int[] arrayOffset = generateOffsets(valueIsNull);
            return fromElementBlock(POSITIONS, valueIsNull, arrayOffset, createLongsBlockWithRandomNulls(arrayOffset[POSITIONS]));
        }
    }

    @Test(dataProvider = "mapBlockProvider")
    public void testWriteMapDefinitionLevels(MapBlockProvider blockProvider)
    {
        ColumnarMap columnarMap = toColumnarMap(blockProvider.getInputBlock());
        int keysMaxDefinitionLevel = 2;
        int valuesMaxDefinitionLevel = 3;
        // Write definition levels for all positions
        assertDefinitionLevels(
                columnarMap,
                ImmutableList.of(),
                keysMaxDefinitionLevel,
                valuesMaxDefinitionLevel);

        // Write definition levels for all positions one-at-a-time
        assertDefinitionLevels(
                columnarMap,
                nCopies(columnarMap.getPositionCount(), 1),
                keysMaxDefinitionLevel,
                valuesMaxDefinitionLevel);

        // Write definition levels for all positions with different group sizes
        assertDefinitionLevels(
                columnarMap,
                generateGroupSizes(columnarMap.getPositionCount()),
                keysMaxDefinitionLevel,
                valuesMaxDefinitionLevel);
    }

    @DataProvider
    public static Object[][] mapBlockProvider()
    {
        return Stream.of(MapBlockProvider.values())
                .collect(toDataProvider());
    }

    private enum MapBlockProvider
    {
        NO_NULLS {
            @Override
            Block getInputBlock()
            {
                return createMapBlock(Optional.empty());
            }
        },
        NO_NULLS_WITH_MAY_HAVE_NULL {
            @Override
            Block getInputBlock()
            {
                return createMapBlock(Optional.of(new boolean[POSITIONS]));
            }
        },
        ALL_NULLS {
            @Override
            Block getInputBlock()
            {
                return createMapBlock(Optional.of(ALL_NULLS_ARRAY));
            }
        },
        RANDOM_NULLS {
            @Override
            Block getInputBlock()
            {
                return createMapBlock(Optional.of(RANDOM_NULLS_ARRAY));
            }
        },
        GROUPED_NULLS {
            @Override
            Block getInputBlock()
            {
                return createMapBlock(Optional.of(GROUPED_NULLS_ARRAY));
            }
        };

        abstract Block getInputBlock();

        private static Block createMapBlock(Optional<boolean[]> mapIsNull)
        {
            int[] offsets = generateOffsets(mapIsNull);
            int positionCount = offsets[POSITIONS];
            Block keyBlock = new LongArrayBlock(positionCount, Optional.empty(), new long[positionCount]);
            Block valueBlock = createLongsBlockWithRandomNulls(positionCount);
            return fromKeyValueBlock(mapIsNull, offsets, keyBlock, valueBlock, new MapType(BIGINT, BIGINT, TYPE_OPERATORS));
        }
    }

    private static class TestingValuesWriter
            extends ValuesWriter
    {
        private final IntList values = new IntArrayList();

        @Override
        public long getBufferedSize()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public BytesInput getBytes()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Encoding getEncoding()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void reset()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getAllocatedSize()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String memUsageString(String prefix)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeInteger(int v)
        {
            values.add(v);
        }

        List<Integer> getWrittenValues()
        {
            return values;
        }
    }

    private static void assertDefinitionLevels(Block block, List<Integer> writePositionCounts, int maxDefinitionLevel)
    {
        TestingValuesWriter valuesWriter = new TestingValuesWriter();
        DefinitionLevelWriter primitiveDefLevelWriter = DefLevelWriterProviders.of(block, maxDefinitionLevel)
                .getDefinitionLevelWriter(Optional.empty(), valuesWriter);
        ValuesCount primitiveValuesCount;
        if (writePositionCounts.isEmpty()) {
            primitiveValuesCount = primitiveDefLevelWriter.writeDefinitionLevels();
        }
        else {
            int totalValuesCount = 0;
            int maxDefinitionLevelValuesCount = 0;
            for (int position = 0; position < block.getPositionCount(); position++) {
                ValuesCount valuesCount = primitiveDefLevelWriter.writeDefinitionLevels(1);
                totalValuesCount += valuesCount.totalValuesCount();
                maxDefinitionLevelValuesCount += valuesCount.maxDefinitionLevelValuesCount();
            }
            primitiveValuesCount = new ValuesCount(totalValuesCount, maxDefinitionLevelValuesCount);
        }

        int maxDefinitionValuesCount = 0;
        ImmutableList.Builder<Integer> expectedDefLevelsBuilder = ImmutableList.builder();
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                expectedDefLevelsBuilder.add(maxDefinitionLevel - 1);
            }
            else {
                expectedDefLevelsBuilder.add(maxDefinitionLevel);
                maxDefinitionValuesCount++;
            }
        }
        assertThat(primitiveValuesCount.totalValuesCount()).isEqualTo(block.getPositionCount());
        assertThat(primitiveValuesCount.maxDefinitionLevelValuesCount()).isEqualTo(maxDefinitionValuesCount);
        assertThat(valuesWriter.getWrittenValues()).isEqualTo(expectedDefLevelsBuilder.build());
    }

    private static void assertDefinitionLevels(
            ColumnarRow columnarRow,
            List<Integer> writePositionCounts,
            int field,
            int maxDefinitionLevel)
    {
        // Write definition levels
        TestingValuesWriter valuesWriter = new TestingValuesWriter();
        DefinitionLevelWriter fieldRootDefLevelWriter = getRootDefinitionLevelWriter(
                ImmutableList.of(
                        DefLevelWriterProviders.of(columnarRow, maxDefinitionLevel - 1),
                        DefLevelWriterProviders.of(columnarRow.getField(field), maxDefinitionLevel)),
                valuesWriter);
        ValuesCount fieldValuesCount;
        if (writePositionCounts.isEmpty()) {
            fieldValuesCount = fieldRootDefLevelWriter.writeDefinitionLevels();
        }
        else {
            int totalValuesCount = 0;
            int maxDefinitionLevelValuesCount = 0;
            for (int positionsCount : writePositionCounts) {
                ValuesCount valuesCount = fieldRootDefLevelWriter.writeDefinitionLevels(positionsCount);
                totalValuesCount += valuesCount.totalValuesCount();
                maxDefinitionLevelValuesCount += valuesCount.maxDefinitionLevelValuesCount();
            }
            fieldValuesCount = new ValuesCount(totalValuesCount, maxDefinitionLevelValuesCount);
        }

        // Verify written definition levels
        int maxDefinitionValuesCount = 0;
        ImmutableList.Builder<Integer> expectedDefLevelsBuilder = ImmutableList.builder();
        int fieldOffset = 0;
        for (int position = 0; position < columnarRow.getPositionCount(); position++) {
            if (columnarRow.isNull(position)) {
                expectedDefLevelsBuilder.add(maxDefinitionLevel - 2);
                continue;
            }
            Block fieldBlock = columnarRow.getField(field);
            if (fieldBlock.isNull(fieldOffset)) {
                expectedDefLevelsBuilder.add(maxDefinitionLevel - 1);
            }
            else {
                expectedDefLevelsBuilder.add(maxDefinitionLevel);
                maxDefinitionValuesCount++;
            }
            fieldOffset++;
        }
        assertThat(fieldValuesCount.totalValuesCount()).isEqualTo(columnarRow.getPositionCount());
        assertThat(fieldValuesCount.maxDefinitionLevelValuesCount()).isEqualTo(maxDefinitionValuesCount);
        assertThat(valuesWriter.getWrittenValues()).isEqualTo(expectedDefLevelsBuilder.build());
    }

    private static void assertDefinitionLevels(
            ColumnarArray columnarArray,
            List<Integer> writePositionCounts,
            int maxDefinitionLevel)
    {
        // Write definition levels
        TestingValuesWriter valuesWriter = new TestingValuesWriter();
        DefinitionLevelWriter elementsRootDefLevelWriter = getRootDefinitionLevelWriter(
                ImmutableList.of(
                        DefLevelWriterProviders.of(columnarArray, maxDefinitionLevel - 1),
                        DefLevelWriterProviders.of(columnarArray.getElementsBlock(), maxDefinitionLevel)),
                valuesWriter);
        ValuesCount elementsValuesCount;
        if (writePositionCounts.isEmpty()) {
            elementsValuesCount = elementsRootDefLevelWriter.writeDefinitionLevels();
        }
        else {
            int totalValuesCount = 0;
            int maxDefinitionLevelValuesCount = 0;
            for (int positionsCount : writePositionCounts) {
                ValuesCount valuesCount = elementsRootDefLevelWriter.writeDefinitionLevels(positionsCount);
                totalValuesCount += valuesCount.totalValuesCount();
                maxDefinitionLevelValuesCount += valuesCount.maxDefinitionLevelValuesCount();
            }
            elementsValuesCount = new ValuesCount(totalValuesCount, maxDefinitionLevelValuesCount);
        }

        // Verify written definition levels
        int maxDefinitionValuesCount = 0;
        int totalValuesCount = 0;
        ImmutableList.Builder<Integer> expectedDefLevelsBuilder = ImmutableList.builder();
        int elementsOffset = 0;
        for (int position = 0; position < columnarArray.getPositionCount(); position++) {
            if (columnarArray.isNull(position)) {
                expectedDefLevelsBuilder.add(maxDefinitionLevel - 3);
                totalValuesCount++;
                continue;
            }
            int arrayLength = columnarArray.getLength(position);
            if (arrayLength == 0) {
                expectedDefLevelsBuilder.add(maxDefinitionLevel - 2);
                totalValuesCount++;
                continue;
            }
            totalValuesCount += arrayLength;
            Block elementsBlock = columnarArray.getElementsBlock();
            for (int i = elementsOffset; i < elementsOffset + arrayLength; i++) {
                if (elementsBlock.isNull(i)) {
                    expectedDefLevelsBuilder.add(maxDefinitionLevel - 1);
                }
                else {
                    expectedDefLevelsBuilder.add(maxDefinitionLevel);
                    maxDefinitionValuesCount++;
                }
            }
            elementsOffset += arrayLength;
        }
        assertThat(elementsValuesCount.totalValuesCount()).isEqualTo(totalValuesCount);
        assertThat(elementsValuesCount.maxDefinitionLevelValuesCount()).isEqualTo(maxDefinitionValuesCount);
        assertThat(valuesWriter.getWrittenValues()).isEqualTo(expectedDefLevelsBuilder.build());
    }

    private static void assertDefinitionLevels(
            ColumnarMap columnarMap,
            List<Integer> writePositionCounts,
            int keysMaxDefinitionLevel,
            int valuesMaxDefinitionLevel)
    {
        // Write definition levels for map keys
        TestingValuesWriter keysWriter = new TestingValuesWriter();
        DefinitionLevelWriter keysRootDefLevelWriter = getRootDefinitionLevelWriter(
                ImmutableList.of(
                        DefLevelWriterProviders.of(columnarMap, keysMaxDefinitionLevel),
                        DefLevelWriterProviders.of(columnarMap.getKeysBlock(), keysMaxDefinitionLevel)),
                keysWriter);
        ValuesCount keysValueCount;
        if (writePositionCounts.isEmpty()) {
            keysValueCount = keysRootDefLevelWriter.writeDefinitionLevels();
        }
        else {
            int totalValuesCount = 0;
            int maxDefinitionLevelValuesCount = 0;
            for (int positionsCount : writePositionCounts) {
                ValuesCount valuesCount = keysRootDefLevelWriter.writeDefinitionLevels(positionsCount);
                totalValuesCount += valuesCount.totalValuesCount();
                maxDefinitionLevelValuesCount += valuesCount.maxDefinitionLevelValuesCount();
            }
            keysValueCount = new ValuesCount(totalValuesCount, maxDefinitionLevelValuesCount);
        }

        // Write definition levels for map values
        TestingValuesWriter valuesWriter = new TestingValuesWriter();
        DefinitionLevelWriter valuesRootDefLevelWriter = getRootDefinitionLevelWriter(
                ImmutableList.of(
                        DefLevelWriterProviders.of(columnarMap, keysMaxDefinitionLevel),
                        DefLevelWriterProviders.of(columnarMap.getValuesBlock(), valuesMaxDefinitionLevel)),
                valuesWriter);
        ValuesCount valuesValueCount;
        if (writePositionCounts.isEmpty()) {
            valuesValueCount = valuesRootDefLevelWriter.writeDefinitionLevels();
        }
        else {
            int totalValuesCount = 0;
            int maxDefinitionLevelValuesCount = 0;
            for (int positionsCount : writePositionCounts) {
                ValuesCount valuesCount = valuesRootDefLevelWriter.writeDefinitionLevels(positionsCount);
                totalValuesCount += valuesCount.totalValuesCount();
                maxDefinitionLevelValuesCount += valuesCount.maxDefinitionLevelValuesCount();
            }
            valuesValueCount = new ValuesCount(totalValuesCount, maxDefinitionLevelValuesCount);
        }

        // Verify written definition levels
        int maxDefinitionKeysCount = 0;
        int maxDefinitionValuesCount = 0;
        int totalValuesCount = 0;
        ImmutableList.Builder<Integer> keysExpectedDefLevelsBuilder = ImmutableList.builder();
        ImmutableList.Builder<Integer> valuesExpectedDefLevelsBuilder = ImmutableList.builder();
        int valuesOffset = 0;
        for (int position = 0; position < columnarMap.getPositionCount(); position++) {
            if (columnarMap.isNull(position)) {
                keysExpectedDefLevelsBuilder.add(keysMaxDefinitionLevel - 2);
                valuesExpectedDefLevelsBuilder.add(valuesMaxDefinitionLevel - 3);
                totalValuesCount++;
                continue;
            }
            int mapLength = columnarMap.getEntryCount(position);
            if (mapLength == 0) {
                keysExpectedDefLevelsBuilder.add(keysMaxDefinitionLevel - 1);
                valuesExpectedDefLevelsBuilder.add(valuesMaxDefinitionLevel - 2);
                totalValuesCount++;
                continue;
            }
            totalValuesCount += mapLength;
            // Map keys cannot be null
            keysExpectedDefLevelsBuilder.addAll(nCopies(mapLength, keysMaxDefinitionLevel));
            maxDefinitionKeysCount += mapLength;
            Block valuesBlock = columnarMap.getValuesBlock();
            for (int i = valuesOffset; i < valuesOffset + mapLength; i++) {
                if (valuesBlock.isNull(i)) {
                    valuesExpectedDefLevelsBuilder.add(valuesMaxDefinitionLevel - 1);
                }
                else {
                    valuesExpectedDefLevelsBuilder.add(valuesMaxDefinitionLevel);
                    maxDefinitionValuesCount++;
                }
            }
            valuesOffset += mapLength;
        }
        assertThat(keysValueCount.totalValuesCount()).isEqualTo(totalValuesCount);
        assertThat(keysValueCount.maxDefinitionLevelValuesCount()).isEqualTo(maxDefinitionKeysCount);
        assertThat(keysWriter.getWrittenValues()).isEqualTo(keysExpectedDefLevelsBuilder.build());

        assertThat(valuesValueCount.totalValuesCount()).isEqualTo(totalValuesCount);
        assertThat(valuesValueCount.maxDefinitionLevelValuesCount()).isEqualTo(maxDefinitionValuesCount);
        assertThat(valuesWriter.getWrittenValues()).isEqualTo(valuesExpectedDefLevelsBuilder.build());
    }

    private static List<Integer> generateGroupSizes(int positionsCount)
    {
        int maxGroupSize = 17;
        int offset = 0;
        ImmutableList.Builder<Integer> groupsBuilder = ImmutableList.builder();
        while (offset < positionsCount) {
            int remaining = positionsCount - offset;
            int groupSize = Math.min(RANDOM.nextInt(maxGroupSize) + 1, remaining);
            groupsBuilder.add(groupSize);
            offset += groupSize;
        }
        return groupsBuilder.build();
    }

    private static int[] generateOffsets(Optional<boolean[]> valueIsNull)
    {
        int maxCardinality = 7; // array length or map size at the current position
        int[] offsets = new int[POSITIONS + 1];
        for (int position = 0; position < POSITIONS; position++) {
            if (valueIsNull.isPresent() && valueIsNull.get()[position]) {
                offsets[position + 1] = offsets[position];
            }
            else {
                offsets[position + 1] = offsets[position] + RANDOM.nextInt(maxCardinality);
            }
        }
        return offsets;
    }

    private static Block createLongsBlockWithRandomNulls(int positionCount)
    {
        boolean[] valueIsNull = new boolean[positionCount];
        for (int i = 0; i < positionCount; i++) {
            valueIsNull[i] = RANDOM.nextBoolean();
        }
        return new LongArrayBlock(positionCount, Optional.of(valueIsNull), new long[positionCount]);
    }
}
