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
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.parquet.ParquetTestUtils.createArrayBlock;
import static io.trino.parquet.ParquetTestUtils.createMapBlock;
import static io.trino.parquet.ParquetTestUtils.createRowBlock;
import static io.trino.parquet.ParquetTestUtils.generateGroupSizes;
import static io.trino.parquet.writer.repdef.DefLevelWriterProvider.DefinitionLevelWriter;
import static io.trino.parquet.writer.repdef.DefLevelWriterProvider.ValuesCount;
import static io.trino.parquet.writer.repdef.DefLevelWriterProvider.getRootDefinitionLevelWriter;
import static io.trino.spi.block.ColumnarArray.toColumnarArray;
import static io.trino.spi.block.ColumnarMap.toColumnarMap;
import static io.trino.spi.block.ColumnarRow.toColumnarRow;
import static java.util.Collections.nCopies;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDefinitionLevelWriter
{
    private static final int POSITIONS = 8096;

    @Test(dataProviderClass = NullsProvider.class, dataProvider = "nullsProviders")
    public void testWritePrimitiveDefinitionLevels(NullsProvider nullsProvider)
    {
        Block block = new LongArrayBlock(POSITIONS, nullsProvider.getNulls(POSITIONS), new long[POSITIONS]);
        int maxDefinitionLevel = 3;
        // Write definition levels for all positions
        assertDefinitionLevels(block, ImmutableList.of(), maxDefinitionLevel);

        // Write definition levels for all positions one-at-a-time
        assertDefinitionLevels(block, nCopies(block.getPositionCount(), 1), maxDefinitionLevel);

        // Write definition levels for all positions with different group sizes
        assertDefinitionLevels(block, generateGroupSizes(block.getPositionCount()), maxDefinitionLevel);
    }

    @Test(dataProviderClass = NullsProvider.class, dataProvider = "nullsProviders")
    public void testWriteRowDefinitionLevels(NullsProvider nullsProvider)
    {
        Block rowBlock = createRowBlock(nullsProvider.getNulls(POSITIONS), POSITIONS);
        ColumnarRow columnarRow = toColumnarRow(rowBlock);
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

    @Test(dataProviderClass = NullsProvider.class, dataProvider = "nullsProviders")
    public void testWriteArrayDefinitionLevels(NullsProvider nullsProvider)
    {
        Block arrayBlock = createArrayBlock(nullsProvider.getNulls(POSITIONS), POSITIONS);
        ColumnarArray columnarArray = toColumnarArray(arrayBlock);
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

    @Test(dataProviderClass = NullsProvider.class, dataProvider = "nullsProviders")
    public void testWriteMapDefinitionLevels(NullsProvider nullsProvider)
    {
        Block mapBlock = createMapBlock(nullsProvider.getNulls(POSITIONS), POSITIONS);
        ColumnarMap columnarMap = toColumnarMap(mapBlock);
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
}
