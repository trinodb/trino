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
import com.google.common.primitives.Booleans;
import io.trino.parquet.writer.repdef.RepLevelWriterProviders;
import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.ColumnarMap;
import io.trino.spi.block.ColumnarRow;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static io.trino.parquet.ParquetTestUtils.createArrayBlock;
import static io.trino.parquet.ParquetTestUtils.createMapBlock;
import static io.trino.parquet.ParquetTestUtils.createRowBlock;
import static io.trino.parquet.ParquetTestUtils.generateGroupSizes;
import static io.trino.parquet.ParquetTestUtils.generateOffsets;
import static io.trino.parquet.writer.NullsProvider.RANDOM_NULLS;
import static io.trino.parquet.writer.repdef.RepLevelWriterProvider.RepetitionLevelWriter;
import static io.trino.parquet.writer.repdef.RepLevelWriterProvider.getRootRepetitionLevelWriter;
import static io.trino.spi.block.ArrayBlock.fromElementBlock;
import static io.trino.spi.block.ColumnarArray.toColumnarArray;
import static io.trino.spi.block.ColumnarMap.toColumnarMap;
import static io.trino.spi.block.ColumnarRow.toColumnarRow;
import static io.trino.spi.block.MapBlock.fromKeyValueBlock;
import static io.trino.spi.block.RowBlock.fromFieldBlocks;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.Math.toIntExact;
import static java.util.Collections.nCopies;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRepetitionLevelWriter
{
    private static final int POSITIONS = 1024;

    @Test(dataProviderClass = NullsProvider.class, dataProvider = "nullsProviders")
    public void testWriteRowRepetitionLevels(NullsProvider nullsProvider)
    {
        // Using an array of row blocks for testing as Structs don't have a repetition level by themselves
        Optional<boolean[]> valueIsNull = RANDOM_NULLS.getNulls(POSITIONS);
        int[] arrayOffsets = generateOffsets(valueIsNull, POSITIONS);
        int rowBlockPositions = arrayOffsets[POSITIONS];
        Block rowBlock = createRowBlock(nullsProvider.getNulls(rowBlockPositions), rowBlockPositions);
        Block arrayBlock = fromElementBlock(POSITIONS, valueIsNull, arrayOffsets, rowBlock);

        ColumnarArray columnarArray = toColumnarArray(arrayBlock);
        ColumnarRow columnarRow = toColumnarRow(columnarArray.getElementsBlock());
        // Write Repetition levels for all positions
        for (int field = 0; field < columnarRow.getFieldCount(); field++) {
            assertRepetitionLevels(columnarArray, columnarRow, field, ImmutableList.of());
            assertRepetitionLevels(columnarArray, columnarRow, field, ImmutableList.of());

            // Write Repetition levels for all positions one-at-a-time
            assertRepetitionLevels(
                    columnarArray,
                    columnarRow,
                    field,
                    nCopies(columnarArray.getPositionCount(), 1));

            // Write Repetition levels for all positions with different group sizes
            assertRepetitionLevels(
                    columnarArray,
                    columnarRow,
                    field,
                    generateGroupSizes(columnarArray.getPositionCount()));
        }
    }

    @Test(dataProviderClass = NullsProvider.class, dataProvider = "nullsProviders")
    public void testWriteArrayRepetitionLevels(NullsProvider nullsProvider)
    {
        Block arrayBlock = createArrayBlock(nullsProvider.getNulls(POSITIONS), POSITIONS);
        ColumnarArray columnarArray = toColumnarArray(arrayBlock);
        // Write Repetition levels for all positions
        assertRepetitionLevels(columnarArray, ImmutableList.of());

        // Write Repetition levels for all positions one-at-a-time
        assertRepetitionLevels(columnarArray, nCopies(columnarArray.getPositionCount(), 1));

        // Write Repetition levels for all positions with different group sizes
        assertRepetitionLevels(columnarArray, generateGroupSizes(columnarArray.getPositionCount()));
    }

    @Test(dataProviderClass = NullsProvider.class, dataProvider = "nullsProviders")
    public void testWriteMapRepetitionLevels(NullsProvider nullsProvider)
    {
        Block mapBlock = createMapBlock(nullsProvider.getNulls(POSITIONS), POSITIONS);
        ColumnarMap columnarMap = toColumnarMap(mapBlock);
        // Write Repetition levels for all positions
        assertRepetitionLevels(columnarMap, ImmutableList.of());

        // Write Repetition levels for all positions one-at-a-time
        assertRepetitionLevels(columnarMap, nCopies(columnarMap.getPositionCount(), 1));

        // Write Repetition levels for all positions with different group sizes
        assertRepetitionLevels(columnarMap, generateGroupSizes(columnarMap.getPositionCount()));
    }

    @Test(dataProviderClass = NullsProvider.class, dataProvider = "nullsProviders")
    public void testNestedStructRepetitionLevels(NullsProvider nullsProvider)
    {
        Block rowBlock = createNestedRowBlock(nullsProvider.getNulls(POSITIONS), POSITIONS);
        ColumnarRow columnarRow = toColumnarRow(rowBlock);

        for (int field = 0; field < columnarRow.getFieldCount(); field++) {
            Block fieldBlock = columnarRow.getField(field);
            ColumnarMap columnarMap = toColumnarMap(fieldBlock);
            for (Block mapElements : ImmutableList.of(columnarMap.getKeysBlock(), columnarMap.getValuesBlock())) {
                ColumnarArray columnarArray = toColumnarArray(mapElements);

                // Write Repetition levels for all positions
                assertRepetitionLevels(rowBlock, columnarMap, columnarArray, ImmutableList.of());

                // Write Repetition levels for all positions one-at-a-time
                assertRepetitionLevels(rowBlock, columnarMap, columnarArray, nCopies(columnarRow.getPositionCount(), 1));

                // Write Repetition levels for all positions with different group sizes
                assertRepetitionLevels(rowBlock, columnarMap, columnarArray, generateGroupSizes(columnarRow.getPositionCount()));
            }
        }
    }

    private static Block createNestedRowBlock(Optional<boolean[]> rowIsNull, int positionCount)
    {
        int fieldPositionCount = rowIsNull.map(nulls -> toIntExact(Booleans.asList(nulls).stream().filter(isNull -> !isNull).count()))
                .orElse(positionCount);
        Block[] fieldBlocks = new Block[2];
        // no nulls map block
        fieldBlocks[0] = createMapOfArraysBlock(Optional.empty(), fieldPositionCount);
        // random nulls map block
        fieldBlocks[1] = createMapOfArraysBlock(RANDOM_NULLS.getNulls(fieldPositionCount), fieldPositionCount);

        return fromFieldBlocks(positionCount, rowIsNull, fieldBlocks);
    }

    private static Block createMapOfArraysBlock(Optional<boolean[]> mapIsNull, int positionCount)
    {
        int[] offsets = generateOffsets(mapIsNull, positionCount);
        int entriesCount = offsets[positionCount];
        Block keyBlock = createArrayBlock(Optional.empty(), entriesCount);
        Block valueBlock = createArrayBlock(RANDOM_NULLS.getNulls(entriesCount), entriesCount);
        return fromKeyValueBlock(mapIsNull, offsets, keyBlock, valueBlock, new MapType(BIGINT, BIGINT, new TypeOperators()));
    }

    private static void assertRepetitionLevels(
            ColumnarArray columnarArray,
            ColumnarRow columnarRow,
            int field,
            List<Integer> writePositionCounts)
    {
        int maxRepetitionLevel = 1;
        // Write Repetition levels
        TestingValuesWriter valuesWriter = new TestingValuesWriter();
        RepetitionLevelWriter fieldRootRepLevelWriter = getRootRepetitionLevelWriter(
                ImmutableList.of(
                        RepLevelWriterProviders.of(columnarArray, maxRepetitionLevel),
                        RepLevelWriterProviders.of(columnarRow),
                        RepLevelWriterProviders.of(columnarRow.getField(field))),
                valuesWriter);
        if (writePositionCounts.isEmpty()) {
            fieldRootRepLevelWriter.writeRepetitionLevels(0);
        }
        else {
            for (int positionsCount : writePositionCounts) {
                fieldRootRepLevelWriter.writeRepetitionLevels(0, positionsCount);
            }
        }

        // Verify written Repetition levels
        Iterator<Integer> expectedRepetitionLevelsIter = RepLevelIterables.getIterator(ImmutableList.<RepLevelIterable>builder()
                .add(RepLevelIterables.of(columnarArray, maxRepetitionLevel))
                .add(RepLevelIterables.of(columnarArray.getElementsBlock()))
                .add(RepLevelIterables.of(columnarRow.getField(field)))
                .build());
        assertThat(valuesWriter.getWrittenValues()).isEqualTo(ImmutableList.copyOf(expectedRepetitionLevelsIter));
    }

    private static void assertRepetitionLevels(
            ColumnarArray columnarArray,
            List<Integer> writePositionCounts)
    {
        int maxRepetitionLevel = 1;
        // Write Repetition levels
        TestingValuesWriter valuesWriter = new TestingValuesWriter();
        RepetitionLevelWriter elementsRootRepLevelWriter = getRootRepetitionLevelWriter(
                ImmutableList.of(
                        RepLevelWriterProviders.of(columnarArray, maxRepetitionLevel),
                        RepLevelWriterProviders.of(columnarArray.getElementsBlock())),
                valuesWriter);
        if (writePositionCounts.isEmpty()) {
            elementsRootRepLevelWriter.writeRepetitionLevels(0);
        }
        else {
            for (int positionsCount : writePositionCounts) {
                elementsRootRepLevelWriter.writeRepetitionLevels(0, positionsCount);
            }
        }

        // Verify written Repetition levels
        ImmutableList.Builder<Integer> expectedRepLevelsBuilder = ImmutableList.builder();
        int elementsOffset = 0;
        for (int position = 0; position < columnarArray.getPositionCount(); position++) {
            if (columnarArray.isNull(position)) {
                expectedRepLevelsBuilder.add(maxRepetitionLevel - 1);
                continue;
            }
            int arrayLength = columnarArray.getLength(position);
            if (arrayLength == 0) {
                expectedRepLevelsBuilder.add(maxRepetitionLevel - 1);
                continue;
            }
            expectedRepLevelsBuilder.add(maxRepetitionLevel - 1);
            for (int i = elementsOffset + 1; i < elementsOffset + arrayLength; i++) {
                expectedRepLevelsBuilder.add(maxRepetitionLevel);
            }
            elementsOffset += arrayLength;
        }
        assertThat(valuesWriter.getWrittenValues()).isEqualTo(expectedRepLevelsBuilder.build());
    }

    private static void assertRepetitionLevels(
            ColumnarMap columnarMap,
            List<Integer> writePositionCounts)
    {
        int maxRepetitionLevel = 1;
        // Write Repetition levels for map keys
        TestingValuesWriter keysWriter = new TestingValuesWriter();
        RepetitionLevelWriter keysRootRepLevelWriter = getRootRepetitionLevelWriter(
                ImmutableList.of(
                        RepLevelWriterProviders.of(columnarMap, maxRepetitionLevel),
                        RepLevelWriterProviders.of(columnarMap.getKeysBlock())),
                keysWriter);
        if (writePositionCounts.isEmpty()) {
            keysRootRepLevelWriter.writeRepetitionLevels(0);
        }
        else {
            for (int positionsCount : writePositionCounts) {
                keysRootRepLevelWriter.writeRepetitionLevels(0, positionsCount);
            }
        }

        // Write Repetition levels for map values
        TestingValuesWriter valuesWriter = new TestingValuesWriter();
        RepetitionLevelWriter valuesRootRepLevelWriter = getRootRepetitionLevelWriter(
                ImmutableList.of(
                        RepLevelWriterProviders.of(columnarMap, maxRepetitionLevel),
                        RepLevelWriterProviders.of(columnarMap.getValuesBlock())),
                valuesWriter);
        if (writePositionCounts.isEmpty()) {
            valuesRootRepLevelWriter.writeRepetitionLevels(0);
        }
        else {
            for (int positionsCount : writePositionCounts) {
                valuesRootRepLevelWriter.writeRepetitionLevels(0, positionsCount);
            }
        }

        // Verify written Repetition levels
        ImmutableList.Builder<Integer> expectedRepLevelsBuilder = ImmutableList.builder();
        for (int position = 0; position < columnarMap.getPositionCount(); position++) {
            if (columnarMap.isNull(position)) {
                expectedRepLevelsBuilder.add(maxRepetitionLevel - 1);
                continue;
            }
            int mapLength = columnarMap.getEntryCount(position);
            if (mapLength == 0) {
                expectedRepLevelsBuilder.add(maxRepetitionLevel - 1);
                continue;
            }
            expectedRepLevelsBuilder.add(maxRepetitionLevel - 1);
            expectedRepLevelsBuilder.addAll(nCopies(mapLength - 1, maxRepetitionLevel));
        }
        assertThat(keysWriter.getWrittenValues()).isEqualTo(expectedRepLevelsBuilder.build());
        assertThat(valuesWriter.getWrittenValues()).isEqualTo(expectedRepLevelsBuilder.build());
    }

    private static void assertRepetitionLevels(
            Block rowBlock,
            ColumnarMap columnarMap,
            ColumnarArray columnarArray,
            List<Integer> writePositionCounts)
    {
        // Write Repetition levels
        TestingValuesWriter valuesWriter = new TestingValuesWriter();
        RepetitionLevelWriter fieldRootRepLevelWriter = getRootRepetitionLevelWriter(
                ImmutableList.of(
                        RepLevelWriterProviders.of(toColumnarRow(rowBlock)),
                        RepLevelWriterProviders.of(columnarMap, 1),
                        RepLevelWriterProviders.of(columnarArray, 2),
                        RepLevelWriterProviders.of(columnarArray.getElementsBlock())),
                valuesWriter);
        if (writePositionCounts.isEmpty()) {
            fieldRootRepLevelWriter.writeRepetitionLevels(0);
        }
        else {
            for (int positionsCount : writePositionCounts) {
                fieldRootRepLevelWriter.writeRepetitionLevels(0, positionsCount);
            }
        }

        // Verify written Repetition levels
        Iterator<Integer> expectedRepetitionLevelsIter = RepLevelIterables.getIterator(ImmutableList.<RepLevelIterable>builder()
                .add(RepLevelIterables.of(rowBlock))
                .add(RepLevelIterables.of(columnarMap, 1))
                .add(RepLevelIterables.of(columnarArray, 2))
                .add(RepLevelIterables.of(columnarArray.getElementsBlock()))
                .build());
        assertThat(valuesWriter.getWrittenValues()).isEqualTo(ImmutableList.copyOf(expectedRepetitionLevelsIter));
    }
}
