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
package io.trino.operator.unnest;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.ColumnarMap;
import io.trino.spi.block.ColumnarRow;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public final class TestingUnnesterUtil
{
    private TestingUnnesterUtil() {}

    public static Block createSimpleBlock(Slice[] values)
    {
        BlockBuilder elementBlockBuilder = VARCHAR.createBlockBuilder(null, values.length);
        for (Slice v : values) {
            if (v == null) {
                elementBlockBuilder.appendNull();
            }
            else {
                VARCHAR.writeSlice(elementBlockBuilder, v);
            }
        }
        return elementBlockBuilder.build();
    }

    public static Block createArrayBlock(Slice[][] values)
    {
        ArrayType arrayType = new ArrayType(VARCHAR);
        BlockBuilder blockBuilder = arrayType.createBlockBuilder(null, 100, 100);
        for (Slice[] expectedValue : values) {
            if (expectedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                arrayType.writeObject(blockBuilder, createSimpleBlock(expectedValue));
            }
        }
        return blockBuilder.build();
    }

    public static Block createArrayBlockOfRowBlocks(Slice[][][] elements, RowType rowType)
    {
        ArrayType arrayType = new ArrayType(rowType);
        BlockBuilder arrayBlockBuilder = arrayType.createBlockBuilder(null, 100, 100);
        for (int i = 0; i < elements.length; i++) {
            if (elements[i] == null) {
                arrayBlockBuilder.appendNull();
            }
            else {
                Slice[][] expectedValues = elements[i];
                RowBlockBuilder elementBlockBuilder = rowType.createBlockBuilder(null, elements[i].length);
                for (Slice[] expectedValue : expectedValues) {
                    if (expectedValue == null) {
                        elementBlockBuilder.appendNull();
                    }
                    else {
                        elementBlockBuilder.buildEntry(fieldBuilders -> {
                            for (int fieldId = 0; fieldId < expectedValue.length; fieldId++) {
                                Slice v = expectedValue[fieldId];
                                if (v == null) {
                                    fieldBuilders.get(fieldId).appendNull();
                                }
                                else {
                                    VARCHAR.writeSlice(fieldBuilders.get(fieldId), v);
                                }
                            }
                        });
                    }
                }
                arrayType.writeObject(arrayBlockBuilder, elementBlockBuilder.build());
            }
        }
        return arrayBlockBuilder.build();
    }

    public static boolean nullExists(Slice[][] elements)
    {
        for (int i = 0; i < elements.length; i++) {
            if (elements[i] != null) {
                for (int j = 0; j < elements[i].length; j++) {
                    if (elements[i][j] == null) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public static Slice[] computeExpectedUnnestedOutput(Slice[][] elements, int[] requiredOutputCounts, int startPosition, int length)
    {
        checkArgument(startPosition >= 0 && length >= 0);
        checkArgument(startPosition + length - 1 < requiredOutputCounts.length);
        checkArgument(elements.length == requiredOutputCounts.length);

        int outputCount = 0;
        for (int i = 0; i < length; i++) {
            int position = startPosition + i;
            int arrayLength = elements[position] == null ? 0 : elements[position].length;
            checkArgument(requiredOutputCounts[position] >= arrayLength);
            outputCount += requiredOutputCounts[position];
        }

        Slice[] expectedOutput = new Slice[outputCount];
        int offset = 0;

        for (int i = 0; i < length; i++) {
            int position = startPosition + i;
            int arrayLength = elements[position] == null ? 0 : elements[position].length;

            int requiredCount = requiredOutputCounts[position];

            for (int j = 0; j < arrayLength; j++) {
                expectedOutput[offset++] = elements[position][j];
            }
            for (int j = 0; j < (requiredCount - arrayLength); j++) {
                expectedOutput[offset++] = null;
            }
        }

        return expectedOutput;
    }

    /**
     * Extract elements corresponding to a specific field from 3D slices
     */
    public static Slice[][] getFieldElements(Slice[][][] slices, int fieldNo)
    {
        Slice[][] output = new Slice[slices.length][];

        for (int i = 0; i < slices.length; i++) {
            if (slices[i] != null) {
                output[i] = new Slice[slices[i].length];

                for (int j = 0; j < slices[i].length; j++) {
                    if (slices[i][j] != null) {
                        output[i][j] = slices[i][j][fieldNo];
                    }
                    else {
                        output[i][j] = null;
                    }
                }
            }
            else {
                output[i] = null;
            }
        }

        return output;
    }

    public static void validateTestInput(int[] requiredOutputCounts, int[] unnestedLengths, Slice[][][] slices, int fieldCount)
    {
        requireNonNull(requiredOutputCounts, "requiredOutputCounts is null");
        requireNonNull(unnestedLengths, "unnestedLengths is null");
        requireNonNull(slices, "slices array is null");

        // verify lengths
        int positionCount = slices.length;
        assertEquals(requiredOutputCounts.length, positionCount);
        assertEquals(unnestedLengths.length, positionCount);

        // Unnested array lengths must be <= required output count
        for (int i = 0; i < requiredOutputCounts.length; i++) {
            assertTrue(unnestedLengths[i] <= requiredOutputCounts[i]);
        }

        // Elements should have the right shape for every field
        for (int index = 0; index < positionCount; index++) {
            Slice[][] entry = slices[index];

            int entryLength = entry != null ? entry.length : 0;
            assertEquals(entryLength, unnestedLengths[index]);

            // Verify number of fields
            for (int i = 0; i < entryLength; i++) {
                if (entry[i] != null) {
                    assertEquals(entry[i].length, fieldCount);
                }
            }
        }
    }

    public static Slice[] createReplicatedOutputSlice(Slice[] input, int[] counts)
    {
        assertEquals(input.length, counts.length);

        int outputLength = 0;
        for (int i = 0; i < input.length; i++) {
            outputLength += counts[i];
        }

        Slice[] output = new Slice[outputLength];
        int offset = 0;
        for (int i = 0; i < input.length; i++) {
            for (int j = 0; j < counts[i]; j++) {
                output[offset++] = input[i];
            }
        }

        return output;
    }

    static Slice[][][] column(Slice[][]... arraysOfRow)
    {
        return arraysOfRow;
    }

    static Slice[][] array(Slice[]... rows)
    {
        return rows;
    }

    static Slice[] toSlices(String... values)
    {
        Slice[] slices = new Slice[values.length];
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                slices[i] = Slices.utf8Slice(values[i]);
            }
        }

        return slices;
    }

    static UnnestedLengths calculateMaxCardinalities(Page page, List<Type> replicatedTypes, List<Type> unnestTypes, boolean outer)
    {
        int positionCount = page.getPositionCount();
        int[] maxCardinalities = new int[positionCount];

        int replicatedChannelCount = replicatedTypes.size();
        int unnestChannelCount = unnestTypes.size();

        for (int i = 0; i < unnestChannelCount; i++) {
            Type type = unnestTypes.get(i);
            Block block = page.getBlock(replicatedChannelCount + i);
            assertTrue(type instanceof ArrayType || type instanceof MapType);

            if (type instanceof ArrayType) {
                ColumnarArray columnarArray = ColumnarArray.toColumnarArray(block);
                for (int j = 0; j < positionCount; j++) {
                    maxCardinalities[j] = max(maxCardinalities[j], columnarArray.getLength(j));
                }
            }
            else if (type instanceof MapType) {
                ColumnarMap columnarMap = ColumnarMap.toColumnarMap(block);
                for (int j = 0; j < positionCount; j++) {
                    maxCardinalities[j] = max(maxCardinalities[j], columnarMap.getEntryCount(j));
                }
            }
            else {
                throw new RuntimeException("expected an ArrayType or MapType, but found " + type);
            }
        }

        if (outer) {
            boolean[] nullAppendForOuter = new boolean[positionCount];
            // For outer node, atleast one row should be output for every input row
            for (int j = 0; j < positionCount; j++) {
                if (maxCardinalities[j] == 0) {
                    maxCardinalities[j] = 1;
                    nullAppendForOuter[j] = true;
                }
            }
            return new UnnestedLengths(maxCardinalities, Optional.of(nullAppendForOuter));
        }

        return new UnnestedLengths(maxCardinalities, Optional.empty());
    }

    static Page buildExpectedPage(
            Page page,
            List<Type> replicatedTypes,
            List<Type> unnestTypes,
            List<Type> outputTypes,
            UnnestedLengths unnestedLengths,
            boolean withOrdinality)
    {
        int totalEntries = IntStream.of(unnestedLengths.getMaxCardinalities()).sum();
        int[] maxCardinalities = unnestedLengths.getMaxCardinalities();

        int channelCount = page.getChannelCount();
        assertTrue(channelCount > 1);

        Block[] outputBlocks = new Block[outputTypes.size()];

        int outputChannel = 0;

        for (int i = 0; i < replicatedTypes.size(); i++) {
            outputBlocks[outputChannel++] = buildExpectedReplicatedBlock(page.getBlock(i), replicatedTypes.get(i), maxCardinalities, totalEntries);
        }

        for (int i = 0; i < unnestTypes.size(); i++) {
            Type type = unnestTypes.get(i);
            Block inputBlock = page.getBlock(replicatedTypes.size() + i);

            if (type instanceof ArrayType) {
                Type elementType = ((ArrayType) type).getElementType();
                if (elementType instanceof RowType) {
                    List<Type> rowTypes = elementType.getTypeParameters();
                    Block[] blocks = buildExpectedUnnestedArrayOfRowBlock(inputBlock, rowTypes, maxCardinalities, totalEntries);
                    for (Block block : blocks) {
                        outputBlocks[outputChannel++] = block;
                    }
                }
                else {
                    outputBlocks[outputChannel++] = buildExpectedUnnestedArrayBlock(inputBlock, ((ArrayType) unnestTypes.get(i)).getElementType(), maxCardinalities, totalEntries);
                }
            }
            else if (type instanceof MapType) {
                MapType mapType = (MapType) unnestTypes.get(i);
                Block[] blocks = buildExpectedUnnestedMapBlocks(inputBlock, mapType.getKeyType(), mapType.getValueType(), maxCardinalities, totalEntries);
                for (Block block : blocks) {
                    outputBlocks[outputChannel++] = block;
                }
            }
            else {
                throw new RuntimeException("expected an ArrayType or MapType, but found " + type);
            }
        }

        if (withOrdinality) {
            outputBlocks[outputChannel++] = buildExpectedOrdinalityBlock(unnestedLengths, totalEntries);
        }

        return new Page(outputBlocks);
    }

    static List<Type> buildOutputTypes(List<Type> replicatedTypes, List<Type> unnestTypes, boolean withOrdinality)
    {
        List<Type> outputTypes = new ArrayList<>();

        for (Type replicatedType : replicatedTypes) {
            outputTypes.add(replicatedType);
        }

        for (Type unnestType : unnestTypes) {
            if (unnestType instanceof ArrayType) {
                Type elementType = ((ArrayType) unnestType).getElementType();
                if (elementType instanceof RowType) {
                    List<Type> rowTypes = ((RowType) elementType).getTypeParameters();
                    for (Type rowType : rowTypes) {
                        outputTypes.add(rowType);
                    }
                }
                else {
                    outputTypes.add(elementType);
                }
            }
            else if (unnestType instanceof MapType) {
                outputTypes.add(((MapType) unnestType).getKeyType());
                outputTypes.add(((MapType) unnestType).getValueType());
            }
        }

        if (withOrdinality) {
            outputTypes.add(BIGINT);
        }

        return outputTypes;
    }

    static Page mergePages(List<Type> types, List<Page> pages)
    {
        PageBuilder pageBuilder = new PageBuilder(types);
        int totalPositionCount = 0;
        for (Page page : pages) {
            verify(page.getChannelCount() == types.size(), format("Number of channels in page %d is not equal to number of types %d", page.getChannelCount(), types.size()));

            for (int i = 0; i < types.size(); i++) {
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
                Block block = page.getBlock(i);
                for (int position = 0; position < page.getPositionCount(); position++) {
                    if (block.isNull(position)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        types.get(i).appendTo(block, position, blockBuilder);
                    }
                }
            }
            totalPositionCount += page.getPositionCount();
        }
        pageBuilder.declarePositions(totalPositionCount);
        return pageBuilder.build();
    }

    private static Block buildExpectedReplicatedBlock(Block block, Type type, int[] maxCardinalities, int totalEntries)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, totalEntries);
        int positionCount = block.getPositionCount();
        for (int i = 0; i < positionCount; i++) {
            int cardinality = maxCardinalities[i];
            for (int j = 0; j < cardinality; j++) {
                type.appendTo(block, i, blockBuilder);
            }
        }
        return blockBuilder.build();
    }

    private static Block buildExpectedUnnestedArrayBlock(Block block, Type type, int[] maxCardinalities, int totalEntries)
    {
        ColumnarArray columnarArray = ColumnarArray.toColumnarArray(block);
        Block elementBlock = columnarArray.getElementsBlock();

        BlockBuilder blockBuilder = type.createBlockBuilder(null, totalEntries);
        int positionCount = block.getPositionCount();
        int elementBlockPosition = 0;
        for (int i = 0; i < positionCount; i++) {
            int cardinality = columnarArray.getLength(i);
            for (int j = 0; j < cardinality; j++) {
                type.appendTo(elementBlock, elementBlockPosition++, blockBuilder);
            }

            int maxCardinality = maxCardinalities[i];
            for (int j = cardinality; j < maxCardinality; j++) {
                blockBuilder.appendNull();
            }
        }
        return blockBuilder.build();
    }

    private static Block[] buildExpectedUnnestedMapBlocks(Block block, Type keyType, Type valueType, int[] maxCardinalities, int totalEntries)
    {
        ColumnarMap columnarMap = ColumnarMap.toColumnarMap(block);
        Block keyBlock = columnarMap.getKeysBlock();
        Block valuesBlock = columnarMap.getValuesBlock();

        BlockBuilder keyBlockBuilder = keyType.createBlockBuilder(null, totalEntries);
        BlockBuilder valueBlockBuilder = valueType.createBlockBuilder(null, totalEntries);

        int positionCount = block.getPositionCount();
        int blockPosition = 0;
        for (int i = 0; i < positionCount; i++) {
            int cardinality = columnarMap.getEntryCount(i);
            for (int j = 0; j < cardinality; j++) {
                keyType.appendTo(keyBlock, blockPosition, keyBlockBuilder);
                valueType.appendTo(valuesBlock, blockPosition, valueBlockBuilder);
                blockPosition++;
            }

            int maxCardinality = maxCardinalities[i];
            for (int j = cardinality; j < maxCardinality; j++) {
                keyBlockBuilder.appendNull();
                valueBlockBuilder.appendNull();
            }
        }

        Block[] blocks = new Block[2];
        blocks[0] = keyBlockBuilder.build();
        blocks[1] = valueBlockBuilder.build();
        return blocks;
    }

    private static Block[] buildExpectedUnnestedArrayOfRowBlock(Block block, List<Type> rowTypes, int[] maxCardinalities, int totalEntries)
    {
        ColumnarArray columnarArray = ColumnarArray.toColumnarArray(block);
        Block elementBlock = columnarArray.getElementsBlock();
        ColumnarRow columnarRow = ColumnarRow.toColumnarRow(elementBlock);

        int fieldCount = columnarRow.getFieldCount();
        Block[] blocks = new Block[fieldCount];

        int positionCount = block.getPositionCount();
        for (int i = 0; i < fieldCount; i++) {
            BlockBuilder blockBuilder = rowTypes.get(i).createBlockBuilder(null, totalEntries);

            int nullRowsEncountered = 0;
            for (int j = 0; j < positionCount; j++) {
                int rowBlockIndex = columnarArray.getOffset(j);
                int cardinality = columnarArray.getLength(j);
                for (int k = 0; k < cardinality; k++) {
                    if (columnarRow.isNull(rowBlockIndex + k)) {
                        blockBuilder.appendNull();
                        nullRowsEncountered++;
                    }
                    else {
                        rowTypes.get(i).appendTo(columnarRow.getField(i), rowBlockIndex + k - nullRowsEncountered, blockBuilder);
                    }
                }

                int maxCardinality = maxCardinalities[j];
                for (int k = cardinality; k < maxCardinality; k++) {
                    blockBuilder.appendNull();
                }
            }

            blocks[i] = blockBuilder.build();
        }

        return blocks;
    }

    private static Block buildExpectedOrdinalityBlock(UnnestedLengths unnestedLengths, int totalEntries)
    {
        int[] maxCardinalities = unnestedLengths.getMaxCardinalities();
        BlockBuilder ordinalityBlockBuilder = BIGINT.createBlockBuilder(null, totalEntries);
        for (int i = 0; i < maxCardinalities.length; i++) {
            int maxCardinality = maxCardinalities[i];
            if (maxCardinality == 1 && unnestedLengths.isNullAppendForOuter(i)) {
                ordinalityBlockBuilder.appendNull();
            }
            else {
                for (int ordinalityCount = 1; ordinalityCount <= maxCardinality; ordinalityCount++) {
                    BIGINT.writeLong(ordinalityBlockBuilder, ordinalityCount);
                }
            }
        }
        return ordinalityBlockBuilder.build();
    }

    static class UnnestedLengths
    {
        private final int[] maxCardinalities;
        private final Optional<boolean[]> nullAppendForOuter;

        public UnnestedLengths(int[] maxCardinalities, Optional<boolean[]> nullAppendForOuter)
        {
            this.maxCardinalities = requireNonNull(maxCardinalities, "maxCardinalities is null");
            this.nullAppendForOuter = requireNonNull(nullAppendForOuter, "nullAppendForOuter is null");

            if (nullAppendForOuter.isPresent()) {
                checkArgument(maxCardinalities.length == nullAppendForOuter.get().length);
            }
        }

        public int[] getMaxCardinalities()
        {
            return maxCardinalities;
        }

        public boolean isNullAppendForOuter(int position)
        {
            if (nullAppendForOuter.isEmpty()) {
                return false;
            }

            checkArgument(position >= 0 && position < nullAppendForOuter.get().length);
            return nullAppendForOuter.get()[position];
        }
    }
}
