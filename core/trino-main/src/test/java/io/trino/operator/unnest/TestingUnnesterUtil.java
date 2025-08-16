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

import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.ColumnarMap;
import io.trino.spi.block.RowBlock;
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
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public final class TestingUnnesterUtil
{
    private TestingUnnesterUtil() {}

    static UnnestedLengths calculateMaxCardinalities(Page page, List<Type> replicatedTypes, List<Type> unnestTypes, boolean outer)
    {
        int positionCount = page.getPositionCount();
        int[] maxCardinalities = new int[positionCount];

        int replicatedChannelCount = replicatedTypes.size();
        int unnestChannelCount = unnestTypes.size();

        for (int i = 0; i < unnestChannelCount; i++) {
            Type type = unnestTypes.get(i);
            Block block = page.getBlock(replicatedChannelCount + i);
            assertThat(type instanceof ArrayType || type instanceof MapType).isTrue();

            if (type instanceof ArrayType) {
                ColumnarArray columnarArray = ColumnarArray.toColumnarArray(block);
                for (int j = 0; j < positionCount; j++) {
                    maxCardinalities[j] = max(maxCardinalities[j], columnarArray.getLength(j));
                }
            }
            else {
                ColumnarMap columnarMap = ColumnarMap.toColumnarMap(block);
                for (int j = 0; j < positionCount; j++) {
                    maxCardinalities[j] = max(maxCardinalities[j], columnarMap.getEntryCount(j));
                }
            }
        }

        if (outer) {
            boolean[] nullAppendForOuter = new boolean[positionCount];
            // For outer node, at least one row should be output for every input row
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
        assertThat(channelCount > 1).isTrue();

        Block[] outputBlocks = new Block[outputTypes.size()];

        int outputChannel = 0;

        for (int i = 0; i < replicatedTypes.size(); i++) {
            outputBlocks[outputChannel++] = buildExpectedReplicatedBlock(page.getBlock(i), replicatedTypes.get(i), maxCardinalities, totalEntries);
        }

        for (int i = 0; i < unnestTypes.size(); i++) {
            Type type = unnestTypes.get(i);
            Block inputBlock = page.getBlock(replicatedTypes.size() + i);

            if (type instanceof ArrayType arrayType) {
                Type elementType = arrayType.getElementType();
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
            else if (type instanceof MapType mapType) {
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
            outputBlocks[outputChannel] = buildExpectedOrdinalityBlock(unnestedLengths, totalEntries);
        }

        return new Page(outputBlocks);
    }

    static List<Type> buildOutputTypes(List<Type> replicatedTypes, List<Type> unnestTypes, boolean withOrdinality)
    {
        List<Type> outputTypes = new ArrayList<>(replicatedTypes);
        for (Type unnestType : unnestTypes) {
            if (unnestType instanceof ArrayType arrayType) {
                Type elementType = arrayType.getElementType();
                if (elementType instanceof RowType) {
                    List<Type> rowTypes = elementType.getTypeParameters();
                    outputTypes.addAll(rowTypes);
                }
                else {
                    outputTypes.add(elementType);
                }
            }
            else if (unnestType instanceof MapType mapType) {
                outputTypes.add(mapType.getKeyType());
                outputTypes.add(mapType.getValueType());
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
            verify(page.getChannelCount() == types.size(), "Number of channels in page %s is not equal to number of types %s", page.getChannelCount(), types.size());

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
        List<Block> fields = RowBlock.getRowFieldsFromBlock(elementBlock);

        Block[] blocks = new Block[fields.size()];
        int positionCount = block.getPositionCount();
        for (int i = 0; i < fields.size(); i++) {
            BlockBuilder blockBuilder = rowTypes.get(i).createBlockBuilder(null, totalEntries);

            for (int j = 0; j < positionCount; j++) {
                int rowBlockIndex = columnarArray.getOffset(j);
                int cardinality = columnarArray.getLength(j);
                for (int k = 0; k < cardinality; k++) {
                    rowTypes.get(i).appendTo(fields.get(i), rowBlockIndex + k, blockBuilder);
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
        BlockBuilder ordinalityBlockBuilder = BIGINT.createFixedSizeBlockBuilder(totalEntries);
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
            nullAppendForOuter.ifPresent(booleans -> checkArgument(maxCardinalities.length == booleans.length));
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
