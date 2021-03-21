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
package io.trino.operator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.MergeProcessorUtilities;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.connector.MergeProcessorUtilities.DELETE_OPERATION_NUMBER;
import static io.trino.spi.connector.MergeProcessorUtilities.INSERT_OPERATION_NUMBER;
import static io.trino.spi.connector.MergeProcessorUtilities.UPDATE_OPERATION_NUMBER;
import static io.trino.spi.connector.MergeProcessorUtilities.getUnderlyingBlock;
import static io.trino.spi.connector.RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DeleteAndInsertMergeProcessor
        implements MergeRowChangeProcessor
{
    private final List<ColumnHandle> dataColumns;
    private final List<Type> dataColumnTypes;
    private final List<ColumnHandle> writeRedistributionColumns;
    private final Type rowIdType;
    private final List<Integer> dataColumnChannels;
    private final int redistributionColumnCount;
    private final List<Integer> redistributionChannelNumbers;

    private final DuplicateRowFinder duplicateRowFinder;

    public DeleteAndInsertMergeProcessor(List<ColumnHandle> dataColumns, List<Type> dataColumnTypes, List<ColumnHandle> writeRedistributionColumns, Type rowIdType)
    {
        this.dataColumns = requireNonNull(dataColumns, "dataColumns is null");
        this.dataColumnTypes = requireNonNull(dataColumnTypes, "dataColumnTypes is null");
        this.rowIdType = requireNonNull(rowIdType, "rowIdType is null");
        this.redistributionColumnCount = writeRedistributionColumns.size();
        this.writeRedistributionColumns = requireNonNull(writeRedistributionColumns, "writeRedistributionColumns is null");
        ImmutableList.Builder<Integer> dataColumnChannelsBuilder = ImmutableList.builder();
        ImmutableList.Builder<Integer> redistributionChannelNumbersBuilder = ImmutableList.builder();
        int dataColumnChannel = 0;
        int redistributionSourceIndex = 0;
        for (ColumnHandle handle : dataColumns) {
            dataColumnChannelsBuilder.add(dataColumnChannel);
            boolean redistributionChannel = writeRedistributionColumns.contains(handle);
            if (redistributionChannel) {
                redistributionChannelNumbersBuilder.add(redistributionSourceIndex);
                redistributionSourceIndex++;
            }
            else {
                redistributionChannelNumbersBuilder.add(-1);
            }
            dataColumnChannel++;
        }
        this.dataColumnChannels = dataColumnChannelsBuilder.build();
        this.redistributionChannelNumbers = redistributionChannelNumbersBuilder.build();
        this.duplicateRowFinder = new DuplicateRowFinder(dataColumns, dataColumnTypes, writeRedistributionColumns, rowIdType);
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm()
    {
        return DELETE_ROW_AND_INSERT_ROW;
    }

    @JsonProperty
    public List<ColumnHandle> getDataColumns()
    {
        return dataColumns;
    }

    @JsonProperty
    public List<Type> getDataColumnTypes()
    {
        return dataColumnTypes;
    }

    @JsonProperty
    public List<ColumnHandle> getWriteRedistributionColumns()
    {
        return writeRedistributionColumns;
    }

    @JsonProperty
    public Type getRowIdType()
    {
        return rowIdType;
    }

    /**
     * Transform the input page containing the target table's write redistribution column
     * blocks; the rowId block; and the merge case RowBlock. Each row in the output Page
     * starts with all the data column blocks, including the partition columns blocks,
     * table column order, followed by a boolean column indicating whether the source row
     * matched any target row, followed by the "operation" block from the merge case RowBlock,
     * whose values are {@link MergeProcessorUtilities#INSERT_OPERATION_NUMBER},
     * {@link MergeProcessorUtilities#DELETE_OPERATION_NUMBER}, or
     * {@link MergeProcessorUtilities#UPDATE_OPERATION_NUMBER}, or
     * {@link MergeRowChangeProcessor#DEFAULT_CASE_OPERATION_NUMBER}
     * @param inputPage A page containing write redistribution column blocks for the target table; the
     * rowId block; the merge case RowBlock; and for partitioned or bucketed tables, a hash column block.
     * @return A page containing all data columns, the operation block and the rowId block,
     * with UPDATE rows expanded into delete and insert rows, so they can be routed separately.
     * The delete rows contain the partition key blocks from the target table, whereas the insert
     * rows have the partition key blocks from the merge case RowBlock.
     */
    @Override
    public Page transformPage(Page inputPage)
    {
        requireNonNull(inputPage, "inputPage is null");
        int inputChannelCount = inputPage.getChannelCount();
        if (inputChannelCount < 2 + redistributionColumnCount) {
            throw new IllegalArgumentException(format("inputPage channelCount (%s) should be >= 2 + partition columns size (%s)", inputChannelCount, redistributionColumnCount));
        }

        int originalPositionCount = inputPage.getPositionCount();
        if (originalPositionCount <= 0) {
            throw new IllegalArgumentException("originalPositionCount should be > 0, but is " + originalPositionCount);
        }
        Block mergeCaseBlock = inputPage.getBlock(redistributionColumnCount + 1);
        List<Block> mergeCaseBlocks = mergeCaseBlock.getChildren();
        int mergeBlocksSize = mergeCaseBlocks.size();
        Block rowMatchedBlock = mergeCaseBlocks.get(mergeBlocksSize - 2);
        Block operationChannelBlock = mergeCaseBlocks.get(mergeBlocksSize - 1);
        duplicateRowFinder.checkForDuplicateTargetRows(inputPage, operationChannelBlock);

        int updatePositions = 0;
        int insertPositions = 0;
        int deletePositions = 0;
        for (int position = 0; position < originalPositionCount; position++) {
            int operation = (int) TINYINT.getLong(operationChannelBlock, position);
            switch (operation) {
                case DEFAULT_CASE_OPERATION_NUMBER:
                    break;
                case INSERT_OPERATION_NUMBER:
                    insertPositions++;
                    break;
                case DELETE_OPERATION_NUMBER:
                    deletePositions++;
                    break;
                case UPDATE_OPERATION_NUMBER:
                    updatePositions++;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown operator number " + operation);
            }
        }

        int totalPositions = insertPositions + deletePositions + 2 * updatePositions;
        Block rowIdBlock = inputPage.getBlock(redistributionColumnCount);

        ImmutableList.Builder<Type> pageTypesBuilder = ImmutableList.builder();
        pageTypesBuilder.addAll(dataColumnTypes);
        pageTypesBuilder.add(INTEGER);
        List<Type> pageTypes = pageTypesBuilder.build();
        PageBuilder pageBuilder = new PageBuilder(totalPositions, pageTypes);
        int resultPosition = 0;
        int rowIdPosition = 0;
        int[] rowIdPositions = new int[totalPositions];
        for (int position = 0; position < originalPositionCount; position++) {
            long operation = TINYINT.getLong(operationChannelBlock, position);
            if (operation != DEFAULT_CASE_OPERATION_NUMBER) {
                // Delete and Update because both create a delete row
                if (operation == DELETE_OPERATION_NUMBER || operation == UPDATE_OPERATION_NUMBER) {
                    addDeleteRow(pageBuilder, inputPage, position);
                    rowIdPositions[resultPosition] = rowIdPosition;
                    resultPosition++;
                }
                // Insert and update because both create an insert row
                if (operation == INSERT_OPERATION_NUMBER || operation == UPDATE_OPERATION_NUMBER) {
                    addInsertRow(pageBuilder, mergeCaseBlocks, position);
                    rowIdPositions[resultPosition] = 0;
                    resultPosition++;
                }
            }
            if (BOOLEAN.getBoolean(rowMatchedBlock, position)) {
                rowIdPosition++;
            }
        }
        checkArgument(resultPosition == totalPositions, "resultPosition (%s) is not equal to totalPositions (%s)", resultPosition, totalPositions);
        Page pageWithoutRowIdBlock = pageBuilder.build();
        int pageChannels = pageWithoutRowIdBlock.getChannelCount();
        Block[] blocksWithRowId = new Block[pageChannels + 1];
        for (int channel = 0; channel < pageChannels; channel++) {
            blocksWithRowId[channel] = pageWithoutRowIdBlock.getBlock(channel);
        }
        blocksWithRowId[pageChannels] = makeOutputRowIdBlock(rowIdBlock, rowIdPositions);

        return new Page(blocksWithRowId);
    }

    private static Block makeOutputRowIdBlock(Block rowIdBlock, int[] rowIdPositions)
    {
        Block underlyingBlock = getUnderlyingBlock(rowIdBlock);
        int totalPositions = rowIdPositions.length;
        if (rowIdBlock.allPositionsAreNull()) {
            return getAllNullsRowIdBlock(rowIdBlock, underlyingBlock, totalPositions);
        }
        else {
            if (underlyingBlock instanceof RowBlock) {
                List<Block> newRowIdChildrenBuilder = new ArrayList<>();
                rowIdBlock.getChildren().stream()
                        .map(block -> block.getPositions(rowIdPositions, 0, totalPositions))
                        .forEach(newRowIdChildrenBuilder::add);
                return RowBlock.fromFieldBlocks(
                        totalPositions,
                        Optional.empty(),
                        newRowIdChildrenBuilder.toArray(new Block[] {}));
            }
            else {
                return rowIdBlock.getPositions(rowIdPositions, 0, totalPositions);
            }
        }
    }

    private static Block getAllNullsRowIdBlock(Block rowIdBlock, Block underlyingBlock, int positionCount)
    {
        boolean[] nulls = new boolean[positionCount];
        Arrays.fill(nulls, true);
        if (underlyingBlock instanceof RowBlock) {
            return RowBlock.fromFieldBlocks(positionCount, Optional.of(nulls), rowIdBlock.getChildren().toArray(new Block[]{}));
        }
        else {
            return ArrayBlock.fromElementBlock(positionCount, Optional.of(nulls), new int[positionCount], underlyingBlock);
        }
    }

    private void addDeleteRow(PageBuilder pageBuilder, Page originalPage, int position)
    {
        // Copy the write redistribution columns and the rowId column
        for (int targetChannel : dataColumnChannels) {
            int redistributionChannelNumber = redistributionChannelNumbers.get(targetChannel);
            Type columnType = dataColumnTypes.get(targetChannel);
            BlockBuilder targetBlock = pageBuilder.getBlockBuilder(targetChannel);

            if (redistributionChannelNumber >= 0) {
                // The value comes from that column of the page
                columnType.appendTo(originalPage.getBlock(redistributionChannelNumber), position, targetBlock);
            }
            else {
                // We don't care about the other data columns
                targetBlock.appendNull();
            }
        }
        // Add the operation column == deleted
        INTEGER.writeLong(pageBuilder.getBlockBuilder(dataColumnChannels.size()), DELETE_OPERATION_NUMBER);
        pageBuilder.declarePosition();
    }

    private void addInsertRow(PageBuilder pageBuilder, List<Block> mergeCaseBlocks, int position)
    {
        int dataColumnCount = dataColumnChannels.size();
        // Copy the values from the merge block
        for (int targetChannel : dataColumnChannels) {
            Type columnType = dataColumnTypes.get(targetChannel);
            BlockBuilder targetBlock = pageBuilder.getBlockBuilder(targetChannel);
            // The value comes from that column of the page
            columnType.appendTo(mergeCaseBlocks.get(targetChannel), position, targetBlock);
        }
        // Add the operation column == insert
        INTEGER.writeLong(pageBuilder.getBlockBuilder(dataColumnCount), INSERT_OPERATION_NUMBER);
        pageBuilder.declarePosition();
    }
}
