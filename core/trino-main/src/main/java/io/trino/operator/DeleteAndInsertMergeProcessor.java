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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.MergeDetails;
import io.trino.spi.connector.MergeProcessorUtilities;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.connector.MergeDetails.DELETE_OPERATION_NUMBER;
import static io.trino.spi.connector.MergeDetails.INSERT_OPERATION_NUMBER;
import static io.trino.spi.connector.MergeDetails.UPDATE_OPERATION_NUMBER;
import static io.trino.spi.connector.MergeProcessorUtilities.getPositionsForPredicate;
import static io.trino.spi.connector.MergeProcessorUtilities.getUnderlyingBlock;
import static io.trino.spi.connector.RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DeleteAndInsertMergeProcessor
        implements MergeRowChangeProcessor
{
    private final MergeDetails mergeDetails;
    private final List<ColumnHandle> dataColumns;
    private final List<Type> dataColumnTypes;
    private final List<ColumnHandle> writeRedistributionColumns;
    private final Type rowIdType;
    private final List<HandleAndChannel> dataColumnChannels;
    private final List<HandleAndChannel> writeRedistributionColumnChannels;

    private final DuplicateRowFinder duplicateRowFinder;

    @JsonCreator
    public DeleteAndInsertMergeProcessor(
            @JsonProperty("mergeDetails") MergeDetails mergeDetails,
            @JsonProperty("dataColumns") List<ColumnHandle> dataColumns,
            @JsonProperty("dataColumnTypes") List<Type> dataColumnTypes,
            @JsonProperty("writeRedistributionColumns") List<ColumnHandle> writeRedistributionColumns,
            @JsonProperty("rowIdType") Type rowIdType)
    {
        this.mergeDetails = requireNonNull(mergeDetails, "mergeDetails is null");
        this.dataColumns = requireNonNull(dataColumns, "dataColumns is null");
        this.dataColumnTypes = requireNonNull(dataColumnTypes, "dataColumnTypes is null");
        this.rowIdType = requireNonNull(rowIdType, "rowIdType is null");
        this.writeRedistributionColumns = requireNonNull(writeRedistributionColumns, "writeRedistributionColumns is null");
        int dataColumnChannel = 0;
        ImmutableList.Builder<HandleAndChannel> dataColumnChannelsBuilder = ImmutableList.builder();
        ImmutableList.Builder<HandleAndChannel> writeRedistributionChannelsBuilder = ImmutableList.builder();
        for (ColumnHandle handle : dataColumns) {
            dataColumnChannelsBuilder.add(new HandleAndChannel(handle, dataColumnChannel));
            dataColumnChannel++;
            if (writeRedistributionColumns.contains(handle)) {
                writeRedistributionChannelsBuilder.add(new HandleAndChannel(handle, dataColumnChannel));
            }
        }
        this.dataColumnChannels = dataColumnChannelsBuilder.build();
        this.writeRedistributionColumnChannels = ImmutableList.copyOf(writeRedistributionChannelsBuilder.build());
        this.duplicateRowFinder = new DuplicateRowFinder(dataColumns, dataColumnTypes, writeRedistributionColumns, rowIdType);
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm()
    {
        return DELETE_ROW_AND_INSERT_ROW;
    }

    @JsonProperty
    public MergeDetails getMergeDetails()
    {
        return mergeDetails;
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
     * Transform the input page containing the target table's partition column blocks; the
     * rowId block; and the merge case RowBlock into a page that duplicates rows for
     * UPDATE_OPERATION_NUMBER operations.  Each row in the outputPage starts with all the
     * data column blocks, including the partition columns blocks, in declared column order;
     * followed by an operation block with DELETE_OPERATION_NUMBER for delete rows or
     * INSERT_OPERATION_NUMBER for insert rows; followed by the rowId block.
     * {@link Block#getPositions(int[], int, int)} is used to do the duplication, so no value
     * copying is required to project the columns into the page.
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
        if (inputChannelCount < 2 + writeRedistributionColumnChannels.size()) {
            throw new IllegalArgumentException(format("inputPage channelCount (%s) should be >= 2 + partition columns size (%s)", inputChannelCount, writeRedistributionColumnChannels.size()));
        }

        int originalPositionCount = inputPage.getPositionCount();
        if (originalPositionCount <= 0) {
            throw new IllegalArgumentException("originalPositionCount should be > 0, but is " + originalPositionCount);
        }
        Block mergeCaseBlock = inputPage.getBlock(writeRedistributionColumnChannels.size() + 1);
        int[] operationPositions = getPositionsExcludingNotMatchedCases(mergeCaseBlock);
        int positionCount = operationPositions.length;

        List<Block> mergeCaseBlocks = mergeCaseBlock.getChildren();
        int mergeBlocksSize = mergeCaseBlocks.size();
        Block operationChannelBlock = mergeCaseBlocks.get(mergeBlocksSize - 1);
        Block rowIdBlock = inputPage.getBlock(writeRedistributionColumnChannels.size());
        duplicateRowFinder.checkForDuplicateTargetRows(inputPage, operationChannelBlock);

        Block restrictedOperationChannelBlock = operationChannelBlock.getPositions(operationPositions, 0, positionCount);
        Block caseNumberBlock = mergeCaseBlocks.get(mergeBlocksSize - 2);
        int[] deletePositions = getDeletePositions(positionCount, restrictedOperationChannelBlock);
        int deleteCount = deletePositions.length;
        int[] insertPositions = getInsertPositions(positionCount, restrictedOperationChannelBlock);
        int insertCount = insertPositions.length;
        int totalPositions = deleteCount + insertCount;

        // Create a position array with both the delete and insert positions,
        // so that it duplicates the UPDATE rows
        int[] deleteAndInsertPositions = new int[totalPositions];
        int[] operationBlockArray = new int[totalPositions];
        for (int offset = 0; offset < deleteCount; offset++) {
            deleteAndInsertPositions[offset] = deletePositions[offset];
            operationBlockArray[offset] = DELETE_OPERATION_NUMBER;
        }
        for (int offset = 0; offset < insertCount; offset++) {
            deleteAndInsertPositions[deleteCount + offset] = insertPositions[offset];
            operationBlockArray[deleteCount + offset] = INSERT_OPERATION_NUMBER;
        }

        // Add the data columns
        List<Block> builder = new ArrayList<>();

        // Partition column blocks for the target table, required to route deletes,
        // are at the top level of the inputPage, starting with channel 2
        int writeRedistributionColumnIndex = 0;
        for (int index = 0; index < dataColumnChannels.size(); index++) {
            HandleAndChannel handleAndChannel = dataColumnChannels.get(index);
            Type columnType = dataColumnTypes.get(index);
            ColumnHandle column = handleAndChannel.getHandle();
            int channel = handleAndChannel.getChannel();
            Block mergeBlock = mergeCaseBlocks.get(channel);
            if (writeRedistributionColumns.contains(column)) {
                Block block = inputPage.getBlock(writeRedistributionColumnIndex);
                builder.add(buildWriteRedistributionColumnValues(columnType, deleteAndInsertPositions, deleteCount, mergeBlock, block));
                writeRedistributionColumnIndex++;
            }
            else {
                DictionaryBlock cleanedBlock = new DictionaryBlock(mergeBlock, operationPositions);
                Block deleteAndInsertBlock = ((DictionaryBlock) cleanedBlock.getPositions(deleteAndInsertPositions, 0, totalPositions)).compact();
                builder.add(deleteAndInsertBlock);
            }
        }

        // Add the operations block
        builder.add(new IntArrayBlock(totalPositions, Optional.empty(), operationBlockArray));

        Block newRowIdBlock;
        Block underlyingBlock = getUnderlyingBlock(rowIdBlock);
        if (rowIdBlock.allPositionsAreNull()) {
            newRowIdBlock = MergeProcessorUtilities.getAllNullsRowIdBlock(rowIdBlock, underlyingBlock, totalPositions);
        }
        else {
            int[] rowIdPositions = getRowIdPositions(operationChannelBlock, caseNumberBlock, totalPositions);
            if (underlyingBlock instanceof RowBlock) {
                List<Block> newRowIdChildrenBuilder = new ArrayList<>();
                rowIdBlock.getChildren().stream()
                        .map(block -> block.getPositions(rowIdPositions, 0, totalPositions))
                        .forEach(newRowIdChildrenBuilder::add);
                newRowIdBlock = RowBlock.fromFieldBlocks(
                        totalPositions,
                        Optional.empty(),
                        newRowIdChildrenBuilder.toArray(new Block[] {}));
            }
            else {
                newRowIdBlock = rowIdBlock.getPositions(rowIdPositions, 0, totalPositions);
            }
        }
        builder.add(newRowIdBlock);

        return new Page(builder.toArray(new Block[]{}));
    }

    private Block buildWriteRedistributionColumnValues(Type columnType, int[] deleteAndInsertPositions, int deleteCount, Block mergeBlock, Block writeRedistributionColumnBlock)
    {
        int positionCount = deleteAndInsertPositions.length;
        BlockBuilder builder = columnType.createBlockBuilder(null, positionCount, 0);
        for (int position = 0; position < positionCount; position++) {
            int dictionaryPosition = deleteAndInsertPositions[position];
            if (position < deleteCount) {
                // The value comes from the target table's partition block
                columnType.appendTo(writeRedistributionColumnBlock, dictionaryPosition, builder);
            }
            else {
                // The value comes from the mergeCaseBlock
                columnType.appendTo(mergeBlock, dictionaryPosition, builder);
            }
        }
        return builder.build();
    }

    private static int[] getRowIdPositions(Block operationBlock, Block caseNumberBlock, int finalPositionCount)
    {
        int inputPositions = caseNumberBlock.getPositionCount();
        int[] positions = new int[finalPositionCount];
        int rowIdCursor = 0;
        int positionCursor = 0;
        for (int position = 0; position < inputPositions; position++) {
            if (caseNumberBlock.getInt(position, 0) != -1) {
                int operation = operationBlock.getInt(position, 0);
                if (operation != INSERT_OPERATION_NUMBER) {
                    positions[positionCursor] = rowIdCursor;
                    rowIdCursor++;
                    positionCursor++;
                }
            }
        }
        for (int position = 0; position < caseNumberBlock.getPositionCount(); position++) {
            if (caseNumberBlock.getInt(position, 0) != -1) {
                int operation = operationBlock.getInt(position, 0);
                if (operation != DELETE_OPERATION_NUMBER) {
                    positions[positionCursor] = 0;
                    positionCursor++;
                }
            }
        }
        if (positionCursor != finalPositionCount) {
            throw new IllegalArgumentException(format("positionCursor (%s) is not equal to finalPositionCount (%s)", positionCursor, finalPositionCount));
        }
        return positions;
    }

    private static int[] getPositionsExcludingNotMatchedCases(Block mergeCaseBlock)
    {
        List<Block> mergeCaseBlocks = mergeCaseBlock.getChildren();
        int mergeBlocksSize = mergeCaseBlocks.size();
        Block caseNumberBlock = mergeCaseBlocks.get(mergeBlocksSize - 2);
        int counter = 0;
        for (int position = 0; position < caseNumberBlock.getPositionCount(); position++) {
            if (caseNumberBlock.getInt(position, 0) != -1) {
                counter++;
            }
        }
        int cursor = 0;
        int[] positions = new int[counter];
        for (int position = 0; position < caseNumberBlock.getPositionCount(); position++) {
            if (caseNumberBlock.getInt(position, 0) != -1) {
                positions[cursor] = position;
                cursor++;
            }
        }
        return positions;
    }

    private static int[] getDeletePositions(int positionCount, Block operationBlock)
    {
        return getPositionsForPredicate(positionCount, position -> {
            int operation = operationBlock.getInt(position, 0);
            return operation == DELETE_OPERATION_NUMBER || operation == UPDATE_OPERATION_NUMBER;
        });
    }

    private static int[] getInsertPositions(int positionCount, Block operationBlock)
    {
        return getPositionsForPredicate(positionCount, position -> {
            int operation = operationBlock.getInt(position, 0);
            return operation == INSERT_OPERATION_NUMBER || operation == UPDATE_OPERATION_NUMBER;
        });
    }

    private static class HandleAndChannel
    {
        private final ColumnHandle handle;
        private final int channel;

        public HandleAndChannel(ColumnHandle handle, int channel)
        {
            this.handle = handle;
            this.channel = channel;
        }

        public ColumnHandle getHandle()
        {
            return handle;
        }

        public int getChannel()
        {
            return channel;
        }
    }
}
