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
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.trino.spi.connector.RowChangeParadigm.CHANGE_ONLY_UPDATED_COLUMNS;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ChangeOnlyUpdatedColumnsMergeProcessor
        implements MergeRowChangeProcessor
{
    private final List<ColumnHandle> dataColumns;
    private final List<Type> dataColumnTypes;
    private final List<ColumnHandle> writeRedistributionColumns;
    private final Type rowIdType;
    private final List<HandleAndChannel> dataColumnChannels;
    private final DuplicateRowFinder duplicateRowFinder;

    @JsonCreator
    public ChangeOnlyUpdatedColumnsMergeProcessor(
            @JsonProperty("dataColumns") List<ColumnHandle> dataColumns,
            @JsonProperty("dataColumnTypes") List<Type> dataColumnTypes,
            @JsonProperty("writeRedistributionColumns") List<ColumnHandle> writeRedistributionColumns,
            @JsonProperty("rowIdType") Type rowIdType)
    {
        this.dataColumns = dataColumns;
        this.dataColumnTypes = dataColumnTypes;
        this.writeRedistributionColumns = writeRedistributionColumns;
        this.rowIdType = requireNonNull(rowIdType, "rowIdType is null");
        int dataColumnChannel = 0;
        List<HandleAndChannel> dataColumnChannelsBuilder = new ArrayList<>();
        for (ColumnHandle handle : dataColumns) {
            dataColumnChannelsBuilder.add(new HandleAndChannel(handle, dataColumnChannel));
            dataColumnChannel++;
        }
        this.dataColumnChannels = Collections.unmodifiableList(dataColumnChannelsBuilder);
        this.duplicateRowFinder = new DuplicateRowFinder(dataColumns, dataColumnTypes, writeRedistributionColumns, rowIdType);
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm()
    {
        return CHANGE_ONLY_UPDATED_COLUMNS;
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
     * table column order; followed by the "operation" block from the merge case RowBlock,
     * whose values are {@link io.trino.spi.connector.MergeDetails#INSERT_OPERATION_NUMBER},
     * {@link io.trino.spi.connector.MergeDetails#DELETE_OPERATION_NUMBER}, or
     * {@link io.trino.spi.connector.MergeDetails#UPDATE_OPERATION_NUMBER}
     * @param inputPage The page to be transformed.
     * @return A page containing all data column blocks, followed by the operation block.
     */
    @Override
    public Page transformPage(Page inputPage)
    {
        requireNonNull(inputPage, "inputPage is null");
        int inputChannelCount = inputPage.getChannelCount();
        if (inputChannelCount != 2 + writeRedistributionColumns.size()) {
            throw new IllegalArgumentException(format("inputPage channelCount (%s) should be = 2 + %s", inputChannelCount, writeRedistributionColumns.size()));
        }

        int positionCount = inputPage.getPositionCount();
        if (positionCount <= 0) {
            throw new IllegalArgumentException("positionCount should be > 0, but is " + positionCount);
        }

        Block mergeCaseBlock = inputPage.getBlock(writeRedistributionColumns.size() + 1);

        List<Block> mergeCaseBlocks = mergeCaseBlock.getChildren();
        int mergeBlocksSize = mergeCaseBlocks.size();
        Block operationChannelBlock = mergeCaseBlocks.get(mergeBlocksSize - 1);
        // The rowId block is the last block of the resulting page
        Block rowIdBlock = inputPage.getBlock(writeRedistributionColumns.size());
        duplicateRowFinder.checkForDuplicateTargetRows(inputPage, operationChannelBlock);

        // Add the data columns
        List<Block> builder = new ArrayList<>();

        dataColumnChannels.forEach(handleAndChannel -> builder.add(mergeCaseBlocks.get(handleAndChannel.getChannel())));

        builder.add(operationChannelBlock);

        builder.add(rowIdBlock);
        return new Page(builder.toArray(new Block[]{}));
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
