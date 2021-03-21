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
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.MergeProcessorUtilities;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.connector.RowChangeParadigm.CHANGE_ONLY_UPDATED_COLUMNS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ChangeOnlyUpdatedColumnsMergeProcessor
        implements MergeRowChangeProcessor
{
    private final List<ColumnHandle> dataColumns;
    private final List<Type> dataColumnTypes;
    private final List<ColumnHandle> writeRedistributionColumns;
    private final Type rowIdType;
    private final List<Integer> dataColumnChannels;
    private final DuplicateRowFinder duplicateRowFinder;

    public ChangeOnlyUpdatedColumnsMergeProcessor(List<ColumnHandle> dataColumns, List<Type> dataColumnTypes, List<ColumnHandle> writeRedistributionColumns, Type rowIdType)
    {
        this.dataColumns = dataColumns;
        this.dataColumnTypes = dataColumnTypes;
        this.writeRedistributionColumns = writeRedistributionColumns;
        this.rowIdType = requireNonNull(rowIdType, "rowIdType is null");
        List<Integer> dataColumnChannelsBuilder = new ArrayList<>();
        for (int channel = 0; channel < dataColumns.size(); channel++) {
            dataColumnChannelsBuilder.add(channel);
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
     * table column order, followed by a boolean column indicating whether the source row
     * matched any target row, followed by the "operation" block from the merge case RowBlock,
     * whose values are {@link MergeProcessorUtilities#INSERT_OPERATION_NUMBER},
     * {@link MergeProcessorUtilities#DELETE_OPERATION_NUMBER}, or
     * {@link MergeProcessorUtilities#UPDATE_OPERATION_NUMBER}
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
        duplicateRowFinder.checkForDuplicateTargetRows(inputPage, operationChannelBlock);

        List<Block> builder = new ArrayList<>();
        dataColumnChannels.forEach(channel -> builder.add(mergeCaseBlocks.get(channel)));
        builder.add(operationChannelBlock);

        // The rowId block is the last block of the resulting page
        Block rowIdBlock = inputPage.getBlock(writeRedistributionColumns.size());
        builder.add(rowIdBlock);
        Page result = new Page(builder.toArray(new Block[]{}));

        int defaultCaseCount = 0;
        for (int position = 0; position < positionCount; position++) {
            if (TINYINT.getLong(operationChannelBlock, position) == DEFAULT_CASE_OPERATION_NUMBER) {
                defaultCaseCount++;
            }
        }
        if (defaultCaseCount == 0) {
            return result;
        }
        int usedCases = 0;
        int[] positions = new int[positionCount - defaultCaseCount];
        for (int position = 0; position < positionCount; position++) {
            if (TINYINT.getLong(operationChannelBlock, position) != DEFAULT_CASE_OPERATION_NUMBER) {
                positions[usedCases] = position;
                usedCases++;
            }
        }
        checkArgument(usedCases + defaultCaseCount == positionCount, "usedCases (%s) + defaultCaseCount (%s) != positionCount (%s)", usedCases, defaultCaseCount, positionCount);
        return result.getPositions(positions, 0, usedCases);
    }
}
