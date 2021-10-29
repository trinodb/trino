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
package io.trino.plugin.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.orc.OrcDeletedRows.MaskDeletedRowsFunction;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarRow;
import io.trino.spi.block.RowBlock;
import io.trino.spi.type.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.SYNTHESIZED;
import static io.trino.plugin.hive.HiveColumnHandle.UPDATE_ROW_ID_COLUMN_INDEX;
import static io.trino.plugin.hive.HiveColumnHandle.UPDATE_ROW_ID_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.HiveUpdatablePageSource.BUCKET_CHANNEL;
import static io.trino.plugin.hive.HiveUpdatablePageSource.ORIGINAL_TRANSACTION_CHANNEL;
import static io.trino.plugin.hive.HiveUpdatablePageSource.ROW_CHANNEL;
import static io.trino.plugin.hive.HiveUpdatablePageSource.ROW_ID_CHANNEL;
import static io.trino.plugin.hive.acid.AcidSchema.ACID_COLUMN_ROW_STRUCT;
import static io.trino.plugin.hive.acid.AcidSchema.ACID_READ_FIELDS;
import static io.trino.spi.block.ColumnarRow.toColumnarRow;
import static io.trino.spi.block.RowBlock.fromFieldBlocks;
import static io.trino.spi.type.RowType.Field;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.RowType.from;
import static java.util.Objects.requireNonNull;

public class HiveUpdateProcessor
{
    private final List<HiveColumnHandle> allDataColumns;
    private final List<HiveColumnHandle> updatedColumns;
    private final Set<String> updatedColumnNames;
    private final List<HiveColumnHandle> nonUpdatedColumns;
    private final Set<String> nonUpdatedColumnNames;

    @JsonCreator
    public HiveUpdateProcessor(
            @JsonProperty("allColumns") List<HiveColumnHandle> allDataColumns,
            @JsonProperty("updatedColumns") List<HiveColumnHandle> updatedColumns)
    {
        this.allDataColumns = requireNonNull(allDataColumns, "allDataColumns is null");
        this.updatedColumns = requireNonNull(updatedColumns, "updatedColumns is null");
        this.updatedColumnNames = updatedColumns.stream().map(HiveColumnHandle::getName).collect(toImmutableSet());
        Set<String> allDataColumnNames = allDataColumns.stream().map(HiveColumnHandle::getName).collect(toImmutableSet());
        checkArgument(allDataColumnNames.containsAll(updatedColumnNames), "allColumns does not contain all updatedColumns");
        this.nonUpdatedColumns = allDataColumns.stream()
                .filter(column -> !updatedColumnNames.contains(column.getName()))
                .collect(toImmutableList());
        this.nonUpdatedColumnNames = nonUpdatedColumns.stream().map(HiveColumnHandle::getName).collect(toImmutableSet());
    }

    @JsonProperty
    public List<HiveColumnHandle> getAllDataColumns()
    {
        return allDataColumns;
    }

    @JsonProperty
    public List<HiveColumnHandle> getUpdatedColumns()
    {
        return updatedColumns;
    }

    @JsonIgnore
    public List<HiveColumnHandle> getNonUpdatedColumns()
    {
        return nonUpdatedColumns;
    }

    /**
     * Merge the non-updated columns with the update dependencies, in allDataColumns order,
     * and finally add the rowId column as the last dependency.
     */
    public List<HiveColumnHandle> mergeWithNonUpdatedColumns(List<HiveColumnHandle> updateDependencies)
    {
        ImmutableList.Builder<HiveColumnHandle> builder = ImmutableList.builder();
        Set<String> updateDependencyNames = updateDependencies.stream().map(HiveColumnHandle::getName).collect(toImmutableSet());
        for (HiveColumnHandle handle : allDataColumns) {
            if (nonUpdatedColumnNames.contains(handle.getName()) || updateDependencyNames.contains(handle.getName())) {
                builder.add(handle);
            }
        }
        // The last updateDependency is the rowId column
        builder.add(updateDependencies.get(updateDependencies.size() - 1));
        return builder.build();
    }

    /**
     * Create a RowBlock containing four children: the three ACID columns - - originalTransaction,
     * rowId, bucket - - and a RowBlock containing all the data columns not changed
     * by the UPDATE statement.
     */
    public Block createUpdateRowBlock(Page page, List<Integer> nonUpdatedChannelNumbers, MaskDeletedRowsFunction maskDeletedRowsFunction)
    {
        requireNonNull(page, "page is null");
        requireNonNull(nonUpdatedChannelNumbers, "nonUpdatedChannelNumbers is null");
        int acidBlocks = 3;
        checkArgument(page.getChannelCount() >= acidBlocks + nonUpdatedColumns.size(), "page doesn't have enough columns");

        Block[] blocks = new Block[acidBlocks + (nonUpdatedColumns.isEmpty() ? 0 : 1)];
        blocks[ORIGINAL_TRANSACTION_CHANNEL] = page.getBlock(ORIGINAL_TRANSACTION_CHANNEL);
        blocks[BUCKET_CHANNEL] = page.getBlock(BUCKET_CHANNEL);
        blocks[ROW_ID_CHANNEL] = page.getBlock(ROW_ID_CHANNEL);

        if (!nonUpdatedColumns.isEmpty()) {
            Block[] nonUpdatedColumnBlocks = new Block[getNonUpdatedColumns().size()];
            int offset = 0;
            for (int sourceChannel : nonUpdatedChannelNumbers) {
                nonUpdatedColumnBlocks[offset] = page.getBlock(acidBlocks + sourceChannel);
                offset++;
            }
            blocks[ROW_CHANNEL] = RowBlock.fromFieldBlocks(page.getPositionCount(), Optional.empty(), nonUpdatedColumnBlocks);
        }
        return maskDeletedRowsFunction.apply(fromFieldBlocks(
                page.getPositionCount(),
                Optional.empty(),
                blocks));
    }

    /**
     * Project expects the page to begin with the update dependencies, followed by
     * the "rowId" column.  Remove columns from the page if they are not
     * update dependencies.
     */
    public Page removeNonDependencyColumns(Page page, List<Integer> dependencyChannels)
    {
        int dependencyCount = dependencyChannels.size();
        Block[] blocks = new Block[dependencyCount + 1];
        int index = 0;
        for (Integer channel : dependencyChannels) {
            blocks[index] = page.getBlock(channel);
            index++;
        }
        // Copy the rowId block
        blocks[dependencyCount] = page.getBlock(page.getChannelCount() - 1);
        return new Page(blocks);
    }

    /**
     * Return the column UPDATE column handle, which depends on the 3 ACID columns as well as the non-updated columns.
     */
    public static HiveColumnHandle getUpdateRowIdColumnHandle(List<HiveColumnHandle> nonUpdatedColumnHandles)
    {
        List<Field> allAcidFields = new ArrayList<>(ACID_READ_FIELDS);
        if (!nonUpdatedColumnHandles.isEmpty()) {
            RowType userColumnRowType = from(nonUpdatedColumnHandles.stream()
                    .map(column -> field(column.getName(), column.getType()))
                    .collect(toImmutableList()));

            allAcidFields.add(field(ACID_COLUMN_ROW_STRUCT, userColumnRowType));
        }
        RowType acidRowType = from(allAcidFields);
        return createBaseColumn(UPDATE_ROW_ID_COLUMN_NAME, UPDATE_ROW_ID_COLUMN_INDEX, toHiveType(acidRowType), acidRowType, SYNTHESIZED, Optional.empty());
    }

    public ColumnarRow getAcidBlock(Page page, List<Integer> columnValueAndRowIdChannels)
    {
        Block acidBlock = page.getBlock(columnValueAndRowIdChannels.get(columnValueAndRowIdChannels.size() - 1));
        return toColumnarRow(acidBlock);
    }

    /**
     * @param page The first block in the page is a RowBlock, containing the three ACID
     * columns - - originalTransaction, bucket and rowId - - plus a RowBlock containing
     * the values of non-updated columns. The remaining blocks are the values of the updated
     * columns, whose offsets given by columnValueAndRowIdChannels
     * @return The RowBlock for updated and non-updated columns
     */
    public Block createMergedColumnsBlock(Page page, List<Integer> columnValueAndRowIdChannels)
    {
        requireNonNull(page, "page is null");
        ColumnarRow acidBlock = getAcidBlock(page, columnValueAndRowIdChannels);
        int fieldCount = acidBlock.getFieldCount();
        List<Block> nonUpdatedColumnRowBlocks;
        if (nonUpdatedColumns.isEmpty()) {
            checkArgument(fieldCount == 3, "The ACID block must contain 3 children, but instead had %s children", fieldCount);
            nonUpdatedColumnRowBlocks = ImmutableList.of();
        }
        else {
            checkArgument(fieldCount == 4, "The first RowBlock must contain 4 children, but instead had %s children", fieldCount);
            Block lastAcidBlock = acidBlock.getField(3);
            checkArgument(lastAcidBlock instanceof RowBlock, "The last block in the acidBlock must be a RowBlock, but instead was %s", lastAcidBlock);
            ColumnarRow nonUpdatedColumnRow = toColumnarRow(lastAcidBlock);
            ImmutableList.Builder builder = ImmutableList.builder();
            for (int field = 0; field < nonUpdatedColumnRow.getFieldCount(); field++) {
                builder.add(nonUpdatedColumnRow.getField(field));
            }
            nonUpdatedColumnRowBlocks = builder.build();
        }

        // Merge the non-updated and updated column blocks
        Block[] dataColumnBlocks = new Block[allDataColumns.size()];
        int targetColumnChannel = 0;
        int nonUpdatedColumnChannel = 0;
        int updatedColumnNumber = 0;
        for (HiveColumnHandle column : allDataColumns) {
            Block block;
            if (nonUpdatedColumnNames.contains(column.getName())) {
                block = nonUpdatedColumnRowBlocks.get(nonUpdatedColumnChannel);
                nonUpdatedColumnChannel++;
            }
            else {
                int index = columnValueAndRowIdChannels.get(updatedColumnNumber);
                block = page.getBlock(index);
                updatedColumnNumber++;
            }
            dataColumnBlocks[targetColumnChannel] = block;
            targetColumnChannel++;
        }
        return RowBlock.fromFieldBlocks(page.getPositionCount(), Optional.empty(), dataColumnBlocks);
    }

    public List<Integer> makeDependencyChannelNumbers(List<HiveColumnHandle> dependencyColumns)
    {
        ImmutableList.Builder<Integer> dependencyIndexBuilder = ImmutableList.builder();
        Set<String> dependencyColumnNames = dependencyColumns.stream().map(HiveColumnHandle::getName).collect(toImmutableSet());
        int dependencyIndex = 0;
        for (HiveColumnHandle handle : allDataColumns) {
            if (dependencyColumnNames.contains(handle.getName())) {
                dependencyIndexBuilder.add(dependencyIndex);
                dependencyIndex++;
            }
            else if (nonUpdatedColumnNames.contains(handle.getName())) {
                dependencyIndex++;
            }
        }
        return dependencyIndexBuilder.build();
    }

    public List<Integer> makeNonUpdatedSourceChannels(List<HiveColumnHandle> dependencyColumns)
    {
        ImmutableMap.Builder<HiveColumnHandle, Integer> nonUpdatedNumbersBuilder = ImmutableMap.builder();
        Set<String> dependencyColumnNames = dependencyColumns.stream().map(HiveColumnHandle::getName).collect(toImmutableSet());
        int nonUpdatedIndex = 0;
        for (HiveColumnHandle handle : allDataColumns) {
            if (nonUpdatedColumnNames.contains(handle.getName())) {
                nonUpdatedNumbersBuilder.put(handle, nonUpdatedIndex);
                nonUpdatedIndex++;
            }
            else if (dependencyColumnNames.contains(handle.getName())) {
                nonUpdatedIndex++;
            }
        }
        Map<HiveColumnHandle, Integer> nonUpdatedMap = nonUpdatedNumbersBuilder.build();
        return nonUpdatedColumns.stream().map(nonUpdatedMap::get).collect(toImmutableList());
    }
}
