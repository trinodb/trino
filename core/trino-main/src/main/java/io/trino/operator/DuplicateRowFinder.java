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

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators;
import io.trino.type.BlockTypeOperators.BlockPositionIsDistinctFrom;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.StandardErrorCode.MERGE_TARGET_ROW_MULTIPLE_MATCHES;
import static io.trino.spi.connector.MergeDetails.DEFAULT_CASE_OPERATION_NUMBER;
import static io.trino.spi.connector.MergeDetails.INSERT_OPERATION_NUMBER;

public class DuplicateRowFinder
{
    private final List<Integer> channels;
    private final List<BlockPositionIsDistinctFrom> channelPositionIsDistinctFrom;
    private Page lastPage;
    private int lastRowIdPosition = -1;

    public DuplicateRowFinder(List<ColumnHandle> dataColumns, List<Type> dataColumnTypes, List<ColumnHandle> writeRedistributionColumns, Type rowIdType)
    {
        // TODO: David said I ought to be able to inject this, but I don't see how
        BlockTypeOperators blockTypeOperators = new BlockTypeOperators();
        Map<ColumnHandle, Type> typeMap = IntStream.range(0, dataColumns.size())
                .boxed()
                .collect(toImmutableMap(dataColumns::get, dataColumnTypes::get));
        ImmutableList.Builder<Integer> channelsBuilder = ImmutableList.builder();
        ImmutableList.Builder<BlockPositionIsDistinctFrom> channelPositionIsDistinctFromBuilder = ImmutableList.builder();
        for (int channel = 0; channel < writeRedistributionColumns.size(); channel++) {
            channelsBuilder.add(channel);
            ColumnHandle handle = writeRedistributionColumns.get(channel);
            channelPositionIsDistinctFromBuilder.add(blockTypeOperators.getDistinctFromOperator(typeMap.get(handle)));
        }
        channelsBuilder.add(writeRedistributionColumns.size());
        channelPositionIsDistinctFromBuilder.add(blockTypeOperators.getDistinctFromOperator(rowIdType));
        channels = channelsBuilder.build();
        channelPositionIsDistinctFrom = channelPositionIsDistinctFromBuilder.build();
    }

    /**
     * This method looks for sequential duplicates in the target rowId block and redistribution column blocks.
     * A sequential duplicate signals that multiple target table rows matched a source row, which violates the
     * SQL MERGE spec.
     * @param page The page
     * @param operationBlock The operation block extracted from the MERGE case RowBlock.
     */
    public void checkForDuplicateTargetRows(Page page, Block operationBlock)
    {
        int positionCount = page.getPositionCount();
        for (int position = 0; position < positionCount; position++) {
            int operation = operationBlock.getInt(position, 0);
            if (operation != INSERT_OPERATION_NUMBER && operation != DEFAULT_CASE_OPERATION_NUMBER) {
                if (lastPage != null) {
                    boolean isDistinct = false;
                    for (int channel = 0; channel < channels.size(); channel++) {
                        if (channelPositionIsDistinctFrom.get(channel).isDistinctFrom(lastPage.getBlock(channel), lastRowIdPosition, page.getBlock(channel), position)) {
                            isDistinct = true;
                            break;
                        }
                    }
                    if (!isDistinct) {
                        throw new TrinoException(MERGE_TARGET_ROW_MULTIPLE_MATCHES, "One MERGE target table row matched more than one source row");
                    }
                }
                lastPage = page;
                lastRowIdPosition = position;
            }
        }
    }
}
