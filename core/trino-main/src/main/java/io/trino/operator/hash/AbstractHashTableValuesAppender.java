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
package io.trino.operator.hash;

import io.trino.operator.WorkProcessor;
import io.trino.operator.aggregation.GroupedAggregator;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import it.unimi.dsi.fastutil.ints.IntIterator;

import java.util.List;

import static io.trino.operator.WorkProcessor.ProcessState;
import static io.trino.operator.WorkProcessor.create;

public abstract class AbstractHashTableValuesAppender
        implements HashTableValuesAppender
{
    private static final int BATCH_SIZE = 64;

    // we use this class to isolate (using class loader) the method below.
    public WorkProcessor<Page> buildResult(HashTableData hashTableData, IntIterator groupIds, PageBuilder pageBuilder, List<GroupedAggregator> groupedAggregators, int accumulatorChannelOffset, boolean outputHash)
    {
        return create(() -> {
            if (!groupIds.hasNext()) {
                return ProcessState.finished();
            }

            pageBuilder.reset();
            int[] groupIdBatch = new int[BATCH_SIZE];
            int[] positionsBatch = new int[BATCH_SIZE];
            int[] valuesOffsetBatch = new int[BATCH_SIZE];
            boolean[] isNullBatch = new boolean[BATCH_SIZE];
            while (!pageBuilder.isFull() && groupIds.hasNext()) {
                int currentBatchSize = 0;
                for (; currentBatchSize < groupIdBatch.length && groupIds.hasNext(); currentBatchSize++) {
                    groupIdBatch[currentBatchSize] = groupIds.nextInt();
                }

                appendValuesTo(hashTableData, currentBatchSize, groupIdBatch, positionsBatch, valuesOffsetBatch, isNullBatch, pageBuilder, 0, outputHash);

                pageBuilder.declarePositions(currentBatchSize);
                for (int i = 0; i < groupedAggregators.size(); i++) {
                    GroupedAggregator groupedAggregator = groupedAggregators.get(i);
                    BlockBuilder output = pageBuilder.getBlockBuilder(accumulatorChannelOffset + i);
                    for (int j = 0; j < currentBatchSize; j++) {
                        int groupId = groupIdBatch[j];
                        groupedAggregator.evaluate(groupId, output);
                    }
                }
            }

            return ProcessState.ofResult(pageBuilder.build());
        });
    }

    protected void appendValuesTo(HashTableData data, int batchSize, int[] groupIdBatch,
            int[] positionsBatch, int[] valuesOffsetBatch, boolean[] isNullBatch, PageBuilder pageBuilder,
            int outputChannelOffset, boolean outputHash)
    {
        for (int i = 0; i < batchSize; i++) {
            appendValuesTo(data, groupIdBatch[i], pageBuilder, outputChannelOffset, outputHash);
        }
    }
}
