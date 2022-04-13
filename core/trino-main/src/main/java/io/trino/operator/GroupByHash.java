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

import com.google.common.annotations.VisibleForTesting;
import io.trino.operator.aggregation.GroupedAggregator;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.ints.IntIterator;

import java.util.List;

import static io.trino.operator.WorkProcessor.ProcessState;

public interface GroupByHash
{
    long getEstimatedSize();

    long getHashCollisions();

    double getExpectedHashCollisions();

    List<Type> getTypes();

    int getGroupCount();

    void appendValuesTo(int groupId, PageBuilder pageBuilder);

    Work<?> addPage(Page page);

    Work<GroupByIdBlock> getGroupIds(Page page);

    boolean contains(int position, Page page);

    default boolean contains(int position, Page page, long rawHash)
    {
        return contains(position, page);
    }

    long getRawHash(int groupyId);

    @VisibleForTesting
    int getCapacity();

    default WorkProcessor<Page> buildResult(IntIterator groupIds, PageBuilder pageBuilder, List<GroupedAggregator> groupedAggregators)
    {
        return WorkProcessor.create(() -> {
            if (!groupIds.hasNext()) {
                return ProcessState.finished();
            }

            pageBuilder.reset();

            List<Type> types = getTypes();
            while (!pageBuilder.isFull() && groupIds.hasNext()) {
                int groupId = groupIds.nextInt();

                appendValuesTo(groupId, pageBuilder);

                pageBuilder.declarePosition();
                for (int i = 0; i < groupedAggregators.size(); i++) {
                    GroupedAggregator groupedAggregator = groupedAggregators.get(i);
                    BlockBuilder output = pageBuilder.getBlockBuilder(types.size() + i);
                    groupedAggregator.evaluate(groupId, output);
                }
            }

            return ProcessState.ofResult(pageBuilder.build());
        });
    }
}
